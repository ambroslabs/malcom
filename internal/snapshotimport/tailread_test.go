package snapshotimport

import (
	"bytes"
	"compress/zlib"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestTailingChunkSourceRoundTrip stages a 3-chunk zlib payload on
// disk in arbitrary order, drives MarkReady from a producer
// goroutine, and confirms the consumer reads the exact decompressed
// bytes back through TailingChunkSource.
func TestTailingChunkSourceRoundTrip(t *testing.T) {
	dir := t.TempDir()
	want := []byte("hello tailing reader — this is the decompressed snapshot stream payload")

	// Build a single zlib stream, then split into 3 fixed-size on-disk
	// chunks (matching cosmos-sdk snapshot wire convention).
	chunks := buildZlibChunks(t, want, 3)

	src := NewTailingChunkSource(dir, uint32(len(chunks)))
	defer src.Close()

	// Stage chunks on disk in reverse order, then mark them ready in
	// stream order. Verifies: (a) MarkReady out of order from when
	// the chunk was written is fine; (b) Read blocks on the missing
	// next-in-order chunk even when later chunks are already on disk.
	for i := len(chunks) - 1; i >= 0; i-- {
		writeChunkAtomic(t, dir, uint32(i), chunks[i])
	}
	go func() {
		for i := uint32(0); i < uint32(len(chunks)); i++ {
			time.Sleep(5 * time.Millisecond)
			src.MarkReady(i)
		}
	}()

	got, err := io.ReadAll(src)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("decompressed bytes mismatch:\n got %q\nwant %q", got, want)
	}
}

// TestTailingChunkSourceBlocksUntilReady starts a Read in a goroutine
// before chunk 0 exists; the read must block until MarkReady fires.
func TestTailingChunkSourceBlocksUntilReady(t *testing.T) {
	dir := t.TempDir()
	want := bytes.Repeat([]byte("x"), 4096)
	chunks := buildZlibChunks(t, want, 1)

	src := NewTailingChunkSource(dir, 1)
	defer src.Close()

	// Spawn the consumer first; it should park inside zlib.NewReader
	// until the producer marks chunk 0 ready.
	done := make(chan []byte, 1)
	errCh := make(chan error, 1)
	go func() {
		got, err := io.ReadAll(src)
		if err != nil {
			errCh <- err
			return
		}
		done <- got
	}()

	// Brief wait — read should still be blocked.
	select {
	case b := <-done:
		t.Fatalf("Read returned before MarkReady: %d bytes", len(b))
	case err := <-errCh:
		t.Fatalf("Read errored before MarkReady: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	writeChunkAtomic(t, dir, 0, chunks[0])
	src.MarkReady(0)

	select {
	case got := <-done:
		if !bytes.Equal(got, want) {
			t.Fatalf("payload mismatch")
		}
	case err := <-errCh:
		t.Fatalf("Read failed: %v", err)
	case <-time.After(time.Second):
		t.Fatalf("Read did not return after MarkReady")
	}
}

// TestTailingChunkSourceFail confirms that Fail() unblocks a stalled
// reader with the supplied error.
func TestTailingChunkSourceFail(t *testing.T) {
	dir := t.TempDir()
	src := NewTailingChunkSource(dir, 1)
	defer src.Close()

	wantErr := errors.New("synthetic fetch failure")
	errCh := make(chan error, 1)
	go func() {
		_, err := io.ReadAll(src)
		errCh <- err
	}()

	time.Sleep(20 * time.Millisecond)
	src.Fail(wantErr)

	select {
	case err := <-errCh:
		if !errors.Is(err, wantErr) {
			t.Fatalf("expected err to wrap %v, got %v", wantErr, err)
		}
	case <-time.After(time.Second):
		t.Fatalf("Read did not surface Fail() error")
	}
}

// buildZlibChunks zlib-compresses payload then splits the compressed
// bytes into n equal-sized chunks (last chunk picks up the remainder),
// mimicking cosmos-sdk's chunked-snapshot wire convention.
func buildZlibChunks(t *testing.T, payload []byte, n int) [][]byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zlib.NewWriter(&buf)
	if _, err := zw.Write(payload); err != nil {
		t.Fatalf("zlib write: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("zlib close: %v", err)
	}
	compressed := buf.Bytes()
	if n <= 0 {
		n = 1
	}
	chunkLen := len(compressed) / n
	if chunkLen == 0 {
		chunkLen = len(compressed)
	}
	chunks := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		start := i * chunkLen
		end := start + chunkLen
		if i == n-1 {
			end = len(compressed)
		}
		chunks = append(chunks, append([]byte(nil), compressed[start:end]...))
	}
	return chunks
}

// writeChunkAtomic mirrors snapfetch.writeFileAtomic — write tmp,
// fsync, rename — so the chunk file appears atomically. We can't
// import snapfetch.writeFileAtomic (it's unexported) so this is a
// minimal copy adequate for tests.
func writeChunkAtomic(t *testing.T, dir string, idx uint32, data []byte) {
	t.Helper()
	final := filepath.Join(dir, fmt.Sprintf("chunk_%05d.bin", idx))
	tmp := final + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		t.Fatalf("write tmp: %v", err)
	}
	if err := os.Rename(tmp, final); err != nil {
		t.Fatalf("rename: %v", err)
	}
}

// TestTailingChunkSourceReady_BeforeRead confirms a producer that
// races ahead and marks every chunk ready before any consumer Read
// is fine — the consumer reads through to completion without
// blocking on the cv.
func TestTailingChunkSourceReady_BeforeRead(t *testing.T) {
	dir := t.TempDir()
	want := bytes.Repeat([]byte("y"), 8192)
	chunks := buildZlibChunks(t, want, 4)
	for i, c := range chunks {
		writeChunkAtomic(t, dir, uint32(i), c)
	}

	src := NewTailingChunkSource(dir, uint32(len(chunks)))
	defer src.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := uint32(0); i < uint32(len(chunks)); i++ {
			src.MarkReady(i)
		}
	}()
	wg.Wait()

	got, err := io.ReadAll(src)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("payload mismatch")
	}
}
