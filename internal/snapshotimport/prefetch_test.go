package snapshotimport

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"testing"
)

func TestPrefetchReaderEqualsSource(t *testing.T) {
	for _, sz := range []int{0, 1, 123, 4096, 1 << 16, (1 << 20) + 17} {
		sz := sz
		t.Run("", func(t *testing.T) {
			src := make([]byte, sz)
			_, _ = rand.Read(src)
			pf := newPrefetchReader(context.Background(), bytes.NewReader(src), 4, 1<<10)
			defer pf.Close()
			got, err := io.ReadAll(pf)
			if err != nil {
				t.Fatalf("ReadAll: %v", err)
			}
			if !bytes.Equal(got, src) {
				t.Fatalf("data mismatch (size=%d): got %d bytes, want %d", sz, len(got), len(src))
			}
		})
	}
}

func TestPrefetchReaderShortReads(t *testing.T) {
	// Stress the partial-drain path: drain p.cur byte-by-byte across
	// many buffers, exercising the per-Read recycle/refill loop.
	src := make([]byte, (1<<20)+777)
	for i := range src {
		src[i] = byte(i)
	}
	pf := newPrefetchReader(context.Background(), bytes.NewReader(src), 3, 4096)
	defer pf.Close()
	one := make([]byte, 1)
	got := make([]byte, 0, len(src))
	for {
		n, err := pf.Read(one)
		got = append(got, one[:n]...)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Equal(got, src) {
		t.Fatalf("data mismatch: got %d bytes, want %d", len(got), len(src))
	}
}

func TestPrefetchReaderPropagatesError(t *testing.T) {
	want := errors.New("synthetic source failure")
	src := &errReader{data: bytes.NewReader([]byte("hello world")), err: want, after: 11}
	pf := newPrefetchReader(context.Background(), src, 2, 4)
	defer pf.Close()

	got, err := io.ReadAll(pf)
	if !errors.Is(err, want) {
		t.Fatalf("ReadAll err = %v, want %v", err, want)
	}
	if string(got) != "hello world" {
		t.Fatalf("got %q, want %q", got, "hello world")
	}
}

func TestPrefetchReaderCloseStopsProducer(t *testing.T) {
	// A reader that never EOFs should not leak its goroutine after
	// Close. Use a slow source so we know we're cancelling mid-flight.
	src := &slowReader{}
	pf := newPrefetchReader(context.Background(), src, 2, 1024)
	one := make([]byte, 1)
	if _, err := pf.Read(one); err != nil {
		t.Fatalf("first Read: %v", err)
	}
	if err := pf.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// done channel closing is what unblocks Close. If Close returned
	// without panic, the producer exited cleanly.
}

// errReader returns the wrapped data, then injects err after `after`
// bytes have been served.
type errReader struct {
	data  *bytes.Reader
	err   error
	after int
	read  int
}

func (e *errReader) Read(p []byte) (int, error) {
	if e.read >= e.after {
		return 0, e.err
	}
	max := e.after - e.read
	if max > len(p) {
		max = len(p)
	}
	n, err := e.data.Read(p[:max])
	e.read += n
	if err == io.EOF && e.read >= e.after {
		return n, e.err
	}
	return n, err
}

// slowReader emits a single byte per Read forever (until Close
// cancels the producer's context, surfaced via the prefetch
// reader's cancel).
type slowReader struct{}

func (slowReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	p[0] = 'x'
	return 1, nil
}
