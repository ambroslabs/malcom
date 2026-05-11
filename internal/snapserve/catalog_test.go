package snapserve

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// makeSnapshotDirIn writes a valid snapshot dir under parent with the
// given chain_id + payloads. Returns the child dir path. Reuses the
// metadata + chunk hashing primitives shared with store_test.go.
func makeSnapshotDirIn(t *testing.T, parent, chainID, subdir string, payloads [][]byte) string {
	t.Helper()
	dir := filepath.Join(parent, subdir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	chunkHashes := make([][]byte, len(payloads))
	agg := sha256.New()
	for i, p := range payloads {
		writeChunk(t, dir, uint32(i), p)
		h := sha256.Sum256(p)
		chunkHashes[i] = h[:]
		agg.Write(p)
	}
	if err := os.WriteFile(
		filepath.Join(dir, "metadata.bin"),
		buildMetadataBin(chunkHashes),
		0o644,
	); err != nil {
		t.Fatal(err)
	}
	mdLen := 0
	for _, h := range chunkHashes {
		mdLen += 1 + 1 + len(h)
	}
	// Encode payload count + total byte count into the pseudo-height
	// so multiple snapshots within one test get distinct heights
	// without the caller threading one through.
	var totalBytes uint64
	for _, p := range payloads {
		totalBytes += uint64(len(p))
	}
	meta := metaJSON{
		ChainID:     chainID,
		Height:      uint64(len(payloads))*1_000_000 + totalBytes,
		Format:      3,
		Chunks:      uint32(len(payloads)),
		HashHex:     hex.EncodeToString(agg.Sum(nil)),
		MetadataLen: mdLen,
	}
	mb, _ := json.Marshal(meta)
	if err := os.WriteFile(filepath.Join(dir, "meta.json"), mb, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".complete"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestLoadStoreFromRoot_MissingComplete(t *testing.T) {
	root := t.TempDir()
	// Make a dir with only a .partial file — no .complete. Must be
	// silently skipped.
	bad := filepath.Join(root, "incoming")
	if err := os.MkdirAll(bad, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bad, "chunk_00000.bin"), []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}
	// And a good dir alongside.
	good := makeSnapshotDirIn(t, root, "osmosis-1", "snapshot_osmosis-1_1", [][]byte{
		[]byte("hello"), []byte("world"),
	})
	store, err := LoadStoreFromRoot(root, "osmosis-1", VerifyPerChunkHash, nil)
	if err != nil {
		t.Fatalf("LoadStoreFromRoot: %v", err)
	}
	if got := store.Len(); got != 1 {
		t.Fatalf("want 1 snapshot (good dir only), got %d", got)
	}
	if store.snapshots[0].Dir != good {
		t.Fatalf("loaded wrong dir: %s", store.snapshots[0].Dir)
	}
}

func TestLoadStoreFromRoot_ChainFilter(t *testing.T) {
	root := t.TempDir()
	// Three dirs: two chains. -chain osmosis-1 should match only the
	// matching one; the cosmoshub one is skipped (not an error).
	makeSnapshotDirIn(t, root, "osmosis-1", "a", [][]byte{[]byte("os-1-a")})
	makeSnapshotDirIn(t, root, "cosmoshub-4", "b", [][]byte{[]byte("ch-1")})
	makeSnapshotDirIn(t, root, "osmosis-1", "c", [][]byte{[]byte("os-1-c-payload")})
	store, err := LoadStoreFromRoot(root, "osmosis-1", VerifyMetadataOnly, nil)
	if err != nil {
		t.Fatalf("LoadStoreFromRoot: %v", err)
	}
	if store.Len() != 2 {
		t.Fatalf("want 2 osmosis-1 snapshots, got %d", store.Len())
	}
	for _, s := range store.snapshots {
		if s.ChainID != "osmosis-1" {
			t.Fatalf("wrong-chain snapshot leaked through: %s", s.ChainID)
		}
	}
}

func TestLoadStoreFromRoot_TamperedSkippedButOthersServed(t *testing.T) {
	root := t.TempDir()
	good := makeSnapshotDirIn(t, root, "osmosis-1", "a", [][]byte{[]byte("clean")})
	bad := makeSnapshotDirIn(t, root, "osmosis-1", "b", [][]byte{[]byte("original")})
	if err := os.WriteFile(filepath.Join(bad, "chunk_00000.bin"), []byte("tampered"), 0o644); err != nil {
		t.Fatal(err)
	}
	store, err := LoadStoreFromRoot(root, "osmosis-1", VerifyAggregateHash, nil)
	if err != nil {
		t.Fatalf("LoadStoreFromRoot: %v", err)
	}
	if store.Len() != 1 {
		t.Fatalf("want 1 surviving snapshot, got %d", store.Len())
	}
	if store.snapshots[0].Dir != good {
		t.Fatalf("kept wrong dir: %s", store.snapshots[0].Dir)
	}
}

func TestLoadStoreFromRoot_EmptyDirIsLegal(t *testing.T) {
	root := t.TempDir()
	store, err := LoadStoreFromRoot(root, "osmosis-1", VerifyMetadataOnly, nil)
	if err != nil {
		t.Fatalf("empty root should not error: %v", err)
	}
	if store.Len() != 0 {
		t.Fatalf("empty root should yield empty store, got %d", store.Len())
	}
}

func TestLoadStoreFromRoot_MissingRootIsError(t *testing.T) {
	if _, err := LoadStoreFromRoot("/nonexistent/path/that/should/not/exist", "x", VerifyMetadataOnly, nil); err == nil {
		t.Fatal("expected error for missing root")
	}
}

func TestCatalog_RescanPicksUpDroppedDir(t *testing.T) {
	root := t.TempDir()
	var (
		emitted   atomic.Int32
		latestLen atomic.Int32
	)
	cat := NewCatalog(CatalogConfig{
		RootDir:        root,
		ChainID:        "osmosis-1",
		VerifyMode:     VerifyPerChunkHash,
		RescanInterval: 0, // trigger-only — we'll Rescan manually
		OnStore: func(s *Store) {
			emitted.Add(1)
			latestLen.Store(int32(s.Len()))
		},
	})
	if err := cat.Rescan(context.Background()); err != nil {
		t.Fatalf("initial rescan: %v", err)
	}
	if got := emitted.Load(); got != 1 {
		t.Fatalf("first rescan should emit once, got %d", got)
	}
	if got := latestLen.Load(); got != 0 {
		t.Fatalf("first emit should carry empty store, got len=%d", got)
	}

	// Drop a snapshot in, rescan — should emit a new store.
	makeSnapshotDirIn(t, root, "osmosis-1", "first", [][]byte{[]byte("payload-1")})
	if err := cat.Rescan(context.Background()); err != nil {
		t.Fatalf("rescan after drop: %v", err)
	}
	if got := emitted.Load(); got != 2 {
		t.Fatalf("change should re-emit, got %d total emits", got)
	}
	if got := latestLen.Load(); got != 1 {
		t.Fatalf("new store should have 1 snapshot, got len=%d", got)
	}

	// No-op rescan — fingerprint matches, no emit.
	if err := cat.Rescan(context.Background()); err != nil {
		t.Fatalf("idempotent rescan: %v", err)
	}
	if got := emitted.Load(); got != 2 {
		t.Fatalf("unchanged catalog should not re-emit, got %d", got)
	}

	// Remove the snapshot and rescan — should re-emit with empty store.
	if err := os.RemoveAll(filepath.Join(root, "first")); err != nil {
		t.Fatal(err)
	}
	if err := cat.Rescan(context.Background()); err != nil {
		t.Fatalf("rescan after remove: %v", err)
	}
	if got := emitted.Load(); got != 3 {
		t.Fatalf("removal should re-emit, got %d", got)
	}
	if got := latestLen.Load(); got != 0 {
		t.Fatalf("after removal store should be empty, got len=%d", got)
	}
}

func TestCatalog_TriggerCoalesces(t *testing.T) {
	root := t.TempDir()
	makeSnapshotDirIn(t, root, "osmosis-1", "a", [][]byte{[]byte("x")})
	cat := NewCatalog(CatalogConfig{
		RootDir:        root,
		ChainID:        "osmosis-1",
		VerifyMode:     VerifyMetadataOnly,
		RescanInterval: 0,
	})
	if err := cat.Rescan(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Bursts of Trigger() must not panic and must not block (channel
	// is buffered 1).
	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			cat.Trigger()
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Trigger should be non-blocking")
	}
	// Drain whatever trigger ended up enqueued.
	select {
	case <-cat.trigger:
	default:
	}
}

func TestCatalog_SwapDuringConcurrentLoadChunk(t *testing.T) {
	// Confirms the atomic.Pointer swap in statesync.Reactor doesn't
	// race with in-flight LoadChunk calls. We don't need the full
	// reactor here — exercise the Store interface directly under
	// concurrent rescan + read.
	root := t.TempDir()
	makeSnapshotDirIn(t, root, "osmosis-1", "snap-a", [][]byte{[]byte("payload-AA")})
	store1, err := LoadStoreFromRoot(root, "osmosis-1", VerifyPerChunkHash, nil)
	if err != nil {
		t.Fatal(err)
	}
	makeSnapshotDirIn(t, root, "osmosis-1", "snap-b", [][]byte{[]byte("payload-BBB"), []byte("payload-CCCC")})
	store2, err := LoadStoreFromRoot(root, "osmosis-1", VerifyPerChunkHash, nil)
	if err != nil {
		t.Fatal(err)
	}

	var current atomic.Pointer[Store]
	current.Store(store1)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			s := current.Load()
			for _, snap := range s.ListSnapshots() {
				_, _, _ = s.LoadChunk(snap.Height, snap.Format, 0)
			}
		}
	}()
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			if current.Load() == store1 {
				current.Store(store2)
			} else {
				current.Store(store1)
			}
		}
	}()
	wg.Wait()
	// If we made it here without -race detecting anything, we're good.
}
