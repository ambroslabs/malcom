package snapshotimport

import (
	"errors"
	"testing"
)

// TestNewReaderRejectsStartBelowHead is the regression test for
// #124. NewReader used to silently accept a start offset below the
// ring's head; the resulting reader's first Read returned
// "chunkRingReader: pos N below ring head H" deep inside an import
// worker, surfaced as a wrapped error chain like
// `store "provider": read item: read envelope length: ...`. The
// fix surfaces the same condition synchronously at construction so
// the failure carries actionable context (which call site, which
// offset).
func TestNewReaderRejectsStartBelowHead(t *testing.T) {
	// Tiny ring, deterministic geometry. 4 MiB chunks isn't relevant
	// here — we drive eviction manually by closing the only reader.
	ring := newChunkRing(4<<20, 16<<20)

	// Write some bytes so there's something to evict.
	if _, err := ring.Write(make([]byte, 8<<20)); err != nil {
		t.Fatalf("ring.Write: %v", err)
	}

	// Advance head by registering a reader, walking it forward, and
	// closing it — this is the in-production sequence that leaves
	// head pinned above its final pos when r.readers becomes empty.
	r1, err := ring.NewReader(0, -1)
	if err != nil {
		t.Fatalf("NewReader(0): %v", err)
	}
	// Read 6 MiB to advance pos.
	buf := make([]byte, 6<<20)
	if _, err := r1.Read(buf); err != nil {
		t.Fatalf("r1.Read: %v", err)
	}
	r1.Close()

	if ring.head == 0 {
		t.Fatalf("setup: ring.head still 0 after reading + closing r1; head should have advanced via eviction")
	}

	// New reader starting BELOW head must fail synchronously with a
	// recognisable sentinel.
	_, err = ring.NewReader(0, -1)
	if !errors.Is(err, ErrStartEvicted) {
		t.Fatalf("NewReader(0) on advanced ring: err = %v, want ErrStartEvicted", err)
	}

	// At-head start is fine.
	r2, err := ring.NewReader(ring.head, -1)
	if err != nil {
		t.Fatalf("NewReader(head): %v", err)
	}
	r2.Close()
}
