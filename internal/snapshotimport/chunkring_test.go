package snapshotimport

import (
	"errors"
	"io"
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

// TestManualPinHoldsBackEviction is the regression test for #124's
// race: in manual mode the ring's eviction tracks the consumer-
// advanced pin, not the buffered-pull pos. Without this, a bufio
// reader sitting in front of a chunkRingReader inflates pos by its
// full buffer size on the first byte read, letting evictLocked drop
// chunks the consumer hasn't actually consumed.
func TestManualPinHoldsBackEviction(t *testing.T) {
	ring := newChunkRing(4<<20, 16<<20)
	if _, err := ring.Write(make([]byte, 8<<20)); err != nil {
		t.Fatalf("ring.Write: %v", err)
	}

	r, err := ring.NewReader(0, -1)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()

	// Switch BEFORE the first Read so pos can never auto-track.
	r.EnableManualPin()

	// Pull 6 MiB through Read; pos jumps to 6 MiB. In auto mode
	// evictLocked would have advanced head along with pos. In manual
	// mode pin stays at 0 — eviction must not move.
	buf := make([]byte, 6<<20)
	if _, err := io.ReadFull(r, buf); err != nil {
		t.Fatalf("ReadFull: %v", err)
	}
	if r.pos < 6<<20 {
		t.Fatalf("setup: pos = %d, want >= 6 MiB after ReadFull", r.pos)
	}
	if r.pin != 0 {
		t.Fatalf("pin = %d, want 0 (consumer hasn't called AdvancePin yet)", r.pin)
	}
	if ring.head != 0 {
		t.Fatalf("head = %d, want 0 (manual-mode eviction tracks pin which is 0)", ring.head)
	}

	// Commit past the first chunk's end. Eviction can now drop
	// chunk 0 and advance head. Chunks are 4 MiB each; advancing pin
	// to 4 MiB exactly evicts the first chunk and parks head there.
	r.AdvancePin(4 << 20)
	if r.pin != 4<<20 {
		t.Fatalf("pin = %d, want 4 MiB after AdvancePin", r.pin)
	}
	if ring.head < 4<<20 {
		t.Fatalf("head = %d, want >= 4 MiB after pin advance past first chunk's end", ring.head)
	}
}

// TestAdvancePinCappedAtPos pins down the invariant that pin can't
// exceed pos — the consumer can't commit past what's been pulled.
func TestAdvancePinCappedAtPos(t *testing.T) {
	ring := newChunkRing(4<<20, 16<<20)
	if _, err := ring.Write(make([]byte, 8<<20)); err != nil {
		t.Fatalf("ring.Write: %v", err)
	}
	r, err := ring.NewReader(0, -1)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()
	r.EnableManualPin()

	buf := make([]byte, 2<<20)
	if _, err := io.ReadFull(r, buf); err != nil {
		t.Fatalf("ReadFull: %v", err)
	}
	if r.pos != 2<<20 {
		t.Fatalf("pos = %d, want 2 MiB", r.pos)
	}

	// Try to commit past pos; pin should cap.
	r.AdvancePin(10 << 20)
	if r.pin != r.pos {
		t.Fatalf("pin = %d, pos = %d, want pin == pos (capped)", r.pin, r.pos)
	}
}
