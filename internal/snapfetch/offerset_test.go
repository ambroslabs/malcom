package snapfetch

import (
	"testing"

	"github.com/zrbecker/cosmos-p2p/internal/statesync"
)

func mkSnap(h uint64, format uint32, hash string) *statesync.Snapshot {
	return &statesync.Snapshot{
		Height: h,
		Format: format,
		Chunks: 1,
		Hash:   []byte(hash),
	}
}

func TestOfferSetAddNewRecord(t *testing.T) {
	o := newOfferSet()
	s := mkSnap(100, 1, "abc")
	s.Metadata = []byte("meta")
	o.add(s, "peer-A")
	if got := o.count(); got != 1 {
		t.Fatalf("count=%d, want 1", got)
	}
	entries := o.at(100)
	if len(entries) != 1 {
		t.Fatalf("at(100) len=%d, want 1", len(entries))
	}
	if entries[0].Offer.Height != 100 {
		t.Fatalf("offer.Height=%d, want 100", entries[0].Offer.Height)
	}
	if entries[0].Offer.Chunks != 1 {
		t.Fatalf("offer.Chunks=%d, want 1", entries[0].Offer.Chunks)
	}
	if string(entries[0].Offer.Metadata) != "meta" {
		t.Fatalf("offer.Metadata=%q, want %q", entries[0].Offer.Metadata, "meta")
	}
	if want := snapKey(s); entries[0].Key != want {
		t.Fatalf("entry.Key=%q, want %q", entries[0].Key, want)
	}
	if !entries[0].Offer.Peers["peer-A"] {
		t.Fatalf("peer-A not in Peers map")
	}
}

func TestOfferSetAddSameContentDifferentPeer(t *testing.T) {
	o := newOfferSet()
	s := mkSnap(100, 1, "abc")
	o.add(s, "peer-A")
	o.add(s, "peer-B")
	o.add(s, "peer-A") // re-adding same peer must be idempotent
	if got := o.count(); got != 1 {
		t.Fatalf("count=%d, want 1 (same content should not duplicate)", got)
	}
	entries := o.at(100)
	if len(entries) != 1 {
		t.Fatalf("at(100) len=%d, want 1", len(entries))
	}
	peers := entries[0].Offer.Peers
	if !peers["peer-A"] || !peers["peer-B"] {
		t.Fatalf("Peers=%v, want both peer-A and peer-B", peers)
	}
	if len(peers) != 2 {
		t.Fatalf("len(Peers)=%d, want 2 (re-adding same peer must not duplicate)", len(peers))
	}
}

func TestOfferSetAtHeightIsolation(t *testing.T) {
	o := newOfferSet()
	o.add(mkSnap(100, 1, "abc"), "peer-A")
	o.add(mkSnap(200, 1, "def"), "peer-B")
	if got := len(o.at(100)); got != 1 {
		t.Fatalf("at(100) len=%d, want 1", got)
	}
	if got := len(o.at(200)); got != 1 {
		t.Fatalf("at(200) len=%d, want 1", got)
	}
	if got := len(o.at(150)); got != 0 {
		t.Fatalf("at(150) len=%d, want 0 (no offer at this height)", got)
	}
	if got := o.count(); got != 2 {
		t.Fatalf("count=%d, want 2", got)
	}
}

func TestOfferSetAtPreservesInsertionOrder(t *testing.T) {
	o := newOfferSet()
	// Three distinct offers (different formats) at the same height.
	o.add(mkSnap(100, 1, "first"), "peer-A")
	o.add(mkSnap(100, 2, "second"), "peer-B")
	o.add(mkSnap(100, 3, "third"), "peer-C")
	entries := o.at(100)
	if len(entries) != 3 {
		t.Fatalf("at(100) len=%d, want 3", len(entries))
	}
	for i, want := range []uint32{1, 2, 3} {
		if entries[i].Offer.Format != want {
			t.Fatalf("entries[%d].Format=%d, want %d", i, entries[i].Offer.Format, want)
		}
	}
}

func TestOfferSetAtUnknownHeightEmpty(t *testing.T) {
	o := newOfferSet()
	if got := o.at(999); got == nil {
		t.Fatalf("at(unknown) returned nil; want empty slice")
	} else if len(got) != 0 {
		t.Fatalf("at(unknown) len=%d, want 0", len(got))
	}
}

func TestOfferSetSameHeightFormatDifferentHash(t *testing.T) {
	o := newOfferSet()
	o.add(mkSnap(100, 1, "abc"), "peer-A")
	o.add(mkSnap(100, 1, "xyz"), "peer-B")
	if got := o.count(); got != 2 {
		t.Fatalf("count=%d, want 2 (different hash → different offers)", got)
	}
	entries := o.at(100)
	if len(entries) != 2 {
		t.Fatalf("at(100) len=%d, want 2", len(entries))
	}
	if entries[0].Key == entries[1].Key {
		t.Fatalf("entries share Key %q; want distinct", entries[0].Key)
	}
}

func TestOfferSetReAddPreservesHeightOrder(t *testing.T) {
	o := newOfferSet()
	o.add(mkSnap(100, 1, "first"), "peer-A")
	o.add(mkSnap(100, 2, "second"), "peer-B")
	o.add(mkSnap(100, 3, "third"), "peer-C")
	// Re-add the first offer with a new peer; height ordering must not change.
	o.add(mkSnap(100, 1, "first"), "peer-D")
	entries := o.at(100)
	if len(entries) != 3 {
		t.Fatalf("at(100) len=%d, want 3", len(entries))
	}
	for i, want := range []uint32{1, 2, 3} {
		if entries[i].Offer.Format != want {
			t.Fatalf("entries[%d].Format=%d, want %d (re-add must not reorder)", i, entries[i].Offer.Format, want)
		}
	}
	if !entries[0].Offer.Peers["peer-D"] {
		t.Fatalf("peer-D not added to first offer on re-add")
	}
}

func TestOfferSetCountDistinctAcrossHeights(t *testing.T) {
	o := newOfferSet()
	o.add(mkSnap(100, 1, "a"), "p1")
	o.add(mkSnap(100, 1, "a"), "p2") // same content, different peer
	o.add(mkSnap(200, 1, "b"), "p3")
	if got := o.count(); got != 2 {
		t.Fatalf("count=%d, want 2 (distinct offers, not events)", got)
	}
}
