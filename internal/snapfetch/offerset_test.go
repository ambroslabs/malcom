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

func TestOfferSetAtOrAboveDescending(t *testing.T) {
	o := newOfferSet()
	o.add(mkSnap(100, 1, "a"), "p1")
	o.add(mkSnap(250, 1, "b"), "p2")
	o.add(mkSnap(175, 1, "c"), "p3")
	o.add(mkSnap(300, 1, "d"), "p4")

	// Range [150, 300]: drops 100, keeps 175/250/300, descending.
	got := o.atOrAbove(150, 300)
	if len(got) != 3 {
		t.Fatalf("len=%d, want 3", len(got))
	}
	wantHeights := []uint64{300, 250, 175}
	for i, h := range wantHeights {
		if got[i].Offer.Height != h {
			t.Fatalf("got[%d].Height=%d, want %d (must be descending)", i, got[i].Offer.Height, h)
		}
	}
}

func TestOfferSetAtOrAboveExcludesAboveMax(t *testing.T) {
	o := newOfferSet()
	o.add(mkSnap(100, 1, "a"), "p1")
	o.add(mkSnap(500, 1, "b"), "p2") // above max
	got := o.atOrAbove(50, 200)
	if len(got) != 1 {
		t.Fatalf("len=%d, want 1 (500 must be excluded)", len(got))
	}
	if got[0].Offer.Height != 100 {
		t.Fatalf("got[0].Height=%d, want 100", got[0].Offer.Height)
	}
}

func TestOfferSetAtOrAboveMaxZeroIgnoresUpperBound(t *testing.T) {
	o := newOfferSet()
	o.add(mkSnap(100, 1, "a"), "p1")
	o.add(mkSnap(50, 1, "b"), "p2")  // below min, dropped
	o.add(mkSnap(999, 1, "c"), "p3") // far above any plausible window
	got := o.atOrAbove(80, 0)
	if len(got) != 2 {
		t.Fatalf("len=%d, want 2 (max=0 means no upper bound)", len(got))
	}
	if got[0].Offer.Height != 999 || got[1].Offer.Height != 100 {
		t.Fatalf("descending order broken: %d, %d", got[0].Offer.Height, got[1].Offer.Height)
	}
}

func TestOfferSetAtOrAboveSameHeightMultipleOffers(t *testing.T) {
	o := newOfferSet()
	o.add(mkSnap(100, 1, "a"), "p1")
	o.add(mkSnap(100, 2, "b"), "p2") // same height, different format
	o.add(mkSnap(200, 1, "c"), "p3")
	got := o.atOrAbove(100, 200)
	if len(got) != 3 {
		t.Fatalf("len=%d, want 3 (both 100 entries plus 200)", len(got))
	}
	if got[0].Offer.Height != 200 {
		t.Fatalf("got[0].Height=%d, want 200 (descending)", got[0].Offer.Height)
	}
	if got[1].Offer.Height != 100 || got[2].Offer.Height != 100 {
		t.Fatalf("got[1,2] heights = %d, %d, want both 100", got[1].Offer.Height, got[2].Offer.Height)
	}
}

func TestOfferSetAtOrAboveEmpty(t *testing.T) {
	o := newOfferSet()
	if got := o.atOrAbove(100, 200); len(got) != 0 {
		t.Fatalf("empty set should return no entries, got %d", len(got))
	}
}
