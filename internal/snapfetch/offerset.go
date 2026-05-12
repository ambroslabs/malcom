package snapfetch

import (
	"sort"

	"github.com/ambroslabs/malcom/internal/statesync"
)

// offerSet indexes snapshot offers by content key and by height
// during walkBackward. Replaces the inline `offers` + `offerByHeight`
// maps + `addOffer` closure that previously shared state via captures.
type offerSet struct {
	byKey    map[string]*snapshotOffer
	byHeight map[uint64][]string
}

func newOfferSet() *offerSet {
	return &offerSet{
		byKey:    map[string]*snapshotOffer{},
		byHeight: map[uint64][]string{},
	}
}

// add records peerID as an offerer of s. Creates the offer record on
// first observation; subsequent calls just record additional peers.
// Returns true on first observation of (height, format, hash) — callers
// can use this to fire one-shot logs without having to track the tuple
// themselves.
func (o *offerSet) add(s *statesync.Snapshot, peerID string) bool {
	k := snapKey(s)
	rec, ok := o.byKey[k]
	first := !ok
	if first {
		rec = &snapshotOffer{
			Height:   s.Height,
			Format:   s.Format,
			Chunks:   s.Chunks,
			Hash:     s.Hash,
			Metadata: s.Metadata,
			Peers:    map[string]bool{},
		}
		o.byKey[k] = rec
		o.byHeight[s.Height] = append(o.byHeight[s.Height], k)
	}
	rec.Peers[peerID] = true
	return first
}

// offerEntry pairs a content key with its offer, so callers that need
// to track per-offer state (e.g. walk's `asked` map keyed by snapKey)
// don't have to recompute the key.
type offerEntry struct {
	Key   string
	Offer *snapshotOffer
}

// at returns the offers seen at height in insertion order, paired
// with their content keys.
func (o *offerSet) at(height uint64) []offerEntry {
	keys := o.byHeight[height]
	out := make([]offerEntry, 0, len(keys))
	for _, k := range keys {
		out = append(out, offerEntry{Key: k, Offer: o.byKey[k]})
	}
	return out
}

// count returns the total distinct offers across all heights.
func (o *offerSet) count() int { return len(o.byKey) }

// atOrAbove returns offers whose height falls in [min, max], sorted
// descending by height. Used by walkBackward so chains that take
// snapshots at non-multiple heights (osmosis publishes mid-interval)
// still match: at walk target H, we ask peers about every offer at
// H or above (capped at MaxHeight), not just exact-height matches.
//
// max == 0 disables the upper bound — caller-side check; we accept
// any height ≥ min in that case so callers without a known chain
// head can still use this method.
func (o *offerSet) atOrAbove(min, max uint64) []offerEntry {
	heights := make([]uint64, 0, len(o.byHeight))
	for h := range o.byHeight {
		if h < min {
			continue
		}
		if max != 0 && h > max {
			continue
		}
		heights = append(heights, h)
	}
	sort.Slice(heights, func(i, j int) bool { return heights[i] > heights[j] })
	out := make([]offerEntry, 0)
	for _, h := range heights {
		for _, k := range o.byHeight[h] {
			out = append(out, offerEntry{Key: k, Offer: o.byKey[k]})
		}
	}
	return out
}
