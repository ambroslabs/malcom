package snapfetch

import "github.com/zrbecker/cosmos-p2p/internal/statesync"

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
func (o *offerSet) add(s *statesync.Snapshot, peerID string) {
	k := snapKey(s)
	rec, ok := o.byKey[k]
	if !ok {
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
