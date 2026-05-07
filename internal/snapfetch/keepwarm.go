package snapfetch

import (
	"context"
	"math/rand"
	"time"

	cmtlog "github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/p2p"
)

// runKeepWarm dials seeds in the background while the connected peer
// count is below warmTarget. Each refresh tick, it dials up to
// dialBatch new seeds (seeds we haven't already connected to). The
// dialed peers are picked up by download's scanForNewPeers tick and
// added to stats as provisional.
//
// Seeds are shuffled once at start so we don't bias toward the front
// of the list, and a cursor advances through the shuffled slice with
// wraparound — over a long bench, every seed eventually gets a try.
func runKeepWarm(ctx context.Context, sw *p2p.Switch, seeds []peerSeed,
	warmTarget int, refresh time.Duration, logger cmtlog.Logger) {

	if len(seeds) == 0 {
		return
	}
	const dialBatch = 4

	shuffled := make([]peerSeed, len(seeds))
	copy(shuffled, seeds)
	rand.Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})
	cursor := 0

	t := time.NewTicker(refresh)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		connected := sw.Peers().Size()
		if connected >= warmTarget {
			continue
		}
		dialed := 0
		// Walk the seed list for at most one full pass per tick — past
		// the cap, give up for now and retry next tick.
		for tries := 0; tries < len(shuffled) && dialed < dialBatch; tries++ {
			s := shuffled[cursor]
			cursor = (cursor + 1) % len(shuffled)
			na, err := p2p.NewNetAddressString(s.addr)
			if err != nil {
				continue
			}
			if peer := sw.Peers().Get(na.ID); peer != nil {
				continue
			}
			dialed++
			go func(na *p2p.NetAddress) {
				_ = sw.DialPeerWithAddress(na)
			}(na)
		}
		if dialed > 0 {
			logger.Debug("keep-warm refresh",
				"connected", connected, "target", warmTarget, "dialed", dialed)
		}
	}
}
