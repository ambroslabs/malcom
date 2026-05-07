package snapfetch

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/cometbft/cometbft/p2p"

	"github.com/zrbecker/cosmos-p2p/internal/logctx"
	"github.com/zrbecker/cosmos-p2p/internal/statesync"
)

// walkBackward replaces the old discover→rank→race-probe pipeline with
// a direct walk: it dials our peer addrs, lets PEX warm the peer set for ~3s,
// then iterates target heights in descending order (interval-aligned)
// from floor(MaxHeight, interval) down to MinHeight. For each target
// it asks every offering peer for chunk-0 and accepts the first valid
// response. On timeout it walks back by SnapshotInterval.
//
// If TargetHeight is set, walks exactly that one height (no fallback).
//
// Returns the chosen offer + a starter good-peers list (the chunk-0
// responder, plus any peer in the offer's Peers map; phase 3
// dispatches to all of them).
func walkBackward(
	ctx context.Context,
	sw *p2p.Switch,
	ssR *statesync.Reactor,
	mux *eventMux,
	peerAddrs []peerAddr,
	cfg Config,
) (*snapshotOffer, []p2p.ID, error) {
	log := logctx.From(ctx)

	// Subscribe to events BEFORE dialing so any SnapshotsResponse
	// arriving during the kickstart-dial wave + warmup is captured
	// (the mux drops events when there are no subscribers).
	evs := mux.subscribe()

	// Kickstart: fire-and-forget dials to a capped subset of our peer
	// addrs. We don't wait for results — each unreachable peer can
	// take 30s+ to time out, and with PEX-accumulated addrbooks of
	// thousands of entries a synchronous wait would stall fetch for
	// tens of minutes. PEX's auto-dial loop (2s tick over the
	// addrbook) handles the rest.
	const kickstartCap = 64
	{
		addrs := make([]string, 0, kickstartCap)
		for i, s := range peerAddrs {
			if i >= kickstartCap {
				break
			}
			addrs = append(addrs, s.addr)
		}
		if err := sw.DialPeersAsync(addrs); err != nil {
			log.Error("kickstart dial", "err", err)
		}
	}

	// Target list.
	var targets []uint64
	switch {
	case cfg.TargetHeight != 0:
		targets = []uint64{cfg.TargetHeight}
	case cfg.MaxHeight == 0:
		return nil, nil, fmt.Errorf("walking requires MaxHeight > 0 or an explicit TargetHeight")
	default:
		targets = walkTargets(cfg.MaxHeight, cfg.MinHeight, cfg.SnapshotInterval)
		if len(targets) == 0 {
			return nil, nil, fmt.Errorf("no target heights in [%d, %d] with stride %d",
				cfg.MinHeight, cfg.MaxHeight, cfg.SnapshotInterval)
		}
	}

	// Single subscription drives both offer collection and chunk-0
	// reception. New SnapshotsResponse events update the offers map;
	// new ChunkResponse events at the current target trigger acceptance.
	// (`evs` was subscribed earlier, before the dial wave.)
	offers := map[string]*snapshotOffer{}
	offerByHeight := map[uint64][]string{}

	// Churn lives in peerWatch (started by RunFetch). We just collect
	// offers here; peerWatch sees them via its own subscription.

	addOffer := func(s *statesync.Snapshot, peerID string) {
		k := snapKey(s)
		rec, ok := offers[k]
		if !ok {
			rec = &snapshotOffer{
				Height:   s.Height,
				Format:   s.Format,
				Chunks:   s.Chunks,
				Hash:     s.Hash,
				Metadata: s.Metadata,
				Peers:    map[string]bool{},
			}
			offers[k] = rec
			offerByHeight[s.Height] = append(offerByHeight[s.Height], k)
		}
		rec.Peers[peerID] = true
	}

	// Drain any events that arrived during the seed-dial wave into
	// the offers map BEFORE we start the 3s warmup. Otherwise the
	// warmup `time.After` blocks the receive loop and offers
	// accumulate in the channel buffer.
	drainEvents := func() {
		for {
			select {
			case ev := <-evs:
				if ev.Snapshot != nil {
					addOffer(ev.Snapshot, ev.PeerID)
				}
			default:
				return
			}
		}
	}
	drainEvents()

	// 3s warmup so PEX-harvested peers can connect and send their
	// SnapshotsResponse. Drain again afterward.
	warmupDeadline := time.NewTimer(3 * time.Second)
	for {
		drainEvents()
		select {
		case <-ctx.Done():
			warmupDeadline.Stop()
			return nil, nil, ctx.Err()
		case <-warmupDeadline.C:
			drainEvents()
			goto walkLoop
		case ev := <-evs:
			if ev.Snapshot != nil {
				addOffer(ev.Snapshot, ev.PeerID)
			}
		}
	}
walkLoop:
	// Dynamic target queue. Walks the precomputed `targets` (newest
	// first) but allows jumping back UP if a fresher offer arrives
	// mid-iteration. `failed` records heights whose deadline has
	// expired (don't revisit). `asked` tracks (peer, snapKey)
	// pairs across iterations so jumping doesn't re-spam peers we
	// already asked.
	queue := append([]uint64(nil), targets...)
	failed := map[uint64]bool{}
	asked := map[string]bool{}
	askKey := func(peerID, k string) string { return peerID + ":" + k }

	dispatch := func(target uint64) int {
		n := 0
		for _, k := range offerByHeight[target] {
			offer := offers[k]
			for pid := range offer.Peers {
				ak := askKey(pid, k)
				if asked[ak] {
					continue
				}
				peer := sw.Peers().Get(p2p.ID(pid))
				if peer == nil {
					continue
				}
				if ssR.RequestChunk(peer, offer.Height, offer.Format, 0) {
					asked[ak] = true
					n++
				}
			}
		}
		return n
	}

	for len(queue) > 0 {
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}
		target := queue[0]
		queue = queue[1:]
		if failed[target] {
			continue
		}

		initialAsks := dispatch(target)
		out, _, dialing := sw.NumPeers()
		log.Info("searching for snapshot",
			"height", target,
			"asking_peers", initialAsks,
			"connected", out,
			"dialing", dialing,
			"book_size", offerCount(offers))

		deadline := time.NewTimer(cfg.PerHeightTimeout)
		var accepted *snapshotOffer
		var responder p2p.ID
		jumped := false
	heightLoop:
		for {
			select {
			case <-ctx.Done():
				deadline.Stop()
				return nil, nil, ctx.Err()
			case <-deadline.C:
				break heightLoop
			case ev, ok := <-evs:
				if !ok {
					deadline.Stop()
					return nil, nil, fmt.Errorf("event channel closed")
				}
				if ev.Snapshot != nil {
					addOffer(ev.Snapshot, ev.PeerID)
					// Jump-up: a new offer arrived for a height
					// fresher than our current target. Abort this
					// iteration; the queue gets the new height
					// prioritized (and the current target requeued
					// behind it, since we never gave it the full
					// 10s window).
					if cfg.TargetHeight == 0 &&
						ev.Snapshot.Height > target &&
						(cfg.MinHeight == 0 || ev.Snapshot.Height >= cfg.MinHeight) &&
						!failed[ev.Snapshot.Height] {
						log.Info("found higher snapshot from new peer; jumping",
							"from_height", target,
							"to_height", ev.Snapshot.Height,
							"peer", ev.PeerID)
						deadline.Stop()
						queue = append([]uint64{ev.Snapshot.Height, target}, queue...)
						jumped = true
						break heightLoop
					}
					if ev.Snapshot.Height == target {
						// New offer at our target — dispatch chunk-0 to this peer.
						dispatch(target)
					}
					continue
				}
				if ev.Chunk == nil || ev.Chunk.Index != 0 {
					continue
				}
				if ev.Chunk.Height != target {
					continue
				}
				if ev.Chunk.Missing || len(ev.Chunk.Bytes) == 0 {
					continue
				}
				// Find the offer whose (height, format) matches.
				for _, k := range offerByHeight[target] {
					o := offers[k]
					if o.Format == ev.Chunk.Format {
						accepted = o
						responder = p2p.ID(ev.PeerID)
						deadline.Stop()
						break heightLoop
					}
				}
			}
		}

		if accepted != nil {
			log.Info("downloading snapshot",
				"height", accepted.Height, "format", accepted.Format,
				"chunks", accepted.Chunks,
				"hash", hex.EncodeToString(accepted.Hash)[:16],
				"served_by", string(responder))
			good := []p2p.ID{responder}
			seen := map[p2p.ID]bool{responder: true}
			for pid := range accepted.Peers {
				id := p2p.ID(pid)
				if !seen[id] {
					good = append(good, id)
					seen[id] = true
				}
			}
			return accepted, good, nil
		}

		if jumped {
			continue
		}

		if cfg.TargetHeight != 0 {
			return nil, nil, fmt.Errorf("target height %d: no peer served chunk-0 within %s",
				cfg.TargetHeight, cfg.PerHeightTimeout)
		}
		failed[target] = true
		next := uint64(0)
		if len(queue) > 0 {
			next = queue[0]
		}
		log.Info("no served offer; walking back",
			"height", target, "next", next)
	}

	return nil, nil, fmt.Errorf("no servable snapshot found in window [%d, %d]",
		cfg.MinHeight, cfg.MaxHeight)
}

// offerCount returns the number of distinct snapshot offers we've
// collected so far (sum across heights). Used for diagnostic logs.
func offerCount(offers map[string]*snapshotOffer) int { return len(offers) }

// walkTargets returns a descending list of heights from
// floor(top, interval) down to >= minHeight, stepping by interval.
func walkTargets(top, minHeight, interval uint64) []uint64 {
	if interval == 0 || top < minHeight {
		return nil
	}
	start := (top / interval) * interval
	var out []uint64
	for h := start; h >= minHeight && h > 0; h -= interval {
		out = append(out, h)
		if h < interval {
			break
		}
	}
	return out
}
