package snapfetch

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/cometbft/cometbft/p2p"

	"github.com/zrbecker/cosmos-p2p/internal/helpers/addrbook"
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
// Chunk-0 is verified against metadata.chunk_hashes[0] before the offer
// is accepted; a peer serving a chunk that matches no offer's hashes
// gets banned via watch and the height keeps waiting for another
// peer's response.
//
// Returns the chosen offer + a starter good-peers list (the chunk-0
// responder, plus any peer in the offer's Peers map; phase 3
// dispatches to all of them) + the verified chunk-0 bytes (so the
// download phase can skip refetching it).
func walkBackward(
	ctx context.Context,
	sw *p2p.Switch,
	ssR *statesync.Reactor,
	mux *eventMux,
	watch *peerWatch,
	peerAddrs []addrbook.PeerAddr,
	cfg Config,
) (*snapshotOffer, []p2p.ID, []byte, error) {
	log := logctx.From(ctx)

	// Subscribe to events BEFORE dialing so any SnapshotsResponse
	// arriving during the kickstart wave + warmup is captured (the mux
	// drops events when there are no subscribers).
	sub := mux.subscribe()

	// Kickstart: fire-and-forget dials to a capped subset of our peer
	// addrs. connect.Manager will continue dialing on its tick, but
	// without this burst the first 2-5 seconds of the walk go by with
	// nothing in flight.
	const kickstartCap = 64
	{
		addrs := make([]string, 0, kickstartCap)
		for i, s := range peerAddrs {
			if i >= kickstartCap {
				break
			}
			addrs = append(addrs, s.Addr)
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
		return nil, nil, nil, fmt.Errorf("%w: walk has no upper bound — %s", ErrWalkFailed, hintMissingHeightInputs)
	default:
		targets = walkTargets(cfg.MaxHeight, cfg.MinHeight, cfg.SnapshotInterval)
		if len(targets) == 0 {
			return nil, nil, nil, fmt.Errorf("%w: no target heights in [%d, %d] with stride %d — %s",
				ErrWalkFailed, cfg.MinHeight, cfg.MaxHeight, cfg.SnapshotInterval,
				hintEmptyTargetWindow(cfg.ChainID))
		}
	}

	// Single subscription drives both offer collection and chunk-0
	// reception. New SnapshotsResponse events update the offerSet;
	// new ChunkResponse events at the current target trigger acceptance.
	// (`evs` was subscribed earlier, before the dial wave.)
	//
	// Churn lives in peerWatch (started by RunFetch). We just collect
	// offers here; peerWatch sees them via its own subscription.
	offers := newOfferSet()

	// recordOffer adds an offer to the set and, on first observation
	// of (height, format, hash), surfaces a one-shot Info log so
	// operators can see what's out there in the wild — including
	// candidates that are too old (in_window=false) and would normally
	// be invisible. age is the distance below MaxHeight (the chain
	// head as known to this run); negative when an offer claims a
	// height above what we believed the head to be.
	recordOffer := func(s *statesync.Snapshot, peerID string) {
		if !offers.add(s, peerID) {
			return
		}
		var age int64
		if cfg.MaxHeight > 0 {
			age = int64(cfg.MaxHeight) - int64(s.Height)
		}
		log.Info("snapshot offer",
			"height", s.Height,
			"peer", peerID,
			"in_window", offerInWindow(s.Height, cfg),
			"age", age)
	}

	// Drain any events that arrived during the seed-dial wave into
	// the offerSet BEFORE we start the 3s warmup. Otherwise the
	// warmup `time.After` blocks the receive loop and offers
	// accumulate in the channel buffer.
	drainEvents := func() {
		for {
			select {
			case ev := <-sub.Ctrl:
				if ev.Snapshot != nil {
					recordOffer(ev.Snapshot, ev.PeerID)
				}
			case <-sub.Chunk:
				// Discard pre-walk chunks; we only request chunk-0
				// once a target is in flight.
			default:
				return
			}
		}
	}
	drainEvents()

	// 3s warmup so PEX-harvested peers can connect and send their
	// SnapshotsResponse. Drain again afterward.
	if err := warmup(ctx, sub, recordOffer, drainEvents, 3*time.Second); err != nil {
		return nil, nil, nil, err
	}

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

	// Bound jump-ups per walk so two peers oscillating between heights
	// can't keep us walking forever. 2*len(targets) leaves generous
	// headroom for legitimate late-arriving offers while guaranteeing
	// termination under adversarial peers.
	maxJumps := 2 * len(targets)
	jumps := 0
	jumpCapLogged := false

	dispatch := func(target uint64) int {
		n := 0
		for _, e := range offers.at(target) {
			for pid := range e.Offer.Peers {
				ak := askKey(pid, e.Key)
				if asked[ak] {
					continue
				}
				peer := sw.Peers().Get(p2p.ID(pid))
				if peer == nil {
					continue
				}
				if ssR.RequestChunk(peer, e.Offer.Height, e.Offer.Format, 0) {
					asked[ak] = true
					n++
				}
			}
		}
		return n
	}

	for len(queue) > 0 {
		if ctx.Err() != nil {
			return nil, nil, nil, ctx.Err()
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
			"book_size", offers.count())

		deadline := time.NewTimer(cfg.PerHeightTimeout)
		var accepted *snapshotOffer
		var acceptedChunk0 []byte
		var responder p2p.ID
		jumped := false
	heightLoop:
		for {
			select {
			case <-ctx.Done():
				deadline.Stop()
				return nil, nil, nil, ctx.Err()
			case <-deadline.C:
				break heightLoop
			case ev, ok := <-sub.Ctrl:
				if !ok {
					deadline.Stop()
					return nil, nil, nil, fmt.Errorf("event channel closed")
				}
				if ev.Snapshot == nil {
					continue
				}
				log.Debug("walk recv offer",
					"target", target,
					"offer_height", ev.Snapshot.Height,
					"peer", ev.PeerID,
					"failed_target", failed[ev.Snapshot.Height])
				recordOffer(ev.Snapshot, ev.PeerID)
				// Jump-up: a fresher offer arrived for a height
				// above the current target. Abort this iteration
				// and prepend the fresher height to the queue (the
				// current target is requeued behind it, since we
				// never gave it the full per-height window).
				if cfg.TargetHeight == 0 &&
					isJumpCandidate(ev.Snapshot.Height, target, cfg.MinHeight, failed) {
					if jumps < maxJumps {
						log.Info("found higher snapshot from new peer; jumping",
							"from_height", target,
							"to_height", ev.Snapshot.Height,
							"peer", ev.PeerID,
							"jumps", jumps+1,
							"max_jumps", maxJumps)
						deadline.Stop()
						queue = append([]uint64{ev.Snapshot.Height, target}, queue...)
						jumps++
						jumped = true
						break heightLoop
					}
					if !jumpCapLogged {
						log.Info("walk jump cap reached; ignoring further higher-height offers this walk",
							"target", target,
							"offer_height", ev.Snapshot.Height,
							"max_jumps", maxJumps)
						jumpCapLogged = true
					}
				}
				if ev.Snapshot.Height == target {
					n := dispatch(target)
					log.Debug("walk dispatch on offer match",
						"target", target, "asked", n, "peer", ev.PeerID)
				}
			case ev, ok := <-sub.Chunk:
				if !ok {
					deadline.Stop()
					return nil, nil, nil, fmt.Errorf("event channel closed")
				}
				if ev.Chunk == nil {
					continue
				}
				log.Debug("walk recv chunk",
					"target", target,
					"chunk_height", ev.Chunk.Height,
					"chunk_format", ev.Chunk.Format,
					"chunk_index", ev.Chunk.Index,
					"missing", ev.Chunk.Missing,
					"bytes", len(ev.Chunk.Bytes),
					"peer", ev.PeerID)
				if ev.Chunk.Index != 0 || ev.Chunk.Height != target {
					continue
				}
				if ev.Chunk.Missing || len(ev.Chunk.Bytes) == 0 {
					continue
				}
				// Match chunk-0 bytes against any format-matching offer's
				// metadata.chunk_hashes[0]. ChunkRequest carries no hash,
				// so the responder might be serving any of several offers
				// with the same (height, format). The first whose hash[0]
				// matches is the offer we accept.
				var matched *snapshotOffer
				var lastErr error
				for _, e := range offers.at(target) {
					if e.Offer.Format != ev.Chunk.Format {
						continue
					}
					if err := verifyChunkZero(e.Offer, ev.Chunk.Bytes); err != nil {
						lastErr = err
						continue
					}
					matched = e.Offer
					break
				}
				if matched == nil {
					// No format-matching offer's hashes matched the
					// bytes. Either pure garbage, or a forged chunk
					// that doesn't even line up with the peer's own
					// advertised metadata. Ban and keep waiting on
					// this height for another peer's response.
					log.Error("chunk-0 verify failed; banning peer",
						"peer", ev.PeerID, "height", target, "err", lastErr)
					if watch != nil {
						if peer := sw.Peers().Get(p2p.ID(ev.PeerID)); peer != nil {
							watch.banPeer(peer, "chunk-0 hash mismatch")
						}
					}
					continue
				}
				accepted = matched
				acceptedChunk0 = ev.Chunk.Bytes
				responder = p2p.ID(ev.PeerID)
				log.Debug("walk accepting", "height", target, "format", matched.Format, "peer", ev.PeerID)
				deadline.Stop()
				break heightLoop
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
			return accepted, good, acceptedChunk0, nil
		}

		if jumped {
			continue
		}

		if cfg.TargetHeight != 0 {
			return nil, nil, nil, fmt.Errorf("%w: target height %d: no peer served chunk-0 within %s — %s",
				ErrWalkFailed, cfg.TargetHeight, cfg.PerHeightTimeout,
				hintTargetHeightUnserved(cfg.ChainID))
		}
		failed[target] = true
		next := uint64(0)
		if len(queue) > 0 {
			next = queue[0]
		}
		log.Info("no served offer; walking back",
			"height", target, "next", next)
	}

	return nil, nil, nil, fmt.Errorf("%w: window [%d, %d] — %s",
		ErrWalkFailed, cfg.MinHeight, cfg.MaxHeight, hintNoServable(cfg.ChainID))
}

// isJumpCandidate reports whether a fresher offer at newHeight is
// a candidate for preempting the current target — i.e., it satisfies
// the height/floor/failed-set rules. The caller still gates on the
// per-walk jump budget and on fixed-target mode (cfg.TargetHeight != 0).
func isJumpCandidate(newHeight, current, minHeight uint64, failed map[uint64]bool) bool {
	if newHeight <= current {
		return false
	}
	if minHeight != 0 && newHeight < minHeight {
		return false
	}
	if failed[newHeight] {
		return false
	}
	return true
}

// warmup waits up to d for the warmup window to elapse, draining any
// SnapshotsResponse events through record as they arrive. Returns
// ctx.Err() if the parent context is cancelled mid-warmup.
func warmup(ctx context.Context, sub *subscription, record func(*statesync.Snapshot, string), drain func(), d time.Duration) error {
	deadline := time.NewTimer(d)
	defer deadline.Stop()
	for {
		drain()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			drain()
			return nil
		case ev := <-sub.Ctrl:
			if ev.Snapshot != nil {
				record(ev.Snapshot, ev.PeerID)
			}
		case <-sub.Chunk:
		}
	}
}

// offerInWindow reports whether height falls in the heights this
// fetch is willing to download. With TargetHeight set, only that exact
// height qualifies; otherwise [MinHeight, MaxHeight] (each side
// disabled when zero).
func offerInWindow(height uint64, cfg Config) bool {
	if cfg.TargetHeight != 0 {
		return height == cfg.TargetHeight
	}
	if cfg.MinHeight != 0 && height < cfg.MinHeight {
		return false
	}
	if cfg.MaxHeight != 0 && height > cfg.MaxHeight {
		return false
	}
	return true
}

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
