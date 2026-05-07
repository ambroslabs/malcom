package snapfetch

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cometbft/cometbft/p2p"

	"github.com/zrbecker/cosmos-p2p/internal/logctx"
	"github.com/zrbecker/cosmos-p2p/internal/statesync"
)

type peerStat struct {
	inflight      int
	failures      int  // missing=true or hash-mismatch responses (real misbehaviour)
	disconnects   int  // socket-level drops (transient) — never used to ban directly
	banned        bool // permanently benched (only set on PeerFailLimit failures)
	provisional   bool // true until peer responds with first verified chunk; provisional peers get one in-flight slot and a single-strike ban budget
	lastDialAt    time.Time
	nextDialAfter time.Time // earliest time a redial may be attempted; computed via exponential backoff over disconnects
}

// download is the phase-3 chunk scheduler. It dispatches chunks across
// good peers, verifies SHA256 against the metadata hashes, and writes
// each verified chunk to <snapDir>/chunk_<idx>.bin. Returns total bytes
// transferred (sum of verified chunk lengths).
//
// Resilience features:
//   - Exponential-backoff redial (no ban-on-disconnect). Only
//     hash-mismatch / missing-chunk strikes ban a peer; transient socket
//     drops just defer the next dial attempt.
//   - Background peer-pool refresher. While the connected peer count is
//     below WarmPeerTarget, dials peer addrs in the background so
//     newly-broken good peers can be replaced.
//   - Provisional peer promotion. Connected non-good peers are added to
//     stats with provisional=true and given one in-flight slot. The first
//     verified chunk promotes them to a full-budget good peer; a hash
//     mismatch single-strikes them out.
func download(ctx context.Context, sw *p2p.Switch, ssR *statesync.Reactor,
	evs chan statesync.Event, target *snapshotOffer, chunkHashes [][]byte,
	good []p2p.ID, snapDir string, peerAddrs []peerAddr,
	perPeer int, chunkTimeout time.Duration, peerFailLimit int,
	maxRedials int, redialBackoff, maxRedialBackoff time.Duration,
	provisionalStrikes, provisionalInflight int,
	warmTarget int, warmRefreshInterval time.Duration,
	watch *peerWatch) (uint64, error) {

	log := logctx.From(ctx)

	// Map peer node-ID → full dial address ("nodeID@host:port"). Used by
	// tryRedial below to dial a peer we previously connected to but
	// dropped — sw.Peers().Get returns nil once disconnected, so we
	// can't recover the address from the Switch.
	addrByNodeID := map[string]string{}
	for _, p := range peerAddrs {
		parts := strings.SplitN(p.addr, "@", 2)
		if len(parts) == 2 {
			addrByNodeID[parts[0]] = p.addr
		}
	}

	// banAndDrop disconnects + addrbook-bans a misbehaving peer so
	// PEX can dial a replacement (banned peers occupying connection
	// slots was previously starving fresh dials at TargetPeers cap).
	banAndDrop := func(pid p2p.ID, reason string) {
		if watch == nil {
			return
		}
		if peer := sw.Peers().Get(pid); peer != nil {
			watch.banPeer(peer, reason)
		}
	}

	N := target.Chunks
	pending := make([]bool, N)
	completed := make([]bool, N)
	inflight := map[uint32]struct {
		peer p2p.ID
		sent time.Time
	}{}
	for i := uint32(0); i < N; i++ {
		pending[i] = true
	}

	stats := map[p2p.ID]*peerStat{}
	for _, p := range good {
		stats[p] = &peerStat{}
	}

	var bytesTotal atomic.Uint64
	doneCount := uint32(0)

	startTime := time.Now()
	lastProgress := time.Now()
	// Aligned to the outer 2s timeoutTicker — multiples of 2s land
	// exactly on a tick (10s gives 10, 20, 30, …). Non-multiples
	// round up: 15s would fire at 16, 32, …
	progressEvery := 10 * time.Second

	// computeRedialDelay returns redialBackoff * 2^(disconnects-1), capped
	// at maxRedialBackoff. With redialBackoff=5s and cap=5m, sequence is
	// 5s, 10s, 20s, 40s, 80s, 160s, 300s, 300s, 300s, ... — keeps trying
	// indefinitely so a peer that comes back online eventually rejoins.
	computeRedialDelay := func(disconnects int) time.Duration {
		if disconnects <= 1 {
			return redialBackoff
		}
		shift := disconnects - 1
		if shift > 10 {
			shift = 10
		}
		d := redialBackoff << uint(shift)
		if d <= 0 || d > maxRedialBackoff {
			return maxRedialBackoff
		}
		return d
	}

	tryRedial := func(pid p2p.ID) {
		st, ok := stats[pid]
		if !ok || st.banned {
			return
		}
		if watch != nil && watch.isBanned(pid) {
			// peerWatch already evicted this peer (channel filter or
			// churn-grace). Mirror the bench locally so we stop trying.
			st.banned = true
			return
		}
		if peer := sw.Peers().Get(pid); peer != nil {
			return
		}
		if !st.nextDialAfter.IsZero() && time.Now().Before(st.nextDialAfter) {
			return
		}
		addr, ok := addrByNodeID[string(pid)]
		if !ok {
			// Unknown address — only happens for peers that joined via
			// PEX rather than the seed list. Drop from stats so a future
			// scanForNewPeers can re-add them if they reconnect.
			delete(stats, pid)
			return
		}
		// Cap consecutive disconnect/redial cycles. Note disconnects can
		// also tick during an in-flight dial (the timeoutTicker fires
		// tryRedial every 2s; the actual dial no-ops via
		// ErrCurrentlyDialingOrExistingAddress) — fine for our purposes,
		// it just makes the cap fire after fewer real dials than the bare
		// surface math suggests, which is on the right side of "give up".
		if maxRedials > 0 && st.disconnects >= maxRedials {
			st.banned = true
			if watch != nil {
				watch.markBannedByID(pid, addr, "max-redials hit")
			}
			log.Debug("benching peer (max redials)", "peer", string(pid), "disconnects", st.disconnects)
			return
		}
		na, err := p2p.NewNetAddressString(addr)
		if err != nil {
			st.banned = true
			return
		}
		st.disconnects++
		st.lastDialAt = time.Now()
		st.nextDialAfter = st.lastDialAt.Add(computeRedialDelay(st.disconnects))
		go func(na *p2p.NetAddress) {
			_ = sw.DialPeerWithAddress(na)
		}(na)
	}

	// pickPeer prefers proven (non-provisional) peers up to perPeer
	// inflight, then falls back to provisionalInflight slots per peer.
	// This way a freshly-warm peer can't take more than its probe
	// budget of concurrent chunks until it's proven it can serve.
	pickPeer := func() p2p.ID {
		var bestProven, bestProvis p2p.ID
		bestProvenInflight := perPeer + 1
		bestProvisInflight := provisionalInflight + 1
		for pid, st := range stats {
			if st.banned {
				continue
			}
			if sw.Peers().Get(pid) == nil {
				continue
			}
			if st.provisional {
				if st.inflight < bestProvisInflight {
					bestProvisInflight = st.inflight
					bestProvis = pid
				}
				continue
			}
			if st.inflight < bestProvenInflight {
				bestProvenInflight = st.inflight
				bestProven = pid
			}
		}
		if bestProvenInflight <= perPeer {
			return bestProven
		}
		if bestProvisInflight <= provisionalInflight {
			return bestProvis
		}
		return ""
	}

	// scanForNewPeers walks sw.Peers() and registers any connected,
	// not-yet-tracked peer in stats as provisional. This is how peers
	// arriving via the keepWarm dialer (or PEX) get drawn into the
	// scheduler without a heavyweight rescan.
	scanForNewPeers := func() {
		for _, p := range sw.Peers().List() {
			pid := p.ID()
			if _, ok := stats[pid]; ok {
				continue
			}
			stats[pid] = &peerStat{provisional: true}
			log.Debug("provisional peer added", "peer", string(pid))
		}
	}

	dispatch := func() int {
		dispatched := 0
		for i := uint32(0); i < N; i++ {
			if !pending[i] {
				continue
			}
			pid := pickPeer()
			if pid == "" {
				return dispatched
			}
			peer := sw.Peers().Get(pid)
			if peer == nil {
				stats[pid].banned = true
				continue
			}
			if !ssR.RequestChunk(peer, target.Height, target.Format, i) {
				continue
			}
			pending[i] = false
			inflight[i] = struct {
				peer p2p.ID
				sent time.Time
			}{peer: pid, sent: time.Now()}
			stats[pid].inflight++
			dispatched++
		}
		return dispatched
	}

	log.Info("download starting",
		"chunks", N, "good_peers", len(good),
		"per_peer_inflight", perPeer, "warm_target", warmTarget)

	dispatch()

	// Background peer-pool refresher. Keeps the connected peer count
	// hovering near warmTarget by dialing peer addrs (shuffled)
	// whenever we drop below the threshold. scanForNewPeers in the
	// main loop picks up the resulting connections as provisional peers.
	if len(peerAddrs) > 0 && warmTarget > 0 {
		go runKeepWarm(ctx, sw, peerAddrs, warmTarget, warmRefreshInterval)
	}

	timeoutTicker := time.NewTicker(2 * time.Second)
	defer timeoutTicker.Stop()

	hadEvent := false

	for doneCount < N {
		alive, connected := 0, 0
		for pid, st := range stats {
			if st.banned {
				continue
			}
			alive++
			if sw.Peers().Get(pid) != nil {
				connected++
			}
		}
		if alive == 0 {
			return bytesTotal.Load(), fmt.Errorf("all peers banned (done %d/%d)", doneCount, N)
		}

		select {
		case <-ctx.Done():
			return bytesTotal.Load(), ctx.Err()

		case <-timeoutTicker.C:
			now := time.Now()
			for idx, info := range inflight {
				if now.Sub(info.sent) > chunkTimeout {
					st := stats[info.peer]
					if st != nil {
						st.inflight--
						if st.inflight < 0 {
							st.inflight = 0
						}
						// Provisional peers that time out on their
						// probe lose their slot immediately — their
						// connection is suspect.
						if st.provisional {
							st.banned = true
							log.Debug("benching provisional peer (probe timeout)",
								"peer", string(info.peer))
							banAndDrop(info.peer, "probe timeout")
						}
					}
					delete(inflight, idx)
					pending[idx] = true
				}
			}
			for pid, st := range stats {
				if st.banned {
					continue
				}
				if sw.Peers().Get(pid) == nil {
					tryRedial(pid)
				}
			}
			scanForNewPeers()
			dispatch()

			if now.Sub(lastProgress) >= progressEvery {
				rate := float64(doneCount) / now.Sub(startTime).Seconds()
				log.Info("download progress",
					"chunks", fmt.Sprintf("%d/%d", doneCount, N),
					"MB", bytesTotal.Load()>>20,
					"chunks_per_s", fmt.Sprintf("%.1f", rate),
					"peers", fmt.Sprintf("%d/%d", connected, alive),
					"inflight", len(inflight))
				lastProgress = now
			}

		case ev := <-evs:
			if ev.Chunk == nil {
				continue
			}
			if ev.Chunk.Height != target.Height || ev.Chunk.Format != target.Format {
				continue
			}
			hadEvent = true
			idx := ev.Chunk.Index
			peer := p2p.ID(ev.PeerID)
			st, ok := stats[peer]
			if !ok {
				st = &peerStat{}
				stats[peer] = st
			}
			if info, ok := inflight[idx]; ok && info.peer == peer {
				delete(inflight, idx)
			}
			if st.inflight > 0 {
				st.inflight--
			}

			if completed[idx] {
				continue
			}

			// Provisional peers get a tighter strike budget on their
			// probe than proven peers (configurable via
			// ProvisionalProbeStrikes / PeerFailLimit).
			banLimit := peerFailLimit
			if st.provisional {
				banLimit = provisionalStrikes
			}

			if ev.Chunk.Missing || len(ev.Chunk.Bytes) == 0 {
				st.failures++
				if st.failures >= banLimit {
					st.banned = true
					log.Debug("benching peer", "peer", string(peer), "failures", st.failures, "provisional", st.provisional)
					banAndDrop(peer, "missing/empty chunk")
				}
				pending[idx] = true
				dispatch()
				continue
			}

			h := sha256.Sum256(ev.Chunk.Bytes)
			if !bytesEq(h[:], chunkHashes[idx]) {
				log.Error("chunk hash mismatch",
					"peer", string(peer), "idx", idx,
					"got_sha", hex.EncodeToString(h[:8]),
					"want_sha", hex.EncodeToString(chunkHashes[idx][:8]))
				st.failures++
				if st.failures >= banLimit {
					st.banned = true
					banAndDrop(peer, "chunk hash mismatch")
				}
				pending[idx] = true
				dispatch()
				continue
			}

			// Verified chunk — promote a provisional peer to proven, and
			// reset the disconnect/redial backoff so a peer that came
			// back from a long outage gets a clean slate.
			if st.provisional {
				st.provisional = false
				log.Info("peer promoted from provisional", "peer", string(peer))
			}
			st.disconnects = 0
			st.nextDialAfter = time.Time{}

			// Write the verified chunk to disk. Errors are logged and
			// ignored so a transient disk hiccup doesn't abort the
			// whole fetch — if the file is missing later, the
			// downstream import step surfaces it.
			chunkPath := filepath.Join(snapDir, fmt.Sprintf("chunk_%05d.bin", idx))
			if err := os.WriteFile(chunkPath, ev.Chunk.Bytes, 0o644); err != nil {
				log.Error("write chunk", "idx", idx, "err", err)
			}
			completed[idx] = true
			doneCount++
			bytesTotal.Add(uint64(len(ev.Chunk.Bytes)))
			dispatch()
		}

		if !hadEvent && time.Since(startTime) > 30*time.Second {
			log.Error("no chunk replies in 30s; check peers", "alive_peers", alive)
			startTime = time.Now()
		}
	}
	log.Info("download finished",
		"chunks", N,
		"bytes", bytesTotal.Load(),
		"elapsed", time.Since(startTime))
	return bytesTotal.Load(), nil
}
