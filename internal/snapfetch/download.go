package snapfetch

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/cometbft/cometbft/p2p"

	"github.com/zrbecker/cosmos-p2p/internal/connect"
	"github.com/zrbecker/cosmos-p2p/internal/logctx"
	"github.com/zrbecker/cosmos-p2p/internal/statesync"
)

type peerStat struct {
	inflight    int
	failures    int  // missing=true or hash-mismatch responses (real misbehaviour)
	banned      bool // permanently benched (PeerFailLimit hit, or peerWatch eviction)
	provisional bool // true until peer responds with first verified chunk; provisional peers get one in-flight slot and a single-strike ban budget
}

// download is the phase-3 chunk scheduler. It dispatches chunks across
// good peers, verifies SHA256 against the metadata hashes, and writes
// each verified chunk to <snapDir>/chunk_<idx>.bin. Returns total bytes
// transferred (sum of verified chunk lengths).
//
// Resilience features:
//   - Pinned peers. Every peer entering stats is Pinned with the
//     connect.Manager so the manager keeps redialing on disconnect
//     (with exponential backoff) without download owning the dial code.
//   - Provisional peer promotion. Connected non-good peers are added to
//     stats with provisional=true and given one in-flight slot. The first
//     verified chunk promotes them to a full-budget good peer; a hash
//     mismatch single-strikes them out.
//   - Misbehavior bans go through peerWatch.banPeer (disconnect +
//     addrbook MarkBad) AND mgr.Ban (manager stops redialing).
func download(ctx context.Context, sw *p2p.Switch, ssR *statesync.Reactor,
	evs <-chan statesync.Event, target *snapshotOffer, chunkHashes [][]byte,
	good []p2p.ID, snapDir string,
	perPeer int, chunkTimeout time.Duration, peerFailLimit int,
	provisionalStrikes, provisionalInflight int,
	watch *peerWatch, mgr *connect.Manager) (uint64, error) {

	log := logctx.From(ctx)

	// banAndDrop disconnects + addrbook-bans a misbehaving peer so
	// PEX can dial a replacement, and tells the manager to stop
	// redialing. peerWatch.banPeer handles the first two; mgr.Ban
	// the third.
	banAndDrop := func(pid p2p.ID, reason string) {
		if mgr != nil {
			mgr.Ban(pid, reason)
		}
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

	// addProvisional registers a freshly-connected peer in stats as
	// provisional and Pins it with the manager. Idempotent.
	addProvisional := func(peer p2p.Peer) {
		pid := peer.ID()
		if _, ok := stats[pid]; ok {
			return
		}
		stats[pid] = &peerStat{provisional: true}
		if mgr != nil {
			mgr.Pin(pid, peer.SocketAddr().String())
		}
		log.Debug("provisional peer added", "peer", string(pid))
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

	// Pin good peers up front so the manager redials them if any
	// drop during the run.
	if mgr != nil {
		for _, pid := range good {
			if peer := sw.Peers().Get(pid); peer != nil {
				mgr.Pin(pid, peer.SocketAddr().String())
			}
		}
	}

	// Initial scan: Connected events only flow forward, but peers may
	// already be connected before we subscribed to the mux (the manager
	// has been dialing since runfetch setup, walk consumed events its
	// own subscription). Walk sw.Peers().List() once to seed stats for
	// non-good already-connected peers.
	for _, p := range sw.Peers().List() {
		addProvisional(p)
	}

	log.Info("download starting",
		"chunks", N, "good_peers", len(good),
		"per_peer_inflight", perPeer, "tracked", len(stats))

	dispatch()

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
			// Mirror peerWatch / manager bans into stats so pickPeer
			// stops considering them. (Manager handles all redialing.)
			for pid, st := range stats {
				if st.banned {
					continue
				}
				if mgr != nil && mgr.IsBanned(pid) {
					st.banned = true
				}
			}
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
			if ev.Connected {
				peer := p2p.ID(ev.PeerID)
				if p := sw.Peers().Get(peer); p != nil {
					addProvisional(p)
				}
				dispatch()
				continue
			}
			if ev.Removed {
				peer := p2p.ID(ev.PeerID)
				// Drop in-flight assignments for this peer so they
				// get retried on the next dispatch. Manager handles
				// redialing; we just clean up our state.
				for idx, info := range inflight {
					if info.peer == peer {
						delete(inflight, idx)
						pending[idx] = true
					}
				}
				if st, ok := stats[peer]; ok {
					st.inflight = 0
				}
				dispatch()
				continue
			}
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
			if !bytes.Equal(h[:], chunkHashes[idx]) {
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

			// Verified chunk — promote a provisional peer to proven.
			if st.provisional {
				st.provisional = false
				log.Info("peer promoted from provisional", "peer", string(peer))
			}

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
