package snapfetch

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"time"

	cmtlog "github.com/cometbft/cometbft/libs/log"
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

func (st *peerStat) addInflight() { st.inflight++ }

func (st *peerStat) removeInflight() {
	if st.inflight > 0 {
		st.inflight--
	}
}

func (st *peerStat) clearInflight() { st.inflight = 0 }

// failureLimit returns the strike budget for this peer based on
// provisional/proven status.
func (st *peerStat) failureLimit(proven, provisional int) int {
	if st.provisional {
		return provisional
	}
	return proven
}

// recordFailure increments failures and bans if the limit is reached.
// Returns true on transition to banned (caller does banAndDrop). Calls
// after the peer is already banned are a no-op and return false.
func (st *peerStat) recordFailure(limit int) bool {
	if st.banned {
		return false
	}
	st.failures++
	if st.failures >= limit {
		st.banned = true
		return true
	}
	return false
}

// promote flips a provisional peer to proven. Returns true on transition.
func (st *peerStat) promote() bool {
	if st.provisional {
		st.provisional = false
		return true
	}
	return false
}

// benchIfProvisional bans the peer if it's still provisional. Returns
// true on transition. Used by probe-timeout handling.
func (st *peerStat) benchIfProvisional() bool {
	if st.provisional {
		st.banned = true
		return true
	}
	return false
}

type inflightEntry struct {
	peer p2p.ID
	sent time.Time
}

// chunkScheduler is the phase-3 chunk scheduler. It dispatches chunks
// across good peers, verifies SHA256 against the metadata hashes, and
// writes each verified chunk to <snapDir>/chunk_<idx>.bin.
//
// Resilience:
//   - Pinned peers. Every peer entering stats is Pinned with
//     connect.Manager so the manager keeps redialing on disconnect.
//   - Provisional peer promotion. New connections enter as provisional
//     with one in-flight slot; first verified chunk promotes them.
//   - Misbehavior bans go through peerWatch.banPeer (disconnect +
//     addrbook MarkBad) AND mgr.Ban (manager stops redialing).
// schedulerSwitch is the subset of *p2p.Switch chunkScheduler needs.
// Defined as an interface so tests can supply a fake.
type schedulerSwitch interface {
	Peers() p2p.IPeerSet
	NumPeers() (outbound, inbound, dialing int)
}

// schedulerReactor is the subset of *statesync.Reactor chunkScheduler needs.
type schedulerReactor interface {
	RequestChunk(peer p2p.Peer, height uint64, format, index uint32) bool
	Drops() (ctrl, chunk int64)
}

// schedulerManager is the subset of *connect.Manager chunkScheduler needs.
type schedulerManager interface {
	Pin(pid p2p.ID, addr string)
	Ban(pid p2p.ID, reason string)
	IsBanned(pid p2p.ID) bool
}

type chunkScheduler struct {
	// dependencies
	sw    schedulerSwitch
	ssR   schedulerReactor
	mgr   schedulerManager
	watch *peerWatch
	log   cmtlog.Logger

	// job
	target      *snapshotOffer
	chunkHashes [][]byte
	snapDir     string

	// tuning
	perPeer             int
	chunkTimeout        time.Duration
	peerFailLimit       int
	provisionalStrikes  int
	provisionalInflight int

	// state
	pending    []bool
	completed  []bool
	inflight   map[uint32]inflightEntry
	stats      map[p2p.ID]*peerStat
	bytesTotal atomic.Uint64
	doneCount  uint32
	hadEvent   bool

	// progress
	startTime    time.Time
	lastProgress time.Time
}

const progressEvery = 10 * time.Second

// download is the library entry point — constructs a chunkScheduler,
// seeds it with the chunk-0 responder + offer's good peers, and runs
// the main loop. Returns total bytes transferred.
func download(ctx context.Context, sw *p2p.Switch, ssR *statesync.Reactor,
	sub *subscription, target *snapshotOffer, chunkHashes [][]byte,
	good []p2p.ID, snapDir string,
	perPeer int, chunkTimeout time.Duration, peerFailLimit int,
	provisionalStrikes, provisionalInflight int,
	watch *peerWatch, mgr *connect.Manager) (uint64, error) {

	N := target.Chunks
	pending := make([]bool, N)
	for i := uint32(0); i < N; i++ {
		pending[i] = true
	}
	now := time.Now()

	s := &chunkScheduler{
		sw:                  sw,
		ssR:                 ssR,
		mgr:                 mgr,
		watch:               watch,
		log:                 logctx.From(ctx),
		target:              target,
		chunkHashes:         chunkHashes,
		snapDir:             snapDir,
		perPeer:             perPeer,
		chunkTimeout:        chunkTimeout,
		peerFailLimit:       peerFailLimit,
		provisionalStrikes:  provisionalStrikes,
		provisionalInflight: provisionalInflight,
		pending:             pending,
		completed:           make([]bool, N),
		inflight:            map[uint32]inflightEntry{},
		stats:               map[p2p.ID]*peerStat{},
		startTime:           now,
		lastProgress:        now,
	}
	s.init(good)
	return s.run(ctx, sub)
}

// init populates stats with the offer's "good" peers, pins them with
// the manager, and folds in any other already-connected peers as
// provisional. Final initial dispatch fires before the main loop.
func (s *chunkScheduler) init(good []p2p.ID) {
	for _, p := range good {
		s.stats[p] = &peerStat{}
	}
	// Pin good peers up front so the manager redials them if any drop.
	if s.mgr != nil {
		for _, pid := range good {
			if peer := s.sw.Peers().Get(pid); peer != nil {
				s.mgr.Pin(pid, peer.SocketAddr().String())
			}
		}
	}
	// Initial scan: Connected events only flow forward, but peers may
	// already be connected before we subscribed to the mux. Walk
	// sw.Peers().List() once to seed stats for non-good peers.
	for _, p := range s.sw.Peers().List() {
		s.addProvisional(p)
	}
	s.log.Info("download starting",
		"chunks", s.target.Chunks, "good_peers", len(good),
		"per_peer_inflight", s.perPeer, "tracked", len(s.stats))
	s.dispatch()
}

// run is the main event loop. Returns when all chunks are received
// (completed = N), all peers are banned, or ctx is cancelled.
func (s *chunkScheduler) run(ctx context.Context, sub *subscription) (uint64, error) {
	timeoutTicker := time.NewTicker(2 * time.Second)
	defer timeoutTicker.Stop()

	N := s.target.Chunks
	for s.doneCount < N {
		alive, connected := s.peerCounts()
		if alive == 0 {
			return s.bytesTotal.Load(),
				fmt.Errorf("all peers banned (done %d/%d)", s.doneCount, N)
		}

		select {
		case <-ctx.Done():
			return s.bytesTotal.Load(), ctx.Err()
		case <-timeoutTicker.C:
			now := time.Now()
			s.onTimeoutTick(now)
			s.logProgress(now, alive, connected)
		case ev := <-sub.Ctrl:
			s.onEvent(ev)
		case ev := <-sub.Chunk:
			s.onEvent(ev)
		}

		if !s.hadEvent && time.Since(s.startTime) > 30*time.Second {
			s.log.Error("no chunk replies in 30s; check peers", "alive_peers", alive)
			s.startTime = time.Now()
		}
	}
	s.log.Info("download finished",
		"chunks", N, "bytes", s.bytesTotal.Load(),
		"elapsed", time.Since(s.startTime))
	return s.bytesTotal.Load(), nil
}

// peerCounts returns (alive, connected). alive = stats entries not
// banned. connected = of those, currently connected.
func (s *chunkScheduler) peerCounts() (alive, connected int) {
	for pid, st := range s.stats {
		if st.banned {
			continue
		}
		alive++
		if s.sw.Peers().Get(pid) != nil {
			connected++
		}
	}
	return
}

// onTimeoutTick handles the 2s ticker: expire timed-out in-flight
// chunks, mirror manager bans into stats, reconcile against the
// current peer set (so a dropped Connected event self-heals), then
// redispatch.
func (s *chunkScheduler) onTimeoutTick(now time.Time) {
	for idx, info := range s.inflight {
		if now.Sub(info.sent) <= s.chunkTimeout {
			continue
		}
		st := s.stats[info.peer]
		if st != nil {
			st.removeInflight()
			// Provisional peers that time out on their probe lose
			// their slot immediately — connection is suspect.
			if st.benchIfProvisional() {
				s.log.Debug("benching provisional peer (probe timeout)",
					"peer", string(info.peer))
				s.banAndDrop(info.peer, "probe timeout")
			}
		}
		delete(s.inflight, idx)
		s.pending[idx] = true
	}
	// Mirror peerWatch / manager bans into stats so pickPeer stops
	// considering them. (Manager handles all redialing.)
	for pid, st := range s.stats {
		if st.banned {
			continue
		}
		if s.mgr != nil && s.mgr.IsBanned(pid) {
			st.banned = true
		}
	}
	for _, p := range s.sw.Peers().List() {
		s.addProvisional(p)
	}
	s.dispatch()
}

// logProgress emits an info-level progress line once per progressEvery.
func (s *chunkScheduler) logProgress(now time.Time, alive, connected int) {
	if now.Sub(s.lastProgress) < progressEvery {
		return
	}
	rate := float64(s.doneCount) / now.Sub(s.startTime).Seconds()
	dropsCtrl, dropsChunk := s.ssR.Drops()
	s.log.Info("download progress",
		"chunks", fmt.Sprintf("%d/%d", s.doneCount, s.target.Chunks),
		"MB", s.bytesTotal.Load()>>20,
		"chunks_per_s", fmt.Sprintf("%.1f", rate),
		"peers", fmt.Sprintf("%d/%d", connected, alive),
		"inflight", len(s.inflight),
		"drops_ctrl", dropsCtrl,
		"drops_chunk", dropsChunk)
	s.lastProgress = now
}

// onEvent dispatches a statesync event by type.
func (s *chunkScheduler) onEvent(ev statesync.Event) {
	switch {
	case ev.Connected:
		s.onConnect(p2p.ID(ev.PeerID))
	case ev.Removed:
		s.onRemoved(p2p.ID(ev.PeerID))
	case ev.Chunk != nil:
		s.onChunk(ev)
	}
}

// onConnect handles a Connected event: registers the peer as
// provisional and triggers dispatch.
func (s *chunkScheduler) onConnect(pid p2p.ID) {
	if peer := s.sw.Peers().Get(pid); peer != nil {
		s.addProvisional(peer)
	}
	s.dispatch()
}

// onRemoved handles a Removed event: drops in-flight assignments for
// the disconnected peer so they get retried. Manager handles redialing.
func (s *chunkScheduler) onRemoved(pid p2p.ID) {
	for idx, info := range s.inflight {
		if info.peer == pid {
			delete(s.inflight, idx)
			s.pending[idx] = true
		}
	}
	if st, ok := s.stats[pid]; ok {
		st.clearInflight()
	}
	s.dispatch()
}

// onChunk handles a ChunkResponse event: validates the chunk against
// the target snapshot, verifies the hash, and writes to disk on success.
func (s *chunkScheduler) onChunk(ev statesync.Event) {
	if ev.Chunk.Height != s.target.Height || ev.Chunk.Format != s.target.Format {
		return
	}
	s.hadEvent = true
	idx := ev.Chunk.Index
	peer := p2p.ID(ev.PeerID)
	st, ok := s.stats[peer]
	if !ok {
		st = &peerStat{}
		s.stats[peer] = st
	}
	if info, ok := s.inflight[idx]; ok && info.peer == peer {
		delete(s.inflight, idx)
	}
	st.removeInflight()

	if s.completed[idx] {
		return
	}

	// Provisional peers get a tighter strike budget on their probe
	// than proven peers.
	limit := st.failureLimit(s.peerFailLimit, s.provisionalStrikes)

	if ev.Chunk.Missing || len(ev.Chunk.Bytes) == 0 {
		if st.recordFailure(limit) {
			s.log.Debug("benching peer",
				"peer", string(peer), "failures", st.failures, "provisional", st.provisional)
			s.banAndDrop(peer, "missing/empty chunk")
		}
		s.pending[idx] = true
		s.dispatch()
		return
	}

	h := sha256.Sum256(ev.Chunk.Bytes)
	if !bytes.Equal(h[:], s.chunkHashes[idx]) {
		s.log.Error("chunk hash mismatch",
			"peer", string(peer), "idx", idx,
			"got_sha", hex.EncodeToString(h[:8]),
			"want_sha", hex.EncodeToString(s.chunkHashes[idx][:8]))
		if st.recordFailure(limit) {
			s.banAndDrop(peer, "chunk hash mismatch")
		}
		s.pending[idx] = true
		s.dispatch()
		return
	}

	// Verified chunk — promote a provisional peer to proven.
	if st.promote() {
		s.log.Info("peer promoted from provisional", "peer", string(peer))
	}

	// Write the verified chunk to disk. Errors are logged and ignored
	// so a transient disk hiccup doesn't abort the whole fetch — if
	// the file is missing later, the import step surfaces it. The
	// write is atomic (tmp + fsync + rename) so a crash mid-write
	// can't leave a half-written chunk_NNNNN.bin in the dir; the
	// snapDir itself is fsynced once at finalize time in writeMeta.
	chunkPath := filepath.Join(s.snapDir, fmt.Sprintf("chunk_%05d.bin", idx))
	if err := writeFileAtomic(chunkPath, ev.Chunk.Bytes, 0o644); err != nil {
		s.log.Error("write chunk", "idx", idx, "err", err)
	}
	s.completed[idx] = true
	s.doneCount++
	s.bytesTotal.Add(uint64(len(ev.Chunk.Bytes)))
	s.dispatch()
}

// addProvisional registers a freshly-connected peer in stats as
// provisional and Pins it with the manager. Idempotent.
func (s *chunkScheduler) addProvisional(peer p2p.Peer) {
	pid := peer.ID()
	if _, ok := s.stats[pid]; ok {
		return
	}
	s.stats[pid] = &peerStat{provisional: true}
	if s.mgr != nil {
		s.mgr.Pin(pid, peer.SocketAddr().String())
	}
	s.log.Debug("provisional peer added", "peer", string(pid))
}

// pickPeer prefers proven peers up to perPeer in-flight, then falls
// back to provisional peers up to provisionalInflight slots. A
// freshly-warm peer can't take more than its probe budget of
// concurrent chunks until it's proven it can serve.
func (s *chunkScheduler) pickPeer() p2p.ID {
	var bestProven, bestProvis p2p.ID
	bestProvenInflight := s.perPeer + 1
	bestProvisInflight := s.provisionalInflight + 1
	for pid, st := range s.stats {
		if st.banned {
			continue
		}
		if s.sw.Peers().Get(pid) == nil {
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
	if bestProvenInflight <= s.perPeer {
		return bestProven
	}
	if bestProvisInflight <= s.provisionalInflight {
		return bestProvis
	}
	return ""
}

// dispatch pairs pending chunks with available peers via pickPeer
// and fires ChunkRequests until either pending is exhausted or no
// peer has an open slot.
func (s *chunkScheduler) dispatch() int {
	dispatched := 0
	for i := uint32(0); i < s.target.Chunks; i++ {
		if !s.pending[i] {
			continue
		}
		pid := s.pickPeer()
		if pid == "" {
			return dispatched
		}
		peer := s.sw.Peers().Get(pid)
		if peer == nil {
			s.stats[pid].banned = true
			continue
		}
		if !s.ssR.RequestChunk(peer, s.target.Height, s.target.Format, i) {
			continue
		}
		s.pending[i] = false
		s.inflight[i] = inflightEntry{peer: pid, sent: time.Now()}
		s.stats[pid].addInflight()
		dispatched++
	}
	return dispatched
}

// banAndDrop is the misbehavior path: tell the manager to stop
// redialing AND have peerWatch disconnect + addrbook-MarkBad.
func (s *chunkScheduler) banAndDrop(pid p2p.ID, reason string) {
	if s.mgr != nil {
		s.mgr.Ban(pid, reason)
	}
	if s.watch == nil {
		return
	}
	if peer := s.sw.Peers().Get(pid); peer != nil {
		s.watch.banPeer(peer, reason)
	}
}
