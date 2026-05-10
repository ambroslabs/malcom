package snapfetch

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"log/slog"

	"github.com/cometbft/cometbft/p2p"

	"github.com/zrbecker/cosmos-p2p/internal/connect"
	"github.com/zrbecker/cosmos-p2p/internal/helpers/served"
	"github.com/zrbecker/cosmos-p2p/internal/logctx"
	"github.com/zrbecker/cosmos-p2p/internal/statesync"
)

type peerStat struct {
	inflight    int
	failures    int  // consecutive hash-mismatch responses since the peer's last verified chunk; reset to 0 on success. Missing chunks no longer increment this — they're tracked in `declined` instead.
	banned      bool // permanently benched (PeerFailLimit hit, or peerWatch eviction)
	provisional bool // true until peer responds with first verified chunk; provisional peers get one in-flight slot and a single-strike ban budget
	chunks      int  // verified chunks served by this peer (post-hash-check)
	wasGood     bool // peer was in the snapshot offer's "good" set at init
	banReason   string
	// declined tracks chunk indices the peer reported as missing.
	// pickPeer skips peers that already declined the chunk being
	// dispatched, so we never re-ask a peer for data they told us
	// they don't have. Persistent for the run.
	declined map[uint32]bool
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
	BanReason(pid p2p.ID) string
	Stats() connect.Stats
}

type chunkScheduler struct {
	// dependencies
	sw    schedulerSwitch
	ssR   schedulerReactor
	mgr   schedulerManager
	watch *peerWatch
	srv   *served.Set
	log   *slog.Logger

	// writeFile is the chunk-write hook. Production wires writeFileAtomic;
	// tests inject a failing stub to drive the disk-failure path without
	// fiddling with filesystem permissions.
	writeFile func(path string, data []byte, mode os.FileMode) error

	// onChunkReady, when non-nil, fires once per verified chunk
	// after it lands durably on disk — both freshly-downloaded chunks
	// and chunks resumed from a prior partial run. Used by the
	// pipelined-import orchestrator to advance its tailing reader.
	onChunkReady func(idx uint32)

	// job
	chainID     string
	target      *snapshotOffer
	chunkHashes [][]byte
	snapDir     string

	// tuning
	perPeer             int
	chunkTimeout        time.Duration
	peerFailLimit       int
	provisionalStrikes  int
	provisionalInflight int
	maxDiskFails        int

	// state
	pending      []bool
	completed    []bool
	inflight     map[uint32]inflightEntry
	stats        map[p2p.ID]*peerStat
	bytesTotal   uint64
	doneCount    uint32
	hadEvent     bool
	diskFails    int
	firstDiskErr error

	// progress
	startTime    time.Time
	lastProgress time.Time
}

const progressEvery = 10 * time.Second

// download is the library entry point — constructs a chunkScheduler,
// seeds it with the chunk-0 responder + offer's good peers, and runs
// the main loop. Returns total bytes transferred.
func download(ctx context.Context, sw *p2p.Switch, ssR *statesync.Reactor,
	sub *subscription, chainID string, target *snapshotOffer, chunkHashes [][]byte,
	good []p2p.ID, snapDir string,
	perPeer int, chunkTimeout time.Duration, peerFailLimit int,
	provisionalStrikes, provisionalInflight, maxDiskFails int,
	watch *peerWatch, mgr *connect.Manager, srv *served.Set,
	onChunkReady func(idx uint32)) (uint64, error) {

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
		srv:                 srv,
		log:                 logctx.From(ctx),
		writeFile:           writeFileAtomic,
		onChunkReady:        onChunkReady,
		chainID:             chainID,
		target:              target,
		chunkHashes:         chunkHashes,
		snapDir:             snapDir,
		perPeer:             perPeer,
		chunkTimeout:        chunkTimeout,
		peerFailLimit:       peerFailLimit,
		provisionalStrikes:  provisionalStrikes,
		provisionalInflight: provisionalInflight,
		maxDiskFails:        maxDiskFails,
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
		s.stats[p] = &peerStat{wasGood: true}
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
	resumed, removed := s.resumeFromDisk()
	s.log.Info("download starting",
		"chunks", s.target.Chunks, "good_peers", len(good),
		"per_peer_inflight", s.perPeer, "tracked", len(s.stats),
		"resumed", resumed, "removed_stale", removed)
	s.dispatch()
}

// resumeFromDisk scans snapDir for chunk artefacts left by a prior
// partial fetch. Each in-range chunk_<idx>.bin is SHA256-verified
// against the chosen offer's chunk_hashes — matches are pre-marked
// completed so the dispatcher skips them; mismatches are removed.
// Out-of-range chunk_<idx>.bin files (orphans from a prior run that
// picked an offer with more chunks) and chunk_*.bin.tmp leftovers
// from a crash mid-rename are also swept. Returns (resumed, removed)
// for logging.
//
// Reuses the same verification logic as onChunk; the only difference
// is the bytes are read from disk instead of arriving in a
// ChunkResponse. Called from init() before the first dispatch.
func (s *chunkScheduler) resumeFromDisk() (resumed, removed int) {
	entries, err := os.ReadDir(s.snapDir)
	if err != nil {
		s.log.Error("resume: read snap dir", "path", s.snapDir, "err", err)
		return
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasPrefix(name, "chunk_") {
			continue
		}
		path := filepath.Join(s.snapDir, name)
		// Stale tmp from a crash mid writeFileAtomic (live runs never
		// see it — rename clears the tmp in the same syscall).
		if strings.HasSuffix(name, ".bin.tmp") {
			if err := os.Remove(path); err != nil {
				s.log.Error("resume: remove stale tmp", "name", name, "err", err)
				continue
			}
			removed++
			continue
		}
		if !strings.HasSuffix(name, ".bin") {
			continue
		}
		idxStr := strings.TrimSuffix(strings.TrimPrefix(name, "chunk_"), ".bin")
		idx64, perr := strconv.ParseUint(idxStr, 10, 32)
		if perr != nil {
			continue
		}
		idx := uint32(idx64)
		// Orphan: prior run picked an offer with more chunks. Drop
		// the file so disk usage tracks the current offer.
		if idx >= s.target.Chunks {
			if err := os.Remove(path); err != nil {
				s.log.Error("resume: remove orphan chunk", "idx", idx, "err", err)
				continue
			}
			removed++
			continue
		}
		// Already marked completed (dup file, e.g. chunk_00001.bin and
		// chunk_001.bin both decoding to idx=1) — skip the second one
		// to keep doneCount/bytesTotal honest.
		if s.completed[idx] {
			continue
		}
		data, rerr := os.ReadFile(path)
		if rerr != nil {
			// Read errors leave the file alone; the dispatcher's
			// atomic rename will clobber it during a refetch.
			continue
		}
		h := sha256.Sum256(data)
		if bytes.Equal(h[:], s.chunkHashes[idx]) {
			s.completed[idx] = true
			s.pending[idx] = false
			s.bytesTotal += uint64(len(data))
			s.doneCount++
			resumed++
			if s.onChunkReady != nil {
				s.onChunkReady(idx)
			}
			continue
		}
		if rmErr := os.Remove(path); rmErr != nil {
			s.log.Error("resume: remove stale chunk", "idx", idx, "err", rmErr)
			continue
		}
		removed++
	}
	return
}

// run is the main event loop. Returns when all chunks are received
// (completed = N), all peers are banned, or ctx is cancelled.
func (s *chunkScheduler) run(ctx context.Context, sub *subscription) (uint64, error) {
	timeoutTicker := time.NewTicker(2 * time.Second)
	defer timeoutTicker.Stop()

	N := s.target.Chunks
	for s.doneCount < N {
		if s.maxDiskFails > 0 && s.diskFails >= s.maxDiskFails {
			return s.bytesTotal,
				fmt.Errorf("%w: %d chunk write failures hit limit (%d) — likely disk full or I/O error: %w",
					ErrDiskFailed, s.diskFails, s.maxDiskFails, s.firstDiskErr)
		}
		alive, connected := s.peerCounts()
		if alive == 0 {
			return s.bytesTotal,
				fmt.Errorf("%w: all peers banned (done %d/%d) — %s",
					ErrDownloadFailed, s.doneCount, N, hintAllPeersBanned(s.chainID))
		}

		select {
		case <-ctx.Done():
			return s.bytesTotal, ctx.Err()
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
		"chunks", N, "bytes", s.bytesTotal,
		"elapsed", time.Since(s.startTime))
	s.logFinalSummary()
	return s.bytesTotal, nil
}

// logFinalSummary emits aggregate + per-peer info-level lines at end
// of download. Aggregates reflect the chunkScheduler's view; manager
// summary covers all dialing across walk + download.
func (s *chunkScheduler) logFinalSummary() {
	// Mirror any straggler manager bans into stats so the summary
	// classifies them correctly. For peers banned via paths the
	// scheduler doesn't observe directly (peerWatch evictions,
	// max-redials auto-bans), recover the reason from the manager
	// so every banned row in the summary has a real cause string.
	if s.mgr != nil {
		for pid, st := range s.stats {
			if !st.banned && s.mgr.IsBanned(pid) {
				st.banned = true
			}
			if st.banned && st.banReason == "" {
				if r := s.mgr.BanReason(pid); r != "" {
					st.banReason = r
				}
			}
		}
	}

	type row struct {
		pid       p2p.ID
		chunks    int
		failures  int
		good      bool
		prov      bool
		banned    bool
		reason    string
		connected bool
	}
	rows := make([]row, 0, len(s.stats))
	var (
		totalTracked     int
		totalServed      int
		totalBanned      int
		totalConnectedNow int
		totalGood        int
		totalGoodServed  int
		totalProvServed  int
	)
	for pid, st := range s.stats {
		conn := s.sw.Peers().Get(pid) != nil
		rows = append(rows, row{
			pid: pid, chunks: st.chunks, failures: st.failures,
			good: st.wasGood, prov: st.provisional,
			banned: st.banned, reason: st.banReason, connected: conn,
		})
		totalTracked++
		if st.banned {
			totalBanned++
		}
		if st.chunks > 0 {
			totalServed++
			if st.wasGood {
				totalGoodServed++
			} else {
				totalProvServed++
			}
		}
		if st.wasGood {
			totalGood++
		}
		if conn {
			totalConnectedNow++
		}
	}

	s.log.Info("peer summary",
		"tracked", totalTracked,
		"served_chunks", totalServed,
		"banned", totalBanned,
		"connected_now", totalConnectedNow,
		"good_seed", totalGood,
		"good_served", totalGoodServed,
		"provisional_served", totalProvServed)

	sort.Slice(rows, func(i, j int) bool {
		if rows[i].chunks != rows[j].chunks {
			return rows[i].chunks > rows[j].chunks
		}
		return rows[i].pid < rows[j].pid
	})
	for _, r := range rows {
		role := "provisional"
		if r.good {
			role = "good"
		} else if !r.prov {
			role = "promoted"
		}
		s.log.Debug("peer detail",
			"peer", string(r.pid),
			"role", role,
			"chunks", r.chunks,
			"failures", r.failures,
			"banned", r.banned,
			"ban_reason", r.reason,
			"connected", r.connected)
	}

	if s.mgr != nil {
		st := s.mgr.Stats()
		s.log.Info("manager summary",
			"dials_fired", st.DialsFired,
			"dial_successes", st.DialSuccesses,
			"dial_failures", st.DialFailures,
			"addr_bans_max_dial_fails", st.AddrBans,
			"peer_bans_max_redials", st.PeerBans,
			"manual_bans", st.ManualBans,
			"pinned_now", st.Pinned,
			"banned_total", st.Banned)
	}
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
		"MB", s.bytesTotal>>20,
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
		// Peer told us they don't have this chunk. Two effects:
		//   1. Record the decline so pickPeer never re-asks them
		//      for this index.
		//   2. Re-flag the chunk as pending so the dispatcher picks
		//      a different peer that hasn't declined it.
		// We do NOT count this against the peer's strike budget —
		// "missing chunk" is data-availability, not misbehaviour.
		// A peer with a partial snapshot still serves the chunks
		// they do have. Hash-mismatch (forgery) is the only path
		// that calls recordFailure now.
		if st.declined == nil {
			st.declined = map[uint32]bool{}
		}
		st.declined[idx] = true
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
	st.chunks++
	// Reset the failure counter: failures count consecutive
	// hash-mismatch responses since the peer's last verified chunk.
	// A peer that occasionally returns a bad chunk but otherwise
	// serves cleanly should not accumulate strikes across the
	// whole run.
	//
	// Trade-off worth noting: a forger can interleave good chunks
	// with hash-mismatch chunks indefinitely without ever hitting
	// PeerFailLimit, since each verified chunk wipes the strike
	// budget. Mitigations if this becomes a real attack: (a) require
	// N consecutive successes before reset, (b) keep a separate
	// cumulative-mismatch counter with a higher cap, or (c) ban
	// outright on the first hash mismatch (the strict pre-PR
	// behaviour for provisionals — was a single strike). Verified
	// downloads still pass the snapshot.Hash check at the end, so a
	// forger has to produce hash-valid forged chunks for every chunk
	// they serve to actually corrupt the output — interleaving real
	// and forged is detected by the per-chunk hash check, just at
	// the cost of extra retry latency.
	st.failures = 0
	// Record the peer as a chunk-server in the cross-run list so
	// future fetches can pin them up front.
	if s.srv != nil {
		if p := s.sw.Peers().Get(peer); p != nil {
			s.srv.Record(string(peer), p.SocketAddr().String())
		}
	}

	// Write the verified chunk to disk. Failures are treated like a
	// hash-mismatch from the chunk's perspective: leave pending=true
	// so the dispatcher retries it, don't bump doneCount/bytesTotal,
	// and bump diskFails. The run loop aborts with ErrDiskFailed once
	// diskFails crosses maxDiskFails — counting "" as missing/half-
	// written chunks would otherwise sneak past finalization and
	// surface much later as a confusing zlib error during import.
	// The write is atomic (tmp + fsync + rename) so a crash mid-write
	// can't leave a half-written chunk_NNNNN.bin in the dir; the
	// snapDir itself is fsynced once at finalize time in writeMeta.
	chunkPath := filepath.Join(s.snapDir, fmt.Sprintf("chunk_%05d.bin", idx))
	if err := s.writeFile(chunkPath, ev.Chunk.Bytes, 0o644); err != nil {
		s.log.Error("write chunk", "idx", idx, "err", err)
		if s.firstDiskErr == nil {
			s.firstDiskErr = err
		}
		s.diskFails++
		s.pending[idx] = true
		s.dispatch()
		return
	}
	s.completed[idx] = true
	s.doneCount++
	s.bytesTotal += uint64(len(ev.Chunk.Bytes))
	if s.onChunkReady != nil {
		s.onChunkReady(idx)
	}
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
//
// Proven peers are gated strictly (inflight < perPeer): the configured
// limit is the actual ceiling. Provisional peers are gated loosely
// (inflight <= provisionalInflight) so dispatch's post-pick addInflight
// gives the probe one slot of headroom — enough to surface a hash or
// timeout strike before the peer either gets proven or banned.
//
// chunkIdx is the chunk being dispatched. Peers that previously
// declined that exact index (returned missing/empty for it) are
// skipped so we don't re-ask them for data they told us they don't
// have.
func (s *chunkScheduler) pickPeer(chunkIdx uint32, skip map[p2p.ID]bool) p2p.ID {
	var bestProven, bestProvis p2p.ID
	bestProvenInflight := s.perPeer + 1
	bestProvisInflight := s.provisionalInflight + 1
	for pid, st := range s.stats {
		if st.banned {
			continue
		}
		if skip[pid] {
			continue
		}
		if st.declined[chunkIdx] {
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
	if bestProvenInflight < s.perPeer {
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
//
// A peer whose RequestChunk returns false (cometbft send queue full)
// is added to a per-dispatch skip set so pickPeer doesn't keep
// reselecting it for every remaining pending chunk in the same call.
func (s *chunkScheduler) dispatch() int {
	dispatched := 0
	var skip map[p2p.ID]bool
	for i := uint32(0); i < s.target.Chunks; i++ {
		if !s.pending[i] {
			continue
		}
		pid := s.pickPeer(i, skip)
		if pid == "" {
			return dispatched
		}
		peer := s.sw.Peers().Get(pid)
		if peer == nil {
			s.stats[pid].banned = true
			continue
		}
		if !s.ssR.RequestChunk(peer, s.target.Height, s.target.Format, i) {
			if skip == nil {
				skip = map[p2p.ID]bool{}
			}
			skip[pid] = true
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
	if st, ok := s.stats[pid]; ok && st.banReason == "" {
		st.banReason = reason
	}
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
