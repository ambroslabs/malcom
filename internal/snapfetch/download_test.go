package snapfetch

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"log/slog"
	"github.com/cometbft/cometbft/p2p"

	"github.com/ambroslabs/malcom/internal/statesync"
)

// scenarioBuilder constructs a chunkScheduler with single-chunk target
// and the supplied fakes wired in. Tests can then call methods
// directly to drive the state machine.
type scenarioBuilder struct {
	sw         *fakeSchedulerSwitch
	reactor    *fakeReactor
	mgr        *fakeManager
	target     *snapshotOffer
	chunkBytes []byte
	chunkHash  []byte
	snapDir    string
}

func newScenario(t *testing.T) *scenarioBuilder {
	chunk := []byte("hello chunk")
	h := sha256.Sum256(chunk)
	return &scenarioBuilder{
		sw:      newFakeSchedulerSwitch(),
		reactor: newFakeReactor(),
		mgr:     newFakeManager(),
		target: &snapshotOffer{
			Height: 100, Format: 1, Chunks: 1,
			Hash:  []byte("snap-hash"),
			Peers: map[string]bool{},
		},
		chunkBytes: chunk,
		chunkHash:  h[:],
		snapDir:    t.TempDir(),
	}
}

func (b *scenarioBuilder) build() *chunkScheduler {
	pending := make([]bool, b.target.Chunks)
	for i := range pending {
		pending[i] = true
	}
	hashes := make([][]byte, b.target.Chunks)
	for i := range hashes {
		hashes[i] = b.chunkHash
	}
	now := time.Now()
	return &chunkScheduler{
		sw:          b.sw,
		ssR:         b.reactor,
		mgr:         b.mgr,
		watch:       nil, // banAndDrop falls through to mgr.Ban only
		log:         slog.New(slog.DiscardHandler),
		writeFile:   writeFileAtomic,
		target:      b.target,
		chunkHashes: hashes,
		snapDir:     b.snapDir,
		perPeer:     4,
		// chunkTimeout is set high so non-timeout tests can never
		// accidentally trigger a timeout. Timeout-focused tests
		// override on the built scheduler.
		chunkTimeout:        time.Hour,
		peerFailLimit:       3,
		provisionalStrikes:  1,
		provisionalInflight: 1,
		maxDiskFails:        3,
		pending:             pending,
		completed:           make([]bool, b.target.Chunks),
		inflight:            map[uint32]inflightEntry{},
		stats:               map[p2p.ID]*peerStat{},
		startTime:           now,
		lastProgress:        now,
	}
}

// assignInflight mirrors what dispatch() does internally: marks the
// chunk in-flight on the peer and clears pending. Setup paths that
// only set s.inflight without addInflight desync peerStat.inflight
// from production state.
func assignInflight(s *chunkScheduler, pid p2p.ID, idx uint32, sent time.Time) {
	s.inflight[idx] = inflightEntry{peer: pid, sent: sent}
	s.pending[idx] = false
	s.stats[pid].addInflight()
}

// chunkEvent constructs a statesync.Event for a chunk reply against
// the scenario's target snapshot.
func (b *scenarioBuilder) chunkEvent(peerID p2p.ID, idx uint32, body []byte, missing bool) statesync.Event {
	return statesync.Event{
		PeerID: string(peerID),
		Chunk: &statesync.ChunkInfo{
			Height:  b.target.Height,
			Format:  b.target.Format,
			Index:   idx,
			Size:    len(body),
			Bytes:   body,
			Missing: missing,
		},
	}
}

// --- chunkScheduler tests ---

func TestChunkSchedulerOnConnectAddsProvisionalAndPins(t *testing.T) {
	b := newScenario(t)
	s := b.build()

	peer := newFakePeer("peer-A", "1.1.1.1", 26656)
	b.sw.peerSet.Add(peer)
	s.onConnect(peer.ID())

	// stats has the peer as provisional.
	st, ok := s.stats[peer.ID()]
	if !ok {
		t.Fatalf("peer not added to stats")
	}
	if !st.provisional {
		t.Fatalf("peer not provisional")
	}
	// Manager was Pin'd with the addr.
	if got := b.mgr.pinned[peer.ID()]; got == "" {
		t.Fatalf("manager.Pin not called")
	}
}

func TestChunkSchedulerOnRemovedDropsInflight(t *testing.T) {
	b := newScenario(t)
	s := b.build()

	peer := newFakePeer("peer-A", "1.1.1.1", 26656)
	// Peer was previously connected, then disconnected. Production
	// flow: Switch removes the peer first, then RemovePeer fires.
	s.stats[peer.ID()] = &peerStat{}
	assignInflight(s, peer.ID(), 0, time.Now())

	s.onRemoved(peer.ID())

	if _, still := s.inflight[0]; still {
		t.Fatalf("in-flight assignment not dropped on Removed")
	}
	if !s.pending[0] {
		t.Fatalf("chunk not re-marked pending after Removed")
	}
	// Stats entry may be retained (current behavior, clearInflight) or
	// removed in the future — both satisfy "inflight cleared".
	if st, ok := s.stats[peer.ID()]; ok && st.inflight != 0 {
		t.Fatalf("inflight not cleared: got %d", st.inflight)
	}
}

func TestChunkSchedulerVerifiedChunkPromotesAndWrites(t *testing.T) {
	b := newScenario(t)
	s := b.build()

	peer := newFakePeer("peer-A", "1.1.1.1", 26656)
	b.sw.peerSet.Add(peer)
	s.stats[peer.ID()] = &peerStat{provisional: true}
	assignInflight(s, peer.ID(), 0, time.Now())

	ev := b.chunkEvent(peer.ID(), 0, b.chunkBytes, false)
	s.onChunk(ev)

	// Promoted.
	if s.stats[peer.ID()].provisional {
		t.Fatalf("peer not promoted from provisional")
	}
	// In-flight bookkeeping cleared.
	if _, still := s.inflight[0]; still {
		t.Fatalf("inflight[0] not cleared after verified chunk")
	}
	if got := s.stats[peer.ID()].inflight; got != 0 {
		t.Fatalf("peerStat.inflight=%d after verified chunk, want 0", got)
	}
	// Completed and written.
	if !s.completed[0] {
		t.Fatalf("chunk not marked completed")
	}
	if s.doneCount != 1 {
		t.Fatalf("doneCount=%d, want 1", s.doneCount)
	}
	if s.bytesTotal != uint64(len(b.chunkBytes)) {
		t.Fatalf("bytesTotal=%d, want %d", s.bytesTotal, len(b.chunkBytes))
	}
	// File on disk.
	written, err := os.ReadFile(filepath.Join(b.snapDir, "chunk_00000.bin"))
	if err != nil {
		t.Fatalf("chunk file not written: %v", err)
	}
	if string(written) != string(b.chunkBytes) {
		t.Fatalf("chunk file content mismatch")
	}
	// Atomic-write must not leave a .tmp behind on success.
	if _, err := os.Stat(filepath.Join(b.snapDir, "chunk_00000.bin.tmp")); !os.IsNotExist(err) {
		t.Fatalf("chunk_00000.bin.tmp leaked: stat err = %v", err)
	}
}

func TestChunkSchedulerHashMismatchSingleStrikeBansProvisional(t *testing.T) {
	b := newScenario(t)
	s := b.build()

	peer := newFakePeer("peer-A", "1.1.1.1", 26656)
	b.sw.peerSet.Add(peer)
	s.stats[peer.ID()] = &peerStat{provisional: true}
	assignInflight(s, peer.ID(), 0, time.Now())

	// Wrong bytes — hash won't match.
	ev := b.chunkEvent(peer.ID(), 0, []byte("WRONG"), false)
	s.onChunk(ev)

	if !s.stats[peer.ID()].banned {
		t.Fatalf("provisional peer not banned on first hash mismatch")
	}
	if reason := b.mgr.banReason(peer.ID()); reason != "chunk hash mismatch" {
		t.Fatalf("manager.Ban reason=%q, want chunk hash mismatch", reason)
	}
	if s.completed[0] {
		t.Fatalf("chunk should not be marked completed after mismatch")
	}
	if !s.pending[0] {
		t.Fatalf("chunk should be re-pending after mismatch")
	}
}

// Missing/empty chunks are data-availability, not misbehaviour: they
// add the chunk index to the peer's `declined` set and re-flag the
// chunk as pending, but they do NOT count toward the strike budget
// and do NOT ban. The peer stays in the rotation for chunks they
// haven't declined.
func TestChunkSchedulerMissingChunkDoesNotBan(t *testing.T) {
	b := newScenario(t)
	b.target.Chunks = 3
	s := b.build()

	peer := newFakePeer("peer-A", "1.1.1.1", 26656)
	b.sw.peerSet.Add(peer)
	s.stats[peer.ID()] = &peerStat{} // proven (provisional=false)

	for _, idx := range []uint32{0, 1, 2} {
		assignInflight(s, peer.ID(), idx, time.Now())
		ev := b.chunkEvent(peer.ID(), idx, nil, true) // missing
		s.onChunk(ev)
	}

	st := s.stats[peer.ID()]
	if st.banned {
		t.Fatalf("peer banned after 3 missing chunks; missing should never ban")
	}
	if got := b.mgr.banReason(peer.ID()); got != "" {
		t.Fatalf("manager.Ban invoked with reason=%q; want no ban for missing chunks", got)
	}
	for _, idx := range []uint32{0, 1, 2} {
		if !st.declined[idx] {
			t.Fatalf("declined[%d] not set; pickPeer would re-ask this peer for it", idx)
		}
		if !s.pending[idx] {
			t.Fatalf("chunk %d not re-pending after missing response", idx)
		}
	}
	// Inflight cleared, no leak.
	if len(s.inflight) != 0 {
		t.Fatalf("inflight=%v after misses; want empty", s.inflight)
	}
}

func TestChunkSchedulerProbeTimeoutBansProvisional(t *testing.T) {
	b := newScenario(t)
	s := b.build()

	peer := newFakePeer("peer-A", "1.1.1.1", 26656)
	b.sw.peerSet.Add(peer)
	s.stats[peer.ID()] = &peerStat{provisional: true}
	// Override the scenario's high default so the assignment below
	// is past the deadline.
	s.chunkTimeout = time.Second
	// Sent 10 minutes ago — way past chunkTimeout.
	assignInflight(s, peer.ID(), 0, time.Now().Add(-10*time.Minute))

	s.onTimeoutTick(time.Now())

	if !s.stats[peer.ID()].banned {
		t.Fatalf("provisional peer not banned on probe timeout")
	}
	if reason := b.mgr.banReason(peer.ID()); reason != "probe timeout" {
		t.Fatalf("manager.Ban reason=%q, want probe timeout", reason)
	}
	if !s.pending[0] {
		t.Fatalf("chunk not re-pending after timeout")
	}
}

func TestChunkSchedulerTimeoutMirrorsManagerBans(t *testing.T) {
	b := newScenario(t)
	s := b.build()

	peer := newFakePeer("peer-A", "1.1.1.1", 26656)
	b.sw.peerSet.Add(peer)
	s.stats[peer.ID()] = &peerStat{}

	// Manager has independently banned the peer (e.g., max-redials).
	b.mgr.Ban(peer.ID(), "max-dial-failures")

	s.onTimeoutTick(time.Now())

	if !s.stats[peer.ID()].banned {
		t.Fatalf("stats.banned not mirrored from manager")
	}
}

// If a Connected event was dropped (reactor.Out full), the peer never
// makes it into stats. onTimeoutTick must reconcile against the
// current peer set so the peer becomes available for dispatch.
func TestChunkSchedulerTimeoutReconcilesConnectedPeers(t *testing.T) {
	b := newScenario(t)
	s := b.build()

	peer := newFakePeer("peer-A", "1.1.1.1", 26656)
	// Peer is connected on the switch but missed Connected — stats is empty.
	b.sw.peerSet.Add(peer)

	s.onTimeoutTick(time.Now())

	st, ok := s.stats[peer.ID()]
	if !ok {
		t.Fatalf("connected peer not folded into stats by reconcile")
	}
	if !st.provisional {
		t.Fatalf("reconciled peer should be provisional")
	}
}

// Reconcile must not undo prior bans: a banned peer that's still
// connected stays banned (and stays non-provisional), regardless of
// how many times the ticker fires.
func TestChunkSchedulerTimeoutReconcileSkipsBannedPeer(t *testing.T) {
	b := newScenario(t)
	s := b.build()

	peer := newFakePeer("peer-A", "1.1.1.1", 26656)
	b.sw.peerSet.Add(peer)
	s.stats[peer.ID()] = &peerStat{banned: true}

	s.onTimeoutTick(time.Now())

	st := s.stats[peer.ID()]
	if !st.banned {
		t.Fatalf("reconcile un-banned a previously banned peer")
	}
	if st.provisional {
		t.Fatalf("reconcile flipped banned peer back to provisional")
	}
}

func TestChunkSchedulerDispatchAssignsChunks(t *testing.T) {
	b := newScenario(t)
	b.target.Chunks = 3
	s := b.build()

	peer := newFakePeer("peer-A", "1.1.1.1", 26656)
	b.sw.peerSet.Add(peer)
	s.stats[peer.ID()] = &peerStat{} // proven, perPeer=4 slots

	dispatched := s.dispatch()

	if dispatched != 3 {
		t.Fatalf("dispatched=%d, want 3", dispatched)
	}
	for i := uint32(0); i < 3; i++ {
		if !b.reactor.sentTo(peer.ID(), i) {
			t.Fatalf("RequestChunk for idx=%d not sent to peer-A", i)
		}
		if s.pending[i] {
			t.Fatalf("pending[%d] still true after dispatch", i)
		}
	}
}

// When RequestChunk returns false for a peer (cometbft send queue
// full), dispatch must not keep reselecting that peer for every
// remaining pending chunk in the same call. Regression test for
// issue #23: with one peer and N pending chunks, a jammed send queue
// should produce exactly one RequestChunk attempt, not N.
func TestChunkSchedulerDispatchSkipsPeerOnSendFail(t *testing.T) {
	b := newScenario(t)
	b.target.Chunks = 100
	b.reactor.requestChunkOK = false
	s := b.build()
	s.chunkHashes = make([][]byte, b.target.Chunks)
	s.pending = make([]bool, b.target.Chunks)
	for i := range s.pending {
		s.pending[i] = true
	}

	peer := newFakePeer("peer-A", "1.1.1.1", 26656)
	b.sw.peerSet.Add(peer)
	s.stats[peer.ID()] = &peerStat{} // proven, perPeer=4 slots

	if d := s.dispatch(); d != 0 {
		t.Fatalf("dispatched=%d, want 0 (send queue jammed)", d)
	}
	if got := b.reactor.numRequests(); got != 1 {
		t.Fatalf("RequestChunk called %d times, want 1 (peer should be skipped after first false)", got)
	}
	if s.stats[peer.ID()].inflight != 0 {
		t.Fatalf("inflight=%d, want 0 (failed RequestChunk must not bump inflight)", s.stats[peer.ID()].inflight)
	}
}

// With multiple peers and a jammed send queue across all of them,
// dispatch should try each peer at most once (skip set), then return.
func TestChunkSchedulerDispatchSkipsAllPeersOnSendFail(t *testing.T) {
	b := newScenario(t)
	b.target.Chunks = 100
	b.reactor.requestChunkOK = false
	s := b.build()
	s.chunkHashes = make([][]byte, b.target.Chunks)
	s.pending = make([]bool, b.target.Chunks)
	for i := range s.pending {
		s.pending[i] = true
	}

	a := newFakePeer("peer-A", "1.1.1.1", 26656)
	bp := newFakePeer("peer-B", "2.2.2.2", 26656)
	c := newFakePeer("peer-C", "3.3.3.3", 26656)
	b.sw.peerSet.Add(a)
	b.sw.peerSet.Add(bp)
	b.sw.peerSet.Add(c)
	s.stats[a.ID()] = &peerStat{}
	s.stats[bp.ID()] = &peerStat{}
	s.stats[c.ID()] = &peerStat{}

	if d := s.dispatch(); d != 0 {
		t.Fatalf("dispatched=%d, want 0", d)
	}
	if got := b.reactor.numRequests(); got != 3 {
		t.Fatalf("RequestChunk called %d times, want 3 (one per peer)", got)
	}
}

func TestChunkSchedulerPickPeerSkipsBanned(t *testing.T) {
	b := newScenario(t)
	s := b.build()

	good := newFakePeer("good", "1.1.1.1", 26656)
	bad := newFakePeer("bad", "2.2.2.2", 26656)
	b.sw.peerSet.Add(good)
	b.sw.peerSet.Add(bad)

	s.stats[good.ID()] = &peerStat{}
	s.stats[bad.ID()] = &peerStat{banned: true}

	for i := 0; i < 5; i++ {
		if pid := s.pickPeer(0, nil); pid != good.ID() {
			t.Fatalf("pickPeer=%q, want %q (banned should never be picked)", pid, good.ID())
		}
	}
}

// pickPeer must skip a peer that previously declined the requested
// chunk index, even if the peer is otherwise eligible. Other chunks
// should still route to that peer.
func TestChunkSchedulerPickPeerSkipsDeclinedChunk(t *testing.T) {
	b := newScenario(t)
	s := b.build()

	a := newFakePeer("a", "1.1.1.1", 26656)
	bb := newFakePeer("b", "2.2.2.2", 26656)
	b.sw.peerSet.Add(a)
	b.sw.peerSet.Add(bb)

	s.stats[a.ID()] = &peerStat{declined: map[uint32]bool{7: true}}
	s.stats[bb.ID()] = &peerStat{}

	if pid := s.pickPeer(7, nil); pid != bb.ID() {
		t.Fatalf("pickPeer(chunk=7)=%q, want %q (peer a declined chunk 7)", pid, bb.ID())
	}
	// A different chunk index — peer a is fine.
	if pid := s.pickPeer(8, nil); pid == "" {
		t.Fatalf("pickPeer(chunk=8) returned empty; want either peer eligible")
	}
}

// A verified chunk must reset the peer's consecutive-failure counter.
// A peer that misses one chunk, then serves another, then misses two
// more in a row should NOT be banned (limit=3 not reached because the
// success in between cleared the counter).
func TestPeerStatRecordSuccessResetsFailures(t *testing.T) {
	st := &peerStat{}
	limit := 3

	// 1st miss
	if banned := st.recordFailure(limit); banned {
		t.Fatalf("banned at failures=1; want false")
	}
	if st.failures != 1 {
		t.Fatalf("failures=%d, want 1", st.failures)
	}

	// Verified chunk arrives — code does st.failures = 0 inline.
	st.failures = 0

	// Two more misses — still under limit because counter reset.
	if banned := st.recordFailure(limit); banned {
		t.Fatalf("banned at failures=1 (post-reset); want false")
	}
	if banned := st.recordFailure(limit); banned {
		t.Fatalf("banned at failures=2 (post-reset); want false")
	}
	if st.failures != 2 {
		t.Fatalf("failures=%d, want 2 post-reset", st.failures)
	}

	// 3rd consecutive miss — limit hit.
	if banned := st.recordFailure(limit); !banned {
		t.Fatalf("banned at failures=3 (post-reset)=%v; want true", banned)
	}
}

// resumeScheduler builds a multi-chunk scheduler with distinct
// per-chunk bytes/hashes, suitable for resume tests. Returns the
// scheduler and the per-chunk byte slices indexed by chunk number.
func resumeScheduler(t *testing.T, n uint32) (*chunkScheduler, [][]byte) {
	t.Helper()
	b := newScenario(t)
	b.target.Chunks = n
	bodies := make([][]byte, n)
	hashes := make([][]byte, n)
	for i := uint32(0); i < n; i++ {
		body := []byte(fmt.Sprintf("chunk-%d-bytes", i))
		bodies[i] = body
		h := sha256.Sum256(body)
		hashes[i] = h[:]
	}
	s := b.build()
	s.chunkHashes = hashes
	s.pending = make([]bool, n)
	for i := range s.pending {
		s.pending[i] = true
	}
	s.completed = make([]bool, n)
	return s, bodies
}

func writeChunk(t *testing.T, dir string, idx uint32, body []byte) {
	t.Helper()
	path := filepath.Join(dir, fmt.Sprintf("chunk_%05d.bin", idx))
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatalf("write chunk_%05d.bin: %v", idx, err)
	}
}

func TestChunkSchedulerResumeAllValidChunksSkipsDispatch(t *testing.T) {
	s, bodies := resumeScheduler(t, 3)

	var totalBytes uint64
	for i, body := range bodies {
		writeChunk(t, s.snapDir, uint32(i), body)
		totalBytes += uint64(len(body))
	}

	resumed, removed := s.resumeFromDisk()
	if resumed != 3 || removed != 0 {
		t.Fatalf("resumeFromDisk = (%d,%d), want (3,0)", resumed, removed)
	}
	for i := uint32(0); i < 3; i++ {
		if !s.completed[i] {
			t.Fatalf("completed[%d] not set after resume", i)
		}
		if s.pending[i] {
			t.Fatalf("pending[%d] still set after resume", i)
		}
	}
	if s.doneCount != 3 {
		t.Fatalf("doneCount=%d, want 3", s.doneCount)
	}
	if got := s.bytesTotal; got != totalBytes {
		t.Fatalf("bytesTotal=%d, want %d", got, totalBytes)
	}

	// With everything completed, dispatch must not fire any RequestChunk.
	if d := s.dispatch(); d != 0 {
		t.Fatalf("dispatch=%d after full resume, want 0", d)
	}
}

func TestChunkSchedulerResumeRemovesMismatchedChunk(t *testing.T) {
	s, _ := resumeScheduler(t, 1)

	// Wrong bytes — hash will not match s.chunkHashes[0].
	writeChunk(t, s.snapDir, 0, []byte("WRONG"))
	path := filepath.Join(s.snapDir, "chunk_00000.bin")

	resumed, removed := s.resumeFromDisk()
	if resumed != 0 || removed != 1 {
		t.Fatalf("resumeFromDisk = (%d,%d), want (0,1)", resumed, removed)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("mismatched chunk not removed: stat err = %v", err)
	}
	if s.completed[0] {
		t.Fatalf("completed[0] set despite mismatch")
	}
	if !s.pending[0] {
		t.Fatalf("pending[0] cleared despite mismatch")
	}
	if s.doneCount != 0 {
		t.Fatalf("doneCount=%d, want 0", s.doneCount)
	}
	if got := s.bytesTotal; got != 0 {
		t.Fatalf("bytesTotal=%d, want 0", got)
	}
}

func TestChunkSchedulerResumePartialMix(t *testing.T) {
	s, bodies := resumeScheduler(t, 4)

	// idx 0,2 valid; idx 1 mismatched; idx 3 missing.
	writeChunk(t, s.snapDir, 0, bodies[0])
	writeChunk(t, s.snapDir, 1, []byte("WRONG"))
	writeChunk(t, s.snapDir, 2, bodies[2])

	resumed, removed := s.resumeFromDisk()
	if resumed != 2 || removed != 1 {
		t.Fatalf("resumeFromDisk = (%d,%d), want (2,1)", resumed, removed)
	}
	if !s.completed[0] || !s.completed[2] {
		t.Fatalf("valid chunks not marked completed: %v", s.completed)
	}
	if s.completed[1] || s.completed[3] {
		t.Fatalf("non-valid chunks wrongly marked completed: %v", s.completed)
	}
	if !s.pending[1] || !s.pending[3] {
		t.Fatalf("non-valid chunks not pending: %v", s.pending)
	}
	if s.doneCount != 2 {
		t.Fatalf("doneCount=%d, want 2", s.doneCount)
	}
	wantBytes := uint64(len(bodies[0]) + len(bodies[2]))
	if got := s.bytesTotal; got != wantBytes {
		t.Fatalf("bytesTotal=%d, want %d", got, wantBytes)
	}
	// Mismatched file removed; missing one stays missing.
	if _, err := os.Stat(filepath.Join(s.snapDir, "chunk_00001.bin")); !os.IsNotExist(err) {
		t.Fatalf("mismatched chunk_00001 not removed: stat err = %v", err)
	}
}

func TestChunkSchedulerResumeRemovesOrphanChunks(t *testing.T) {
	s, bodies := resumeScheduler(t, 3)

	// In-range valid chunk: kept.
	writeChunk(t, s.snapDir, 0, bodies[0])
	// Out-of-range orphans from a prior run that picked a 6-chunk
	// offer: must be removed so disk usage tracks the current offer.
	writeChunk(t, s.snapDir, 4, []byte("orphan-4"))
	writeChunk(t, s.snapDir, 5, []byte("orphan-5"))

	resumed, removed := s.resumeFromDisk()
	if resumed != 1 || removed != 2 {
		t.Fatalf("resumeFromDisk = (%d,%d), want (1,2)", resumed, removed)
	}
	for _, idx := range []uint32{4, 5} {
		path := filepath.Join(s.snapDir, fmt.Sprintf("chunk_%05d.bin", idx))
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("orphan chunk %d not removed: stat err = %v", idx, err)
		}
	}
	if !s.completed[0] {
		t.Fatalf("in-range valid chunk wrongly cleared")
	}
}

func TestChunkSchedulerResumeSweepsStaleTmpFiles(t *testing.T) {
	s, _ := resumeScheduler(t, 2)

	tmp := filepath.Join(s.snapDir, "chunk_00000.bin.tmp")
	if err := os.WriteFile(tmp, []byte("partial-write"), 0o644); err != nil {
		t.Fatalf("seed tmp: %v", err)
	}

	resumed, removed := s.resumeFromDisk()
	if resumed != 0 || removed != 1 {
		t.Fatalf("resumeFromDisk = (%d,%d), want (0,1)", resumed, removed)
	}
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatalf("stale tmp not removed: stat err = %v", err)
	}
}

func TestChunkSchedulerPickPeerProvenSlotsLimited(t *testing.T) {
	b := newScenario(t)
	b.target.Chunks = 8
	s := b.build()

	proven := newFakePeer("proven", "1.1.1.1", 26656)
	b.sw.peerSet.Add(proven)
	s.stats[proven.ID()] = &peerStat{}

	// perPeer = 4 in the scenario. pickPeer must hand out at most
	// perPeer concurrent slots — once inflight reaches the limit the
	// peer must not be picked again, so dispatch can't bump it past
	// PerPeerLimit.
	for i := 0; i < 4; i++ {
		if pid := s.pickPeer(0, nil); pid != proven.ID() {
			t.Fatalf("pick #%d=%q, want %q", i+1, pid, proven.ID())
		}
		s.stats[proven.ID()].addInflight()
	}
	if pid := s.pickPeer(0, nil); pid != "" {
		t.Fatalf("pick with proven inflight=perPeer should return empty, got %q", pid)
	}
}

func TestChunkSchedulerPickPeerProvisionalSlotsLimited(t *testing.T) {
	b := newScenario(t)
	b.target.Chunks = 5
	s := b.build()

	prov := newFakePeer("prov", "1.1.1.1", 26656)
	b.sw.peerSet.Add(prov)
	s.stats[prov.ID()] = &peerStat{provisional: true}

	// provisionalInflight = 1 in the scenario. pickPeer's
	// implementation lets dispatch overshoot by one (peer at the
	// limit gets picked, then dispatch's addInflight pushes them
	// past the limit). Reaching inflight == provisionalInflight+1
	// is the actual "no more slots" state.
	s.stats[prov.ID()].addInflight()
	s.stats[prov.ID()].addInflight()
	if pid := s.pickPeer(0, nil); pid != "" {
		t.Fatalf("pick with provisional inflight=2 should return empty, got %q", pid)
	}
}

// A verified chunk that fails to write must NOT be marked completed
// and must not bump doneCount/bytesTotal. Regression for issue #22:
// the prior code logged the write error then marked the chunk
// completed, so the fetch would "succeed" and import would later
// fail with a confusing zlib error.
func TestChunkSchedulerWriteFailureNotMarkedCompleted(t *testing.T) {
	b := newScenario(t)
	s := b.build()
	boom := errors.New("simulated ENOSPC")
	s.writeFile = func(string, []byte, os.FileMode) error { return boom }

	peer := newFakePeer("peer-A", "1.1.1.1", 26656)
	b.sw.peerSet.Add(peer)
	s.stats[peer.ID()] = &peerStat{provisional: true}
	assignInflight(s, peer.ID(), 0, time.Now())

	ev := b.chunkEvent(peer.ID(), 0, b.chunkBytes, false)
	s.onChunk(ev)

	if s.completed[0] {
		t.Fatalf("completed[0] set despite write failure")
	}
	if s.doneCount != 0 {
		t.Fatalf("doneCount=%d, want 0 (write failed)", s.doneCount)
	}
	if got := s.bytesTotal; got != 0 {
		t.Fatalf("bytesTotal=%d, want 0", got)
	}
	if s.diskFails != 1 {
		t.Fatalf("diskFails=%d, want 1", s.diskFails)
	}
	if !errors.Is(s.firstDiskErr, boom) {
		t.Fatalf("firstDiskErr=%v, want %v", s.firstDiskErr, boom)
	}
	if _, err := os.Stat(filepath.Join(b.snapDir, "chunk_00000.bin")); !os.IsNotExist(err) {
		t.Fatalf("chunk file present despite write failure: stat err = %v", err)
	}
}

// When no peer is available for retry (e.g. peer dropped), a write
// failure leaves the chunk pending so the next dispatch wave will
// pick it up.
func TestChunkSchedulerWriteFailureLeavesPendingWhenNoRetryPeer(t *testing.T) {
	b := newScenario(t)
	s := b.build()
	s.writeFile = func(string, []byte, os.FileMode) error {
		return errors.New("EIO")
	}

	peer := newFakePeer("peer-A", "1.1.1.1", 26656)
	b.sw.peerSet.Add(peer)
	s.stats[peer.ID()] = &peerStat{provisional: true}
	assignInflight(s, peer.ID(), 0, time.Now())
	// Peer disconnects between dispatch and chunk-arrival: dispatch's
	// retry from inside onChunk has nobody to reassign to.
	b.sw.peerSet.Remove(peer.ID())

	s.onChunk(b.chunkEvent(peer.ID(), 0, b.chunkBytes, false))

	if !s.pending[0] {
		t.Fatalf("pending[0] cleared even though no peer was available for retry")
	}
	if _, still := s.inflight[0]; still {
		t.Fatalf("inflight[0] retained after write failure with no retry peer")
	}
}

// Repeated write failures across retries accumulate on diskFails;
// firstDiskErr captures the original cause.
func TestChunkSchedulerWriteFailuresAccumulate(t *testing.T) {
	b := newScenario(t)
	s := b.build()
	first := errors.New("ENOSPC")
	second := errors.New("EIO")
	calls := 0
	s.writeFile = func(string, []byte, os.FileMode) error {
		calls++
		if calls == 1 {
			return first
		}
		return second
	}

	peer := newFakePeer("peer-A", "1.1.1.1", 26656)
	b.sw.peerSet.Add(peer)
	s.stats[peer.ID()] = &peerStat{}

	for i := 0; i < 2; i++ {
		s.pending[0] = true
		delete(s.inflight, 0)
		s.stats[peer.ID()].clearInflight()
		assignInflight(s, peer.ID(), 0, time.Now())
		s.onChunk(b.chunkEvent(peer.ID(), 0, b.chunkBytes, false))
	}

	if s.diskFails != 2 {
		t.Fatalf("diskFails=%d, want 2", s.diskFails)
	}
	if !errors.Is(s.firstDiskErr, first) {
		t.Fatalf("firstDiskErr=%v, want first error %v (must not be overwritten by later failures)", s.firstDiskErr, first)
	}
}

// Once diskFails crosses maxDiskFails the run loop must abort with
// an ErrDiskFailed-typed error wrapping the first cause, regardless
// of how healthy the peer set looks.
func TestChunkSchedulerRunAbortsOnDiskFailureLimit(t *testing.T) {
	b := newScenario(t)
	b.target.Chunks = 2
	s := b.build()
	s.maxDiskFails = 3
	s.pending = []bool{true, true}
	s.completed = []bool{false, false}
	s.chunkHashes = [][]byte{b.chunkHash, b.chunkHash}

	// At least one healthy peer so the alive==0 branch isn't what
	// returns first.
	peer := newFakePeer("peer-A", "1.1.1.1", 26656)
	b.sw.peerSet.Add(peer)
	s.stats[peer.ID()] = &peerStat{}

	boom := errors.New("simulated ENOSPC")
	s.diskFails = 3
	s.firstDiskErr = boom

	ctrl := make(chan statesync.Event)
	chunk := make(chan statesync.Event)
	sub := &subscription{Ctrl: ctrl, Chunk: chunk}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	bytes, err := s.run(ctx, sub)
	if err == nil {
		t.Fatalf("run returned nil error, want ErrDiskFailed")
	}
	if !errors.Is(err, ErrDiskFailed) {
		t.Fatalf("run err = %v, want ErrDiskFailed", err)
	}
	if !errors.Is(err, boom) {
		t.Fatalf("run err = %v, want chain to wrap firstDiskErr %v", err, boom)
	}
	if bytes != 0 {
		t.Fatalf("run bytes = %d, want 0 (no successful writes)", bytes)
	}
}

// The disk gate must take precedence over the all-peers-banned gate
// when both apply: disk is the more actionable diagnosis.
func TestChunkSchedulerRunDiskGateBeatsAllPeersBanned(t *testing.T) {
	b := newScenario(t)
	b.target.Chunks = 1
	s := b.build()
	s.maxDiskFails = 1
	s.diskFails = 1
	boom := errors.New("EIO")
	s.firstDiskErr = boom

	// No peers in stats — the all-peers-banned branch would fire if
	// the disk gate weren't checked first.

	ctrl := make(chan statesync.Event)
	chunk := make(chan statesync.Event)
	sub := &subscription{Ctrl: ctrl, Chunk: chunk}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := s.run(ctx, sub)
	if !errors.Is(err, ErrDiskFailed) {
		t.Fatalf("run err = %v, want ErrDiskFailed (disk gate must precede peers-banned)", err)
	}
}
