package snapfetch

import (
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"
	"time"

	cmtlog "github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/p2p"

	"github.com/zrbecker/cosmos-p2p/internal/statesync"
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
		log:         cmtlog.NewNopLogger(),
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
	if s.bytesTotal.Load() != uint64(len(b.chunkBytes)) {
		t.Fatalf("bytesTotal=%d, want %d", s.bytesTotal.Load(), len(b.chunkBytes))
	}
	// File on disk.
	written, err := os.ReadFile(filepath.Join(b.snapDir, "chunk_00000.bin"))
	if err != nil {
		t.Fatalf("chunk file not written: %v", err)
	}
	if string(written) != string(b.chunkBytes) {
		t.Fatalf("chunk file content mismatch")
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

func TestChunkSchedulerProvenPeerNeedsThreeStrikesToBan(t *testing.T) {
	b := newScenario(t)
	s := b.build()

	peer := newFakePeer("peer-A", "1.1.1.1", 26656)
	b.sw.peerSet.Add(peer)
	s.stats[peer.ID()] = &peerStat{} // proven (provisional=false)

	// Seed the first assignment; onChunk's redispatch sets up the
	// next two iterations naturally, mirroring production flow.
	assignInflight(s, peer.ID(), 0, time.Now())
	for i := 0; i < 3; i++ {
		ev := b.chunkEvent(peer.ID(), 0, nil, true) // missing
		s.onChunk(ev)
	}

	if !s.stats[peer.ID()].banned {
		t.Fatalf("proven peer not banned after 3 strikes")
	}
	if b.mgr.banReason(peer.ID()) == "" {
		t.Fatalf("manager.Ban not invoked")
	}
	// After ban, the failed assignment must be released, not leaked.
	if _, still := s.inflight[0]; still {
		t.Fatalf("inflight[0] leaked after final strike")
	}
	if !s.pending[0] {
		t.Fatalf("chunk not re-pending after final strike")
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
		if pid := s.pickPeer(); pid != good.ID() {
			t.Fatalf("pickPeer=%q, want %q (banned should never be picked)", pid, good.ID())
		}
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
	if pid := s.pickPeer(); pid != "" {
		t.Fatalf("pick with provisional inflight=2 should return empty, got %q", pid)
	}
}
