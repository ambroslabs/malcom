package snapfetch

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/p2p/conn"
	pexcb "github.com/cometbft/cometbft/p2p/pex"

	cmtlog "github.com/cometbft/cometbft/libs/log"
)

// fakeWatchSwitch satisfies watchSwitch.
type fakeWatchSwitch struct {
	peerSet *fakePeerSet

	mu      sync.Mutex
	stopped []p2p.ID
}

func newFakeWatchSwitch() *fakeWatchSwitch {
	return &fakeWatchSwitch{peerSet: newFakePeerSet()}
}

func (s *fakeWatchSwitch) Peers() p2p.IPeerSet { return s.peerSet }

func (s *fakeWatchSwitch) StopPeerGracefully(peer p2p.Peer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopped = append(s.stopped, peer.ID())
	s.peerSet.Remove(peer.ID())
}

func (s *fakeWatchSwitch) wasStopped(pid p2p.ID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, p := range s.stopped {
		if p == pid {
			return true
		}
	}
	return false
}

// fakeWatchBook is a pexcb.AddrBook with just MarkBad recording.
type fakeWatchBook struct {
	mu              sync.Mutex
	markedBad       map[string]int
	markBadDuration map[string]time.Duration // last TTL forwarded to MarkBad
}

func newFakeWatchBook() *fakeWatchBook {
	return &fakeWatchBook{
		markedBad:       map[string]int{},
		markBadDuration: map[string]time.Duration{},
	}
}

func (b *fakeWatchBook) MarkBad(addr *p2p.NetAddress, d time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.markedBad[addr.String()]++
	b.markBadDuration[addr.String()] = d
}

// pexcb.AddrBook surface — defaults.
func (b *fakeWatchBook) Save()                                            {}
func (b *fakeWatchBook) AddOurAddress(*p2p.NetAddress)                    {}
func (b *fakeWatchBook) OurAddress(*p2p.NetAddress) bool                  { return false }
func (b *fakeWatchBook) AddPrivateIDs([]string)                           {}
func (b *fakeWatchBook) AddAddress(*p2p.NetAddress, *p2p.NetAddress) error { return nil }
func (b *fakeWatchBook) RemoveAddress(*p2p.NetAddress)                    {}
func (b *fakeWatchBook) NeedMoreAddrs() bool                              { return false }
func (b *fakeWatchBook) Empty() bool                                      { return false }
func (b *fakeWatchBook) PickAddress(int) *p2p.NetAddress                  { return nil }
func (b *fakeWatchBook) MarkGood(p2p.ID)                                  {}
func (b *fakeWatchBook) MarkAttempt(*p2p.NetAddress)                      {}
func (b *fakeWatchBook) IsGood(*p2p.NetAddress) bool                      { return false }
func (b *fakeWatchBook) IsBanned(*p2p.NetAddress) bool                    { return false }
func (b *fakeWatchBook) HasAddress(*p2p.NetAddress) bool                  { return false }
func (b *fakeWatchBook) GetSelection() []*p2p.NetAddress                  { return nil }
func (b *fakeWatchBook) GetSelectionWithBias(int) []*p2p.NetAddress       { return nil }
func (b *fakeWatchBook) ListOfKnownAddresses() []*p2p.NetAddress          { return nil }
func (b *fakeWatchBook) Size() int                                       { return 0 }
func (b *fakeWatchBook) ReinstateBadPeers()                              {}
func (b *fakeWatchBook) Start() error                                    { return nil }
func (b *fakeWatchBook) OnStart() error                                  { return nil }
func (b *fakeWatchBook) Stop() error                                     { return nil }
func (b *fakeWatchBook) OnStop()                                         {}
func (b *fakeWatchBook) Reset() error                                    { return nil }
func (b *fakeWatchBook) OnReset() error                                  { return nil }
func (b *fakeWatchBook) IsRunning() bool                                 { return true }
func (b *fakeWatchBook) Quit() <-chan struct{}                           { return nil }
func (b *fakeWatchBook) String() string                                  { return "fakebook" }
func (b *fakeWatchBook) SetLogger(cmtlog.Logger)                         {}

var _ pexcb.AddrBook = (*fakeWatchBook)(nil)

// fakeWatchManager satisfies watchManager.
type fakeWatchManager struct {
	mu     sync.Mutex
	banned map[p2p.ID]string
}

func newFakeWatchManager() *fakeWatchManager {
	return &fakeWatchManager{banned: map[p2p.ID]string{}}
}

func (m *fakeWatchManager) Ban(pid p2p.ID, reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.banned[pid] = reason
}

func (m *fakeWatchManager) banReason(pid p2p.ID) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.banned[pid]
}

// peerWatchScenario builds a peerWatch with sensible defaults for tests.
type peerWatchScenario struct {
	sw   *fakeWatchSwitch
	book *fakeWatchBook
	mgr  *fakeWatchManager
}

func newPeerWatchScenario(t *testing.T) *peerWatchScenario {
	return &peerWatchScenario{
		sw:   newFakeWatchSwitch(),
		book: newFakeWatchBook(),
		mgr:  newFakeWatchManager(),
	}
}

func (b *peerWatchScenario) build(minHeight uint64, grace time.Duration, requireChannel bool) *peerWatch {
	ctx := context.Background()
	return newPeerWatch(ctx, b.sw, b.book, b.mgr, minHeight, grace, time.Hour, requireChannel)
}

// fakePeer's NodeInfo by default has channels [0x60, 0x61], satisfying
// the snapshot-channel filter. Override below for the no-channel test.
type fakePeerNoSnap struct{ *fakePeer }

func (p fakePeerNoSnap) NodeInfo() p2p.NodeInfo {
	return p2p.DefaultNodeInfo{DefaultNodeID: p.id, Channels: []byte{0x40}}
}

func (p fakePeerNoSnap) Status() conn.ConnectionStatus { return conn.ConnectionStatus{} }

// --- tests ---

func TestPeerWatchOnConnectRecordsFirstSeen(t *testing.T) {
	b := newPeerWatchScenario(t)
	w := b.build(100, time.Second, false)

	peer := newFakePeer("peer-A", "1.1.1.1", 26656)
	b.sw.peerSet.Add(peer)
	w.onConnect(peer.ID())

	w.mu.Lock()
	defer w.mu.Unlock()
	if _, ok := w.firstSeen[peer.ID()]; !ok {
		t.Fatalf("firstSeen not set on connect")
	}
}

func TestPeerWatchTickRequireChannelMissingBans(t *testing.T) {
	b := newPeerWatchScenario(t)
	w := b.build(100, time.Second, true) // requireStateSyncChannel = true

	// Peer with NodeInfo lacking the snapshot channel.
	peerNoSnap := fakePeerNoSnap{fakePeer: newFakePeer("peer-A", "1.1.1.1", 26656)}
	b.sw.peerSet.Add(peerNoSnap)

	w.onConnect(peerNoSnap.ID())
	if b.sw.wasStopped(peerNoSnap.ID()) {
		t.Fatalf("peer Stopped on connect; channel filter must defer to tick so PEX has time to reply")
	}

	w.tick()

	if !b.sw.wasStopped(peerNoSnap.ID()) {
		t.Fatalf("peer without state-sync channel not Stopped on tick")
	}
	if b.mgr.banReason(peerNoSnap.ID()) != "no state-sync channel" {
		t.Fatalf("manager.Ban reason=%q, want 'no state-sync channel'",
			b.mgr.banReason(peerNoSnap.ID()))
	}
	addrStr := peerNoSnap.SocketAddr().String()
	if b.book.markedBad[addrStr] == 0 {
		t.Fatalf("book.MarkBad not invoked")
	}
	if got := b.book.markBadDuration[addrStr]; got != time.Hour {
		t.Fatalf("book.MarkBad duration=%s, want banDuration=%s", got, time.Hour)
	}
}

func TestPeerWatchOnDisconnectClearsFirstSeen(t *testing.T) {
	b := newPeerWatchScenario(t)
	w := b.build(100, time.Second, false)

	peer := newFakePeer("peer-A", "1.1.1.1", 26656)
	b.sw.peerSet.Add(peer)
	w.onConnect(peer.ID())
	w.onDisconnect(peer.ID())

	w.mu.Lock()
	defer w.mu.Unlock()
	if _, ok := w.firstSeen[peer.ID()]; ok {
		t.Fatalf("firstSeen not cleared on disconnect")
	}
}

func TestPeerWatchTickEvictsAfterGrace(t *testing.T) {
	b := newPeerWatchScenario(t)
	// grace=0 → any peer without the useful flag expires on next tick.
	// Avoids wall-clock flakiness on loaded CI runners.
	w := b.build(100, 0, false)

	peer := newFakePeer("peer-A", "1.1.1.1", 26656)
	b.sw.peerSet.Add(peer)
	w.onConnect(peer.ID())

	w.tick()

	if !b.sw.wasStopped(peer.ID()) {
		t.Fatalf("peer not evicted after grace expired without useful flag")
	}
	if b.mgr.banReason(peer.ID()) != "no useful offer in window" {
		t.Fatalf("manager.Ban reason=%q, want 'no useful offer in window'",
			b.mgr.banReason(peer.ID()))
	}
}

func TestPeerWatchUsefulPeerImmuneToTick(t *testing.T) {
	b := newPeerWatchScenario(t)
	w := b.build(100, 0, false)

	peer := newFakePeer("peer-A", "1.1.1.1", 26656)
	b.sw.peerSet.Add(peer)
	w.onConnect(peer.ID())
	w.markUseful(peer.ID())

	w.tick()

	if b.sw.wasStopped(peer.ID()) {
		t.Fatalf("useful peer was evicted")
	}
}

func TestPeerWatchMinHeightZeroDisablesChurning(t *testing.T) {
	b := newPeerWatchScenario(t)
	w := b.build(0, 0, false) // minHeight = 0 → tick() short-circuits regardless of grace

	peer := newFakePeer("peer-A", "1.1.1.1", 26656)
	b.sw.peerSet.Add(peer)
	w.onConnect(peer.ID())

	w.tick()

	if b.sw.wasStopped(peer.ID()) {
		t.Fatalf("peer evicted with minHeight=0 (churning should be off)")
	}
}

// If a Removed event was dropped (reactor.Out full), firstSeen would
// leak forever. tick() must reconcile against sw.Peers() and clear
// entries whose peer is gone.
func TestPeerWatchTickReclaimsFirstSeenForGonePeers(t *testing.T) {
	b := newPeerWatchScenario(t)
	w := b.build(100, time.Hour, false) // long grace so eviction-by-grace can't fire

	peer := newFakePeer("peer-A", "1.1.1.1", 26656)
	b.sw.peerSet.Add(peer)
	w.onConnect(peer.ID())

	// Simulate a missed Removed event: peer leaves the set without
	// onDisconnect being called.
	b.sw.peerSet.Remove(peer.ID())

	w.tick()

	w.mu.Lock()
	defer w.mu.Unlock()
	if _, ok := w.firstSeen[peer.ID()]; ok {
		t.Fatalf("firstSeen still has entry for disconnected peer; tick didn't reconcile")
	}
}

// markOutOfRange + tick → bench, even before grace expires.
func TestPeerWatchOutOfRangeOfferBenchesImmediately(t *testing.T) {
	b := newPeerWatchScenario(t)
	w := b.build(100, time.Hour, false) // long grace — bench should fire on out-of-range, not grace.

	peer := newFakePeer("peer-A", "1.1.1.1", 26656)
	b.sw.peerSet.Add(peer)
	w.onConnect(peer.ID())
	w.markOutOfRange(peer.ID())

	w.tick()

	if !b.sw.wasStopped(peer.ID()) {
		t.Fatalf("peer with out-of-range offer was not evicted on tick")
	}
	if b.mgr.banReason(peer.ID()) != "offered only out-of-range snapshot" {
		t.Fatalf("manager.Ban reason=%q, want 'offered only out-of-range snapshot'",
			b.mgr.banReason(peer.ID()))
	}
}

// Out-of-range followed by an in-range offer: peer is useful, no bench.
func TestPeerWatchInRangeOfferOverridesOutOfRange(t *testing.T) {
	b := newPeerWatchScenario(t)
	w := b.build(100, time.Hour, false)

	peer := newFakePeer("peer-A", "1.1.1.1", 26656)
	b.sw.peerSet.Add(peer)
	w.onConnect(peer.ID())
	w.markOutOfRange(peer.ID())
	w.markUseful(peer.ID())

	w.tick()

	if b.sw.wasStopped(peer.ID()) {
		t.Fatalf("peer with both out-of-range AND in-range offers was evicted; useful flag should win")
	}
}

func TestPeerWatchIsBannedAfterEviction(t *testing.T) {
	b := newPeerWatchScenario(t)
	w := b.build(100, 0, false)

	peer := newFakePeer("peer-A", "1.1.1.1", 26656)
	b.sw.peerSet.Add(peer)
	w.onConnect(peer.ID())

	w.tick()

	if !w.isBanned(peer.ID()) {
		t.Fatalf("peer not flagged in `banned` after tick eviction")
	}
}
