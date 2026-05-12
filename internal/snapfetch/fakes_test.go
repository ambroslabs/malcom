package snapfetch

// Test fakes for chunkScheduler and peerWatch tests. Keep minimal —
// only the interface methods we actually call.

import (
	"net"
	"sync"

	cmtlog "github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/p2p/conn"

	"github.com/ambroslabs/malcom/internal/connect"
	"github.com/ambroslabs/malcom/internal/statesync"
)

// fakePeer satisfies p2p.Peer just enough for our tests.
type fakePeer struct {
	id   p2p.ID
	addr *p2p.NetAddress
	info p2p.NodeInfo
}

func newFakePeer(id string, host string, port uint16) *fakePeer {
	pid := p2p.ID(id)
	na := p2p.NewNetAddressIPPort(net.ParseIP(host), port)
	na.ID = pid
	return &fakePeer{
		id:   pid,
		addr: na,
		info: p2p.DefaultNodeInfo{
			DefaultNodeID: pid,
			Channels:      []byte{statesync.SnapshotChannel, statesync.ChunkChannel},
		},
	}
}

func (p *fakePeer) ID() p2p.ID                    { return p.id }
func (p *fakePeer) RemoteIP() net.IP              { return p.addr.IP }
func (p *fakePeer) RemoteAddr() net.Addr          { return nil } // unused by code under test; nil-deref will panic loudly if that changes

func (p *fakePeer) IsOutbound() bool              { return true }
func (p *fakePeer) IsPersistent() bool            { return false }
func (p *fakePeer) CloseConn() error              { return nil }
func (p *fakePeer) NodeInfo() p2p.NodeInfo        { return p.info }
func (p *fakePeer) Status() conn.ConnectionStatus { return conn.ConnectionStatus{} }
func (p *fakePeer) SocketAddr() *p2p.NetAddress   { return p.addr }
func (p *fakePeer) Send(p2p.Envelope) bool        { return true }
func (p *fakePeer) TrySend(p2p.Envelope) bool     { return true }
func (p *fakePeer) Set(string, interface{})       {}
func (p *fakePeer) Get(string) interface{}        { return nil }
func (p *fakePeer) SetRemovalFailed()             {}
func (p *fakePeer) GetRemovalFailed() bool        { return false }
func (p *fakePeer) FlushStop()                    {}

// service.Service surface — minimal stubs.
func (p *fakePeer) Start() error            { return nil }
func (p *fakePeer) OnStart() error          { return nil }
func (p *fakePeer) Stop() error             { return nil }
func (p *fakePeer) OnStop()                 {}
func (p *fakePeer) Reset() error            { return nil }
func (p *fakePeer) OnReset() error          { return nil }
func (p *fakePeer) IsRunning() bool         { return true }
func (p *fakePeer) Quit() <-chan struct{}   { return nil }
func (p *fakePeer) String() string          { return string(p.id) }
func (p *fakePeer) SetLogger(cmtlog.Logger) {}

var _ p2p.Peer = (*fakePeer)(nil)

// fakePeerSet satisfies p2p.IPeerSet — supports Add/Remove for test
// setup and Get/List/Size for the production code.
type fakePeerSet struct {
	mu    sync.Mutex
	peers map[p2p.ID]p2p.Peer
}

func newFakePeerSet() *fakePeerSet {
	return &fakePeerSet{peers: map[p2p.ID]p2p.Peer{}}
}

func (s *fakePeerSet) Add(p p2p.Peer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.peers[p.ID()] = p
}

func (s *fakePeerSet) Remove(id p2p.ID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.peers, id)
}

func (s *fakePeerSet) Has(id p2p.ID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.peers[id]
	return ok
}

func (s *fakePeerSet) HasIP(net.IP) bool { return false } // no caller exercises this; implement against s.peers if a test needs it


func (s *fakePeerSet) Get(id p2p.ID) p2p.Peer {
	s.mu.Lock()
	defer s.mu.Unlock()
	if p, ok := s.peers[id]; ok {
		return p
	}
	return nil
}

func (s *fakePeerSet) List() []p2p.Peer {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]p2p.Peer, 0, len(s.peers))
	for _, p := range s.peers {
		out = append(out, p)
	}
	return out
}

func (s *fakePeerSet) Size() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.peers)
}

var _ p2p.IPeerSet = (*fakePeerSet)(nil)

// fakeSchedulerSwitch satisfies schedulerSwitch.
type fakeSchedulerSwitch struct {
	peerSet *fakePeerSet
	out     int
	in      int
	dialing int
}

func newFakeSchedulerSwitch() *fakeSchedulerSwitch {
	return &fakeSchedulerSwitch{peerSet: newFakePeerSet()}
}

func (s *fakeSchedulerSwitch) Peers() p2p.IPeerSet { return s.peerSet }
func (s *fakeSchedulerSwitch) NumPeers() (int, int, int) {
	return s.peerSet.Size(), s.in, s.dialing
}

// fakeReactor satisfies schedulerReactor.
type fakeReactor struct {
	mu       sync.Mutex
	requests []chunkRequest
	// requestChunkOK is the value RequestChunk returns: true for the
	// happy path, false to simulate a full send queue.
	requestChunkOK bool
}

type chunkRequest struct {
	PeerID p2p.ID
	Height uint64
	Format uint32
	Index  uint32
}

func newFakeReactor() *fakeReactor {
	return &fakeReactor{requestChunkOK: true}
}

func (r *fakeReactor) RequestChunk(peer p2p.Peer, height uint64, format, index uint32) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.requests = append(r.requests, chunkRequest{
		PeerID: peer.ID(), Height: height, Format: format, Index: index,
	})
	return r.requestChunkOK
}

func (r *fakeReactor) Drops() (ctrl, chunk int64) { return 0, 0 }

func (r *fakeReactor) sentTo(pid p2p.ID, idx uint32) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, q := range r.requests {
		if q.PeerID == pid && q.Index == idx {
			return true
		}
	}
	return false
}

func (r *fakeReactor) numRequests() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.requests)
}

func (r *fakeReactor) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.requests = nil
}

// fakeManager satisfies schedulerManager. Records every call.
type fakeManager struct {
	mu     sync.Mutex
	pinned map[p2p.ID]string
	banned map[p2p.ID]string // pid → reason
}

func newFakeManager() *fakeManager {
	return &fakeManager{
		pinned: map[p2p.ID]string{},
		banned: map[p2p.ID]string{},
	}
}

func (m *fakeManager) Pin(pid p2p.ID, addr string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pinned[pid] = addr
}

func (m *fakeManager) Ban(pid p2p.ID, reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.banned[pid] = reason
}

func (m *fakeManager) IsBanned(pid p2p.ID) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.banned[pid]
	return ok
}

func (m *fakeManager) Stats() connect.Stats {
	m.mu.Lock()
	defer m.mu.Unlock()
	return connect.Stats{Pinned: len(m.pinned), Banned: len(m.banned)}
}

// BanReason satisfies the schedulerManager interface. Delegates to the
// existing per-test banReason() helper so tests can keep using the
// short form.
func (m *fakeManager) BanReason(pid p2p.ID) string {
	return m.banReason(pid)
}

func (m *fakeManager) banReason(pid p2p.ID) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.banned[pid]
}
