package connect

import (
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	cmtlog "github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/p2p/conn"

	"github.com/zrbecker/cosmos-p2p/internal/helpers/addrbook"
)

// --- fakes ---

type fakePeer struct {
	id   p2p.ID
	addr *p2p.NetAddress
}

func newFakePeer(id, host string, port uint16) *fakePeer {
	pid := p2p.ID(id)
	na := p2p.NewNetAddressIPPort(net.ParseIP(host), port)
	na.ID = pid
	return &fakePeer{id: pid, addr: na}
}

func (p *fakePeer) ID() p2p.ID                    { return p.id }
func (p *fakePeer) RemoteIP() net.IP              { return p.addr.IP }
func (p *fakePeer) RemoteAddr() net.Addr          { return nil }
func (p *fakePeer) IsOutbound() bool              { return true }
func (p *fakePeer) IsPersistent() bool            { return false }
func (p *fakePeer) CloseConn() error              { return nil }
func (p *fakePeer) NodeInfo() p2p.NodeInfo        { return p2p.DefaultNodeInfo{DefaultNodeID: p.id} }
func (p *fakePeer) Status() conn.ConnectionStatus { return conn.ConnectionStatus{} }
func (p *fakePeer) SocketAddr() *p2p.NetAddress   { return p.addr }
func (p *fakePeer) Send(p2p.Envelope) bool        { return true }
func (p *fakePeer) TrySend(p2p.Envelope) bool     { return true }
func (p *fakePeer) Set(string, interface{})       {}
func (p *fakePeer) Get(string) interface{}        { return nil }
func (p *fakePeer) SetRemovalFailed()             {}
func (p *fakePeer) GetRemovalFailed() bool        { return false }
func (p *fakePeer) FlushStop()                    {}
func (p *fakePeer) Start() error                  { return nil }
func (p *fakePeer) OnStart() error                { return nil }
func (p *fakePeer) Stop() error                   { return nil }
func (p *fakePeer) OnStop()                       {}
func (p *fakePeer) Reset() error                  { return nil }
func (p *fakePeer) OnReset() error                { return nil }
func (p *fakePeer) IsRunning() bool               { return true }
func (p *fakePeer) Quit() <-chan struct{}         { return nil }
func (p *fakePeer) String() string                { return string(p.id) }
func (p *fakePeer) SetLogger(cmtlog.Logger)       {}

type fakePeerSet struct {
	mu    sync.Mutex
	peers map[p2p.ID]p2p.Peer
}

func newFakePeerSet() *fakePeerSet {
	return &fakePeerSet{peers: map[p2p.ID]p2p.Peer{}}
}
func (s *fakePeerSet) add(p p2p.Peer) {
	s.mu.Lock()
	s.peers[p.ID()] = p
	s.mu.Unlock()
}
func (s *fakePeerSet) Has(id p2p.ID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.peers[id]
	return ok
}
func (s *fakePeerSet) HasIP(net.IP) bool { return false }
func (s *fakePeerSet) Get(id p2p.ID) p2p.Peer {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.peers[id]
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

// fakeSwitch satisfies managerSwitch. Records DialPeerWithAddress calls.
type fakeSwitch struct {
	peerSet *fakePeerSet

	mu      sync.Mutex
	dials   []*p2p.NetAddress
	dialErr error // returned by DialPeerWithAddress
}

func newFakeSwitch() *fakeSwitch {
	return &fakeSwitch{peerSet: newFakePeerSet()}
}

func (s *fakeSwitch) Peers() p2p.IPeerSet { return s.peerSet }
func (s *fakeSwitch) NumPeers() (int, int, int) {
	return s.peerSet.Size(), 0, 0
}
func (s *fakeSwitch) IsDialingOrExistingAddress(*p2p.NetAddress) bool {
	return false
}
func (s *fakeSwitch) DialPeerWithAddress(addr *p2p.NetAddress) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dials = append(s.dials, addr)
	return s.dialErr
}
func (s *fakeSwitch) dialCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.dials)
}
func (s *fakeSwitch) waitForDials(t *testing.T, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if s.dialCount() >= want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("expected %d dials, got %d within %s", want, s.dialCount(), timeout)
}

// fakeBook is a minimal pexcb.AddrBook stand-in. Only the methods the
// Manager calls during dial/auto-ban paths are implemented.
type fakeBook struct {
	mu              sync.Mutex
	picks           []*p2p.NetAddress
	pickQueue       []*p2p.NetAddress // returned in order
	markedBad       map[string]int
	markedAttempt   map[string]int
	removed         map[string]int
}

func newFakeBook() *fakeBook {
	return &fakeBook{
		markedBad:     map[string]int{},
		markedAttempt: map[string]int{},
		removed:       map[string]int{},
	}
}

func (b *fakeBook) PickAddress(int) *p2p.NetAddress {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.pickQueue) == 0 {
		return nil
	}
	pick := b.pickQueue[0]
	b.pickQueue = b.pickQueue[1:]
	b.picks = append(b.picks, pick)
	return pick
}

func (b *fakeBook) MarkBad(addr *p2p.NetAddress, _ time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.markedBad[addr.String()]++
}

func (b *fakeBook) MarkAttempt(addr *p2p.NetAddress) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.markedAttempt[addr.String()]++
}

func (b *fakeBook) RemoveAddress(addr *p2p.NetAddress) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.removed[addr.String()]++
}

// pexcb.AddrBook has many other methods. We define them as no-ops.
func (b *fakeBook) Save()                                            {}
func (b *fakeBook) AddOurAddress(*p2p.NetAddress)                    {}
func (b *fakeBook) OurAddress(*p2p.NetAddress) bool                  { return false }
func (b *fakeBook) AddPrivateIDs([]string)                           {}
func (b *fakeBook) AddAddress(*p2p.NetAddress, *p2p.NetAddress) error { return nil }
func (b *fakeBook) NeedMoreAddrs() bool                              { return false }
func (b *fakeBook) Empty() bool                                      { return false }
func (b *fakeBook) MarkGood(p2p.ID)                                  {}
func (b *fakeBook) IsGood(*p2p.NetAddress) bool                      { return false }
func (b *fakeBook) IsBanned(*p2p.NetAddress) bool                    { return false }
func (b *fakeBook) HasAddress(*p2p.NetAddress) bool                  { return false }
func (b *fakeBook) GetSelection() []*p2p.NetAddress                  { return nil }
func (b *fakeBook) GetSelectionWithBias(int) []*p2p.NetAddress       { return nil }
func (b *fakeBook) ListOfKnownAddresses() []*p2p.NetAddress          { return nil }
func (b *fakeBook) Size() int                                        { return 0 }
func (b *fakeBook) ReinstateBadPeers()                               {}

// service.Service surface — all no-ops.
func (b *fakeBook) Start() error            { return nil }
func (b *fakeBook) OnStart() error          { return nil }
func (b *fakeBook) Stop() error             { return nil }
func (b *fakeBook) OnStop()                 {}
func (b *fakeBook) Reset() error            { return nil }
func (b *fakeBook) OnReset() error          { return nil }
func (b *fakeBook) IsRunning() bool         { return true }
func (b *fakeBook) Quit() <-chan struct{}   { return nil }
func (b *fakeBook) String() string          { return "fakebook" }
func (b *fakeBook) SetLogger(cmtlog.Logger) {}

// fakeBanlist captures Add calls.
type fakeBanlist struct {
	mu     sync.Mutex
	added  map[string]string // addr → reason
}

func newFakeBanlist() *fakeBanlist {
	return &fakeBanlist{added: map[string]string{}}
}

func (l *fakeBanlist) Add(addr, reason string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.added[addr] = reason
}

// --- helpers ---

// newManagerForTest builds a Manager but does NOT start the dial loop
// goroutine. Tests call tick() directly for deterministic stepping.
func newManagerForTest(t *testing.T, sw managerSwitch, c Config) *Manager {
	t.Helper()
	c.Switch = sw
	c.defaults()
	m := &Manager{
		cfg:        c,
		log:        slog.New(slog.DiscardHandler),
		pinned:     map[p2p.ID]string{},
		backoffs:   map[p2p.ID]*peerBackoff{},
		banned:     map[p2p.ID]bool{},
		banReasons: map[p2p.ID]string{},
		dialFail:   map[string]int{},
		pool:       append([]addrbook.PeerAddr(nil), c.Pool...),
		cancel:     func() {}, // no goroutine started
	}
	return m
}

// --- tests ---

func TestManagerPinnedDisconnectedPeerDials(t *testing.T) {
	sw := newFakeSwitch()
	m := newManagerForTest(t, sw, Config{
		WarmTarget: 0, // no warm-fill
		Backoff:    100 * time.Millisecond,
	})

	pid := p2p.ID("0123456789abcdef0123456789abcdef01234567")
	m.Pin(pid, string(pid)+"@1.2.3.4:26656")

	// Peer is not in the switch (disconnected). tick() should fire a dial.
	m.tick()

	// Dial happens in a goroutine; wait briefly.
	sw.waitForDials(t, 1, 500*time.Millisecond)
	if got := sw.dials[0].ID; got != pid {
		t.Fatalf("dial peer=%q, want %q", got, pid)
	}
}

func TestManagerPinnedConnectedPeerSkipped(t *testing.T) {
	sw := newFakeSwitch()
	pid := p2p.ID("0123456789abcdef0123456789abcdef01234567")
	peer := newFakePeer(string(pid), "1.2.3.4", 26656)
	sw.peerSet.add(peer)

	m := newManagerForTest(t, sw, Config{
		WarmTarget: 0,
		Backoff:    100 * time.Millisecond,
	})
	m.Pin(pid, string(pid)+"@1.2.3.4:26656")

	// The connected-peer skip in tick() runs before any dial goroutine
	// is spawned, so dialCount is authoritative the moment tick returns.
	m.tick()

	if sw.dialCount() != 0 {
		t.Fatalf("connected peer was dialed; want skip")
	}
}

// A peer that connects briefly (less than StabilityWindow) and then
// disconnects must NOT have its backoff schedule wiped. Without this,
// fast-flappers (EOF in <RefreshTick) could keep redialing without
// ever accumulating exponential backoff or hitting MaxRedials.
func TestManagerFastFlapperKeepsBackoff(t *testing.T) {
	sw := newFakeSwitch()
	pid := p2p.ID("0123456789abcdef0123456789abcdef01234567")
	peer := newFakePeer(string(pid), "1.2.3.4", 26656)

	m := newManagerForTest(t, sw, Config{
		WarmTarget:      0,
		Backoff:         100 * time.Millisecond,
		StabilityWindow: time.Hour, // long window so we never cross it in this test
	})
	m.Pin(pid, string(pid)+"@1.2.3.4:26656")

	// Seed an existing backoff schedule (peer has already disconnected
	// once and is waiting for the next dial).
	m.mu.Lock()
	m.backoffs[pid] = &peerBackoff{disconnects: 2, nextDialAt: time.Now().Add(time.Hour)}
	m.mu.Unlock()

	// Peer briefly appears in the switch — what fast-flappers look like
	// to the poll-based tick.
	sw.peerSet.add(peer)
	m.tick()

	// Backoff schedule must still be intact. Without the stability
	// window, the old code would have deleted it here.
	m.mu.Lock()
	bo, ok := m.backoffs[pid]
	m.mu.Unlock()
	if !ok {
		t.Fatalf("backoff schedule wiped on first connected observation; want preserved until StabilityWindow elapses")
	}
	if bo.disconnects != 2 {
		t.Fatalf("disconnects=%d, want 2 (schedule should be preserved)", bo.disconnects)
	}
	if bo.connectedSince.IsZero() {
		t.Fatalf("connectedSince not set on first connected observation")
	}
}

// A peer that has been continuously connected for >= StabilityWindow
// SHOULD have its backoff schedule wiped. This is what allows a peer
// that survived a flap to reclaim a fresh redial budget.
func TestManagerStableConnectionWipesBackoff(t *testing.T) {
	sw := newFakeSwitch()
	pid := p2p.ID("0123456789abcdef0123456789abcdef01234567")
	peer := newFakePeer(string(pid), "1.2.3.4", 26656)

	m := newManagerForTest(t, sw, Config{
		WarmTarget:      0,
		Backoff:         100 * time.Millisecond,
		StabilityWindow: 10 * time.Millisecond, // short for test speed
	})
	m.Pin(pid, string(pid)+"@1.2.3.4:26656")

	// Seed a backoff schedule and a connectedSince well past the window.
	m.mu.Lock()
	m.backoffs[pid] = &peerBackoff{
		disconnects:    3,
		connectedSince: time.Now().Add(-time.Hour),
	}
	m.mu.Unlock()

	sw.peerSet.add(peer)
	m.tick()

	m.mu.Lock()
	_, ok := m.backoffs[pid]
	m.mu.Unlock()
	if ok {
		t.Fatalf("backoff schedule preserved after stability window elapsed; want wiped")
	}
}

func TestManagerMaxRedialsAutoBans(t *testing.T) {
	sw := newFakeSwitch()
	book := newFakeBook()
	bans := newFakeBanlist()

	pid := p2p.ID("0123456789abcdef0123456789abcdef01234567")
	addr := string(pid) + "@1.2.3.4:26656"

	m := newManagerForTest(t, sw, Config{
		Book:        book,
		Banlist:     bans,
		WarmTarget:  0,
		MaxRedials:  2,
		BanDuration: time.Hour,
	})
	m.Pin(pid, addr)

	// Bypass the natural disconnect counter to pin the threshold check
	// directly. Driving disconnects up via repeated ticks would also work
	// but couples the test to backoff-timer behavior we don't care about
	// here.
	m.mu.Lock()
	m.backoffs[pid] = &peerBackoff{disconnects: 2}
	m.mu.Unlock()

	m.tick()

	// Auto-banned in manager.
	if !m.IsBanned(pid) {
		t.Fatalf("pinned peer not auto-banned after MaxRedials")
	}
	// MarkBad called on book.
	if book.markedBad[addr] == 0 {
		t.Fatalf("book.MarkBad not invoked on auto-ban")
	}
}

// BanReason returns the reason an explicit Ban() recorded.
func TestManagerBanReasonRecordsExplicitReason(t *testing.T) {
	sw := newFakeSwitch()
	m := newManagerForTest(t, sw, Config{WarmTarget: 0})

	pid := p2p.ID("0123456789abcdef0123456789abcdef01234567")
	m.Ban(pid, "chunk hash mismatch")

	if got := m.BanReason(pid); got != "chunk hash mismatch" {
		t.Fatalf("BanReason=%q, want 'chunk hash mismatch'", got)
	}
	// Unbanned peer returns empty.
	other := p2p.ID("ffffffffffffffffffffffffffffffffffffffff")
	if got := m.BanReason(other); got != "" {
		t.Fatalf("BanReason for unbanned peer=%q, want empty", got)
	}
}

// BanReason returns "max-redials" for peers auto-banned via the
// MaxRedials path in tick().
func TestManagerBanReasonMaxRedialsPath(t *testing.T) {
	sw := newFakeSwitch()
	book := newFakeBook()
	bans := newFakeBanlist()

	pid := p2p.ID("0123456789abcdef0123456789abcdef01234567")
	addr := string(pid) + "@1.2.3.4:26656"

	m := newManagerForTest(t, sw, Config{
		Book:        book,
		Banlist:     bans,
		WarmTarget:  0,
		MaxRedials:  2,
		BanDuration: time.Hour,
	})
	m.Pin(pid, addr)
	m.mu.Lock()
	m.backoffs[pid] = &peerBackoff{disconnects: 2}
	m.mu.Unlock()

	m.tick()

	if got := m.BanReason(pid); got != "max-redials" {
		t.Fatalf("BanReason after auto-ban=%q, want 'max-redials'", got)
	}
}

func TestManagerWarmFillDrawsFromPool(t *testing.T) {
	sw := newFakeSwitch()
	pool := []addrbook.PeerAddr{
		{Addr: "0123456789abcdef0123456789abcdef01234567@1.1.1.1:26656", Source: addrbook.SourceBootstrap},
		{Addr: "abcdef0123456789abcdef0123456789abcdef01@2.2.2.2:26656", Source: addrbook.SourceBootstrap},
	}

	m := newManagerForTest(t, sw, Config{
		Pool:       pool,
		WarmTarget: 5, // we want 5 connected; we have 0
		DialBatch:  10,
	})

	m.tick()
	sw.waitForDials(t, 2, 500*time.Millisecond)
}

// BookDisabled: warm-fill must NOT draw from the book even when the
// pool is empty. Curated-peers mode locks dialing to bootstrap_peers.
func TestManagerWarmFillBookDisabledIgnoresBook(t *testing.T) {
	sw := newFakeSwitch()
	book := newFakeBook()
	// Seed the book with an address that PickAddress would otherwise
	// hand out.
	addr, _ := p2p.NewNetAddressString("0123456789abcdef0123456789abcdef01234567@7.7.7.7:26656")
	book.pickQueue = append(book.pickQueue, addr)

	m := newManagerForTest(t, sw, Config{
		Book:         book,
		WarmTarget:   8,
		DialBatch:    4,
		BookDisabled: true,
		// No Pool: forces the only candidate source to be the (now-disabled) book.
	})

	m.tick()

	if sw.dialCount() != 0 {
		t.Fatalf("BookDisabled manager dialed %d addresses; want 0", sw.dialCount())
	}
	if len(book.picks) != 0 {
		t.Fatalf("BookDisabled manager called PickAddress %d times; want 0", len(book.picks))
	}
}

func TestManagerWarmFillFallsBackToBookWhenPoolEmpty(t *testing.T) {
	sw := newFakeSwitch()
	book := newFakeBook()

	pickAddr := p2p.NewNetAddressIPPort(net.ParseIP("3.3.3.3"), 26656)
	pickAddr.ID = p2p.ID("0123456789abcdef0123456789abcdef01234567")
	book.pickQueue = append(book.pickQueue, pickAddr)

	m := newManagerForTest(t, sw, Config{
		Book:       book,
		Pool:       nil, // empty pool → forces book fallback
		WarmTarget: 5,
		DialBatch:  10,
	})

	m.tick()
	sw.waitForDials(t, 1, 500*time.Millisecond)

	// MarkAttempt should be called on the book pick.
	if book.markedAttempt[pickAddr.String()] == 0 {
		t.Fatalf("book.MarkAttempt not called on book-sourced dial")
	}
}

func TestManagerBanSkipsPeerInPool(t *testing.T) {
	sw := newFakeSwitch()
	pid := p2p.ID("0123456789abcdef0123456789abcdef01234567")
	pool := []addrbook.PeerAddr{
		{Addr: string(pid) + "@1.1.1.1:26656", Source: addrbook.SourceBootstrap},
	}

	m := newManagerForTest(t, sw, Config{
		Pool:       pool,
		WarmTarget: 5,
		DialBatch:  10,
	})
	m.Ban(pid, "test")

	// dialFromPool decides whether to spawn a dial goroutine synchronously,
	// so dialCount is authoritative the moment tick returns.
	m.tick()

	if sw.dialCount() != 0 {
		t.Fatalf("banned peer was dialed from pool")
	}
}

func TestManagerDialFailureBansAtMaxDialFailures(t *testing.T) {
	sw := newFakeSwitch()
	sw.dialErr = &dialError{} // every dial fails

	book := newFakeBook()
	bans := newFakeBanlist()

	addr := "0123456789abcdef0123456789abcdef01234567@1.1.1.1:26656"

	m := newManagerForTest(t, sw, Config{
		Book:            book,
		Banlist:         bans,
		MaxDialFailures: 2,
	})

	na, err := p2p.NewNetAddressString(addr)
	if err != nil {
		t.Fatal(err)
	}
	// Drive dial directly (bypass tick — we just want to verify
	// recordDialFail behavior at threshold).
	m.recordDialFail(na, sw.dialErr)
	if _, ok := book.removed[addr]; ok {
		t.Fatalf("book.RemoveAddress called after first failure (threshold=2)")
	}
	m.recordDialFail(na, sw.dialErr)
	if book.removed[addr] == 0 {
		t.Fatalf("book.RemoveAddress not called at threshold")
	}
	if bans.added[addr] != "max-dial-failures" {
		t.Fatalf("banlist.Add reason=%q, want max-dial-failures", bans.added[addr])
	}
}

// dialError is a stub net.OpError-like.
type dialError struct{}

func (*dialError) Error() string { return "dial fail" }

// ErrCurrentlyDialingOrExistingAddress comes back from cometbft when a
// concurrent dial is already in flight (TOCTOU between the
// IsDialingOrExistingAddress gate and DialPeerWithAddress, or pinned-
// redial racing warm-fill). It must NOT count toward MaxDialFailures —
// otherwise a healthy peer can be evicted from the addrbook and added
// to the persistent banlist purely from internal dial racing.
func TestManagerCurrentlyDialingErrorNotCountedAsFailure(t *testing.T) {
	sw := newFakeSwitch()
	sw.dialErr = p2p.ErrCurrentlyDialingOrExistingAddress{Addr: "1.1.1.1:26656"}

	book := newFakeBook()
	bans := newFakeBanlist()

	addr := "0123456789abcdef0123456789abcdef01234567@1.1.1.1:26656"
	na, err := p2p.NewNetAddressString(addr)
	if err != nil {
		t.Fatal(err)
	}

	m := newManagerForTest(t, sw, Config{
		Book:            book,
		Banlist:         bans,
		MaxDialFailures: 2,
	})

	// Drive dial well past MaxDialFailures.
	for i := 0; i < 5; i++ {
		m.dial(na)
	}

	m.mu.Lock()
	count := m.dialFail[na.String()]
	m.mu.Unlock()
	if count != 0 {
		t.Fatalf("dialFail[%s]=%d after ErrCurrentlyDialingOrExistingAddress, want 0", na, count)
	}
	if book.removed[na.String()] != 0 {
		t.Fatalf("book.RemoveAddress called on currently-dialing race")
	}
	if _, banned := bans.added[na.String()]; banned {
		t.Fatalf("banlist.Add called on currently-dialing race")
	}
}

// buildPool must keep bootstrap-sourced entries at the front of the
// pool. Without that, dialFromPool's cursor takes thousands of dial
// ticks to reach the seeds — the walk's per-height timeout fires
// long before any bootstrap peer is tried.
func TestBuildPoolKeepsBootstrapAtFront(t *testing.T) {
	bootstrap := []addrbook.PeerAddr{
		{Addr: "0000000000000000000000000000000000000001@1.1.1.1:26656", Source: addrbook.SourceBootstrap},
		{Addr: "0000000000000000000000000000000000000002@2.2.2.2:26656", Source: addrbook.SourceBootstrap},
		{Addr: "0000000000000000000000000000000000000003@3.3.3.3:26656", Source: addrbook.SourceBootstrap},
	}
	addrbookEntries := make([]addrbook.PeerAddr, 100)
	for i := range addrbookEntries {
		addrbookEntries[i] = addrbook.PeerAddr{
			Addr:   "ffffffffffffffffffffffffffffffffffffffff@10.0.0.1:26656",
			Source: addrbook.SourceAddrbook,
		}
	}
	in := append(append([]addrbook.PeerAddr(nil), bootstrap...), addrbookEntries...)

	pool := buildPool(in)

	if len(pool) != len(in) {
		t.Fatalf("pool len=%d, want %d", len(pool), len(in))
	}
	for i, want := range bootstrap {
		if pool[i].Source != addrbook.SourceBootstrap {
			t.Fatalf("pool[%d].Source=%q, want bootstrap", i, pool[i].Source)
		}
		if pool[i].Addr != want.Addr {
			t.Fatalf("pool[%d].Addr=%q, want %q (bootstrap order changed)", i, pool[i].Addr, want.Addr)
		}
	}
	for i := len(bootstrap); i < len(pool); i++ {
		if pool[i].Source != addrbook.SourceAddrbook {
			t.Fatalf("pool[%d].Source=%q after bootstrap split, want addrbook", i, pool[i].Source)
		}
	}
}

func TestBuildPoolHandlesEmptyAndAllBootstrap(t *testing.T) {
	if got := buildPool(nil); len(got) != 0 {
		t.Fatalf("empty input -> pool len=%d, want 0", len(got))
	}
	allBoot := []addrbook.PeerAddr{
		{Addr: "0000000000000000000000000000000000000001@1.1.1.1:26656", Source: addrbook.SourceBootstrap},
		{Addr: "0000000000000000000000000000000000000002@2.2.2.2:26656", Source: addrbook.SourceBootstrap},
	}
	got := buildPool(allBoot)
	for i, want := range allBoot {
		if got[i].Addr != want.Addr {
			t.Fatalf("all-bootstrap pool[%d].Addr=%q, want %q", i, got[i].Addr, want.Addr)
		}
	}
}
