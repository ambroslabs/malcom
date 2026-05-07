// Package connect owns all outbound dialing for snapfetch. Callers
// (download, peerWatch) interact via Pin/Unpin/Ban — they do not
// touch p2p.Switch.DialPeer*.
//
// Manager runs one goroutine that ticks at RefreshTick. Per tick:
//
//  1. Pinned redials. For each pinned peer not currently connected
//     and past its backoff window, fire a dial. After MaxRedials
//     consecutive disconnect/redial cycles, auto-Ban the peer (and
//     book.MarkBad it so cometbft's PEX won't pick it back up).
//
//  2. Warm-fill. If Switch.NumPeers().Out < WarmTarget, draw addrs
//     from the static Pool (shuffled cursor, wraparound) and fire
//     up to DialBatch dials. Skip already-connected, pinned, and
//     banned peers.
//
// Pool is a static seed list provided at construction (typically
// bootstrap CSV + previously-loaded addrbook entries). PEX gossip
// adds to the cometbft addrbook independently; in Phase 1 the
// manager doesn't pick from there — pex.AutoReactor still owns its
// own dial loop. Phase 3 collapses both.
//
// Manager is purely poll-based on Switch.Peers() — no event
// subscription. RefreshTick of 1-2s gives sub-tick latency from
// disconnect to redial; events would only shave that to zero.
package connect

import (
	"context"
	"math/rand"
	"sync"
	"time"

	cmtlog "github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/p2p"
	pexcb "github.com/cometbft/cometbft/p2p/pex"

	"github.com/zrbecker/cosmos-p2p/internal/helpers/addrbook"
	"github.com/zrbecker/cosmos-p2p/internal/logctx"
)

// Config holds Manager dependencies and tuning. All durations have
// sensible defaults if zero.
type Config struct {
	Switch *p2p.Switch
	Book   pexcb.AddrBook // for MarkBad on auto-ban (in-memory cometbft layer)
	Pool   []addrbook.PeerAddr

	WarmTarget  int           // refill if Switch.NumPeers().Out < WarmTarget
	DialBatch   int           // max parallel dials per tick (default 4)
	RefreshTick time.Duration // how often the dial loop fires (default 2s)

	// Pinned-redial backoff schedule. backoff << (disconnects-1),
	// capped at MaxBackoff.
	Backoff    time.Duration // base (default 5s)
	MaxBackoff time.Duration // cap (default 5m)

	// MaxRedials caps consecutive disconnect/redial cycles for a
	// pinned peer before auto-Ban. 0 = unlimited.
	MaxRedials int

	// BanDuration is the TTL passed to book.MarkBad when auto-banning.
	BanDuration time.Duration
}

func (c *Config) defaults() {
	if c.DialBatch == 0 {
		c.DialBatch = 4
	}
	if c.RefreshTick == 0 {
		c.RefreshTick = 2 * time.Second
	}
	if c.Backoff == 0 {
		c.Backoff = 5 * time.Second
	}
	if c.MaxBackoff == 0 {
		c.MaxBackoff = 5 * time.Minute
	}
	if c.BanDuration == 0 {
		c.BanDuration = time.Hour
	}
}

type Manager struct {
	cfg Config
	log cmtlog.Logger

	mu       sync.Mutex
	pinned   map[p2p.ID]string         // pid → addr (id@host:port)
	backoffs map[p2p.ID]*peerBackoff   // pid → schedule state
	banned   map[p2p.ID]bool           // never-redial within this run
	pool     []addrbook.PeerAddr       // shuffled at New()
	cursor   int

	cancel context.CancelFunc
}

type peerBackoff struct {
	disconnects int
	nextDialAt  time.Time
}

// New constructs a Manager and starts its dial loop. Returns
// immediately; loop runs until the returned ctx is cancelled or Stop
// is called.
func New(ctx context.Context, c Config) *Manager {
	c.defaults()
	loopCtx, cancel := context.WithCancel(ctx)

	pool := append([]addrbook.PeerAddr(nil), c.Pool...)
	rand.Shuffle(len(pool), func(i, j int) { pool[i], pool[j] = pool[j], pool[i] })

	m := &Manager{
		cfg:      c,
		log:      logctx.From(ctx).With("module", "connect"),
		pinned:   map[p2p.ID]string{},
		backoffs: map[p2p.ID]*peerBackoff{},
		banned:   map[p2p.ID]bool{},
		pool:     pool,
		cancel:   cancel,
	}
	go m.loop(loopCtx)
	return m
}

// Stop halts the dial loop. Idempotent.
func (m *Manager) Stop() { m.cancel() }

// Pin marks pid as worth redialing. addr is the dial string
// ("nodeID@host:port") so we can dial without sw.Peers().Get
// (which returns nil once the peer disconnects). Idempotent —
// re-pinning updates addr but doesn't reset backoff.
func (m *Manager) Pin(pid p2p.ID, addr string) {
	if pid == "" || addr == "" {
		return
	}
	m.mu.Lock()
	m.pinned[pid] = addr
	m.mu.Unlock()
}

// Unpin removes a pin. The Manager will no longer redial pid (but
// existing connection is unaffected). Idempotent.
func (m *Manager) Unpin(pid p2p.ID) {
	m.mu.Lock()
	delete(m.pinned, pid)
	delete(m.backoffs, pid)
	m.mu.Unlock()
}

// Ban marks pid as never-redial within this run. Removes any
// existing pin. Idempotent. The reason is logged.
func (m *Manager) Ban(pid p2p.ID, reason string) {
	m.mu.Lock()
	if _, already := m.banned[pid]; already {
		m.mu.Unlock()
		return
	}
	m.banned[pid] = true
	delete(m.pinned, pid)
	delete(m.backoffs, pid)
	m.mu.Unlock()
	m.log.Debug("peer banned", "peer", string(pid), "reason", reason)
}

// IsBanned reports whether pid has been banned for this run.
// Used by callers to mirror the verdict in their own state.
func (m *Manager) IsBanned(pid p2p.ID) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.banned[pid]
}

func (m *Manager) loop(ctx context.Context) {
	t := time.NewTicker(m.cfg.RefreshTick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.tick()
		}
	}
}

func (m *Manager) tick() {
	now := time.Now()
	sw := m.cfg.Switch
	if sw == nil {
		return
	}

	// Snapshot pinned + banned under the lock; release before doing
	// any I/O.
	m.mu.Lock()
	pinned := make(map[p2p.ID]string, len(m.pinned))
	for k, v := range m.pinned {
		pinned[k] = v
	}
	m.mu.Unlock()

	// 1. Pinned redials.
	for pid, addr := range pinned {
		if sw.Peers().Get(pid) != nil {
			// Connected — clear any backoff.
			m.mu.Lock()
			delete(m.backoffs, pid)
			m.mu.Unlock()
			continue
		}

		m.mu.Lock()
		bo, ok := m.backoffs[pid]
		if ok && now.Before(bo.nextDialAt) {
			m.mu.Unlock()
			continue
		}
		// Cap consecutive cycles. After MaxRedials, auto-ban.
		if m.cfg.MaxRedials > 0 && bo != nil && bo.disconnects >= m.cfg.MaxRedials {
			delete(m.pinned, pid)
			delete(m.backoffs, pid)
			m.banned[pid] = true
			m.mu.Unlock()
			m.markBadByAddr(addr)
			m.log.Debug("auto-banning pinned peer (max redials)",
				"peer", string(pid), "disconnects", bo.disconnects)
			continue
		}
		// Bump the backoff *before* firing the dial: the dial may
		// no-op via ErrCurrentlyDialingOrExistingAddress, which
		// shouldn't reset our schedule.
		if bo == nil {
			bo = &peerBackoff{}
			m.backoffs[pid] = bo
		}
		bo.disconnects++
		bo.nextDialAt = now.Add(m.computeBackoff(bo.disconnects))
		m.mu.Unlock()

		na, err := p2p.NewNetAddressString(addr)
		if err != nil {
			continue
		}
		go func(na *p2p.NetAddress) {
			_ = sw.DialPeerWithAddress(na)
		}(na)
	}

	// 2. Warm-fill from the static pool.
	out, _, dialing := sw.NumPeers()
	if out+dialing >= m.cfg.WarmTarget {
		return
	}
	need := m.cfg.WarmTarget - out - dialing
	if need > m.cfg.DialBatch {
		need = m.cfg.DialBatch
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.pool) == 0 || need <= 0 {
		return
	}
	fired := 0
	for tries := 0; tries < len(m.pool) && fired < need; tries++ {
		s := m.pool[m.cursor]
		m.cursor = (m.cursor + 1) % len(m.pool)

		na, err := p2p.NewNetAddressString(s.Addr)
		if err != nil {
			continue
		}
		if m.banned[na.ID] {
			continue
		}
		if _, isPinned := m.pinned[na.ID]; isPinned {
			// Pinned path handled this peer above; don't double-dial.
			continue
		}
		if sw.Peers().Get(na.ID) != nil {
			continue
		}
		fired++
		go func(na *p2p.NetAddress) {
			_ = sw.DialPeerWithAddress(na)
		}(na)
	}
	if fired > 0 {
		m.log.Debug("warm-fill", "out", out, "target", m.cfg.WarmTarget, "dialed", fired)
	}
}

func (m *Manager) computeBackoff(disconnects int) time.Duration {
	if disconnects <= 1 {
		return m.cfg.Backoff
	}
	shift := disconnects - 1
	if shift > 10 {
		shift = 10
	}
	d := m.cfg.Backoff << uint(shift)
	if d <= 0 || d > m.cfg.MaxBackoff {
		return m.cfg.MaxBackoff
	}
	return d
}

func (m *Manager) markBadByAddr(addr string) {
	if m.cfg.Book == nil || addr == "" {
		return
	}
	na, err := p2p.NewNetAddressString(addr)
	if err != nil {
		return
	}
	m.cfg.Book.MarkBad(na, m.cfg.BanDuration)
}

