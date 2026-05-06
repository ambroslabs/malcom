// AutoReactor is the full PEX reactor used by `malcom snapshot fetch`:
// it sends PexRequest on every AddPeer, receives PexAddrs and writes
// them to a cometbft AddrBook, and runs a dial loop that grows the
// connected-peer set toward TargetPeers.
//
// We do not serve PEX to inbound peers (we're a leaf, not a seed).
// Some peers may bench us for not responding to their PexRequest; for
// a one-shot snapshot fetch this is acceptable.

package pex

import (
	"time"

	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/p2p/conn"
	pexcb "github.com/cometbft/cometbft/p2p/pex"
	tmp2p "github.com/cometbft/cometbft/proto/tendermint/p2p"
)

// AutoConfig tunes the dial loop. Zero values get sensible defaults.
type AutoConfig struct {
	// TargetPeers is the connected-outbound count we aim for. The dial
	// loop fires waves until (out + dialing) >= TargetPeers.
	TargetPeers int

	// MaxPerWave caps parallel dials per tick.
	MaxPerWave int

	// DialInterval is how often the dial loop ticks. Each tick may
	// fire up to MaxPerWave dials. Receiving fresh PexAddrs also
	// signals the loop opportunistically.
	DialInterval time.Duration

	// BookBias passes to AddrBook.PickAddress: 0..100 percent bias
	// toward "new" (untried) addresses. cometbft convention.
	BookBias int
}

func (c *AutoConfig) defaults() {
	if c.TargetPeers == 0 {
		c.TargetPeers = 50
	}
	if c.MaxPerWave == 0 {
		c.MaxPerWave = 8
	}
	if c.DialInterval == 0 {
		c.DialInterval = 2 * time.Second
	}
	if c.BookBias == 0 {
		c.BookBias = 50
	}
}

type AutoReactor struct {
	p2p.BaseReactor
	book pexcb.AddrBook
	cfg  AutoConfig
	log  log.Logger

	dialCh chan struct{}
}

// NewAutoReactor returns a PEX reactor wired to a cometbft AddrBook.
// Register on the Switch alongside any state-sync / blocksync reactors
// and call sw.SetAddrBook(book) before sw.Start().
func NewAutoReactor(book pexcb.AddrBook, cfg AutoConfig, logger log.Logger) *AutoReactor {
	cfg.defaults()
	r := &AutoReactor{
		book:   book,
		cfg:    cfg,
		log:    logger,
		dialCh: make(chan struct{}, 1),
	}
	r.BaseReactor = *p2p.NewBaseReactor("PEX-Auto", r)
	r.BaseReactor.SetLogger(logger)
	return r
}

// GetChannels advertises channel 0x00. Recv buffer sized generously —
// PEX traffic is tiny but some peers ship 30-entry batches with IPv6
// addrs that nudge a few hundred bytes over the cometbft-spec cap.
func (r *AutoReactor) GetChannels() []*conn.ChannelDescriptor {
	return []*conn.ChannelDescriptor{{
		ID:                  Channel,
		Priority:            1,
		SendQueueCapacity:   10,
		RecvMessageCapacity: 64 * 1024,
		RecvBufferCapacity:  64 * 1024,
		MessageType:         &tmp2p.Message{},
	}}
}

// OnStart launches the dial loop.
func (r *AutoReactor) OnStart() error {
	go r.dialLoop()
	return nil
}

// AddPeer sends a PexRequest, marks the peer "good" in the addrbook
// (the addrbook biases future PickAddress calls toward known-good
// peers, both in this run and after .Save()), and signals the dial
// loop in case our connected count drops below TargetPeers.
func (r *AutoReactor) AddPeer(peer p2p.Peer) {
	r.book.MarkGood(peer.ID())
	if !peer.Send(p2p.Envelope{ChannelID: Channel, Message: &tmp2p.PexRequest{}}) {
		r.log.Debug("PEX: PexRequest send queue full", "peer", peer.ID())
	}
	r.kick()
}

// RemovePeer signals the dial loop to refill if we dropped below
// TargetPeers.
func (r *AutoReactor) RemovePeer(peer p2p.Peer, reason interface{}) {
	r.kick()
}

// Receive handles inbound PEX messages. We answer PexRequest with
// nothing (intentional — we're a leaf). PexAddrs entries land in the
// AddrBook keyed by the peer who told us.
func (r *AutoReactor) Receive(env p2p.Envelope) {
	switch m := env.Message.(type) {
	case *tmp2p.PexRequest:
		// Don't serve.
	case *tmp2p.PexAddrs:
		r.handleAddrs(env.Src, m.Addrs)
	}
}

func (r *AutoReactor) handleAddrs(src p2p.Peer, raw []tmp2p.NetAddress) {
	addrs, err := p2p.NetAddressesFromProto(raw)
	if err != nil {
		r.log.Debug("PEX: bad addrs", "peer", src.ID(), "err", err)
		return
	}
	srcAddr := src.SocketAddr()
	added := 0
	for _, a := range addrs {
		if a == nil || !a.HasID() {
			continue
		}
		if err := r.book.AddAddress(a, srcAddr); err != nil {
			r.log.Debug("PEX: addr rejected by book", "addr", a, "err", err)
			continue
		}
		added++
	}
	if added > 0 {
		r.log.Debug("PEX: book grew",
			"from", src.ID(), "added", added, "book_size", r.book.Size())
		r.kick()
	}
}

func (r *AutoReactor) dialLoop() {
	t := time.NewTicker(r.cfg.DialInterval)
	defer t.Stop()
	for {
		select {
		case <-r.Quit():
			return
		case <-t.C:
		case <-r.dialCh:
		}
		r.dialWave()
	}
}

// dialWave fires up to MaxPerWave parallel dials toward TargetPeers.
// It uses Switch.IsDialingOrExistingAddress to skip duplicates and
// MarkAttempt on the book so PickAddress's freshness scoring stays
// honest.
func (r *AutoReactor) dialWave() {
	sw := r.Switch
	if sw == nil {
		return
	}
	out, _, dialing := sw.NumPeers()
	need := r.cfg.TargetPeers - out - dialing
	if need <= 0 {
		return
	}
	if need > r.cfg.MaxPerWave {
		need = r.cfg.MaxPerWave
	}
	fired := 0
	// Cap iterations defensively — if every PickAddress returns a
	// duplicate we don't want an infinite loop.
	for tries := 0; tries < need*4 && fired < need; tries++ {
		addr := r.book.PickAddress(r.cfg.BookBias)
		if addr == nil {
			return
		}
		if sw.IsDialingOrExistingAddress(addr) {
			continue
		}
		r.book.MarkAttempt(addr)
		fired++
		go func(a *p2p.NetAddress) {
			if err := sw.DialPeerWithAddress(a); err != nil {
				r.log.Debug("PEX: dial failed", "addr", a, "err", err)
				// Short ban so the addrbook stops re-picking this
				// address during the current run; expires soon
				// enough that a future run gets to retry fresh.
				r.book.MarkBad(a, 5*time.Minute)
			}
		}(addr)
	}
}

func (r *AutoReactor) kick() {
	select {
	case r.dialCh <- struct{}{}:
	default:
	}
}
