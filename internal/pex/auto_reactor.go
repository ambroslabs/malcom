// AutoReactor is a leaf-only PEX reactor: it sends PexRequest on every
// AddPeer, receives PexAddrs and writes them to a cometbft AddrBook
// (filtered against the Banlist), and answers nothing to inbound
// PexRequests. It does NOT dial — outbound dialing is owned entirely
// by internal/connect.Manager, which picks from the same AddrBook.
//
// We do not serve PEX to inbound peers (we're a leaf, not a seed).
// Some peers may bench us for not responding to their PexRequest; for
// a one-shot snapshot fetch this is acceptable.

package pex

import (
	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/p2p/conn"
	pexcb "github.com/cometbft/cometbft/p2p/pex"
	tmp2p "github.com/cometbft/cometbft/proto/tendermint/p2p"
)

// Banlist is the optional persistent banlist of peer addresses. When
// non-nil, PEX-gossiped addresses matching Has() are dropped before
// book.AddAddress so re-gossip can't resurrect a banned peer.
//
// Implemented by *internal/helpers/banlist.Set; defined as an interface
// here to keep the pex package free of a hard dependency on it.
type Banlist interface {
	Has(addr string) bool
}

// Kicker is the optional dialer signal. Implemented by
// *internal/connect.Manager. After every non-empty PEX gossip we call
// Kick() so freshly-learned addrs get dialed before the next refresh
// tick fires — without this, gossip-discovered peers wait up to
// RefreshTick (5s) before they're reachable.
type Kicker interface {
	Kick()
}

// AutoConfig holds optional dependencies.
type AutoConfig struct {
	// Banlist filters gossip before it lands in the addrbook.
	Banlist Banlist
	// Kicker, if non-nil, is signalled after each non-empty gossip.
	Kicker Kicker
}

type AutoReactor struct {
	p2p.BaseReactor
	book pexcb.AddrBook
	cfg  AutoConfig
	log  log.Logger
}

// NewAutoReactor returns a PEX reactor wired to a cometbft AddrBook.
// Register on the Switch alongside any state-sync / blocksync reactors
// and call sw.SetAddrBook(book) before sw.Start().
func NewAutoReactor(book pexcb.AddrBook, cfg AutoConfig, logger log.Logger) *AutoReactor {
	r := &AutoReactor{
		book: book,
		cfg:  cfg,
		log:  logger,
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

// AddPeer sends a PexRequest and marks the peer "good" in the addrbook
// (the addrbook biases future PickAddress calls toward known-good
// peers, both in this run and after .Save()).
func (r *AutoReactor) AddPeer(peer p2p.Peer) {
	r.book.MarkGood(peer.ID())
	if !peer.Send(p2p.Envelope{ChannelID: Channel, Message: &tmp2p.PexRequest{}}) {
		r.log.Debug("PEX: PexRequest send queue full", "peer", peer.ID())
	}
}

// RemovePeer is a no-op. Reconnection / refill is owned by
// internal/connect.Manager, which polls Switch.NumPeers() on its tick.
func (r *AutoReactor) RemovePeer(peer p2p.Peer, reason interface{}) {}

// Receive handles inbound PEX messages. We answer PexRequest with
// nothing (intentional — we're a leaf). PexAddrs entries land in the
// AddrBook keyed by the peer who told us, after banlist filtering.
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
	skippedBanned := 0
	for _, a := range addrs {
		if a == nil || !a.HasID() {
			continue
		}
		if r.cfg.Banlist != nil && r.cfg.Banlist.Has(a.String()) {
			skippedBanned++
			continue
		}
		if err := r.book.AddAddress(a, srcAddr); err != nil {
			r.log.Debug("PEX: addr rejected by book", "addr", a, "err", err)
			continue
		}
		added++
	}
	if added > 0 || skippedBanned > 0 {
		r.log.Debug("PEX: gossip processed",
			"from", src.ID(), "added", added, "skipped_banned", skippedBanned, "book_size", r.book.Size())
	}
	if added > 0 && r.cfg.Kicker != nil {
		r.cfg.Kicker.Kick()
	}
}
