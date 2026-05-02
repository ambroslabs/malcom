// Package pex implements the receive half of CometBFT's peer-exchange
// protocol on channel 0x00. We send a PexRequest to every peer we connect
// to and forward the resulting PexAddrs out a Go channel for a crawler to
// consume.
//
// We intentionally do *not* serve PexRequest from inbound peers — we're a
// crawler, not a real node, so we have no address book to share.
package pex

import (
	"sync"

	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/p2p/conn"
	tmp2p "github.com/cometbft/cometbft/proto/tendermint/p2p"
	"github.com/cosmos/gogoproto/proto"
)

// Channel is the CometBFT PEX channel ID.
const Channel = byte(0x00)

// AddrEvent carries one batch of peer addresses received from a single peer.
type AddrEvent struct {
	Source string // node ID of the peer that sent the batch
	Addrs  []tmp2p.NetAddress
}

type Reactor struct {
	p2p.BaseReactor
	logger log.Logger
	Out    chan AddrEvent

	mu        sync.Mutex
	self      *p2p.NetAddress // advertised; only address we hand out via PEX
	servedTo  int64
	bytesRecv int64
	bytesSent int64

	// AskOnAdd, when true, sends a PexRequest to every peer we connect to
	// so we keep learning about other peers. Defaults to true.
	AskOnAdd bool
}

func NewReactor(logger log.Logger) *Reactor {
	r := &Reactor{
		logger:   logger,
		Out:      make(chan AddrEvent, 1024),
		AskOnAdd: true,
	}
	r.BaseReactor = *p2p.NewBaseReactor("pex-server", r)
	r.BaseReactor.SetLogger(logger)
	return r
}

// SetSelf installs the address we advertise to peers asking us for PEX.
// Pass our publicly-dialable nodeID@host:port. If unset, we serve an empty
// PexAddrs (no gossip happens).
func (r *Reactor) SetSelf(addr p2p.NetAddress) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := addr
	r.self = &cp
}

// ServedCount returns how many PexRequests we've answered.
func (r *Reactor) ServedCount() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.servedTo
}

func (r *Reactor) GetChannels() []*conn.ChannelDescriptor {
	return []*conn.ChannelDescriptor{{
		ID:                Channel,
		Priority:          1,
		SendQueueCapacity: 10,
		// Some peers ship PexAddrs slightly over the cometbft-spec
		// maxAddressSize*maxGetSelection (=7680) limit — IPv6 addrs and
		// proto framing nudge a 30-entry batch a few hundred bytes higher
		// in the wild. Set generously; PEX traffic is tiny anyway.
		RecvMessageCapacity: 64 * 1024,
		RecvBufferCapacity:  64 * 1024,
		MessageType:         &tmp2p.Message{},
	}}
}

func (r *Reactor) AddPeer(peer p2p.Peer) {
	if r.AskOnAdd {
		req := &tmp2p.PexRequest{}
		if peer.Send(p2p.Envelope{ChannelID: Channel, Message: req}) {
			r.mu.Lock()
			r.bytesSent += int64(proto.Size(req))
			r.mu.Unlock()
		} else {
			r.logger.Error("PexRequest send queue full", "peer", peer.ID())
		}
	}
}

func (r *Reactor) Receive(env p2p.Envelope) {
	r.mu.Lock()
	if pm, ok := env.Message.(proto.Message); ok {
		r.bytesRecv += int64(proto.Size(pm))
	}
	r.mu.Unlock()

	switch m := env.Message.(type) {
	case *tmp2p.PexRequest:
		// Only ever announce ourselves. We don't relay other peers' addrs.
		r.mu.Lock()
		self := r.self
		r.mu.Unlock()
		if self == nil {
			return
		}
		resp := &tmp2p.PexAddrs{Addrs: p2p.NetAddressesToProto([]*p2p.NetAddress{self})}
		ok := env.Src.Send(p2p.Envelope{ChannelID: Channel, Message: resp})
		if !ok {
			r.logger.Error("PexAddrs send queue full", "peer", env.Src.ID())
			return
		}
		r.mu.Lock()
		r.servedTo++
		r.bytesSent += int64(proto.Size(resp))
		r.mu.Unlock()
		r.logger.Info("PEX served self", "peer", env.Src.ID(), "self", self.String())

	case *tmp2p.PexAddrs:
		r.logger.Info("PEX got addrs", "peer", env.Src.ID(), "n", len(m.Addrs))
		select {
		case r.Out <- AddrEvent{Source: string(env.Src.ID()), Addrs: m.Addrs}:
		default:
			r.logger.Error("pex Out channel full; dropping", "peer", env.Src.ID(), "n", len(m.Addrs))
		}
	}
}

// Bytes returns recv/sent byte counters for channel 0x00 (PEX).
func (r *Reactor) Bytes() (recv, sent int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.bytesRecv, r.bytesSent
}
