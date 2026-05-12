// NoopReactor is the channel-0x00 stub registered when PEX is
// disabled. It does nothing: receive accepts and drops the message,
// AddPeer / RemovePeer / InitPeer are no-ops, GetChannels returns a
// descriptor so cometbft's MConnection has a registered handler for
// the PEX channel.
//
// Why this exists: a fetcher with pex_disabled=true that *also*
// doesn't register any reactor for channel 0x00 tears down every
// MConnection where the peer sends a PEX hello — cometbft's recv
// routine rejects packets for channels with no reactor with
// "unknown channel 0". See issue #118. The fix is to drop channel
// 0x00 from our own NodeInfo.Channels (so peers' PEX reactors won't
// initiate gossip with us) AND register this no-op so packets that
// arrive anyway land in a function that returns immediately.
//
// Curated-peers semantics are preserved: the reactor does not gossip
// outbound, does not parse inbound, and does not touch the addrbook.

package pex

import (
	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/p2p/conn"
	tmp2p "github.com/cometbft/cometbft/proto/tendermint/p2p"
)

type NoopReactor struct {
	p2p.BaseReactor
}

// NewNoopReactor returns a reactor that registers channel 0x00 but
// does nothing with incoming or outgoing packets.
func NewNoopReactor(logger log.Logger) *NoopReactor {
	r := &NoopReactor{}
	r.BaseReactor = *p2p.NewBaseReactor("PEX-Noop", r)
	r.BaseReactor.SetLogger(logger)
	return r
}

// GetChannels returns a single channel descriptor for 0x00 so cometbft's
// MConnection has a registered handler. Capacities mirror AutoReactor —
// PEX packets are tiny, but some peers ship 30-entry IPv6 gossip
// batches that nudge over the cometbft-spec default cap.
func (r *NoopReactor) GetChannels() []*conn.ChannelDescriptor {
	return []*conn.ChannelDescriptor{{
		ID:                  Channel,
		Priority:            1,
		SendQueueCapacity:   10,
		RecvMessageCapacity: 64 * 1024,
		RecvBufferCapacity:  64 * 1024,
		MessageType:         &tmp2p.Message{},
	}}
}

func (r *NoopReactor) AddPeer(p2p.Peer)              {}
func (r *NoopReactor) RemovePeer(p2p.Peer, any)      {}
func (r *NoopReactor) InitPeer(p p2p.Peer) p2p.Peer  { return p }
func (r *NoopReactor) Receive(p2p.Envelope)          {}
