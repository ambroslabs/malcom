// Package blocksync implements a minimal CometBFT block-sync reactor that
// speaks just enough of channel 0x40 to send a StatusRequest, then a
// BlockRequest, and surface what comes back to a caller via the Done channel.
//
// The full cometbft blocksync.Reactor depends on a live BlockExecutor,
// BlockStore, and consensus state. We only want to *fetch* a block, not
// validate or persist it, so we implement p2p.Reactor directly.
package blocksync

import (
	"fmt"
	"sync"

	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/p2p/conn"
	bcproto "github.com/cometbft/cometbft/proto/tendermint/blocksync"
	"github.com/cometbft/cometbft/types"
)

// Channel is the CometBFT block-sync channel ID.
const Channel = byte(0x40)

// Result is one event from the peer: a status, a block, or a no-block reply.
// Exactly one field is non-nil.
type Result struct {
	Status  *bcproto.StatusResponse
	Block   *types.Block
	NoBlock *bcproto.NoBlockResponse
	PeerID  string
}

// Reactor sends StatusRequest on AddPeer, then BlockRequest after the peer's
// StatusResponse. Results are delivered (non-blocking) on Done.
type Reactor struct {
	p2p.BaseReactor
	logger log.Logger
	Done   chan Result
	// HeightOffset is how many blocks below the peer's tip we ask for.
	// 0 means tip; default is 100 to give the peer's pruner plenty of room.
	HeightOffset int64
	// StatusOnly suppresses BlockRequest after StatusResponse. Used by the
	// crawler when we just want each peer's (base, height) range.
	StatusOnly bool

	// Peers send StatusResponse twice — once unsolicited on AddPeer, once in
	// reply to our StatusRequest. Track which peers we've already asked a
	// block from so we don't double-fire BlockRequest.
	mu    sync.Mutex
	asked map[string]int64
}

func NewReactor(logger log.Logger) *Reactor {
	r := &Reactor{
		logger:       logger,
		Done:         make(chan Result, 8),
		HeightOffset: 100,
		asked:        map[string]int64{},
	}
	r.BaseReactor = *p2p.NewBaseReactor("blocksync-explorer", r)
	r.BaseReactor.SetLogger(logger)
	return r
}

func (r *Reactor) GetChannels() []*conn.ChannelDescriptor {
	return []*conn.ChannelDescriptor{
		{
			ID:                  Channel,
			Priority:            5,
			SendQueueCapacity:   1000,
			RecvBufferCapacity:  50 * 4096,
			RecvMessageCapacity: types.MaxBlockSizeBytes + 16,
			MessageType:         &bcproto.Message{},
		},
	}
}

func (r *Reactor) AddPeer(peer p2p.Peer) {
	r.logger.Info("peer up; sending StatusRequest", "peer", peer.ID())
	ok := peer.Send(p2p.Envelope{
		ChannelID: Channel,
		Message:   &bcproto.StatusRequest{},
	})
	if !ok {
		r.logger.Error("StatusRequest send queue full", "peer", peer.ID())
	}
}

func (r *Reactor) RemovePeer(peer p2p.Peer, reason interface{}) {
	r.logger.Info("peer removed", "peer", peer.ID(), "reason", fmt.Sprintf("%v", reason))
}

func (r *Reactor) Receive(env p2p.Envelope) {
	peerID := string(env.Src.ID())
	switch m := env.Message.(type) {
	case *bcproto.StatusResponse:
		r.logger.Info("StatusResponse", "peer", peerID, "base", m.Base, "height", m.Height)

		r.mu.Lock()
		_, already := r.asked[peerID]
		if !already {
			r.asked[peerID] = -1 // marker; updated below if we actually request
		}
		r.mu.Unlock()
		if already {
			return
		}
		// Always emit the status event (the crawler relies on it).
		select {
		case r.Done <- Result{Status: m, PeerID: peerID}:
		default:
		}
		if r.StatusOnly {
			return
		}

		want := m.Height - r.HeightOffset
		if want < m.Base+1 {
			want = m.Base + 1
		}
		r.mu.Lock()
		r.asked[peerID] = want
		r.mu.Unlock()
		r.logger.Info("sending BlockRequest", "peer", peerID, "height", want)
		if ok := env.Src.Send(p2p.Envelope{
			ChannelID: Channel,
			Message:   &bcproto.BlockRequest{Height: want},
		}); !ok {
			r.logger.Error("BlockRequest send queue full", "peer", peerID)
		}

	case *bcproto.BlockResponse:
		b, err := types.BlockFromProto(m.Block)
		if err != nil {
			r.logger.Error("decode BlockResponse", "peer", peerID, "err", err)
			return
		}
		r.logger.Info("BlockResponse", "peer", peerID, "height", b.Height)
		r.Done <- Result{Block: b, PeerID: peerID}

	case *bcproto.NoBlockResponse:
		r.logger.Info("NoBlockResponse", "peer", peerID, "height", m.Height)
		r.Done <- Result{NoBlock: m, PeerID: peerID}

	default:
		r.logger.Debug("unexpected message", "peer", peerID, "type", fmt.Sprintf("%T", m))
	}
}
