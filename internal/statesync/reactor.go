// Package statesync implements a minimal CometBFT state-sync client reactor
// that speaks just enough of channels 0x60 (snapshot) and 0x61 (chunk) to
// probe peers for available snapshots and optionally fetch a single chunk
// to measure its size.
//
// We don't apply snapshots — we have no app to feed them to. This is purely
// a discovery/measurement tool for cosmoshub state-sync availability.
package statesync

import (
	"sync"

	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/p2p/conn"
	ssproto "github.com/cometbft/cometbft/proto/tendermint/statesync"
	"github.com/cosmos/gogoproto/proto"
)

const (
	// SnapshotChannel carries SnapshotsRequest / SnapshotsResponse.
	SnapshotChannel = byte(0x60)
	// ChunkChannel carries ChunkRequest / ChunkResponse.
	ChunkChannel = byte(0x61)

	// Mirror cometbft's own caps so we accept anything a peer might send.
	snapshotMsgSize = 4 * 1024 * 1024
	chunkMsgSize    = 16 * 1024 * 1024
)

// Snapshot is the subset of SnapshotsResponse we expose to callers.
type Snapshot struct {
	Height   uint64 `json:"height"`
	Format   uint32 `json:"format"`
	Chunks   uint32 `json:"chunks"`
	Hash     []byte `json:"hash"`
	Metadata []byte `json:"metadata,omitempty"`
}

// ChunkInfo summarises a ChunkResponse. When KeepBytes is true on the
// reactor, Bytes carries the full chunk payload; otherwise it is nil and
// only Size is populated (cheap probe mode).
type ChunkInfo struct {
	Height  uint64
	Format  uint32
	Index   uint32
	Size    int
	Bytes   []byte
	Missing bool
}

// Event is the union surfaced via Out: either a snapshot offer or a chunk reply.
type Event struct {
	PeerID   string
	Snapshot *Snapshot
	Chunk    *ChunkInfo
}

// Reactor probes peers for snapshots. AddPeer fires a SnapshotsRequest;
// inbound SnapshotsResponse is forwarded on Out. ChunkRequest can be
// dispatched explicitly via RequestChunk.
type Reactor struct {
	p2p.BaseReactor
	logger log.Logger
	Out    chan Event

	mu        sync.Mutex
	bytesRecv int64
	bytesSent int64

	// AskOnAdd, when true, sends SnapshotsRequest to every peer on AddPeer.
	// Defaults to true.
	AskOnAdd bool
	// KeepBytes, when true, copies the full chunk payload into ChunkInfo.Bytes.
	// Probe mode (default false) only records lengths to keep memory low.
	KeepBytes bool
}

func NewReactor(logger log.Logger) *Reactor {
	r := &Reactor{
		logger:   logger,
		Out:      make(chan Event, 256),
		AskOnAdd: true,
	}
	r.BaseReactor = *p2p.NewBaseReactor("statesync-probe", r)
	r.BaseReactor.SetLogger(logger)
	return r
}

func (r *Reactor) GetChannels() []*conn.ChannelDescriptor {
	return []*conn.ChannelDescriptor{
		{
			ID:                  SnapshotChannel,
			Priority:            5,
			SendQueueCapacity:   10,
			RecvBufferCapacity:  snapshotMsgSize,
			RecvMessageCapacity: snapshotMsgSize,
			MessageType:         &ssproto.Message{},
		},
		{
			ID:                  ChunkChannel,
			Priority:            3,
			SendQueueCapacity:   10,
			RecvBufferCapacity:  chunkMsgSize,
			RecvMessageCapacity: chunkMsgSize,
			MessageType:         &ssproto.Message{},
		},
	}
}

func (r *Reactor) AddPeer(peer p2p.Peer) {
	if !r.AskOnAdd {
		return
	}
	req := &ssproto.SnapshotsRequest{}
	if peer.Send(p2p.Envelope{ChannelID: SnapshotChannel, Message: req}) {
		r.mu.Lock()
		r.bytesSent += int64(proto.Size(req))
		r.mu.Unlock()
		r.logger.Debug("SnapshotsRequest sent", "peer", peer.ID())
	} else {
		r.logger.Error("SnapshotsRequest send queue full", "peer", peer.ID())
	}
}

// RequestChunk dispatches a ChunkRequest for (height, format, index) to peer.
// Used after we've seen a snapshot offer to measure real chunk size.
func (r *Reactor) RequestChunk(peer p2p.Peer, height uint64, format, index uint32) bool {
	req := &ssproto.ChunkRequest{Height: height, Format: format, Index: index}
	if !peer.Send(p2p.Envelope{ChannelID: ChunkChannel, Message: req}) {
		return false
	}
	r.mu.Lock()
	r.bytesSent += int64(proto.Size(req))
	r.mu.Unlock()
	return true
}

func (r *Reactor) Receive(env p2p.Envelope) {
	if pm, ok := env.Message.(proto.Message); ok {
		r.mu.Lock()
		r.bytesRecv += int64(proto.Size(pm))
		r.mu.Unlock()
	}
	peerID := string(env.Src.ID())
	switch m := env.Message.(type) {
	case *ssproto.SnapshotsResponse:
		r.logger.Info("snapshot offered",
			"peer", peerID, "height", m.Height, "format", m.Format,
			"chunks", m.Chunks, "metadata_bytes", len(m.Metadata))
		select {
		case r.Out <- Event{
			PeerID: peerID,
			Snapshot: &Snapshot{
				Height: m.Height, Format: m.Format, Chunks: m.Chunks,
				Hash: m.Hash, Metadata: m.Metadata,
			},
		}:
		default:
			r.logger.Error("snapshot Out channel full; dropping", "peer", peerID)
		}

	case *ssproto.ChunkResponse:
		r.logger.Debug("chunk received",
			"peer", peerID, "height", m.Height, "format", m.Format,
			"index", m.Index, "bytes", len(m.Chunk), "missing", m.Missing)
		ci := &ChunkInfo{
			Height: m.Height, Format: m.Format, Index: m.Index,
			Size: len(m.Chunk), Missing: m.Missing,
		}
		if r.KeepBytes && len(m.Chunk) > 0 {
			ci.Bytes = make([]byte, len(m.Chunk))
			copy(ci.Bytes, m.Chunk)
		}
		select {
		case r.Out <- Event{PeerID: peerID, Chunk: ci}:
		default:
			r.logger.Error("chunk Out channel full; dropping", "peer", peerID)
		}

	case *ssproto.SnapshotsRequest, *ssproto.ChunkRequest:
		// We're a probe — we don't serve snapshots. Ignore inbound requests
		// rather than answering with nothing (which is what cometbft does
		// when ListSnapshots returns empty anyway).

	default:
		r.logger.Debug("unexpected statesync msg", "peer", peerID, "type", m)
	}
}

// Bytes returns recv/sent byte counters across both channels.
func (r *Reactor) Bytes() (recv, sent int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.bytesRecv, r.bytesSent
}
