// Package statesync implements a minimal CometBFT state-sync client reactor
// that speaks just enough of channels 0x60 (snapshot) and 0x61 (chunk) to
// probe peers for available snapshots and optionally fetch a single chunk
// to measure its size.
//
// We don't apply snapshots — we have no app to feed them to. This is purely
// a discovery/measurement tool for cosmoshub state-sync availability.
package statesync

import (
	"sync/atomic"

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

// ChunkInfo summarises a ChunkResponse, including the full chunk
// payload in Bytes (consumers verify hashes and write to disk).
type ChunkInfo struct {
	Height  uint64
	Format  uint32
	Index   uint32
	Size    int
	Bytes   []byte
	Missing bool
}

// Event is the union surfaced via Out / OutChunks:
//   - Snapshot != nil  → a SnapshotsResponse arrived (Out)
//   - Chunk != nil     → a ChunkResponse arrived (OutChunks)
//   - Connected        → AddPeer fired (Out)
//   - Removed          → RemovePeer fired (Out)
//
// Exactly one of these is set per event.
type Event struct {
	PeerID    string
	Snapshot  *Snapshot
	Chunk     *ChunkInfo
	Connected bool
	Removed   bool
}

// Reactor probes peers for snapshots. AddPeer fires a SnapshotsRequest;
// inbound SnapshotsResponse is forwarded on Out. ChunkRequest can be
// dispatched explicitly via RequestChunk; ChunkResponse is forwarded on
// OutChunks. The two channels are separate so a 16 MiB chunk burst
// can't queue tiny control events behind it.
type Reactor struct {
	p2p.BaseReactor
	logger    log.Logger
	Out       chan Event // Connected, Removed, Snapshot
	OutChunks chan Event // Chunk

	bytesRecv  atomic.Int64
	bytesSent  atomic.Int64
	dropsCtrl  atomic.Int64
	dropsChunk atomic.Int64

	// AskOnAdd, when true, sends SnapshotsRequest to every peer on AddPeer.
	// Defaults to true.
	AskOnAdd bool
}

const outChunksCapacity = 8

func NewReactor(logger log.Logger) *Reactor {
	r := &Reactor{
		logger:    logger,
		Out:       make(chan Event, 256),
		OutChunks: make(chan Event, outChunksCapacity),
		AskOnAdd:  true,
	}
	r.BaseReactor = *p2p.NewBaseReactor("statesync-probe", r)
	r.BaseReactor.SetLogger(logger)
	return r
}

func (r *Reactor) GetChannels() []*conn.ChannelDescriptor {
	// Leave RecvBufferCapacity unset (defaults to 4 KiB in cometbft's
	// p2p/conn). Setting it equal to RecvMessageCapacity preallocates
	// the worst-case message buffer per channel per peer up front:
	// 20 MiB × MaxOutboundPeers resident before a single byte arrives,
	// which can OOM small (8 GiB) hosts under a malicious peer set.
	// Upstream cometbft's own statesync reactor leaves this unset for
	// the same reason — the buffer grows on demand up to
	// RecvMessageCapacity, so steady-state cost is per active transfer
	// rather than per connected peer.
	return []*conn.ChannelDescriptor{
		{
			ID:                  SnapshotChannel,
			Priority:            5,
			SendQueueCapacity:   10,
			RecvMessageCapacity: snapshotMsgSize,
			MessageType:         &ssproto.Message{},
		},
		{
			ID:                  ChunkChannel,
			Priority:            3,
			SendQueueCapacity:   10,
			RecvMessageCapacity: chunkMsgSize,
			MessageType:         &ssproto.Message{},
		},
	}
}

func (r *Reactor) AddPeer(peer p2p.Peer) {
	peerID := string(peer.ID())
	// Publish a Connected event so consumers (download, peerWatch)
	// can react without polling sw.Peers().List().
	select {
	case r.Out <- Event{PeerID: peerID, Connected: true}:
	default:
		r.dropsCtrl.Add(1)
		r.logger.Error("connect Out channel full; dropping", "peer", peerID)
	}
	if !r.AskOnAdd {
		return
	}
	req := &ssproto.SnapshotsRequest{}
	if peer.Send(p2p.Envelope{ChannelID: SnapshotChannel, Message: req}) {
		r.bytesSent.Add(int64(proto.Size(req)))
		r.logger.Debug("SnapshotsRequest sent", "peer", peer.ID())
	} else {
		r.logger.Error("SnapshotsRequest send queue full", "peer", peer.ID())
	}
}

// RemovePeer publishes a Removed event so consumers can clean up
// per-peer state (drop in-flight assignments, drop firstSeen, etc.).
func (r *Reactor) RemovePeer(peer p2p.Peer, reason interface{}) {
	peerID := string(peer.ID())
	select {
	case r.Out <- Event{PeerID: peerID, Removed: true}:
	default:
		r.dropsCtrl.Add(1)
		r.logger.Error("disconnect Out channel full; dropping", "peer", peerID)
	}
}

// RequestChunk dispatches a ChunkRequest for (height, format, index) to peer.
// Used after we've seen a snapshot offer to measure real chunk size.
func (r *Reactor) RequestChunk(peer p2p.Peer, height uint64, format, index uint32) bool {
	req := &ssproto.ChunkRequest{Height: height, Format: format, Index: index}
	if !peer.Send(p2p.Envelope{ChannelID: ChunkChannel, Message: req}) {
		return false
	}
	r.bytesSent.Add(int64(proto.Size(req)))
	return true
}

func (r *Reactor) Receive(env p2p.Envelope) {
	if pm, ok := env.Message.(proto.Message); ok {
		r.bytesRecv.Add(int64(proto.Size(pm)))
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
			r.dropsCtrl.Add(1)
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
		if len(m.Chunk) > 0 {
			ci.Bytes = make([]byte, len(m.Chunk))
			copy(ci.Bytes, m.Chunk)
		}
		select {
		case r.OutChunks <- Event{PeerID: peerID, Chunk: ci}:
		default:
			r.dropsChunk.Add(1)
			r.logger.Error("chunk OutChunks channel full; dropping", "peer", peerID)
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
	return r.bytesRecv.Load(), r.bytesSent.Load()
}

// Drops returns counts of events dropped because the relevant outgoing
// channel was full. ctrl covers Connected/Removed/Snapshot; chunk
// covers ChunkResponse.
func (r *Reactor) Drops() (ctrl, chunk int64) {
	return r.dropsCtrl.Load(), r.dropsChunk.Load()
}
