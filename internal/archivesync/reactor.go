package archivesync

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/p2p/conn"
	bcproto "github.com/cometbft/cometbft/proto/tendermint/blocksync"
	"github.com/cometbft/cometbft/types"
	"github.com/cosmos/gogoproto/proto"

	"github.com/zrbecker/cosmos-p2p/internal/archive"
)

// Channel is the cometbft block-sync channel ID we operate on.
const Channel = byte(0x40)

type peerState struct {
	base     int64
	tip      int64
	lastSeen time.Time
	moniker  string
	version  string
	outbound bool

	// Per-peer in-flight count to throttle.
	inflight int
}

type inflightEntry struct {
	peer     p2p.ID
	deadline time.Time
}

// Reactor is a block-sync reactor specialized for archive download. It
// pulls heights from a Queue, asks suitable peers for them via
// BlockRequest, validates the BlockResponse, and writes the raw cmtproto
// bytes into a Store.
type Reactor struct {
	p2p.BaseReactor
	logger log.Logger
	store  *archive.Store
	queue  *Queue

	mu       sync.Mutex
	peers    map[p2p.ID]*peerState
	inflight map[int64]inflightEntry

	// Counters (atomic).
	cReceived     int64
	cWritten      int64 // successful Put into the archive
	cNoBlock      int64
	cTimedOut     int64
	cWrongHeight  int64
	cDecodeFailed int64
	cBytesIn      int64

	// Tunables.
	StatusPollInterval time.Duration
	SyncTickInterval   time.Duration
	InflightTimeout    time.Duration
	MaxInflight        int // global concurrent BlockRequests
	MaxInflightPerPeer int
	BatchSize          int // max BlockRequests issued per sync tick

	// MinPeerBase: only request from peers whose StatusResponse.Base is at
	// or below this height. We set this to (oldest height we want to
	// download) to filter out non-archive nodes that won't have the data.
	MinPeerBase int64
}

func NewReactor(store *archive.Store, q *Queue, logger log.Logger) *Reactor {
	r := &Reactor{
		logger:             logger,
		store:              store,
		queue:              q,
		peers:              make(map[p2p.ID]*peerState),
		inflight:           make(map[int64]inflightEntry),
		StatusPollInterval: 5 * time.Second,
		SyncTickInterval:   200 * time.Millisecond,
		InflightTimeout:    10 * time.Second,
		MaxInflight:        256,
		MaxInflightPerPeer: 16,
		BatchSize:          128,
	}
	r.BaseReactor = *p2p.NewBaseReactor("archivesync", r)
	r.BaseReactor.SetLogger(logger)
	return r
}

// GetChannels — block-sync only.
func (r *Reactor) GetChannels() []*conn.ChannelDescriptor {
	return []*conn.ChannelDescriptor{{
		ID:                  Channel,
		Priority:            5,
		SendQueueCapacity:   1000,
		RecvBufferCapacity:  50 * 4096,
		RecvMessageCapacity: types.MaxBlockSizeBytes + 16,
		MessageType:         &bcproto.Message{},
	}}
}

func (r *Reactor) AddPeer(peer p2p.Peer) {
	ps := &peerState{outbound: peer.IsOutbound()}
	if di, ok := peer.NodeInfo().(p2p.DefaultNodeInfo); ok {
		ps.moniker = di.Moniker
		ps.version = di.Version
	}
	r.mu.Lock()
	r.peers[peer.ID()] = ps
	r.mu.Unlock()
	// Probe.
	peer.TrySend(p2p.Envelope{ChannelID: Channel, Message: &bcproto.StatusRequest{}})
}

func (r *Reactor) RemovePeer(peer p2p.Peer, reason interface{}) {
	r.mu.Lock()
	delete(r.peers, peer.ID())
	// Cancel inflights pinned to this peer; queue will retry.
	for h, e := range r.inflight {
		if e.peer == peer.ID() {
			delete(r.inflight, h)
			r.queue.Add(h) // make sure the height is still pending
		}
	}
	r.mu.Unlock()
}

func (r *Reactor) Receive(env p2p.Envelope) {
	pid := env.Src.ID()
	if pm, ok := env.Message.(proto.Message); ok {
		atomic.AddInt64(&r.cBytesIn, int64(proto.Size(pm)))
	}
	switch m := env.Message.(type) {

	case *bcproto.StatusRequest:
		// Be polite: report empty range. We're a downloader, not a server.
		env.Src.TrySend(p2p.Envelope{
			ChannelID: Channel,
			Message:   &bcproto.StatusResponse{Base: 0, Height: 0},
		})

	case *bcproto.StatusResponse:
		r.mu.Lock()
		if ps, ok := r.peers[pid]; ok {
			ps.base = m.Base
			ps.tip = m.Height
			ps.lastSeen = time.Now()
		}
		r.mu.Unlock()

	case *bcproto.BlockRequest:
		// Don't serve from the archive — that's not this reactor's job.
		env.Src.TrySend(p2p.Envelope{
			ChannelID: Channel,
			Message:   &bcproto.NoBlockResponse{Height: m.Height},
		})

	case *bcproto.BlockResponse:
		atomic.AddInt64(&r.cReceived, 1)
		if m.Block == nil {
			atomic.AddInt64(&r.cDecodeFailed, 1)
			return
		}
		h := m.Block.Header.Height
		// Free the inflight slot regardless of outcome.
		r.mu.Lock()
		entry, hadEntry := r.inflight[h]
		if hadEntry && entry.peer == pid {
			delete(r.inflight, h)
			if ps, ok := r.peers[pid]; ok && ps.inflight > 0 {
				ps.inflight--
			}
		}
		r.mu.Unlock()
		// Sanity: chain ID we expected? Cometbft doesn't surface that
		// here directly, but the caller knows. Verify height looks sane.
		if h <= 0 {
			atomic.AddInt64(&r.cDecodeFailed, 1)
			return
		}
		// Re-marshal canonically and Put.
		raw, err := proto.Marshal(m.Block)
		if err != nil {
			r.logger.Error("re-marshal block", "h", h, "err", err)
			atomic.AddInt64(&r.cDecodeFailed, 1)
			return
		}
		// Idempotent; if a different peer beat us with the same content,
		// this returns nil. Different content errors — log and skip.
		if err := r.store.Put(uint64(h), raw); err != nil {
			r.logger.Error("store.Put", "h", h, "err", err)
			return
		}
		atomic.AddInt64(&r.cWritten, 1)
		r.queue.Remove(h)

	case *bcproto.NoBlockResponse:
		atomic.AddInt64(&r.cNoBlock, 1)
		r.mu.Lock()
		if entry, ok := r.inflight[m.Height]; ok && entry.peer == pid {
			delete(r.inflight, m.Height)
			if ps, ok2 := r.peers[pid]; ok2 && ps.inflight > 0 {
				ps.inflight--
			}
		}
		r.mu.Unlock()
		// Leave height in the queue so another peer is tried.
	}
}

// SyncLoop drives status polls + work assignment until ctx is cancelled.
func (r *Reactor) SyncLoop(ctx context.Context) {
	statusT := time.NewTicker(r.StatusPollInterval)
	defer statusT.Stop()
	syncT := time.NewTicker(r.SyncTickInterval)
	defer syncT.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-statusT.C:
			r.pollStatus()
		case <-syncT.C:
			r.assignWork()
		}
	}
}

func (r *Reactor) pollStatus() {
	if r.Switch == nil {
		return
	}
	for _, p := range r.Switch.Peers().List() {
		p.TrySend(p2p.Envelope{ChannelID: Channel, Message: &bcproto.StatusRequest{}})
	}
}

func (r *Reactor) assignWork() {
	if r.Switch == nil || r.queue.Size() == 0 {
		return
	}
	// Snapshot peer state.
	r.mu.Lock()
	type pt struct {
		id       p2p.ID
		base     int64
		tip      int64
		inflight int
	}
	pts := make([]pt, 0, len(r.peers))
	for id, ps := range r.peers {
		// Filter: must report a meaningful tip and (if MinPeerBase set) be
		// archival enough.
		if ps.tip <= 0 {
			continue
		}
		if r.MinPeerBase > 0 && ps.base > r.MinPeerBase {
			continue
		}
		pts = append(pts, pt{id, ps.base, ps.tip, ps.inflight})
	}
	// GC expired inflights.
	now := time.Now()
	expired := 0
	for h, e := range r.inflight {
		if now.After(e.deadline) {
			delete(r.inflight, h)
			if ps, ok := r.peers[e.peer]; ok && ps.inflight > 0 {
				ps.inflight--
			}
			r.queue.Add(h)
			expired++
		}
	}
	atomic.AddInt64(&r.cTimedOut, int64(expired))
	inflightCount := len(r.inflight)
	r.mu.Unlock()

	if len(pts) == 0 {
		return
	}

	peerByID := make(map[p2p.ID]p2p.Peer, len(pts))
	for _, p := range r.Switch.Peers().List() {
		peerByID[p.ID()] = p
	}

	// Issue up to BatchSize requests this tick.
	issued := 0
	for issued < r.BatchSize && inflightCount+issued < r.MaxInflight {
		h, ok := r.queue.Next()
		if !ok {
			break
		}
		// Skip if already in flight.
		r.mu.Lock()
		_, busy := r.inflight[h]
		r.mu.Unlock()
		if busy {
			continue
		}
		// Pick a peer that covers h and isn't saturated.
		var picked pt
		havePicked := false
		for i := range pts {
			cand := pts[(int(h)+i+issued)%len(pts)]
			if cand.base <= h && h <= cand.tip && cand.inflight < r.MaxInflightPerPeer {
				picked = cand
				havePicked = true
				break
			}
		}
		if !havePicked {
			// No peer can serve h right now — we already removed it via
			// queue.Next, put it back at the end.
			r.queue.Add(h)
			break
		}
		peer, ok := peerByID[picked.id]
		if !ok {
			r.queue.Add(h)
			continue
		}
		req := &bcproto.BlockRequest{Height: h}
		if !peer.TrySend(p2p.Envelope{ChannelID: Channel, Message: req}) {
			r.queue.Add(h)
			continue
		}
		r.mu.Lock()
		r.inflight[h] = inflightEntry{peer: picked.id, deadline: time.Now().Add(r.InflightTimeout)}
		if ps, ok := r.peers[picked.id]; ok {
			ps.inflight++
			// Keep our local pts copy in sync so subsequent picks this
			// tick respect the cap.
			for i := range pts {
				if pts[i].id == picked.id {
					pts[i].inflight++
					break
				}
			}
		}
		r.mu.Unlock()
		issued++
	}
}

// Snapshot exports atomic counters.
type Counters struct {
	Received     int64
	Written      int64
	NoBlock      int64
	TimedOut     int64
	WrongHeight  int64
	DecodeFailed int64
	BytesIn      int64
	Peers        int
	EligibleP    int // peers passing MinPeerBase
	Inflight     int
}

func (r *Reactor) Snapshot() Counters {
	r.mu.Lock()
	peers := len(r.peers)
	elig := 0
	for _, ps := range r.peers {
		if ps.tip > 0 && (r.MinPeerBase == 0 || ps.base <= r.MinPeerBase) {
			elig++
		}
	}
	inflight := len(r.inflight)
	r.mu.Unlock()
	return Counters{
		Received:     atomic.LoadInt64(&r.cReceived),
		Written:      atomic.LoadInt64(&r.cWritten),
		NoBlock:      atomic.LoadInt64(&r.cNoBlock),
		TimedOut:     atomic.LoadInt64(&r.cTimedOut),
		WrongHeight:  atomic.LoadInt64(&r.cWrongHeight),
		DecodeFailed: atomic.LoadInt64(&r.cDecodeFailed),
		BytesIn:      atomic.LoadInt64(&r.cBytesIn),
		Peers:        peers,
		EligibleP:    elig,
		Inflight:     inflight,
	}
}

// PeerSummary exposes per-peer status for debug logging.
type PeerSummary struct {
	NodeID      string
	Moniker     string
	Version     string
	Base        int64
	Tip         int64
	Inflight    int
	Outbound    bool
	LastSeenAgo time.Duration
}

func (r *Reactor) PeerSummaries() []PeerSummary {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]PeerSummary, 0, len(r.peers))
	now := time.Now()
	for id, ps := range r.peers {
		out = append(out, PeerSummary{
			NodeID: string(id), Moniker: ps.moniker, Version: ps.version,
			Base: ps.base, Tip: ps.tip, Inflight: ps.inflight,
			Outbound: ps.outbound, LastSeenAgo: now.Sub(ps.lastSeen),
		})
	}
	return out
}

// Verify the formatter satisfies the type the linter expects.
var _ p2p.Reactor = (*Reactor)(nil)

// nudge avoids an "imported and not used" error when only the type is referenced.
var _ = fmt.Sprintf
