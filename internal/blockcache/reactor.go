// Reactor implements the cometbft block-sync protocol on channel 0x40 as a
// cache-and-serve participant: we ask peers for blocks to keep our sliding
// window full, and we serve back to peers from the cache when they ask us.
package blockcache

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/p2p/conn"
	bcproto "github.com/cometbft/cometbft/proto/tendermint/blocksync"
	"github.com/cometbft/cometbft/types"
	"github.com/cosmos/gogoproto/proto"
)

const Channel = byte(0x40)

type peerState struct {
	base     int64
	tip      int64
	lastSeen time.Time

	// Direction (true = we dialed them; false = they dialed us).
	Outbound bool

	// Inbound activity from this peer.
	StatusReqsRecv int64
	BlockReqsRecv  int64
	BlocksServed   int64
	NoBlockSent    int64
	BytesServedOut int64

	Moniker     string
	NodeVersion string
	RemoteAddr  string
	FirstSeen   time.Time
}

type Reactor struct {
	p2p.BaseReactor
	cache  *Cache
	logger log.Logger

	mu       sync.Mutex
	peers    map[p2p.ID]*peerState
	inflight map[int64]inflightEntry // height → peer asked + deadline

	// archiveLogged is the set of node IDs we've already announced as archive
	// (base < ArchiveThreshold). One [ARCHIVE] line per peer per run.
	archiveLogged map[p2p.ID]struct{}
	// inboundLogged is the set of node IDs we've already announced as a new
	// inbound connection.
	inboundLogged map[p2p.ID]struct{}

	// ArchiveThreshold: base_height below this triggers the [ARCHIVE] log.
	ArchiveThreshold int64

	// counters for human consumption
	servedBlocks int64
	missedBlocks int64
	fetched      int64
	noblock      int64
	statusServed int64

	// Tunables
	StatusPollInterval time.Duration // how often we re-StatusRequest peers
	SyncTickInterval   time.Duration // how often we evaluate gaps
	InflightTimeout    time.Duration // when to abandon a block request
	MaxInflight        int           // global concurrent BlockRequests
	BatchSize          int           // max BlockRequests issued per tick
}

type inflightEntry struct {
	peer     p2p.ID
	deadline time.Time
}

func NewReactor(cache *Cache, logger log.Logger) *Reactor {
	r := &Reactor{
		cache:         cache,
		logger:        logger,
		peers:         make(map[p2p.ID]*peerState),
		inflight:      make(map[int64]inflightEntry),
		archiveLogged: make(map[p2p.ID]struct{}),
		inboundLogged: make(map[p2p.ID]struct{}),

		StatusPollInterval: 5 * time.Second,
		SyncTickInterval:   500 * time.Millisecond,
		InflightTimeout:    8 * time.Second,
		MaxInflight:        64,
		BatchSize:          32,
		ArchiveThreshold:   6_000_000,
	}
	r.BaseReactor = *p2p.NewBaseReactor("blockcache", r)
	r.BaseReactor.SetLogger(logger)
	return r
}

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
	r.mu.Lock()
	ps := &peerState{
		FirstSeen:  time.Now(),
		Outbound:   peer.IsOutbound(),
		RemoteAddr: peer.RemoteAddr().String(),
	}
	if di, ok := peer.NodeInfo().(p2p.DefaultNodeInfo); ok {
		ps.Moniker = di.Moniker
		ps.NodeVersion = di.Version
	}
	r.peers[peer.ID()] = ps
	firstInbound := false
	if !peer.IsOutbound() {
		if _, seen := r.inboundLogged[peer.ID()]; !seen {
			r.inboundLogged[peer.ID()] = struct{}{}
			firstInbound = true
		}
	}
	r.mu.Unlock()
	dir := "out"
	if !peer.IsOutbound() {
		dir = "in"
	}
	r.logger.Info("peer added", "id", peer.ID(), "dir", dir, "addr", peer.RemoteAddr().String(), "moniker", ps.Moniker)
	if firstInbound {
		// Eye-catching log line so it's obvious in the [stat] stream that
		// somebody dialed us. Includes their advertised ListenAddr so we
		// can later test if they're outbound-reachable.
		listenAddr := ""
		if di, ok := peer.NodeInfo().(p2p.DefaultNodeInfo); ok {
			listenAddr = di.ListenAddr
		}
		fmt.Printf("[INBOUND] %s  remote=%s  advertised=%s  moniker=%s  ver=%s\n",
			peer.ID(), peer.RemoteAddr().String(), listenAddr, ps.Moniker, ps.NodeVersion)
	}

	// Tell them what we have, then ask what they have.
	base, tip := r.cache.Range()
	peer.Send(p2p.Envelope{
		ChannelID: Channel,
		Message:   &bcproto.StatusResponse{Base: base, Height: tip},
	})
	peer.Send(p2p.Envelope{
		ChannelID: Channel,
		Message:   &bcproto.StatusRequest{},
	})
}

func (r *Reactor) RemovePeer(peer p2p.Peer, reason interface{}) {
	r.mu.Lock()
	delete(r.peers, peer.ID())
	// Cancel inflight requests we'd assigned to this peer so the next tick
	// can reroute them.
	for h, e := range r.inflight {
		if e.peer == peer.ID() {
			delete(r.inflight, h)
		}
	}
	r.mu.Unlock()
}

func (r *Reactor) Receive(env p2p.Envelope) {
	pid := env.Src.ID()
	switch m := env.Message.(type) {

	case *bcproto.StatusRequest:
		// Peer wants to know our (base, tip).
		base, tip := r.cache.Range()
		env.Src.Send(p2p.Envelope{
			ChannelID: Channel,
			Message:   &bcproto.StatusResponse{Base: base, Height: tip},
		})
		r.mu.Lock()
		r.statusServed++
		if ps, ok := r.peers[pid]; ok {
			ps.StatusReqsRecv++
		}
		r.mu.Unlock()

	case *bcproto.StatusResponse:
		r.mu.Lock()
		var firstArchive bool
		var psSnap peerState
		if ps, ok := r.peers[pid]; ok {
			ps.base = m.Base
			ps.tip = m.Height
			ps.lastSeen = time.Now()
			if m.Base > 0 && m.Base < r.ArchiveThreshold {
				if _, already := r.archiveLogged[pid]; !already {
					r.archiveLogged[pid] = struct{}{}
					firstArchive = true
					psSnap = *ps
				}
			}
		}
		r.mu.Unlock()
		if firstArchive {
			dir := "out"
			if !psSnap.Outbound {
				dir = "in"
			}
			fmt.Printf("[ARCHIVE] %s dir=%s base=%d tip=%d moniker=%s ver=%s\n",
				pid, dir, m.Base, m.Height, psSnap.Moniker, psSnap.NodeVersion)
		}

	case *bcproto.BlockRequest:
		// Peer wants block H. Serve from cache if we have it.
		if m.Height <= 0 {
			return
		}
		parsed := r.cache.GetParsed(m.Height)
		raw := r.cache.GetRaw(m.Height)
		if parsed != nil {
			env.Src.Send(p2p.Envelope{
				ChannelID: Channel,
				Message:   &bcproto.BlockResponse{Block: parsed},
			})
			r.mu.Lock()
			r.servedBlocks++
			if ps, ok := r.peers[pid]; ok {
				ps.BlockReqsRecv++
				ps.BlocksServed++
				ps.BytesServedOut += int64(len(raw))
			}
			r.mu.Unlock()
		} else {
			env.Src.Send(p2p.Envelope{
				ChannelID: Channel,
				Message:   &bcproto.NoBlockResponse{Height: m.Height},
			})
			r.mu.Lock()
			r.missedBlocks++
			if ps, ok := r.peers[pid]; ok {
				ps.BlockReqsRecv++
				ps.NoBlockSent++
			}
			r.mu.Unlock()
		}

	case *bcproto.BlockResponse:
		if m.Block == nil {
			return
		}
		h := m.Block.Header.Height
		raw, err := proto.Marshal(m.Block)
		if err != nil {
			r.logger.Error("re-marshal incoming block", "h", h, "err", err)
			return
		}
		if err := r.cache.Put(h, raw); err != nil {
			r.logger.Error("cache put", "h", h, "err", err)
			return
		}
		r.mu.Lock()
		delete(r.inflight, h)
		r.fetched++
		r.mu.Unlock()

	case *bcproto.NoBlockResponse:
		r.mu.Lock()
		delete(r.inflight, m.Height)
		r.noblock++
		r.mu.Unlock()
	}
}

// SyncLoop is called by main as a goroutine. It periodically evaluates the
// gap between our cache and the network's tip and issues BlockRequests to
// peers that can serve them.
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
			r.fillGaps()
		}
	}
}

// pollStatus asks every connected peer for its current (base, tip) so we
// know who can serve which heights.
func (r *Reactor) pollStatus() {
	if r.Switch == nil {
		return
	}
	for _, peer := range r.Switch.Peers().List() {
		peer.TrySend(p2p.Envelope{
			ChannelID: Channel,
			Message:   &bcproto.StatusRequest{},
		})
	}
}

// fillGaps figures out the network tip from peer state and fires off
// BlockRequests for heights we don't have yet.
func (r *Reactor) fillGaps() {
	if r.Switch == nil {
		return
	}
	r.mu.Lock()
	// Snapshot peer state.
	type pt struct {
		id        p2p.ID
		base, tip int64
	}
	pts := make([]pt, 0, len(r.peers))
	var netTip int64
	for id, ps := range r.peers {
		if ps.tip == 0 {
			continue
		}
		pts = append(pts, pt{id, ps.base, ps.tip})
		if ps.tip > netTip {
			netTip = ps.tip
		}
	}
	// Garbage-collect expired inflights.
	now := time.Now()
	for h, e := range r.inflight {
		if now.After(e.deadline) {
			delete(r.inflight, h)
		}
	}
	inflightCount := len(r.inflight)
	r.mu.Unlock()

	if netTip == 0 || len(pts) == 0 {
		return
	}

	// Target window: [netTip - cap + 1, netTip].
	targetBase := netTip - int64(r.cache.Capacity()) + 1
	if targetBase < 1 {
		targetBase = 1
	}
	// Within a single tick we fetch a small batch of missing heights so we
	// progress steadily without flooding peers.
	want := r.cache.MissingHeights(targetBase, netTip, r.BatchSize*4)
	if len(want) == 0 {
		return
	}

	requested := 0
	r.mu.Lock()
	defer r.mu.Unlock()
	peerObjs := r.Switch.Peers().List()
	peerByID := make(map[p2p.ID]p2p.Peer, len(peerObjs))
	for _, p := range peerObjs {
		peerByID[p.ID()] = p
	}

	for _, h := range want {
		if requested >= r.BatchSize || inflightCount+requested >= r.MaxInflight {
			break
		}
		if _, busy := r.inflight[h]; busy {
			continue
		}
		// Round-robin pick a peer that covers h.
		var picked p2p.Peer
		for i := range pts {
			p := pts[(int(h)+i)%len(pts)]
			if p.base <= h && h <= p.tip {
				if pp, ok := peerByID[p.id]; ok {
					picked = pp
					break
				}
			}
		}
		if picked == nil {
			continue
		}
		ok := picked.TrySend(p2p.Envelope{
			ChannelID: Channel,
			Message:   &bcproto.BlockRequest{Height: h},
		})
		if !ok {
			continue
		}
		r.inflight[h] = inflightEntry{
			peer:     picked.ID(),
			deadline: time.Now().Add(r.InflightTimeout),
		}
		requested++
	}
	if requested > 0 {
		r.logger.Debug("fillGaps", "requested", requested, "inflight", len(r.inflight), "cache_tip", r.cache.Stats().Tip, "net_tip", netTip)
	}
}

// CountersString returns a human-readable summary line of work done so far.
func (r *Reactor) CountersString() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return fmt.Sprintf("fetched=%d served=%d missed=%d status_served=%d noblock=%d inflight=%d peers=%d",
		r.fetched, r.servedBlocks, r.missedBlocks, r.statusServed, r.noblock, len(r.inflight), len(r.peers))
}

// Counters is a snapshot of global counters at one moment.
type Counters struct {
	Fetched       int64
	ServedBlocks  int64
	MissedBlocks  int64
	StatusServed  int64
	NoBlock       int64
	InflightCount int
	PeerCount     int
	OutboundPeers int
	InboundPeers  int
}

func (r *Reactor) Counters() Counters {
	r.mu.Lock()
	defer r.mu.Unlock()
	c := Counters{
		Fetched:       r.fetched,
		ServedBlocks:  r.servedBlocks,
		MissedBlocks:  r.missedBlocks,
		StatusServed:  r.statusServed,
		NoBlock:       r.noblock,
		InflightCount: len(r.inflight),
		PeerCount:     len(r.peers),
	}
	for _, ps := range r.peers {
		if ps.Outbound {
			c.OutboundPeers++
		} else {
			c.InboundPeers++
		}
	}
	return c
}

// PeerActivity is the per-peer view exposed to main for logging / dumping.
type PeerActivity struct {
	NodeID         string    `json:"node_id"`
	Direction      string    `json:"direction"` // "out" or "in"
	RemoteAddr     string    `json:"remote_addr"`
	Moniker        string    `json:"moniker"`
	NodeVersion    string    `json:"node_version"`
	Base           int64     `json:"base"`
	Tip            int64     `json:"tip"`
	StatusReqsRecv int64     `json:"status_reqs_recv"`
	BlockReqsRecv  int64     `json:"block_reqs_recv"`
	BlocksServed   int64     `json:"blocks_served"`
	NoBlockSent    int64     `json:"no_block_sent"`
	BytesServedOut int64     `json:"bytes_served_out"`
	FirstSeen      time.Time `json:"first_seen"`
	LastSeen       time.Time `json:"last_seen"`
}

func (r *Reactor) PeerActivity() []PeerActivity {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]PeerActivity, 0, len(r.peers))
	for id, ps := range r.peers {
		dir := "in"
		if ps.Outbound {
			dir = "out"
		}
		out = append(out, PeerActivity{
			NodeID:         string(id),
			Direction:      dir,
			RemoteAddr:     ps.RemoteAddr,
			Moniker:        ps.Moniker,
			NodeVersion:    ps.NodeVersion,
			Base:           ps.base,
			Tip:            ps.tip,
			StatusReqsRecv: ps.StatusReqsRecv,
			BlockReqsRecv:  ps.BlockReqsRecv,
			BlocksServed:   ps.BlocksServed,
			NoBlockSent:    ps.NoBlockSent,
			BytesServedOut: ps.BytesServedOut,
			FirstSeen:      ps.FirstSeen,
			LastSeen:       ps.lastSeen,
		})
	}
	return out
}
