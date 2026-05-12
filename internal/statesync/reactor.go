// Package statesync implements a minimal CometBFT state-sync reactor
// that speaks just enough of channels 0x60 (snapshot) and 0x61 (chunk)
// for two roles:
//
//   - probing/fetching peers for available snapshots (the default
//     behavior, used by snapfetch);
//   - serving snapshots from a local store back to other peers (opt-in
//     via SetProvider, used by snapserve).
//
// Both roles can coexist on one reactor in principle, but malcom's two
// CLI subcommands run them in isolation: fetch is probe-only, serve is
// provider-only with probe disabled.
package statesync

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/p2p/conn"
	ssproto "github.com/cometbft/cometbft/proto/tendermint/statesync"
	"github.com/cosmos/gogoproto/proto"
	"golang.org/x/time/rate"
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

// SnapshotProvider is the inbound-request handler for serve mode. It
// owns whatever store backs the local snapshots — see internal/snapserve.
//
// ListSnapshots returns the catalogue we'll advertise on every inbound
// SnapshotsRequest. Callers must not mutate the returned slice or any
// element; the reactor copies the wire fields when building responses.
//
// LoadChunk returns the raw chunk bytes for (height, format, index).
// found=false signals the chunk doesn't exist for that snapshot — the
// reactor then sends a ChunkResponse with Missing=true and no payload.
// A non-nil err means we couldn't read the chunk we should have had
// (transient I/O, corruption); the reactor logs and treats it as
// Missing on the wire so the requester moves on instead of waiting
// for our timeout.
type SnapshotProvider interface {
	ListSnapshots() []Snapshot
	LoadChunk(height uint64, format, index uint32) (data []byte, found bool, err error)
}

// Reactor probes peers for snapshots. AddPeer fires a SnapshotsRequest
// (probe mode); inbound SnapshotsResponse is forwarded on Out.
// ChunkRequest can be dispatched explicitly via RequestChunk;
// ChunkResponse is forwarded on OutChunks. The two channels are
// separate so a 16 MiB chunk burst can't queue tiny control events
// behind it.
//
// In serve mode, SetProvider installs a SnapshotProvider and SetProbe
// disables the AddPeer probe. Inbound SnapshotsRequest then triggers
// one SnapshotsResponse per snapshot in the provider's catalogue; an
// inbound ChunkRequest triggers a ChunkResponse with the chunk bytes
// (or Missing=true).
type Reactor struct {
	p2p.BaseReactor
	logger    log.Logger
	Out       chan Event // Connected, Removed, Snapshot
	OutChunks chan Event // Chunk

	// provider is swappable at runtime via SetProvider — see the
	// snapserve Catalog, which rescans its root dir periodically and
	// installs a fresh Store on each successful scan. We hold the
	// interface in a one-field wrapper so atomic.Pointer can give us
	// lock-free reads on the hot Receive path.
	provider atomic.Pointer[providerSlot]
	probe    bool

	// shuttingDown signals the serve-side drain phase: once set, any
	// inbound ChunkRequest is fast-failed with Missing=true so the
	// requester refetches elsewhere instead of waiting for our
	// per-chunk timeout. Set by BeginShutdown; the reactor doesn't
	// clear it (a graceful drain is always followed by sw.Stop).
	// SnapshotsRequest stays answered because operators may still
	// want to advertise during drain.
	shuttingDown atomic.Bool

	// Rate-limit state. peerLimiters maps p2p.ID → *peerLimiterEntry,
	// installed lazily on first ChunkRequest when SetChunkRateLimit
	// has been called; sync.Map's LoadOrStore handles the get-or-
	// create race when two ChunkRequests for the same fresh peer
	// race into the hot path. globalLim is a single reactor-wide
	// bucket. Both are nil-equivalent (no globalLim, peerLimiters
	// empty + chunkRatePerPeer/Burst zeroed) by default — the
	// fetch-side reactor never calls SetChunkRateLimit, so neither
	// bucket fires and Receive doesn't even cross the limiter branch.
	//
	// Entries persist past RemovePeer until sweepStaleLimiters evicts
	// them (see #108 C4): deleting on disconnect would let a peer
	// with a stable Node ID disconnect/reconnect to refresh its
	// burst budget.
	globalLim         *rate.Limiter
	peerLimiters      sync.Map
	chunkRatePerPeer  rate.Limit
	chunkBurstPerPeer int

	// setRateCalled is the single-call guard for SetChunkRateLimit.
	// The plain-field writes inside SetChunkRateLimit have no
	// happens-before with the Receive goroutines if called after
	// sw.Start, AND calling it twice would leave existing per-peer
	// limiters with the old rate baked in while new peers get the
	// new rate (inconsistent). Enforcing single-call semantics keeps
	// the contract honest. The atomic.Bool is fine here because the
	// race we're guarding against is "more than one caller invokes
	// this method"; the *contents* of SetChunkRateLimit don't need
	// to be lock-protected because the documented contract is
	// "before sw.Start" (memory ordering via goroutine creation).
	setRateCalled atomic.Bool

	bytesRecv  atomic.Int64
	bytesSent  atomic.Int64
	dropsCtrl  atomic.Int64
	dropsChunk atomic.Int64

	snapshotsServed   atomic.Int64
	chunksServed      atomic.Int64
	chunksMissing     atomic.Int64
	chunksDrained     atomic.Int64
	droppedRatePeer   atomic.Int64
	droppedRateGlobal atomic.Int64
}

// ChunkRateLimit configures the inbound ChunkRequest rate limiter.
// Both buckets are independent and additive: a request must pass
// both to be served. Set rates/bursts to 0 to disable the
// corresponding bucket.
//
// Per-peer protects against a single peer hammering us with chunk
// requests; global is the safety net against many peers each below
// their per-peer cap but aggregating to more than we want to serve.
// See issue #85 for the threat model.
type ChunkRateLimit struct {
	// PerPeerRate is the steady-state requests-per-second allowed
	// from any one peer.ID. Zero disables the per-peer bucket.
	PerPeerRate float64
	// PerPeerBurst is the maximum burst (tokens at full bucket) per
	// peer.ID. Must be ≥ 1 when PerPeerRate > 0.
	PerPeerBurst int
	// GlobalRate is the steady-state requests-per-second allowed
	// across all peers. Zero disables the global bucket.
	GlobalRate float64
	// GlobalBurst is the maximum burst across all peers. Must be
	// ≥ 1 when GlobalRate > 0.
	GlobalBurst int
}

// providerSlot wraps SnapshotProvider so we can store it in an
// atomic.Pointer. atomic.Pointer needs a concrete type, and we want
// the interface flexibility, so the indirection costs one allocation
// per swap (cheap — swaps happen on the rescan cadence, not per
// request).
type providerSlot struct{ p SnapshotProvider }

const outChunksCapacity = 8

// NewReactor constructs a probe-mode reactor (the default for fetch).
// SnapshotsRequest fires on every AddPeer; inbound responses route to
// Out / OutChunks. To run in serve mode, also call SetProvider and
// SetProbe(false).
func NewReactor(logger log.Logger) *Reactor {
	r := &Reactor{
		logger:    logger,
		Out:       make(chan Event, 256),
		OutChunks: make(chan Event, outChunksCapacity),
		probe:     true,
	}
	r.BaseReactor = *p2p.NewBaseReactor("statesync-probe", r)
	r.BaseReactor.SetLogger(logger)
	return r
}

// SetProvider installs a SnapshotProvider so the reactor responds to
// inbound SnapshotsRequest / ChunkRequest from peers. Safe to call any
// time — including while the reactor is running and serving requests
// — so a Catalog rescan can swap the active store without dropping
// peers. Pass nil to clear (probe-only).
func (r *Reactor) SetProvider(p SnapshotProvider) {
	if p == nil {
		r.provider.Store(nil)
		return
	}
	r.provider.Store(&providerSlot{p: p})
}

// loadProvider returns the current SnapshotProvider (or nil). One
// atomic load on the Receive hot path; the interface wrapper costs
// nothing once the slot is in cache.
func (r *Reactor) loadProvider() SnapshotProvider {
	slot := r.provider.Load()
	if slot == nil {
		return nil
	}
	return slot.p
}

// SetProbe controls whether AddPeer fires a SnapshotsRequest. Probe is
// true by default (fetch mode); serve mode passes false so we don't
// pester peers for their snapshots when we have nothing to do with
// them.
func (r *Reactor) SetProbe(probe bool) { r.probe = probe }

// SetChunkRateLimit installs token-bucket limits on inbound
// ChunkRequest. Single-call: must be called at most once, before
// sw.Start. A second call logs an error and returns without touching
// state — there's no safe way to reconfigure rates mid-flight
// because existing per-peer limiters in the sync.Map have the old
// rate baked in.
//
// A bucket with rate ≤ 0 or burst ≤ 0 is treated as disabled. The
// zero-value ChunkRateLimit{} is equivalent to never calling this —
// matches the default fetch-mode reactor behaviour (no
// SetChunkRateLimit call, both buckets nil). Operators who explicitly
// want "no rate limiting" can call it with a zero-value struct, or
// just skip the call.
func (r *Reactor) SetChunkRateLimit(c ChunkRateLimit) {
	if !r.setRateCalled.CompareAndSwap(false, true) {
		r.logger.Error("SetChunkRateLimit called more than once; ignoring (rate parameters must be set once, before sw.Start)")
		return
	}
	if c.PerPeerRate > 0 && c.PerPeerBurst > 0 {
		r.chunkRatePerPeer = rate.Limit(c.PerPeerRate)
		r.chunkBurstPerPeer = c.PerPeerBurst
	}
	if c.GlobalRate > 0 && c.GlobalBurst > 0 {
		r.globalLim = rate.NewLimiter(rate.Limit(c.GlobalRate), c.GlobalBurst)
	}
}

// peerLimiterEntry wraps a *rate.Limiter with a tombstone timestamp
// so RemovePeer can defer eviction without losing the bucket state.
// removedAt is unix nanos: zero means the peer is still considered
// connected; nonzero is the moment RemovePeer fired. The sweeper
// evicts entries whose tombstone is older than 2 × (burst/rate) —
// by which point the bucket has refilled past full and a same-ID
// reconnect getting a fresh limiter is observationally identical
// to the original peer hitting the steady-state rate.
//
// atomic.Int64 lets the sweeper (Range under sync.Map) read the
// tombstone concurrently with RemovePeer's set and AddPeer's clear.
type peerLimiterEntry struct {
	lim       *rate.Limiter
	removedAt atomic.Int64
}

// peerLimiter returns the per-peer.ID rate limiter, creating one on
// first use. sync.Map.LoadOrStore makes the get-or-create atomic
// under concurrent first-touch from the same peer (two goroutines
// race to LoadOrStore; whichever stores first wins, the other's
// freshly-allocated entry is GC'd). Returns nil when per-peer rate
// limiting is disabled.
func (r *Reactor) peerLimiter(id p2p.ID) *rate.Limiter {
	if r.chunkRatePerPeer <= 0 || r.chunkBurstPerPeer <= 0 {
		return nil
	}
	if v, ok := r.peerLimiters.Load(id); ok {
		return v.(*peerLimiterEntry).lim
	}
	fresh := &peerLimiterEntry{lim: rate.NewLimiter(r.chunkRatePerPeer, r.chunkBurstPerPeer)}
	actual, _ := r.peerLimiters.LoadOrStore(id, fresh)
	return actual.(*peerLimiterEntry).lim
}

// sweepStaleLimiters evicts entries whose peer has been disconnected
// for at least 2 × (burst/rate) — the time it takes an empty bucket
// to refill to full. After that the limiter holds no rate-limit
// state worth preserving, so a same-NodeID reconnect getting a
// fresh limiter is indistinguishable from the original peer hitting
// the steady-state rate.
//
// Called from RemovePeer (the only place the tombstoned set grows),
// so a long-running daemon's peerLimiters stays bounded by recent
// disconnect activity.
func (r *Reactor) sweepStaleLimiters() {
	if r.chunkRatePerPeer <= 0 || r.chunkBurstPerPeer <= 0 {
		return
	}
	refill := time.Duration(float64(r.chunkBurstPerPeer) / float64(r.chunkRatePerPeer) * float64(time.Second))
	cutoff := time.Now().Add(-2 * refill).UnixNano()
	r.peerLimiters.Range(func(k, v any) bool {
		e := v.(*peerLimiterEntry)
		t := e.removedAt.Load()
		if t > 0 && t < cutoff {
			r.peerLimiters.Delete(k)
		}
		return true
	})
}

// BeginShutdown puts the reactor into drain mode: subsequent
// ChunkRequest messages are fast-failed with Missing=true (counted in
// the chunksDrained metric, not chunksServed/chunksMissing) so peers
// stop streaming bytes through us and refetch elsewhere. Idempotent.
//
// The caller is expected to then wait its drain window before
// stopping the switch — see snapserve.RunServe's cleanup defer.
func (r *Reactor) BeginShutdown() { r.shuttingDown.Store(true) }

// IsShuttingDown reports whether BeginShutdown has been called. Used
// by snapserve's stats logging during the drain window.
func (r *Reactor) IsShuttingDown() bool { return r.shuttingDown.Load() }

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
	// If a same-NodeID limiter is still in the map from a prior
	// disconnect, clear its tombstone so the sweeper doesn't evict
	// it while the peer is connected (and reset its bucket the
	// next time RemovePeer fires). #108 C4.
	if v, ok := r.peerLimiters.Load(peer.ID()); ok {
		v.(*peerLimiterEntry).removedAt.Store(0)
	}
	if !r.probe {
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
//
// The per-peer rate limiter is NOT deleted — only tombstoned.
// Deleting immediately would let a peer with a stable Node ID
// disconnect and reconnect to refresh its burst budget (#108 C4).
// Eviction is deferred to sweepStaleLimiters, which runs here at
// the moment the tombstoned-set grows by one.
func (r *Reactor) RemovePeer(peer p2p.Peer, reason interface{}) {
	peerID := string(peer.ID())
	select {
	case r.Out <- Event{PeerID: peerID, Removed: true}:
	default:
		r.dropsCtrl.Add(1)
		r.logger.Error("disconnect Out channel full; dropping", "peer", peerID)
	}
	if v, ok := r.peerLimiters.Load(peer.ID()); ok {
		v.(*peerLimiterEntry).removedAt.Store(time.Now().UnixNano())
	}
	r.sweepStaleLimiters()
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
	r.bytesRecv.Add(int64(proto.Size(env.Message)))
	peerID := string(env.Src.ID())
	switch m := env.Message.(type) {
	case *ssproto.SnapshotsResponse:
		r.logger.Debug("snapshot offered",
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

	case *ssproto.SnapshotsRequest:
		prov := r.loadProvider()
		if prov == nil {
			// Probe-only build: cometbft does the same when its
			// ListSnapshots ABCI call returns empty — silence rather than
			// an empty response.
			return
		}
		snaps := prov.ListSnapshots()
		for i := range snaps {
			s := &snaps[i]
			resp := &ssproto.SnapshotsResponse{
				Height:   s.Height,
				Format:   s.Format,
				Chunks:   s.Chunks,
				Hash:     s.Hash,
				Metadata: s.Metadata,
			}
			if env.Src.Send(p2p.Envelope{ChannelID: SnapshotChannel, Message: resp}) {
				r.bytesSent.Add(int64(proto.Size(resp)))
				r.snapshotsServed.Add(1)
				r.logger.Debug("SnapshotsResponse sent",
					"peer", peerID, "height", s.Height, "format", s.Format,
					"chunks", s.Chunks)
			} else {
				r.logger.Error("SnapshotsResponse send queue full",
					"peer", peerID, "height", s.Height, "format", s.Format)
			}
		}

	case *ssproto.ChunkRequest:
		prov := r.loadProvider()
		if prov == nil {
			return
		}
		// Drain mode: fast-fail every inbound ChunkRequest so the
		// peer's per-chunk timeout doesn't gate their refetch
		// elsewhere. Tracked under chunksDrained (separate counter)
		// so operators can spot "requests during drain" without it
		// muddying the steady-state chunksMissing metric.
		//
		// Drain wins over rate-limit because drain is cooperative
		// (Missing → peer refetches elsewhere immediately) while
		// rate-limit is silent (peer waits its own per-chunk
		// timeout). We'd rather signal "go elsewhere, we're done"
		// to a polite peer than drop their request on the floor.
		if r.shuttingDown.Load() {
			resp := &ssproto.ChunkResponse{
				Height:  m.Height,
				Format:  m.Format,
				Index:   m.Index,
				Missing: true,
			}
			if env.Src.Send(p2p.Envelope{ChannelID: ChunkChannel, Message: resp}) {
				r.bytesSent.Add(int64(proto.Size(resp)))
				r.chunksDrained.Add(1)
				r.logger.Debug("ChunkResponse drained (shutting down)",
					"peer", peerID, "height", m.Height, "format", m.Format,
					"index", m.Index)
			}
			return
		}

		// Rate limit (#85). Two-bucket guard, global first (cheap —
		// single shared limiter, no map lookup), then per-peer
		// (sync.Map get-or-create). Denials drop the request
		// *silently* — no Missing response — so the peer's own
		// pipelining backoff sees the throttle as a timeout rather
		// than a "try a different index". A Missing response would
		// tell an abusive peer to immediately ask for a different
		// chunk, defeating the bucket.
		if r.globalLim != nil && !r.globalLim.Allow() {
			r.droppedRateGlobal.Add(1)
			r.logger.Debug("ChunkRequest dropped: global rate limit",
				"peer", peerID, "height", m.Height, "format", m.Format, "index", m.Index)
			return
		}
		if peerLim := r.peerLimiter(env.Src.ID()); peerLim != nil && !peerLim.Allow() {
			r.droppedRatePeer.Add(1)
			r.logger.Debug("ChunkRequest dropped: per-peer rate limit",
				"peer", peerID, "height", m.Height, "format", m.Format, "index", m.Index)
			return
		}

		data, found, err := prov.LoadChunk(m.Height, m.Format, m.Index)
		if err != nil {
			// Treat read errors as Missing on the wire so requesters
			// move on instead of waiting for their per-chunk timeout.
			// Operator gets the actual error in our logs.
			r.logger.Error("LoadChunk failed; serving Missing",
				"peer", peerID, "height", m.Height, "format", m.Format,
				"index", m.Index, "err", err)
			found = false
			data = nil
		}
		resp := &ssproto.ChunkResponse{
			Height:  m.Height,
			Format:  m.Format,
			Index:   m.Index,
			Chunk:   data,
			Missing: !found,
		}
		if env.Src.Send(p2p.Envelope{ChannelID: ChunkChannel, Message: resp}) {
			r.bytesSent.Add(int64(proto.Size(resp)))
			if found {
				r.chunksServed.Add(1)
			} else {
				r.chunksMissing.Add(1)
			}
			r.logger.Debug("ChunkResponse sent",
				"peer", peerID, "height", m.Height, "format", m.Format,
				"index", m.Index, "bytes", len(data), "missing", !found)
		} else {
			r.logger.Error("ChunkResponse send queue full",
				"peer", peerID, "height", m.Height, "format", m.Format,
				"index", m.Index)
		}

	default:
		r.logger.Debug("unexpected statesync msg", "peer", peerID, "type", m)
	}
}

// Served returns counters for serve-mode bookkeeping: how many
// SnapshotsResponse and ChunkResponse messages we've sent in reply to
// inbound requests, plus chunks served as Missing (we don't have them
// or read failed).
func (r *Reactor) Served() (snapshots, chunks, missing int64) {
	return r.snapshotsServed.Load(), r.chunksServed.Load(), r.chunksMissing.Load()
}

// Drained returns the number of ChunkRequest messages fast-failed
// with Missing=true because BeginShutdown had been called. Separate
// from chunksMissing so steady-state misses (chunk index out of
// range, read errors) don't conflate with drain-window activity.
func (r *Reactor) Drained() int64 { return r.chunksDrained.Load() }

// RateDropped returns the per-bucket count of inbound ChunkRequest
// messages dropped silently because they exceeded the configured
// rate limits (#85). Bucketed separately so an operator can tell
// "a single peer is hammering us" (peer) from "we're saturated
// overall" (global). Returns (0, 0) when rate limiting is disabled
// (the fetch-mode reactor case).
func (r *Reactor) RateDropped() (perPeer, global int64) {
	return r.droppedRatePeer.Load(), r.droppedRateGlobal.Load()
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
