package statesync

import (
	"net"
	"sync"
	"testing"
	"time"

	cmtlog "github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/p2p/conn"
	ssproto "github.com/cometbft/cometbft/proto/tendermint/statesync"
)

// recordingPeer is a p2p.Peer stub that records every Send/TrySend
// envelope so a test can assert what we wrote back. Send always
// succeeds (returns true).
type recordingPeer struct {
	id   p2p.ID
	mu   sync.Mutex
	sent []p2p.Envelope
	deny bool // when true, Send returns false (simulates full queue)
}

func newRecordingPeer(id string) *recordingPeer { return &recordingPeer{id: p2p.ID(id)} }

func (p *recordingPeer) ID() p2p.ID                    { return p.id }
func (p *recordingPeer) RemoteIP() net.IP              { return net.IPv4(127, 0, 0, 1) }
func (p *recordingPeer) RemoteAddr() net.Addr          { return nil }
func (p *recordingPeer) IsOutbound() bool              { return false }
func (p *recordingPeer) IsPersistent() bool            { return false }
func (p *recordingPeer) CloseConn() error              { return nil }
func (p *recordingPeer) NodeInfo() p2p.NodeInfo        { return p2p.DefaultNodeInfo{DefaultNodeID: p.id} }
func (p *recordingPeer) Status() conn.ConnectionStatus { return conn.ConnectionStatus{} }
func (p *recordingPeer) SocketAddr() *p2p.NetAddress   { return nil }
func (p *recordingPeer) Send(env p2p.Envelope) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.deny {
		return false
	}
	p.sent = append(p.sent, env)
	return true
}
func (p *recordingPeer) TrySend(env p2p.Envelope) bool { return p.Send(env) }
func (p *recordingPeer) Set(string, interface{})       {}
func (p *recordingPeer) Get(string) interface{}        { return nil }
func (p *recordingPeer) SetRemovalFailed()             {}
func (p *recordingPeer) GetRemovalFailed() bool        { return false }
func (p *recordingPeer) FlushStop()                    {}
func (p *recordingPeer) Start() error                  { return nil }
func (p *recordingPeer) OnStart() error                { return nil }
func (p *recordingPeer) Stop() error                   { return nil }
func (p *recordingPeer) OnStop()                       {}
func (p *recordingPeer) Reset() error                  { return nil }
func (p *recordingPeer) OnReset() error                { return nil }
func (p *recordingPeer) IsRunning() bool               { return true }
func (p *recordingPeer) Quit() <-chan struct{}         { return nil }
func (p *recordingPeer) String() string                { return string(p.id) }
func (p *recordingPeer) SetLogger(cmtlog.Logger)       {}

func (p *recordingPeer) snapshotResponses() []*ssproto.SnapshotsResponse {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []*ssproto.SnapshotsResponse
	for _, e := range p.sent {
		if r, ok := e.Message.(*ssproto.SnapshotsResponse); ok {
			out = append(out, r)
		}
	}
	return out
}

func (p *recordingPeer) chunkResponses() []*ssproto.ChunkResponse {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []*ssproto.ChunkResponse
	for _, e := range p.sent {
		if r, ok := e.Message.(*ssproto.ChunkResponse); ok {
			out = append(out, r)
		}
	}
	return out
}

var _ p2p.Peer = (*recordingPeer)(nil)

// fakeProvider is a minimal SnapshotProvider for tests.
type fakeProvider struct {
	snaps  []Snapshot
	chunks map[string][]byte // (h,f,i) → bytes
	miss   map[string]bool   // (h,f,i) treated as missing
	errOn  map[string]error  // (h,f,i) returns error
}

func newFakeProvider(snaps ...Snapshot) *fakeProvider {
	return &fakeProvider{
		snaps:  snaps,
		chunks: map[string][]byte{},
		miss:   map[string]bool{},
		errOn:  map[string]error{},
	}
}

func chunkKey(h uint64, f, i uint32) string {
	return string(rune(h)) + ":" + string(rune(f)) + ":" + string(rune(i))
}

func (p *fakeProvider) ListSnapshots() []Snapshot { return p.snaps }
func (p *fakeProvider) LoadChunk(h uint64, f, i uint32) ([]byte, bool, error) {
	k := chunkKey(h, f, i)
	if err, ok := p.errOn[k]; ok {
		return nil, false, err
	}
	if p.miss[k] {
		return nil, false, nil
	}
	if b, ok := p.chunks[k]; ok {
		return b, true, nil
	}
	return nil, false, nil
}

func newServingReactor(p *fakeProvider) *Reactor {
	r := NewReactor(cmtlog.NewNopLogger())
	r.SetProvider(p)
	r.SetProbe(false)
	return r
}

func TestServingReactor_AddPeer_NoProbe(t *testing.T) {
	prov := newFakeProvider()
	r := newServingReactor(prov)
	peer := newRecordingPeer("nodeid1")
	r.AddPeer(peer)
	// Drain Connected event so we don't mistake it for a wire send.
	select {
	case ev := <-r.Out:
		if !ev.Connected {
			t.Fatalf("expected Connected event, got %+v", ev)
		}
	default:
		t.Fatal("AddPeer should publish a Connected event even in serve mode")
	}
	if got := peer.snapshotResponses(); len(got) != 0 {
		t.Fatalf("serve mode should not fire SnapshotsRequest on AddPeer, got %d sends", len(peer.sent))
	}
}

func TestProbeReactor_AddPeer_FiresProbe(t *testing.T) {
	r := NewReactor(cmtlog.NewNopLogger())
	peer := newRecordingPeer("nodeid1")
	r.AddPeer(peer)
	// Drain the Connected event.
	<-r.Out
	if len(peer.sent) != 1 {
		t.Fatalf("probe mode should fire SnapshotsRequest, got %d sends", len(peer.sent))
	}
	if _, ok := peer.sent[0].Message.(*ssproto.SnapshotsRequest); !ok {
		t.Fatalf("expected SnapshotsRequest, got %T", peer.sent[0].Message)
	}
}

func TestServing_SnapshotsRequest_Responds(t *testing.T) {
	prov := newFakeProvider(
		Snapshot{Height: 1000, Format: 3, Chunks: 2, Hash: []byte("hash-1000"), Metadata: []byte("md-1000")},
		Snapshot{Height: 2000, Format: 3, Chunks: 4, Hash: []byte("hash-2000"), Metadata: []byte("md-2000")},
	)
	r := newServingReactor(prov)
	peer := newRecordingPeer("requester")
	r.Receive(p2p.Envelope{
		ChannelID: SnapshotChannel,
		Src:       peer,
		Message:   &ssproto.SnapshotsRequest{},
	})

	resps := peer.snapshotResponses()
	if len(resps) != 2 {
		t.Fatalf("want 2 SnapshotsResponse, got %d", len(resps))
	}
	if resps[0].Height != 1000 || resps[1].Height != 2000 {
		t.Fatalf("unexpected response heights: %d, %d", resps[0].Height, resps[1].Height)
	}
	if string(resps[0].Hash) != "hash-1000" || string(resps[1].Metadata) != "md-2000" {
		t.Fatalf("response payload mismatch")
	}
	snaps, _, _ := r.Served()
	if snaps != 2 {
		t.Fatalf("Served snapshots counter = %d, want 2", snaps)
	}
}

func TestServing_ChunkRequest_Hit(t *testing.T) {
	prov := newFakeProvider(Snapshot{Height: 1000, Format: 3, Chunks: 2})
	prov.chunks[chunkKey(1000, 3, 0)] = []byte("payload-0")
	r := newServingReactor(prov)
	peer := newRecordingPeer("requester")
	r.Receive(p2p.Envelope{
		ChannelID: ChunkChannel,
		Src:       peer,
		Message:   &ssproto.ChunkRequest{Height: 1000, Format: 3, Index: 0},
	})
	resps := peer.chunkResponses()
	if len(resps) != 1 {
		t.Fatalf("want 1 ChunkResponse, got %d", len(resps))
	}
	if resps[0].Missing {
		t.Fatalf("expected Missing=false, got true")
	}
	if string(resps[0].Chunk) != "payload-0" {
		t.Fatalf("payload mismatch: %q", string(resps[0].Chunk))
	}
	if resps[0].Height != 1000 || resps[0].Format != 3 || resps[0].Index != 0 {
		t.Fatalf("response identifiers wrong: %+v", resps[0])
	}
	_, served, missing := r.Served()
	if served != 1 || missing != 0 {
		t.Fatalf("served=%d missing=%d, want 1/0", served, missing)
	}
}

func TestServing_ChunkRequest_MissReturnsMissingFlag(t *testing.T) {
	prov := newFakeProvider(Snapshot{Height: 1000, Format: 3, Chunks: 5})
	prov.miss[chunkKey(1000, 3, 4)] = true
	r := newServingReactor(prov)
	peer := newRecordingPeer("requester")
	r.Receive(p2p.Envelope{
		ChannelID: ChunkChannel,
		Src:       peer,
		Message:   &ssproto.ChunkRequest{Height: 1000, Format: 3, Index: 4},
	})
	resps := peer.chunkResponses()
	if len(resps) != 1 || !resps[0].Missing {
		t.Fatalf("want Missing=true response; got %+v", resps)
	}
	if len(resps[0].Chunk) != 0 {
		t.Fatalf("Missing response should carry no payload, got %d bytes", len(resps[0].Chunk))
	}
	_, served, missing := r.Served()
	if missing != 1 || served != 0 {
		t.Fatalf("served=%d missing=%d, want 0/1", served, missing)
	}
}

func TestServing_ChunkRequest_LoadErrorBecomesMissing(t *testing.T) {
	prov := newFakeProvider(Snapshot{Height: 1000, Format: 3, Chunks: 5})
	prov.errOn[chunkKey(1000, 3, 2)] = errSimulated{}
	r := newServingReactor(prov)
	peer := newRecordingPeer("requester")
	r.Receive(p2p.Envelope{
		ChannelID: ChunkChannel,
		Src:       peer,
		Message:   &ssproto.ChunkRequest{Height: 1000, Format: 3, Index: 2},
	})
	resps := peer.chunkResponses()
	if len(resps) != 1 || !resps[0].Missing {
		t.Fatalf("read error should surface as Missing=true; got %+v", resps)
	}
}

func TestServing_DrainModeFastFailsChunkRequest(t *testing.T) {
	// BeginShutdown makes ChunkRequest produce a Missing=true response
	// *without* asking the provider — the provider's LoadChunk is the
	// expensive path (file I/O on a 10 MiB chunk) and drain mode
	// exists precisely to avoid paying that cost mid-shutdown.
	prov := newFakeProvider(Snapshot{Height: 1000, Format: 3, Chunks: 2})
	prov.chunks[chunkKey(1000, 3, 0)] = []byte("payload-0") // would normally be served
	r := newServingReactor(prov)
	r.BeginShutdown()
	if !r.IsShuttingDown() {
		t.Fatal("IsShuttingDown should reflect BeginShutdown")
	}

	peer := newRecordingPeer("requester")
	r.Receive(p2p.Envelope{
		ChannelID: ChunkChannel,
		Src:       peer,
		Message:   &ssproto.ChunkRequest{Height: 1000, Format: 3, Index: 0},
	})

	resps := peer.chunkResponses()
	if len(resps) != 1 || !resps[0].Missing {
		t.Fatalf("drain mode should fast-fail with Missing=true; got %+v", resps)
	}
	if len(resps[0].Chunk) != 0 {
		t.Fatalf("drain response carried %d bytes; should be empty", len(resps[0].Chunk))
	}
	if got := r.Drained(); got != 1 {
		t.Fatalf("Drained() = %d, want 1", got)
	}
	// The chunksServed counter must NOT advance during drain —
	// otherwise we'd be conflating "served from disk" with "fast-
	// failed for shutdown".
	_, served, missing := r.Served()
	if served != 0 {
		t.Fatalf("chunks_served leaked through drain: %d", served)
	}
	if missing != 0 {
		t.Fatalf("chunks_missing leaked through drain: %d (should land in Drained)", missing)
	}
}

func TestServing_DrainMode_BeginShutdownIdempotent(t *testing.T) {
	// Calling BeginShutdown twice must not double-count anything and
	// must not break further drain responses.
	prov := newFakeProvider(Snapshot{Height: 1000, Format: 3, Chunks: 2})
	r := newServingReactor(prov)
	r.BeginShutdown()
	r.BeginShutdown()
	r.BeginShutdown()
	if !r.IsShuttingDown() {
		t.Fatal("IsShuttingDown should still be true after repeat calls")
	}

	peer := newRecordingPeer("p")
	r.Receive(p2p.Envelope{
		ChannelID: ChunkChannel,
		Src:       peer,
		Message:   &ssproto.ChunkRequest{Height: 1000, Format: 3, Index: 0},
	})
	if got := r.Drained(); got != 1 {
		t.Fatalf("Drained() = %d, want 1 (repeat BeginShutdown should not inflate)", got)
	}
}

func TestServing_RateLimit_PerPeerBurstThenDrop(t *testing.T) {
	// Per-peer bucket: burst=3, rate=0.0001/sec (effectively never
	// refills during this test). After 3 requests the 4th is dropped
	// silently — no ChunkResponse emitted, dropped_rate_peer
	// counter ticks.
	prov := newFakeProvider(Snapshot{Height: 1000, Format: 3, Chunks: 100})
	for i := uint32(0); i < 10; i++ {
		prov.chunks[chunkKey(1000, 3, i)] = []byte{0x42}
	}
	r := newServingReactor(prov)
	r.SetChunkRateLimit(ChunkRateLimit{PerPeerRate: 0.0001, PerPeerBurst: 3})

	peer := newRecordingPeer("p1")
	for i := uint32(0); i < 5; i++ {
		r.Receive(p2p.Envelope{
			ChannelID: ChunkChannel,
			Src:       peer,
			Message:   &ssproto.ChunkRequest{Height: 1000, Format: 3, Index: i},
		})
	}

	// First 3 served; 4th and 5th dropped silently.
	resps := peer.chunkResponses()
	if len(resps) != 3 {
		t.Fatalf("want 3 ChunkResponses (burst=3); got %d", len(resps))
	}
	perDrops, globDrops := r.RateDropped()
	if perDrops != 2 {
		t.Fatalf("dropped_rate_peer = %d, want 2 (requests 4 and 5)", perDrops)
	}
	if globDrops != 0 {
		t.Fatalf("dropped_rate_global = %d, want 0 (global bucket disabled)", globDrops)
	}
}

func TestServing_RateLimit_PerPeerIsolation(t *testing.T) {
	// Two peers, each with burst=2. Peer A exhausts; peer B's
	// budget should be untouched — that's the whole point of per-
	// peer buckets.
	prov := newFakeProvider(Snapshot{Height: 1000, Format: 3, Chunks: 10})
	for i := uint32(0); i < 10; i++ {
		prov.chunks[chunkKey(1000, 3, i)] = []byte{1}
	}
	r := newServingReactor(prov)
	r.SetChunkRateLimit(ChunkRateLimit{PerPeerRate: 0.0001, PerPeerBurst: 2})

	pA := newRecordingPeer("A")
	pB := newRecordingPeer("B")
	// Drain A's budget plus one over.
	for i := uint32(0); i < 3; i++ {
		r.Receive(p2p.Envelope{
			ChannelID: ChunkChannel,
			Src:       pA,
			Message:   &ssproto.ChunkRequest{Height: 1000, Format: 3, Index: i},
		})
	}
	// B should get its full burst without interference.
	for i := uint32(0); i < 2; i++ {
		r.Receive(p2p.Envelope{
			ChannelID: ChunkChannel,
			Src:       pB,
			Message:   &ssproto.ChunkRequest{Height: 1000, Format: 3, Index: i},
		})
	}
	if got := len(pA.chunkResponses()); got != 2 {
		t.Fatalf("peer A served %d, want 2 (burst)", got)
	}
	if got := len(pB.chunkResponses()); got != 2 {
		t.Fatalf("peer B served %d, want 2 (independent burst); A's exhaustion bled through?", got)
	}
}

func TestServing_RateLimit_GlobalCapsAcrossPeers(t *testing.T) {
	// Global=2, per-peer disabled. Two different peers each making
	// 2 requests: the global bucket allows only the first 2 in
	// total. Important property: the limit is checked *before* the
	// per-peer bucket, so the global one wins on contention.
	prov := newFakeProvider(Snapshot{Height: 1000, Format: 3, Chunks: 10})
	for i := uint32(0); i < 10; i++ {
		prov.chunks[chunkKey(1000, 3, i)] = []byte{1}
	}
	r := newServingReactor(prov)
	r.SetChunkRateLimit(ChunkRateLimit{GlobalRate: 0.0001, GlobalBurst: 2})

	pA := newRecordingPeer("A")
	pB := newRecordingPeer("B")
	for _, p := range []*recordingPeer{pA, pB} {
		for i := uint32(0); i < 2; i++ {
			r.Receive(p2p.Envelope{
				ChannelID: ChunkChannel,
				Src:       p,
				Message:   &ssproto.ChunkRequest{Height: 1000, Format: 3, Index: i},
			})
		}
	}
	totalServed := len(pA.chunkResponses()) + len(pB.chunkResponses())
	if totalServed != 2 {
		t.Fatalf("total served = %d, want 2 (global burst); per-peer slipped through?", totalServed)
	}
	perDrops, globDrops := r.RateDropped()
	if globDrops != 2 {
		t.Fatalf("dropped_rate_global = %d, want 2", globDrops)
	}
	if perDrops != 0 {
		t.Fatalf("dropped_rate_peer = %d, want 0 (per-peer bucket disabled)", perDrops)
	}
}

func TestServing_RateLimit_DisabledByDefault(t *testing.T) {
	// Zero-value ChunkRateLimit (or never calling SetChunkRateLimit
	// at all) means no rate limiting — the fetch-mode reactor
	// MUST continue working without a SetChunkRateLimit call.
	prov := newFakeProvider(Snapshot{Height: 1000, Format: 3, Chunks: 100})
	for i := uint32(0); i < 50; i++ {
		prov.chunks[chunkKey(1000, 3, i)] = []byte{1}
	}
	r := newServingReactor(prov) // no SetChunkRateLimit call

	peer := newRecordingPeer("hot")
	for i := uint32(0); i < 50; i++ {
		r.Receive(p2p.Envelope{
			ChannelID: ChunkChannel,
			Src:       peer,
			Message:   &ssproto.ChunkRequest{Height: 1000, Format: 3, Index: i},
		})
	}
	if got := len(peer.chunkResponses()); got != 50 {
		t.Fatalf("default (no rate limit) served %d, want 50", got)
	}
	perDrops, globDrops := r.RateDropped()
	if perDrops != 0 || globDrops != 0 {
		t.Fatalf("default should not produce any drops; got peer=%d global=%d", perDrops, globDrops)
	}
}

func TestServing_DrainWinsOverRateLimit(t *testing.T) {
	// When the reactor is both shutting down AND rate-limited, drain
	// must win — a polite peer getting Missing=true refetches
	// elsewhere immediately, whereas a rate-limited peer waits its
	// own per-chunk timeout. Pin the order in code (drain check
	// first) so a future refactor can't silently swap them.
	prov := newFakeProvider(Snapshot{Height: 1000, Format: 3, Chunks: 1})
	prov.chunks[chunkKey(1000, 3, 0)] = []byte{1}
	r := newServingReactor(prov)
	// Per-peer rate enabled with a tiny budget; we'd normally exhaust
	// it on the very first request from this peer (burst=1, then
	// 0.0001/s refill is effectively never). But drain wins.
	r.SetChunkRateLimit(ChunkRateLimit{PerPeerRate: 0.0001, PerPeerBurst: 1})
	r.BeginShutdown()

	peer := newRecordingPeer("p")
	// Two requests: in pure rate-limit mode the second would silent-
	// drop; in pure drain mode both get Missing=true. Drain-wins
	// expects both to come back as Missing=true.
	for i := 0; i < 2; i++ {
		r.Receive(p2p.Envelope{
			ChannelID: ChunkChannel,
			Src:       peer,
			Message:   &ssproto.ChunkRequest{Height: 1000, Format: 3, Index: 0},
		})
	}
	resps := peer.chunkResponses()
	if len(resps) != 2 {
		t.Fatalf("drain should produce a Missing response for both reqs; got %d", len(resps))
	}
	for i, r := range resps {
		if !r.Missing {
			t.Fatalf("response %d: want Missing=true (drain), got %+v", i, r)
		}
	}
	if got := r.Drained(); got != 2 {
		t.Fatalf("Drained() = %d, want 2 (both should hit the drain path)", got)
	}
	perDrops, globDrops := r.RateDropped()
	if perDrops != 0 || globDrops != 0 {
		t.Fatalf("rate-limit must NOT have fired during drain; got peer=%d global=%d",
			perDrops, globDrops)
	}
}

func TestServing_RateLimit_ExplicitZeroConfigIsDisabled(t *testing.T) {
	// Pin: calling SetChunkRateLimit(ChunkRateLimit{}) is observably
	// identical to never calling it. Same end state (no limiting), but
	// a different code path through SetChunkRateLimit — make sure the
	// "explicitly disabled" branch matches the "never set" branch.
	prov := newFakeProvider(Snapshot{Height: 1000, Format: 3, Chunks: 50})
	for i := uint32(0); i < 50; i++ {
		prov.chunks[chunkKey(1000, 3, i)] = []byte{1}
	}
	r := newServingReactor(prov)
	r.SetChunkRateLimit(ChunkRateLimit{}) // explicit-zero path

	peer := newRecordingPeer("burst")
	for i := uint32(0); i < 50; i++ {
		r.Receive(p2p.Envelope{
			ChannelID: ChunkChannel,
			Src:       peer,
			Message:   &ssproto.ChunkRequest{Height: 1000, Format: 3, Index: i},
		})
	}
	if got := len(peer.chunkResponses()); got != 50 {
		t.Fatalf("explicit-zero config should not rate-limit; served %d, want 50", got)
	}
	if pd, gd := r.RateDropped(); pd != 0 || gd != 0 {
		t.Fatalf("explicit-zero config should record no drops; got peer=%d global=%d", pd, gd)
	}
}

func TestServing_RateLimit_SecondSetCallIgnored(t *testing.T) {
	// Single-call enforcement: a second SetChunkRateLimit must not
	// mutate the existing limits (otherwise per-peer limiters created
	// at the first rate would coexist with new peers at the second
	// rate, an inconsistency that's easier to refuse than reconcile).
	prov := newFakeProvider(Snapshot{Height: 1000, Format: 3, Chunks: 10})
	for i := uint32(0); i < 10; i++ {
		prov.chunks[chunkKey(1000, 3, i)] = []byte{1}
	}
	r := newServingReactor(prov)
	r.SetChunkRateLimit(ChunkRateLimit{PerPeerRate: 0.0001, PerPeerBurst: 2})

	// Second call attempts to disable rate limiting. Should be a
	// no-op; existing limits stand.
	r.SetChunkRateLimit(ChunkRateLimit{})

	peer := newRecordingPeer("p")
	for i := uint32(0); i < 5; i++ {
		r.Receive(p2p.Envelope{
			ChannelID: ChunkChannel,
			Src:       peer,
			Message:   &ssproto.ChunkRequest{Height: 1000, Format: 3, Index: i},
		})
	}
	// First config was burst=2: 2 served, 3 dropped. If the second
	// call had taken effect, all 5 would have been served.
	if got := len(peer.chunkResponses()); got != 2 {
		t.Fatalf("served %d, want 2 — second SetChunkRateLimit must not have replaced the first config", got)
	}
	if pd, _ := r.RateDropped(); pd != 3 {
		t.Fatalf("dropped_rate_peer = %d, want 3 (3 over-burst requests)", pd)
	}
}

func TestServing_RateLimit_RemovePeerEvictsLimiter(t *testing.T) {
	// A long-running serve node sees thousands of peers come and go.
	// Limiters must eventually be evicted (so the sync.Map doesn't
	// accumulate one per historical peer.ID forever), but NOT
	// immediately on RemovePeer — that would let a same-NodeID
	// reconnect refresh its burst budget (#108 C4). The fix defers
	// eviction by 2× the bucket refill time, by which point the
	// limiter holds no rate-limit state worth preserving.
	prov := newFakeProvider(Snapshot{Height: 1000, Format: 3, Chunks: 5})
	prov.chunks[chunkKey(1000, 3, 0)] = []byte{1}
	r := newServingReactor(prov)
	r.SetChunkRateLimit(ChunkRateLimit{PerPeerRate: 4, PerPeerBurst: 8})

	peer := newRecordingPeer("transient")
	r.Receive(p2p.Envelope{
		ChannelID: ChunkChannel,
		Src:       peer,
		Message:   &ssproto.ChunkRequest{Height: 1000, Format: 3, Index: 0},
	})
	if _, ok := r.peerLimiters.Load(peer.ID()); !ok {
		t.Fatal("expected peer limiter to be created on first request")
	}

	// RemovePeer tombstones the entry instead of deleting it. The
	// same-NodeID reconnect path relies on this — see
	// TestServing_RateLimit_PerPeerBucketSurvivesReconnect.
	r.RemovePeer(peer, "test")
	if _, ok := r.peerLimiters.Load(peer.ID()); !ok {
		t.Fatal("entry must persist past RemovePeer (else same-ID reconnect would refresh the bucket; #108 C4)")
	}

	// Fast-forward the tombstone past the TTL and exercise the
	// auto-sweep that RemovePeer runs whenever the tombstoned set
	// grows. Using a fresh unrelated peer here just to drive the
	// sweep — the assertion is about the original entry's eviction.
	v, _ := r.peerLimiters.Load(peer.ID())
	v.(*peerLimiterEntry).removedAt.Store(time.Now().Add(-time.Hour).UnixNano())
	other := newRecordingPeer("other")
	r.RemovePeer(other, "test")
	if _, ok := r.peerLimiters.Load(peer.ID()); ok {
		t.Fatal("entry should be swept once tombstone is past TTL; auto-sweep from RemovePeer didn't fire")
	}
}

func TestServing_NoProvider_RequestsSilent(t *testing.T) {
	// Probe-only reactor (default): inbound requests must not panic and
	// must not produce any wire responses.
	r := NewReactor(cmtlog.NewNopLogger())
	peer := newRecordingPeer("requester")
	r.Receive(p2p.Envelope{
		ChannelID: SnapshotChannel,
		Src:       peer,
		Message:   &ssproto.SnapshotsRequest{},
	})
	r.Receive(p2p.Envelope{
		ChannelID: ChunkChannel,
		Src:       peer,
		Message:   &ssproto.ChunkRequest{Height: 1, Format: 1, Index: 0},
	})
	if len(peer.sent) != 0 {
		t.Fatalf("probe-only reactor should not respond to inbound requests; got %d sends", len(peer.sent))
	}
}

type errSimulated struct{}

func (errSimulated) Error() string { return "simulated read error" }

// TestServing_RateLimit_PerPeerBucketSurvivesReconnect is the
// regression test for issue #108 finding C4: RemovePeer used to
// delete the per-peer rate.Limiter immediately, so a peer holding a
// fixed Node ID could disconnect and reconnect to refresh its full
// burst budget. The whole point of PerPeerRate is to bound a single
// peer's request rate; that bypass defeats it.
//
// The scenario: burst-exhaust the per-peer bucket at a rate so slow
// it can't refill during the test, RemovePeer, then reconnect with a
// fresh recordingPeer carrying the SAME Node ID. On vulnerable code
// the limiter is gone and the reconnect gets a full burst. On the
// fixed code the limiter survives RemovePeer (eviction is deferred
// to a sweeper that waits ~2× the bucket refill time), so the
// reconnect sees the same depleted bucket and is dropped.
func TestServing_RateLimit_PerPeerBucketSurvivesReconnect(t *testing.T) {
	prov := newFakeProvider(Snapshot{Height: 1000, Format: 3, Chunks: 100})
	for i := uint32(0); i < 10; i++ {
		prov.chunks[chunkKey(1000, 3, i)] = []byte{1}
	}
	r := newServingReactor(prov)
	// Rate is effectively zero so the bucket can't refill during the test.
	r.SetChunkRateLimit(ChunkRateLimit{PerPeerRate: 0.0001, PerPeerBurst: 3})

	peer := newRecordingPeer("attacker")
	r.AddPeer(peer)
	for i := uint32(0); i < 5; i++ {
		r.Receive(p2p.Envelope{
			ChannelID: ChunkChannel,
			Src:       peer,
			Message:   &ssproto.ChunkRequest{Height: 1000, Format: 3, Index: i},
		})
	}
	if got := len(peer.chunkResponses()); got != 3 {
		t.Fatalf("setup: served %d, want 3 (burst); rate-limit config not applied?", got)
	}

	// Disconnect, then reconnect with the SAME Node ID. A fresh
	// recordingPeer instance simulates a brand-new connection; the
	// Node ID is what binds it to the same rate-limit bucket.
	r.RemovePeer(peer, "test")
	peer2 := newRecordingPeer("attacker")
	r.AddPeer(peer2)

	r.Receive(p2p.Envelope{
		ChannelID: ChunkChannel,
		Src:       peer2,
		Message:   &ssproto.ChunkRequest{Height: 1000, Format: 3, Index: 5},
	})
	if got := len(peer2.chunkResponses()); got != 0 {
		t.Fatalf("post-reconnect: served %d, want 0 (bucket should still be empty); reconnect bypassed rate limit", got)
	}
	if pd, _ := r.RateDropped(); pd < 3 {
		t.Fatalf("dropped_rate_peer = %d, want >= 3 (2 over-burst + 1 reconnect); per-peer counter not ticking", pd)
	}
}
