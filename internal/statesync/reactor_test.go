package statesync

import (
	"net"
	"sync"
	"testing"

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
