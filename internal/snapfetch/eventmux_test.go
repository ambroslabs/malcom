package snapfetch

import (
	"context"
	"testing"
	"time"

	"github.com/zrbecker/cosmos-p2p/internal/statesync"
)

func TestEventMuxFanOutToAllSubscribers(t *testing.T) {
	t.Parallel()

	inCtrl := make(chan statesync.Event, 8)
	inChunk := make(chan statesync.Event, 8)
	mux := newEventMux(context.Background(), inCtrl, inChunk)
	defer mux.stop()

	// Subscribe before sending: the channel send on `inCtrl` synchronizes
	// with the mux loop's receive, so any subscribers registered before
	// the send are guaranteed visible when the event is dispatched.
	a := mux.subscribe()
	b := mux.subscribe()

	inCtrl <- statesync.Event{PeerID: "p1", Connected: true}

	timeout := time.After(time.Second)
	for _, sub := range []*subscription{a, b} {
		select {
		case ev := <-sub.Ctrl:
			if ev.PeerID != "p1" || !ev.Connected {
				t.Fatalf("subscriber got unexpected event: %+v", ev)
			}
		case <-timeout:
			t.Fatalf("subscriber didn't receive event within timeout")
		}
	}
}

func TestEventMuxFansOutChunkChannel(t *testing.T) {
	t.Parallel()

	inCtrl := make(chan statesync.Event, 4)
	inChunk := make(chan statesync.Event, 4)
	mux := newEventMux(context.Background(), inCtrl, inChunk)
	defer mux.stop()

	sub := mux.subscribe()
	inChunk <- statesync.Event{PeerID: "p1", Chunk: &statesync.ChunkInfo{Index: 7}}

	select {
	case ev := <-sub.Chunk:
		if ev.Chunk == nil || ev.Chunk.Index != 7 {
			t.Fatalf("subscriber got %+v, want chunk idx=7", ev)
		}
	case <-time.After(time.Second):
		t.Fatalf("subscriber didn't receive chunk event within timeout")
	}
}

// subscribeCtrl returns only the control channel; chunks must not flow
// to ctrl-only subscribers (no allocation, no spurious deliveries).
func TestEventMuxSubscribeCtrlIgnoresChunks(t *testing.T) {
	t.Parallel()

	inCtrl := make(chan statesync.Event, 4)
	inChunk := make(chan statesync.Event, 4)
	mux := newEventMux(context.Background(), inCtrl, inChunk)
	defer mux.stop()

	ctrl := mux.subscribeCtrl()
	inChunk <- statesync.Event{PeerID: "c", Chunk: &statesync.ChunkInfo{}}
	inCtrl <- statesync.Event{PeerID: "p", Connected: true}

	// First event the ctrl-only subscriber sees must be the connect,
	// not the chunk that was sent earlier.
	select {
	case ev := <-ctrl:
		if !ev.Connected || ev.PeerID != "p" {
			t.Fatalf("ctrl-only subscriber received non-control event: %+v", ev)
		}
	case <-time.After(time.Second):
		t.Fatalf("ctrl-only subscriber didn't receive control event")
	}
}

func TestEventMuxDropsOnSlowSubscriber(t *testing.T) {
	t.Parallel()

	inCtrl := make(chan statesync.Event, 8)
	inChunk := make(chan statesync.Event, 8)
	mux := newEventMux(context.Background(), inCtrl, inChunk)
	defer mux.stop()

	// Slow subscriber: never reads. Buffer is subCtrlBuffer.
	slow := mux.subscribe()
	// Fast subscriber must keep receiving even while slow is wedged —
	// proves the per-subscriber send is non-blocking and slow consumers
	// don't head-of-line block the mux loop.
	fast := mux.subscribe()
	_ = slow

	const n = 300
	go func() {
		for i := 0; i < n; i++ {
			inCtrl <- statesync.Event{PeerID: "p"}
		}
	}()

	// Drain at least n events from fast within a generous deadline.
	// If the mux were blocked by slow, fast would starve.
	deadline := time.After(2 * time.Second)
	got := 0
	for got < n {
		select {
		case <-fast.Ctrl:
			got++
		case <-deadline:
			t.Fatalf("fast subscriber starved: got %d/%d before deadline", got, n)
		}
	}
}

// Chunk pressure on the chunk channel must not delay control events
// through the mux. With chunks and control on separate per-subscriber
// channels, a hot chunk stream cannot push control events out.
func TestEventMuxControlNotStarvedByChunks(t *testing.T) {
	t.Parallel()

	inCtrl := make(chan statesync.Event, 4)
	inChunk := make(chan statesync.Event, 4)
	mux := newEventMux(context.Background(), inCtrl, inChunk)
	defer mux.stop()

	sub := mux.subscribe()

	// Stream chunks continuously to keep inChunk hot.
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			case inChunk <- statesync.Event{PeerID: "c", Chunk: &statesync.ChunkInfo{}}:
			}
		}
	}()
	defer close(stop)

	inCtrl <- statesync.Event{PeerID: "p", Connected: true}

	select {
	case ev := <-sub.Ctrl:
		if !ev.Connected || ev.PeerID != "p" {
			t.Fatalf("got %+v, want Connected=true PeerID=p", ev)
		}
	case <-time.After(time.Second):
		t.Fatalf("control event never delivered while chunk channel was hot")
	}
}

func TestEventMuxStopHaltsDispatch(t *testing.T) {
	t.Parallel()

	inCtrl := make(chan statesync.Event, 4)
	inChunk := make(chan statesync.Event, 4)
	mux := newEventMux(context.Background(), inCtrl, inChunk)
	sub := mux.subscribe()

	mux.stop()
	// Idempotent: calling stop twice must be safe.
	mux.stop()

	// After stop, events sent on `inCtrl` must not reach subscribers.
	// The mux loop re-checks ctx.Err() after each receive, so even if
	// it picks up the buffered event before exiting, fanout is skipped.
	inCtrl <- statesync.Event{PeerID: "after-stop"}
	select {
	case ev := <-sub.Ctrl:
		t.Fatalf("subscriber received event after stop: %+v", ev)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestEventMuxParentContextCancelStops(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	inCtrl := make(chan statesync.Event, 4)
	inChunk := make(chan statesync.Event, 4)
	mux := newEventMux(ctx, inCtrl, inChunk)
	sub := mux.subscribe()

	cancel()

	inCtrl <- statesync.Event{PeerID: "after-cancel"}
	select {
	case ev := <-sub.Ctrl:
		t.Fatalf("subscriber received event after parent ctx cancel: %+v", ev)
	case <-time.After(50 * time.Millisecond):
	}
	// stop() after ctx cancel must still be safe.
	mux.stop()
}
