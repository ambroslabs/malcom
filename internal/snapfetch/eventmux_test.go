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
	for _, sub := range []<-chan statesync.Event{a, b} {
		select {
		case ev := <-sub:
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
	case ev := <-sub:
		if ev.Chunk == nil || ev.Chunk.Index != 7 {
			t.Fatalf("subscriber got %+v, want chunk idx=7", ev)
		}
	case <-time.After(time.Second):
		t.Fatalf("subscriber didn't receive chunk event within timeout")
	}
}

func TestEventMuxDropsOnSlowSubscriber(t *testing.T) {
	t.Parallel()

	inCtrl := make(chan statesync.Event, 8)
	inChunk := make(chan statesync.Event, 8)
	mux := newEventMux(context.Background(), inCtrl, inChunk)
	defer mux.stop()

	// Slow subscriber: never reads. Buffer is 256.
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
		case <-fast:
			got++
		case <-deadline:
			t.Fatalf("fast subscriber starved: got %d/%d before deadline", got, n)
		}
	}
}

// Chunk-channel pressure must not starve control events through the
// mux. With chunks and control events on separate inputs the mux's
// select sees both as eligible and round-robins; control events are
// guaranteed to reach subscribers even while chunks back up.
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

	deadline := time.After(time.Second)
	for {
		select {
		case ev := <-sub:
			if ev.Connected && ev.PeerID == "p" {
				return
			}
		case <-deadline:
			t.Fatalf("control event never delivered while chunk channel was hot")
		}
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
	// Give the mux goroutine a chance to observe ctx.Done() before we
	// send on inCtrl. Without this, the goroutine's select may still
	// see both ctx.Done() and inCtrl as eligible and randomly pick the
	// receive, dispatching the event after stop.
	time.Sleep(50 * time.Millisecond)

	// After stop, events sent on `inCtrl` must not reach subscribers.
	inCtrl <- statesync.Event{PeerID: "after-stop"}
	select {
	case ev := <-sub:
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
	// Give the mux goroutine a chance to observe ctx.Done() before we
	// send on inCtrl. Without this, the goroutine's select may still
	// see both ctx.Done() and inCtrl as eligible and randomly pick the
	// receive, dispatching the event after cancel.
	time.Sleep(50 * time.Millisecond)

	inCtrl <- statesync.Event{PeerID: "after-cancel"}
	select {
	case ev := <-sub:
		t.Fatalf("subscriber received event after parent ctx cancel: %+v", ev)
	case <-time.After(50 * time.Millisecond):
	}
	// stop() after ctx cancel must still be safe.
	mux.stop()
}
