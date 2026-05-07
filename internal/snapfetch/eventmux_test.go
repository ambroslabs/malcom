package snapfetch

import (
	"context"
	"testing"
	"time"

	"github.com/zrbecker/cosmos-p2p/internal/statesync"
)

func TestEventMuxFanOutToAllSubscribers(t *testing.T) {
	t.Parallel()

	in := make(chan statesync.Event, 8)
	mux := newEventMux(context.Background(), in)
	defer mux.stop()

	// Subscribe before sending: the channel send on `in` synchronizes
	// with the mux loop's receive, so any subscribers registered before
	// the send are guaranteed visible when the event is dispatched.
	a := mux.subscribe()
	b := mux.subscribe()

	in <- statesync.Event{PeerID: "p1", Connected: true}

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

func TestEventMuxDropsOnSlowSubscriber(t *testing.T) {
	t.Parallel()

	in := make(chan statesync.Event, 8)
	mux := newEventMux(context.Background(), in)
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
			in <- statesync.Event{PeerID: "p"}
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

func TestEventMuxStopHaltsDispatch(t *testing.T) {
	t.Parallel()

	in := make(chan statesync.Event, 4)
	mux := newEventMux(context.Background(), in)
	sub := mux.subscribe()

	mux.stop()
	// Idempotent: calling stop twice must be safe.
	mux.stop()

	// After stop, events sent on `in` must not reach subscribers.
	// We can't guarantee the goroutine has exited synchronously, so
	// allow a brief window then assert no event arrived.
	in <- statesync.Event{PeerID: "after-stop"}
	select {
	case ev := <-sub:
		t.Fatalf("subscriber received event after stop: %+v", ev)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestEventMuxParentContextCancelStops(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	in := make(chan statesync.Event, 4)
	mux := newEventMux(ctx, in)
	sub := mux.subscribe()

	cancel()

	in <- statesync.Event{PeerID: "after-cancel"}
	select {
	case ev := <-sub:
		t.Fatalf("subscriber received event after parent ctx cancel: %+v", ev)
	case <-time.After(50 * time.Millisecond):
	}
	// stop() after ctx cancel must still be safe.
	mux.stop()
}
