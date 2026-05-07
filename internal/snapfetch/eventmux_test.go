package snapfetch

import (
	"context"
	"testing"
	"time"

	"github.com/zrbecker/cosmos-p2p/internal/statesync"
)

func TestEventMuxFanOutToAllSubscribers(t *testing.T) {
	in := make(chan statesync.Event, 8)
	mux := newEventMux(context.Background(), in)
	defer mux.stop()

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
	in := make(chan statesync.Event, 8)
	mux := newEventMux(context.Background(), in)
	defer mux.stop()

	// Slow subscriber: never reads. Buffer is 256.
	mux.subscribe()
	// Send 300 events; the mux's per-subscriber send is non-blocking,
	// so excess events drop without blocking the mux loop.
	for i := 0; i < 300; i++ {
		in <- statesync.Event{PeerID: "p"}
	}
	// If the mux were blocked, the test would hang. We just need to
	// exit normally.
	time.Sleep(50 * time.Millisecond) // give the mux time to drain `in`
}

func TestEventMuxStopIsIdempotent(t *testing.T) {
	in := make(chan statesync.Event, 1)
	mux := newEventMux(context.Background(), in)
	mux.stop()
	// Calling stop twice should be safe (cancelfunc is idempotent).
	mux.stop()
}
