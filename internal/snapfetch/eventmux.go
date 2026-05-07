package snapfetch

import (
	"context"
	"sync"

	"github.com/zrbecker/cosmos-p2p/internal/statesync"
)

// statesync.Reactor publishes on two channels — Out (control + snapshot)
// and OutChunks (16 MiB chunk payloads). eventMux selects across both
// and fans out the merged stream to all current subscribers, so
// downstream consumers stay on a single inbox. Per-subscriber inboxes
// drop events when full.
type eventMux struct {
	inCtrl  <-chan statesync.Event
	inChunk <-chan statesync.Event
	mu      sync.Mutex
	subs    []chan<- statesync.Event
	cancel  context.CancelFunc
}

func newEventMux(ctx context.Context, inCtrl, inChunk <-chan statesync.Event) *eventMux {
	ctx, cancel := context.WithCancel(ctx)
	m := &eventMux{inCtrl: inCtrl, inChunk: inChunk, cancel: cancel}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case ev := <-inCtrl:
				m.fanout(ev)
			case ev := <-inChunk:
				m.fanout(ev)
			}
		}
	}()
	return m
}

func (m *eventMux) fanout(ev statesync.Event) {
	m.mu.Lock()
	subs := append([]chan<- statesync.Event(nil), m.subs...)
	m.mu.Unlock()
	for _, s := range subs {
		select {
		case s <- ev:
		default:
		}
	}
}

func (m *eventMux) subscribe() <-chan statesync.Event {
	c := make(chan statesync.Event, 256)
	m.mu.Lock()
	m.subs = append(m.subs, c)
	m.mu.Unlock()
	return c
}

func (m *eventMux) stop() { m.cancel() }
