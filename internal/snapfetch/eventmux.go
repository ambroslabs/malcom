package snapfetch

import (
	"context"
	"sync"

	"github.com/zrbecker/cosmos-p2p/internal/statesync"
)

// statesync.Reactor has one Out channel; we want multiple consumers
// across phases. eventMux fans out events to all current subscribers.
// Subscribers drop events if their inbox is full.
type eventMux struct {
	in     <-chan statesync.Event
	mu     sync.Mutex
	subs   []chan<- statesync.Event
	cancel context.CancelFunc
}

func newEventMux(ctx context.Context, in <-chan statesync.Event) *eventMux {
	ctx, cancel := context.WithCancel(ctx)
	m := &eventMux{in: in, cancel: cancel}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case ev := <-in:
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
		}
	}()
	return m
}

func (m *eventMux) subscribe() <-chan statesync.Event {
	c := make(chan statesync.Event, 256)
	m.mu.Lock()
	m.subs = append(m.subs, c)
	m.mu.Unlock()
	return c
}

func (m *eventMux) stop() { m.cancel() }
