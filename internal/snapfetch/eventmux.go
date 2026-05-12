package snapfetch

import (
	"context"
	"sync"

	"github.com/ambroslabs/malcom/internal/statesync"
)

// eventMux fans the reactor's Out and OutChunks channels out to multiple
// subscribers. Each subscription gets its own pair of buffered channels
// so a slow consumer can't pin a 16 MiB chunk payload behind a tiny
// control event in a single shared inbox.
type eventMux struct {
	inCtrl  <-chan statesync.Event
	inChunk <-chan statesync.Event
	mu      sync.Mutex
	subs    []subEntry
	cancel  context.CancelFunc
}

// subscription is what subscribe() returns. Consumers select on the
// channels they care about; chunk-only events on Chunk, everything else
// on Ctrl. Ctrl is always non-nil; Chunk is nil for control-only
// subscribers (subscribeCtrl).
type subscription struct {
	Ctrl  <-chan statesync.Event
	Chunk <-chan statesync.Event
}

type subEntry struct {
	ctrl  chan<- statesync.Event
	chunk chan<- statesync.Event // may be nil for ctrl-only subs
}

const (
	subCtrlBuffer  = 256
	subChunkBuffer = 8
)

func newEventMux(ctx context.Context, inCtrl, inChunk <-chan statesync.Event) *eventMux {
	ctx, cancel := context.WithCancel(ctx)
	m := &eventMux{inCtrl: inCtrl, inChunk: inChunk, cancel: cancel}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case ev := <-inCtrl:
				// Re-check after the receive: select doesn't prioritize
				// ctx.Done() over a ready buffered receive, so a cancel
				// concurrent with a final send must be caught here.
				if ctx.Err() != nil {
					return
				}
				m.fanoutCtrl(ev)
			case ev := <-inChunk:
				if ctx.Err() != nil {
					return
				}
				m.fanoutChunk(ev)
			}
		}
	}()
	return m
}

func (m *eventMux) fanoutCtrl(ev statesync.Event) {
	m.mu.Lock()
	subs := append([]subEntry(nil), m.subs...)
	m.mu.Unlock()
	for _, s := range subs {
		select {
		case s.ctrl <- ev:
		default:
		}
	}
}

func (m *eventMux) fanoutChunk(ev statesync.Event) {
	m.mu.Lock()
	subs := append([]subEntry(nil), m.subs...)
	m.mu.Unlock()
	for _, s := range subs {
		if s.chunk == nil {
			continue
		}
		select {
		case s.chunk <- ev:
		default:
		}
	}
}

// subscribe registers a chunk-aware consumer. Use subscribeCtrl when
// chunks aren't needed (e.g., peerWatch) to avoid allocating a chunk
// channel that nothing reads.
func (m *eventMux) subscribe() *subscription {
	ctrl := make(chan statesync.Event, subCtrlBuffer)
	chunk := make(chan statesync.Event, subChunkBuffer)
	m.mu.Lock()
	m.subs = append(m.subs, subEntry{ctrl: ctrl, chunk: chunk})
	m.mu.Unlock()
	return &subscription{Ctrl: ctrl, Chunk: chunk}
}

func (m *eventMux) subscribeCtrl() <-chan statesync.Event {
	ctrl := make(chan statesync.Event, subCtrlBuffer)
	m.mu.Lock()
	m.subs = append(m.subs, subEntry{ctrl: ctrl})
	m.mu.Unlock()
	return ctrl
}

func (m *eventMux) stop() { m.cancel() }
