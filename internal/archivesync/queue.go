// Package archivesync implements the block-sync reactor that fills the
// on-disk archive from cosmoshub archive peers.
package archivesync

import (
	"sort"
	"sync"
)

// Queue is a thread-safe set of heights to fetch.
//
// Internally it maintains two sub-queues:
//
//   - "fresh" — the long, sorted main queue iterated by a round-robin
//     cursor. Used to walk through the initial download window.
//   - "retry" — a small LIFO of heights that previously failed
//     (NoBlockResponse, timeout, send-queue-full). Next() drains retry
//     first so failures get a second pass within seconds, not after a
//     full cursor wrap.
//
// Both share the same `pending` set for dedup, so a height can be added
// to retry without duplicating it in the main slice.
type Queue struct {
	mu        sync.Mutex
	pending   map[int64]struct{}
	fresh     []int64 // sorted main queue (asc by default, desc if Descending)
	retry     []int64 // LIFO of heights to retry first
	cursor    int
	dirtySort bool

	// Descending, when true, sorts fresh from highest height to lowest. Used
	// when the operator wants newer blocks downloaded first (more useful for
	// serving recent state, analytics, snapshot anchoring).
	Descending bool
}

func NewQueue() *Queue {
	return &Queue{pending: make(map[int64]struct{})}
}

// AddRange enqueues every height in [lo, hi] into the fresh queue.
// Returns count newly added.
func (q *Queue) AddRange(lo, hi int64) int {
	if lo > hi {
		return 0
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	added := 0
	for h := lo; h <= hi; h++ {
		if _, ok := q.pending[h]; !ok {
			q.pending[h] = struct{}{}
			q.fresh = append(q.fresh, h)
			added++
		}
	}
	q.dirtySort = true
	return added
}

// Add enqueues a single height into the fresh queue. Returns true if new.
func (q *Queue) Add(h int64) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.pending[h]; ok {
		return false
	}
	q.pending[h] = struct{}{}
	q.fresh = append(q.fresh, h)
	q.dirtySort = true
	return true
}

// Retry pushes a height to the front of the retry queue so the next
// Next() call returns it. Idempotent — if h isn't in pending it's a
// no-op (height was already fetched). If h IS in pending, we add to
// retry even if it's also still in fresh — the pending set dedups.
func (q *Queue) Retry(h int64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.pending[h]; !ok {
		return
	}
	q.retry = append(q.retry, h)
}

// Remove drops a height (e.g. when fetched). No-op if absent.
func (q *Queue) Remove(h int64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.pending, h)
	// Don't compact slices; Next skips dead entries lazily.
}

// Has reports whether h is still pending.
func (q *Queue) Has(h int64) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	_, ok := q.pending[h]
	return ok
}

// Size returns the number of pending heights.
func (q *Queue) Size() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.pending)
}

// Next returns the next height to dispatch. Drains retry first (LIFO),
// then walks fresh (round-robin sorted). Returns 0, false if empty.
func (q *Queue) Next() (int64, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.pending) == 0 {
		return 0, false
	}
	// Drain retry first.
	for len(q.retry) > 0 {
		h := q.retry[len(q.retry)-1]
		q.retry = q.retry[:len(q.retry)-1]
		if _, live := q.pending[h]; live {
			return h, true
		}
	}
	// Then walk fresh round-robin.
	if q.dirtySort {
		if q.Descending {
			sort.Slice(q.fresh, func(i, j int) bool { return q.fresh[i] > q.fresh[j] })
		} else {
			sort.Slice(q.fresh, func(i, j int) bool { return q.fresh[i] < q.fresh[j] })
		}
		q.dirtySort = false
	}
	for steps := 0; steps < len(q.fresh); steps++ {
		if q.cursor >= len(q.fresh) {
			q.cursor = 0
		}
		h := q.fresh[q.cursor]
		q.cursor++
		if _, live := q.pending[h]; live {
			return h, true
		}
	}
	return 0, false
}

// Compact rebuilds the fresh slice from the live set; resets cursor.
// Cheap to call periodically after many removes.
func (q *Queue) Compact() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.fresh = q.fresh[:0]
	for h := range q.pending {
		q.fresh = append(q.fresh, h)
	}
	if q.Descending {
		sort.Slice(q.fresh, func(i, j int) bool { return q.fresh[i] > q.fresh[j] })
	} else {
		sort.Slice(q.fresh, func(i, j int) bool { return q.fresh[i] < q.fresh[j] })
	}
	q.cursor = 0
	q.dirtySort = false
}
