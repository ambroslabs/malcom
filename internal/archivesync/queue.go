// Package archivesync implements the block-sync reactor that fills the
// on-disk archive from cosmoshub archive peers.
package archivesync

import (
	"sort"
	"sync"
)

// Queue is a thread-safe set of heights to fetch, with a round-robin Next.
// Heights can be re-added on timeout/NoBlockResponse without duplication.
type Queue struct {
	mu        sync.Mutex
	pending   map[int64]struct{}
	order     []int64 // ordered slice for stable iteration
	cursor    int
	dirtySort bool
}

func NewQueue() *Queue {
	return &Queue{pending: make(map[int64]struct{})}
}

// AddRange enqueues every height in [lo, hi] (inclusive) that isn't already
// present. Returns count of newly enqueued heights.
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
			q.order = append(q.order, h)
			added++
		}
	}
	q.dirtySort = true
	return added
}

// Add enqueues a single height. Returns true if it was new.
func (q *Queue) Add(h int64) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.pending[h]; ok {
		return false
	}
	q.pending[h] = struct{}{}
	q.order = append(q.order, h)
	q.dirtySort = true
	return true
}

// Remove drops a height (e.g. when fetched). No-op if absent.
func (q *Queue) Remove(h int64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.pending, h)
	// Don't compact order here; Next skips removed entries lazily.
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

// Next returns the next pending height in ascending order, cycling when it
// reaches the end. Returns 0, false if the queue is empty.
//
// Skips entries that have been Removed since they were enqueued.
func (q *Queue) Next() (int64, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.pending) == 0 {
		return 0, false
	}
	if q.dirtySort {
		sort.Slice(q.order, func(i, j int) bool { return q.order[i] < q.order[j] })
		q.dirtySort = false
	}
	for steps := 0; steps < len(q.order); steps++ {
		if q.cursor >= len(q.order) {
			q.cursor = 0
		}
		h := q.order[q.cursor]
		q.cursor++
		if _, live := q.pending[h]; live {
			return h, true
		}
	}
	return 0, false
}

// Compact rebuilds the order slice from the live set. Cheap to call after
// many removes.
func (q *Queue) Compact() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.order = q.order[:0]
	for h := range q.pending {
		q.order = append(q.order, h)
	}
	sort.Slice(q.order, func(i, j int) bool { return q.order[i] < q.order[j] })
	q.cursor = 0
	q.dirtySort = false
}
