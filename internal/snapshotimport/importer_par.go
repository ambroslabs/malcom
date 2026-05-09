// Wave-parallel storeImporter: defer all hashing — leaves AND inner
// nodes — to a worker pool, with frame-based dependency tracking and
// bounded backpressure on the main thread's submission queue.
//
// Architecture (per store, on top of the per-store-worker pool that
// lives in parallel.go):
//
//	[main]    reads stream → addLeaf/addInner → builds parFrame
//	            → setFast inline (sorted by post-order leaf order)
//	            → submitFrame(blocking on cap)
//	[disp]    drains parPending slice (unbounded, mu-guarded) → parReady (bounded chan)
//	[wkr × N] take ready frame → hash + encode → push to writeQ
//	            → atomic events bump → maybe submitFrame(parent)
//	[writer]  drains writeQ → batch.Set (called by parallel.go)
//	[abw]     async batch commit (the asyncBatchWriter from #71)
//
// Dependency tracking on each parFrame uses three atomics: pending
// (unhashed-children count, 2 for inners, 0 for leaves), events (0/1/2
// counter — main increments on parent-link, worker on hash-done; the
// second event triggers parent.pending--), built (true once the inner
// has been popped from the stack and linked), submitted (CAS-guarded
// idempotent submission). Original design lives in commit 1bceb37
// (retired in ffd20d6); this is a frame-based reimplementation that
// avoids the iavl.Node memory blow-up by holding only ~200 B/frame
// (vs full Node objects) and by bounding main-thread submissions.
//
// Worker → writer hand-off uses an unbounded slice + cond. Main
// blocks on append when the slice exceeds parPendingCap; workers
// don't block on cap (they only contribute when promoting an inner
// whose pending hits 0, ~one-per-leaf in steady state). The
// CAS-guarded submitted flag means the same frame is enqueued at
// most once even if main and a worker race to submit the same inner.

package snapshotimport

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// parFrame is the wave-parallel frame. Pointer-typed so workers and
// main share atomic state via the same instance. Holds enough info
// for a worker to compute its hash + encoded bytes after children
// have been hashed.
type parFrame struct {
	// Populated by the worker that hashes this node. Read by the
	// parent's worker (after parent.pending hits 0) and by finalize
	// for the root.
	hash    [32]byte
	encoded []byte // owned bytes pushed to writeQ; reused later as canonical-root bytes for the root frame

	// Coordination atomics.
	pending   atomic.Int32 // unhashed-children count: 2 for inners, 0 for leaves
	events    atomic.Int32 // 0/1/2 — main's parent-link + worker's hash-done; second event triggers parent.pending--
	built     atomic.Bool  // true once parent has been linked + popped (inner only)
	submitted atomic.Bool  // CAS-guarded; true once submitted to dispatcher

	// Tree shape + storage metadata.
	height  int8
	size    int64
	version int64
	nonce   uint32

	// Parent pointer for worker's wake-up signal. nil for root.
	parent *parFrame

	// Leaf-only payload. Owned bytes copied by main at submission;
	// the worker frees these after hashing to keep peak memory bounded.
	leafKey   []byte
	leafValue []byte

	// Inner-only payload. innerKey is the BST routing pivot (owned
	// copy). leftChild/rightChild let the worker read children's
	// hashes once both have completed. Pointer chain extends the
	// frame's GC lifetime to children, so children must drop their
	// own large fields after hashing.
	innerKey   []byte
	leftChild  *parFrame
	rightChild *parFrame
}

// writeEnt is what a worker pushes to the per-store writer goroutine.
// The bytes are owned by the worker until pebble.Batch.Set copies
// them; after that the writer can drop the reference.
type writeEnt struct {
	key, encoded []byte
}

// parState holds the wave-parallel goroutines + queues for one store.
// Created by enableWaveParallel; shut down by finishStreaming.
type parState struct {
	workers  int
	writeQ   chan<- writeEnt
	ready    chan *parFrame

	// pending is an unbounded slice queue, mu-guarded. Main and
	// workers both append; dispatcher drains. Bounded only by
	// pendingCap as soft backpressure on the main thread (workers
	// don't honor the cap so a full queue can't deadlock by
	// stalling the consumer-of-its-own-promotions).
	mu          sync.Mutex
	cond        *sync.Cond
	pending     []*parFrame
	pendingCap  int
	closed      bool

	dispatcherWg sync.WaitGroup
	workerWg     sync.WaitGroup
	hashWg       sync.WaitGroup // tracks frames submitted but not yet processed; finishStreaming waits on this before closing pending

	firstErr atomic.Pointer[error]

	// rootEncoded is set by the worker that hashes the root (the one
	// frame whose parent is nil). finalize reads it for the canonical
	// root re-emit. Single writer (the lone root-hashing worker), so
	// no atomic needed.
	rootEncoded []byte
}

// enableWaveParallel sets up the worker pool + dispatcher and
// switches addNode into the wave-parallel path. writeQ receives
// (nodeKey, encoded) tuples from hash workers; the caller (in
// parallel.go) drains writeQ into a pebble.Batch and is responsible
// for closing writeQ AFTER finishStreaming returns. workers is the
// number of hash worker goroutines (typically NumCPU).
//
// pendingCap bounds the main-thread submission backlog. ~8K means
// peak in-flight leaf bytes ≈ 8K × ~200 B = ~1.6 MB per store —
// well below the original wave-parallel design's 4.4 GB blow-up
// because we hold (key, value) bytes on the parFrame, not full
// iavl.Node objects.
func (s *storeImporter) enableWaveParallel(workers int, writeQ chan<- writeEnt) {
	if workers <= 0 || writeQ == nil {
		return
	}
	s.par = &parState{
		workers:    workers,
		writeQ:     writeQ,
		ready:      make(chan *parFrame, workers*4),
		pendingCap: 8192,
	}
	s.par.cond = sync.NewCond(&s.par.mu)

	s.par.dispatcherWg.Add(1)
	go s.parDispatcher()
	for i := 0; i < workers; i++ {
		s.par.workerWg.Add(1)
		go s.parWorker()
	}
}

// parallelEnabled reports whether wave-parallel mode is active for
// this storeImporter.
func (s *storeImporter) parallelEnabled() bool { return s.par != nil }

// addLeafPar handles the wave-parallel leaf path. The leaf's hash is
// deferred to a worker; the f/ fast-storage entry is written inline
// here to preserve byte-sorted order (post-order leaves yield
// sorted user-keys).
func (s *storeImporter) addLeafPar(setFast func(key, value []byte) error,
	version int64, key, value []byte) error {

	nonce := s.nextNonce(version)

	// Leaf payload is captured in owned buffers — main reuses scratch
	// on the next call, but the worker may read these later.
	f := &parFrame{
		height:    0,
		size:      1,
		version:   version,
		nonce:     nonce,
		leafKey:   append([]byte(nil), key...),
		leafValue: append([]byte(nil), value...),
	}
	// pending stays at 0 for leaves: no children to wait for.

	// f/ entry inline — preserves the sorted-write contract that the
	// fastIngester depends on. Workers writing f/ would lose this
	// ordering since they complete out-of-order.
	s.valueScratch = encodeFastNodeInto(s.valueScratch[:0], version, value)
	s.keyScratch = fastDBKeyInto(s.keyScratch[:0], s.storePrefix, key)
	if err := setFast(s.keyScratch, s.valueScratch); err != nil {
		return fmt.Errorf("set fast node: %w", err)
	}

	s.parStack = append(s.parStack, f)
	s.leafCount++

	// Submit leaf to workers. Leaves have pending==0, so they are
	// immediately ready. blocking=true → main may stall here if the
	// pending slice exceeds pendingCap (memory backpressure).
	s.submitParFrame(f, true)
	return nil
}

// addInnerPar handles the wave-parallel inner path. Pops two child
// frames, builds the parent, links the parent for both children
// (bumping their events counter), and submits the parent if both
// children are already hashed (= pending hits 0 here).
func (s *storeImporter) addInnerPar(version int64, height int8, key []byte) error {
	if !s.closesPairPar(height) {
		return fmt.Errorf("inner node at height %d does not close a sibling pair "+
			"(stack depth=%d, top heights=%v); malformed snapshot stream",
			height, len(s.parStack), s.topHeightsPar())
	}

	right := s.parStack[len(s.parStack)-1]
	left := s.parStack[len(s.parStack)-2]
	s.parStack = s.parStack[:len(s.parStack)-2]

	size := left.size + right.size
	nonce := s.nextNonce(version)

	inner := &parFrame{
		height:     height,
		size:       size,
		version:    version,
		nonce:      nonce,
		innerKey:   append([]byte(nil), key...),
		leftChild:  left,
		rightChild: right,
	}
	inner.pending.Store(2) // both children unhashed-from-our-POV until events==2

	// Link parent for each child. Main contributes the second event
	// (parent-linkage). If the worker already fired its event
	// (events == 1 → 2 here), decrement parent.pending now.
	left.parent = inner
	right.parent = inner
	for _, child := range [2]*parFrame{left, right} {
		if child.events.Add(1) == 2 {
			inner.pending.Add(-1)
		}
	}

	// Mark built, then check pending. Either side that observes
	// "pending==0 && built==true" attempts submission; CAS in
	// submitParFrame ensures only one wins.
	inner.built.Store(true)
	if inner.pending.Load() == 0 {
		s.submitParFrame(inner, true)
	}

	s.parStack = append(s.parStack, inner)
	s.innerCount++
	return nil
}

func (s *storeImporter) closesPairPar(height int8) bool {
	n := len(s.parStack)
	if n < 2 {
		return false
	}
	return s.parStack[n-1].height < height && s.parStack[n-2].height < height
}

func (s *storeImporter) topHeightsPar() []int8 {
	n := len(s.parStack)
	out := make([]int8, 0, 4)
	for i := n - 1; i >= 0 && i >= n-4; i-- {
		out = append(out, s.parStack[i].height)
	}
	return out
}

// submitParFrame appends a frame to the pending queue, with
// CAS-guarded idempotency and main-thread backpressure on cap.
//
// blocking=true is set by the main thread; it stalls on the cond
// when the queue exceeds pendingCap. blocking=false is for workers;
// they always succeed (workers only ever contribute when promoting
// an inner whose pending dropped to 0, which is at most O(items)
// total — and they're already on the consumer side of the
// dispatcher, so blocking them on cap would deadlock).
func (s *storeImporter) submitParFrame(f *parFrame, blocking bool) {
	if !f.submitted.CompareAndSwap(false, true) {
		return
	}
	s.par.hashWg.Add(1)
	s.par.mu.Lock()
	if blocking {
		for len(s.par.pending) >= s.par.pendingCap && !s.par.closed {
			s.par.cond.Wait()
		}
	}
	s.par.pending = append(s.par.pending, f)
	s.par.cond.Broadcast()
	s.par.mu.Unlock()
}

// parDispatcher drains pending → ready, blocking on the ready
// channel's capacity. Exits when finishStreaming has set
// par.closed AND pending is empty (= no work in flight, no work to
// come).
func (s *storeImporter) parDispatcher() {
	defer s.par.dispatcherWg.Done()
	defer close(s.par.ready)
	for {
		s.par.mu.Lock()
		for len(s.par.pending) == 0 && !s.par.closed {
			s.par.cond.Wait()
		}
		if len(s.par.pending) == 0 && s.par.closed {
			s.par.mu.Unlock()
			return
		}
		next := s.par.pending[0]
		s.par.pending = s.par.pending[1:]
		s.par.cond.Broadcast() // wake main waiters that were blocked on cap
		s.par.mu.Unlock()
		s.par.ready <- next
	}
}

// parWorker drains ready, hashes + encodes + pushes to writeQ +
// signals parent. Errors are captured on par.firstErr; the worker
// keeps draining so the dispatcher can exit cleanly.
func (s *storeImporter) parWorker() {
	defer s.par.workerWg.Done()
	for f := range s.par.ready {
		err := s.processParFrame(f)
		if err != nil {
			e := err
			s.par.firstErr.CompareAndSwap(nil, &e)
		}
		s.par.hashWg.Done()
	}
}

// processParFrame hashes + encodes the frame's payload, pushes the
// (nodeKey, encoded) to writeQ, frees the payload buffers, and
// signals the parent's pending counter via the events rendezvous.
func (s *storeImporter) processParFrame(f *parFrame) error {
	// Hash + encode based on leaf vs inner.
	if f.height == 0 {
		f.hash = hashLeaf(f.version, f.leafKey, f.leafValue)
		f.encoded = encodeLeafNode(f.leafKey, f.leafValue)
		// Drop large buffers — children's leafKey/leafValue are no
		// longer reachable through the parent inner's child pointer
		// because the parent only reads f.hash + f.version + f.nonce.
		f.leafKey = nil
		f.leafValue = nil
	} else {
		// Inner — both children's hashes are populated since we won't
		// be submitted until pending == 0 (= both children's
		// events == 2 = both hashed).
		f.hash = hashInner(f.version, f.height, f.size,
			f.leftChild.hash, f.rightChild.hash)
		f.encoded = encodeInnerNode(f.height, f.size, f.innerKey, f.hash,
			f.leftChild.version, f.leftChild.nonce,
			f.rightChild.version, f.rightChild.nonce)
		f.innerKey = nil

		// Drop child pointers. We've extracted the only fields a
		// parent needs (hash + version + nonce) into f.encoded; no
		// further reads happen on leftChild/rightChild from any
		// goroutine. Without this, the inner retains its entire
		// subtree's frames as live state — bank's 14M frames × ~100 B
		// = ~1.4 GB held until root completes (the OOM that killed
		// the first run on a 24 GB box).
		f.leftChild = nil
		f.rightChild = nil
	}

	nodeKey := nodeDBKey(s.storePrefix, f.version, f.nonce)
	// Capture the root's encoded bytes for finalize's canonical-root
	// re-emit BEFORE pushing to writeQ — the writer doesn't retain
	// the slice, but the root's encoded must survive past finalize.
	// Only the root has parent==nil, and only one worker will hash
	// it, so this assignment is race-free.
	rootEncoded := f.encoded
	if f.parent == nil {
		s.par.rootEncoded = rootEncoded
	}
	s.par.writeQ <- writeEnt{key: nodeKey, encoded: f.encoded}
	// f.encoded is now owned by the writer (and, for the root, by
	// par.rootEncoded). We don't need the field on the frame anymore;
	// dropping the slice header lets the frame itself be small for
	// the rest of its lifetime (still reachable via parent.leftChild
	// /rightChild until the parent is hashed and nils those).
	if f.parent != nil {
		f.encoded = nil
	}

	// Worker contributes the "hash-done" event UNCONDITIONALLY — at
	// the time we hash, main may not have linked our parent yet
	// (events==0 here means main hasn't reached addInnerPar for our
	// parent). The 0→1→2 rendezvous works because the atomic Add
	// gives both sides a sequential view: whoever observes the second
	// event has happens-before with both increments, and main's
	// non-atomic parent assignment is program-ordered before main's
	// own events.Add — so by the time *anyone* sees events==2, parent
	// is published and safe to read.
	//
	// Root has no main-side event (no parent linkage), so its events
	// stays at 1 forever; the f.parent==nil check below skips the
	// decrement, which is the correct no-op for the root.
	if f.events.Add(1) == 2 {
		parent := f.parent
		f.parent = nil
		if parent != nil {
			if parent.pending.Add(-1) == 0 {
				if parent.built.Load() {
					s.submitParFrame(parent, false)
				}
			}
		}
	}
	return nil
}

// finishStreaming waits for all submitted frames to be processed,
// then closes the pending queue and waits for the dispatcher and
// workers to exit. After this returns successfully, the writeQ has
// no more pending pushes — the caller can safely close it and wait
// for the writer goroutine.
func (s *storeImporter) finishStreaming() error {
	if !s.parallelEnabled() {
		return nil
	}
	// Wait for all in-flight hashing to settle. This includes any
	// chained submissions a worker triggers when it promotes its
	// parent (the parent's hashWg.Add(1) happens before the
	// triggering worker's hashWg.Done(), so the counter doesn't
	// reach 0 prematurely).
	s.par.hashWg.Wait()
	if e := s.par.firstErr.Load(); e != nil {
		return *e
	}

	s.par.mu.Lock()
	s.par.closed = true
	s.par.cond.Broadcast()
	s.par.mu.Unlock()

	s.par.dispatcherWg.Wait()
	s.par.workerWg.Wait()

	if e := s.par.firstErr.Load(); e != nil {
		return *e
	}
	return nil
}

// finalizePar is the wave-parallel finalize: re-emit the canonical
// root + redirect (if any) + storage_version marker. Must be called
// after finishStreaming has returned and the writer goroutine has
// drained writeQ — at that point the batch is exclusively the
// caller's again, so set may be called inline.
func (s *storeImporter) finalizePar(set func(key, value []byte) error) ([]byte, error) {
	if len(s.parStack) == 0 {
		s.keyScratch = nodeDBKeyInto(s.keyScratch[:0], s.storePrefix, s.height, 1)
		if err := set(s.keyScratch, nil); err != nil {
			return nil, fmt.Errorf("set empty root marker: %w", err)
		}
		if err := s.writeStorageVersionMarker(set); err != nil {
			return nil, err
		}
		return emptyIAVLTreeHash[:], nil
	}
	if len(s.parStack) != 1 {
		return nil, fmt.Errorf("invalid stream: stack has %d roots, expected 1",
			len(s.parStack))
	}
	root := s.parStack[0]
	if s.par.rootEncoded == nil {
		return nil, fmt.Errorf("internal: root encoded bytes missing "+
			"(version=%d nonce=%d height=%d)",
			root.version, root.nonce, root.height)
	}

	s.keyScratch = nodeDBKeyInto(s.keyScratch[:0], s.storePrefix, root.version, 1)
	if err := set(s.keyScratch, s.par.rootEncoded); err != nil {
		return nil, fmt.Errorf("set canonical root at (rootVersion, 1): %w", err)
	}

	if root.version != s.height {
		var redirectVal [13]byte
		redirectVal[0] = 's'
		putBE64(redirectVal[1:], uint64(root.version))
		putBE32(redirectVal[9:], 1)
		s.keyScratch = nodeDBKeyInto(s.keyScratch[:0], s.storePrefix, s.height, 1)
		if err := set(s.keyScratch, redirectVal[:]); err != nil {
			return nil, fmt.Errorf("set root redirect at (snapshotHeight, 1): %w", err)
		}
	}

	if err := s.writeStorageVersionMarker(set); err != nil {
		return nil, err
	}

	return root.hash[:], nil
}

// putBE64 / putBE32 — local big-endian writers to avoid a
// `encoding/binary` import in this file.
func putBE64(b []byte, v uint64) {
	b[0] = byte(v >> 56)
	b[1] = byte(v >> 48)
	b[2] = byte(v >> 40)
	b[3] = byte(v >> 32)
	b[4] = byte(v >> 24)
	b[5] = byte(v >> 16)
	b[6] = byte(v >> 8)
	b[7] = byte(v)
}

func putBE32(b []byte, v uint32) {
	b[0] = byte(v >> 24)
	b[1] = byte(v >> 16)
	b[2] = byte(v >> 8)
	b[3] = byte(v)
}
