package iavl

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"

	"cosmossdk.io/core/store"
)

// maxBatchSize is the maximum size of the import batch before flushing it to the database
const maxBatchSize = 10000

// ErrNoImport is returned when calling methods on a closed importer
var ErrNoImport = errors.New("no import in progress")

// Importer imports data into an empty MutableTree. It is created by MutableTree.Import(). Users
// must call Close() when done.
//
// ExportNodes must be imported in the order returned by Exporter, i.e. depth-first post-order (LRN).
//
// Importer is not concurrency-safe, it is the caller's responsibility to ensure the tree is not
// modified while performing an import.
//
// FORK: wave-parallel import.
//
// The upstream importer hashed nodes synchronously in Add as soon as
// a sibling pair was popped from the stack. We defer that hashing to
// a worker pool and a single batch-writer goroutine. The dependency
// graph implied by the post-order stream is processed bottom-up:
// leaves are immediately ready (no children); inner nodes become
// ready when both children's hashes complete and the inner has been
// "confirmed non-root" by being popped from the stack.
//
// Concurrency primitives live on each Node (importPending, importEvents,
// importBuilt, importSubmitted, importParent — all atomic, all
// zero-valued outside import). The events counter takes one increment
// from the worker (after hashing) and one from the main goroutine
// (after parent linkage); the second event triggers the parent's
// pending decrement. submitToReady uses CAS so a node is enqueued at
// most once even if both sides race to submit it.
//
// The root never has importBuilt set (it's never popped) so workers
// never submit it. Commit drains the pool, then synchronously hashes
// + writes the root with nonce=1 (cosmos-sdk root key convention).
type Importer struct {
	tree    *MutableTree
	version int64
	stack   []*Node
	nonces  []uint32

	// FORK: parallel pipeline.
	workers      int
	ready        chan *Node    // dispatcher → workers (bounded)
	writeQ       chan writeEnt // hashed-node-bytes to commit to batch
	workerWG     sync.WaitGroup
	writerWG     sync.WaitGroup
	dispatcherWG sync.WaitGroup

	// pending is the unbounded slice queue feeding the dispatcher.
	// Workers and the main goroutine both append here (non-blocking),
	// avoiding the producer/consumer cycle that deadlocks if workers
	// directly send to a bounded ready channel — when ready fills,
	// every worker would block on its own submit while no other
	// goroutine is left to drain ready.
	pendingMu     sync.Mutex
	pendingCond   *sync.Cond
	pending       []*Node
	pendingClosed bool

	// hashWG counts non-root nodes whose hash is in flight. Add(1) on
	// submitToReady, Done() in worker after hashing. Commit waits for
	// it to drain.
	hashWG sync.WaitGroup

	// firstErr captures the first error from any goroutine. Commit
	// returns it. Subsequent errors are dropped.
	firstErr atomic.Pointer[error]

	// Batch state. Owned by the writer goroutine; the main goroutine
	// touches it only at Commit (after writer has exited) for the
	// final root + WriteSync.
	batch          store.Batch
	batchSize      uint32
	inflightCommit <-chan error
}

// writeEnt is a hashed node ready to be added to the batch.
type writeEnt struct {
	key   []byte
	bytes []byte
}

// newImporter creates a new Importer for an empty MutableTree.
//
// version should correspond to the version that was initially exported. It must be greater than
// or equal to the highest ExportNode version number given.
func newImporter(tree *MutableTree, version int64) (*Importer, error) {
	if version < 0 {
		return nil, errors.New("imported version cannot be negative")
	}
	if tree.ndb.latestVersion > 0 {
		return nil, fmt.Errorf("found database at version %d, must be 0", tree.ndb.latestVersion)
	}
	if !tree.IsEmpty() {
		return nil, errors.New("tree must be empty")
	}

	workers := runtime.NumCPU()
	if workers > 8 {
		workers = 8
	}
	if workers < 1 {
		workers = 1
	}

	imp := &Importer{
		tree:    tree,
		version: version,
		stack:   make([]*Node, 0, 8),
		nonces:  make([]uint32, version+1),
		batch:   tree.ndb.db.NewBatch(),
		workers: workers,
		ready:   make(chan *Node, 1024),
		writeQ:  make(chan writeEnt, 4096),
	}
	imp.pendingCond = sync.NewCond(&imp.pendingMu)

	imp.dispatcherWG.Add(1)
	go imp.dispatcher()

	imp.workerWG.Add(workers)
	for k := 0; k < workers; k++ {
		go imp.hashWorker()
	}
	imp.writerWG.Add(1)
	go imp.writerLoop()

	return imp, nil
}

// dispatcher drains the unbounded pending slice into the bounded
// ready channel. Single goroutine — only producer to ready, breaking
// the worker-as-producer-and-consumer deadlock cycle. Exits when
// pendingClosed is set and the slice is fully drained.
func (i *Importer) dispatcher() {
	defer i.dispatcherWG.Done()
	defer close(i.ready)
	for {
		i.pendingMu.Lock()
		for len(i.pending) == 0 && !i.pendingClosed {
			i.pendingCond.Wait()
		}
		if i.pendingClosed && len(i.pending) == 0 {
			i.pendingMu.Unlock()
			return
		}
		batch := i.pending
		i.pending = nil
		i.pendingMu.Unlock()
		for _, n := range batch {
			i.ready <- n
		}
	}
}

// closePending tells the dispatcher no more submissions are coming.
// Idempotent.
func (i *Importer) closePending() {
	i.pendingMu.Lock()
	if !i.pendingClosed {
		i.pendingClosed = true
		i.pendingCond.Broadcast()
	}
	i.pendingMu.Unlock()
}

// setErr records the first error from any goroutine. Subsequent errors
// are dropped — the first failure is the most informative cause.
func (i *Importer) setErr(err error) {
	if err == nil {
		return
	}
	i.firstErr.CompareAndSwap(nil, &err)
}

// hashWorker hashes nodes pulled from the ready channel, sends the
// resulting batch entry to the writer, and propagates the "hashed"
// event up the dependency graph (which may make the parent ready).
func (i *Importer) hashWorker() {
	defer i.workerWG.Done()
	for node := range i.ready {
		if i.firstErr.Load() != nil {
			i.hashWG.Done()
			continue
		}
		k, b, err := i.hashAndSerialize(node)
		if err != nil {
			i.setErr(err)
			i.hashWG.Done()
			continue
		}
		// Send to writer. If writer has crashed, hashWG.Done still
		// runs so Commit can proceed to error handling.
		i.writeQ <- writeEnt{key: k, bytes: b}

		// eventDone fires the "hashed" event. If main has already
		// fired "parent set" (events would go to 2), the drop in
		// eventDone runs now. Otherwise main's inner-Add will fire
		// the second event later and drop then.
		i.eventDone(node)
		i.hashWG.Done()
	}
}

// hashAndSerialize computes a node's hash and protobuf-serialises it.
// Concurrency-safe across distinct nodes: the only shared state is
// the global bufPool, and each node's _hash reads only its own and
// its children's already-finalised state.
func (i *Importer) hashAndSerialize(node *Node) ([]byte, []byte, error) {
	node._hash(node.nodeKey.version)
	if err := node.validate(); err != nil {
		return nil, nil, err
	}

	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufPool.Put(buf)

	if err := node.writeBytes(buf); err != nil {
		return nil, nil, err
	}
	bytesCopy := make([]byte, buf.Len())
	copy(bytesCopy, buf.Bytes())
	return i.tree.ndb.nodeKey(node.GetKey()), bytesCopy, nil
}

// writerLoop drains writeQ and appends each entry to the in-memory
// pebble batch. Single goroutine — pebble.Batch is not thread-safe.
// Triggers an async batch.Write (via inflightCommit) every maxBatchSize
// entries; the previous in-flight write is awaited before starting
// the next so we never have more than 2 batches alive at once.
func (i *Importer) writerLoop() {
	defer i.writerWG.Done()
	for ent := range i.writeQ {
		if err := i.batch.Set(ent.key, ent.bytes); err != nil {
			i.setErr(err)
			continue
		}
		i.batchSize++
		if i.batchSize >= maxBatchSize {
			if err := i.flushBatch(); err != nil {
				i.setErr(err)
			}
		}
	}
}

// flushBatch hands the current batch off to a background commit
// goroutine and starts a fresh batch. Caller must own the writer
// goroutine.
func (i *Importer) flushBatch() error {
	if i.inflightCommit != nil {
		if err := <-i.inflightCommit; err != nil {
			i.inflightCommit = nil
			return err
		}
		i.inflightCommit = nil
	}
	result := make(chan error, 1)
	i.inflightCommit = result
	go func(batch store.Batch) {
		defer batch.Close()
		result <- batch.Write()
	}(i.batch)
	i.batch = i.tree.ndb.db.NewBatch()
	i.batchSize = 0
	return nil
}

// submitToReady enqueues a node for hashing exactly once. Both the
// main goroutine (when an inner-Add finishes setting up children) and
// any worker (when its decrement makes a parent ready) may try to
// submit; the CAS picks one winner. Append is non-blocking under a
// mutex — the dispatcher goroutine forwards from `pending` to the
// bounded `ready` channel.
func (i *Importer) submitToReady(node *Node) {
	if !node.importSubmitted.CompareAndSwap(false, true) {
		return
	}
	i.hashWG.Add(1)
	i.pendingMu.Lock()
	i.pending = append(i.pending, node)
	i.pendingCond.Signal()
	i.pendingMu.Unlock()
}

// eventDone increments a node's events counter (worker contributes
// "hashed", main contributes "parent-set"). When both have fired, the
// parent's pending count is decremented AND the node's bytes are
// dropped — at this point the node has been hashed (so writeBytes ran
// using key/value/etc.) AND it's been popped from the stack
// (confirmed non-root, so Commit won't re-serialise it). The root
// never has its events hit 2 (no parent → no parent-set event), so
// its bytes survive for Commit's nonce=1 re-serialisation.
func (i *Importer) eventDone(node *Node) {
	if node.importEvents.Add(1) == 2 {
		i.decrementParent(node)
		// Both events fired. Drop fields used during hash + serialise.
		// Frees ~80% of leaf memory — for cosmoshub bank with 9M
		// leaves this is GBs reclaimed mid-import. Inner nodes also
		// free their child pointers (not needed once their own hash
		// is in node.hash).
		node.key = nil
		node.value = nil
		node.leftNodeKey = nil
		node.rightNodeKey = nil
		if node.subtreeHeight > 0 {
			node.leftNode = nil
			node.rightNode = nil
		}
	}
}

// decrementParent decrements parent.importPending. When pending hits
// zero, if the parent has been confirmed non-root (importBuilt set),
// submit it for hashing. Root never has importBuilt set, so it
// never enters the pool — Commit handles it.
func (i *Importer) decrementParent(child *Node) {
	parent := child.importParent
	if parent == nil {
		return
	}
	if parent.importPending.Add(-1) == 0 {
		if parent.importBuilt.Load() {
			i.submitToReady(parent)
		}
	}
}

// markBuilt marks a node as confirmed non-root (popped from the
// stack). If pending is already 0 (children were hashed before the
// parent's Add ran), the worker's decrement may have raced ahead and
// found importBuilt still false; we re-check here and submit.
func (i *Importer) markBuilt(node *Node) {
	node.importBuilt.Store(true)
	if node.importPending.Load() == 0 {
		i.submitToReady(node)
	}
}

// Add adds an ExportNode to the import. ExportNodes must be added in the order returned by
// Exporter, i.e. depth-first post-order (LRN). Nodes are periodically flushed to the database,
// but the imported version is not visible until Commit() is called.
func (i *Importer) Add(exportNode *ExportNode) error {
	if i.tree == nil {
		return ErrNoImport
	}
	if exportNode == nil {
		return errors.New("node cannot be nil")
	}
	if exportNode.Version > i.version {
		return fmt.Errorf("node version %v can't be greater than import version %v",
			exportNode.Version, i.version)
	}
	// Surface any worker error early.
	if e := i.firstErr.Load(); e != nil {
		return *e
	}

	node := &Node{
		key:           exportNode.Key,
		value:         exportNode.Value,
		subtreeHeight: exportNode.Height,
	}

	// We build the tree from the bottom-left up. The stack is used to store unresolved left
	// children while constructing right children. When all children are built, the parent can
	// be constructed and the resolved children can be discarded from the stack. Using a stack
	// ensures that we can handle additional unresolved left children while building a right branch.
	//
	// We don't modify the stack until we've verified the built node, to avoid leaving the
	// importer in an inconsistent state when we return an error.
	stackSize := len(i.stack)
	closesPair := false
	var leftNode, rightNode *Node
	if node.subtreeHeight == 0 {
		node.size = 1
	} else if stackSize >= 2 && i.stack[stackSize-1].subtreeHeight < node.subtreeHeight && i.stack[stackSize-2].subtreeHeight < node.subtreeHeight {
		closesPair = true
		leftNode = i.stack[stackSize-2]
		rightNode = i.stack[stackSize-1]

		node.leftNode = leftNode
		node.rightNode = rightNode
		node.leftNodeKey = leftNode.GetKey()
		node.rightNodeKey = rightNode.GetKey()
		node.size = leftNode.size + rightNode.size
	}
	i.nonces[exportNode.Version]++
	node.nodeKey = &NodeKey{
		version: exportNode.Version,
		// Nonce is 1-indexed, but start at 2 since the root node having a nonce of 1.
		nonce: i.nonces[exportNode.Version] + 1,
	}

	if node.subtreeHeight == 0 {
		// Leaves: pending=0 (no children to wait for). Mark built and
		// submit immediately. The worker hashes; later, when the
		// parent's inner-Add runs, eventDone fires the second event
		// and decrements parent's pending.
		node.importBuilt.Store(true)
		i.submitToReady(node)
	} else if closesPair {
		// Inner closing a sibling pair. Initialise pending=2 BEFORE
		// linking children so a worker that races ahead doesn't see
		// pending=0 prematurely.
		node.importPending.Store(2)

		// Both children are now off the stack — confirmed non-root.
		// Mark them built. If their own pending is already 0, this
		// also submits them (in case workers had already drained
		// their grandchildren ahead of this Add).
		i.markBuilt(leftNode)
		i.markBuilt(rightNode)

		// Link parent and fire the "parent-set" event for each child.
		// If the child has already been hashed, this triggers the
		// pending decrement immediately.
		leftNode.importParent = node
		i.eventDone(leftNode)
		rightNode.importParent = node
		i.eventDone(rightNode)

		// Pop children from stack.
		i.stack = i.stack[:stackSize-2]

		// FORK: do NOT nil out leftNode.leftNode / rightNode here.
		// Upstream nils these to drop references to grandchildren
		// that are no longer needed, but it does so AFTER calling
		// writeNode (which hashes them). In our model the children
		// are hashed asynchronously by workers. Nilling now would
		// race the worker's _hash, which reads child.leftNode.hash
		// for inner children. Workers nil these themselves after
		// hashing — see hashWorker.
	}
	// Inner that doesn't close a pair (rare; only happens at the very
	// start of malformed streams): pending stays 0 but built stays
	// false until popped. Will be marked built when its parent's
	// inner-Add runs and pops it.

	i.stack = append(i.stack, node)
	return nil
}

// Commit finalises the import by waiting for all in-flight hash work
// to drain, hashing the root (with nonce=1), flushing remaining
// batches, and marking the version visible. It can only be called
// once, and calls Close() internally.
func (i *Importer) Commit() error {
	if i.tree == nil {
		return ErrNoImport
	}

	// 1. Wait for all submitted nodes to finish hashing. Workers
	// continue draining ready until we close it via the dispatcher,
	// and may submit more parents along the way as decrement chains
	// fire.
	i.hashWG.Wait()

	// 2. No more submissions can come from main (Adds done) or from
	// workers (no pending nodes left to ready). Tell the dispatcher
	// to drain and close ready; that propagates: workers exit on
	// closed ready → close writeQ → writer exits.
	i.closePending()
	i.dispatcherWG.Wait()
	i.workerWG.Wait()
	close(i.writeQ)
	i.writerWG.Wait()

	// 3. If anything went wrong, surface it before touching the
	// final root.
	if e := i.firstErr.Load(); e != nil {
		i.Close()
		return *e
	}

	// 4. Handle the root. Stack must hold exactly one element by now
	// (or none, for the empty-tree case).
	switch len(i.stack) {
	case 0:
		if err := i.batch.Set(i.tree.ndb.nodeKey(GetRootKey(i.version)), []byte{}); err != nil {
			i.Close()
			return err
		}
	case 1:
		root := i.stack[0]
		// Override the provisional nonce with the canonical root nonce=1.
		// Hash and write *after* this so the cached root.hash and the
		// stored bytes use the final nonce. (Hash actually doesn't
		// depend on nonce — see writeHashBytes — but writeBytes does
		// for the leftNodeKey / rightNodeKey of the root's parent,
		// which is moot here because root has no parent.)
		root.nodeKey.nonce = 1
		k, b, err := i.hashAndSerialize(root)
		if err != nil {
			i.Close()
			return err
		}
		if err := i.batch.Set(k, b); err != nil {
			i.Close()
			return err
		}
		if root.nodeKey.version < i.version { // there is no update in this version
			if err := i.batch.Set(i.tree.ndb.nodeKey(GetRootKey(i.version)), i.tree.ndb.nodeKey(root.nodeKey.GetKey())); err != nil {
				i.Close()
				return err
			}
		}
	default:
		i.Close()
		return fmt.Errorf("invalid node structure, found stack size %v when committing",
			len(i.stack))
	}

	// 5. Drain the inflight commit (if any), then write the final
	// batch synchronously so the version is durable.
	if i.inflightCommit != nil {
		if err := <-i.inflightCommit; err != nil {
			i.Close()
			return err
		}
		i.inflightCommit = nil
	}
	if err := i.batch.WriteSync(); err != nil {
		i.Close()
		return err
	}
	i.tree.ndb.resetLatestVersion(i.version)

	if _, err := i.tree.LoadVersion(i.version); err != nil {
		i.Close()
		return err
	}

	i.Close()
	return nil
}

// Close frees all resources. Safe to call multiple times. If Commit
// has not been called, in-flight goroutines are torn down and any
// uncommitted work is discarded.
func (i *Importer) Close() {
	// Commit may have already drained the goroutines (idempotent
	// here: dispatcher/worker/writer have exited and ready/writeQ
	// have been closed). If Commit was NOT called, drive the
	// shutdown sequence manually.
	if i.pendingCond != nil {
		i.closePending()        // idempotent
		i.dispatcherWG.Wait()   // dispatcher exits, closes ready
		i.workerWG.Wait()       // workers exit on closed ready
		// writeQ: close once. Use safeClose since Commit may have
		// already closed it.
		if i.writeQ != nil {
			safeClose(i.writeQ)
			i.writerWG.Wait()
			i.writeQ = nil
		}
		i.pendingCond = nil
	}
	if i.inflightCommit != nil {
		<-i.inflightCommit
		i.inflightCommit = nil
	}
	if i.batch != nil {
		i.batch.Close()
		i.batch = nil
	}
	i.tree = nil
}

// safeClose closes a channel idempotently, swallowing the panic from
// closing an already-closed channel. Used in Close so that double-
// close (Commit closes, then defer Close also closes) is harmless.
func safeClose[T any](ch chan T) {
	defer func() { _ = recover() }()
	close(ch)
}
