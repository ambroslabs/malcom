package iavl

import (
	"bytes"
	"errors"
	"fmt"
	"sync"

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
// FORK: a single hashing worker runs alongside the caller goroutine.
// In Add(), when a sibling pair is built, the left child's hash +
// serialise is dispatched to the worker while the caller hashes the
// right child inline. Sibling nodes share no mutable state, so this
// is safe (each _hash reads only its own already-populated subtree
// hashes). batch.Set remains serial — pebble.Batch is not thread-safe.
type Importer struct {
	tree      *MutableTree
	version   int64
	batch     store.Batch
	batchSize uint32
	stack     []*Node
	nonces    []uint32

	// inflightCommit tracks a batch commit, if any.
	inflightCommit <-chan error

	// FORK: persistent hashing worker. hashJobs receives left-child
	// nodes for parallel hash + serialise; results return on the
	// per-call out channel. workerWG closes when the worker exits
	// after Close() drains the channel.
	hashJobs chan hashJob
	workerWG sync.WaitGroup
}

// hashJob is a hash + serialise unit submitted to the persistent worker.
type hashJob struct {
	node *Node
	out  chan<- hashResult
}

// hashResult carries the hash + serialised batch entry produced by
// hashAndSerialize. err is non-nil iff hashing or serialisation failed.
type hashResult struct {
	key, bytes []byte
	err        error
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

	imp := &Importer{
		tree:     tree,
		version:  version,
		batch:    tree.ndb.db.NewBatch(),
		stack:    make([]*Node, 0, 8),
		nonces:   make([]uint32, version+1),
		hashJobs: make(chan hashJob, 1),
	}
	imp.workerWG.Add(1)
	go imp.hashWorker()
	return imp, nil
}

// hashAndSerialize computes the node's hash and serialises it to bytes.
// Concurrency-safe with another hashAndSerialize on a different node:
// the only shared state is the global bufPool and each node's own
// fields. Sibling nodes don't share children, so reads of child .hash
// fields don't race with each other.
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

// hashWorker drains hashJobs until the channel is closed and dispatches
// results back through each job's out channel. There is exactly one
// worker (sibling pair → 1 dispatch + 1 inline = 2-way parallel).
func (i *Importer) hashWorker() {
	defer i.workerWG.Done()
	for job := range i.hashJobs {
		k, b, err := i.hashAndSerialize(job.node)
		job.out <- hashResult{key: k, bytes: b, err: err}
	}
}

// writeBatched appends a hashed (key, bytes) entry to the in-memory
// pebble batch and flushes (asynchronously) when batchSize is reached.
// Caller must own the goroutine — pebble.Batch.Set is not thread-safe,
// so all calls must funnel through one goroutine (Add()).
func (i *Importer) writeBatched(key, value []byte) error {
	if err := i.batch.Set(key, value); err != nil {
		return err
	}
	i.batchSize++
	if i.batchSize >= maxBatchSize {
		var err error
		if i.inflightCommit != nil {
			err = <-i.inflightCommit
			i.inflightCommit = nil
		}
		if err != nil {
			return err
		}
		result := make(chan error)
		i.inflightCommit = result
		go func(batch store.Batch) {
			defer batch.Close()
			result <- batch.Write()
		}(i.batch)
		i.batch = i.tree.ndb.db.NewBatch()
		i.batchSize = 0
	}
	return nil
}

// writeNode hashes, serialises, and queues a single node into the
// batch. Used by Commit() for the final root.
func (i *Importer) writeNode(node *Node) error {
	key, bytes, err := i.hashAndSerialize(node)
	if err != nil {
		return err
	}
	return i.writeBatched(key, bytes)
}

// writeNodePair hashes leftNode and rightNode in parallel (left on the
// background hashWorker, right inline) and then writes both to the
// batch in left-then-right order. Order in the batch doesn't matter
// for correctness — pebble keys are unique per node — but matching the
// original sequential order keeps the batch contents byte-identical to
// the upstream importer for easier diffing.
func (i *Importer) writeNodePair(leftNode, rightNode *Node) error {
	out := make(chan hashResult, 1)
	i.hashJobs <- hashJob{node: leftNode, out: out}

	rk, rb, rerr := i.hashAndSerialize(rightNode)
	lr := <-out

	if lr.err != nil {
		return lr.err
	}
	if rerr != nil {
		return rerr
	}
	if err := i.writeBatched(lr.key, lr.bytes); err != nil {
		return err
	}
	return i.writeBatched(rk, rb)
}

// Close frees all resources. It is safe to call multiple times. Uncommitted nodes may already have
// been flushed to the database, but will not be visible.
func (i *Importer) Close() {
	// FORK: tear down the hash worker first. After this returns, no
	// goroutine is sending writes to i.batch concurrently.
	if i.hashJobs != nil {
		close(i.hashJobs)
		i.workerWG.Wait()
		i.hashJobs = nil
	}
	if i.inflightCommit != nil {
		<-i.inflightCommit
		i.inflightCommit = nil
	}
	if i.batch != nil {
		i.batch.Close()
	}
	i.batch = nil
	i.tree = nil
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
	if node.subtreeHeight == 0 {
		node.size = 1
	} else if stackSize >= 2 && i.stack[stackSize-1].subtreeHeight < node.subtreeHeight && i.stack[stackSize-2].subtreeHeight < node.subtreeHeight {
		leftNode := i.stack[stackSize-2]
		rightNode := i.stack[stackSize-1]

		node.leftNode = leftNode
		node.rightNode = rightNode
		node.leftNodeKey = leftNode.GetKey()
		node.rightNodeKey = rightNode.GetKey()
		node.size = leftNode.size + rightNode.size

		// Update the stack now.
		if err := i.writeNodePair(leftNode, rightNode); err != nil {
			return err
		}
		i.stack = i.stack[:stackSize-2]

		// remove the recursive references to avoid memory leak
		leftNode.leftNode = nil
		leftNode.rightNode = nil
		rightNode.leftNode = nil
		rightNode.rightNode = nil
	}
	i.nonces[exportNode.Version]++
	node.nodeKey = &NodeKey{
		version: exportNode.Version,
		// Nonce is 1-indexed, but start at 2 since the root node having a nonce of 1.
		nonce: i.nonces[exportNode.Version] + 1,
	}

	i.stack = append(i.stack, node)

	return nil
}

// Commit finalizes the import by flushing any outstanding nodes to the database, making the
// version visible, and updating the tree metadata. It can only be called once, and calls Close()
// internally.
func (i *Importer) Commit() error {
	if i.tree == nil {
		return ErrNoImport
	}

	switch len(i.stack) {
	case 0:
		if err := i.batch.Set(i.tree.ndb.nodeKey(GetRootKey(i.version)), []byte{}); err != nil {
			return err
		}
	case 1:
		i.stack[0].nodeKey.nonce = 1
		if err := i.writeNode(i.stack[0]); err != nil {
			return err
		}
		if i.stack[0].nodeKey.version < i.version { // it means there is no update in the given version
			if err := i.batch.Set(i.tree.ndb.nodeKey(GetRootKey(i.version)), i.tree.ndb.nodeKey(i.stack[0].nodeKey.GetKey())); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("invalid node structure, found stack size %v when committing",
			len(i.stack))
	}

	err := i.batch.WriteSync()
	if err != nil {
		return err
	}
	i.tree.ndb.resetLatestVersion(i.version)

	_, err = i.tree.LoadVersion(i.version)
	if err != nil {
		return err
	}

	i.Close()
	return nil
}
