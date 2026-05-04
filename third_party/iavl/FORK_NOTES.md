# cosmos/iavl fork — local modifications

Forked from `github.com/cosmos/iavl v1.3.6`. Wired in via the
`replace` directive at the bottom of the parent module's `go.mod`.

## Why

`Importer.Add` is the bottleneck during snapshot import. Each Add call
that closes a sibling pair does two `writeNode` calls back-to-back —
hash + serialise + batch.Set for the left child, then the same for
the right. Both are CPU-bound (sha256 over key+value for leaves,
sha256 over varint-encoded child hashes for inner nodes). Sibling
nodes share no mutable state — `_hash` only reads each node's own
already-populated subtree state — so the two `writeNode` calls are
trivially parallelisable.

## What changed

`import.go` only.

- Refactored `writeNode` into `hashAndSerialize` (CPU work, parallel-
  safe) + `writeBatched` (`pebble.Batch.Set` + flush, must stay
  serial because pebble.Batch is not thread-safe).
- Added a single persistent `hashWorker` goroutine, started in
  `newImporter`, drained + waited in `Close`.
- New `writeNodePair(left, right)`: dispatches the left child to the
  worker, hashes the right child inline on the caller goroutine,
  joins, then writes both batch entries left-then-right.
- `Add` calls `writeNodePair` instead of two sequential `writeNode`
  calls.

## Consistency

Output is byte-identical to upstream:

- Hash values are determined by node fields and child hashes — both
  are fully populated before either sibling is hashed. Order of
  computation can't affect either's result.
- Batch entries are written in the same left-then-right order, so
  the pebble manifest sees the same key sequence as upstream.
- The root hash on Commit is verified externally against the
  snapshot's `AppHash`. If consistency breaks, the AppHash check at
  the end of `cosmos-snapshot-to-appdb` (or the cometbft handshake
  in gaiad) will catch it loudly.

## Wave-parallel rewrite (current)

Add no longer hashes anything. It only builds the tree shape on the
stack, sets per-node atomic state, and forwards work to a pool of
hash workers and a single batch-writer goroutine.

`node.go` got five fields (all zero-valued outside import use):

  - `importPending` (atomic.Int32): children whose hash is pending.
    2 for inner, 0 for leaves. Decremented as children's
    "both events fired" (hashed + parent-set) propagates upward.
  - `importEvents` (atomic.Int32): counts the two events that must
    fire on a node before its parent's pending can be decremented.
    The worker contributes "hashed"; the main goroutine contributes
    "parent-set" inside the inner-Add for the grandparent.
  - `importBuilt` (atomic.Bool): true once the node has been popped
    from the stack (= confirmed non-root). Workers only submit a
    parent to ready when both pending==0 AND built==true.
  - `importSubmitted` (atomic.Bool): CAS-guarded "have we already
    pushed this node into the ready channel?" — both main and worker
    can race to submit; CAS picks one.
  - `importParent` (*Node): set when the node is popped during its
    parent's inner-Add.

`import.go` rewritten:

  - `newImporter` starts N hash workers (default min(NumCPU, 8)) and
    1 writer goroutine. The writer owns the pebble batch.
  - Add submits leaves to the ready channel immediately, marks them
    built. For inner nodes that close a sibling pair, it sets
    pending=2, marks both children built (which may submit them if
    they were already pending=0 from grandchildren completion),
    links each child's parent pointer, and fires the parent-set
    event for each.
  - Workers drain ready, hash + serialise, push to writeQ, then call
    `eventDone` on the hashed node. eventDone may decrement the
    parent's pending count and submit the parent to ready when the
    parent is also built.
  - The writer drains writeQ, calls batch.Set, and triggers async
    flushes at maxBatchSize. Single goroutine — pebble.Batch is not
    thread-safe.
  - Commit waits on `hashWG` (counts submitted-but-not-hashed),
    closes ready (workers exit), closes writeQ (writer exits), then
    handles the root: it's the only stack entry, was never marked
    built, never went through the pool. Commit overrides its nonce
    to 1 and synchronously writes it via the (now writer-free)
    batch, awaits any inflight commit, and WriteSyncs.

## Consistency

Output is byte-identical to upstream:

- Hash values are determined by node fields and child hashes — both
  are fully populated before any worker reads them. Order of
  hash computation cannot affect the result.
- Per-store batch entries arrive in different orders compared to
  upstream (workers drain in completion order, not insertion order),
  but each iavl node-key is unique so the on-disk pebble manifest is
  identical after batch flush.
- Root nonce override matches upstream Commit exactly.
- Verified on cosmoshub-4 height 30,950,000: AppHash
  `0F22F949B584C1C0ABBDB1A045C5B02B46151675FD64C4ACD4A208B937C9DDB5`
  matches consensus.

## Memory

Live frontier during import is bounded by tree depth × peak wave
width. For cosmoshub-4 bank (9M leaves, depth ≈ 23), the worst-case
in-flight working set is a few thousand nodes × node size — under
a GB even when the value bytes are still attached to leaves
awaiting hashing. After a node's parent is hashed, the GC can
reclaim it.
