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

## Why only 2-way parallelism

The current change unlocks at most 2 cores per store. A sibling pair
is the only set of nodes the upstream algorithm hashes "together" in
a single Add call. To go beyond, we'd need to defer all hashing to
Commit and walk the tree bottom-up in waves, with up to wave-width
parallelism. That's a larger rewrite and a separate change.
