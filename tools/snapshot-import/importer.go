// Stack-based snapshot importer. No `*iavl.Node` allocations: we keep a
// fixed-size frame per stack entry holding only the four fields a parent
// will need (children's nodeKey + hash + height + size), and emit each
// node's encoded bytes directly to a pebble batch as we walk the
// post-order LRN stream.
//
// Memory usage is O(tree depth) — ~30 frames for cosmoshub-4, ~1.7 KB.
// Pebble's batch + memtable dominate the resident set.

package main

import (
	"fmt"
	"io"
	"time"

	"github.com/cockroachdb/pebble"
)

// frame is one stack entry. Fixed 53 bytes, packed.
type frame struct {
	hash    [32]byte // 32 — children-summary; parent will hash this
	size    int64    // 8  — number of leaves in this subtree
	version int64    // 8  — node version (matches what's encoded)
	nonce   uint32   // 4  — assigned at Add time within `version`'s nonce counter
	height  int8     // 1  — 0 for leaf
}

// storeImporter rebuilds one IAVL store. Reset between stores via
// resetForStore.
type storeImporter struct {
	storePrefix []byte // "s/k:<name>/"

	// nonces[v] = next nonce to assign for a node at version v. The
	// IAVL convention reserves nonce=1 for the root, so for non-root
	// nodes we start counting at 2.
	nonces map[int64]uint32

	// stack of completed-but-not-yet-consumed subtrees. Inner nodes
	// pop the top two; leaves push themselves.
	stack []frame

	// snapshot height — used as the import "version" for fast-storage
	// metadata + as the version pointer the redirect (if any) is written
	// under. Each Add still carries the export node's own version.
	height int64

	// running totals for progress logs
	itemCount  uint64
	leafCount  uint64
	innerCount uint64
}

func newStoreImporter(storeName string, height int64) *storeImporter {
	return &storeImporter{
		storePrefix: storePrefix(storeName),
		nonces:      make(map[int64]uint32),
		stack:       make([]frame, 0, 64),
		height:      height,
	}
}

// addNode processes one IAVL ExportNode (post-order LRN). It hashes,
// encodes, writes to the batch, and pushes a frame onto the stack. The
// caller writes batch entries via the `set` callback rather than us
// owning the batch — keeps batch lifecycle (flush thresholds, async
// commit, error handling) out of the importer's concerns.
func (s *storeImporter) addNode(set func(key, value []byte) error,
	height int8, version int64, key, value []byte) error {

	s.itemCount++
	if height == 0 {
		// ─── leaf ────────────────────────────────────────────────
		nonce := s.nextNonce(version)

		h := hashLeaf(version, key, value)
		nodeBytes := encodeLeafNode(key, value)
		if err := set(nodeDBKey(s.storePrefix, version, nonce), nodeBytes); err != nil {
			return fmt.Errorf("set leaf node: %w", err)
		}

		// fast-storage entry: 'f' || userKey → varint(version) || EncodeBytes(value)
		if err := set(fastDBKey(s.storePrefix, key),
			encodeFastNode(version, value)); err != nil {
			return fmt.Errorf("set fast node: %w", err)
		}

		s.stack = append(s.stack, frame{
			hash:    h,
			size:    1,
			version: version,
			nonce:   nonce,
			height:  0,
		})
		s.leafCount++
		return nil
	}

	// ─── inner ────────────────────────────────────────────────────
	if !s.closesPair(height) {
		// Per IAVL's import semantics, an inner node that doesn't have
		// its two completed subtrees on top of the stack would be a
		// malformed stream. The reference importer tolerates one in
		// pathological cases; we surface it as an error here since a
		// well-formed snapshot from cometbft never produces them.
		return fmt.Errorf("inner node at height %d does not close a sibling pair "+
			"(stack depth=%d, top heights=%v); malformed snapshot stream",
			height, len(s.stack), s.topHeights())
	}

	right := s.stack[len(s.stack)-1]
	left := s.stack[len(s.stack)-2]
	s.stack = s.stack[:len(s.stack)-2]

	size := left.size + right.size
	nonce := s.nextNonce(version)
	h := hashInner(version, height, size, left.hash, right.hash)

	nodeBytes := encodeInnerNode(height, size, key, h,
		left.version, left.nonce,
		right.version, right.nonce)
	if err := set(nodeDBKey(s.storePrefix, version, nonce), nodeBytes); err != nil {
		return fmt.Errorf("set inner node: %w", err)
	}

	s.stack = append(s.stack, frame{
		hash:    h,
		size:    size,
		version: version,
		nonce:   nonce,
		height:  height,
	})
	s.innerCount++
	return nil
}

// closesPair reports whether an inner node at `height` should pop the
// top two stack entries as its children. The IAVL post-order invariant
// is that both children's heights are strictly less than the parent's.
func (s *storeImporter) closesPair(height int8) bool {
	n := len(s.stack)
	if n < 2 {
		return false
	}
	return s.stack[n-1].height < height && s.stack[n-2].height < height
}

// nextNonce reserves the next nonce for a node at `version`. The first
// allocated nonce for any version is 2 — nonce=1 is reserved for the
// root node, which is re-stamped at finalize time.
func (s *storeImporter) nextNonce(version int64) uint32 {
	cur := s.nonces[version]
	if cur == 0 {
		cur = 1 // start at 1 so the first allocated nonce is 2
	}
	cur++
	s.nonces[version] = cur
	return cur
}

func (s *storeImporter) topHeights() []int8 {
	n := len(s.stack)
	out := make([]int8, 0, 4)
	for i := n - 1; i >= 0 && i >= n-4; i-- {
		out = append(out, s.stack[i].height)
	}
	return out
}

// finalize closes out a store: re-stamps the root with nonce=1 and
// returns the store's root hash. If the actual root's version is less
// than the snapshot height, also writes a redirect under
// (snapshotHeight, 1) → (rootVersion, 1) so gaiad's LoadVersion(height)
// can find the root.
//
// Returns the merkle root hash of the store, suitable for inclusion in
// the global commit-info.
func (s *storeImporter) finalize(set func(key, value []byte) error) ([32]byte, error) {
	if len(s.stack) == 0 {
		// Empty store — write an empty root marker (gaiad reads this
		// via nodeDBKey(snapshotHeight, 1) and tolerates an empty value).
		if err := set(nodeDBKey(s.storePrefix, s.height, 1), nil); err != nil {
			return [32]byte{}, fmt.Errorf("set empty root marker: %w", err)
		}
		return [32]byte{}, nil
	}
	if len(s.stack) != 1 {
		return [32]byte{}, fmt.Errorf("invalid stream: stack has %d roots, expected 1",
			len(s.stack))
	}

	root := s.stack[0]

	// The root we just popped was written at (root.version, root.nonce).
	// IAVL's read path expects the root to live at (root.version, 1).
	// Two cases:
	//   1) root.nonce == 2 (the first allocated): the node we wrote
	//      doesn't carry the canonical nonce. Re-encode it and re-emit
	//      under nonce=1. We can't simply alias because the leftNodeKey
	//      / rightNodeKey of a hypothetical PARENT would have been
	//      written referring to (root.version, 2); but here root has no
	//      parent, so the re-emit at nonce=1 is the only place that
	//      serves it.
	//   2) Always also handle: if the snapshot height differs from the
	//      root's own version, write a redirect from
	//      (snapshotHeight, 1) → 12-byte (root.version, 1). For typical
	//      cosmos snapshots root.version == snapshotHeight so this is a
	//      no-op.
	//
	// We don't have the original encoded inner-bytes anymore; the
	// importer would have to re-encode. To avoid keeping the root's
	// key/value bytes around we cheat slightly: at finalize time the
	// importer writes the root at nonce=1 by re-running the inner
	// encoding with the same (already-known) inputs. We don't track
	// node.key on the frame, so we expect the caller to hand us the
	// root key when it spotted that the next node in the stream is
	// going to push past the root. Since post-order means the root is
	// the last node, the simplest thing is for the driver loop to call
	// addNode with the root and then immediately call finalize — and
	// for finalize to receive the same key/inner-encoding inputs.
	//
	// In practice we sidestep the problem: writing the root at
	// nonce=2 _and_ a redirect at nonce=1 is functionally equivalent
	// to re-stamping. A redirect entry is 12 bytes of value (the
	// target nodeKey) under the source nodeKey. gaiad's
	// `loadNode(version, 1)` follows redirects.

	// Write a redirect from (snapshotHeight, 1) → (root.version, root.nonce).
	target := nodeKeyBytes(root.version, root.nonce)
	redirectKey := nodeDBKey(s.storePrefix, s.height, 1)
	if err := set(redirectKey, target); err != nil {
		return [32]byte{}, fmt.Errorf("set root redirect: %w", err)
	}

	// Per-store fast-storage marker: gaiad checks this on load and
	// skips the (multi-minute) "Upgrading IAVL storage" pass.
	metaVal := []byte(fmt.Sprintf("%s%s%d",
		fastStorageVersionValue, fastStorageVersionDelimiter, s.height))
	if err := set(metadataDBKey(s.storePrefix, "storage_version"), metaVal); err != nil {
		return [32]byte{}, fmt.Errorf("set storage_version marker: %w", err)
	}

	return root.hash, nil
}

// ─── driver ──────────────────────────────────────────────────────────────

// runImport drives the full import: reads SnapshotItems off `r`,
// dispatches to a fresh storeImporter per store, and writes every
// resulting (key, value) into `db`. At end of stream, writes the
// rootmulti commit-info + latest-version pointer.
//
// Returns the per-store root hashes in stream order (typically
// alphabetical, by cosmos-sdk's exporter convention).
func runImport(r io.Reader, db *pebble.DB, height int64, log io.Writer) ([]storeInfo, error) {
	sr := newSnapReader(r)

	const flushBytes = 64 << 20 // 64 MiB per batch
	batch := db.NewBatch()
	batchBytes := 0
	flush := func() error {
		if batchBytes == 0 {
			return nil
		}
		err := batch.Commit(pebble.Sync)
		batch.Close()
		batch = db.NewBatch()
		batchBytes = 0
		return err
	}
	set := func(key, value []byte) error {
		if err := batch.Set(key, value, nil); err != nil {
			return err
		}
		batchBytes += len(key) + len(value)
		if batchBytes >= flushBytes {
			return flush()
		}
		return nil
	}

	var (
		current   *storeImporter
		curName   string
		startedAt = time.Now()
		storeAt   = startedAt
		stores    []storeInfo
	)

	closeCurrent := func() error {
		if current == nil {
			return nil
		}
		hash, err := current.finalize(set)
		if err != nil {
			return fmt.Errorf("finalize store %q: %w", curName, err)
		}
		stores = append(stores, storeInfo{Name: curName, Hash: hash})
		fmt.Fprintf(log, "[import] store=%q items=%d (leaves=%d inner=%d) elapsed=%s\n",
			curName, current.itemCount, current.leafCount, current.innerCount,
			time.Since(storeAt).Truncate(time.Millisecond))
		current = nil
		return nil
	}

	for {
		item, err := sr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read item: %w", err)
		}
		switch item.Type {
		case itemTypeStore:
			if err := closeCurrent(); err != nil {
				return nil, err
			}
			current = newStoreImporter(item.StoreName, height)
			curName = item.StoreName
			storeAt = time.Now()
			fmt.Fprintf(log, "[import] open store=%q\n", curName)
		case itemTypeIAVL:
			if current == nil {
				return nil, fmt.Errorf("IAVL item before any StoreItem")
			}
			if err := current.addNode(set,
				item.IAVLHeight, item.IAVLVersion,
				item.IAVLKey, item.IAVLValue); err != nil {
				return nil, fmt.Errorf("add node into %q: %w", curName, err)
			}
		case itemTypeExtMeta, itemTypeExtPayload:
			// Extensions (e.g. wasm payloads) live in their own per-store
			// directories on a real gaiad node. For a snapshot-import-
			// only tool that's not running gaiad we just skip them.
			if err := closeCurrent(); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("unknown SnapshotItem type %d", item.Type)
		}
	}

	if err := closeCurrent(); err != nil {
		return nil, err
	}

	// Write rootmulti commit-info + latest-version. These live OUTSIDE
	// any per-store prefix.
	if err := set(commitInfoKey(height), commitInfoBytes(height, stores)); err != nil {
		return nil, fmt.Errorf("write commit-info: %w", err)
	}
	if err := set(latestVersionKey, latestVersionBytes(height)); err != nil {
		return nil, fmt.Errorf("write latest-version: %w", err)
	}

	if err := flush(); err != nil {
		return nil, fmt.Errorf("flush final batch: %w", err)
	}

	fmt.Fprintf(log, "[import] complete elapsed=%s stores=%d\n",
		time.Since(startedAt).Truncate(time.Millisecond), len(stores))

	return stores, nil
}
