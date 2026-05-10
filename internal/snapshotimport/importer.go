// Stack-based snapshot importer. No `*iavl.Node` allocations: we keep a
// fixed-size frame per stack entry holding only the four fields a parent
// will need (children's nodeKey + hash + height + size), and emit each
// node's encoded bytes directly to a pebble batch as we walk the
// post-order LRN stream.
//
// Memory usage is O(tree depth) — ~30 frames for cosmoshub-4, ~1.7 KB.
// Pebble's batch + memtable dominate the resident set.

package snapshotimport

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/cockroachdb/pebble"
)

// emptyIAVLTreeHash is the cosmos-sdk iavl convention for the hash of
// a tree with no nodes: sha256(""). The CommitInfo.Hash() multistore
// merkle then double-hashes this in simpleMap.Set, so emitting nil or
// 32 zeros for an empty store would land on a different AppHash than
// the chain expects. This constant lets the importer keep its hash
// shape uniformly 32 bytes while staying consensus-correct for stores
// whose IAVL tree happens to be empty at snapshot time.
//
// Reference: github.com/cosmos/iavl@v1.2.2/node.go hashWithCount —
// "if node == nil { return sha256.New().Sum(nil) }".
var emptyIAVLTreeHash = sha256.Sum256(nil)

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
	storeName   string
	storePrefix []byte // "s/k:<name>/"

	// nonces[v] = next nonce to assign for a node at version v. The
	// IAVL convention reserves nonce=1 for the root, so for non-root
	// nodes we start counting at 2.
	//
	// nonceCacheVersion + nonceCacheCounter keep the most-recent
	// (version, counter) pair out of the map so that consecutive
	// nodes at the same version (the common case — bursts of leaves
	// at the snapshot height) skip both the map lookup and the map
	// write. Profile on bbn finality showed nextNonce at ~15% of the
	// main-goroutine CPU before this; the cache cuts it to a single
	// uint32 increment for ~99% of calls.
	nonces            map[int64]uint32
	nonceCacheVersion int64  // 0 = empty cache
	nonceCacheCounter uint32

	// stack of completed-but-not-yet-consumed subtrees. Inner nodes
	// pop the top two; leaves push themselves.
	stack []frame

	// snapshot height — used as the import "version" for fast-storage
	// metadata + as the version pointer the redirect (if any) is written
	// under. Each Add still carries the export node's own version.
	height int64

	// lastIavlBytes caches the most-recently-emitted IAVL-node bytes.
	// Because the snapshot stream is post-order LRN, the last node we
	// emit per store is the root. finalize re-emits these bytes under
	// (snapshotHeight, 1) so iavl's read path finds the root at the
	// canonical key — bypassing the need for a redirect entry. (iavl's
	// isReferenceRoot only follows redirects whose value starts with
	// the 's' nodeKey prefix; raw nodeKey bytes are NOT recognised.)
	//
	// Owned storage (we copy into it) so it survives the scratch-buf
	// reuse on the next addNode call.
	lastIavlBytes []byte

	// Scratch buffers reused across addNode calls. set/setFast both
	// copy the bytes into pebble's internal buffers, so we can hand
	// them slices into our scratch and reuse on the next call.
	keyScratch   []byte
	valueScratch []byte

	// running totals for progress logs
	itemCount  uint64
	leafCount  uint64
	innerCount uint64

	// Wave-parallel state, set by enableWaveParallel. When non-nil,
	// addNode dispatches to the wave-parallel path (deferred hashing
	// across a worker pool) and the parStack is used in place of stack.
	// See importer_par.go.
	par      *parState
	parStack []*parFrame
}

func newStoreImporter(storeName string, height int64) *storeImporter {
	return &storeImporter{
		storeName:   storeName,
		storePrefix: storePrefix(storeName),
		nonces:      make(map[int64]uint32),
		stack:       make([]frame, 0, 64),
		height:      height,
	}
}

// addNode processes one IAVL ExportNode (post-order LRN). It hashes,
// encodes, writes to the batch, and pushes a frame onto the stack.
//
// Two callbacks: `set` is the batch path (used for s/ node entries
// and m/ metadata, which arrive in non-byte-sorted order). `setFast`
// receives f/ fast-storage entries, which arrive in byte-sorted
// order (postorder leaves = sorted user-keys) and can be routed to
// an SSTable Writer for bulk-ingest. Pass the same callback for
// both to disable bulk-ingest.
func (s *storeImporter) addNode(set, setFast func(key, value []byte) error,
	height int8, version int64, key, value []byte) error {

	s.itemCount++

	// Wave-parallel path: defer hashing to the worker pool.
	if s.parallelEnabled() {
		if height == 0 {
			return s.addLeafPar(setFast, version, key, value)
		}
		return s.addInnerPar(version, height, key)
	}

	if height == 0 {
		// ─── leaf ────────────────────────────────────────────────
		nonce := s.nextNonce(version)

		h := hashLeaf(version, key, value)
		s.valueScratch = encodeLeafNodeInto(s.valueScratch[:0], key, value)
		s.keyScratch = nodeDBKeyInto(s.keyScratch[:0], s.storePrefix, version, nonce)
		if err := set(s.keyScratch, s.valueScratch); err != nil {
			return fmt.Errorf("set leaf node: %w", err)
		}
		// Preserve a copy for finalize's canonical-root re-emit; the
		// scratch buffer gets clobbered on the next addNode call.
		s.lastIavlBytes = append(s.lastIavlBytes[:0], s.valueScratch...)

		// fast-storage entry: 'f' || userKey → varint(version) || EncodeBytes(value)
		s.valueScratch = encodeFastNodeInto(s.valueScratch[:0], version, value)
		s.keyScratch = fastDBKeyInto(s.keyScratch[:0], s.storePrefix, key)
		if err := setFast(s.keyScratch, s.valueScratch); err != nil {
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

	s.valueScratch = encodeInnerNodeInto(s.valueScratch[:0],
		height, size, key, h,
		left.version, left.nonce,
		right.version, right.nonce)
	s.keyScratch = nodeDBKeyInto(s.keyScratch[:0], s.storePrefix, version, nonce)
	if err := set(s.keyScratch, s.valueScratch); err != nil {
		return fmt.Errorf("set inner node: %w", err)
	}
	s.lastIavlBytes = append(s.lastIavlBytes[:0], s.valueScratch...)

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
//
// Single-version cache: most consecutive calls share the same version
// (a leaf burst within one snapshot height), so we keep the active
// counter in nonceCacheCounter and only round-trip through the map on
// version transitions. Profiling showed this path at ~15% of main-
// goroutine CPU on bbn finality; cached path is a single increment.
func (s *storeImporter) nextNonce(version int64) uint32 {
	if version != s.nonceCacheVersion {
		// Flush the prior cache back to the map so a subsequent
		// transition (or a re-visit of an older version) reads the
		// up-to-date counter.
		if s.nonceCacheVersion != 0 {
			s.nonces[s.nonceCacheVersion] = s.nonceCacheCounter
		}
		s.nonceCacheVersion = version
		cur := s.nonces[version]
		if cur == 0 {
			cur = 1 // start at 1 so the first allocated nonce is 2
		}
		s.nonceCacheCounter = cur
	}
	s.nonceCacheCounter++
	return s.nonceCacheCounter
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
// Returns the merkle root hash of the store, suitable for inclusion
// in the global commit-info. For a store whose IAVL tree had zero
// nodes in the snapshot, the returned hash is sha256("") rather than
// nil — that's the value cosmos-sdk's iavl produces for an empty tree
// (hashWithCount returns sha256.New().Sum(nil) when node == nil), and
// what the network's AppHash assumes. See emptyIAVLTreeHash.
func (s *storeImporter) finalize(set func(key, value []byte) error) ([]byte, error) {
	// Wave-parallel path: callers must have called finishStreaming +
	// drained writeQ before reaching here. finalizePar lives in
	// importer_par.go.
	if s.parallelEnabled() {
		return s.finalizePar(set)
	}
	if len(s.stack) == 0 {
		// Empty store — write an empty root marker (gaiad reads this
		// via nodeDBKey(snapshotHeight, 1) and tolerates an empty value).
		s.keyScratch = nodeDBKeyInto(s.keyScratch[:0], s.storePrefix, s.height, 1)
		if err := set(s.keyScratch, nil); err != nil {
			return nil, fmt.Errorf("set empty root marker: %w", err)
		}
		// Write the fast-storage marker even for empty stores so the
		// runtime daemon doesn't fall into the "Upgrading IAVL storage"
		// pass on first load. Without this, every chain with at least
		// one empty store at snapshot time pays a multi-minute startup
		// hit on first boot.
		if err := s.writeStorageVersionMarker(set); err != nil {
			return nil, err
		}
		return emptyIAVLTreeHash[:], nil
	}
	if len(s.stack) != 1 {
		return nil, fmt.Errorf("invalid stream: stack has %d roots, expected 1",
			len(s.stack))
	}

	root := s.stack[0]

	// IAVL's read path looks for the root at the canonical nodeKey
	// (root.version, 1). We can't simply use the addNode-time write
	// (which lives at (root.version, root.nonce ≥ 2)) because iavl's
	// LoadVersion calls GetRoot(version) which reads at nonce=1
	// specifically. So we re-emit the root's encoded bytes there.
	//
	// We DON'T write a 12-byte raw-nodeKey "redirect" — iavl's
	// isReferenceRoot (nodedb.go:1172) only recognises a redirect when
	// the value's first byte is the 's' nodeKey prefix. A raw 12-byte
	// nodeKey starting with the version's high byte fails that check;
	// iavl then tries to MakeNode from the 12-byte garbage and silently
	// gets wrong-but-plausible state, which propagates as a divergent
	// post-execute apphash on the next block.
	//
	// The encoded inner-node bytes don't embed the node's own nodeKey
	// (only its children's leftNodeKey/rightNodeKey are encoded), so
	// the same byte sequence is valid at any storage location.
	s.keyScratch = nodeDBKeyInto(s.keyScratch[:0], s.storePrefix, root.version, 1)
	if err := set(s.keyScratch, s.lastIavlBytes); err != nil {
		return nil, fmt.Errorf("set canonical root at (rootVersion, 1): %w", err)
	}

	// If the snapshot's "current version" (s.height) differs from the
	// root's actual version (root.version), iavl needs a redirect from
	// `GetRoot(s.height)` to `(root.version, 1)`. This happens when a
	// store hasn't been written-to since some earlier height — e.g. on
	// cosmoshub the `08-wasm` store's root may date to height ~28.6M
	// while we're snapshotting at 30.9M.
	//
	// Wire format (matches iavl's SaveRoot in nodedb.go:1042):
	//   key   = nodeKeyFormat.Key(GetRootKey(s.height))   = 's'||height||0x00000001
	//   value = nodeKeyFormat.Key(target.GetKey())         = 's'||rootVersion||0x00000001
	// (13 bytes each. The 's' prefix on the VALUE is what makes
	// isReferenceRoot recognise this entry as a redirect.)
	if root.version != s.height {
		var redirectVal [13]byte
		redirectVal[0] = 's'
		binary.BigEndian.PutUint64(redirectVal[1:], uint64(root.version))
		binary.BigEndian.PutUint32(redirectVal[9:], 1)
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

// writeStorageVersionMarker writes the per-store metadataDB
// `storage_version` key that signals to the chain runtime that this
// store's IAVL is already on the fast-storage layout. Without it,
// gaiad-style daemons fall into the "Upgrading IAVL storage for
// faster queries + execution on live state" startup pass — fast for
// empty stores but still adds latency, and on populated stores can
// take many minutes.
//
// Same value (`<fastStorageVersionValue>-<height>`) on both the
// empty-store and populated paths; factored out so they stay in sync.
func (s *storeImporter) writeStorageVersionMarker(set func(key, value []byte) error) error {
	metaVal := []byte(fmt.Sprintf("%s%s%d",
		fastStorageVersionValue, fastStorageVersionDelimiter, s.height))
	s.keyScratch = metadataDBKeyInto(s.keyScratch[:0], s.storePrefix, "storage_version")
	if err := set(s.keyScratch, metaVal); err != nil {
		return fmt.Errorf("set storage_version marker: %w", err)
	}
	return nil
}

// ─── driver ──────────────────────────────────────────────────────────────

// runImport drives the full import: reads SnapshotItems off `r`,
// dispatches to a fresh storeImporter per store, and writes every
// resulting (key, value) into `db`. At end of stream, writes the
// rootmulti commit-info + latest-version pointer.
//
// extDir, if non-empty, receives extracted SnapshotExtensionPayload
// items at <extDir>/<extName>/payload-<i>-format<F>.bin, mirroring the
// layout cosmos-bootstrap-gaia consumes. Pass "" to skip extensions.
//
// stats accumulates Items / Extensions / ExtensionPayloads inline so
// callers can report progress without re-walking the stream.
//
// Returns the per-store root hashes in stream order (typically
// alphabetical, by cosmos-sdk's exporter convention).
func runImport(r io.Reader, db *pebble.DB, height int64, extDir, ingestTmpDir string, stats *Stats, log *slog.Logger) ([]StoreInfo, error) {
	// Decouple zlib decompression from the IAVL+pebble work via a
	// prefetch goroutine. 8 × 1 MiB buffers ≈ 8 MiB of read-ahead;
	// enough to absorb the variance in chunk read latency without
	// touching the 256 MiB memtable budget. Per-buffer allocation
	// adds ~36 MB/s of GC pressure (negligible — measured 0.2-0.5%
	// GC CPU in baseline).
	pf := newPrefetchReader(context.Background(), r, 8, 1<<20)
	defer pf.Close()
	sr := newSnapReader(pf)

	// Sub-phase accumulators reported on Stats. Decode = sr.Next()
	// (proto decode + chunk read + zlib decompress). Pebble = anything
	// inside set / flush / batch.Commit. IAVL is computed as the
	// residual: streamElapsed - decode - pebble (covers IAVL hashing,
	// addNode, finalize, plus a sliver of bookkeeping).
	var decodeNanos, pebbleNanos int64

	const flushBytes = 64 << 20 // 64 MiB per batch
	batch := db.NewBatch()
	batchBytes := 0
	flush := func() error {
		if batchBytes == 0 {
			return nil
		}
		// NoSync: skip per-batch fsync. The import is recoverable —
		// a crash mid-stream means re-running snapshot import from a
		// fresh DB, not preserving partial state. Final flush at end
		// of stream uses Sync to make the completed import durable.
		err := batch.Commit(pebble.NoSync)
		batch.Close()
		batch = db.NewBatch()
		batchBytes = 0
		return err
	}
	set := func(key, value []byte) error {
		t0 := time.Now()
		defer func() { pebbleNanos += time.Since(t0).Nanoseconds() }()
		if err := batch.Set(key, value, nil); err != nil {
			return err
		}
		batchBytes += len(key) + len(value)
		if batchBytes >= flushBytes {
			return flush()
		}
		return nil
	}

	// Fast-path SSTable ingester. One *sstable.Writer per store
	// accumulates byte-sorted f/ entries; on closeCurrent the file
	// is closed and atomically linked into the LSM via db.Ingest.
	// Bypasses memtable + WAL + batch sort + L0→L6 compaction for
	// fast-storage data, which is ~half of the import volume.
	ing := newFastIngester(db, ingestTmpDir, log)
	defer ing.cleanup()

	var (
		current   *storeImporter
		curName   string
		startedAt = time.Now()
		storeAt   = startedAt
		stores    []StoreInfo
		extWriter = newExtensionWriter(extDir)
	)

	closeCurrent := func() error {
		if current == nil {
			return nil
		}
		hash, err := current.finalize(set)
		if err != nil {
			return fmt.Errorf("finalize store %q: %w", curName, err)
		}
		// Close + ingest the per-store fast-path SSTable. ingestStore
		// is a no-op if no fast entries were emitted (empty stores).
		// Done before flushing the batch so the f/ entries are visible
		// to any subsequent reads, though no readers exist mid-import.
		if err := ing.ingestStore(current.storeName); err != nil {
			return fmt.Errorf("ingest fast SSTable for %q: %w", curName, err)
		}
		stores = append(stores, StoreInfo{Name: curName, Hash: hash, LeafCount: current.leafCount})
		stats.Items += current.itemCount
		log.Info("store complete",
			"store", curName,
			"items", current.itemCount,
			"leaves", current.leafCount,
			"inner", current.innerCount,
			"elapsed", time.Since(storeAt).Truncate(time.Millisecond))
		current = nil
		return nil
	}

	for {
		decT0 := time.Now()
		item, err := sr.Next()
		decodeNanos += time.Since(decT0).Nanoseconds()
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
			if err := ing.openStore(item.StoreName); err != nil {
				return nil, fmt.Errorf("open fast SSTable for %q: %w", curName, err)
			}
			log.Info("open store", "store", curName)
		case itemTypeIAVL:
			if current == nil {
				return nil, fmt.Errorf("IAVL item before any StoreItem")
			}
			if err := current.addNode(set, ing.set,
				item.IAVLHeight, item.IAVLVersion,
				item.IAVLKey, item.IAVLValue); err != nil {
				return nil, fmt.Errorf("add node into %q: %w", curName, err)
			}
		case itemTypeExtMeta:
			// Transitioning out of an IAVL store and into the extensions
			// section of the stream. Close any in-flight store first.
			if err := closeCurrent(); err != nil {
				return nil, err
			}
			if err := extWriter.openMeta(item.ExtName, item.ExtFormat, stats, log); err != nil {
				return nil, err
			}
		case itemTypeExtPayload:
			if err := extWriter.writePayload(item.ExtPayload, stats); err != nil {
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

	// Final flush uses Sync so the completed import survives a crash —
	// per-batch fsyncs were skipped via NoSync above for throughput.
	finalFlushStart := time.Now()
	if batchBytes > 0 {
		if err := batch.Commit(pebble.Sync); err != nil {
			return nil, fmt.Errorf("flush final batch: %w", err)
		}
		batch.Close()
		batch = db.NewBatch()
		batchBytes = 0
	}
	pebbleNanos += time.Since(finalFlushStart).Nanoseconds()

	streamDur := time.Since(startedAt)
	stats.StreamDecodeElapsed = time.Duration(decodeNanos)
	stats.StreamPebbleElapsed = time.Duration(pebbleNanos)
	residual := streamDur - stats.StreamDecodeElapsed - stats.StreamPebbleElapsed
	if residual < 0 {
		residual = 0
	}
	stats.StreamIAVLElapsed = residual

	log.Info("stream complete",
		"elapsed", streamDur.Truncate(time.Millisecond),
		"stores", len(stores),
		"decode_elapsed", stats.StreamDecodeElapsed.Truncate(time.Millisecond),
		"iavl_elapsed", stats.StreamIAVLElapsed.Truncate(time.Millisecond),
		"pebble_elapsed", stats.StreamPebbleElapsed.Truncate(time.Millisecond))

	return stores, nil
}
