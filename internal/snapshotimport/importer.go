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
	"encoding/binary"
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

	// lastIavlBytes caches the most-recently-emitted IAVL-node bytes.
	// Because the snapshot stream is post-order LRN, the last node we
	// emit per store is the root. finalize re-emits these bytes under
	// (snapshotHeight, 1) so iavl's read path finds the root at the
	// canonical key — bypassing the need for a redirect entry. (iavl's
	// isReferenceRoot only follows redirects whose value starts with
	// the 's' nodeKey prefix; raw nodeKey bytes are NOT recognised.)
	lastIavlBytes []byte

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
		s.lastIavlBytes = nodeBytes

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
	s.lastIavlBytes = nodeBytes

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
	rootCanonicalKey := nodeDBKey(s.storePrefix, root.version, 1)
	if err := set(rootCanonicalKey, s.lastIavlBytes); err != nil {
		return [32]byte{}, fmt.Errorf("set canonical root at (rootVersion, 1): %w", err)
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
		if err := set(nodeDBKey(s.storePrefix, s.height, 1), redirectVal[:]); err != nil {
			return [32]byte{}, fmt.Errorf("set root redirect at (snapshotHeight, 1): %w", err)
		}
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
// extDir, if non-empty, receives extracted SnapshotExtensionPayload
// items at <extDir>/<extName>/payload-<i>-format<F>.bin, mirroring the
// layout cosmos-bootstrap-gaia consumes. Pass "" to skip extensions.
//
// stats accumulates Items / Extensions / ExtensionPayloads inline so
// callers can report progress without re-walking the stream.
//
// Returns the per-store root hashes in stream order (typically
// alphabetical, by cosmos-sdk's exporter convention).
func runImport(r io.Reader, db *pebble.DB, height int64, extDir string, stats *Stats, log io.Writer) ([]StoreInfo, error) {
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
		stores = append(stores, StoreInfo{Name: curName, Hash: hash})
		stats.Items += current.itemCount
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

	if err := flush(); err != nil {
		return nil, fmt.Errorf("flush final batch: %w", err)
	}

	fmt.Fprintf(log, "[import] complete elapsed=%s stores=%d\n",
		time.Since(startedAt).Truncate(time.Millisecond), len(stores))

	return stores, nil
}
