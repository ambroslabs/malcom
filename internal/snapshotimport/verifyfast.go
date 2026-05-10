// Verify-fast: count the f/ (fast-storage) entries in pebble for
// each store and compare against the leaf count the import recorded.
//
// Trust model: the s/ tree itself is already trusted because the
// import's AppHash matches consensus. If the f/ entry count for a
// store equals the leaf count we wrote, no entry was silently
// dropped. This catches the failure mode operators care about
// (gaiad's fast-storage path serving partial state) without the
// hours-long cost of a full point-lookup-per-leaf walk.
//
// Cost: one sequential pebble iterator pass per store over the
// `'f'` range. Tens of seconds for finality-class stores; subsecond
// for the rest.

package snapshotimport

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log/slog"
	"time"

	"github.com/cockroachdb/pebble"
)

// readVarintAt reads a zigzag-encoded signed varint at b[pos:].
// Returns the decoded value and bytes consumed (0 on error).
func readVarintAt(b []byte, pos int) (int64, int) {
	if pos >= len(b) {
		return 0, 0
	}
	ux, n := binary.Uvarint(b[pos:])
	if n <= 0 {
		return 0, 0
	}
	x := int64(ux >> 1)
	if ux&1 != 0 {
		x = ^x
	}
	return x, n
}

// VerifyFast iterates each store's f/ namespace and checks the
// entry count matches the leaf count. When the caller already knows
// the leaf count (e.g., the import path passes its in-memory
// LeafCount), VerifyFast uses that. Otherwise (LeafCount==0) it
// computes the leaf count on-the-fly by iterating the s/ namespace
// and decoding each node's height varint — a sequential pass that
// makes the function usable as a post-hoc check on any imported
// appdb (see the `verify-fast` subcommand).
//
// Returns the first mismatch encountered (or nil if every store
// passes).
func VerifyFast(db *pebble.DB, stores []StoreInfo, log *slog.Logger) error {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	t0 := time.Now()
	var totalCounted, totalExpected uint64
	for _, si := range stores {
		prefix := storePrefix(si.Name)
		got, err := countFastEntries(db, prefix)
		if err != nil {
			return fmt.Errorf("store %q: count f/: %w", si.Name, err)
		}
		expected := si.LeafCount
		source := "leaf-count"
		if expected == 0 {
			expected, err = countSLeaves(db, prefix)
			if err != nil {
				return fmt.Errorf("store %q: count s/ leaves: %w", si.Name, err)
			}
			source = "s/-leaves"
		}
		if got != expected {
			return fmt.Errorf("store %q: f/ count %d != %s %d (silent drop or duplicate)",
				si.Name, got, source, expected)
		}
		totalCounted += got
		totalExpected += expected
		log.Info("verify-fast store", "store", si.Name, "f_entries", got, "leaves", expected, "source", source)
	}
	log.Info("verify-fast complete",
		"stores", len(stores),
		"f_entries", totalCounted,
		"leaves", totalExpected,
		"elapsed", time.Since(t0).Truncate(time.Millisecond))
	return nil
}

// countSLeaves iterates the s/ namespace for the given store and
// returns the number of leaf entries (= entries whose decoded
// height==0). Each iteration decodes the leading height varint of
// the value and skips otherwise — no full node decode needed.
//
// Skips the canonical-root re-emit at nonce==1: finalize re-writes
// the root's encoded bytes under nodeKey (snapshotHeight, 1) so
// iavl.GetRoot finds it without a redirect; for a single-leaf
// store that re-emit duplicates the leaf bytes (height==0),
// double-counting them. Real leaves and inners always live at
// nonce >= 2 (nextNonce starts allocating at 2).
func countSLeaves(db *pebble.DB, prefix []byte) (uint64, error) {
	lo := append(append([]byte{}, prefix...), 's')
	hi := append(append([]byte{}, prefix...), 't')
	it, err := db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return 0, err
	}
	defer it.Close()
	var n uint64
	for it.First(); it.Valid(); it.Next() {
		k := it.Key()
		// nodeKey shape: storePrefix || 's' || version(BE 8) || nonce(BE 4).
		// Skip the canonical-root re-emit at nonce==1.
		if len(k) >= 4 {
			nonce := binary.BigEndian.Uint32(k[len(k)-4:])
			if nonce == 1 {
				continue
			}
		}
		v := it.Value()
		// Empty-store sentinels have a 0-length value.
		if len(v) == 0 {
			continue
		}
		// 13-byte redirects start with 's' on the value side.
		if len(v) == 13 && v[0] == 's' {
			continue
		}
		// First varint of the value is the IAVL height. height==0
		// means leaf.
		h, kk := readVarintAt(v, 0)
		if kk == 0 {
			continue
		}
		if h == 0 {
			n++
		}
	}
	return n, it.Error()
}

// countFastEntries returns the number of `'f'`-namespace entries
// belonging to the given store prefix. We use pebble's iterator with
// LowerBound/UpperBound for a sequential range scan; this hits block
// cache + bloom filters and is much faster than per-key Get.
func countFastEntries(db *pebble.DB, prefix []byte) (uint64, error) {
	lo := append(append([]byte{}, prefix...), 'f')
	hi := append(append([]byte{}, prefix...), 'g')
	it, err := db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return 0, err
	}
	defer it.Close()
	var n uint64
	for it.First(); it.Valid(); it.Next() {
		// Sanity: the key must start with prefix||'f'. UpperBound
		// already prevents bleeding into 'g'-and-beyond, but the
		// guard is cheap and protects against a future change to
		// the key layout.
		k := it.Key()
		if !bytes.HasPrefix(k, lo) {
			break
		}
		n++
	}
	return n, it.Error()
}
