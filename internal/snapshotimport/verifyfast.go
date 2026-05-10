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
	"fmt"
	"log/slog"
	"time"

	"github.com/cockroachdb/pebble"
)

// VerifyFast iterates each store's f/ namespace and checks the
// entry count matches the LeafCount the import recorded. Returns
// the first mismatch encountered (or nil if every store passes).
func VerifyFast(db *pebble.DB, stores []StoreInfo, log *slog.Logger) error {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	t0 := time.Now()
	var totalCounted, totalExpected uint64
	for _, si := range stores {
		got, err := countFastEntries(db, storePrefix(si.Name))
		if err != nil {
			return fmt.Errorf("store %q: count f/: %w", si.Name, err)
		}
		if got != si.LeafCount {
			return fmt.Errorf("store %q: f/ count %d != leaf count %d (silent drop or duplicate)",
				si.Name, got, si.LeafCount)
		}
		totalCounted += got
		totalExpected += si.LeafCount
		log.Info("verify-fast store", "store", si.Name, "f_entries", got, "leaves", si.LeafCount)
	}
	log.Info("verify-fast complete",
		"stores", len(stores),
		"f_entries", totalCounted,
		"leaves", totalExpected,
		"elapsed", time.Since(t0).Truncate(time.Millisecond))
	return nil
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
