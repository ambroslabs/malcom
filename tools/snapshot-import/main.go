// snapshot-import: standalone tool that converts a downloaded cosmos
// snapshot directory into a gaiad-compatible application.db pebble dir.
//
// No cosmos / cometbft / iavl dependencies. Reads a directory of
// chunk_NNNNN.bin files (zlib-compressed slices of one continuous
// SnapshotItem stream) and writes the resulting IAVL nodes, fast-storage
// entries, per-store metadata, and rootmulti commit-info into a pebble
// database. The resulting directory can be opened by a stock gaiad as
// `data/application.db` after a `cometbft BootstrapState` to produce
// `state.db` + `blockstore.db`.
//
// Usage:
//
//	snapshot-import \
//	    -snapshot=/path/to/30960000_3 \
//	    -out=/path/to/application.db \
//	    -height=30960000

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"time"

	"github.com/cockroachdb/pebble"
)

var (
	snapshotDir = flag.String("snapshot", "", "directory containing chunk_NNNNN.bin files")
	outDir      = flag.String("out", "", "output pebble directory (typically data/application.db)")
	height      = flag.Int64("height", 0, "snapshot height (used as the import version)")
	memtable    = flag.Int("memtable-mb", 256, "pebble memtable size in MiB (per memtable; 2 slots are kept)")
	cache       = flag.Int("cache-mb", 16, "pebble block cache in MiB (small for write-only workload)")
	bulkLoad    = flag.Bool("bulk-load", true, "defer L0→L1 compactions until end of import (huge throughput win for one-shot imports)")
	maxCompact  = flag.Int("max-concurrent-compactions", 4, "max concurrent compactions while running, and during the final compact pass")

	// AppHash verification. Either -expected-apphash (manual) or -rpc
	// (auto-fetch from a cometbft RPC). With neither, we just compute
	// and print the local hash so the caller can compare manually.
	expectedAppHash = flag.String("expected-apphash", "", "consensus AppHash hex; if set, verify the import reproduces it")
	rpcURL          = flag.String("rpc", "", "cometbft RPC base URL to fetch the expected AppHash (e.g. https://cosmos-rpc.polkachu.com)")

	// verify-only mode: skip the entire import pipeline and just read
	// the commit-info that a prior run wrote to the pebble DB at
	// s/<height>. Use this to retry RPC verification without re-running
	// the 10-minute import (e.g. when a flaky RPC failed at end of a
	// successful import).
	verifyOnly = flag.Bool("verify-only", false, "skip import; read commit-info from -out and verify AppHash against -rpc / -expected-apphash")
)

func main() {
	flag.Parse()

	if *verifyOnly {
		if *outDir == "" || *height == 0 {
			fmt.Fprintln(os.Stderr, "verify-only mode requires -out and -height")
			os.Exit(2)
		}
		if err := runVerifyOnly(*outDir, *height, *expectedAppHash, *rpcURL, os.Stdout); err != nil {
			fmt.Printf("\n  ✗ %v\n", err)
			os.Exit(1)
		}
		return
	}

	if *snapshotDir == "" || *outDir == "" || *height == 0 {
		flag.Usage()
		os.Exit(2)
	}

	t0 := time.Now()

	cr, err := openChunkDir(*snapshotDir)
	if err != nil {
		log.Fatalf("open snapshot dir: %v", err)
	}
	defer cr.Close()

	// Bulk-load tuning. The import is a write-only one-shot: every key
	// is unique, every Set lands in the WAL+memtable, every memtable
	// flushes to L0. Pebble's defaults aggressively compact L0→L1 to
	// keep read amplification low for general-purpose workloads, but
	// during a bulk import we never read, so compaction work mid-stream
	// is wasted I/O that competes with the writer for disk bandwidth
	// AND triggers `L0StopWrites` throttling that blocks Add().
	//
	// With `DisableAutomaticCompactions=true` and the L0 thresholds
	// pushed to effectively-never, every chunk lands in a fresh L0 SST
	// without compaction overhead. After the import the CLI runs a
	// single full-keyspace compact, which is dramatically faster than
	// the trickle of mid-import compactions because it sees the whole
	// dataset at once and can choose merge boundaries optimally.
	opts := &pebble.Options{
		MemTableSize:                uint64(*memtable) << 20,
		MemTableStopWritesThreshold: 4, // 4 slots so we don't stall on flushes
		Cache:                       pebble.NewCache(int64(*cache) << 20),
		MaxOpenFiles:                4096,
		MaxConcurrentCompactions:    func() int { return *maxCompact },
	}
	if *bulkLoad {
		opts.DisableAutomaticCompactions = true
		opts.L0CompactionThreshold = 1024 // effectively never
		opts.L0StopWritesThreshold = 4096
	}
	db, err := pebble.Open(*outDir, opts)
	if err != nil {
		log.Fatalf("open pebble db at %s: %v", *outDir, err)
	}
	// Note: no `defer db.Close()` — we explicitly Close before the
	// cleanup pass so pebble's manifest replay sees the final state.

	stores, err := runImport(cr, db, *height, os.Stdout)
	if err != nil {
		log.Fatalf("import: %v", err)
	}

	// In bulk-load mode L0 is huge (every memtable became its own SST
	// with no L0→L1 compaction along the way). Run one full-keyspace
	// compact now: pebble sees the whole keyspace at once and produces
	// a tight LSM in one pass.
	t1 := time.Now()
	fmt.Printf("[import] starting final compaction (this can take a while on cosmoshub)...\n")
	if err := db.Flush(); err != nil {
		log.Printf("warning: pre-compact flush failed: %v", err)
	}
	if err := db.Compact([]byte{0x00}, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, true); err != nil {
		log.Printf("warning: final compact failed: %v", err)
	}
	fmt.Printf("[import] final compaction elapsed=%s\n",
		time.Since(t1).Truncate(time.Millisecond))

	// Close the import-tuned DB. The Compact above queued obsolete L0
	// SSTs for deletion via pebble's cleanup manager, but those
	// deletions don't always drain before Close — leaving orphan SSTs
	// on disk that the next Open will reclaim via manifest recovery.
	// On a fresh cosmoshub-4 import this is the difference between
	// ~25 GB and ~14 GB on disk, so the cleanup pass is mandatory.
	if err := db.Close(); err != nil {
		log.Printf("warning: close after compact failed: %v", err)
	}

	// Cleanup pass: reopen with pebble's default options. The first
	// Open triggers manifest replay which identifies + unlinks any
	// SSTs no longer referenced. Run a quick Flush + Compact to nudge
	// pebble through any remaining housekeeping, then close cleanly.
	t2 := time.Now()
	fmt.Printf("[import] starting cleanup pass (reopen + reclaim orphan SSTs)...\n")
	cleanup, err := pebble.Open(*outDir, &pebble.Options{
		MaxConcurrentCompactions: func() int { return *maxCompact },
	})
	if err != nil {
		log.Fatalf("cleanup reopen: %v", err)
	}
	if err := cleanup.Flush(); err != nil {
		log.Printf("warning: cleanup flush failed: %v", err)
	}
	if err := cleanup.Compact([]byte{0x00}, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, true); err != nil {
		log.Printf("warning: cleanup compact failed: %v", err)
	}
	if err := cleanup.Close(); err != nil {
		log.Printf("warning: cleanup close failed: %v", err)
	}
	fmt.Printf("[import] cleanup pass elapsed=%s\n",
		time.Since(t2).Truncate(time.Millisecond))

	// AppHash verification. Always compute + print the local hash; if a
	// reference is provided (either an explicit hex or an RPC URL),
	// compare. Distinguish two failure modes:
	//   - Genuine MISMATCH (local hash doesn't equal reference) → exit 1.
	//     The import is not consensus-correct; the user must investigate.
	//   - RPC fetch failure (network, server, etc.) → warn but exit 0.
	//     The import itself is fine; the user can retry verification
	//     later via `-verify-only -rpc=<other-rpc>` without re-running.
	if _, err := verifyAppHash(stores, *height, *expectedAppHash, *rpcURL, os.Stdout); err != nil {
		// MismatchError → fatal. Anything else (network, parse) → warn.
		if isMismatch(err) {
			fmt.Printf("\n  ✗ MISMATCH: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("\n  ⚠ AppHash verification deferred: %v\n", err)
		fmt.Printf("    Local hash printed above. To retry without re-running the import:\n")
		fmt.Printf("    %s -verify-only -out=%s -height=%d -rpc=<other-rpc>\n",
			os.Args[0], *outDir, *height)
	}

	fmt.Printf("[import] total elapsed=%s, stores=%d, output=%s\n",
		time.Since(t0).Truncate(time.Millisecond), len(stores), *outDir)
	fmt.Printf("[import] runtime stats: NumGoroutine=%d\n",
		runtime.NumGoroutine())
}
