// Public entry point for the snapshot importer.
//
// Import opens a fresh pebble database at <opts.OutDir>/application.db,
// streams the snapshot at opts.SnapshotDir through the stack-based
// importer (importer.go + iavlenc.go + commitinfo.go), and optionally
// extracts SnapshotExtensionPayload items to <opts.OutDir>/extensions/.
//
// The output layout matches what gaiad expects:
//
//	<OutDir>/application.db/   pebble dir gaiad reads (db_backend = "pebbledb")
//	<OutDir>/extensions/<n>/   wasm bytecode (cosmwasm + 08-light-client)
//
// Use cosmos-p2p-toolkit's `malcom bootstrap` step downstream to
// assemble these into a runnable gaiad home directory.
package snapshotimport

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/cockroachdb/pebble"
)

// Options controls Import. Zero-value defaults are tuned for a 64 GiB
// host; override MemtableMB/CacheMB/MaxConcurrentCompactions on smaller
// hosts.
type Options struct {
	// SnapshotDir is the directory containing chunk_NNNNN.bin files (and
	// optionally meta.json + metadata.bin). Required.
	SnapshotDir string

	// OutDir is the parent directory where Import writes
	// `application.db/` and (unless NoExtensions) `extensions/`. Required.
	OutDir string

	// Height is the snapshot height — used as the IAVL import version,
	// the storage_version metadata, and the rootmulti latest-version
	// pointer. Required (Import does not infer from meta.json).
	Height int64

	// NoExtensions skips writing SnapshotExtensionPayload items. The
	// importer still drains the items from the stream so the tail of
	// the snapshot decodes correctly. Default false.
	NoExtensions bool

	// MemtableMB is the size of each pebble memtable in MiB.
	// Default 256.
	MemtableMB int

	// CacheMB is the pebble block cache size in MiB. Default 16
	// (small — the import is write-only).
	CacheMB int

	// MaxConcurrentCompactions caps parallel compactions during the
	// final compact + cleanup pass. Default 4.
	MaxConcurrentCompactions int

	// BulkLoad disables automatic L0→L1 compactions during the import
	// stream and runs a single full-keyspace compact at the end.
	// Default true. Disable on memory-constrained hosts that benefit
	// from incremental compaction.
	BulkLoad bool

	// Log receives progress messages. Defaults to os.Stdout.
	Log io.Writer
}

// Stats summarises a completed import. Returned by Import.
type Stats struct {
	Stores              []StoreInfo
	Items               uint64
	Extensions          int
	ExtensionPayloads   int
	Elapsed             time.Duration
	StreamElapsed       time.Duration
	FinalCompactElapsed time.Duration
	CleanupElapsed      time.Duration
}

// StoreInfo is declared in commitinfo.go (Name, Hash[32]byte).
// It's the same struct that gets serialized into the rootmulti CommitInfo.

// Import runs the full import pipeline: open pebble, stream the
// snapshot, finalize per-store roots + commit-info, run a final
// full-keyspace compact, then a cleanup pass to reclaim slack.
func Import(opts Options) (*Stats, error) {
	if opts.SnapshotDir == "" {
		return nil, fmt.Errorf("Options.SnapshotDir is required")
	}
	if opts.OutDir == "" {
		return nil, fmt.Errorf("Options.OutDir is required")
	}
	if opts.Height == 0 {
		return nil, fmt.Errorf("Options.Height is required")
	}
	logw := opts.Log
	if logw == nil {
		logw = os.Stdout
	}
	memMB := opts.MemtableMB
	if memMB <= 0 {
		memMB = 256
	}
	cacheMB := opts.CacheMB
	if cacheMB <= 0 {
		cacheMB = 16
	}
	maxCompact := opts.MaxConcurrentCompactions
	if maxCompact <= 0 {
		maxCompact = 4
	}
	bulkLoad := opts.BulkLoad
	if !opts.BulkLoad {
		// Zero-value default differs from struct default — caller has
		// to opt out explicitly via a sentinel field. Today we just
		// always default to true; a "disable bulk-load" toggle can be
		// added later if needed.
		bulkLoad = true
	}

	t0 := time.Now()

	// ─── output layout ───────────────────────────────────────────────
	if err := os.MkdirAll(opts.OutDir, 0o755); err != nil {
		return nil, fmt.Errorf("create out dir %s: %w", opts.OutDir, err)
	}
	appdbDir := filepath.Join(opts.OutDir, "application.db")
	extDir := ""
	if !opts.NoExtensions {
		extDir = filepath.Join(opts.OutDir, "extensions")
	}

	// ─── snapshot stream ─────────────────────────────────────────────
	cr, err := openChunkDir(opts.SnapshotDir)
	if err != nil {
		return nil, fmt.Errorf("open snapshot dir: %w", err)
	}
	defer cr.Close()

	// ─── pebble open ─────────────────────────────────────────────────
	popts := &pebble.Options{
		MemTableSize:                uint64(memMB) << 20,
		MemTableStopWritesThreshold: 4,
		Cache:                       pebble.NewCache(int64(cacheMB) << 20),
		MaxOpenFiles:                4096,
		MaxConcurrentCompactions:    func() int { return maxCompact },
	}
	if bulkLoad {
		popts.DisableAutomaticCompactions = true
		popts.L0CompactionThreshold = 1024
		popts.L0StopWritesThreshold = 4096
	}
	db, err := pebble.Open(appdbDir, popts)
	if err != nil {
		return nil, fmt.Errorf("open pebble at %s: %w", appdbDir, err)
	}
	// No defer Close — we explicitly Close before the cleanup reopen.

	// ─── run the streaming import ────────────────────────────────────
	streamStart := time.Now()
	stats := &Stats{}
	stores, err := runImport(cr, db, opts.Height, extDir, stats, logw)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	stats.Stores = stores
	stats.StreamElapsed = time.Since(streamStart).Truncate(time.Millisecond)

	// ─── final compaction ────────────────────────────────────────────
	t1 := time.Now()
	fmt.Fprintf(logw, "[import] starting final compaction (this can take a while on cosmoshub)...\n")
	if err := db.Flush(); err != nil {
		fmt.Fprintf(logw, "warning: pre-compact flush failed: %v\n", err)
	}
	if err := db.Compact([]byte{0x00}, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, true); err != nil {
		fmt.Fprintf(logw, "warning: final compact failed: %v\n", err)
	}
	stats.FinalCompactElapsed = time.Since(t1).Truncate(time.Millisecond)
	fmt.Fprintf(logw, "[import] final compaction elapsed=%s\n", stats.FinalCompactElapsed)

	if err := db.Close(); err != nil {
		fmt.Fprintf(logw, "warning: close after compact failed: %v\n", err)
	}

	// ─── cleanup pass: reopen + Compact to reclaim orphan SSTs ───────
	// On a fresh cosmoshub-4 import the in-process Compact above queues
	// obsolete L0 files for deletion via pebble's cleanup manager, but
	// those deletions don't always drain before Close. The next Open
	// reclaims them via manifest replay — typically saves ~10 GiB.
	t2 := time.Now()
	fmt.Fprintf(logw, "[import] starting cleanup pass (reopen + reclaim orphan SSTs)...\n")
	cleanup, err := pebble.Open(appdbDir, &pebble.Options{
		MaxConcurrentCompactions: func() int { return maxCompact },
	})
	if err != nil {
		return nil, fmt.Errorf("cleanup reopen: %w", err)
	}
	if err := cleanup.Flush(); err != nil {
		fmt.Fprintf(logw, "warning: cleanup flush failed: %v\n", err)
	}
	if err := cleanup.Compact([]byte{0x00}, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, true); err != nil {
		fmt.Fprintf(logw, "warning: cleanup compact failed: %v\n", err)
	}
	if err := cleanup.Close(); err != nil {
		fmt.Fprintf(logw, "warning: cleanup close failed: %v\n", err)
	}
	stats.CleanupElapsed = time.Since(t2).Truncate(time.Millisecond)
	fmt.Fprintf(logw, "[import] cleanup pass elapsed=%s\n", stats.CleanupElapsed)

	stats.Elapsed = time.Since(t0).Truncate(time.Millisecond)
	fmt.Fprintf(logw, "[import] total elapsed=%s, stores=%d, output=%s\n",
		stats.Elapsed, len(stores), appdbDir)
	fmt.Fprintf(logw, "[import] runtime stats: NumGoroutine=%d\n", runtime.NumGoroutine())

	return stats, nil
}
