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
	"log/slog"
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

	// Log receives structured progress events. nil → discard.
	Log *slog.Logger
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
	// snapfetch writes .complete only after every chunk, metadata.bin,
	// and meta.json have been fsynced. A missing marker means the fetch
	// was interrupted — importing the partial dir can succeed at zlib
	// decompression and still produce a half-built application.db.
	if _, err := os.Stat(filepath.Join(opts.SnapshotDir, ".complete")); err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("snapshot dir %s is missing .complete marker; the fetch did not finish — re-run `malcom snapshot fetch` (or delete the dir and start over) before importing", opts.SnapshotDir)
		}
		return nil, fmt.Errorf("stat .complete in %s: %w", opts.SnapshotDir, err)
	}
	log := opts.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	log = log.With("module", "import")
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
	stores, err := runImport(cr, db, opts.Height, extDir, stats, log)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	stats.Stores = stores
	stats.StreamElapsed = time.Since(streamStart).Truncate(time.Millisecond)

	// ─── final compaction ────────────────────────────────────────────
	t1 := time.Now()
	log.Info("starting final compaction")
	if err := db.Flush(); err != nil {
		log.Warn("pre-compact flush failed", "err", err)
	}
	if err := db.Compact([]byte{0x00}, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, true); err != nil {
		log.Warn("final compact failed", "err", err)
	}
	stats.FinalCompactElapsed = time.Since(t1).Truncate(time.Millisecond)
	log.Info("final compaction complete", "elapsed", stats.FinalCompactElapsed)

	if err := db.Close(); err != nil {
		log.Warn("close after compact failed", "err", err)
	}

	// ─── cleanup pass: reopen + Compact to reclaim orphan SSTs ───────
	// On a fresh cosmoshub-4 import the in-process Compact above queues
	// obsolete L0 files for deletion via pebble's cleanup manager, but
	// those deletions don't always drain before Close. The next Open
	// reclaims them via manifest replay — typically saves ~10 GiB.
	t2 := time.Now()
	log.Info("starting cleanup pass")
	cleanup, err := pebble.Open(appdbDir, &pebble.Options{
		MaxConcurrentCompactions: func() int { return maxCompact },
	})
	if err != nil {
		return nil, fmt.Errorf("cleanup reopen: %w", err)
	}
	if err := cleanup.Flush(); err != nil {
		log.Warn("cleanup flush failed", "err", err)
	}
	if err := cleanup.Compact([]byte{0x00}, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, true); err != nil {
		log.Warn("cleanup compact failed", "err", err)
	}
	if err := cleanup.Close(); err != nil {
		log.Warn("cleanup close failed", "err", err)
	}
	stats.CleanupElapsed = time.Since(t2).Truncate(time.Millisecond)
	log.Info("cleanup pass complete", "elapsed", stats.CleanupElapsed)

	stats.Elapsed = time.Since(t0).Truncate(time.Millisecond)
	log.Info("import complete",
		"elapsed", stats.Elapsed, "stores", len(stores), "output", appdbDir,
		"goroutines", runtime.NumGoroutine())

	return stats, nil
}
