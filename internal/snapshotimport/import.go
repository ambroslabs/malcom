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
	"math"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/cockroachdb/pebble"

	malcomlog "github.com/zrbecker/cosmos-p2p/internal/log"
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

	// CompactDuringImport enables pebble's auto-compactions while the
	// import streams. Default false (bulk-load mode): compactions are
	// deferred to a manual `malcom compact` pass or to gaiad's
	// runtime auto-compactions. Setting true trades import wall time
	// for less peak disk usage and a tighter LSM at end of import.
	CompactDuringImport bool

	// FlushSplitMB caps L0 SSTable size from memtable flushes. 0 =
	// pebble default (4 MiB). Setting equal to MemtableMB produces
	// ~1 SSTable per memtable flush; with CompactDuringImport off
	// this dramatically reduces the L0 file count gaiad sees on
	// first open.
	FlushSplitMB int

	// Log receives structured progress events. nil → discard.
	Log *slog.Logger
}

// Stats summarises a completed import. Returned by Import.
type Stats struct {
	Stores            []StoreInfo
	Items             uint64
	Extensions        int
	ExtensionPayloads int
	Elapsed           time.Duration
	StreamElapsed     time.Duration

	// Sub-timing inside the stream phase. Time is sampled at chunk
	// boundaries — each loop iteration accumulates one of three
	// buckets, so the three sum to roughly StreamElapsed (small slack
	// for the bookkeeping itself).
	StreamDecodeElapsed time.Duration // proto decode of SnapshotItems off the wire
	StreamIAVLElapsed   time.Duration // IAVL stack ops (addNode / finalize / hash)
	StreamPebbleElapsed time.Duration // batch.Set + batch.Commit
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
	pebbleLog := malcomlog.PebbleShim(log.With("module", "pebble"))
	popts := &pebble.Options{
		MemTableSize:                uint64(memMB) << 20,
		MemTableStopWritesThreshold: 4,
		Cache:                       pebble.NewCache(int64(cacheMB) << 20),
		MaxOpenFiles:                4096,
		MaxConcurrentCompactions:    func() int { return maxCompact },
		Logger:                      pebbleLog,
		// L0 SSTables produced during bulk-load are throwaway — the
		// post-import compact (`malcom compact`) or gaiad's runtime
		// auto-compactions rewrite them into a snappy-compressed L6
		// set. Skipping L0 compression saves ~10% of the stream's
		// CPU budget. TargetFileSize=MaxInt64 disables pebble's
		// per-flush file-size splitter (default 2 MiB) — see
		// parallel.go for the full reasoning.
		Levels: []pebble.LevelOptions{{
			Compression:    pebble.NoCompression,
			TargetFileSize: math.MaxInt64,
		}},
	}
	if !opts.CompactDuringImport {
		popts.DisableAutomaticCompactions = true
		popts.L0CompactionThreshold = 1024
		popts.L0StopWritesThreshold = 4096
		// One L0 SST per memtable flush — see parallel.go for the
		// reasoning. opts.FlushSplitMB is ignored in bulk mode.
		popts.FlushSplitBytes = math.MaxInt64
	} else if opts.FlushSplitMB > 0 {
		popts.FlushSplitBytes = int64(opts.FlushSplitMB) << 20
	}
	db, err := pebble.Open(appdbDir, popts)
	if err != nil {
		return nil, fmt.Errorf("open pebble at %s: %w", appdbDir, err)
	}
	// No defer Close — we explicitly Close before the cleanup reopen.

	// Tmp dir for per-store fast-path SSTable Writers. The bulk-ingest
	// path writes f/ entries to one SSTable per store, then atomically
	// links them into the LSM via db.Ingest at end-of-store. Removed
	// after Import returns.
	ingestTmpDir := filepath.Join(opts.OutDir, "ingest-tmp")
	if err := os.MkdirAll(ingestTmpDir, 0o755); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("mkdir ingest tmp: %w", err)
	}
	defer os.RemoveAll(ingestTmpDir)

	// ─── run the streaming import ────────────────────────────────────
	streamStart := time.Now()
	stats := &Stats{}
	stores, err := runImport(cr, db, opts.Height, extDir, ingestTmpDir, stats, log)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	stats.Stores = stores
	stats.StreamElapsed = time.Since(streamStart).Truncate(time.Millisecond)

	// Flush any remaining memtable data so the closed DB is durable
	// without us running an explicit compact. The user runs `malcom
	// compact -dir <appdb>` afterward (or lets gaiad's pebble
	// auto-compact at runtime) to consolidate the LSM. Skipping the
	// upfront compact saves ~2m wall on cosmoshub-4 — that work
	// isn't gone, just deferred.
	if err := db.Flush(); err != nil {
		log.Warn("pre-close flush failed", "err", err)
	}
	if err := db.Close(); err != nil {
		log.Warn("close db failed", "err", err)
	}

	stats.Elapsed = time.Since(t0).Truncate(time.Millisecond)
	log.Info("import complete",
		"elapsed", stats.Elapsed, "stores", len(stores), "output", appdbDir,
		"goroutines", runtime.NumGoroutine())

	return stats, nil
}
