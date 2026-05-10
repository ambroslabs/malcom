// Package snapshotimport is the `malcom snapshot import` subcommand:
// take a downloaded snapshot directory and produce gaiad-compatible
// artefacts.
//
// Output: <-out>/appdb_<chain>_<height>/{application.db,extensions}/.
// Default -out is the current working directory.
//
// Pebble bulk-load tuning lives in the [chains.<id>.import] section
// of config.toml.
package snapshotimport

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/cockroachdb/pebble"

	"github.com/zrbecker/cosmos-p2p/internal/config"
	malcomlog "github.com/zrbecker/cosmos-p2p/internal/log"
	"github.com/zrbecker/cosmos-p2p/internal/snapshotdiff"
	"github.com/zrbecker/cosmos-p2p/internal/snapshotimport"
)

type metaJSON struct {
	ChainID string `json:"chain_id"`
	Height  uint64 `json:"height"`
	HashHex string `json:"hash_hex"`
}

// Run is the malcom subcommand entry point.
func Run(args []string) int {
	fs := flag.NewFlagSet("malcom snapshot import", flag.ContinueOnError)
	chain := fs.String("chain", "", "chain id (override; required if snapshot meta.json is missing or omits chain_id)")
	snapshotDir := fs.String("snapshot", "", "snapshot directory to import (with chunk_*.bin + meta.json)")
	out := fs.String("out", ".", "parent dir for the output (subdir appdb_<chain>_<height>/ created inside)")
	height := fs.Int64("height", 0, "height override (required if snapshot meta.json is missing or omits height)")
	noExt := fs.Bool("no-extensions", false, "skip writing extension payloads")
	logMode := fs.String("log", "", "log output: auto (default), pretty, text, json")
	debug := fs.Bool("debug", false, "verbose logging")
	cpuProfile := fs.String("cpuprofile", "", "write a pprof CPU profile to this path; open with `go tool pprof -http=: <path>`")
	memProfile := fs.String("memprofile", "", "write a pprof heap profile to this path at end of run")
	memStats := fs.Bool("mem-stats", false, "log runtime.MemStats every 10s during the run (module=memstats)")
	parallel := fs.Bool("parallel", false, "use the parallel pipeline: decompress to temp file once, then process stores concurrently across workers")
	parallelWorkers := fs.Int("workers", 0, "max concurrent store workers in -parallel mode (default = NumCPU)")
	tempDir := fs.String("tmp-dir", "", "(unused — preserved for backward CLI compatibility; the parallel pipeline streams through an in-memory chunk ring rather than a temp file)")
	chunkMB := fs.Int("chunk-mb", 0, "in-memory chunk-ring budget in MiB (the streaming buffer between stage 1 decompression and stage 2 readers). 0 = default 512. Bigger values reduce stage-1 backpressure on multi-store-concurrent chains (cosmoshub bank+ibc, osmosis cl/ibc/wasm) at the cost of more peak RSS; single-polestar chains (bbn finality) don't benefit from larger rings.")
	fastIngest := fs.Bool("fast-ingest", true, "route f/ (fast-storage) entries through pebble's bulk-ingest path (per-store sstable.Writer + db.Ingest at end-of-store). Disable to send them through the regular pebble.Batch instead. Output is bit-identical either way; this flag exists to A/B the bulk-ingest performance benefit.")
	verifyFast := fs.Bool("verify-fast", false, "after import, count each store's f/ (fast-storage) entries and check the count matches the leaves the import wrote. Catches silent drops or duplicates. Cost: one sequential pebble iterator pass per store — tens of seconds for finality-class stores, subsecond for the rest. (Doesn't catch corrupted values; for that, re-run with the snapshot to compare value bytes.)")
	waveParallel := fs.Bool("wave-parallel", false, "within-store wave-parallel hashing: each per-store worker spawns a hash worker pool to overlap iavl hash + encode work across cores. Helps single-store-dominated chains (bbn finality: ~-2 min vs async-only). Slightly regresses chains where multiple polestar stores run concurrent (osmosis cl/ibc/wasm). Default off.")
	flushSplitMB := fs.Int("flush-split-mb", -1, "cap on L0 SSTable size from memtable flushes; -1 = use [import].flush_split_mb (defaults to memtable_mb)")
	compactDuringImport := fs.Bool("compact-during-import", false, "enable pebble auto-compactions during import (default off; trades wall time for tighter end-of-import LSM)")
	compactWorkers := fs.Int("compact-workers", 0, "override [compact].max_concurrent_compactions for this run (only meaningful when -compact-during-import is set)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *snapshotDir == "" {
		fs.Usage()
		return 2
	}

	mode, ok := malcomlog.ParseMode(*logMode)
	if !ok {
		fmt.Fprintf(os.Stderr, "invalid -log %q (want auto/pretty/text/json)\n", *logMode)
		return 2
	}
	// Load config so the logger picks up [log] / [log.modules]. Fall
	// back to defaults if config is missing (the import path doesn't
	// strictly require a config — chain id can come from meta.json).
	var logTuning malcomlog.Tuning
	if cfg, err := config.Load(); err == nil {
		logTuning = malcomlog.Tuning{Level: cfg.Log.Level, Modules: cfg.Log.Modules}
	}
	logOpts, err := malcomlog.BuildOptions(logTuning, mode, *debug, os.Stderr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "log config: %v\n", err)
		return 2
	}
	logger := malcomlog.New(logOpts)
	log := logger.With("module", "import-cli")

	// CPU profile spans the whole Run including the final compact +
	// cleanup pass — useful for profiling the entire pipeline, not
	// just the stream phase.
	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			log.Error("create cpu profile failed", "path", *cpuProfile, "err", err)
			return 1
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Error("start cpu profile failed", "err", err)
			_ = f.Close()
			return 1
		}
		defer func() {
			pprof.StopCPUProfile()
			_ = f.Close()
			log.Info("cpu profile written", "path", *cpuProfile)
		}()
	}

	if *memStats {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go runMemStatsTicker(ctx, logger.With("module", "memstats"), 10*time.Second)
	}

	// Read meta.json if present. The chain id and height are normally
	// drawn from here so the user doesn't have to repeat themselves;
	// the -chain / -height flags only kick in when meta.json is
	// missing or pre-dates the field, in which case they're required
	// overrides. Any flag value that's set must match meta.json.
	metaPath := filepath.Join(*snapshotDir, "meta.json")
	meta, metaErr := readMeta(metaPath)
	switch {
	case metaErr != nil && !os.IsNotExist(metaErr):
		log.Error("read meta.json", "path", metaPath, "err", metaErr)
		return 1
	case metaErr != nil:
		// missing meta.json — flags must fully specify chain + height.
		if *chain == "" || *height == 0 {
			log.Error("no meta.json; pass -chain and -height to override", "path", *snapshotDir)
			return 1
		}
	default:
		if meta.ChainID != "" && *chain != "" && meta.ChainID != *chain {
			log.Error("meta.json chain_id does not match -chain", "meta", meta.ChainID, "flag", *chain)
			return 1
		}
		if meta.Height != 0 && *height != 0 && uint64(*height) != meta.Height {
			log.Error("meta.json height does not match -height", "meta", meta.Height, "flag", *height)
			return 1
		}
		if *chain == "" {
			if meta.ChainID == "" {
				log.Error("meta.json has no chain_id; pass -chain to override")
				return 1
			}
			*chain = meta.ChainID
		}
		if *height == 0 {
			if meta.Height == 0 {
				log.Error("meta.json has no height; pass -height to override")
				return 1
			}
			*height = int64(meta.Height)
		}
	}

	cfg, err := config.Load()
	if err != nil {
		log.Error("config load", "err", err)
		return 1
	}
	ch, err := cfg.Resolve(*chain)
	if err != nil {
		log.Error("resolve chain", "err", err, "chain", *chain)
		return 1
	}

	outDir := filepath.Join(*out, fmt.Sprintf("appdb_%s_%d", ch.ChainID, *height))
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		log.Error("mkdir out failed", "err", err, "dir", outDir)
		return 1
	}

	if ch.Import.MinFreeGB > 0 {
		if err := snapshotdiff.CheckFreeSpace(outDir, uint64(ch.Import.MinFreeGB)<<30); err != nil {
			log.Error("disk check failed", "err", err)
			return 1
		}
	}

	log.Info("starting",
		"config", cfg.Path(),
		"chain", ch.ChainID,
		"snapshot", *snapshotDir,
		"out", outDir,
		"height", *height,
		"memtable_mb", ch.Import.MemtableMB,
		"cache_mb", ch.Import.CacheMB,
		"max_compact", ch.Import.MaxConcurrentCompactions,
		"extensions", !*noExt)

	maxCompact := ch.Compact.MaxConcurrentCompactions
	if *compactWorkers > 0 {
		maxCompact = *compactWorkers
	}
	flushSplit := ch.Import.FlushSplitMB
	if *flushSplitMB >= 0 {
		flushSplit = *flushSplitMB
	}
	compactDuring := ch.Import.CompactDuringImport || *compactDuringImport
	importOpts := snapshotimport.Options{
		SnapshotDir:              *snapshotDir,
		OutDir:                   outDir,
		Height:                   *height,
		NoExtensions:             *noExt,
		MemtableMB:               ch.Import.MemtableMB,
		CacheMB:                  ch.Import.CacheMB,
		MaxConcurrentCompactions: maxCompact,
		FlushSplitMB:             flushSplit,
		CompactDuringImport:      compactDuring,
		Log:                      logger,
	}
	var stats *snapshotimport.Stats
	if *parallel {
		stats, err = snapshotimport.ImportParallel(snapshotimport.ParallelOptions{
			Options:      importOpts,
			Workers:      *parallelWorkers,
			TempDir:      *tempDir,
			ChunkMB:      *chunkMB,
			WaveParallel: *waveParallel,
			FastIngest:   *fastIngest,
		})
	} else {
		if *waveParallel {
			log.Warn("-wave-parallel ignored without -parallel (wave-parallel only applies to per-store workers in the parallel pipeline)")
		}
		stats, err = snapshotimport.Import(importOpts)
	}
	if err != nil {
		log.Error("import failed", "err", err)
		return 1
	}

	appdbMeta := snapshotimport.AppDBMeta{
		ChainID:               ch.ChainID,
		Height:                *height,
		ImportedAt:            time.Now().UTC(),
		SourceSnapshotHashHex: meta.HashHex,
		DBBackend:             snapshotimport.DBBackendPebble,
	}
	if err := snapshotimport.WriteAppDBMeta(outDir, appdbMeta); err != nil {
		log.Error("write appdb meta", "err", err)
		return 1
	}

	if *verifyFast {
		// Re-open the DB read-only so the verifier can iterate the
		// f/ namespaces without disturbing pebble's internal state.
		// VerifyFast iterates each store's f/ range and compares the
		// count to the LeafCount the import recorded; mismatch =
		// silent drop or duplicate.
		appdbDir := filepath.Join(outDir, "application.db")
		vdb, err := pebble.Open(appdbDir, &pebble.Options{ReadOnly: true})
		if err != nil {
			log.Error("verify-fast: open appdb", "err", err)
			return 1
		}
		err = snapshotimport.VerifyFast(vdb, stats.Stores, log)
		_ = vdb.Close()
		if err != nil {
			log.Error("verify-fast failed", "err", err)
			return 1
		}
		log.Info("verify-fast passed")
	}

	finalDB := filepath.Join(outDir, "application.db")
	dbBytes := uint64(0)
	if size, err := dirSize(finalDB); err == nil {
		dbBytes = uint64(size)
	}
	log.Info("complete",
		"elapsed", stats.Elapsed,
		"stores", len(stats.Stores),
		"items", stats.Items,
		"extensions", stats.Extensions,
		"ext_payloads", stats.ExtensionPayloads,
		"stream_elapsed", stats.StreamElapsed,
		"stream_decode_elapsed", stats.StreamDecodeElapsed,
		"stream_iavl_elapsed", stats.StreamIAVLElapsed,
		"stream_pebble_elapsed", stats.StreamPebbleElapsed,
		"appdb", finalDB,
		"appdb_bytes", dbBytes)

	if *memProfile != "" {
		f, err := os.Create(*memProfile)
		if err != nil {
			log.Error("create mem profile failed", "path", *memProfile, "err", err)
			return 1
		}
		runtime.GC() // GC once so the heap profile reflects steady-state, not leftover obsolete allocations
		if err := pprof.Lookup("heap").WriteTo(f, 0); err != nil {
			log.Error("write mem profile failed", "err", err)
			_ = f.Close()
			return 1
		}
		_ = f.Close()
		log.Info("mem profile written", "path", *memProfile)
	}
	return 0
}

// runMemStatsTicker logs runtime.MemStats every interval. Deltas
// (num_gc, gc_pause_ns) are reported since the previous tick so the
// reader can spot bursts; absolute fields (heap_alloc, heap_sys,
// next_gc) show steady-state.
func runMemStatsTicker(ctx context.Context, log *slog.Logger, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()

	var prev runtime.MemStats
	runtime.ReadMemStats(&prev)
	log.Info("baseline",
		"heap_alloc_bytes", prev.HeapAlloc,
		"heap_sys_bytes", prev.HeapSys,
		"next_gc_bytes", prev.NextGC,
		"num_gc", uint64(prev.NumGC),
	)

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			log.Info("snapshot",
				"heap_alloc_bytes", m.HeapAlloc,
				"heap_sys_bytes", m.HeapSys,
				"heap_inuse_bytes", m.HeapInuse,
				"next_gc_bytes", m.NextGC,
				"num_gc_delta", uint64(m.NumGC-prev.NumGC),
				"gc_pause_ns_delta", m.PauseTotalNs-prev.PauseTotalNs,
				"gc_cpu_fraction", m.GCCPUFraction,
				"goroutines", runtime.NumGoroutine(),
			)
			prev = m
		}
	}
}

func readMeta(path string) (metaJSON, error) {
	var m metaJSON
	f, err := os.Open(path)
	if err != nil {
		return m, err
	}
	defer f.Close()
	if err := json.NewDecoder(f).Decode(&m); err != nil {
		return m, err
	}
	return m, nil
}

func dirSize(dir string) (int64, error) {
	var total int64
	err := filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	return total, err
}

