// Package snapshotimport is the `malcom snapshot import` subcommand:
// take a downloaded snapshot directory and produce cosmos-sdk-compatible
// artefacts.
//
// Output: <--out>/appdb_<chain>_<height>/{application.db,extensions}/.
// Default --out is the current working directory.
//
// Pebble bulk-load tuning lives in the [chains.<id>.import] section
// of config.toml.
package snapshotimport

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/spf13/cobra"

	"github.com/ambroslabs/malcom/internal/cli/cliexit"
	"github.com/ambroslabs/malcom/internal/config"
	malcomlog "github.com/ambroslabs/malcom/internal/log"
	"github.com/ambroslabs/malcom/internal/snapshotdiff"
	"github.com/ambroslabs/malcom/internal/snapshotimport"
)

type metaJSON struct {
	ChainID string `json:"chain_id"`
	Height  uint64 `json:"height"`
	HashHex string `json:"hash_hex"`
}

type importFlags struct {
	chain               string
	snapshotDir         string
	out                 string
	height              int64
	noExt               bool
	logMode             string
	debug               bool
	cpuProfile          string
	memProfile          string
	memStats            bool
	parallel            bool
	parallelWorkers     int
	tempDir             string
	chunkMB             int
	fastIngest          bool
	waveParallel        bool
	flushSplitMB        int
	compactDuringImport bool
	compactWorkers      int
	noVerify            bool
}

// NewCmd returns the `malcom snapshot import` cobra command.
func NewCmd() *cobra.Command {
	f := &importFlags{}
	cmd := &cobra.Command{
		Use:   "import",
		Short: "convert a snapshot dir into application.db + extensions/",
		Long:  "Take a downloaded snapshot directory and produce cosmos-sdk-compatible artefacts (application.db + extensions).",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return run(f)
		},
	}
	cmd.Flags().StringVar(&f.chain, "chain", "", "chain id (override; required if snapshot meta.json is missing or omits chain_id)")
	cmd.Flags().StringVar(&f.snapshotDir, "snapshot", "", "snapshot directory to import (with chunk_*.bin + meta.json)")
	cmd.Flags().StringVar(&f.out, "out", ".", "parent dir for the output (subdir appdb_<chain>_<height>/ created inside)")
	cmd.Flags().Int64Var(&f.height, "height", 0, "height override (required if snapshot meta.json is missing or omits height)")
	cmd.Flags().BoolVar(&f.noExt, "no-extensions", false, "skip writing extension payloads")
	cmd.Flags().StringVar(&f.logMode, "log", "", "log output: auto (default), pretty, text, json")
	cmd.Flags().BoolVar(&f.debug, "debug", false, "verbose logging")
	cmd.Flags().StringVar(&f.cpuProfile, "cpuprofile", "", "write a pprof CPU profile to this path; open with 'go tool pprof -http=: <path>'")
	cmd.Flags().StringVar(&f.memProfile, "memprofile", "", "write a pprof heap profile to this path at end of run")
	cmd.Flags().BoolVar(&f.memStats, "mem-stats", false, "log runtime.MemStats every 10s during the run (module=memstats)")
	cmd.Flags().BoolVar(&f.parallel, "parallel", false, "use the parallel pipeline: decompress to temp file once, then process stores concurrently across workers")
	cmd.Flags().IntVar(&f.parallelWorkers, "workers", 0, "max concurrent store workers in --parallel mode (default = NumCPU)")
	cmd.Flags().StringVar(&f.tempDir, "tmp-dir", "", "(unused — preserved for backward CLI compatibility; the parallel pipeline streams through an in-memory chunk ring rather than a temp file)")
	cmd.Flags().IntVar(&f.chunkMB, "chunk-mb", 0, "in-memory chunk-ring budget in MiB (the streaming buffer between stage 1 decompression and stage 2 readers). 0 = default 512. Bigger values reduce stage-1 backpressure on multi-store-concurrent chains (cosmoshub bank+ibc, osmosis cl/ibc/wasm) at the cost of more peak RSS; single-polestar chains (bbn finality) don't benefit from larger rings.")
	cmd.Flags().BoolVar(&f.fastIngest, "fast-ingest", true, "route f/ (fast-storage) entries through pebble's bulk-ingest path (per-store sstable.Writer + db.Ingest at end-of-store). Disable to send them through the regular pebble.Batch instead. Output is bit-identical either way; this flag exists to A/B the bulk-ingest performance benefit.")
	cmd.Flags().BoolVar(&f.waveParallel, "wave-parallel", false, "within-store wave-parallel hashing: each per-store worker spawns a hash worker pool to overlap iavl hash + encode work across cores. Helps single-store-dominated chains (bbn finality: ~-2 min vs async-only). Slightly regresses chains where multiple polestar stores run concurrent (osmosis cl/ibc/wasm). Default off.")
	cmd.Flags().IntVar(&f.flushSplitMB, "flush-split-mb", -1, "cap on L0 SSTable size from memtable flushes; -1 = use [import].flush_split_mb (defaults to memtable_mb)")
	cmd.Flags().BoolVar(&f.compactDuringImport, "compact-during-import", false, "enable pebble auto-compactions during import (default off; trades wall time for tighter end-of-import LSM)")
	cmd.Flags().IntVar(&f.compactWorkers, "compact-workers", 0, "override [compact].max_concurrent_compactions for this run (only meaningful when --compact-during-import is set)")
	cmd.Flags().BoolVar(&f.noVerify, "no-verify", false, "skip the post-import AppHash check against the chain's configured rpcs. Default behaviour: after import completes, when the chain has rpcs, run the same check 'malcom verify' performs and exit non-zero on mismatch.")
	return cmd
}

func run(f *importFlags) error {
	if f.snapshotDir == "" {
		fmt.Fprintln(os.Stderr, "required: --snapshot <dir>")
		return &cliexit.Error{Code: 2}
	}

	mode, ok := malcomlog.ParseMode(f.logMode)
	if !ok {
		fmt.Fprintf(os.Stderr, "invalid --log %q (want auto/pretty/text/json)\n", f.logMode)
		return &cliexit.Error{Code: 2}
	}
	// Load config so the logger picks up [log] / [log.modules]. Fall
	// back to defaults if config is missing (the import path doesn't
	// strictly require a config — chain id can come from meta.json).
	var logTuning malcomlog.Tuning
	if cfg, err := config.Load(); err == nil {
		logTuning = malcomlog.Tuning{Level: cfg.Log.Level, Modules: cfg.Log.Modules}
	}
	logOpts, err := malcomlog.BuildOptions(logTuning, mode, f.debug, os.Stderr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "log config: %v\n", err)
		return &cliexit.Error{Code: 2}
	}
	logger := malcomlog.New(logOpts)
	log := logger.With("module", "import-cli")

	// CPU profile spans the whole Run including the final compact +
	// cleanup pass — useful for profiling the entire pipeline, not
	// just the stream phase.
	if f.cpuProfile != "" {
		cf, err := os.Create(f.cpuProfile)
		if err != nil {
			log.Error("create cpu profile failed", "path", f.cpuProfile, "err", err)
			return &cliexit.Error{Code: 1}
		}
		if err := pprof.StartCPUProfile(cf); err != nil {
			log.Error("start cpu profile failed", "err", err)
			_ = cf.Close()
			return &cliexit.Error{Code: 1}
		}
		defer func() {
			pprof.StopCPUProfile()
			_ = cf.Close()
			log.Info("cpu profile written", "path", f.cpuProfile)
		}()
	}

	if f.memStats {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go runMemStatsTicker(ctx, logger.With("module", "memstats"), 10*time.Second)
	}

	// Read meta.json if present. The chain id and height are normally
	// drawn from here so the user doesn't have to repeat themselves;
	// the --chain / --height flags only kick in when meta.json is
	// missing or pre-dates the field, in which case they're required
	// overrides. Any flag value that's set must match meta.json.
	metaPath := filepath.Join(f.snapshotDir, "meta.json")
	meta, metaErr := readMeta(metaPath)
	switch {
	case metaErr != nil && !os.IsNotExist(metaErr):
		log.Error("read meta.json", "path", metaPath, "err", metaErr)
		return &cliexit.Error{Code: 1}
	case metaErr != nil:
		// missing meta.json — flags must fully specify chain + height.
		if f.chain == "" || f.height == 0 {
			log.Error("no meta.json; pass --chain and --height to override", "path", f.snapshotDir)
			return &cliexit.Error{Code: 1}
		}
	default:
		if meta.ChainID != "" && f.chain != "" && meta.ChainID != f.chain {
			log.Error("meta.json chain_id does not match --chain", "meta", meta.ChainID, "flag", f.chain)
			return &cliexit.Error{Code: 1}
		}
		if meta.Height != 0 && f.height != 0 && uint64(f.height) != meta.Height {
			log.Error("meta.json height does not match --height", "meta", meta.Height, "flag", f.height)
			return &cliexit.Error{Code: 1}
		}
		if f.chain == "" {
			if meta.ChainID == "" {
				log.Error("meta.json has no chain_id; pass --chain to override")
				return &cliexit.Error{Code: 1}
			}
			f.chain = meta.ChainID
		}
		if f.height == 0 {
			if meta.Height == 0 {
				log.Error("meta.json has no height; pass --height to override")
				return &cliexit.Error{Code: 1}
			}
			f.height = int64(meta.Height)
		}
	}

	cfg, err := config.Load()
	if err != nil {
		log.Error("config load", "err", err)
		return &cliexit.Error{Code: 1}
	}
	ch, err := cfg.Resolve(f.chain)
	if err != nil {
		log.Error("resolve chain", "err", err, "chain", f.chain)
		return &cliexit.Error{Code: 1}
	}

	outDir := filepath.Join(f.out, fmt.Sprintf("appdb_%s_%d", ch.ChainID, f.height))
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		log.Error("mkdir out failed", "err", err, "dir", outDir)
		return &cliexit.Error{Code: 1}
	}

	if ch.Import.MinFreeGB > 0 {
		if err := snapshotdiff.CheckFreeSpace(outDir, uint64(ch.Import.MinFreeGB)<<30); err != nil {
			log.Error("disk check failed", "err", err)
			return &cliexit.Error{Code: 1}
		}
	}

	log.Info("starting",
		"config", cfg.Path(),
		"chain", ch.ChainID,
		"snapshot", f.snapshotDir,
		"out", outDir,
		"height", f.height,
		"memtable_mb", ch.Import.MemtableMB,
		"cache_mb", ch.Import.CacheMB,
		"max_compact", ch.Import.MaxConcurrentCompactions,
		"extensions", !f.noExt)

	maxCompact := ch.Compact.MaxConcurrentCompactions
	if f.compactWorkers > 0 {
		maxCompact = f.compactWorkers
	}
	flushSplit := ch.Import.FlushSplitMB
	if f.flushSplitMB >= 0 {
		flushSplit = f.flushSplitMB
	}
	compactDuring := ch.Import.CompactDuringImport || f.compactDuringImport
	importOpts := snapshotimport.Options{
		SnapshotDir:              f.snapshotDir,
		OutDir:                   outDir,
		Height:                   f.height,
		NoExtensions:             f.noExt,
		MemtableMB:               ch.Import.MemtableMB,
		CacheMB:                  ch.Import.CacheMB,
		MaxConcurrentCompactions: maxCompact,
		FlushSplitMB:             flushSplit,
		CompactDuringImport:      compactDuring,
		Log:                      logger,
	}
	var stats *snapshotimport.Stats
	if f.parallel {
		stats, err = snapshotimport.ImportParallel(snapshotimport.ParallelOptions{
			Options:      importOpts,
			Workers:      f.parallelWorkers,
			TempDir:      f.tempDir,
			ChunkMB:      f.chunkMB,
			WaveParallel: f.waveParallel,
			FastIngest:   f.fastIngest,
		})
	} else {
		if f.waveParallel {
			log.Warn("--wave-parallel ignored without --parallel (wave-parallel only applies to per-store workers in the parallel pipeline)")
		}
		stats, err = snapshotimport.Import(importOpts)
	}
	if err != nil {
		log.Error("import failed", "err", err)
		return &cliexit.Error{Code: 1}
	}

	appdbMeta := snapshotimport.AppDBMeta{
		ChainID:               ch.ChainID,
		Height:                f.height,
		ImportedAt:            time.Now().UTC(),
		SourceSnapshotHashHex: meta.HashHex,
		DBBackend:             snapshotimport.DBBackendPebble,
	}
	if err := snapshotimport.WriteAppDBMeta(outDir, appdbMeta); err != nil {
		log.Error("write appdb meta", "err", err)
		return &cliexit.Error{Code: 1}
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

	if rc := runPostImportVerify(log, outDir, f.height, ch.RPCs, f.noVerify); rc != 0 {
		// Mismatch exits 7, RPC-unreachable returns 0 so the operator
		// can still inspect the imported db while they sort out an
		// RPC. See runPostImportVerify for the breakdown.
		return &cliexit.Error{Code: rc}
	}

	if f.memProfile != "" {
		mf, err := os.Create(f.memProfile)
		if err != nil {
			log.Error("create mem profile failed", "path", f.memProfile, "err", err)
			return &cliexit.Error{Code: 1}
		}
		runtime.GC() // GC once so the heap profile reflects steady-state, not leftover obsolete allocations
		if err := pprof.Lookup("heap").WriteTo(mf, 0); err != nil {
			log.Error("write mem profile failed", "err", err)
			_ = mf.Close()
			return &cliexit.Error{Code: 1}
		}
		_ = mf.Close()
		log.Info("mem profile written", "path", f.memProfile)
	}
	return nil
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
