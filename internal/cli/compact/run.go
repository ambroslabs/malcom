// Package compact is the `malcom compact` subcommand: a full-keyspace
// pebble compaction pass to consolidate L0/L1 SSTs after a bulk-load
// (typical post-bulk-load DBs shrink 30-50% after one pass).
package compact

import (
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/ambroslabs/malcom/internal/cli/cliexit"
	"github.com/ambroslabs/malcom/internal/config"
	malcomlog "github.com/ambroslabs/malcom/internal/log"
	"github.com/ambroslabs/malcom/internal/pebbleutil"
)

// NewCmd returns the `malcom compact` cobra command.
func NewCmd() *cobra.Command {
	var (
		dir              string
		workers          int
		cacheMB          int
		targetFileSizeMB int
		logMode          string
		debug            bool
	)
	cmd := &cobra.Command{
		Use:   "compact",
		Short: "full-keyspace pebble compaction (reclaim slack post-import)",
		Long:  "Run a full-keyspace pebble compaction pass to consolidate L0/L1 SSTs after a bulk-load.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return run(dir, workers, cacheMB, targetFileSizeMB, logMode, debug)
		},
	}
	cmd.Flags().StringVar(&dir, "dir", "", "path to pebble DB directory (required)")
	cmd.Flags().IntVar(&workers, "workers", 0, "override [compact].max_concurrent_compactions (default = config or 8)")
	cmd.Flags().IntVar(&cacheMB, "cache-mb", 0, "override [compact].cache_mb — pebble block-cache size in MiB during the compact. 0 = use [compact].cache_mb from config; if that's also 0, compute a memory-proportional default from /proc/meminfo (1/8 of MemAvailable, clamped to [64, 1024] MiB).")
	cmd.Flags().IntVar(&targetFileSizeMB, "target-file-size-mb", -1, "override [compact].target_file_size_mb (per-level TargetFileSize in MiB; 0 = pebble defaults; e.g. 1024 = merge into a small number of large output files at the cost of wall time)")
	cmd.Flags().StringVar(&logMode, "log", "", "log output: auto (default), pretty, text, json")
	cmd.Flags().BoolVar(&debug, "debug", false, "verbose logging")
	return cmd
}

func run(dir string, workers, cacheMB, targetFileSizeMB int, logMode string, debug bool) error {
	if dir == "" {
		fmt.Fprintln(os.Stderr, "required: --dir <path>")
		return &cliexit.Error{Code: 2}
	}

	mode, ok := malcomlog.ParseMode(logMode)
	if !ok {
		fmt.Fprintf(os.Stderr, "invalid --log %q (want auto/pretty/text/json)\n", logMode)
		return &cliexit.Error{Code: 2}
	}
	level := slog.LevelInfo
	if debug {
		level = slog.LevelDebug
	}
	log := malcomlog.New(malcomlog.Options{
		Writer: os.Stderr,
		Mode:   mode,
		Level:  level,
	}).With("module", "compact")

	// Resolve max-concurrent-compactions, target-file-size-mb, and
	// cache-mb:
	//   1. CLI flag wins.
	//   2. else, [compact] from global config (no chain selected —
	//      `malcom compact` doesn't take --chain).
	//   3. else: NumCPU for workers / pebble defaults for target-file-
	//      size / memory-proportional heuristic for cache-mb (1/8 of
	//      MemAvailable, clamped to [64, 1024] MiB — see #100).
	maxCompact := workers
	targetFS := targetFileSizeMB
	var cfgCompact config.CompactTuning
	if maxCompact <= 0 || targetFS < 0 || cacheMB <= 0 {
		if cfg, err := config.Load(); err == nil {
			// applyCompactDefaults runs in Resolve, but the global
			// config.Load doesn't apply per-chain defaults — apply
			// our compact-section default here to honor NumCPU.
			cfgCompact = cfg.Compact
			config.ApplyCompactDefaults(&cfgCompact)
			if maxCompact <= 0 {
				maxCompact = cfgCompact.MaxConcurrentCompactions
			}
			if targetFS < 0 {
				targetFS = cfgCompact.TargetFileSizeMB
			}
		}
	}
	if targetFS < 0 {
		targetFS = 0
	}

	// cache-mb resolution. Both /proc/meminfo and the cgroup limit
	// feed the heuristic — using the smaller keeps containers and
	// systemd MemoryMax= units from picking a host-sized cache that
	// would OOM their own cgroup. resolveCacheMB is pure (testable);
	// the I/O happens here.
	memMB, memErr := readMemAvailableMB()
	if memErr == nil {
		if cgroupMB, cgErr := readCgroupMemMaxMB(); cgErr == nil {
			memMB = effectiveAvailableMB(memMB, cgroupMB)
		}
		// cgroup parse errors are non-fatal: fall through with the
		// uncorrected /proc/meminfo value. The heuristic's clamp
		// still bounds the damage.
	}
	resolved := resolveCacheMB(cacheMB, cfgCompact.CacheMB, memMB, memErr)
	cacheMBValue := resolved.Value
	logKVs := append([]any{"cache_mb", cacheMBValue, "source", resolved.Source}, resolved.LogKVs...)
	if resolved.Source == "fallback" {
		log.Warn("cache-mb fallback (could not read /proc/meminfo)", logKVs...)
	} else if resolved.Source == "heuristic" {
		log.Info("cache-mb heuristic", append(logKVs, "hint", "set [compact].cache_mb to pin")...)
	}

	t0 := time.Now()
	log.Info("starting", "dir", dir, "max_concurrent_compactions", maxCompact, "cache_mb", cacheMBValue, "target_file_size_mb", targetFS)
	if err := pebbleutil.CleanupCompact(dir, maxCompact, cacheMBValue, targetFS, log); err != nil {
		log.Error("compact failed", "err", err)
		return &cliexit.Error{Code: 1}
	}
	log.Info("complete", "elapsed", time.Since(t0).Truncate(time.Millisecond))
	return nil
}
