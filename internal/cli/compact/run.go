// Package compact is the `malcom compact` subcommand: a full-keyspace
// pebble compaction pass to consolidate L0/L1 SSTs after a bulk-load
// (typical post-bulk-load DBs shrink 30-50% after one pass).
package compact

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/zrbecker/cosmos-p2p/internal/config"
	malcomlog "github.com/zrbecker/cosmos-p2p/internal/log"
	"github.com/zrbecker/cosmos-p2p/internal/pebbleutil"
)

// Run is the malcom subcommand entry point.
func Run(args []string) int {
	fs := flag.NewFlagSet("malcom compact", flag.ContinueOnError)
	dir := fs.String("dir", "", "path to pebble DB directory (required)")
	workers := fs.Int("workers", 0, "override [compact].max_concurrent_compactions (default = config or 8)")
	cacheMB := fs.Int("cache-mb", 0, "pebble block-cache size in MiB during the compact (default 8192). Larger = more bloom/index blocks resident, fewer disk reads on the L0→L1 pass when the DB has tens of thousands of L0 SSTables.")
	targetFileSizeMB := fs.Int("target-file-size-mb", -1, "override [compact].target_file_size_mb (per-level TargetFileSize in MiB; 0 = pebble defaults; e.g. 1024 = merge into a small number of large output files at the cost of wall time)")
	logMode := fs.String("log", "", "log output: auto (default), pretty, text, json")
	debug := fs.Bool("debug", false, "verbose logging")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *dir == "" {
		fs.Usage()
		return 2
	}

	mode, ok := malcomlog.ParseMode(*logMode)
	if !ok {
		fmt.Fprintf(os.Stderr, "invalid -log %q (want auto/pretty/text/json)\n", *logMode)
		return 2
	}
	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	log := malcomlog.New(malcomlog.Options{
		Writer: os.Stderr,
		Mode:   mode,
		Level:  level,
	}).With("module", "compact")

	// Resolve max-concurrent-compactions and target-file-size-mb:
	//   1. CLI flag wins.
	//   2. else, [compact] from global config (no chain selected —
	//      `malcom compact` doesn't take -chain).
	//   3. else, the pebbleutil default (8 / pebble defaults).
	maxCompact := *workers
	targetFS := *targetFileSizeMB
	if maxCompact <= 0 || targetFS < 0 {
		if cfg, err := config.Load(); err == nil {
			// applyCompactDefaults runs in Resolve, but the global
			// config.Load doesn't apply per-chain defaults — apply
			// our compact-section default here to honor NumCPU.
			ct := cfg.Compact
			config.ApplyCompactDefaults(&ct)
			if maxCompact <= 0 {
				maxCompact = ct.MaxConcurrentCompactions
			}
			if targetFS < 0 {
				targetFS = ct.TargetFileSizeMB
			}
		}
	}
	if targetFS < 0 {
		targetFS = 0
	}

	t0 := time.Now()
	log.Info("starting", "dir", *dir, "max_concurrent_compactions", maxCompact, "cache_mb", *cacheMB, "target_file_size_mb", targetFS)
	if err := pebbleutil.CleanupCompact(*dir, maxCompact, *cacheMB, targetFS, log); err != nil {
		log.Error("compact failed", "err", err)
		return 1
	}
	log.Info("complete", "elapsed", time.Since(t0).Truncate(time.Millisecond))
	return 0
}
