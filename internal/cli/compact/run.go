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

	malcomlog "github.com/zrbecker/cosmos-p2p/internal/log"
	"github.com/zrbecker/cosmos-p2p/internal/pebbleutil"
)

// Run is the malcom subcommand entry point.
func Run(args []string) int {
	fs := flag.NewFlagSet("malcom compact", flag.ContinueOnError)
	dir := fs.String("dir", "", "path to pebble DB directory (required)")
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

	t0 := time.Now()
	log.Info("starting", "dir", *dir)
	if err := pebbleutil.CleanupCompact(*dir, log); err != nil {
		log.Error("compact failed", "err", err)
		return 1
	}
	log.Info("complete", "elapsed", time.Since(t0).Truncate(time.Millisecond))
	return 0
}
