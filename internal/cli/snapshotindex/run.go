// Package snapshotindex is the `malcom snapshot index` subcommand:
// build a per-store offset index of a snapshot directory and print
// timing + size summary. Used to evaluate the parallel-import design
// — measures how fast we can locate per-store boundaries before
// committing to the full per-store-parallel architecture.
package snapshotindex

import (
	"flag"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/zrbecker/cosmos-p2p/internal/config"
	malcomlog "github.com/zrbecker/cosmos-p2p/internal/log"
	"github.com/zrbecker/cosmos-p2p/internal/snapshotimport"
)

// Run is the malcom subcommand entry point.
func Run(args []string) int {
	fs := flag.NewFlagSet("malcom snapshot index", flag.ContinueOnError)
	snapshotDir := fs.String("snapshot", "", "snapshot directory to index (with chunk_*.bin)")
	logMode := fs.String("log", "", "log output: auto (default), pretty, text, json")
	debug := fs.Bool("debug", false, "verbose logging")
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
	var logTuning malcomlog.Tuning
	if cfg, err := config.Load(); err == nil {
		logTuning = malcomlog.Tuning{Level: cfg.Log.Level, Modules: cfg.Log.Modules}
	}
	logOpts, err := malcomlog.BuildOptions(logTuning, mode, *debug, os.Stderr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "log config: %v\n", err)
		return 2
	}
	log := malcomlog.New(logOpts).With("module", "index")

	idx, err := snapshotimport.BuildIndex(*snapshotDir, log)
	if err != nil {
		log.Error("build index failed", "err", err)
		return 1
	}

	// Print the per-store table to stdout — separate from the
	// structured event log so it's pipeable / greppable.
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "STORE\tITEMS\tIAVL_BYTES\tDECOMP_START\tDECOMP_END\tSPAN")
	for _, s := range idx.Stores {
		span := s.DecompressedEnd - s.DecompressedStart
		fmt.Fprintf(tw, "%s\t%d\t%s\t%d\t%d\t%s\n",
			s.Name, s.ItemCount, humanBytes(uint64(s.IAVLBytes)),
			s.DecompressedStart, s.DecompressedEnd, humanBytes(uint64(span)))
	}
	tw.Flush()
	fmt.Println()
	fmt.Printf("total: stores=%d  items=%d  bytes=%s  build=%s  rate=%.1f MiB/s\n",
		len(idx.Stores), idx.TotalItems, humanBytes(uint64(idx.TotalBytes)),
		idx.BuildElapsed.Truncate(time.Millisecond), idx.BuildBytesRate)

	return 0
}

// humanBytes formats binary units. Local copy rather than pulling the
// internal/humanbytes package — the index subcommand is otherwise
// dependency-free.
func humanBytes(n uint64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	div, exp := uint64(unit), 0
	for n2 := n / unit; n2 >= unit; n2 /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f%ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
