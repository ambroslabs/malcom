// Package compact is the `malcom compact` subcommand: a full-keyspace
// pebble compaction pass to consolidate L0/L1 SSTs after a bulk-load
// (typical post-bulk-load DBs shrink 30-50% after one pass).
package compact

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/zrbecker/cosmos-p2p/internal/pebbleutil"
)

// Run is the malcom subcommand entry point.
func Run(args []string) int {
	fs := flag.NewFlagSet("malcom compact", flag.ContinueOnError)
	dir := fs.String("dir", "", "path to pebble DB directory (required)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *dir == "" {
		fs.Usage()
		return 2
	}

	t0 := time.Now()
	if err := pebbleutil.CleanupCompact(*dir); err != nil {
		fmt.Fprintf(os.Stderr, "compact: %v\n", err)
		return 1
	}
	fmt.Printf("[compact] total time %s\n", time.Since(t0).Truncate(time.Millisecond))
	return 0
}
