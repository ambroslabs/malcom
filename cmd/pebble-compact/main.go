// pebble-compact runs a full-keyspace manual compaction over a pebble DB
// directory. Useful after a bulk-load (DisableAutomaticCompactions=true) to
// consolidate L0/L1 SSTs into the bottom level and reclaim slack — typical
// post-bulk-load DBs shrink 30-50% after a thorough compaction.
//
// Usage:
//
//	pebble-compact -dir <path-to-pebble-dir>
package main

import (
	"flag"
	"log"
	"time"

	"github.com/zrbecker/cosmos-p2p/internal/snapshotappdb"
)

func main() {
	dir := flag.String("dir", "", "path to pebble DB directory")
	flag.Parse()
	if *dir == "" {
		flag.Usage()
		return
	}

	t0 := time.Now()
	if err := snapshotappdb.PebbleCleanupCompact(*dir); err != nil {
		log.Fatalf("compact: %v", err)
	}
	log.Printf("[compact] total time %s", time.Since(t0).Truncate(time.Millisecond))
}
