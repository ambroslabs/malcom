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
	"fmt"
	"log"
	"time"

	"github.com/cockroachdb/pebble"
)

func main() {
	dir := flag.String("dir", "", "path to pebble DB directory")
	flag.Parse()
	if *dir == "" {
		flag.Usage()
		return
	}

	t0 := time.Now()
	db, err := pebble.Open(*dir, &pebble.Options{
		// Allow lots of parallel compaction work.
		MaxConcurrentCompactions: func() int { return 8 },
	})
	if err != nil {
		log.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := db.Flush(); err != nil {
		log.Fatalf("flush: %v", err)
	}
	fmt.Printf("[compact] flush done in %s\n", time.Since(t0).Truncate(time.Millisecond))

	t1 := time.Now()
	// Full keyspace: pebble keys are arbitrary bytes; spanning [0x00, 0xff*8) is enough
	// for any real-world DB (cosmos-sdk store keys are ASCII-prefixed and well below this).
	start := []byte{0x00}
	end := []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	if err := db.Compact(start, end, true); err != nil {
		log.Fatalf("compact: %v", err)
	}
	fmt.Printf("[compact] full-range compact done in %s\n", time.Since(t1).Truncate(time.Millisecond))

	m := db.Metrics()
	fmt.Printf("[compact] post-compact LSM:\n")
	for i, l := range m.Levels {
		if l.NumFiles > 0 || l.Size > 0 {
			fmt.Printf("  L%d: files=%d size=%s\n", i, l.NumFiles, humanBytes(uint64(l.Size)))
		}
	}
	fmt.Printf("[compact] total time %s\n", time.Since(t0).Truncate(time.Millisecond))
}

func humanBytes(n uint64) string {
	const (
		KiB = 1024
		MiB = 1024 * KiB
		GiB = 1024 * MiB
	)
	switch {
	case n >= GiB:
		return fmt.Sprintf("%.2f GiB", float64(n)/GiB)
	case n >= MiB:
		return fmt.Sprintf("%.2f MiB", float64(n)/MiB)
	case n >= KiB:
		return fmt.Sprintf("%.2f KiB", float64(n)/KiB)
	}
	return fmt.Sprintf("%d B", n)
}
