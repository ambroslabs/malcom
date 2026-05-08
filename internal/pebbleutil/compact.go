// Package pebbleutil provides post-import housekeeping for a pebble
// database: a full-keyspace compaction with periodic LSM metrics.
//
// CleanupCompact reopens an existing pebble dir, flushes any pending
// memtable, runs a single Compact() over the full keyspace, and closes.
// Use after a one-shot bulk-import that left the LSM in an unbalanced
// state (every memtable became its own L0 SST with no L0→L1 work
// done). Pebble sees the whole dataset at once and produces a tight
// LSM in a single pass.
//
// Also reclaims orphan SSTs left by the bulk-import close — pebble's
// async cleanup manager doesn't always drain before Close, leaving
// 8-10 GiB of slack on a fresh cosmoshub-4 application.db that
// disappears on the next Open via manifest replay. CleanupCompact
// makes that reopen explicit.
package pebbleutil

import (
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"

	"github.com/zrbecker/cosmos-p2p/internal/humanbytes"
)

// CleanupCompact opens a pebble DB at dir with default options,
// flushes, runs a full-keyspace compaction, and closes. See package
// docs for when to use it.
func CleanupCompact(dir string) error {
	db, err := pebble.Open(dir, &pebble.Options{
		MaxConcurrentCompactions: func() int { return 8 },
	})
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	if err := compactWithMetrics(db, "cleanup"); err != nil {
		_ = db.Close()
		return err
	}
	return db.Close()
}

// compactWithMetrics flushes the memtable and runs a full-keyspace
// compaction on db. While Compact is running, a background goroutine
// polls db.Metrics() every 15s and prints per-level file counts +
// sizes plus in-progress compaction state.
func compactWithMetrics(db *pebble.DB, label string) error {
	if err := db.Flush(); err != nil {
		return fmt.Errorf("pebble flush: %w", err)
	}

	stopCh := make(chan struct{})
	pollerDone := make(chan struct{})
	go func() {
		defer close(pollerDone)
		printLSM(label+"-start", db.Metrics())
		t := time.NewTicker(15 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-stopCh:
				return
			case <-t.C:
				printLSM(label, db.Metrics())
			}
		}
	}()

	start := []byte{0x00}
	end := []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	err := db.Compact(start, end, true)
	close(stopCh)
	<-pollerDone

	if err != nil {
		return fmt.Errorf("pebble compact: %w", err)
	}
	printLSM(label+"-done", db.Metrics())
	return nil
}

// printLSM emits a single line summarising per-level file counts,
// sizes, total size, and any in-progress compaction work.
func printLSM(label string, m *pebble.Metrics) {
	var totalFiles int64
	var totalSize int64
	var parts []string
	for i, l := range m.Levels {
		if l.NumFiles > 0 || l.Size > 0 {
			parts = append(parts, fmt.Sprintf("L%d=%d/%s", i, l.NumFiles, humanbytes.Format(uint64(l.Size))))
			totalFiles += l.NumFiles
			totalSize += l.Size
		}
	}
	fmt.Printf("[pebble-%s] %s | total=%d/%s in_progress=%d (%s)\n",
		label, strings.Join(parts, " "),
		totalFiles, humanbytes.Format(uint64(totalSize)),
		m.Compact.NumInProgress, humanbytes.Format(uint64(m.Compact.InProgressBytes)))
}
