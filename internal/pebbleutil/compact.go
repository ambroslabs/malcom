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
	"log/slog"
	"time"

	"github.com/cockroachdb/pebble"

	malcomlog "github.com/zrbecker/cosmos-p2p/internal/log"
)

// CleanupCompact opens a pebble DB at dir, flushes, runs a
// full-keyspace compaction, and closes. log may be nil for silent
// operation. maxConcurrent caps pebble's compaction goroutines —
// 0 falls back to 8 (a sensible default for typical multi-core
// boxes; compactions are largely I/O-bound at cosmos scale).
//
// cacheMB sizes the pebble block cache used during the compact —
// large is much better here because compaction iterators read the
// per-SSTable bloom filters and index blocks repeatedly. Default
// (cacheMB == 0) is 8 GiB. MaxOpenFiles is set to 200_000 so a
// post-bulk-load LSM with tens of thousands of L0 SSTables doesn't
// thrash the FD cache; the kernel ulimit is the real backstop.
func CleanupCompact(dir string, maxConcurrent, cacheMB int, log *slog.Logger) error {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	if maxConcurrent <= 0 {
		maxConcurrent = 8
	}
	if cacheMB <= 0 {
		cacheMB = 8 * 1024
	}
	cache := pebble.NewCache(int64(cacheMB) << 20)
	defer cache.Unref()
	db, err := pebble.Open(dir, &pebble.Options{
		MaxConcurrentCompactions: func() int { return maxConcurrent },
		Cache:                    cache,
		MaxOpenFiles:             200_000,
		Logger:                   malcomlog.PebbleShim(log.With("module", "pebble")),
	})
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	if err := compactWithMetrics(db, "cleanup", log); err != nil {
		_ = db.Close()
		return err
	}
	return db.Close()
}

// compactWithMetrics flushes the memtable and runs a full-keyspace
// compaction on db. While Compact is running, a background goroutine
// polls db.Metrics() every 15s and emits per-level file counts +
// sizes plus in-progress compaction state.
func compactWithMetrics(db *pebble.DB, label string, log *slog.Logger) error {
	if err := db.Flush(); err != nil {
		return fmt.Errorf("pebble flush: %w", err)
	}

	stopCh := make(chan struct{})
	pollerDone := make(chan struct{})
	go func() {
		defer close(pollerDone)
		emitLSM(log, label+"-start", db.Metrics())
		t := time.NewTicker(15 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-stopCh:
				return
			case <-t.C:
				emitLSM(log, label, db.Metrics())
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
	emitLSM(log, label+"-done", db.Metrics())
	return nil
}

// emitLSM logs a "lsm metrics" event with per-level file counts,
// sizes, totals, and in-progress compaction work as structured attrs.
// The pretty handler renders bytes via the "size"/"_bytes" key
// convention; JSON consumers get raw numbers.
func emitLSM(log *slog.Logger, label string, m *pebble.Metrics) {
	attrs := []any{"phase", label}
	var totalFiles int64
	var totalSize int64
	for i, l := range m.Levels {
		if l.NumFiles > 0 || l.Size > 0 {
			attrs = append(attrs,
				fmt.Sprintf("L%d_files", i), l.NumFiles,
				fmt.Sprintf("L%d_size", i), uint64(l.Size))
			totalFiles += l.NumFiles
			totalSize += l.Size
		}
	}
	attrs = append(attrs,
		"total_files", totalFiles,
		"total_size", uint64(totalSize),
		"compactions_in_progress", m.Compact.NumInProgress,
		"in_progress_bytes", uint64(m.Compact.InProgressBytes))
	log.Info("lsm metrics", attrs...)
}
