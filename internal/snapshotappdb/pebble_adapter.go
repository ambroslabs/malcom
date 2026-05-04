package snapshotappdb

import (
	"errors"
	"fmt"
	"math"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	corestore "cosmossdk.io/core/store"
	"github.com/cockroachdb/pebble"
)

// PebbleProfile bundles the bulk-load knobs that scale with available
// host RAM. The defaults in this package target a 64 GiB+ host; lower
// memory machines need this dialed down to avoid OOM or — worse — GC
// thrash where the heap stays pinned at 99% and throughput collapses
// from ~1M items/s to ~10/s.
//
// Set via SetPebbleProfile or pass auto-detected via AutoProfile.
type PebbleProfile struct {
	Name string // for logging

	// MemTableSize is the size of each in-memory write buffer. Pebble
	// caps this below 4 GiB; we cap at 2 GiB defensively.
	MemTableSize uint64

	// MemTableStopWrites is the in-flight memtable count after which
	// pebble blocks new writes. Combined with MemTableSize this
	// determines the max RAM held in memtables.
	MemTableStopWrites int

	// Cache is the pebble block cache size in bytes.
	Cache int64

	// MaxOpenFiles caps SST + WAL file descriptors held open.
	MaxOpenFiles int

	// MaxConcurrentCompactions limits parallel compactions (also gates
	// memtable flushes — keep >= 2).
	MaxConcurrentCompactions int

	// DisableAutomaticCompactions: when true, no L0+ compactions during
	// import — everything goes to L0 and we rely on a single big
	// FinalCompact at the end. Maximum throughput at the cost of peak
	// memory + disk amplification. Always false on memory-constrained
	// profiles.
	DisableAutomaticCompactions bool

	// L0CompactionThreshold / L0StopWritesThreshold tune backpressure
	// when DisableAutomaticCompactions=false. With AutomaticCompactions
	// on, we want generous L0 thresholds to avoid stalling the stream.
	L0CompactionThreshold int
	L0StopWritesThreshold int

	// Concurrency is the snapshotappdb store-worker concurrency. 1 =
	// serial per-store (lowest peak iavl frontier RAM); >1 lets
	// multiple stores import in parallel. 0 = auto via runtime.NumCPU()
	// capped at 8.
	Concurrency int
}

var (
	profileMu      sync.RWMutex
	activeProfile  *PebbleProfile
)

// AutoProfile returns a PebbleProfile sized for `availableMB` of host
// RAM. The aim is to cap pebble's structurally-pinned memory (memtable
// queue + cache) at ~30% of RAM, leaving headroom for iavl's mid-store
// frontier (5–10 GiB peak for cosmoshub bank), Go runtime overhead, OS
// page cache, and the streaming reorder buffer.
//
// Profile names match -pebble-profile flag values:
//
//	high  : 64 GiB+ — DisableAutomaticCompactions, max throughput
//	mid   : 24-64 GiB
//	low   : 12-24 GiB — automatic compactions on, concurrency=1
//	tiny  : 6-12 GiB — smallest profile that still finishes
//	(below 6 GiB this workload doesn't fit; consider snapshot-restore
//	 or smaller-state chains.)
func AutoProfile(availableMB int64) PebbleProfile {
	switch {
	case availableMB >= 56*1024:
		return PebbleProfile{
			Name:                        "high",
			MemTableSize:                1 << 30, // 1 GiB
			MemTableStopWrites:          8,
			Cache:                       2 << 30, // 2 GiB
			MaxOpenFiles:                4096,
			MaxConcurrentCompactions:    4,
			DisableAutomaticCompactions: true,
			L0CompactionThreshold:       math.MaxInt32,
			L0StopWritesThreshold:       math.MaxInt32,
			Concurrency:                 0, // auto
		}
	case availableMB >= 24*1024:
		return PebbleProfile{
			Name:                        "mid",
			MemTableSize:                512 << 20, // 512 MiB
			MemTableStopWrites:          4,
			Cache:                       1 << 30, // 1 GiB
			MaxOpenFiles:                4096,
			MaxConcurrentCompactions:    4,
			DisableAutomaticCompactions: true,
			L0CompactionThreshold:       math.MaxInt32,
			L0StopWritesThreshold:       math.MaxInt32,
			Concurrency:                 0,
		}
	case availableMB >= 12*1024:
		return PebbleProfile{
			Name:                        "low",
			MemTableSize:                256 << 20, // 256 MiB
			MemTableStopWrites:          2,
			Cache:                       256 << 20, // 256 MiB
			MaxOpenFiles:                2048,
			MaxConcurrentCompactions:    2,
			DisableAutomaticCompactions: false, // let pebble flush L0 during import
			L0CompactionThreshold:       8,
			L0StopWritesThreshold:       24,
			Concurrency:                 1, // sequential per-store: iavl frontier doesn't stack
		}
	default:
		return PebbleProfile{
			Name:                        "tiny",
			MemTableSize:                128 << 20, // 128 MiB
			MemTableStopWrites:          2,
			Cache:                       128 << 20, // 128 MiB
			MaxOpenFiles:                1024,
			MaxConcurrentCompactions:    2,
			DisableAutomaticCompactions: false,
			L0CompactionThreshold:       4,
			L0StopWritesThreshold:       16,
			Concurrency:                 1,
		}
	}
}

// ApplyMemoryLimit calls debug.SetMemoryLimit with a fraction of host
// RAM, telling Go's GC pacer to GC harder as the heap approaches that
// budget. This is a SOFT limit — Go can exceed it briefly under
// burst allocation, but it dramatically reduces the chance of OOM
// when paired with a sensible PebbleProfile and bounded reorder
// buffer.
//
// Defaults are tuned for the snapshot-import workload: 75% of host
// MemAvailable. The remaining 25% covers OS page cache for the
// pebble SST mmap'd files, the kernel itself, and any concurrent
// processes (e.g. gaiad after import completes).
//
// Honours the GOMEMLIMIT env var if already set (Go runtime parses it
// before main() runs); only overrides when GOMEMLIMIT is unset, which
// is the common case for our binaries.
//
// pct should be in (0, 1]. 0 or negative disables (returns the
// previous limit unchanged).
func ApplyMemoryLimit(pct float64) (limitBytes int64) {
	if pct <= 0 || pct > 1 {
		return debug.SetMemoryLimit(-1) // -1 returns current without changing
	}
	if os.Getenv("GOMEMLIMIT") != "" {
		// Respect operator's explicit override.
		return debug.SetMemoryLimit(-1)
	}
	mb := detectAvailableMB()
	if mb == 0 {
		return debug.SetMemoryLimit(-1)
	}
	bytes := int64(float64(mb*1024*1024) * pct)
	debug.SetMemoryLimit(bytes)
	fmt.Printf("[appdb] GOMEMLIMIT auto-set to %dMiB (%.0f%% of %dMiB available)\n",
		bytes/(1<<20), pct*100, mb)
	return bytes
}

// SetPebbleProfile overrides the auto-detected profile. Callers that
// know the host's RAM budget should invoke this before Import /
// ImportStream. The profile applies to subsequent openPebbleDB calls.
func SetPebbleProfile(p PebbleProfile) {
	profileMu.Lock()
	activeProfile = &p
	profileMu.Unlock()
}

// CurrentPebbleProfile returns the active profile, lazily initialising
// it via AutoProfile() if SetPebbleProfile has not been called.
func CurrentPebbleProfile() PebbleProfile {
	profileMu.RLock()
	if activeProfile != nil {
		p := *activeProfile
		profileMu.RUnlock()
		return p
	}
	profileMu.RUnlock()

	profileMu.Lock()
	defer profileMu.Unlock()
	if activeProfile == nil {
		p := AutoProfile(detectAvailableMB())
		activeProfile = &p
	}
	return *activeProfile
}

// detectAvailableMB returns OS-reported MemAvailable in MiB. Returns 0
// on error so callers fall through to the safest profile.
func detectAvailableMB() int64 {
	// runtime.MemStats doesn't expose host memory; on Linux read
	// /proc/meminfo MemAvailable. On other OSes, fall back to NumCPU
	// heuristic (4 GiB per core, capped at 64 GiB). Reasonable in
	// practice for both dev laptops and DO droplets.
	if mb := readLinuxMemAvailableMB(); mb > 0 {
		return mb
	}
	mb := int64(runtime.NumCPU()) * 4 * 1024
	if mb > 64*1024 {
		mb = 64 * 1024
	}
	return mb
}

// pebbleDB wraps *pebble.DB so it satisfies iavl/db.DB and
// corestore.KVStoreWithBatch. Note: gaiad's "pebbledb" backend in
// cometbft-db uses the same pebble library and an analogous adapter,
// so a database we produce here is binary-compatible with what gaiad
// would read when configured with `db_backend = "pebbledb"`.
type pebbleDB struct {
	db *pebble.DB
}

// readLinuxMemAvailableMB parses /proc/meminfo's MemAvailable line.
// Returns 0 if not on Linux or parse fails.
func readLinuxMemAvailableMB() int64 {
	b, err := osReadFile("/proc/meminfo")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(b), "\n") {
		if !strings.HasPrefix(line, "MemAvailable:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return 0
		}
		var kb int64
		_, _ = fmt.Sscanf(fields[1], "%d", &kb)
		return kb / 1024
	}
	return 0
}

// indirection that tests can swap. Default reads via os.ReadFile.
var osReadFile = os.ReadFile

// openPebbleDB opens an application.db using the active PebbleProfile
// (CurrentPebbleProfile, optionally set via SetPebbleProfile). The
// profile fields determine memtable size, cache size, compaction
// behaviour, and the snapshotappdb store-worker concurrency.
func openPebbleDB(dir string) (*pebbleDB, error) {
	p := CurrentPebbleProfile()
	maxConcCompact := p.MaxConcurrentCompactions
	if maxConcCompact < 1 {
		maxConcCompact = 1
	}

	opts := &pebble.Options{
		MemTableSize:                p.MemTableSize,
		MemTableStopWritesThreshold: p.MemTableStopWrites,
		Cache:                       pebble.NewCache(p.Cache),
		MaxOpenFiles:                p.MaxOpenFiles,
		MaxConcurrentCompactions:    func() int { return maxConcCompact },
		DisableAutomaticCompactions: p.DisableAutomaticCompactions,
		L0CompactionThreshold:       p.L0CompactionThreshold,
		L0StopWritesThreshold:       p.L0StopWritesThreshold,
	}
	if p.L0CompactionThreshold == 0 {
		opts.L0CompactionThreshold = math.MaxInt32
	}
	if p.L0StopWritesThreshold == 0 {
		opts.L0StopWritesThreshold = math.MaxInt32
	}

	fmt.Printf("[appdb] pebble profile=%s memtable=%dMiB×%d cache=%dMiB max-conc-compact=%d auto-compact=%v concurrency=%d\n",
		p.Name,
		p.MemTableSize/(1<<20), p.MemTableStopWrites,
		p.Cache/(1<<20),
		maxConcCompact,
		!p.DisableAutomaticCompactions,
		p.Concurrency,
	)

	db, err := pebble.Open(dir, opts)
	if err != nil {
		return nil, err
	}
	return &pebbleDB{db: db}, nil
}

// FinalCompact flushes the memtable and runs a full-keyspace compaction.
// Call after bulk writes are complete and before Close. parallelize=true
// uses all available cores. Skips silently if the DB has no data.
//
// Polls db.Metrics() on a 15s ticker so the caller sees per-level file
// counts/sizes shrink in real time — pebble's Compact() is otherwise a
// silent multi-minute call.
func (p *pebbleDB) FinalCompact() error {
	return runPebbleCompactWithMetrics(p.db, "compact")
}

// PebbleCleanupCompact opens a pebble DB at dir with default options,
// flushes, runs a full-keyspace compaction, and closes. Use as a second
// pass after Import to reclaim slack — the in-process Compact during
// Import queues obsolete files for deletion but doesn't always finish
// the cleanup before Close, leaving 8-10 GB of orphaned SSTs on a
// cosmoshub appdb that disappear on the next Open. This function makes
// that reopen explicit.
func PebbleCleanupCompact(dir string) error {
	db, err := pebble.Open(dir, &pebble.Options{
		MaxConcurrentCompactions: func() int { return 8 },
	})
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	if err := runPebbleCompactWithMetrics(db, "cleanup"); err != nil {
		_ = db.Close()
		return err
	}
	return db.Close()
}

// runPebbleCompactWithMetrics flushes the memtable and runs a
// full-keyspace compaction on db. While Compact is running, a
// background goroutine polls db.Metrics() every 15s and prints
// per-level file counts/sizes plus in-progress compaction state.
func runPebbleCompactWithMetrics(db *pebble.DB, label string) error {
	if err := db.Flush(); err != nil {
		return fmt.Errorf("pebble flush: %w", err)
	}

	stopCh := make(chan struct{})
	pollerDone := make(chan struct{})
	go func() {
		defer close(pollerDone)
		printPebbleLSM(label+"-start", db.Metrics())
		t := time.NewTicker(15 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-stopCh:
				return
			case <-t.C:
				printPebbleLSM(label, db.Metrics())
			}
		}
	}()

	// Pebble's Compact is exclusive on end. Use the full byte range.
	start := []byte{0x00}
	end := []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	err := db.Compact(start, end, true)
	close(stopCh)
	<-pollerDone

	if err != nil {
		return fmt.Errorf("pebble compact: %w", err)
	}
	printPebbleLSM(label+"-done", db.Metrics())
	return nil
}

// printPebbleLSM emits a single line summarising per-level file counts
// and sizes, total size across all levels, and any in-progress
// compaction work. Intended for ~15s tick output during long compactions.
func printPebbleLSM(label string, m *pebble.Metrics) {
	var totalFiles int64
	var totalSize int64
	var parts []string
	for i, l := range m.Levels {
		if l.NumFiles > 0 || l.Size > 0 {
			parts = append(parts, fmt.Sprintf("L%d=%d/%s", i, l.NumFiles, HumanBytes(uint64(l.Size))))
			totalFiles += l.NumFiles
			totalSize += l.Size
		}
	}
	fmt.Printf("[appdb-%s] %s | total=%d/%s in_progress=%d (%s)\n",
		label, strings.Join(parts, " "),
		totalFiles, HumanBytes(uint64(totalSize)),
		m.Compact.NumInProgress, HumanBytes(uint64(m.Compact.InProgressBytes)))
}

func (p *pebbleDB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, errors.New("empty key")
	}
	val, closer, err := p.db.Get(key)
	if err == pebble.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	out := make([]byte, len(val))
	copy(out, val)
	closer.Close()
	return out, nil
}

func (p *pebbleDB) Has(key []byte) (bool, error) {
	if len(key) == 0 {
		return false, errors.New("empty key")
	}
	_, closer, err := p.db.Get(key)
	if err == pebble.ErrNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	closer.Close()
	return true, nil
}

func (p *pebbleDB) Set(key, value []byte) error {
	return p.db.Set(key, value, pebble.NoSync)
}

func (p *pebbleDB) SetSync(key, value []byte) error {
	return p.db.Set(key, value, pebble.Sync)
}

func (p *pebbleDB) Delete(key []byte) error {
	return p.db.Delete(key, pebble.NoSync)
}

func (p *pebbleDB) DeleteSync(key []byte) error {
	return p.db.Delete(key, pebble.Sync)
}

func (p *pebbleDB) Iterator(start, end []byte) (corestore.Iterator, error) {
	if start != nil && len(start) == 0 {
		return nil, errors.New("empty (non-nil) start")
	}
	if end != nil && len(end) == 0 {
		return nil, errors.New("empty (non-nil) end")
	}
	it, err := p.db.NewIter(&pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	})
	if err != nil {
		return nil, err
	}
	it.First()
	return &pebbleIter{it: it, start: start, end: end, reverse: false}, nil
}

func (p *pebbleDB) ReverseIterator(start, end []byte) (corestore.Iterator, error) {
	if start != nil && len(start) == 0 {
		return nil, errors.New("empty (non-nil) start")
	}
	if end != nil && len(end) == 0 {
		return nil, errors.New("empty (non-nil) end")
	}
	it, err := p.db.NewIter(&pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	})
	if err != nil {
		return nil, err
	}
	it.Last()
	return &pebbleIter{it: it, start: start, end: end, reverse: true}, nil
}

func (p *pebbleDB) Close() error {
	return p.db.Close()
}

func (p *pebbleDB) NewBatch() corestore.Batch {
	return &pebbleBatch{db: p.db, batch: p.db.NewBatch()}
}

func (p *pebbleDB) NewBatchWithSize(size int) corestore.Batch {
	return &pebbleBatch{db: p.db, batch: p.db.NewBatchWithSize(size)}
}

// Print is a debug helper required by iavl/db.DB. We don't need it.
func (p *pebbleDB) Print() error { return nil }

// Stats is required by iavl/db.DB. Return empty map.
func (p *pebbleDB) Stats() map[string]string { return map[string]string{} }

// ─── iterator ────────────────────────────────────────────────────────────

type pebbleIter struct {
	it      *pebble.Iterator
	start   []byte
	end     []byte
	reverse bool
}

func (i *pebbleIter) Domain() (start, end []byte) { return i.start, i.end }
func (i *pebbleIter) Valid() bool                 { return i.it.Valid() }

func (i *pebbleIter) Next() {
	if i.reverse {
		i.it.Prev()
	} else {
		i.it.Next()
	}
}

func (i *pebbleIter) Key() []byte {
	if !i.it.Valid() {
		panic("invalid iterator")
	}
	k := i.it.Key()
	out := make([]byte, len(k))
	copy(out, k)
	return out
}

func (i *pebbleIter) Value() []byte {
	if !i.it.Valid() {
		panic("invalid iterator")
	}
	v := i.it.Value()
	out := make([]byte, len(v))
	copy(out, v)
	return out
}

func (i *pebbleIter) Error() error { return i.it.Error() }

func (i *pebbleIter) Close() error { return i.it.Close() }

// ─── batch ────────────────────────────────────────────────────────────

type pebbleBatch struct {
	db    *pebble.DB
	batch *pebble.Batch
}

func (b *pebbleBatch) Set(key, value []byte) error {
	return b.batch.Set(key, value, nil)
}

func (b *pebbleBatch) Delete(key []byte) error {
	return b.batch.Delete(key, nil)
}

func (b *pebbleBatch) Write() error {
	if err := b.batch.Commit(pebble.NoSync); err != nil {
		return fmt.Errorf("pebble batch commit: %w", err)
	}
	return nil
}

func (b *pebbleBatch) WriteSync() error {
	if err := b.batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("pebble batch commit (sync): %w", err)
	}
	return nil
}

func (b *pebbleBatch) Close() error { return b.batch.Close() }

func (b *pebbleBatch) GetByteSize() (int, error) {
	return int(b.batch.Len()), nil
}
