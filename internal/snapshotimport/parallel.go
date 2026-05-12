// ImportParallel: a stage-1 (decompress + index + tee to disk) /
// stage-2 (parallel per-store workers) variant of the importer.
//
// Stream phase wall is dominated by the longest single-store IAVL +
// pebble work (bank: ~3min on cosmoshub-4). The serial path can't
// run other stores' workers concurrently because the wire format
// is store-sequential. ImportParallel breaks that bottleneck by:
//
//  1. Stage 1: decompress the snapshot once into a temp file on
//     disk, recording per-store byte offsets in an index. ~70s wall
//     for cosmoshub-4 (paced by zlib decompress on the prefetch
//     goroutine).
//
//  2. Stage 2: spawn N≤Workers worker goroutines. Each worker pops
//     a store from the work queue (sorted big-first), seeks to its
//     offset in the temp file, and runs the IAVL stack + ingest
//     against its own per-store batch + SSTable Writer. With N=4
//     and a 4-vCPU box, total wall ≈ max-store time = bank's ~3min.
//
//  3. Stage 3: extension items (cosmwasm bytecode, 08-light-client
//     wasm) are at the tail of the stream after the last store, and
//     they're not parallelizable — handled sequentially on the main
//     goroutine.
//
// Disk cost: temp file = decompressed snapshot size (~14 GiB for
// cosmoshub-4, ~100 GiB for babylon). Removed after Import returns.
//
// Memory cost: 1 batch (64 MiB) + 1 SSTable Writer block per active
// worker = ~256 MiB at N=4. The decompressed bytes themselves never
// live in memory — they stream to disk.

package snapshotimport

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"

	malcomlog "github.com/ambroslabs/malcom/internal/log"
)

// ParallelOptions extends Options with parallel-specific knobs.
type ParallelOptions struct {
	Options

	// Workers caps the number of concurrent store-processing
	// goroutines. Default = runtime.NumCPU(). Going above NumCPU
	// gives diminishing returns (stores are mostly CPU-bound), but
	// the bookkeeping is bounded so it doesn't break.
	Workers int

	// TempDir was the directory for decompressed.tmp under the old
	// disk-temp-file design. The pipeline now keeps decompressed
	// bytes in an in-memory chunk ring and never writes to disk;
	// the field is preserved for backward CLI compatibility but
	// has no effect.
	TempDir string

	// ChunkMB caps the in-memory chunk ring (the streaming buffer
	// between stage 1 decompression and stage 2 per-store readers).
	// 0 = use defaultChunkMB (512 MiB). Bigger values reduce stage-1
	// backpressure on multi-store-concurrent chains (cosmoshub bank+
	// ibc, osmosis cl/ibc/wasm) at the cost of more peak RSS. Single-
	// polestar chains (bbn finality) don't benefit from larger rings.
	ChunkMB int

	// FastIngest routes f/ (fast-storage) entries through pebble's
	// bulk-ingest path: per-store sstable.Writer → db.Ingest at end-
	// of-store. When false, f/ entries flow through the same
	// pebble.Batch as s/ entries (memtable + WAL + L0 build).
	FastIngest bool

	// WaveParallel turns on within-store wave-parallel hashing per
	// store: each per-store worker spawns a hash worker pool and
	// dispatcher (see importer_par.go) so the IAVL hash + encode
	// work overlaps across cores. Helps single-store-dominated
	// chains (bbn finality: -2 min vs the async-only baseline);
	// neutral on cosmoshub and slightly regresses osmosis (3 polestar
	// stores running concurrent oversubscribe the inner-worker pool).
	// Off by default; enable per chain via the `-wave-parallel` CLI
	// flag when the polestar runs solo.
	WaveParallel bool

	// DecompressedSource, when non-nil, is used in place of opening
	// SnapshotDir's chunk_NNNNN.bin files + zlib-decoding them. It must
	// yield the same byte stream that openChunkDir would (the
	// SnapshotItem wire format). Closed by ImportParallel before
	// returning.
	//
	// The pipelined-fetch orchestrator (`malcom snapshot fetch -import`)
	// passes a TailingChunkSource here so stage 1 can begin streaming
	// chunk 0 while later chunks are still in flight on the wire.
	// SnapshotDir is still required (used for ext payloads in stage 3
	// via the chunk ring, and by the .complete check unless
	// AllowIncomplete is set).
	DecompressedSource io.ReadCloser

	// AllowIncomplete skips the snapshot-dir .complete-marker check.
	// Set by the pipelined orchestrator: at the time ImportParallel
	// starts, fetch is mid-flight and the marker hasn't been written
	// yet. The marker is the user's "fetch finished cleanly" sentinel
	// — for the in-process pipeline the orchestrator itself owns that
	// invariant (it only finalizes after both sides return without
	// error), so the on-disk marker is redundant.
	AllowIncomplete bool
}

// ImportParallel is the parallel sibling of Import. Same output, same
// AppHash, same Stats shape — just faster on multi-core boxes for
// snapshots dominated by a few large stores.
func ImportParallel(opts ParallelOptions) (*Stats, error) {
	if opts.SnapshotDir == "" {
		return nil, fmt.Errorf("Options.SnapshotDir is required")
	}
	if opts.OutDir == "" {
		return nil, fmt.Errorf("Options.OutDir is required")
	}
	if opts.Height == 0 {
		return nil, fmt.Errorf("Options.Height is required")
	}
	if !opts.AllowIncomplete {
		if _, err := os.Stat(filepath.Join(opts.SnapshotDir, ".complete")); err != nil {
			if os.IsNotExist(err) {
				return nil, fmt.Errorf("snapshot dir %s is missing .complete marker", opts.SnapshotDir)
			}
			return nil, err
		}
	}
	log := opts.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	log = log.With("module", "import")

	workers := opts.Workers
	if workers <= 0 {
		workers = runtime.NumCPU()
		if workers < 1 {
			workers = 1
		}
	}

	// In-memory chunk ring replaces the on-disk decompressed.tmp.
	// Stage 1 streams decompressed bytes into the ring; stage 2
	// workers each create a reader at their store's start offset.
	// Readers' min-cursor pins eviction so old chunks free as the
	// slowest reader advances. The ring uses an internal free-list
	// so its memory budget (active chunks + free slabs) is bounded
	// at exactly chunkMB MiB regardless of allocation rate.
	const ringChunkSize = 4 << 20
	chunkMB := opts.ChunkMB
	if chunkMB <= 0 {
		chunkMB = defaultChunkMB
	}
	ringMaxBytes := int64(chunkMB) << 20
	if ringMaxBytes < int64(ringChunkSize) {
		ringMaxBytes = int64(ringChunkSize)
	}
	log.Info("chunk ring sized", "chunk_mb", chunkMB, "default", opts.ChunkMB <= 0)
	ring := newChunkRing(ringChunkSize, ringMaxBytes)

	memMB := opts.MemtableMB
	if memMB <= 0 {
		memMB = 256
	}
	cacheMB := opts.CacheMB
	if cacheMB <= 0 {
		cacheMB = 16
	}
	maxCompact := opts.MaxConcurrentCompactions
	if maxCompact <= 0 {
		maxCompact = 4
	}

	t0 := time.Now()

	if err := os.MkdirAll(opts.OutDir, 0o755); err != nil {
		return nil, fmt.Errorf("create out dir %s: %w", opts.OutDir, err)
	}
	appdbDir := filepath.Join(opts.OutDir, "application.db")
	extDir := ""
	if !opts.NoExtensions {
		extDir = filepath.Join(opts.OutDir, "extensions")
	}

	// ─── pebble open ─────────────────────────────────────────────────
	pebbleLog := malcomlog.PebbleShim(log.With("module", "pebble"))
	popts := &pebble.Options{
		MemTableSize:                uint64(memMB) << 20,
		MemTableStopWritesThreshold: 4,
		Cache:                       pebble.NewCache(int64(cacheMB) << 20),
		MaxOpenFiles:                4096,
		MaxConcurrentCompactions:    func() int { return maxCompact },
		Logger:                      pebbleLog,
		Levels: []pebble.LevelOptions{{
			Compression: pebble.NoCompression,
			// One L0 SST per memtable flush. Pebble's default L0
			// TargetFileSize is 2 MiB, which fragments every memtable
			// flush into hundreds of tiny SSTs (we measured ~2.5 MB
			// average across 71k files on a 140 GiB bbn import). Setting
			// to math.MaxInt64 disables the file-size splitter so each
			// flush writes one SST sized = memtable_mb. Combined with
			// FlushSplitBytes=MaxInt64 below, no splitter fires.
			TargetFileSize: math.MaxInt64,
		}},
	}
	if !opts.CompactDuringImport {
		// Bulk-load: skip auto compactions during the stream. The user
		// or the chain daemon runs compactions afterward.
		popts.DisableAutomaticCompactions = true
		popts.L0CompactionThreshold = 1024
		popts.L0StopWritesThreshold = 4096
		// FlushSplitBytes governs the OTHER memtable-flush splitter
		// (L0 sublevel boundary alignment). Pebble's default is 2 MiB
		// and EnsureDefaults silently replaces 0 with the default, so
		// pass a sentinel-large positive int to disable it.
		// opts.FlushSplitMB is ignored in bulk mode.
		popts.FlushSplitBytes = math.MaxInt64
	} else if opts.FlushSplitMB > 0 {
		popts.FlushSplitBytes = int64(opts.FlushSplitMB) << 20
	}
	db, err := pebble.Open(appdbDir, popts)
	if err != nil {
		return nil, fmt.Errorf("open pebble at %s: %w", appdbDir, err)
	}

	// ingest tmp dir for SSTable Writers (bulk-ingest path)
	ingestTmpDir := filepath.Join(opts.OutDir, "ingest-tmp")
	if err := os.MkdirAll(ingestTmpDir, 0o755); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("mkdir ingest tmp: %w", err)
	}
	defer func() { _ = os.RemoveAll(ingestTmpDir) }()

	stats := &Stats{}

	// ─── stages 1 + 2 run concurrently ───────────────────────────────
	//
	// Stage 1 emits a StoreEntry to storeCh as each store's full
	// byte range becomes known (= when the next StoreItem or the
	// extension tail is encountered). Stage 2 workers consume from
	// the channel as events arrive — bank can start at T~30s when
	// its end offset is identified, instead of waiting for stage 1
	// to finish at T~70s.
	stagesStart := time.Now()
	storeCh := make(chan StoreEntry, 64)
	type stage1Result struct {
		idx *SnapshotIndex
		err error
	}
	stage1Out := make(chan stage1Result, 1)

	// Pick the decompressed-stream source. Default: open SnapshotDir's
	// chunk files + zlib-decode in one shot. Pipelined-fetch caller
	// supplies a TailingChunkSource that opens chunks as they land.
	decompressed := opts.DecompressedSource
	if decompressed == nil {
		cr, err := openChunkDir(opts.SnapshotDir)
		if err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("open snapshot dir: %w", err)
		}
		decompressed = cr
	}
	go func() {
		idx, err := stage1WriteRingAndIndex(decompressed, ring, storeCh, log)
		close(storeCh)
		ring.Close(err)
		stage1Out <- stage1Result{idx: idx, err: err}
	}()

	// Stage 2 runs concurrently; returns when storeCh closes and
	// all in-flight workers drain.
	stage2Start := time.Now()
	stores, parStats, err := stage2RunWorkersFromChan(
		storeCh, ring, db, opts.Height, ingestTmpDir, log, workers, opts.WaveParallel, opts.FastIngest)
	if err != nil {
		_ = db.Close()
		// Drain stage 1 to surface its error too if it had one.
		if r := <-stage1Out; r.err != nil {
			return nil, fmt.Errorf("stage 2: %w (stage 1 also failed: %v)", err, r.err)
		}
		return nil, fmt.Errorf("stage 2: %w", err)
	}
	stage2Elapsed := time.Since(stage2Start).Truncate(time.Millisecond)

	r := <-stage1Out
	if r.err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("stage 1: %w", r.err)
	}
	idx := r.idx
	stage1Elapsed := time.Since(stagesStart).Truncate(time.Millisecond)
	log.Info("stage 1 complete",
		"elapsed", stage1Elapsed,
		"stores", len(idx.Stores),
		"items", idx.TotalItems,
		"bytes", uint64(idx.TotalBytes),
		"rate_mb_s", fmt.Sprintf("%.1f", idx.BuildBytesRate))

	stats.Items = parStats.Items
	stats.StreamDecodeElapsed = stage1Elapsed
	stats.StreamIAVLElapsed = parStats.StreamIAVLElapsed
	stats.StreamPebbleElapsed = parStats.StreamPebbleElapsed

	// Sort stores by name (cosmos rootmulti commit-info convention).
	sort.SliceStable(stores, func(i, j int) bool { return stores[i].Name < stores[j].Name })
	stats.Stores = stores
	stats.StreamElapsed = time.Since(stagesStart).Truncate(time.Millisecond)
	log.Info("stage 2 complete", "elapsed", stage2Elapsed, "workers", workers)

	// ─── stage 3: extensions (sequential) ────────────────────────────
	if !opts.NoExtensions && idx.ExtStart > 0 {
		stage3Start := time.Now()
		if err := stage3ExtensionsFromRing(ring, idx.ExtStart, idx.TotalBytes, db, extDir, stats, log); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("stage 3: %w", err)
		}
		log.Info("stage 3 complete",
			"elapsed", time.Since(stage3Start).Truncate(time.Millisecond),
			"extensions", stats.Extensions,
			"payloads", stats.ExtensionPayloads)
	}

	// ─── write rootmulti commit-info + latest-version ────────────────
	{
		batch := db.NewBatch()
		if err := batch.Set(commitInfoKey(opts.Height), commitInfoBytes(opts.Height, stores), nil); err != nil {
			_ = batch.Close()
			_ = db.Close()
			return nil, fmt.Errorf("write commit-info: %w", err)
		}
		if err := batch.Set(latestVersionKey, latestVersionBytes(opts.Height), nil); err != nil {
			_ = batch.Close()
			_ = db.Close()
			return nil, fmt.Errorf("write latest-version: %w", err)
		}
		if err := batch.Commit(pebble.Sync); err != nil {
			_ = batch.Close()
			_ = db.Close()
			return nil, fmt.Errorf("commit final batch: %w", err)
		}
		batch.Close()
	}

	// Flush any remaining memtable data so the closed DB is durable.
	// We don't run an explicit compact — the user runs
	// `malcom compact -dir <appdb>` after, or lets the daemon's pebble
	// auto-compact at runtime. Skipping the upfront compact saves
	// ~2m wall on cosmoshub-4.
	if err := db.Flush(); err != nil {
		log.Warn("pre-close flush failed", "err", err)
	}
	if err := db.Close(); err != nil {
		log.Warn("close db failed", "err", err)
	}

	stats.Elapsed = time.Since(t0).Truncate(time.Millisecond)
	log.Info("import complete",
		"elapsed", stats.Elapsed, "stores", len(stores), "output", appdbDir,
		"goroutines", runtime.NumGoroutine())

	return stats, nil
}

// stage1WriteRingAndIndex consumes the decompressed SnapshotItem
// stream from `decompressed`, writing those bytes to the in-memory
// chunkRing while building the per-store offset index AND emitting
// StoreEntry events to storeCh.
//
// `decompressed` is closed before this function returns (success or
// error). The caller passes either a chunkReader (snapshot-dir +
// zlib) for the standard one-shot import or a TailingChunkSource for
// the pipelined `fetch -import` mode where chunks are still in flight.
//
// Each StoreEntry is emitted as soon as the store OPENS (its
// DecompressedStart is known), with an EndCh single-shot channel
// the consumer reads to learn the store's end offset (= when the
// next StoreItem or the extension tail is parsed). This lets stage
// 2 workers start processing the polestar (e.g., bbn finality) the
// moment its bytes start flowing into the ring, pipelining stage 1
// decompression with stage 2 hashing. Previously stage 2 had to
// wait for stage 1 to scan past the polestar to learn its End.
//
// Caller closes storeCh after this function returns and calls
// ring.Close to signal EOF.
func stage1WriteRingAndIndex(
	decompressed io.ReadCloser,
	ring *chunkRing,
	storeCh chan<- StoreEntry,
	log *slog.Logger,
) (*SnapshotIndex, error) {
	defer decompressed.Close()

	pf := newPrefetchReader(context.Background(), decompressed, 8, 1<<20)
	defer pf.Close()

	teeR := io.TeeReader(pf, ring)

	// Per-store EndCh map so onClose can signal the matching open
	// emission. Closed (without value) on stage-1 error so worker
	// goroutines waiting on receive don't leak.
	ends := make(map[string]chan int64)
	defer func() {
		for _, ch := range ends {
			close(ch)
		}
	}()

	onOpen := func(s StoreEntry) error {
		s.EndCh = make(chan int64, 1)
		ends[s.Name] = s.EndCh
		// Pin the ring's eviction at this store's DecompressedStart
		// by creating the reader NOW, while stage 1 has just
		// produced the bytes at that offset. If we let the worker
		// create the reader after pulling from storeCh, other readers
		// could advance their cursors past start in the meantime,
		// causing the start chunk to be evicted before this store's
		// worker arrives. NewReader's start-below-head precondition
		// surfaces the race (#124) instead of letting a bad reader
		// blow up at first Read deep inside the worker.
		rdr, err := ring.NewReader(s.DecompressedStart, -1)
		if err != nil {
			return fmt.Errorf("pin reader for store %q: %w", s.Name, err)
		}
		s.reader = rdr
		storeCh <- s
		return nil
	}
	onClose := func(name string, end int64) error {
		ch, ok := ends[name]
		if !ok {
			return fmt.Errorf("internal: onClose for unknown store %q", name)
		}
		ch <- end
		close(ch)
		delete(ends, name)
		return nil
	}

	idx, err := buildIndexFromReader(teeR, onOpen, onClose, log)
	if err != nil {
		return nil, err
	}
	return idx, nil
}

// buildIndexFromReader scans the stream, builds the per-store
// index, and emits open/close events as it goes:
//
//   - onOpen(StoreEntry) fires the moment a StoreItem is encountered
//     (DecompressedStart known; DecompressedEnd unknown).
//   - onClose(name, end) fires when the next StoreItem or the
//     extension tail is encountered (the store's last byte offset is
//     `end`).
//
// Either callback may be nil. Returning an error from either aborts
// the scan. Stores are emitted in stream order. The returned
// SnapshotIndex carries fully-populated DecompressedStart/End for
// every store so the caller can use it as a tabular view.
func buildIndexFromReader(
	r io.Reader,
	onOpen func(StoreEntry) error,
	onClose func(name string, end int64) error,
	log *slog.Logger,
) (*SnapshotIndex, error) {
	t0 := time.Now()
	scanner := newIndexScanner(r)
	idx := &SnapshotIndex{}
	var (
		curStore  *StoreEntry
		streamEnd int64
	)
	closeCur := func(end int64) error {
		if curStore == nil {
			return nil
		}
		curStore.DecompressedEnd = end
		name := curStore.Name
		curStore = nil
		if onClose != nil {
			return onClose(name, end)
		}
		return nil
	}
	for {
		envStart := scanner.pos
		envLen, ok, err := scanner.readUvarint()
		if err != nil {
			return nil, fmt.Errorf("read envelope length at offset %d: %w", envStart, err)
		}
		if !ok {
			streamEnd = envStart
			break
		}
		if envLen == 0 {
			return nil, fmt.Errorf("zero-length envelope at offset %d", envStart)
		}
		tag, err := scanner.peekByte()
		if err != nil {
			return nil, fmt.Errorf("peek tag at offset %d: %w", scanner.pos, err)
		}
		field := tag >> 3

		switch itemType(field) {
		case itemTypeStore:
			name, err := scanner.readStoreName(envLen)
			if err != nil {
				return nil, fmt.Errorf("read store name at offset %d: %w", envStart, err)
			}
			if err := closeCur(envStart); err != nil {
				return nil, err
			}
			idx.Stores = append(idx.Stores, StoreEntry{
				Name:              name,
				DecompressedStart: envStart,
				ItemCount:         1,
			})
			curStore = &idx.Stores[len(idx.Stores)-1]
			if onOpen != nil {
				if err := onOpen(*curStore); err != nil {
					return nil, err
				}
			}
		case itemTypeIAVL:
			if err := scanner.discard(int64(envLen)); err != nil {
				return nil, fmt.Errorf("discard IAVL body at offset %d: %w", envStart, err)
			}
			if curStore != nil {
				curStore.IAVLBytes += int64(envLen)
				curStore.ItemCount++
			}
		case itemTypeExtMeta, itemTypeExtPayload:
			if err := closeCur(envStart); err != nil {
				return nil, err
			}
			if idx.ExtStart == 0 {
				idx.ExtStart = envStart
			}
			if err := scanner.discard(int64(envLen)); err != nil {
				return nil, fmt.Errorf("discard ext body at offset %d: %w", envStart, err)
			}
		default:
			return nil, fmt.Errorf("unknown item field %d at offset %d", field, envStart)
		}
		idx.TotalItems++
	}
	if err := closeCur(streamEnd); err != nil {
		return nil, err
	}
	idx.TotalBytes = streamEnd
	idx.BuildElapsed = time.Since(t0)
	if idx.BuildElapsed > 0 {
		idx.BuildBytesRate = float64(idx.TotalBytes) / idx.BuildElapsed.Seconds() / (1 << 20)
	}
	sort.SliceStable(idx.Stores, func(i, j int) bool {
		return idx.Stores[i].IAVLBytes > idx.Stores[j].IAVLBytes
	})
	return idx, nil
}

// stage2RunWorkersFromChan consumes StoreEntry events from storeCh
// (sent by stage 1) and dispatches them to a fixed-size worker
// pool. Each worker creates a chunkRingReader at the store's
// DecompressedStart and reads sequentially via the ring (which
// shares decompressed bytes across all readers and evicts behind
// the slowest cursor). Returns when storeCh closes and all
// in-flight workers drain.
//
// Stores arrive in stream order from stage 1 — bank typically lands
// at position 4 (after 08-wasm, acc, authz), so the longest-pole
// store starts processing well before stage 1 finishes.
func stage2RunWorkersFromChan(
	storeCh <-chan StoreEntry, ring *chunkRing, db *pebble.DB, height int64,
	ingestTmpDir string, log *slog.Logger, numWorkers int, waveParallel, fastIngest bool,
) ([]StoreInfo, *Stats, error) {

	type workerOut struct {
		info StoreInfo
		err  error
	}
	// Buffered enough for typical chains so a fast main goroutine
	// rarely blocks on receive, but we drain concurrently so workers
	// also never block on send. The previous code waited on the
	// WaitGroup before draining, which deadlocked on chains with more
	// stores than the channel buffer (e.g. dydx-mainnet-1's 40 stores
	// vs. a numWorkers*4 = 32 buffer).
	resultsCh := make(chan workerOut, numWorkers*4)

	stats := &Stats{}
	var statsMu sync.Mutex
	var iavlNanos, pebbleNanos int64
	var itemsTotal uint64

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			ing := newFastIngester(db, ingestTmpDir, log)
			defer ing.cleanup()

			for store := range storeCh {
				info, items, ivl, pbl, err := processStoreSegment(
					ring, store, db, height, ing, log, waveParallel, fastIngest)
				if err != nil {
					resultsCh <- workerOut{err: fmt.Errorf("store %q: %w", store.Name, err)}
					continue
				}
				atomic.AddInt64(&iavlNanos, ivl)
				atomic.AddInt64(&pebbleNanos, pbl)
				statsMu.Lock()
				itemsTotal += items
				statsMu.Unlock()
				resultsCh <- workerOut{info: info}
			}
		}(i)
	}
	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	var (
		stores   []StoreInfo
		firstErr error
	)
	for r := range resultsCh {
		if r.err != nil {
			if firstErr == nil {
				firstErr = r.err
				log.Error("worker error", "err", r.err)
			}
			continue
		}
		stores = append(stores, r.info)
	}
	if firstErr != nil {
		return nil, nil, firstErr
	}
	stats.Items = itemsTotal
	stats.StreamIAVLElapsed = time.Duration(iavlNanos)
	stats.StreamPebbleElapsed = time.Duration(pebbleNanos)
	return stores, stats, nil
}

// defaultParWorkers returns the per-store wave-parallel hash worker
// count. NumCPU is reasonable: when other stores in the outer pool
// are still running, the OS scheduler shares; when only one store
// remains (= the polestar at the tail of the run, e.g. babylon's
// finality), it can absorb the spare cores.
func defaultParWorkers() int {
	n := runtime.NumCPU()
	if n < 1 {
		return 1
	}
	return n
}

// asyncBatchWriter owns the pebble Commit work for one store. The
// main loop submits filled batches; this goroutine drains them and
// calls Commit + Close, freeing the main loop to keep building the
// next batch (and its iavl hash work) while the previous one is
// flushing. Buffered channel of 2 caps in-flight batches at 3
// (building + queued + committing) ≈ 192 MiB peak per store at the
// 64 MiB flush threshold.
//
// pebble.DB.Apply (called by Commit) serializes internally on a
// single mutex, so multiple goroutines calling Commit don't increase
// write throughput — the win is the overlap with the main loop's
// iavl/parse work, not concurrent writes.
type asyncBatchWriter struct {
	db          *pebble.DB
	ch          chan *pebble.Batch
	wg          sync.WaitGroup
	closeOnce   sync.Once
	err         atomic.Pointer[error]
	commitNanos atomic.Int64 // measured inside the writer goroutine
}

func newAsyncBatchWriter(db *pebble.DB) *asyncBatchWriter {
	w := &asyncBatchWriter{
		db: db,
		ch: make(chan *pebble.Batch, 2),
	}
	w.wg.Add(1)
	go w.run()
	return w
}

func (w *asyncBatchWriter) run() {
	defer w.wg.Done()
	for batch := range w.ch {
		t0 := time.Now()
		if err := batch.Commit(pebble.NoSync); err != nil {
			err = fmt.Errorf("async batch commit: %w", err)
			w.err.CompareAndSwap(nil, &err)
		}
		batch.Close()
		w.commitNanos.Add(time.Since(t0).Nanoseconds())
	}
}

// submit hands ownership of a filled batch to the writer goroutine.
// Returns the first earlier error if any; the batch is still queued
// (Close is the writer's responsibility).
func (w *asyncBatchWriter) submit(batch *pebble.Batch) error {
	w.ch <- batch
	if e := w.err.Load(); e != nil {
		return *e
	}
	return nil
}

// drain closes the channel and blocks until all queued batches commit.
// Idempotent — safe to call on error paths and again at end of normal
// flow.
func (w *asyncBatchWriter) drain() error {
	w.closeOnce.Do(func() { close(w.ch) })
	w.wg.Wait()
	if e := w.err.Load(); e != nil {
		return *e
	}
	return nil
}

// processStoreSegment runs the IAVL + ingest pipeline for one store
// segment of the temp file. Returns the store's root hash + per-bucket
// timings (for stats aggregation across workers).
func processStoreSegment(
	ring *chunkRing, store StoreEntry, db *pebble.DB, height int64,
	ing *fastIngester, log *slog.Logger, waveParallel, fastIngest bool,
) (StoreInfo, uint64, int64, int64, error) {

	var iavlNanos, pebbleNanos int64

	// store.reader was pre-created by stage 1's onOpen callback to
	// pin the ring's eviction at DecompressedStart; we just use it.
	// The fallback path (BuildIndex/non-streaming callers) opens a
	// reader on the fly using the populated DecompressedEnd.
	rdr := store.reader
	if rdr == nil {
		var err error
		rdr, err = ring.NewReader(store.DecompressedStart, store.DecompressedEnd)
		if err != nil {
			return StoreInfo{}, 0, 0, 0, fmt.Errorf("open reader for store %q: %w", store.Name, err)
		}
	}
	defer rdr.Close()
	if store.EndCh != nil {
		go func() {
			if end, ok := <-store.EndCh; ok {
				rdr.SetEnd(end)
			}
		}()
	}
	sr := newSnapReader(rdr)

	// Async batch commit pipeline: the main thread keeps building the
	// next batch while a writer goroutine commits the previous one.
	// Buffer of 2 caps in-flight batches at 3 (current + queued +
	// committing) ≈ 192 MiB peak per store. Pebble's DB.Apply
	// serializes internally, so multiple Commits don't race; the win is
	// overlap with iavl/parse work, not write parallelism.
	const flushBytes = 64 << 20
	bw := newAsyncBatchWriter(db)
	// drain is idempotent; the explicit call at line 1022 surfaces the
	// real error path. This defer just reaps the writer goroutine on
	// error returns — drain's own error has already been reported (or
	// will be in the caller's explicit drain).
	defer func() { _ = bw.drain() }()
	batch := db.NewBatch()
	batchBytes := 0
	flush := func() error {
		if batchBytes == 0 {
			return nil
		}
		err := bw.submit(batch)
		batch = db.NewBatch()
		batchBytes = 0
		return err
	}
	set := func(key, value []byte) error {
		if err := batch.Set(key, value, nil); err != nil {
			return err
		}
		batchBytes += len(key) + len(value)
		if batchBytes >= flushBytes {
			return flush()
		}
		return nil
	}
	// setFast routes f/ entries either to the per-store SSTable
	// bulk-ingest path (FastIngest) or to the regular pebble.Batch
	// (same path as s/ entries; pebble sorts at flush time so the
	// order workers emit doesn't matter). Output is bit-identical
	// either way.
	var setFast func(key, value []byte) error
	if fastIngest {
		setFast = func(key, value []byte) error { return ing.set(key, value) }
		if err := ing.openStore(store.Name); err != nil {
			_ = batch.Close()
			return StoreInfo{}, 0, 0, 0, fmt.Errorf("open fast SSTable: %w", err)
		}
	} else {
		setFast = set
	}

	si := newStoreImporter(store.Name, height)
	storeStart := time.Now()
	var items uint64

	if waveParallel {
		// Wave-parallel mode: spawn a writer goroutine that drains the
		// per-store writeQ into the batch, plus the storeImporter's hash
		// worker pool. The writer goroutine is the SOLE caller of `set`
		// during the streaming phase, so pebble.Batch's single-writer
		// requirement is satisfied even with N concurrent hash workers
		// pushing to writeQ.
		//
		// Decoder pipeline: a separate goroutine reads sr.Next() and
		// pushes decoded items to itemQ. The main loop reads from itemQ
		// instead of calling sr.Next() inline. Profiling showed sr.Next
		// at ~33% of the per-store main-goroutine CPU on bbn finality;
		// pulling that off the main goroutine lets the hash-worker pool
		// run closer to its throughput ceiling on a single-store-dominated
		// chain.
		parWorkers := defaultParWorkers()
		writeQ := make(chan writeEnt, 4096)
		writerDone := make(chan error, 1)
		go func() {
			var werr error
			for ent := range writeQ {
				if werr != nil {
					continue
				}
				if err := set(ent.key, ent.encoded); err != nil {
					werr = err
				}
			}
			writerDone <- werr
		}()
		si.enableWaveParallel(parWorkers, writeQ)

		const itemQBuf = 4096
		itemQ := make(chan *snapItem, itemQBuf)
		decErr := make(chan error, 1)
		go func() {
			defer close(itemQ)
			for {
				item, err := sr.Next()
				if err != nil {
					decErr <- err
					return
				}
				itemQ <- item
			}
		}()

		// Deferred teardown of the wave-parallel goroutine forest:
		// writer, dispatcher, NumCPU hash workers, decoder. Without
		// this, every error return from the wpLoop below leaked
		// 2+NumCPU goroutines and pinned the chunk ring's memory —
		// issue #108 C1.
		//
		// finishStreaming is idempotent (flips par.closed, broadcasts,
		// joins dispatcher + workers — Waits are no-ops the second
		// time). close(writeQ) is NOT idempotent, so the success path
		// sets writerJoined=true after running its explicit teardown
		// to stop the defer from double-closing. Draining itemQ wakes
		// a decoder that may have blocked pushing into a full buffer;
		// it then reaches its store's endLimit, gets io.EOF, and
		// exits via the existing close(itemQ) defer.
		//
		// Ordering: this defer is declared after `defer bw.drain()`
		// and `defer rdr.Close()` above, so it runs FIRST in the
		// LIFO chain — the wave-parallel writer drains into bw, and
		// the decoder reads from rdr, so both upstreams must still
		// be alive while we join them.
		writerJoined := false
		defer func() {
			_ = si.finishStreaming()
			if !writerJoined {
				close(writeQ)
				<-writerDone
			}
			for range itemQ {
			}
		}()

		first := true
	wpLoop:
		for item := range itemQ {
			switch item.Type {
			case itemTypeStore:
				if first {
					if item.StoreName != store.Name {
						_ = batch.Close()
						return StoreInfo{}, 0, 0, 0, fmt.Errorf(
							"expected StoreItem %q, got %q at offset %d",
							store.Name, item.StoreName, store.DecompressedStart)
					}
				} else {
					// Read past our store's last byte; the next store
					// is starting. Treat as end-of-store. (The reader's
					// SetEnd may not have been called yet by the EndCh
					// listener goroutine — the boundary item itself is
					// the authoritative signal.)
					break wpLoop
				}
			case itemTypeIAVL:
				if first {
					_ = batch.Close()
					return StoreInfo{}, 0, 0, 0, fmt.Errorf(
						"first item not StoreItem (got IAVL) for store %q at offset %d",
						store.Name, store.DecompressedStart)
				}
				if err := si.addNode(set, setFast,
					item.IAVLHeight, item.IAVLVersion,
					item.IAVLKey, item.IAVLValue); err != nil {
					_ = batch.Close()
					return StoreInfo{}, 0, 0, 0, fmt.Errorf("add node: %w", err)
				}
			case itemTypeExtMeta, itemTypeExtPayload:
				// Extension tail — last store has ended.
				break wpLoop
			default:
				_ = batch.Close()
				return StoreInfo{}, 0, 0, 0, fmt.Errorf(
					"unexpected item type %d in store segment", item.Type)
			}
			first = false
			items++
		}
		// At this point either itemQ closed (sr error/EOF) or we broke
		// out on a boundary item. Either way the store is done; we
		// don't drain decErr because the decoder may keep producing
		// for the next worker. Best-effort: surface decoder error if
		// already seen and not yet drained.
		select {
		case err := <-decErr:
			if err != io.EOF {
				_ = batch.Close()
				return StoreInfo{}, 0, 0, 0, fmt.Errorf("read item: %w", err)
			}
		default:
		}

		if err := si.finishStreaming(); err != nil {
			_ = batch.Close()
			return StoreInfo{}, 0, 0, 0, fmt.Errorf("finish streaming: %w", err)
		}
		close(writeQ)
		writerJoined = true
		if werr := <-writerDone; werr != nil {
			_ = batch.Close()
			return StoreInfo{}, 0, 0, 0, fmt.Errorf("writer goroutine: %w", werr)
		}
	} else {
		// Synchronous path: main thread does decode + addNode inline.
		// No hash worker pool, no writer goroutine — just async batch
		// commits via the existing asyncBatchWriter from PR #71.
		first := true
	syncLoop:
		for {
			item, err := sr.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				_ = batch.Close()
				return StoreInfo{}, 0, 0, 0, fmt.Errorf("read item: %w", err)
			}
			switch item.Type {
			case itemTypeStore:
				if first {
					if item.StoreName != store.Name {
						_ = batch.Close()
						return StoreInfo{}, 0, 0, 0, fmt.Errorf(
							"expected StoreItem %q, got %q at offset %d",
							store.Name, item.StoreName, store.DecompressedStart)
					}
				} else {
					// Boundary into next store — treat as end-of-store.
					// The reader's EndCh-driven SetEnd may not have
					// fired yet for tiny stores; the boundary item
					// itself is the authoritative signal.
					break syncLoop
				}
			case itemTypeIAVL:
				if first {
					_ = batch.Close()
					return StoreInfo{}, 0, 0, 0, fmt.Errorf(
						"first item not StoreItem (got IAVL) for store %q at offset %d",
						store.Name, store.DecompressedStart)
				}
				if err := si.addNode(set, setFast,
					item.IAVLHeight, item.IAVLVersion,
					item.IAVLKey, item.IAVLValue); err != nil {
					_ = batch.Close()
					return StoreInfo{}, 0, 0, 0, fmt.Errorf("add node: %w", err)
				}
			case itemTypeExtMeta, itemTypeExtPayload:
				// Extension tail — last store has ended.
				break syncLoop
			default:
				_ = batch.Close()
				return StoreInfo{}, 0, 0, 0, fmt.Errorf(
					"unexpected item type %d in store segment", item.Type)
			}
			first = false
			items++
		}
	}

	hash, err := si.finalize(set)
	if err != nil {
		_ = batch.Close()
		return StoreInfo{}, 0, 0, 0, fmt.Errorf("finalize: %w", err)
	}
	if err := flush(); err != nil {
		return StoreInfo{}, 0, 0, 0, fmt.Errorf("flush final: %w", err)
	}
	batch.Close()
	if err := bw.drain(); err != nil {
		return StoreInfo{}, 0, 0, 0, fmt.Errorf("drain async writer: %w", err)
	}
	if fastIngest {
		if err := ing.ingestStore(store.Name); err != nil {
			return StoreInfo{}, 0, 0, 0, fmt.Errorf("ingest fast: %w", err)
		}
	}

	storeWall := time.Since(storeStart).Nanoseconds()
	pebbleNanos = bw.commitNanos.Load()
	iavlNanos = storeWall - pebbleNanos
	if iavlNanos < 0 {
		iavlNanos = 0
	}

	log.Info("store complete",
		"store", store.Name,
		"items", si.itemCount,
		"leaves", si.leafCount,
		"inner", si.innerCount,
		"elapsed", time.Duration(storeWall).Truncate(time.Millisecond))

	return StoreInfo{Name: store.Name, Hash: hash}, items, iavlNanos, pebbleNanos, nil
}

// stage3ExtensionsFromRing reads the extension tail from the ring
// and runs the existing extensionWriter sequentially. Extensions
// are not parallelizable — they're a small fraction of total work
// (~100MB on cosmoshub-4) and live at the very end of the stream.
//
// Called after stage 1 has finished writing to the ring (closed) and
// stage 2 readers have all closed. The ring's eviction logic only
// fires when readers exist, so the ext bytes (which were never
// pinned by a stage-2 reader) are still in memory when stage 3
// allocates its reader.
func stage3ExtensionsFromRing(
	ring *chunkRing, extStart, extEnd int64,
	db *pebble.DB, extDir string, stats *Stats, log *slog.Logger,
) error {
	rdr, err := ring.NewReader(extStart, extEnd)
	if err != nil {
		return fmt.Errorf("open extension reader: %w", err)
	}
	defer rdr.Close()
	sr := newSnapReader(rdr)

	const flushBytes = 64 << 20
	batch := db.NewBatch()
	batchBytes := 0
	flush := func() error {
		if batchBytes == 0 {
			return nil
		}
		err := batch.Commit(pebble.NoSync)
		batch.Close()
		batch = db.NewBatch()
		batchBytes = 0
		return err
	}
	set := func(key, value []byte) error {
		if err := batch.Set(key, value, nil); err != nil {
			return err
		}
		batchBytes += len(key) + len(value)
		if batchBytes >= flushBytes {
			return flush()
		}
		return nil
	}
	_ = set // extensionWriter currently writes payloads to the
	// extensions/ directory directly, not via the pebble batch.
	// Kept here so adding metadata writes later is straightforward.

	extWriter := newExtensionWriter(extDir)
	for {
		item, err := sr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			_ = batch.Close()
			return fmt.Errorf("read ext item: %w", err)
		}
		switch item.Type {
		case itemTypeExtMeta:
			if err := extWriter.openMeta(item.ExtName, item.ExtFormat, stats, log); err != nil {
				_ = batch.Close()
				return err
			}
		case itemTypeExtPayload:
			if err := extWriter.writePayload(item.ExtPayload, stats); err != nil {
				_ = batch.Close()
				return err
			}
		default:
			_ = batch.Close()
			return fmt.Errorf("unexpected item type %d in extension tail", item.Type)
		}
	}
	if err := flush(); err != nil {
		return fmt.Errorf("flush extensions: %w", err)
	}
	batch.Close()
	return nil
}
