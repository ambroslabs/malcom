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
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"

	malcomlog "github.com/zrbecker/cosmos-p2p/internal/log"
)

// ParallelOptions extends Options with parallel-specific knobs.
type ParallelOptions struct {
	Options

	// Workers caps the number of concurrent store-processing
	// goroutines. Default = runtime.NumCPU(). Going above NumCPU
	// gives diminishing returns (stores are mostly CPU-bound), but
	// the bookkeeping is bounded so it doesn't break.
	Workers int

	// TempDir holds the decompressed-stream temp file. Default =
	// OutDir; the temp file is removed when ImportParallel returns.
	TempDir string
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
	if _, err := os.Stat(filepath.Join(opts.SnapshotDir, ".complete")); err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("snapshot dir %s is missing .complete marker", opts.SnapshotDir)
		}
		return nil, err
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

	tmpRoot := opts.TempDir
	if tmpRoot == "" {
		tmpRoot = opts.OutDir
	}
	if err := os.MkdirAll(tmpRoot, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir tmp root: %w", err)
	}
	tempPath := filepath.Join(tmpRoot, "decompressed.tmp")
	// Pre-create the temp file so stage-2 workers can open it before
	// stage 1's own os.Create runs. The file is truncated again by
	// stage 1's open (no-op since we just emptied it), then grows
	// as stage 1 streams decompressed bytes in.
	if f, err := os.Create(tempPath); err != nil {
		return nil, fmt.Errorf("create temp %s: %w", tempPath, err)
	} else {
		f.Close()
	}
	defer os.Remove(tempPath)

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
		Levels:                      []pebble.LevelOptions{{Compression: pebble.NoCompression}},
	}
	if !opts.CompactDuringImport {
		// Bulk-load: skip auto compactions during the stream. The user
		// or gaiad runs compactions afterward.
		popts.DisableAutomaticCompactions = true
		popts.L0CompactionThreshold = 1024
		popts.L0StopWritesThreshold = 4096
	}
	if opts.FlushSplitMB > 0 {
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
	defer os.RemoveAll(ingestTmpDir)

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
	go func() {
		idx, err := stage1WriteAndIndex(opts.SnapshotDir, tempPath, storeCh, log)
		close(storeCh)
		stage1Out <- stage1Result{idx: idx, err: err}
	}()

	// Stage 2 runs concurrently; returns when storeCh closes and
	// all in-flight workers drain.
	stage2Start := time.Now()
	stores, parStats, err := stage2RunWorkersFromChan(
		storeCh, tempPath, db, opts.Height, ingestTmpDir, log, workers)
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
		if err := stage3Extensions(tempPath, idx.ExtStart, idx.TotalBytes, db, extDir, stats, log); err != nil {
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
	// `malcom compact -dir <appdb>` after, or lets gaiad's pebble
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

// stage1WriteAndIndex decompresses the snapshot once, writing the
// decompressed bytes to tempPath while building the per-store offset
// index AND emitting StoreEntry events to storeCh as each store's
// full byte range becomes known. This lets stage 2 workers start
// processing bank (and other stores) as soon as their end offsets
// are identified, instead of waiting for the whole stream to be
// scanned.
//
// Caller closes storeCh after this function returns. Final flush +
// fsync of the temp file happen here so workers see a stable file
// before they read.
func stage1WriteAndIndex(
	snapshotDir, tempPath string,
	storeCh chan<- StoreEntry,
	log *slog.Logger,
) (*SnapshotIndex, error) {
	cr, err := openChunkDir(snapshotDir)
	if err != nil {
		return nil, fmt.Errorf("open snapshot dir: %w", err)
	}
	defer cr.Close()

	pf := newPrefetchReader(context.Background(), cr, 8, 1<<20)
	defer pf.Close()

	f, err := os.Create(tempPath)
	if err != nil {
		return nil, fmt.Errorf("create temp file %s: %w", tempPath, err)
	}
	defer f.Close()

	// Use bufio for the write side, flushed at every store boundary
	// so workers reading the temp file see a consistent end-of-store
	// position. Reads from the same fd would also see un-flushed
	// bytes via the page cache, but workers open a separate fd.
	bw := bufio.NewWriterSize(f, 4<<20)
	teeR := io.TeeReader(pf, bw)

	emit := func(s StoreEntry) error {
		// Flush so the worker that picks up this store sees all of
		// its bytes on disk via its independent fd.
		if err := bw.Flush(); err != nil {
			return fmt.Errorf("flush before emit %q: %w", s.Name, err)
		}
		storeCh <- s
		return nil
	}

	idx, err := buildIndexFromReader(teeR, emit, log)
	if err != nil {
		return nil, err
	}

	if err := bw.Flush(); err != nil {
		return nil, fmt.Errorf("flush temp: %w", err)
	}
	if err := f.Sync(); err != nil {
		return nil, fmt.Errorf("fsync temp: %w", err)
	}
	return idx, nil
}

// buildIndexFromReader scans the stream, builds the per-store index,
// and (when emit != nil) calls emit(store) as each store's full
// range becomes known — i.e., when the next StoreItem or the
// extension tail is encountered. emit returning an error aborts the
// scan.
//
// Stores are emitted in stream order. The returned SnapshotIndex
// (with .Stores re-sorted big-first) is for callers that want the
// full tabular view; the channel-based dispatch path doesn't depend
// on that sort.
func buildIndexFromReader(
	r io.Reader,
	emit func(StoreEntry) error,
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
		if emit != nil {
			if err := emit(*curStore); err != nil {
				return err
			}
		}
		curStore = nil
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
// pool. Each worker opens its own *os.File handle on tempPath and
// seeks to the store's offset, then runs the IAVL stack + ingest
// path. Returns when storeCh closes and all in-flight workers
// drain.
//
// Stores arrive in stream order from stage 1 — bank typically lands
// at position 4 (after 08-wasm, acc, authz), so the longest-pole
// store starts processing well before stage 1 finishes.
func stage2RunWorkersFromChan(
	storeCh <-chan StoreEntry, tempPath string, db *pebble.DB, height int64,
	ingestTmpDir string, log *slog.Logger, numWorkers int,
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
			file, err := os.Open(tempPath)
			if err != nil {
				resultsCh <- workerOut{err: fmt.Errorf("worker %d open temp: %w", wid, err)}
				return
			}
			defer file.Close()

			ing := newFastIngester(db, ingestTmpDir, log)
			defer ing.cleanup()

			for store := range storeCh {
				info, items, ivl, pbl, err := processStoreSegment(
					file, store, db, height, ing, log)
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

	var stores []StoreInfo
	for r := range resultsCh {
		if r.err != nil {
			return nil, nil, r.err
		}
		stores = append(stores, r.info)
	}
	stats.Items = itemsTotal
	stats.StreamIAVLElapsed = time.Duration(iavlNanos)
	stats.StreamPebbleElapsed = time.Duration(pebbleNanos)
	return stores, stats, nil
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
	file *os.File, store StoreEntry, db *pebble.DB, height int64,
	ing *fastIngester, log *slog.Logger,
) (StoreInfo, uint64, int64, int64, error) {

	var iavlNanos, pebbleNanos int64

	if _, err := file.Seek(store.DecompressedStart, io.SeekStart); err != nil {
		return StoreInfo{}, 0, 0, 0, fmt.Errorf("seek: %w", err)
	}
	span := store.DecompressedEnd - store.DecompressedStart
	r := io.LimitReader(file, span)
	br := bufio.NewReaderSize(r, 1<<20)
	sr := newSnapReader(br)

	// Async batch commit pipeline: the main thread keeps building the
	// next batch while a writer goroutine commits the previous one.
	// Buffer of 2 caps in-flight batches at 3 (current + queued +
	// committing) ≈ 192 MiB peak per store. Pebble's DB.Apply
	// serializes internally, so multiple Commits don't race; the win is
	// overlap with iavl/parse work, not write parallelism.
	const flushBytes = 64 << 20
	bw := newAsyncBatchWriter(db)
	defer bw.drain() // idempotent; ensures the writer goroutine is reaped on error returns
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
	setFast := func(key, value []byte) error {
		return ing.set(key, value)
	}

	if err := ing.openStore(store.Name); err != nil {
		_ = batch.Close()
		return StoreInfo{}, 0, 0, 0, fmt.Errorf("open fast SSTable: %w", err)
	}

	si := newStoreImporter(store.Name, height)
	storeStart := time.Now()
	var items uint64
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
			// First item must be the StoreItem marking this segment.
			if item.StoreName != store.Name {
				_ = batch.Close()
				return StoreInfo{}, 0, 0, 0, fmt.Errorf(
					"expected StoreItem %q, got %q at offset %d",
					store.Name, item.StoreName, store.DecompressedStart)
			}
		case itemTypeIAVL:
			if err := si.addNode(set, setFast,
				item.IAVLHeight, item.IAVLVersion,
				item.IAVLKey, item.IAVLValue); err != nil {
				_ = batch.Close()
				return StoreInfo{}, 0, 0, 0, fmt.Errorf("add node: %w", err)
			}
		default:
			_ = batch.Close()
			return StoreInfo{}, 0, 0, 0, fmt.Errorf(
				"unexpected item type %d in store segment", item.Type)
		}
		items++
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
	if err := ing.ingestStore(store.Name); err != nil {
		return StoreInfo{}, 0, 0, 0, fmt.Errorf("ingest fast: %w", err)
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

// stage3Extensions reads the extension tail of the temp file and
// runs the existing extensionWriter sequentially. Extensions are
// not parallelizable — they're a small fraction of total work
// (~100MB on cosmoshub-4) and live at the very end of the stream.
func stage3Extensions(
	tempPath string, extStart, extEnd int64,
	db *pebble.DB, extDir string, stats *Stats, log *slog.Logger,
) error {
	f, err := os.Open(tempPath)
	if err != nil {
		return fmt.Errorf("open temp for extensions: %w", err)
	}
	defer f.Close()
	if _, err := f.Seek(extStart, io.SeekStart); err != nil {
		return fmt.Errorf("seek to extensions: %w", err)
	}
	r := io.LimitReader(f, extEnd-extStart)
	br := bufio.NewReaderSize(r, 1<<20)
	sr := newSnapReader(br)

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
