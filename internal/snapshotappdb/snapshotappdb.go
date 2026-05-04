// Package snapshotappdb imports a cosmos-sdk format-3 snapshot into an
// application.db that gaiad can read directly. This is the offline
// equivalent of state-sync's IAVL import phase: walk the snapshot stream,
// feed each store's items into iavl.Importer, write the resulting trees
// plus multistore CommitInfo into a single dbm.DB.
//
// What this writes:
//   - per-store IAVL nodes under prefix "s/k:<storename>/" (cosmos-sdk
//     rootmulti convention)
//   - CommitInfo at key "s/<version>"
//   - latest version pointer at key "s/latest"
//   - extension payloads (08-wasm, wasm) to extDir/<extname>/ if extDir
//     is non-empty
//
// What it does NOT write:
//   - state.db (cometbft consensus state) — separate tool, populated
//     from RPC
//   - blockstore.db — optional, can be empty or pruned post-bootstrap
package snapshotappdb

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	corestore "cosmossdk.io/core/store"
	"github.com/cosmos/iavl"
	idb "github.com/cosmos/iavl/db"
)

// Stats summarises an Import run.
type Stats struct {
	Stores            int
	Items             uint64
	Extensions        int
	ExtensionPayloads int
	BytesUncompressed uint64
}

// Backend selects the application.db on-disk format. The chosen backend
// must match what gaiad is configured to read (`db_backend` in app.toml).
//
//   - goleveldb (default): universally readable, no special build tags.
//   - pebbledb: faster bulk writes (~2-3x), but requires gaiad to be
//     built with -tags pebbledb and configured for `db_backend =
//     "pebbledb"`.
type Backend string

const (
	BackendGoLevel Backend = "goleveldb"
	BackendPebble  Backend = "pebbledb"
)

// Import reads the snapshot at snapshotDir and writes an application.db at
// outDir using the chosen backend, tagged at version=height. extDir (if
// non-empty) is the directory where extension payloads (e.g. wasm
// bytecode) will be written; pass "" to skip them.
//
// concurrency caps the number of stores allowed to have work in flight at
// once. The snapshot stream is itself serial (all of store A's items
// arrive before store B's), so reads are single-threaded; the parallel
// gain comes from pipelining store A's commit + final fast-batch flush
// behind store B's iavl.Importer.Add work. concurrency=0 picks
// min(NumCPU, 8); concurrency=1 effectively serializes (next store can't
// start until prev fully commits).
func Import(snapshotDir, outDir string, height int64, backend Backend, extDir string, concurrency int) (Stats, error) {
	var stats Stats

	if concurrency <= 0 {
		// Low-RAM profiles pin Concurrency=1; otherwise auto.
		if p := CurrentPebbleProfile(); p.Concurrency > 0 {
			concurrency = p.Concurrency
		} else {
			concurrency = runtime.NumCPU()
			if concurrency > 8 {
				concurrency = 8
			}
		}
	}

	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return stats, err
	}

	rootDB, err := openBackend(backend, outDir)
	if err != nil {
		return stats, fmt.Errorf("open output db: %w", err)
	}
	defer rootDB.Close()

	r, err := newSnapItemReader(snapshotDir)
	if err != nil {
		return stats, fmt.Errorf("open snapshot: %w", err)
	}
	defer r.Close()

	return runImportPipeline(context.Background(), rootDB, r, height, extDir, concurrency)
}

// firstWorkerError returns the first non-nil err on any worker that has
// finished. Used by the reader to surface a worker-side failure when
// the context is cancelled.
func firstWorkerError(workers []*storeWorker) error {
	for _, w := range workers {
		select {
		case <-w.done:
			if w.err != nil {
				return w.err
			}
		default:
		}
	}
	return fmt.Errorf("import cancelled (worker error)")
}

// storeWorker owns one store's iavl.Importer + fast-storage batch and
// runs a 3-stage internal pipeline:
//
//	reader → items   → iavlLoop      → leaves → fastWriteLoop → flushQ → flushLoop
//	                  (iavl.Add)              (writeFastNode +              (pebble
//	                                           batch.Set)                    Commit)
//
// Splitting the work across goroutines lets iavl.Add (heavy: hashing
// + node serialization + batch.Set on nodedb) overlap with
// writeFastNode (varint encoding + batch.Set on fast batch), and the
// occasional 16 MiB pebble flush no longer stalls the Add path. On a
// single-flow store like bank or wasm — where the snapshot stream
// gives no opportunity for inter-store overlap — this is the main
// place actual intra-store CPU concurrency comes from.
//
// Workers across stores are independent: each writes only under its
// own "s/k:<name>/" prefix in the shared rootDB. Pebble tolerates
// concurrent writers via internal locking, so multiple workers (and
// multiple flushLoops) can commit in parallel.
type storeWorker struct {
	name        string
	height      int64
	pref        idb.DB
	tree        *iavl.MutableTree
	imp         *iavl.Importer
	importStart time.Time
	totalItems  *uint64 // shared across workers, atomic

	items  chan *iavl.ExportNode  // reader → iavlLoop
	leaves chan *iavl.ExportNode  // iavlLoop → fastWriteLoop (leaves only)
	flushQ chan corestore.Batch   // fastWriteLoop → flushLoop

	done    chan struct{}
	err     error
	errOnce sync.Once

	itemsAdded uint64 // atomic
}

const (
	storeWorkerQueueSize = 1024 // reader → iavlLoop
	leafQueueSize        = 1024 // iavlLoop → fastWriteLoop
	flushQueueSize       = 4    // fastWriteLoop → flushLoop (16 MiB batches)
)

func newStoreWorker(rootDB rootStore, name string, height int64, importStart time.Time, totalItems *uint64) (*storeWorker, error) {
	pref := idb.NewPrefixDB(rootDB, storePrefix(name))
	tree := iavl.NewMutableTree(pref, 0, true, iavl.NewNopLogger())
	imp, err := tree.Import(height)
	if err != nil {
		return nil, fmt.Errorf("Import(%d) on store %q: %w", height, name, err)
	}
	return &storeWorker{
		name:        name,
		height:      height,
		pref:        pref,
		tree:        tree,
		imp:         imp,
		importStart: importStart,
		totalItems:  totalItems,
		items:       make(chan *iavl.ExportNode, storeWorkerQueueSize),
		leaves:      make(chan *iavl.ExportNode, leafQueueSize),
		flushQ:      make(chan corestore.Batch, flushQueueSize),
		done:        make(chan struct{}),
	}, nil
}

// setErr records the first error from any of the worker's goroutines.
// Subsequent errors are dropped — the first failure is the most
// informative cause.
func (w *storeWorker) setErr(err error) {
	w.errOnce.Do(func() { w.err = err })
}

// run starts the 3-stage pipeline and waits for all stages to drain.
// Stages exit in order: items closed → iavlLoop drains → leaves closed
// → fastWriteLoop drains → flushQ closed → flushLoop drains. Any stage
// that errors records via setErr and stops doing work; downstream
// stages still drain so they exit cleanly (avoids deadlocks on channel
// sends from upstream stages).
func (w *storeWorker) run() {
	defer close(w.done)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); w.fastWriteLoop() }()
	go func() { defer wg.Done(); w.flushLoop() }()

	// iavlLoop runs in this goroutine so its Commit + Close happen
	// before we report the commit-time line.
	w.iavlLoop()

	commitStart := time.Now()
	// imp.Commit finalises the IAVL nodedb writes (under the same
	// "s/k:<name>/" prefix as the fast-storage entries, but at
	// distinct sub-keys). Safe to run concurrently with the still-
	// draining fastWriteLoop / flushLoop.
	if err := w.imp.Commit(); err != nil {
		w.setErr(fmt.Errorf("commit store %q: %w", w.name, err))
	} else {
		w.imp.Close()
	}
	w.tree.Close()

	// Wait for fastWriteLoop + flushLoop to finish — they handle the
	// final fast batch including the storage_version metadata key.
	wg.Wait()

	if w.err == nil {
		fmt.Printf("[appdb] commit %s in %s (hash deferred, fast-storage written)\n",
			w.name, time.Since(commitStart).Truncate(time.Millisecond))
	}
}

// iavlLoop drains items from w.items, calls iavl.Importer.Add, and
// forwards leaves (Height==0) to fastWriteLoop. Inner nodes don't go
// to fast-storage — fast-storage is the flat-map of leaf_key →
// leaf_value. Closes w.leaves on exit so fastWriteLoop drains and
// terminates.
func (w *storeWorker) iavlLoop() {
	defer close(w.leaves)
	const reportInterval = 30 * time.Second
	var (
		lastReport        = time.Now()
		lastItemsAtReport uint64
	)
	for node := range w.items {
		if w.err != nil {
			// A downstream stage errored. Drain remaining items so
			// the reader doesn't deadlock on send, but don't do work.
			continue
		}
		if err := w.imp.Add(node); err != nil {
			w.setErr(fmt.Errorf("import node into %q: %w", w.name, err))
			continue
		}
		if node.Height == 0 {
			w.leaves <- node
		}
		nLocal := atomic.AddUint64(&w.itemsAdded, 1)
		nTotal := atomic.AddUint64(w.totalItems, 1)

		if time.Since(lastReport) >= reportInterval {
			now := time.Now()
			window := now.Sub(lastReport).Seconds()
			if window < 0.1 {
				window = 0.1
			}
			rate := float64(nLocal-lastItemsAtReport) / window
			fmt.Printf("[appdb] %s: %s items=%s total=%s rate=%.0f/s\n",
				formatElapsed(now.Sub(w.importStart)),
				w.name, formatCount(nLocal), formatCount(nTotal), rate)
			lastReport = now
			lastItemsAtReport = nLocal
		}
	}
}

// fastWriteLoop drains leaves, encodes each into the current fast-
// storage batch, and on the 16 MiB threshold hands the full batch to
// flushLoop. After w.leaves closes, sets the storage_version metadata
// key on the final batch and sends it. Closes w.flushQ on exit so
// flushLoop drains and terminates.
func (w *storeWorker) fastWriteLoop() {
	defer close(w.flushQ)
	const fastBatchFlushBytes = 16 << 20 // 16 MiB
	fbatch := w.pref.NewBatch()
	for node := range w.leaves {
		if w.err != nil {
			continue
		}
		if err := writeFastNode(fbatch, node); err != nil {
			w.setErr(fmt.Errorf("write fast-node into %q: %w", w.name, err))
			continue
		}
		if size, err := fbatch.GetByteSize(); err == nil && size > fastBatchFlushBytes {
			w.flushQ <- fbatch
			fbatch = w.pref.NewBatch()
		}
	}
	// Mark fast-storage as built for this store: 'm'+"storage_version"
	// = "1.1.0-<height>". iavl's shouldForceFastStorageUpgrade compares
	// versions[1] against the latest version on the tree (== height
	// for snapshot import), and IsUpgradeable also checks
	// hasUpgradedToFastStorage which returns true for versions
	// >= "1.1.0". With this metadata in place, gaiad's LoadVersion
	// sees fast-storage as already-built and skips the upgrade.
	if w.err == nil {
		metaKey := append([]byte{'m'}, []byte("storage_version")...)
		metaVal := []byte(fmt.Sprintf("1.1.0-%d", w.height))
		if err := fbatch.Set(metaKey, metaVal); err != nil {
			w.setErr(fmt.Errorf("set metadata for store %q: %w", w.name, err))
		}
	}
	w.flushQ <- fbatch
}

// flushLoop drains full batches from w.flushQ and commits each to
// pebble. Batches arrive in produce order; pebble.Batch.Commit is
// thread-safe across DBs but our own writes within one store are
// serialised through this single goroutine, so a downstream reader of
// fast-storage sees a consistent view.
func (w *storeWorker) flushLoop() {
	var (
		batchCount int
		totalDur   time.Duration
		maxDur     time.Duration
		lastReport = time.Now()
	)
	const reportInterval = 30 * time.Second
	for batch := range w.flushQ {
		if w.err != nil {
			_ = batch.Close()
			continue
		}
		t0 := time.Now()
		if err := batch.Write(); err != nil {
			w.setErr(fmt.Errorf("flush fast batch %q: %w", w.name, err))
		}
		d := time.Since(t0)
		batchCount++
		totalDur += d
		if d > maxDur {
			maxDur = d
		}
		if err := batch.Close(); err != nil && w.err == nil {
			w.setErr(fmt.Errorf("close fast batch %q: %w", w.name, err))
		}
		if time.Since(lastReport) >= reportInterval {
			fmt.Printf("[appdb] %s: pebble flushes=%d avg=%s max=%s\n",
				w.name, batchCount,
				(totalDur / time.Duration(max1(batchCount))).Truncate(time.Millisecond),
				maxDur.Truncate(time.Millisecond))
			batchCount = 0
			totalDur = 0
			maxDur = 0
			lastReport = time.Now()
		}
	}
	if batchCount > 0 {
		fmt.Printf("[appdb] %s: pebble flushes=%d avg=%s max=%s (final)\n",
			w.name, batchCount,
			(totalDur / time.Duration(max1(batchCount))).Truncate(time.Millisecond),
			maxDur.Truncate(time.Millisecond))
	}
}

func max1(n int) int {
	if n < 1 {
		return 1
	}
	return n
}

// runMemoryReporter periodically logs Go heap stats and (if rootDB is
// pebble-backed) pebble's memtable + cache + L0 file count. Lets us
// see during a stuck run whether RAM is going into iavl import
// frontier (Go heap), pebble memtable backlog, or pebble block cache.
func runMemoryReporter(ctx context.Context, rootDB rootStore) {
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		heap := ms.HeapAlloc
		sys := ms.Sys
		stk := ms.StackInuse
		// pebble metrics if available
		var pebbleStr string
		if pdb, ok := rootDB.(*pebbleDB); ok && pdb != nil && pdb.db != nil {
			m := pdb.db.Metrics()
			memSize := m.MemTable.Size
			memCount := m.MemTable.Count
			cacheSize := m.BlockCache.Size
			l0 := m.Levels[0]
			pebbleStr = fmt.Sprintf(" pebble=memtable:%dMiB×%d cache:%dMiB L0:%d files",
				memSize>>20, memCount, cacheSize>>20, l0.NumFiles)
		}
		fmt.Printf("[mem] go=heap:%dMiB sys:%dMiB stack:%dMiB GCs=%d%s\n",
			heap>>20, sys>>20, stk>>20, ms.NumGC, pebbleStr)
	}
}

// formatElapsed renders a duration as a compact "Hh Mm Ss" / "Mm Ss" / "Ss" string.
func formatElapsed(d time.Duration) string {
	s := int(d.Seconds())
	if s < 60 {
		return fmt.Sprintf("%ds", s)
	}
	if s < 3600 {
		return fmt.Sprintf("%dm%02ds", s/60, s%60)
	}
	return fmt.Sprintf("%dh%02dm%02ds", s/3600, (s%3600)/60, s%60)
}

// formatCount renders an integer with K/M/B suffix.
func formatCount(n uint64) string {
	switch {
	case n < 10_000:
		return fmt.Sprintf("%d", n)
	case n < 10_000_000:
		return fmt.Sprintf("%.1fK", float64(n)/1e3)
	case n < 10_000_000_000:
		return fmt.Sprintf("%.1fM", float64(n)/1e6)
	default:
		return fmt.Sprintf("%.2fB", float64(n)/1e9)
	}
}

// writeFastNode appends an iavl fast-storage entry for a leaf to batch.
// Encoding (matches iavl/nodedb):
//
//	key   = 'f' || leaf.Key
//	value = varint(version) || varint(len(value)) || value
func writeFastNode(batch corestore.Batch, node *iavl.ExportNode) error {
	fkey := make([]byte, 0, 1+len(node.Key))
	fkey = append(fkey, 'f')
	fkey = append(fkey, node.Key...)

	var fval bytes.Buffer
	var vbuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(vbuf[:], uint64(node.Version))
	fval.Write(vbuf[:n])
	n = binary.PutUvarint(vbuf[:], uint64(len(node.Value)))
	fval.Write(vbuf[:n])
	fval.Write(node.Value)

	return batch.Set(fkey, fval.Bytes())
}

// rootStore is the union of methods we need from the application.db
// handle: iavl/db.DB for IAVL trees plus SetSync for direct writes
// (CommitInfo + latest version pointer).
type rootStore interface {
	idb.DB
	Set(key, value []byte) error
	SetSync(key, value []byte) error
	Delete(key []byte) error
	DeleteSync(key []byte) error
	NewBatch() corestore.Batch
	NewBatchWithSize(size int) corestore.Batch
}

// openBackend opens an application.db at dir using the given backend.
//
// goleveldb produces "<dir>/application.db" (cometbft-db convention).
// pebbledb produces "<dir>/application.db" as a pebble directory (also
// cometbft-db convention; gaiad reads it identically).
func openBackend(b Backend, dir string) (rootStore, error) {
	switch b {
	case BackendGoLevel, "":
		return idb.NewGoLevelDB("application", dir)
	case BackendPebble:
		return openPebbleDB(filepath.Join(dir, "application.db"))
	default:
		return nil, fmt.Errorf("unsupported backend %q", b)
	}
}

// storePrefix is cosmos-sdk rootmulti's key namespace per substore:
// "s/k:<name>/". Everything the IAVL tree writes (nodes, root pointers,
// orphan markers) is prefixed with this.
func storePrefix(name string) []byte {
	return []byte("s/k:" + name + "/")
}

// ─── snapshot stream reader ──────────────────────────────────────────────

type snapItemReader struct {
	files []*os.File
	zr    io.ReadCloser
	br    *bufio.Reader

	cached    []byte
	cachedTag uint8
}

func newSnapItemReader(dir string) (*snapItemReader, error) {
	chunks, err := listChunkPaths(dir)
	if err != nil {
		return nil, err
	}
	files := make([]*os.File, 0, len(chunks))
	readers := make([]io.Reader, 0, len(chunks))
	for _, p := range chunks {
		f, err := os.Open(p)
		if err != nil {
			for _, x := range files {
				x.Close()
			}
			return nil, err
		}
		files = append(files, f)
		readers = append(readers, f)
	}
	zr, err := zlib.NewReader(io.MultiReader(readers...))
	if err != nil {
		for _, x := range files {
			x.Close()
		}
		return nil, err
	}
	return &snapItemReader{
		files: files,
		zr:    zr,
		br:    bufio.NewReaderSize(zr, 1<<20),
	}, nil
}

func (s *snapItemReader) Close() error {
	if s.zr != nil {
		s.zr.Close()
	}
	for _, f := range s.files {
		f.Close()
	}
	return nil
}

func (s *snapItemReader) peekItem() (tag uint8, body []byte, eof bool, err error) {
	if s.cached != nil {
		return s.cachedTag, s.cached, false, nil
	}
	length, err := binary.ReadUvarint(s.br)
	if err == io.EOF {
		return 0, nil, true, nil
	}
	if err != nil {
		return 0, nil, false, err
	}
	if length == 0 {
		return s.peekItem()
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(s.br, buf); err != nil {
		return 0, nil, false, err
	}
	t := buf[0] >> 3
	innerLen, n := binary.Uvarint(buf[1:])
	if n <= 0 || int(innerLen) > len(buf)-1-n {
		s.cached = buf
		s.cachedTag = t
		return t, buf, false, nil
	}
	body = buf[1+n : 1+n+int(innerLen)]
	s.cached = body
	s.cachedTag = t
	return t, body, false, nil
}

func (s *snapItemReader) consumeItem() {
	s.cached = nil
}

func listChunkPaths(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	type chunk struct {
		idx  int
		path string
	}
	var cs []chunk
	for _, e := range entries {
		n := e.Name()
		if !strings.HasPrefix(n, "chunk_") || !strings.HasSuffix(n, ".bin") {
			continue
		}
		idxStr := strings.TrimSuffix(strings.TrimPrefix(n, "chunk_"), ".bin")
		idx, err := strconv.Atoi(idxStr)
		if err != nil {
			continue
		}
		cs = append(cs, chunk{idx, filepath.Join(dir, n)})
	}
	sort.Slice(cs, func(i, j int) bool { return cs[i].idx < cs[j].idx })
	out := make([]string, len(cs))
	for i, c := range cs {
		out[i] = c.path
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no chunk_*.bin files in %s", dir)
	}
	return out, nil
}

// ─── proto field parsers (hand-rolled, no proto lib needed) ──────────────

func parseStringField(buf []byte, field int) []byte {
	return parseBytesField(buf, field)
}

func parseBytesField(buf []byte, field int) []byte {
	want := byte((field << 3) | 2)
	for i := 0; i < len(buf); {
		tag := buf[i]
		i++
		if tag == want {
			ln, m := binary.Uvarint(buf[i:])
			if m <= 0 || i+m+int(ln) > len(buf) {
				return nil
			}
			i += m
			return buf[i : i+int(ln)]
		}
		switch tag & 0x07 {
		case 0:
			_, m := binary.Uvarint(buf[i:])
			if m <= 0 {
				return nil
			}
			i += m
		case 2:
			ln, m := binary.Uvarint(buf[i:])
			if m <= 0 || i+m+int(ln) > len(buf) {
				return nil
			}
			i += m + int(ln)
		default:
			return nil
		}
	}
	return nil
}

func parseVarintField(buf []byte, field int) uint64 {
	want := byte((field << 3) | 0)
	for i := 0; i < len(buf); {
		tag := buf[i]
		i++
		if tag == want {
			v, m := binary.Uvarint(buf[i:])
			if m <= 0 {
				return 0
			}
			return v
		}
		switch tag & 0x07 {
		case 0:
			_, m := binary.Uvarint(buf[i:])
			if m <= 0 {
				return 0
			}
			i += m
		case 2:
			ln, m := binary.Uvarint(buf[i:])
			if m <= 0 || i+m+int(ln) > len(buf) {
				return 0
			}
			i += m + int(ln)
		default:
			return 0
		}
	}
	return 0
}

func parseIAVLExportNode(buf []byte) *iavl.ExportNode {
	n := &iavl.ExportNode{}
	sawValue := false
	for i := 0; i < len(buf); {
		tag := buf[i]
		i++
		field := int(tag >> 3)
		wire := tag & 0x07
		switch {
		case field == 1 && wire == 2:
			ln, m := binary.Uvarint(buf[i:])
			if m <= 0 {
				return n
			}
			i += m
			n.Key = append([]byte(nil), buf[i:i+int(ln)]...)
			i += int(ln)
		case field == 2 && wire == 2:
			ln, m := binary.Uvarint(buf[i:])
			if m <= 0 {
				return n
			}
			i += m
			n.Value = append([]byte(nil), buf[i:i+int(ln)]...)
			i += int(ln)
			sawValue = true
		case field == 3 && wire == 0:
			v, m := binary.Uvarint(buf[i:])
			if m <= 0 {
				return n
			}
			i += m
			n.Version = int64(v)
		case field == 4 && wire == 0:
			v, m := binary.Uvarint(buf[i:])
			if m <= 0 {
				return n
			}
			i += m
			n.Height = int8(v)
		default:
			switch wire {
			case 0:
				_, m := binary.Uvarint(buf[i:])
				if m <= 0 {
					return n
				}
				i += m
			case 2:
				ln, m := binary.Uvarint(buf[i:])
				if m <= 0 || i+m+int(ln) > len(buf) {
					return n
				}
				i += m + int(ln)
			default:
				return n
			}
		}
	}
	// Leaves (Height==0) need non-nil Value even if empty (proto3 omits
	// empty bytes). Inner nodes (Height>0) must have nil Value — the
	// importer derives the hash from children.
	if n.Height == 0 && !sawValue {
		n.Value = []byte{}
	}
	return n
}

// ─── CommitInfo / latest version writers ─────────────────────────────────
//
// cosmos-sdk's RootMultiStore writes a CommitInfo proto under "s/<v>" keys
// per version, plus a "s/latest" pointer with the highest committed
// version (a `cmttypes.IntValue`-style proto wrapper around int64).
//
//	message CommitID    { int64 version = 1; bytes hash = 2; }
//	message StoreInfo   { string name = 1; CommitID commit_id = 2; }
//	message CommitInfo  { int64 version = 1; repeated StoreInfo store_infos = 2; google.protobuf.Timestamp timestamp = 3; }
//	message IntValue    { int64 value = 1; }
//
// We omit the timestamp field — gaiad's load path tolerates absence.

type commitID struct {
	Version int64
	Hash    []byte
}

type storeInfo struct {
	Name     string
	CommitID commitID
}

func writeCommitInfo(db rootStore, version int64, infos []storeInfo) error {
	body := encodeCommitInfo(version, infos)
	key := []byte(fmt.Sprintf("s/%d", version))
	return db.SetSync(key, body)
}

func writeLatestVersion(db rootStore, version int64) error {
	body := encVarintField(1, uint64(version))
	return db.SetSync([]byte("s/latest"), body)
}

func encodeCommitInfo(version int64, infos []storeInfo) []byte {
	out := []byte{}
	out = append(out, encVarintField(1, uint64(version))...)
	for _, si := range infos {
		out = append(out, encEmbeddedField(2, encodeStoreInfo(si))...)
	}
	return out
}

func encodeStoreInfo(si storeInfo) []byte {
	out := []byte{}
	out = append(out, encStringFieldRaw(1, si.Name)...)
	out = append(out, encEmbeddedField(2, encodeCommitID(si.CommitID))...)
	return out
}

func encodeCommitID(c commitID) []byte {
	out := []byte{}
	out = append(out, encVarintField(1, uint64(c.Version))...)
	out = append(out, encBytesFieldRaw(2, c.Hash)...)
	return out
}

func encVarintField(field int, v uint64) []byte {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], v)
	out := make([]byte, 0, 1+n)
	out = append(out, byte((field<<3)|0))
	out = append(out, buf[:n]...)
	return out
}

func encStringFieldRaw(field int, s string) []byte {
	return encBytesFieldRaw(field, []byte(s))
}

func encBytesFieldRaw(field int, b []byte) []byte {
	var lenBuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lenBuf[:], uint64(len(b)))
	out := make([]byte, 0, 1+n+len(b))
	out = append(out, byte((field<<3)|2))
	out = append(out, lenBuf[:n]...)
	out = append(out, b...)
	return out
}

func encEmbeddedField(field int, body []byte) []byte {
	var lenBuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lenBuf[:], uint64(len(body)))
	out := make([]byte, 0, 1+n+len(body))
	out = append(out, byte((field<<3)|2))
	out = append(out, lenBuf[:n]...)
	out = append(out, body...)
	return out
}

// HumanBytes formats a byte count.
func HumanBytes(n uint64) string {
	const (
		k = 1024
		m = k * 1024
		g = m * 1024
	)
	switch {
	case n >= g:
		return fmt.Sprintf("%.2f GB", float64(n)/float64(g))
	case n >= m:
		return fmt.Sprintf("%.1f MB", float64(n)/float64(m))
	case n >= k:
		return fmt.Sprintf("%.1f KB", float64(n)/float64(k))
	default:
		return fmt.Sprintf("%d B", n)
	}
}
