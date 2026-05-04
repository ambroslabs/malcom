// Streaming variant of Import. Identical state-machine, but reads chunk
// bytes from a channel instead of from chunk_*.bin files. This lets the
// caller (cosmos-rapid-bootstrap) interleave snapshot-fetch with the
// IAVL import — chunks flow into ImportStream as they're verified.

package snapshotappdb

import (
	"bufio"
	"compress/zlib"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cosmos/iavl"
	idb "github.com/cosmos/iavl/db"
)

// ChunkBytes is one chunk's raw bytes plus its zero-based index. Sent
// over the chunks channel by the producer (snapfetch). Index ordering
// is arbitrary in the channel; ImportStream's reorder buffer reassembles
// the linear stream.
type ChunkBytes struct {
	Index uint32
	Data  []byte
}

// ImportStream is the streaming counterpart to Import. Instead of
// reading chunk_*.bin files from snapshotDir, it consumes ChunkBytes
// from `chunks` until either all `totalChunks` chunks are seen or the
// channel is closed by the producer.
//
// Out-of-order chunks are buffered in memory; if a producer races
// ahead of the importer the buffer can grow proportionally to the
// snapshot's chunk size × the lead. For cosmos-hub format-3 chunks
// this is ~10 MiB × ~30 chunks = ~300 MiB worst case — fine for the
// rapid-bootstrap use case where memory is not the bottleneck.
//
// The caller is responsible for closing `chunks` when all chunks have
// been delivered. If the producer errors out before delivering all
// chunks, it should close the channel anyway and rely on the underlying
// context (from the caller's environment) to cancel the importer; in
// practice cosmos-rapid-bootstrap cancels its parent context on a
// snapfetch error and the importer's reader observes that via the
// reorderReader's ctx.Done() check.
//
// Same outDir/height/backend/extDir/concurrency semantics as Import.
func ImportStream(ctx context.Context, chunks <-chan ChunkBytes, totalChunks uint32,
	outDir string, height int64, backend Backend, extDir string, concurrency int) (Stats, error) {
	var stats Stats

	if concurrency <= 0 {
		concurrency = runtime.NumCPU()
		if concurrency > 8 {
			concurrency = 8
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

	rr := newReorderReader(ctx, chunks, totalChunks)
	zr, err := zlib.NewReader(rr)
	if err != nil {
		return stats, fmt.Errorf("zlib: %w", err)
	}
	defer zr.Close()
	r := &snapItemReader{
		zr: zr,
		br: bufio.NewReaderSize(zr, 1<<20),
	}

	return runImportPipeline(ctx, rootDB, r, height, extDir, concurrency)
}

// runImportPipeline holds the shared logic between Import and
// ImportStream: spawn store workers, decode the SnapshotItem stream,
// dispatch IAVL items to per-store workers, write extension payloads,
// and finally compute commit-info + latest-version pointers. The only
// difference between callers is how snapItemReader is built — disk vs.
// channel-backed reader.
func runImportPipeline(ctx context.Context, rootDB rootStore, r *snapItemReader,
	height int64, extDir string, concurrency int) (Stats, error) {
	var stats Stats
	startTime := time.Now()

	var workers []*storeWorker
	var current *storeWorker
	sem := make(chan struct{}, concurrency)
	pipelineCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var totalItems uint64

	closeCurrent := func() {
		if current == nil {
			return
		}
		close(current.items)
		current = nil
	}

	spawnWorker := func(name string) error {
		select {
		case sem <- struct{}{}:
		case <-pipelineCtx.Done():
			return firstWorkerError(workers)
		}
		w, err := newStoreWorker(rootDB, name, height, startTime, &totalItems)
		if err != nil {
			<-sem
			return err
		}
		workers = append(workers, w)
		current = w
		go func(w *storeWorker) {
			defer func() { <-sem }()
			w.run()
			if w.err != nil {
				cancel()
			}
		}(w)
		return nil
	}

	sendNode := func(node *iavl.ExportNode) error {
		select {
		case current.items <- node:
			return nil
		case <-pipelineCtx.Done():
			return firstWorkerError(workers)
		}
	}

	var (
		curExt       string
		curExtFormat uint32
		curExtIndex  int
	)

readLoop:
	for {
		tag, body, eof, err := r.peekItem()
		if err != nil {
			return stats, err
		}
		if eof {
			break
		}
		r.consumeItem()
		stats.BytesUncompressed += uint64(len(body))

		switch tag {
		case 1: // SnapshotStoreItem
			closeCurrent()
			name := string(parseStringField(body, 1))
			fmt.Printf("[appdb] %s open store=%q\n",
				time.Since(startTime).Truncate(time.Second), name)
			if err := spawnWorker(name); err != nil {
				return stats, err
			}
		case 2: // SnapshotIAVLItem
			if current == nil {
				return stats, fmt.Errorf("IAVL item before any StoreItem")
			}
			node := parseIAVLExportNode(body)
			if err := sendNode(node); err != nil {
				return stats, err
			}
		case 3: // SnapshotExtensionMeta
			closeCurrent()
			curExt = string(parseStringField(body, 1))
			curExtFormat = uint32(parseVarintField(body, 2))
			curExtIndex = 0
			fmt.Printf("[appdb] %s open extension=%q format=%d\n",
				time.Since(startTime).Truncate(time.Second), curExt, curExtFormat)
			if extDir != "" {
				if err := os.MkdirAll(filepath.Join(extDir, curExt), 0o755); err != nil {
					return stats, err
				}
			}
			stats.Extensions++
		case 4: // SnapshotExtensionPayload
			payload := parseBytesField(body, 1)
			if extDir != "" && curExt != "" {
				path := filepath.Join(extDir, curExt,
					fmt.Sprintf("payload-%d-format%d.bin", curExtIndex, curExtFormat))
				if err := os.WriteFile(path, payload, 0o644); err != nil {
					return stats, err
				}
			}
			curExtIndex++
			stats.ExtensionPayloads++
		default:
			// unknown — skip
		}
		if pipelineCtx.Err() != nil {
			break readLoop
		}
	}

	closeCurrent()

	var storeNamesForHash []string
	for _, w := range workers {
		<-w.done
		if w.err != nil {
			return stats, w.err
		}
		stats.Stores++
		stats.Items += atomic.LoadUint64(&w.itemsAdded)
		storeNamesForHash = append(storeNamesForHash, w.name)
	}

	if fc, ok := rootDB.(interface{ FinalCompact() error }); ok {
		fmt.Printf("[appdb] %s starting final compaction (this can take a few minutes)...\n",
			time.Since(startTime).Truncate(time.Second))
		compactStart := time.Now()
		if err := fc.FinalCompact(); err != nil {
			return stats, fmt.Errorf("final compact: %w", err)
		}
		fmt.Printf("[appdb] final compaction complete in %s\n",
			time.Since(compactStart).Truncate(time.Second))
	}

	fmt.Printf("[appdb] %s resolving per-store hashes...\n",
		time.Since(startTime).Truncate(time.Second))
	hashStart := time.Now()
	var storeInfos []storeInfo
	for _, name := range storeNamesForHash {
		t := iavl.NewMutableTree(idb.NewPrefixDB(rootDB, storePrefix(name)), 0, true, iavl.NewNopLogger())
		ver, err := t.LoadVersion(height)
		if err != nil {
			return stats, fmt.Errorf("load store %q at v%d: %w", name, height, err)
		}
		if ver != height {
			return stats, fmt.Errorf("store %q loaded v%d, expected v%d", name, ver, height)
		}
		h := t.Hash()
		storeInfos = append(storeInfos, storeInfo{
			Name: name,
			CommitID: commitID{
				Version: height,
				Hash:    append([]byte(nil), h...),
			},
		})
		t.Close()
	}
	fmt.Printf("[appdb] hashes resolved in %s\n",
		time.Since(hashStart).Truncate(time.Millisecond))

	sort.Slice(storeInfos, func(i, j int) bool { return storeInfos[i].Name < storeInfos[j].Name })
	if err := writeCommitInfo(rootDB, height, storeInfos); err != nil {
		return stats, fmt.Errorf("write commit info: %w", err)
	}
	if err := writeLatestVersion(rootDB, height); err != nil {
		return stats, fmt.Errorf("write latest version: %w", err)
	}

	return stats, nil
}

// reorderReader is an io.Reader that consumes ChunkBytes from a channel
// in arbitrary order, buffers them in a small map keyed by chunk index,
// and serves bytes to its consumer (the zlib reader) in strict
// chunk-index order.
//
// We don't bound the reorder buffer because:
//  1. snapfetch caps in-flight chunks per peer at perPeer × peerCount,
//     so the producer side has its own implicit bound;
//  2. the importer naturally back-pressures: zlib.Read will block on
//     Read once it consumes everything available, which causes the
//     reorder buffer to drain naturally;
//  3. for cosmos-hub the worst-case buffer is bounded by the number of
//     good peers (typically 5–20) × perPeer (default 2) × ~10 MiB =
//     ~200 MiB.
//
// Concurrency model: a single reader goroutine (the zlib decompressor's
// caller) calls Read; a single producer (snapfetch) sends to the chunks
// channel. The reader holds the lock during Read, including while
// blocking on cond.Wait. The lock is released by sync.Cond.Wait, so the
// channel-pump goroutine can acquire it to insert new chunks.
type reorderReader struct {
	ctx     context.Context
	chunks  <-chan ChunkBytes
	total   uint32
	nextIdx uint32 // next chunk index the reader expects to consume

	mu     sync.Mutex
	cond   *sync.Cond
	buffer map[uint32][]byte // buffered out-of-order chunks
	closed bool              // producer has closed `chunks`
	curBuf []byte            // bytes pending consumption from buffer[nextIdx]
}

func newReorderReader(ctx context.Context, chunks <-chan ChunkBytes, total uint32) *reorderReader {
	r := &reorderReader{
		ctx:    ctx,
		chunks: chunks,
		total:  total,
		buffer: make(map[uint32][]byte),
	}
	r.cond = sync.NewCond(&r.mu)
	go r.pump()
	// Wake any blocked Read() if ctx is cancelled while we wait. Without
	// this the reader can stall forever if the producer never closes
	// the channel and never sends anything.
	go func() {
		<-ctx.Done()
		r.mu.Lock()
		r.cond.Broadcast()
		r.mu.Unlock()
	}()
	return r
}

// pump drains the chunks channel into the reorder buffer and broadcasts
// to any reader blocked in Read. Exits when the channel closes (or ctx
// cancels), then marks closed so Read can return io.EOF after draining.
func (r *reorderReader) pump() {
	defer func() {
		r.mu.Lock()
		r.closed = true
		r.cond.Broadcast()
		r.mu.Unlock()
	}()
	for {
		select {
		case <-r.ctx.Done():
			return
		case ch, ok := <-r.chunks:
			if !ok {
				return
			}
			r.mu.Lock()
			// Defensive copy: snapfetch's chunk buffer may be reused
			// across requests in some configurations. We pay one extra
			// copy here to be safe; for ~3 GB total this is ~3 GB of
			// extra allocation across the lifetime of an import — fine.
			r.buffer[ch.Index] = append([]byte(nil), ch.Data...)
			r.cond.Broadcast()
			r.mu.Unlock()
		}
	}
}

func (r *reorderReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for {
		// If we already have bytes mid-chunk, serve them.
		if len(r.curBuf) > 0 {
			n := copy(p, r.curBuf)
			r.curBuf = r.curBuf[n:]
			return n, nil
		}
		// If the next chunk in order is buffered, promote it.
		if buf, ok := r.buffer[r.nextIdx]; ok {
			delete(r.buffer, r.nextIdx)
			r.nextIdx++
			r.curBuf = buf
			continue
		}
		// Done condition: we've delivered every chunk the producer
		// promised. Note: total may be 0 if the producer doesn't know
		// it ahead of time, but for snapfetch it's always known via
		// OnChosen.
		if r.total > 0 && r.nextIdx >= r.total {
			return 0, io.EOF
		}
		// Closed-and-empty: producer closed and the next chunk hasn't
		// arrived — short snapshot or producer error.
		if r.closed {
			return 0, io.EOF
		}
		// Honour ctx cancellation.
		if err := r.ctx.Err(); err != nil {
			return 0, err
		}
		// Block until pump wakes us with a new chunk (or ctx cancel).
		r.cond.Wait()
	}
}
