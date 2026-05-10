// TailingChunkSource is a chunk-stream io.ReadCloser that opens
// chunk_NNNNN.bin lazily, in stream order, blocking until the
// producer (fetch) signals each index ready. It exists so
// `malcom snapshot fetch -import` can start the import pipeline
// the moment chunk 0 lands instead of waiting for the full snapshot
// to finish writing.
//
// Concurrency contract:
//   - Exactly one consumer goroutine calls Read/Close.
//   - Any number of producer goroutines call MarkReady / Fail / FetchDone.
//   - MarkReady(i) must be called only after chunk_<i>.bin is durably
//     on disk (post-rename), since Read opens the file as soon as
//     the index is marked ready.
//
// On a successful fetch the consumer reads chunks 0..total-1 to EOF.
// On a failed fetch the producer calls Fail(err); the next consumer
// Read returns that error so the import unwinds.
package snapshotimport

import (
	"compress/zlib"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// TailingChunkSource yields the decompressed SnapshotItem stream by
// reading chunk_NNNNN.bin files as they appear under dir, lazily
// stitched together and zlib-decoded.
type TailingChunkSource struct {
	raw *tailingRawReader
	zr  io.ReadCloser
}

// NewTailingChunkSource returns a source that will yield decompressed
// bytes from dir/chunk_00000.bin … chunk_<total-1>.bin in order. The
// caller is responsible for marking each chunk ready (via MarkReady)
// once it lands on disk, and for calling Fail or Close on the read
// path on early termination.
func NewTailingChunkSource(dir string, total uint32) *TailingChunkSource {
	return &TailingChunkSource{
		raw: newTailingRawReader(dir, total),
	}
}

// Read decompressed bytes. The first call blocks until chunk 0 is
// marked ready (zlib needs the header before returning).
func (t *TailingChunkSource) Read(p []byte) (int, error) {
	if t.zr == nil {
		// zlib.NewReader will Read from t.raw to consume the header,
		// which blocks until chunk 0 is marked ready. That's the
		// desired behaviour: stage 1 can't make progress without
		// chunk 0 anyway.
		zr, err := zlib.NewReader(t.raw)
		if err != nil {
			return 0, fmt.Errorf("zlib: %w", err)
		}
		t.zr = zr
	}
	return t.zr.Read(p)
}

// Close releases the zlib reader and the underlying tailing reader.
// Idempotent.
func (t *TailingChunkSource) Close() error {
	if t.zr != nil {
		t.zr.Close()
		t.zr = nil
	}
	return t.raw.Close()
}

// MarkReady tells the source that chunk_<idx>.bin is durably on disk
// and may be opened. Safe to call from any goroutine; idempotent.
func (t *TailingChunkSource) MarkReady(idx uint32) { t.raw.MarkReady(idx) }

// Fail aborts pending and future Reads with err. Used when the
// producer (fetch) terminates with an error before delivering all
// chunks. Idempotent — only the first error sticks.
func (t *TailingChunkSource) Fail(err error) { t.raw.Fail(err) }

// tailingRawReader yields the raw concatenation of chunk_NNNNN.bin
// files as a single io.ReadCloser. It opens one file at a time, in
// order, blocking on a condition variable until each next index has
// been marked ready.
type tailingRawReader struct {
	dir   string
	total uint32

	mu      sync.Mutex
	cv      *sync.Cond
	ready   []bool // ready[i] == true once chunk_<i>.bin is durable on disk
	failErr error  // non-nil if Fail() has been called; sticky
	closed  bool

	// cur is the index of the chunk currently being read. f is the
	// open file handle, or nil if we haven't opened cur yet (or have
	// already exhausted everything).
	cur uint32
	f   *os.File
}

func newTailingRawReader(dir string, total uint32) *tailingRawReader {
	r := &tailingRawReader{
		dir:   dir,
		total: total,
		ready: make([]bool, total),
	}
	r.cv = sync.NewCond(&r.mu)
	return r
}

func (r *tailingRawReader) MarkReady(idx uint32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if idx >= uint32(len(r.ready)) {
		// Stale signal (e.g. a chunk reported ready after the source
		// has been resized). Ignore; the consumer's bounds check
		// would catch any drift.
		return
	}
	r.ready[idx] = true
	r.cv.Broadcast()
}

func (r *tailingRawReader) Fail(err error) {
	if err == nil {
		err = io.ErrUnexpectedEOF
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failErr == nil {
		r.failErr = err
	}
	r.cv.Broadcast()
}

func (r *tailingRawReader) Read(p []byte) (int, error) {
	for {
		if r.f == nil {
			if r.cur >= r.total {
				return 0, io.EOF
			}
			if err := r.openCurrent(); err != nil {
				return 0, err
			}
		}
		n, err := r.f.Read(p)
		if n > 0 {
			return n, err
		}
		if err == io.EOF {
			if cerr := r.f.Close(); cerr != nil {
				r.f = nil
				return 0, fmt.Errorf("close chunk %d: %w", r.cur, cerr)
			}
			r.f = nil
			r.cur++
			continue
		}
		return n, err
	}
}

// openCurrent waits for chunk r.cur to be marked ready, then opens
// it. Returns the failure error if Fail or Close was called while we
// were waiting.
func (r *tailingRawReader) openCurrent() error {
	r.mu.Lock()
	for !r.ready[r.cur] && r.failErr == nil && !r.closed {
		r.cv.Wait()
	}
	switch {
	case r.failErr != nil:
		err := r.failErr
		r.mu.Unlock()
		return err
	case r.closed:
		r.mu.Unlock()
		return io.ErrClosedPipe
	}
	r.mu.Unlock()

	path := filepath.Join(r.dir, fmt.Sprintf("chunk_%05d.bin", r.cur))
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open chunk %d: %w", r.cur, err)
	}
	r.f = f
	return nil
}

func (r *tailingRawReader) Close() error {
	r.mu.Lock()
	r.closed = true
	r.cv.Broadcast()
	r.mu.Unlock()
	if r.f != nil {
		err := r.f.Close()
		r.f = nil
		return err
	}
	return nil
}
