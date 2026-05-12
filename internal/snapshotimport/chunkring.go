// In-memory chunk ring: stage 1 (decompressor) writes decompressed
// bytes to the ring; stage 2 (per-store workers) read from the ring
// at their store's offset. Replaces the on-disk decompressed.tmp
// file (which was 76 GB on bbn finality).
//
// The ring is bounded by maxBytes; stage 1 blocks on Write when the
// total in-ring bytes would exceed it. Old chunks are evicted when
// the minimum reader cursor advances past the chunk's end. With one
// reader (the bbn finality tail), the ring shrinks to chunks just
// ahead of the consumer; with multiple concurrent readers (osmosis
// cl/ibc/wasm), the ring spans from the slowest reader's cursor to
// the producer's tail.
//
// Pipelines stage 1 + stage 2 (vs the temp-file design where stage
// 2 had to wait for stage 1 to finish). For a chain whose polestar
// is the last store in the stream, this lifts the stage-1
// decompression time off the wall.

package snapshotimport

import (
	"errors"
	"fmt"
	"io"
	"sync"
)

// defaultChunkMB is the chunk-ring memory budget (in MiB) when the
// user doesn't pass -chunk-mb. 512 MiB picks predictable, modest
// RSS over peak wall throughput: bbn-1's solo polestar runs equally
// fast at 512 MiB or higher (one consumer easily keeps up); cosmoshub
// and osmosis pay ~23% more wall vs an oversized ring because their
// multi-polestar concurrent readers spread the slowest cursor across
// a wider byte range. Operators on big boxes can opt up via the flag.
const defaultChunkMB = 512

// chunkEnt is one in-memory decompressed chunk, owned by the ring
// until evicted. start is its offset in the decompressed stream;
// data is the bytes (length up to ring.chunkSize).
type chunkEnt struct {
	start int64
	data  []byte
}

// chunkRing is the producer/consumer ring. Single producer (stage 1)
// calls Write + Close; many consumers (per-store readers) call
// NewReader + Read. Threadsafe.
type chunkRing struct {
	chunkSize int   // size of each emitted chunk during Write
	maxBytes  int64 // soft cap on total in-ring bytes; producer blocks above this

	mu     sync.Mutex
	cond   *sync.Cond  // signaled on chunk added, chunk evicted, ring closed, reader.SetEnd
	chunks []chunkEnt  // ordered by start offset, oldest first
	tail   int64       // total bytes the producer has written
	head   int64       // first byte still in ring (= chunks[0].start when non-empty)
	closed bool        // producer signaled EOF
	err    error       // producer error captured here so readers see it
	readers []*chunkRingReader

	// freeBufs is a fixed-cap pool of chunkSize-cap byte slices
	// recycled across allocations. Cap is maxBytes/chunkSize so the
	// ring's total memory (active chunks + free-list) never exceeds
	// maxBytes by more than one chunkSize. Avoids GC churn on the
	// 76 GB / run that flows through the ring on bbn finality —
	// without a pool, every 4 MiB chunk is a fresh make() and an
	// eventual GC reclamation.
	freeBufs [][]byte
}

// newChunkRing returns a ring with the given chunk size and max-bytes
// soft cap. Panics on chunkSize <= 0 or maxBytes < chunkSize.
func newChunkRing(chunkSize int, maxBytes int64) *chunkRing {
	if chunkSize <= 0 {
		panic("chunkRing: chunkSize must be positive")
	}
	if maxBytes < int64(chunkSize) {
		panic("chunkRing: maxBytes must be >= chunkSize")
	}
	maxChunks := int(maxBytes / int64(chunkSize))
	r := &chunkRing{
		chunkSize: chunkSize,
		maxBytes:  maxBytes,
		freeBufs:  make([][]byte, 0, maxChunks),
	}
	r.cond = sync.NewCond(&r.mu)
	return r
}

// allocBuf returns a chunkSize-cap, zero-length byte slice. Pulls
// from the free list when possible; otherwise allocates fresh.
// Caller holds r.mu.
func (r *chunkRing) allocBufLocked() []byte {
	if n := len(r.freeBufs); n > 0 {
		buf := r.freeBufs[n-1]
		r.freeBufs[n-1] = nil
		r.freeBufs = r.freeBufs[:n-1]
		return buf[:0]
	}
	return make([]byte, 0, r.chunkSize)
}

// freeBufLocked returns a buffer to the free list. Drops on the
// floor if the list is at cap (= ring.maxBytes worth of free
// slabs already pooled). Caller holds r.mu.
func (r *chunkRing) freeBufLocked(b []byte) {
	if cap(b) != r.chunkSize {
		return
	}
	if len(r.freeBufs) >= cap(r.freeBufs) {
		return
	}
	r.freeBufs = append(r.freeBufs, b[:0])
}

// Write appends bytes to the ring, splitting into chunks of
// chunkSize. Blocks when the ring is full of un-evicted chunks
// (consumers haven't advanced past them). Implements io.Writer so
// stage 1 can hand it to a TeeReader.
func (r *chunkRing) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	written := 0
	for written < len(p) {
		// Wait for room under the maxBytes cap before any append.
		// The cap applies to BOTH paths (grow-existing and new
		// chunk); without this check on the grow path, tail-head
		// could blow past maxBytes if writes happen to land within
		// the most-recent chunk's remaining capacity.
		for !r.closed && r.tail-r.head >= r.maxBytes {
			r.cond.Wait()
		}
		if r.closed {
			return written, errors.New("chunkRing: write to closed ring")
		}
		// Grow most-recent chunk if it has room.
		if n := len(r.chunks); n > 0 && len(r.chunks[n-1].data) < r.chunkSize {
			c := &r.chunks[n-1]
			room := r.chunkSize - len(c.data)
			take := len(p) - written
			if take > room {
				take = room
			}
			// Also bound the take by remaining cap, in case the
			// cap allows fewer bytes than the chunk's room.
			if cap := r.maxBytes - (r.tail - r.head); int64(take) > cap {
				take = int(cap)
			}
			if take == 0 {
				// No room under cap; loop to re-wait.
				continue
			}
			c.data = append(c.data, p[written:written+take]...)
			r.tail += int64(take)
			written += take
			r.cond.Broadcast()
			continue
		}
		take := len(p) - written
		if take > r.chunkSize {
			take = r.chunkSize
		}
		if cap := r.maxBytes - (r.tail - r.head); int64(take) > cap {
			take = int(cap)
		}
		if take == 0 {
			continue
		}
		// Pull a chunkSize-cap buffer from the free list (or alloc
		// fresh) and seed it with the new bytes.
		buf := r.allocBufLocked()
		buf = append(buf, p[written:written+take]...)
		r.chunks = append(r.chunks, chunkEnt{start: r.tail, data: buf})
		r.tail += int64(take)
		written += take
		r.cond.Broadcast()
	}
	return written, nil
}

// Close signals end-of-stream. Subsequent Read on a reader past
// ring.tail returns io.EOF. err, if non-nil, is propagated to
// readers via Read returning that error once the ring is drained.
func (r *chunkRing) Close(err error) {
	r.mu.Lock()
	r.closed = true
	if r.err == nil {
		r.err = err
	}
	r.cond.Broadcast()
	r.mu.Unlock()
}

// NewReader creates a reader starting at the given offset. start
// must be >= ring.head (the bytes haven't been evicted yet);
// callers that hit ErrStartEvicted got beaten to the punch by an
// eviction triggered while they were preparing the call — see #124
// for the race the precondition catches. endLimit is the exclusive
// end byte; -1 means "until ring is closed past this reader's
// cursor." SetEnd lets stage 1 finalize endLimit later when the
// store's boundary becomes known.
func (r *chunkRing) NewReader(start, endLimit int64) (*chunkRingReader, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if start < r.head {
		return nil, fmt.Errorf("%w: start %d below ring head %d", ErrStartEvicted, start, r.head)
	}
	rd := &chunkRingReader{ring: r, pos: start, endLimit: endLimit}
	r.readers = append(r.readers, rd)
	return rd, nil
}

// ErrStartEvicted is returned by NewReader when start < head. The
// chunk containing start has already been recycled, so a reader
// born here would silently see "pos below ring head" on its first
// Read. Better to surface the failure at construction.
var ErrStartEvicted = errors.New("chunkRing: start offset already evicted")

// chunkRingReader tracks one reader's cursor into the ring. The
// reader's pos pins eviction so the ring can't drop bytes the
// reader still needs.
type chunkRingReader struct {
	ring     *chunkRing
	pos      int64
	endLimit int64 // -1 means "until ring closed past pos"
}

// Read implements io.Reader. Blocks until bytes are available at
// pos (the producer hasn't reached pos yet) or the ring is closed
// past pos. Returns io.EOF when pos has reached endLimit (if set)
// or when the ring is closed and pos >= ring.tail.
func (r *chunkRingReader) Read(p []byte) (int, error) {
	r.ring.mu.Lock()
	defer r.ring.mu.Unlock()
	for {
		if r.endLimit >= 0 && r.pos >= r.endLimit {
			return 0, io.EOF
		}
		idx := r.findChunkLocked()
		if idx >= 0 {
			c := &r.ring.chunks[idx]
			off := int(r.pos - c.start)
			avail := len(c.data) - off
			if avail <= 0 {
				// Producer hasn't extended this chunk yet; wait.
				r.ring.cond.Wait()
				continue
			}
			toRead := avail
			if toRead > len(p) {
				toRead = len(p)
			}
			if r.endLimit >= 0 && r.pos+int64(toRead) > r.endLimit {
				toRead = int(r.endLimit - r.pos)
			}
			copy(p, c.data[off:off+toRead])
			r.pos += int64(toRead)
			r.ring.evictLocked()
			r.ring.cond.Broadcast()
			return toRead, nil
		}
		// No chunk contains r.pos. Either pos is past tail (wait or
		// EOF) or pos was evicted (caller bug — shouldn't happen
		// because the reader's own cursor pins eviction).
		if r.pos < r.ring.head {
			return 0, fmt.Errorf("chunkRingReader: pos %d below ring head %d (evicted under reader)",
				r.pos, r.ring.head)
		}
		if r.ring.closed && r.pos >= r.ring.tail {
			if r.ring.err != nil {
				return 0, r.ring.err
			}
			return 0, io.EOF
		}
		r.ring.cond.Wait()
	}
}

// SetEnd updates the reader's end-of-store offset. Stage 1 calls
// this when the store boundary becomes known (= the next StoreItem
// or the extension tail is parsed). Reads past end return io.EOF.
func (r *chunkRingReader) SetEnd(end int64) {
	r.ring.mu.Lock()
	r.endLimit = end
	r.ring.cond.Broadcast()
	r.ring.mu.Unlock()
}

// Close releases the reader's hold on the ring so its chunks can
// be evicted. Idempotent.
func (r *chunkRingReader) Close() {
	r.ring.mu.Lock()
	for i, x := range r.ring.readers {
		if x == r {
			r.ring.readers = append(r.ring.readers[:i], r.ring.readers[i+1:]...)
			break
		}
	}
	r.ring.evictLocked()
	r.ring.cond.Broadcast()
	r.ring.mu.Unlock()
}

// findChunkLocked returns the index of the chunk containing r.pos,
// or -1 if no chunk is present. Caller holds r.ring.mu.
func (r *chunkRingReader) findChunkLocked() int {
	for i := range r.ring.chunks {
		c := &r.ring.chunks[i]
		if r.pos >= c.start && r.pos < c.start+int64(len(c.data)) {
			return i
		}
	}
	// Allow pos == c.start + len(c.data) for the most-recent chunk
	// (= producer just wrote out a full chunk and we're waiting for
	// the next one). Not a hit; caller's wait loop handles it.
	return -1
}

// evictLocked drops chunks whose end offset is <= the minimum
// reader cursor. Caller holds mu.
func (r *chunkRing) evictLocked() {
	if len(r.readers) == 0 {
		// No readers (stage 1 producing into the void): keep
		// everything; stage 1 will block on maxBytes.
		return
	}
	minPos := r.readers[0].pos
	for _, rd := range r.readers[1:] {
		if rd.pos < minPos {
			minPos = rd.pos
		}
	}
	for len(r.chunks) > 0 {
		c := r.chunks[0]
		if c.start+int64(len(c.data)) > minPos {
			break
		}
		r.head = c.start + int64(len(c.data))
		r.chunks = r.chunks[1:]
		r.freeBufLocked(c.data)
	}
}
