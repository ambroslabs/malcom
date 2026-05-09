package snapshotimport

import (
	"context"
	"io"
)

// prefetchReader wraps an io.Reader with a goroutine that reads
// ahead into freshly-allocated buffers, decoupling decode-side work
// from consumer-side work.
//
// Used to overlap the snapshot's zlib decompression with the IAVL +
// pebble work in runImport. The profile shows decode is ~26% of
// stream time and is independent of the IAVL stack on the consumer
// goroutine, so running it on its own goroutine is the largest
// single lever remaining in the stream phase.
//
// Allocates a fresh buffer per shipped chunk rather than recycling
// — at ~36 MB/s for a cosmoshub-4 import, GC handles this fine and
// the simpler design avoids the channel-balance footguns that bit
// the previous recycling implementation.
type prefetchReader struct {
	ch     chan readChunk // producer → consumer; closed when producer exits
	cancel context.CancelFunc
	done   chan struct{} // closed when producer goroutine exits

	cur []byte // chunk being drained by Read
	err error  // sticky end-of-stream error
}

type readChunk struct {
	data []byte // non-nil when err is nil; both nil legal as a no-op
	err  error  // nil for normal data; io.EOF or transport error otherwise
}

// newPrefetchReader spawns a goroutine that reads from src into
// fresh bufSize-byte buffers. numBufs controls the channel
// capacity = how far ahead the producer can run before blocking on
// the consumer. The total in-flight memory is ≤ numBufs × bufSize.
func newPrefetchReader(parent context.Context, src io.Reader, numBufs, bufSize int) *prefetchReader {
	if numBufs < 1 {
		numBufs = 1
	}
	if bufSize < 1 {
		bufSize = 4096
	}
	ctx, cancel := context.WithCancel(parent)
	p := &prefetchReader{
		ch:     make(chan readChunk, numBufs),
		cancel: cancel,
		done:   make(chan struct{}),
	}
	go p.run(ctx, src, bufSize)
	return p
}

func (p *prefetchReader) run(ctx context.Context, src io.Reader, bufSize int) {
	defer close(p.ch)
	defer close(p.done)
	for {
		buf := make([]byte, bufSize)
		n, err := io.ReadFull(src, buf)
		if n > 0 {
			select {
			case <-ctx.Done():
				return
			case p.ch <- readChunk{data: buf[:n]}:
			}
		}
		if err != nil {
			// ReadFull returns ErrUnexpectedEOF on a short last read;
			// from the consumer's perspective that's just EOF.
			if err == io.ErrUnexpectedEOF {
				err = io.EOF
			}
			select {
			case <-ctx.Done():
			case p.ch <- readChunk{err: err}:
			}
			return
		}
	}
}

// Read drains buffered bytes into dst. Returns io.EOF (or the
// underlying error) once the producer has signalled end-of-stream
// and all buffered bytes are consumed.
func (p *prefetchReader) Read(dst []byte) (int, error) {
	for len(p.cur) == 0 {
		if p.err != nil {
			return 0, p.err
		}
		c, ok := <-p.ch
		if !ok {
			if p.err == nil {
				p.err = io.EOF
			}
			return 0, p.err
		}
		if c.err != nil {
			p.err = c.err
			if len(c.data) == 0 {
				return 0, p.err
			}
			// Edge case (current producer never hits this, but it's
			// cheap to be correct): err shipped alongside last data.
			p.cur = c.data
			break
		}
		p.cur = c.data
	}
	n := copy(dst, p.cur)
	p.cur = p.cur[n:]
	return n, nil
}

// Close stops the producer and waits for it to exit. Safe to call
// multiple times.
func (p *prefetchReader) Close() error {
	p.cancel()
	<-p.done
	return nil
}
