// Snapshot index: a per-store offset table built by walking the
// decompressed stream once. Lets a future parallel-import driver
// schedule stores concurrently without re-decompressing the whole
// stream per worker.
//
// The index records, for each store seen in the stream:
//
//   - DecompressedStart: byte offset (in the decompressed stream) at
//     which the store's leading SnapshotStoreItem envelope begins.
//   - DecompressedEnd:   byte offset of the *next* store's StoreItem
//     (or end-of-stream for the last store).
//   - ItemCount / IAVLBytes: rough size proxies for scheduling
//     (bigger first).
//
// Memory cost is O(num_stores) — for cosmoshub-4 that's 25 entries
// = a handful of KiB. Babylon may have more stores but still bounded.
//
// The index does NOT yet include zlib state checkpoints. Per-store
// parallel readers will, in a follow-up, either (a) pre-decompress
// the whole stream to a temp file and mmap it, or (b) carry zlib
// state at the index entries. This file is the offset-and-sizing
// half of either approach.

package snapshotimport

import (
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"time"
)

// SnapshotIndex is the result of one BuildIndex pass.
type SnapshotIndex struct {
	// Stores is sorted by IAVLBytes descending so a parallel
	// scheduler can dispatch the largest stores first.
	Stores []StoreEntry

	// ExtStart is the offset where the extension tail begins, or 0
	// if the snapshot has no extensions. Extensions are not
	// parallelizable — stage 3 reads them sequentially after all
	// store workers complete.
	ExtStart int64

	TotalItems     uint64        // sum of items across all stores
	TotalBytes     int64         // size of the decompressed stream
	BuildElapsed   time.Duration // wall time of the index build
	BuildBytesRate float64       // MB/s of decompressed scan
}

// StoreEntry describes one store's location and rough size in the
// decompressed snapshot stream.
type StoreEntry struct {
	Name              string
	DecompressedStart int64  // offset of the StoreItem envelope's leading varint
	DecompressedEnd   int64  // offset where the next store starts (or stream end); 0 if not yet known
	ItemCount         uint64 // total items in this store (StoreItem + IAVL items)
	IAVLBytes         int64  // sum of IAVL item-envelope bytes (size proxy)

	// EndCh, when non-nil, is a single-shot channel that the
	// streaming-pipeline producer (parallel.go's stage 1) sends the
	// store's end offset on as soon as it's known (= the next
	// StoreItem or the extension tail is parsed). Workers using the
	// chunkRing-based reader block on this to set their reader's
	// end. BuildIndex returns entries with EndCh == nil and the
	// final DecompressedEnd populated normally.
	EndCh chan int64

	// reader is set by stage 1's onOpen callback at the moment the
	// store is emitted, NOT when the worker pulls it from storeCh.
	// Creating the reader at emit time pins the ring's eviction at
	// DecompressedStart — without this, the start chunk could be
	// evicted while the StoreEntry sits in storeCh waiting for a
	// worker (other readers advance, ring.head climbs past start).
	// nil for BuildIndex callers.
	reader *chunkRingReader
}

// BuildIndex decompresses the snapshot once and emits a per-store
// offset+size index. log may be nil for silent operation. The index
// is sorted by IAVLBytes descending — bigger stores first, suited
// for a parallel scheduler that wants the longest pole running ASAP.
func BuildIndex(snapshotDir string, log *slog.Logger) (*SnapshotIndex, error) {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	cr, err := openChunkDir(snapshotDir)
	if err != nil {
		return nil, fmt.Errorf("open snapshot dir: %w", err)
	}
	defer cr.Close()

	t0 := time.Now()

	scanner := newIndexScanner(cr)
	idx := &SnapshotIndex{}
	var (
		curStore     *StoreEntry // ptr into idx.Stores tail
		streamEnd    int64
	)
	for {
		envStart := scanner.pos
		envLen, ok, err := scanner.readUvarint()
		if err != nil {
			return nil, fmt.Errorf("read envelope length at offset %d: %w", envStart, err)
		}
		if !ok {
			// EOF
			streamEnd = envStart
			break
		}
		if envLen == 0 {
			return nil, fmt.Errorf("zero-length envelope at offset %d", envStart)
		}
		if envLen > maxEnvelopeBytes {
			return nil, fmt.Errorf("snapshot envelope %d bytes exceeds cap %d at offset %d", envLen, maxEnvelopeBytes, envStart)
		}
		// Peek the first byte of the item to know its type. The wire
		// tag is (field<<3)|2 for length-delimited; we only branch on
		// the field number.
		tag, err := scanner.peekByte()
		if err != nil {
			return nil, fmt.Errorf("peek tag at offset %d: %w", scanner.pos, err)
		}
		field := tag >> 3

		switch itemType(field) {
		case itemTypeStore:
			// New store: read the inner StoreItem to extract its
			// name. Cheap — StoreItem is just a length-delimited
			// string field.
			name, err := scanner.readStoreName(envLen)
			if err != nil {
				return nil, fmt.Errorf("read store name at offset %d: %w", envStart, err)
			}
			// Close the previous store entry.
			if curStore != nil {
				curStore.DecompressedEnd = envStart
			}
			idx.Stores = append(idx.Stores, StoreEntry{
				Name:              name,
				DecompressedStart: envStart,
				ItemCount:         1,
			})
			curStore = &idx.Stores[len(idx.Stores)-1]
		default:
			// Skip the item body without parsing. We're not
			// interested in IAVL contents, just sizes.
			if err := scanner.discard(int64(envLen)); err != nil {
				return nil, fmt.Errorf("discard item body at offset %d: %w", envStart, err)
			}
			if curStore != nil && itemType(field) == itemTypeIAVL {
				curStore.IAVLBytes += int64(envLen)
				curStore.ItemCount++
			} else if curStore != nil {
				// Extension items count toward ItemCount but not IAVLBytes.
				curStore.ItemCount++
			}
		}
		idx.TotalItems++
	}
	if curStore != nil {
		curStore.DecompressedEnd = streamEnd
	}
	idx.TotalBytes = streamEnd
	idx.BuildElapsed = time.Since(t0)
	if idx.BuildElapsed > 0 {
		idx.BuildBytesRate = float64(idx.TotalBytes) / idx.BuildElapsed.Seconds() / (1 << 20)
	}

	// Sort by IAVLBytes descending (size). Stable sort so equal-size
	// stores keep stream order.
	sort.SliceStable(idx.Stores, func(i, j int) bool {
		return idx.Stores[i].IAVLBytes > idx.Stores[j].IAVLBytes
	})

	log.Info("index built",
		"stores", len(idx.Stores),
		"items", idx.TotalItems,
		"bytes", uint64(idx.TotalBytes),
		"elapsed", idx.BuildElapsed.Truncate(time.Millisecond),
		"rate_mb_s", fmt.Sprintf("%.1f", idx.BuildBytesRate))

	return idx, nil
}

// indexScanner is a minimal buffered reader that tracks the
// decompressed-stream offset. We can't use snapReader directly
// because it does full proto decoding per item; for indexing we
// only need item type, length, and (for StoreItem) the inner
// string. Skipping the rest saves the proto-decode time we'd
// otherwise pay (which the cpu profile attributed to ~60s of
// snapReader.Next overhead).
type indexScanner struct {
	r   io.Reader
	buf []byte // ring buffer
	r0  int    // read cursor
	w0  int    // write cursor (== bytes available = w0 - r0)
	pos int64  // total bytes consumed from r
}

func newIndexScanner(r io.Reader) *indexScanner {
	return &indexScanner{r: r, buf: make([]byte, 1<<20)}
}

// fill ensures at least n bytes are available, reading from r as
// needed. Returns io.EOF if the stream ends before n bytes are
// available; partial bytes remain consumable.
func (s *indexScanner) fill(n int) error {
	if s.w0-s.r0 >= n {
		return nil
	}
	// Compact the buffer if needed.
	if s.r0 > 0 {
		copy(s.buf, s.buf[s.r0:s.w0])
		s.w0 -= s.r0
		s.r0 = 0
	}
	for s.w0 < n {
		if s.w0 == len(s.buf) {
			// Buffer too small for the request; grow.
			grown := make([]byte, n*2)
			copy(grown, s.buf[:s.w0])
			s.buf = grown
		}
		k, err := s.r.Read(s.buf[s.w0:])
		s.w0 += k
		if err != nil {
			if err == io.EOF && s.w0 >= n {
				return nil
			}
			return err
		}
	}
	return nil
}

// readUvarint reads a protobuf-style varint from the stream. Returns
// (val, true, nil) on success, (0, false, nil) on clean EOF (no
// bytes available), (0, false, err) on read error.
func (s *indexScanner) readUvarint() (uint64, bool, error) {
	var v uint64
	var shift uint
	for i := 0; i < binary.MaxVarintLen64; i++ {
		if err := s.fill(1); err != nil {
			if err == io.EOF && i == 0 {
				return 0, false, nil // clean EOF
			}
			return 0, false, err
		}
		b := s.buf[s.r0]
		s.r0++
		s.pos++
		if b < 0x80 {
			v |= uint64(b) << shift
			return v, true, nil
		}
		v |= uint64(b&0x7F) << shift
		shift += 7
	}
	return 0, false, fmt.Errorf("varint overflow")
}

// peekByte returns the next byte without consuming it.
func (s *indexScanner) peekByte() (byte, error) {
	if err := s.fill(1); err != nil {
		return 0, err
	}
	return s.buf[s.r0], nil
}

// discard advances pos by n bytes without copying. Reads from the
// source if needed.
func (s *indexScanner) discard(n int64) error {
	for n > 0 {
		avail := int64(s.w0 - s.r0)
		if avail >= n {
			s.r0 += int(n)
			s.pos += n
			return nil
		}
		// Consume what's buffered, then refill.
		s.r0 = 0
		s.w0 = 0
		s.pos += avail
		n -= avail
		// Use Read directly so we don't grow buf for big skips. We
		// do still need to read into a buffer though — use s.buf as
		// scratch.
		k, err := s.r.Read(s.buf)
		s.w0 = k
		if err != nil {
			if err == io.EOF && k == 0 && n > 0 {
				return io.ErrUnexpectedEOF
			}
			if err == io.EOF {
				continue
			}
			return err
		}
	}
	return nil
}

// readStoreName decodes the StoreItem envelope's inner string field
// and returns the store name. envLen is the outer envelope length
// (= total bytes in the StoreItem proto message).
//
// SnapshotItem proto encoding for a StoreItem:
//
//	tag(1<<3|2) || varint(innerLen) || StoreItem
//	  StoreItem = tag(1<<3|2) || varint(nameLen) || name
//
// We've already advanced pos by 0 for envLen — caller is at the
// start of the envelope's content.
func (s *indexScanner) readStoreName(envLen uint64) (string, error) {
	// Snapshot of where the envelope content starts; we'll need to
	// land exactly at start+envLen at the end.
	envStart := s.pos
	envEnd := envStart + int64(envLen)

	// Outer tag for SnapshotStoreItem: (field=1, wire=2) = 0x0A.
	tag, err := s.readByte()
	if err != nil {
		return "", err
	}
	if tag != 0x0A {
		return "", fmt.Errorf("expected store-item tag 0x0A, got 0x%02x", tag)
	}
	innerLen, ok, err := s.readUvarint()
	if err != nil || !ok {
		return "", fmt.Errorf("read inner length: %v", err)
	}

	// Inner StoreItem: a single string field (field=1, wire=2).
	innerEnd := s.pos + int64(innerLen)
	if innerLen == 0 {
		// Empty StoreItem (no name). Possible? Skip rest of envelope.
		if err := s.discard(envEnd - s.pos); err != nil {
			return "", err
		}
		return "", nil
	}
	nameTag, err := s.readByte()
	if err != nil {
		return "", err
	}
	if nameTag != 0x0A {
		// Some other field; advance to envelope end and return empty.
		if err := s.discard(envEnd - s.pos); err != nil {
			return "", err
		}
		return "", nil
	}
	nameLen, ok, err := s.readUvarint()
	if err != nil || !ok {
		return "", fmt.Errorf("read name length: %v", err)
	}
	if nameLen > maxEnvelopeBytes {
		return "", fmt.Errorf("store name %d bytes exceeds cap %d", nameLen, maxEnvelopeBytes)
	}
	if int64(nameLen) > envEnd-s.pos {
		return "", fmt.Errorf("name length %d exceeds envelope remainder", nameLen)
	}
	name := make([]byte, nameLen)
	if err := s.readExact(name); err != nil {
		return "", err
	}
	// Advance past any trailing fields in the inner or outer message.
	if s.pos < innerEnd {
		if err := s.discard(innerEnd - s.pos); err != nil {
			return "", err
		}
	}
	if s.pos < envEnd {
		if err := s.discard(envEnd - s.pos); err != nil {
			return "", err
		}
	}
	return string(name), nil
}

func (s *indexScanner) readByte() (byte, error) {
	if err := s.fill(1); err != nil {
		return 0, err
	}
	b := s.buf[s.r0]
	s.r0++
	s.pos++
	return b, nil
}

func (s *indexScanner) readExact(p []byte) error {
	for len(p) > 0 {
		if err := s.fill(1); err != nil {
			return err
		}
		n := copy(p, s.buf[s.r0:s.w0])
		s.r0 += n
		s.pos += int64(n)
		p = p[n:]
	}
	return nil
}
