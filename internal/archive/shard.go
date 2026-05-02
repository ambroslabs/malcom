package archive

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

// ErrNotPresent is returned when a height has no entry in the shard.
var ErrNotPresent = errors.New("archive: block not present")

// Shard is one (base, base+ChunkSize) pair of files. Use Open or Create.
type Shard struct {
	base       uint64
	blocksPath string
	idxPath    string

	mu     sync.Mutex
	blocks *os.File
	idx    *os.File
}

// Open opens a shard for reading and writing. Creates files if they don't
// exist. The .idx is initialized with an empty header + zeroed entries.
func Open(blocksPath, idxPath string, base uint64) (*Shard, error) {
	bf, err := os.OpenFile(blocksPath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open blocks: %w", err)
	}
	idxNew := false
	idxStat, err := os.Stat(idxPath)
	if err != nil {
		if !os.IsNotExist(err) {
			_ = bf.Close()
			return nil, err
		}
		idxNew = true
	} else if idxStat.Size() < int64(headerLen) {
		idxNew = true
	}

	idxF, err := os.OpenFile(idxPath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		_ = bf.Close()
		return nil, fmt.Errorf("open idx: %w", err)
	}

	if idxNew {
		// Write header.
		hdr, _ := Header{
			Version:    formatVersion,
			BaseHeight: base,
			ChunkSize:  ChunkSize,
		}.MarshalBinary()
		if _, err := idxF.WriteAt(hdr, 0); err != nil {
			_ = bf.Close()
			_ = idxF.Close()
			return nil, fmt.Errorf("write header: %w", err)
		}
		// Pre-extend to full size with zero entries.
		full := int64(headerLen) + int64(ChunkSize)*int64(indexEntrySize)
		if err := idxF.Truncate(full); err != nil {
			_ = bf.Close()
			_ = idxF.Close()
			return nil, fmt.Errorf("truncate idx: %w", err)
		}
	} else {
		// Validate header.
		var hdr Header
		buf := make([]byte, headerLen)
		if _, err := idxF.ReadAt(buf, 0); err != nil {
			_ = bf.Close()
			_ = idxF.Close()
			return nil, fmt.Errorf("read header: %w", err)
		}
		if err := hdr.UnmarshalBinary(buf); err != nil {
			_ = bf.Close()
			_ = idxF.Close()
			return nil, err
		}
		if hdr.BaseHeight != base {
			_ = bf.Close()
			_ = idxF.Close()
			return nil, fmt.Errorf("idx base mismatch: file=%d want=%d", hdr.BaseHeight, base)
		}
		if uint32(ChunkSize) != hdr.ChunkSize {
			_ = bf.Close()
			_ = idxF.Close()
			return nil, fmt.Errorf("idx chunk size mismatch: file=%d want=%d", hdr.ChunkSize, ChunkSize)
		}
	}

	return &Shard{
		base:       base,
		blocksPath: blocksPath,
		idxPath:    idxPath,
		blocks:     bf,
		idx:        idxF,
	}, nil
}

// Base returns the chunk-aligned base height of the shard.
func (s *Shard) Base() uint64 { return s.base }

// Close releases the file handles.
func (s *Shard) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var firstErr error
	if s.blocks != nil {
		if err := s.blocks.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		s.blocks = nil
	}
	if s.idx != nil {
		if err := s.idx.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		s.idx = nil
	}
	return firstErr
}

// Has reports whether height h has a non-zero entry in this shard.
func (s *Shard) Has(h uint64) bool {
	if h < s.base || h >= s.base+ChunkSize {
		return false
	}
	e, err := s.entry(h)
	return err == nil && e.Length > 0
}

// Get reads and returns the raw block bytes for height h, or ErrNotPresent.
// Verifies the on-disk CRC matches the index.
func (s *Shard) Get(h uint64) ([]byte, error) {
	if h < s.base || h >= s.base+ChunkSize {
		return nil, fmt.Errorf("height %d outside shard [%d, %d)", h, s.base, s.base+ChunkSize)
	}
	e, err := s.entry(h)
	if err != nil {
		return nil, err
	}
	if e.Length == 0 {
		return nil, ErrNotPresent
	}
	if e.Length > maxBlockBytes {
		return nil, fmt.Errorf(
			"absurd block length %d at height %d  (file=%s offset=%d  idx may be corrupt)",
			e.Length, h, s.blocksPath, int64(e.Offset)+4)
	}
	// .blocks layout: [u32 length][block bytes]. Skip the prefix.
	buf := make([]byte, e.Length)
	if _, err := s.blocks.ReadAt(buf, int64(e.Offset)+4); err != nil {
		return nil, fmt.Errorf(
			"read block bytes at height %d: %w  (file=%s offset=%d length=%d)",
			h, err, s.blocksPath, int64(e.Offset)+4, e.Length)
	}
	if got := crc32.ChecksumIEEE(buf); got != e.CRC32 {
		return nil, fmt.Errorf(
			"crc mismatch at height %d: stored=%08x got=%08x  (file=%s offset=%d length=%d)",
			h, e.CRC32, got, s.blocksPath, int64(e.Offset)+4, e.Length)
	}
	return buf, nil
}

// Put writes block bytes for height h. Idempotent — if the height is
// already present with the same CRC, returns nil without writing again.
// If present with a different CRC, returns an error (refuses to overwrite).
func (s *Shard) Put(h uint64, blockBytes []byte) error {
	if h < s.base || h >= s.base+ChunkSize {
		return fmt.Errorf("height %d outside shard [%d, %d)", h, s.base, s.base+ChunkSize)
	}
	if len(blockBytes) == 0 {
		return errors.New("empty block")
	}
	if len(blockBytes) > maxBlockBytes {
		return fmt.Errorf("block too large: %d bytes (max %d)", len(blockBytes), maxBlockBytes)
	}

	crc := crc32.ChecksumIEEE(blockBytes)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Check existing entry.
	existing, err := s.readEntryLocked(h)
	if err != nil {
		return err
	}
	if existing.Length > 0 {
		if existing.Length == uint32(len(blockBytes)) && existing.CRC32 == crc {
			return nil // already have it; idempotent
		}
		return fmt.Errorf("height %d already present with different content", h)
	}

	// Append [u32 length][block bytes] to .blocks.
	off, err := s.blocks.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("seek blocks: %w", err)
	}
	var lenPrefix [4]byte
	binary.BigEndian.PutUint32(lenPrefix[:], uint32(len(blockBytes)))
	if _, err := s.blocks.Write(lenPrefix[:]); err != nil {
		return fmt.Errorf("write length prefix: %w", err)
	}
	if _, err := s.blocks.Write(blockBytes); err != nil {
		return fmt.Errorf("write block: %w", err)
	}

	// Update index entry.
	e := IndexEntry{
		Offset: uint64(off),
		Length: uint32(len(blockBytes)),
		CRC32:  crc,
	}
	buf, _ := e.MarshalBinary()
	if _, err := s.idx.WriteAt(buf, IndexOffset(h)); err != nil {
		return fmt.Errorf("write idx entry: %w", err)
	}
	return nil
}

// Sync calls fsync on both files. Cheap to call after a batch of Puts.
func (s *Shard) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.blocks.Sync(); err != nil {
		return err
	}
	return s.idx.Sync()
}

// entry reads one IndexEntry without locking (uses pread).
func (s *Shard) entry(h uint64) (IndexEntry, error) {
	var buf [indexEntrySize]byte
	if _, err := s.idx.ReadAt(buf[:], IndexOffset(h)); err != nil {
		return IndexEntry{}, fmt.Errorf("read idx: %w", err)
	}
	var e IndexEntry
	if err := e.UnmarshalBinary(buf[:]); err != nil {
		return IndexEntry{}, err
	}
	return e, nil
}

// readEntryLocked is entry() but for callers already holding s.mu.
func (s *Shard) readEntryLocked(h uint64) (IndexEntry, error) {
	return s.entry(h) // no extra synchronization needed for ReadAt
}

// AllEntries scans every slot and yields (height, entry) for present heights
// only (Length > 0). Allocates a single 1.6 MB buffer for the scan.
func (s *Shard) AllEntries(yield func(height uint64, e IndexEntry) bool) error {
	full := int64(ChunkSize) * int64(indexEntrySize)
	buf := make([]byte, full)
	if _, err := s.idx.ReadAt(buf, int64(headerLen)); err != nil && err != io.EOF {
		return fmt.Errorf("read full idx: %w", err)
	}
	for i := 0; i < ChunkSize; i++ {
		var e IndexEntry
		_ = e.UnmarshalBinary(buf[i*indexEntrySize:])
		if e.Length == 0 {
			continue
		}
		if !yield(s.base+uint64(i), e) {
			return nil
		}
	}
	return nil
}

// PresentRange returns the (min, max, count) of present heights in this
// shard. count == 0 means the shard is empty (min, max meaningless).
func (s *Shard) PresentRange() (min, max uint64, count int) {
	full := int64(ChunkSize) * int64(indexEntrySize)
	buf := make([]byte, full)
	if _, err := s.idx.ReadAt(buf, int64(headerLen)); err != nil && err != io.EOF {
		return 0, 0, 0
	}
	first := -1
	last := -1
	for i := 0; i < ChunkSize; i++ {
		// length is at offset i*16+8, 4 bytes
		l := binary.BigEndian.Uint32(buf[i*indexEntrySize+8:])
		if l == 0 {
			continue
		}
		if first < 0 {
			first = i
		}
		last = i
		count++
	}
	if count == 0 {
		return 0, 0, 0
	}
	return s.base + uint64(first), s.base + uint64(last), count
}

// FSCKReport is the output of WalkBlocks.
type FSCKReport struct {
	BlocksPath string
	IdxPath    string

	// IdxEntries: number of slots with length>0 in .idx
	IdxEntries int
	// IdxBadEntries: idx entries whose length > maxBlockBytes (corrupt)
	IdxBadEntries int

	// .blocks self-walk via length prefixes
	BlocksFileSize int64
	WalkRecords    int
	WalkLastEnd    int64
	WalkErrors     []string

	// Cross-checks (idx vs walk)
	OrphanBytesAfterWalk int64 // bytes in .blocks past the last record we could parse
	OrphanRecords        int   // records found in walk but no matching idx entry
	IdxNotInBlocks       int   // idx entries pointing past EOF
	IdxLengthMismatch    int   // idx says length=L but length-prefix says L'

	// CRC checks (limited to MaxCRC, otherwise the caller can run verify)
	CRCChecked int
	CRCBad     int

	// Fatal: per-shard go-no-go.
	Errors []string
}

// FSCK runs a deep consistency check on this shard:
//   - Walks .blocks from offset 0 using length prefixes (independent of .idx).
//   - For each .blocks record, looks up the corresponding .idx entry by
//     scanning idx entries with matching offset.
//   - Counts orphan bytes (in .blocks but unreachable via .idx).
//   - Counts idx entries that point past EOF or have absurd lengths.
//
// maxCRC limits how many records get a full CRC32 check (-1 = all).
func (s *Shard) FSCK(maxCRC int) (FSCKReport, error) {
	rep := FSCKReport{
		BlocksPath: s.blocksPath,
		IdxPath:    s.idxPath,
	}
	bs, err := s.blocks.Stat()
	if err != nil {
		return rep, fmt.Errorf("stat .blocks: %w", err)
	}
	rep.BlocksFileSize = bs.Size()

	// Build a map of expected idx entries by offset, for cross-check.
	full := int64(ChunkSize) * int64(indexEntrySize)
	idxBuf := make([]byte, full)
	if _, err := s.idx.ReadAt(idxBuf, int64(headerLen)); err != nil && err != io.EOF {
		return rep, fmt.Errorf("read full idx: %w", err)
	}
	type idxRef struct {
		height uint64
		entry  IndexEntry
	}
	byOffset := make(map[uint64]idxRef, ChunkSize/4)
	for i := 0; i < ChunkSize; i++ {
		var e IndexEntry
		_ = e.UnmarshalBinary(idxBuf[i*indexEntrySize:])
		if e.Length == 0 {
			continue
		}
		rep.IdxEntries++
		if e.Length > maxBlockBytes {
			rep.IdxBadEntries++
			rep.Errors = append(rep.Errors, fmt.Sprintf(
				"idx entry h=%d has absurd length %d", s.base+uint64(i), e.Length))
			continue
		}
		// idx points past EOF?
		needEnd := int64(e.Offset) + 4 + int64(e.Length)
		if needEnd > rep.BlocksFileSize {
			rep.IdxNotInBlocks++
			rep.Errors = append(rep.Errors, fmt.Sprintf(
				"idx entry h=%d points past EOF: needs offset %d..%d, file size %d",
				s.base+uint64(i), e.Offset, needEnd, rep.BlocksFileSize))
			continue
		}
		byOffset[e.Offset] = idxRef{height: s.base + uint64(i), entry: e}
	}

	// Walk .blocks from 0, following length prefixes.
	var off int64
	for off < rep.BlocksFileSize {
		var lenPrefix [4]byte
		if _, err := s.blocks.ReadAt(lenPrefix[:], off); err != nil {
			rep.WalkErrors = append(rep.WalkErrors, fmt.Sprintf(
				"can't read length prefix at offset %d: %v", off, err))
			break
		}
		recLen := int64(binary.BigEndian.Uint32(lenPrefix[:]))
		if recLen <= 0 || recLen > maxBlockBytes {
			rep.WalkErrors = append(rep.WalkErrors, fmt.Sprintf(
				"bad length prefix %d at offset %d (.blocks may be truncated/corrupt here)",
				recLen, off))
			break
		}
		nextOff := off + 4 + recLen
		if nextOff > rep.BlocksFileSize {
			rep.WalkErrors = append(rep.WalkErrors, fmt.Sprintf(
				"record at offset %d claims length %d but extends past EOF (file size %d)",
				off, recLen, rep.BlocksFileSize))
			break
		}

		// Cross-check against idx.
		if ref, ok := byOffset[uint64(off)]; ok {
			if uint32(recLen) != ref.entry.Length {
				rep.IdxLengthMismatch++
				rep.Errors = append(rep.Errors, fmt.Sprintf(
					"idx vs .blocks length mismatch at offset %d (h=%d): idx says %d, length-prefix says %d",
					off, ref.height, ref.entry.Length, recLen))
			}
			if maxCRC < 0 || rep.CRCChecked < maxCRC {
				buf := make([]byte, recLen)
				if _, err := s.blocks.ReadAt(buf, off+4); err == nil {
					if got := crc32.ChecksumIEEE(buf); got != ref.entry.CRC32 {
						rep.CRCBad++
						rep.Errors = append(rep.Errors, fmt.Sprintf(
							"crc mismatch at offset %d (h=%d): stored=%08x got=%08x",
							off, ref.height, ref.entry.CRC32, got))
					}
					rep.CRCChecked++
				}
			}
			delete(byOffset, uint64(off))
		} else {
			rep.OrphanRecords++
			rep.Errors = append(rep.Errors, fmt.Sprintf(
				"orphan record at offset %d length %d — present in .blocks but no idx entry points here",
				off, recLen))
		}

		rep.WalkRecords++
		rep.WalkLastEnd = nextOff
		off = nextOff
	}

	// Anything left in byOffset = idx-claimed records we couldn't find via walk.
	for _, ref := range byOffset {
		rep.Errors = append(rep.Errors, fmt.Sprintf(
			"idx says h=%d at offset %d length %d, but blocks-walk never reached that offset",
			ref.height, ref.entry.Offset, ref.entry.Length))
	}

	// Trailing bytes after the last successful walk record (no records expected past here).
	if rep.WalkLastEnd < rep.BlocksFileSize {
		rep.OrphanBytesAfterWalk = rep.BlocksFileSize - rep.WalkLastEnd
	}

	return rep, nil
}

// PresentBitmap returns a slice of length ChunkSize where bit i is 1 if
// height base+i is present. Allocates ChunkSize/8 bytes.
func (s *Shard) PresentBitmap() ([]byte, error) {
	full := int64(ChunkSize) * int64(indexEntrySize)
	buf := make([]byte, full)
	if _, err := s.idx.ReadAt(buf, int64(headerLen)); err != nil && err != io.EOF {
		return nil, err
	}
	bm := make([]byte, (ChunkSize+7)/8)
	for i := 0; i < ChunkSize; i++ {
		if binary.BigEndian.Uint32(buf[i*indexEntrySize+8:]) > 0 {
			bm[i/8] |= 1 << (uint(i) % 8)
		}
	}
	return bm, nil
}
