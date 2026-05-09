package snapshotdiff

import (
	"bufio"
	"compress/zlib"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/cockroachdb/pebble"

	malcomlog "github.com/zrbecker/cosmos-p2p/internal/log"
)

// StateSyncStats summarises a CSDS diff after generation.
type StateSyncStats struct {
	BaseItems       uint64
	TargetItems     uint64
	Refs            uint64 // target items that matched a base item
	Literals        uint64 // target items that didn't match
	LiteralBytes    uint64 // raw bytes of literal data
	BodyBytesRaw    uint64
	BodyBytesGz     uint64
}

// ComputeStateSync produces a CSDS-format diff that captures the full
// SnapshotItem stream of target as content-addressed delta against base.
//
// Disk requirements (under tmpDir):
//   - Pebble index of base hashes ~= numBaseItems * (32 + 8) * Pebble overhead.
//     For cosmoshub-4 at ~148M items that's ~6 GB raw, ~10–15 GB on disk.
//
// The tmp dir is left in place if keepTmp is true; otherwise the caller
// should ensure it's removed (newChunkWriter and friends don't touch it).
func ComputeStateSync(
	baseDir, targetDir, outPath, tmpDir string,
	baseHashHex, targetHashHex string,
	baseHeight, targetHeight uint64,
	log *slog.Logger,
) (StateSyncStats, error) {
	var stats StateSyncStats
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}

	dbPath := filepath.Join(tmpDir, "base-index")
	if err := os.MkdirAll(dbPath, 0o755); err != nil {
		return stats, fmt.Errorf("mkdir base-index: %w", err)
	}
	db, err := pebble.Open(dbPath, &pebble.Options{
		DisableWAL:   true,
		MemTableSize: 64 << 20,
		Cache:        pebble.NewCache(128 << 20),
		Logger:       malcomlog.PebbleShim(log.With("module", "pebble")),
	})
	if err != nil {
		return stats, fmt.Errorf("open pebble: %w", err)
	}
	defer db.Close()

	// Phase 1: walk base, build hash → index in Pebble.
	baseRdr, err := newSnapItemReader(baseDir)
	if err != nil {
		return stats, fmt.Errorf("open base: %w", err)
	}
	defer baseRdr.Close()

	batch := db.NewBatch()
	const flushEvery = 4 << 20 // 4 MB batch
	var idx uint64
	for {
		item, eof, err := baseRdr.Next()
		if err != nil {
			return stats, fmt.Errorf("read base: %w", err)
		}
		if eof {
			break
		}
		h := sha256.Sum256(item)
		var idxBuf [8]byte
		binary.LittleEndian.PutUint64(idxBuf[:], idx)
		if err := batch.Set(h[:], idxBuf[:], nil); err != nil {
			return stats, err
		}
		idx++
		if batch.Len() > flushEvery {
			if err := db.Apply(batch, pebble.NoSync); err != nil {
				return stats, err
			}
			batch = db.NewBatch()
		}
	}
	if batch.Len() > 0 {
		if err := db.Apply(batch, pebble.NoSync); err != nil {
			return stats, err
		}
	}
	stats.BaseItems = idx

	// Phase 2: walk target, emit refs + literals.
	out, err := os.Create(outPath)
	if err != nil {
		return stats, fmt.Errorf("create %s: %w", outPath, err)
	}
	defer out.Close()
	hdr := StateSyncHeader{
		BaseHeight:    baseHeight,
		TargetHeight:  targetHeight,
		BaseHashHex:   baseHashHex,
		TargetHashHex: targetHashHex,
		// TargetItemCount filled in below; we'll seek back and rewrite.
	}
	headerEnd, err := writeStateSyncHeaderPlaceholder(out, hdr)
	if err != nil {
		return stats, err
	}

	rawCtr := &countWriter{w: out}
	zw, err := zlib.NewWriterLevel(rawCtr, zlib.DefaultCompression)
	if err != nil {
		return stats, err
	}
	defer zw.Close()
	gzCtr := &countWriter{w: zw}
	bw := bufio.NewWriterSize(gzCtr, 1<<20)

	emitOp := func(op byte, payload ...[]byte) error {
		if err := bw.WriteByte(op); err != nil {
			return err
		}
		for _, p := range payload {
			if _, err := bw.Write(p); err != nil {
				return err
			}
		}
		return nil
	}
	encVarint := func(v uint64) []byte {
		var buf [binary.MaxVarintLen64]byte
		n := binary.PutUvarint(buf[:], v)
		return buf[:n]
	}

	targetRdr, err := newSnapItemReader(targetDir)
	if err != nil {
		return stats, err
	}
	defer targetRdr.Close()

	for {
		item, eof, err := targetRdr.Next()
		if err != nil {
			return stats, fmt.Errorf("read target: %w", err)
		}
		if eof {
			break
		}
		stats.TargetItems++
		h := sha256.Sum256(item)
		val, closer, err := db.Get(h[:])
		if err == nil {
			baseIdx := binary.LittleEndian.Uint64(val)
			closer.Close()
			if err := emitOp(OpRef, encVarint(baseIdx)); err != nil {
				return stats, err
			}
			stats.Refs++
		} else if err == pebble.ErrNotFound {
			if err := emitOp(OpLiteral, encVarint(uint64(len(item))), item); err != nil {
				return stats, err
			}
			stats.Literals++
			stats.LiteralBytes += uint64(len(item))
		} else {
			return stats, fmt.Errorf("pebble get: %w", err)
		}
	}

	if err := bw.Flush(); err != nil {
		return stats, err
	}
	if err := zw.Close(); err != nil {
		return stats, err
	}
	stats.BodyBytesRaw = gzCtr.n
	stats.BodyBytesGz = rawCtr.n

	// Rewrite header with final TargetItemCount.
	hdr.TargetItemCount = stats.TargetItems
	if err := rewriteStateSyncHeader(out, hdr, headerEnd); err != nil {
		return stats, err
	}

	return stats, nil
}

// writeStateSyncHeaderPlaceholder writes a header (with TargetItemCount=0)
// and returns the file offset just past the header — used to seek back
// and patch the count after we know it.
func writeStateSyncHeaderPlaceholder(f *os.File, h StateSyncHeader) (int64, error) {
	if err := WriteStateSyncHeader(f, h); err != nil {
		return 0, err
	}
	off, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	return off, nil
}

func rewriteStateSyncHeader(f *os.File, h StateSyncHeader, _ int64) error {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	// Re-write only the bytes that actually fit in the original header
	// region (its size is deterministic for fixed hash-hex lengths).
	return WriteStateSyncHeader(f, h)
}

// snapItemReader walks a snapshot directory chunk_*.bin → zlib → SnapshotItems
// stream, returning each item's full bytes (the message body after the
// outer length prefix). Items are emitted in their natural snapshot order.
type snapItemReader struct {
	files []*os.File
	zr    io.ReadCloser
	br    *bufio.Reader
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
	multi := io.MultiReader(readers...)
	zr, err := zlib.NewReader(multi)
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

// Next returns the bytes of the next SnapshotItem (the inner proto, after
// the outer length prefix). Returns eof=true at end of stream.
func (s *snapItemReader) Next() (item []byte, eof bool, err error) {
	length, err := binary.ReadUvarint(s.br)
	if err == io.EOF {
		return nil, true, nil
	}
	if err != nil {
		return nil, false, err
	}
	if length == 0 {
		return s.Next()
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(s.br, buf); err != nil {
		return nil, false, err
	}
	return buf, false, nil
}
