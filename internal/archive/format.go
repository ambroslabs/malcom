// Package archive stores cosmoshub block data on disk in a sharded,
// O(1)-lookup binary format.
//
// Layout
//
// Each shard covers a fixed range of [base, base+ChunkSize) heights. There
// are two files per shard:
//
//   <root>/shards/<base zero-padded>.blocks   raw block records
//   <root>/shards/<base zero-padded>.idx      fixed-size index
//
// .blocks file
//
// A concatenation of length-prefixed records:
//
//	repeated {
//	    uint32 length     // big-endian, 0 means a tombstone (skip)
//	    bytes  block      // exactly `length` bytes of cmtproto.Block
//	}
//
// The length prefix is for self-description: if .idx is lost or corrupt
// we can rebuild it by walking .blocks alone.
//
// .idx file
//
//	header (64 bytes):
//	    "CMTBKARC"        magic         8 B
//	    uint8             format ver    1 B
//	    uint8 [7]         reserved      7 B
//	    uint64            base height   8 B
//	    uint32            chunk size    4 B
//	    uint8 [36]        reserved     36 B
//
//	then ChunkSize × IndexEntry (16 bytes each):
//	    uint64            file offset of length prefix in .blocks
//	    uint32            block byte length (0 = absent)
//	    uint32            crc32 of block bytes
//
// Lookup is O(1):
//
//	shard_base = (height / chunk) * chunk
//	idx_off    = headerLen + (height − shard_base) * sizeof(IndexEntry)
//	read 16 B at idx_off, then read length B at offset+4 in .blocks.
//
// Threading: one Shard struct guards its own files with a sync.Mutex.
// Concurrent reads via pread are safe without the mutex.
package archive

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// ChunkSize is the number of heights per shard. Must be > 0.
// 100k yields ~2 GB shards on cosmoshub at ~20 KB/block average.
const ChunkSize = 100_000

const (
	formatMagic    = "CMTBKARC"
	formatVersion  = 1
	headerLen      = 64
	indexEntrySize = 16
	maxBlockBytes  = 1 << 25 // 32 MiB cap; cometbft default block max is ~22 MiB
)

// IndexEntry is one slot in a shard's .idx file. length == 0 means the
// height is absent.
type IndexEntry struct {
	Offset uint64 // byte offset in .blocks where the length prefix sits
	Length uint32 // bytes of the block payload (after the length prefix)
	CRC32  uint32 // crc32 IEEE of the block payload bytes
}

// Marshal/Unmarshal are big-endian for portability.

func (e IndexEntry) MarshalBinary() ([]byte, error) {
	b := make([]byte, indexEntrySize)
	binary.BigEndian.PutUint64(b[0:8], e.Offset)
	binary.BigEndian.PutUint32(b[8:12], e.Length)
	binary.BigEndian.PutUint32(b[12:16], e.CRC32)
	return b, nil
}

func (e *IndexEntry) UnmarshalBinary(b []byte) error {
	if len(b) < indexEntrySize {
		return fmt.Errorf("index entry: short buffer (%d < %d)", len(b), indexEntrySize)
	}
	e.Offset = binary.BigEndian.Uint64(b[0:8])
	e.Length = binary.BigEndian.Uint32(b[8:12])
	e.CRC32 = binary.BigEndian.Uint32(b[12:16])
	return nil
}

// Header is the 64-byte preamble of an .idx file.
type Header struct {
	Version    uint8
	BaseHeight uint64
	ChunkSize  uint32
}

func (h Header) MarshalBinary() ([]byte, error) {
	b := make([]byte, headerLen)
	copy(b[0:8], formatMagic)
	b[8] = h.Version
	// b[9..15] reserved (zero)
	binary.BigEndian.PutUint64(b[16:24], h.BaseHeight)
	binary.BigEndian.PutUint32(b[24:28], h.ChunkSize)
	// b[28..63] reserved (zero)
	return b, nil
}

func (h *Header) UnmarshalBinary(b []byte) error {
	if len(b) < headerLen {
		return fmt.Errorf("idx header: short buffer (%d < %d)", len(b), headerLen)
	}
	if !bytes.Equal(b[0:8], []byte(formatMagic)) {
		return fmt.Errorf("idx header: bad magic %q (want %q)", string(b[0:8]), formatMagic)
	}
	h.Version = b[8]
	if h.Version != formatVersion {
		return fmt.Errorf("idx header: unsupported version %d (want %d)", h.Version, formatVersion)
	}
	h.BaseHeight = binary.BigEndian.Uint64(b[16:24])
	h.ChunkSize = binary.BigEndian.Uint32(b[24:28])
	if h.ChunkSize == 0 {
		return errors.New("idx header: zero chunk size")
	}
	return nil
}

// ShardBase returns the chunk-aligned base height for the shard containing h.
func ShardBase(h uint64) uint64 {
	return (h / ChunkSize) * ChunkSize
}

// ShardSlot returns the index slot (0..ChunkSize-1) for height h.
func ShardSlot(h uint64) int {
	return int(h - ShardBase(h))
}

// IndexOffset is the byte offset in .idx for the slot of height h.
func IndexOffset(h uint64) int64 {
	return int64(headerLen) + int64(ShardSlot(h))*int64(indexEntrySize)
}

// readU32 reads a big-endian uint32 from r.
func readU32(r io.Reader) (uint32, error) {
	var b [4]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(b[:]), nil
}

func writeU32(w io.Writer, v uint32) error {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], v)
	_, err := w.Write(b[:])
	return err
}
