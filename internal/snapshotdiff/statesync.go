// Package snapshotdiff state-sync mode.
//
// The "state-sync" diff format (magic "CSDS") differs from the leaf-only
// "CSDF" format: it captures the full SnapshotItem stream of the target
// snapshot, with content-addressed delta encoding against the base. The
// resulting diff is bigger than CSDF (~50% of target size) but the
// reconstructed snapshot is byte-equivalent to a producer-side cosmos-sdk
// snapshot — chunk_*.bin files match expected chunk hashes, and the
// imported AppHash matches consensus. That makes it usable for state-sync
// over P2P.
//
// File format (.diff):
//
//	HEADER (uncompressed):
//	  4 bytes  "CSDS"            magic
//	  1 byte   version           = 1
//	  8 bytes  base_height       little-endian uint64
//	  8 bytes  target_height     little-endian uint64
//	  4 bytes  base_hash_len     little-endian uint32
//	  N bytes  base_hash         (hex of base meta.json hash_hex)
//	  4 bytes  target_hash_len   little-endian uint32
//	  N bytes  target_hash       (hex of target meta.json hash_hex)
//	  8 bytes  target_item_count little-endian uint64
//
//	BODY (zlib-compressed):
//	  Sequence of operations, each prefixed with a single-byte tag:
//	    0x00 REF      varint base_index
//	    0x01 LITERAL  varint length, length bytes (raw SnapshotItem bytes,
//	                  i.e. what is between successive recLen prefixes in
//	                  the snapshot stream)
//
// During apply, every operation contributes one SnapshotItem to the
// output stream in order. Output is then split into 10 MiB chunks,
// matching cosmos-sdk's default snapshot chunk size, so chunk hashes line
// up with what the receiving node expects (assuming same Go zlib version
// produced both sides; this happens to be the case in our environment).
package snapshotdiff

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	StateSyncMagic         = "CSDS"
	StateSyncFormatVersion = 1

	OpRef     = 0x00
	OpLiteral = 0x01

	// CosmosSDKChunkSize is the default snapshot chunk size used by
	// cosmos-sdk (10 MiB). State-sync producers use this; we match it so
	// our reconstructed chunks line up.
	CosmosSDKChunkSize = 10 << 20
)

// StateSyncHeader is the metadata at the start of a CSDS diff.
type StateSyncHeader struct {
	BaseHeight      uint64
	TargetHeight    uint64
	BaseHashHex     string
	TargetHashHex   string
	TargetItemCount uint64
}

func WriteStateSyncHeader(w io.Writer, h StateSyncHeader) error {
	if _, err := io.WriteString(w, StateSyncMagic); err != nil {
		return err
	}
	if _, err := w.Write([]byte{StateSyncFormatVersion}); err != nil {
		return err
	}
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], h.BaseHeight)
	if _, err := w.Write(buf[:]); err != nil {
		return err
	}
	binary.LittleEndian.PutUint64(buf[:], h.TargetHeight)
	if _, err := w.Write(buf[:]); err != nil {
		return err
	}
	if err := writeLenPrefixed(w, []byte(h.BaseHashHex)); err != nil {
		return err
	}
	if err := writeLenPrefixed(w, []byte(h.TargetHashHex)); err != nil {
		return err
	}
	binary.LittleEndian.PutUint64(buf[:], h.TargetItemCount)
	if _, err := w.Write(buf[:]); err != nil {
		return err
	}
	return nil
}

func ReadStateSyncHeader(r io.Reader) (StateSyncHeader, error) {
	var h StateSyncHeader
	magic := make([]byte, 4)
	if _, err := io.ReadFull(r, magic); err != nil {
		return h, fmt.Errorf("read magic: %w", err)
	}
	if string(magic) != StateSyncMagic {
		return h, fmt.Errorf("bad magic %q (want %q)", magic, StateSyncMagic)
	}
	var ver [1]byte
	if _, err := io.ReadFull(r, ver[:]); err != nil {
		return h, err
	}
	if ver[0] != StateSyncFormatVersion {
		return h, fmt.Errorf("unknown CSDS format version %d", ver[0])
	}
	var u8 [8]byte
	if _, err := io.ReadFull(r, u8[:]); err != nil {
		return h, err
	}
	h.BaseHeight = binary.LittleEndian.Uint64(u8[:])
	if _, err := io.ReadFull(r, u8[:]); err != nil {
		return h, err
	}
	h.TargetHeight = binary.LittleEndian.Uint64(u8[:])
	bh, err := readLenPrefixed(r)
	if err != nil {
		return h, err
	}
	h.BaseHashHex = string(bh)
	th, err := readLenPrefixed(r)
	if err != nil {
		return h, err
	}
	h.TargetHashHex = string(th)
	if _, err := io.ReadFull(r, u8[:]); err != nil {
		return h, err
	}
	h.TargetItemCount = binary.LittleEndian.Uint64(u8[:])
	return h, nil
}
