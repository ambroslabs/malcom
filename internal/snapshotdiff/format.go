// Package snapshotdiff produces and applies compact diffs between two
// cosmos-sdk format-3 state-sync snapshots. A diff is much smaller than the
// underlying snapshots — typically <2% of snapshot size for ~10K-block
// gaps — making it cheap to store many historical anchors.
//
// File format (.diff):
//
//	HEADER (uncompressed, fixed 9 bytes + variable):
//	  4 bytes  "CSDF"            magic
//	  1 byte   version           = 1
//	  8 bytes  base_height       little-endian uint64
//	  8 bytes  target_height     little-endian uint64
//	  4 bytes  base_hash_len     little-endian uint32
//	  N bytes  base_hash         (hex-encoded snapshot.Hash of base, for safety)
//	  4 bytes  target_hash_len   little-endian uint32
//	  N bytes  target_hash       (hex-encoded snapshot.Hash of target)
//
//	BODY (zlib-compressed stream of varint-length-delimited Record protos):
//	  Record is a oneof — one of these per record:
//	    field 1: StoreEnter { string name }
//	    field 2: StoreLeave { }
//	    field 3: Insert { bytes key, bytes value }
//	    field 4: Update { bytes key, bytes value }
//	    field 5: Delete { bytes key }
//	    field 6: ExtEnter { string name, uint32 format }
//	    field 7: ExtLeave { }
//	    field 8: ExtAdd { bytes payload }
//	    field 9: ExtRemove { bytes payload }
//
// Diff scope:
//   - We compare logical state (key, value), ignoring IAVL version metadata.
//     Version numbers in a reconstructed snapshot will all be set to the
//     target_height on apply, which is correct for state-sync purposes.
//   - Within each store, we maintain a map of keys → values for the target
//     snapshot, then walk the base snapshot finding mismatches. This costs
//     O(target_store_size) memory per store but avoids an O(N²) scan.
package snapshotdiff

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	Magic      = "CSDF"
	FormatVersion = 1
)

// Record field tags (top-level oneof in our pseudo-proto).
const (
	tagStoreEnter = 1
	tagStoreLeave = 2
	tagInsert     = 3
	tagUpdate     = 4
	tagDelete     = 5
	tagExtEnter   = 6
	tagExtLeave   = 7
	tagExtAdd     = 8
	tagExtRemove  = 9
)

// FileHeader carries diff metadata. Stored uncompressed at the start of the
// file so a reader can validate base/target before paying decompression cost.
type FileHeader struct {
	BaseHeight   uint64
	TargetHeight uint64
	BaseHashHex  string
	TargetHashHex string
}

func WriteHeader(w io.Writer, h FileHeader) error {
	if _, err := io.WriteString(w, Magic); err != nil {
		return err
	}
	if _, err := w.Write([]byte{FormatVersion}); err != nil {
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
	return nil
}

func ReadHeader(r io.Reader) (FileHeader, error) {
	var h FileHeader
	magic := make([]byte, 4)
	if _, err := io.ReadFull(r, magic); err != nil {
		return h, fmt.Errorf("read magic: %w", err)
	}
	if string(magic) != Magic {
		return h, fmt.Errorf("bad magic %q (want %q)", magic, Magic)
	}
	var ver [1]byte
	if _, err := io.ReadFull(r, ver[:]); err != nil {
		return h, fmt.Errorf("read version: %w", err)
	}
	if ver[0] != FormatVersion {
		return h, fmt.Errorf("unknown diff format version %d", ver[0])
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
	return h, nil
}

func writeLenPrefixed(w io.Writer, b []byte) error {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], uint32(len(b)))
	if _, err := w.Write(buf[:]); err != nil {
		return err
	}
	if _, err := w.Write(b); err != nil {
		return err
	}
	return nil
}

func readLenPrefixed(r io.Reader) ([]byte, error) {
	var buf [4]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return nil, err
	}
	n := binary.LittleEndian.Uint32(buf[:])
	if n > 1<<20 {
		return nil, fmt.Errorf("len-prefix unreasonably large: %d", n)
	}
	out := make([]byte, n)
	if _, err := io.ReadFull(r, out); err != nil {
		return nil, err
	}
	return out, nil
}

// uvarintSize returns how many bytes an unsigned varint takes.
func uvarintSize(x uint64) int {
	var b [10]byte
	return binary.PutUvarint(b[:], x)
}
