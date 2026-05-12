// Package snapshotinspect decodes a cosmos-sdk format-3 state-sync snapshot
// stored on disk as a directory of zstd-compressed chunks, and reports the
// per-store / per-extension structure.
//
// We hand-roll the proto parser so we don't drag cosmos-sdk into the build.
// The on-wire format is documented in cosmossdk.io/store/snapshots/types:
//
//	stream of length-delimited SnapshotItem proto messages, where each
//	SnapshotItem is a oneof:
//	  field 1: SnapshotStoreItem        { string name }
//	  field 2: SnapshotIAVLItem         { bytes key, bytes value, int32 version, int32 height }
//	  field 3: SnapshotExtensionMeta    { string name, uint32 format }
//	  field 4: SnapshotExtensionPayload { bytes payload }
//	  field 5: SnapshotKVItem           (older format, unused in format=3)
//	  field 6: SnapshotSchema
//
// Chunks are arbitrary cuts of the compressed stream (NOT aligned with
// SnapshotItem boundaries), so we concatenate all chunk files and feed the
// result to a single zstd decoder.
package snapshotinspect

import (
	"bufio"
	"compress/zlib"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/ambroslabs/malcom/internal/humanbytes"
)

// Result is the structural summary of a parsed snapshot.
type Result struct {
	Stores              []StoreInfo     `json:"stores"`
	Extensions          []ExtensionInfo `json:"extensions"`
	TotalItems          uint64          `json:"total_items"`
	DecompressedBytes   uint64          `json:"decompressed_bytes"`
	UnknownItemTags     map[uint8]uint64 `json:"unknown_item_tags,omitempty"`
}

type StoreInfo struct {
	Name              string `json:"name"`
	Items             uint64 `json:"items"`
	BytesUncompressed uint64 `json:"bytes"`
	BytesHuman        string `json:"bytes_human"`
}

type ExtensionInfo struct {
	Name              string `json:"name"`
	Format            uint32 `json:"format"`
	Payloads          uint64 `json:"payloads"`
	BytesUncompressed uint64 `json:"bytes"`
	BytesHuman        string `json:"bytes_human"`
}

// Inspect reads chunk_NNNNN.bin files from dir, decompresses, parses, and
// returns structural info. Streams through the data without buffering the
// full decompressed payload (which can be tens of GB).
func Inspect(dir string) (Result, error) {
	var res Result
	res.UnknownItemTags = map[uint8]uint64{}

	chunks, err := listChunks(dir)
	if err != nil {
		return res, err
	}
	if len(chunks) == 0 {
		return res, fmt.Errorf("no chunk files found in %s", dir)
	}

	// Open every chunk in order; concatenate into one io.Reader.
	files := make([]*os.File, 0, len(chunks))
	defer func() {
		for _, f := range files {
			f.Close()
		}
	}()
	readers := make([]io.Reader, 0, len(chunks))
	for _, p := range chunks {
		f, err := os.Open(p)
		if err != nil {
			return res, fmt.Errorf("open %s: %w", p, err)
		}
		files = append(files, f)
		readers = append(readers, f)
	}
	multi := io.MultiReader(readers...)

	// zlib decoder over the concatenated chunk stream. cosmos-sdk's snapshot
	// manager uses compress/zlib (NOT zstd) for the chunk payload.
	zr, err := zlib.NewReader(multi)
	if err != nil {
		return res, fmt.Errorf("zlib reader: %w", err)
	}
	defer zr.Close()

	// Buffered for efficient byte reads when decoding varints.
	br := bufio.NewReaderSize(zr, 1<<20)

	var (
		curStore *StoreInfo
		curExt   *ExtensionInfo
	)

	for {
		// Read varint length of next SnapshotItem.
		length, err := binary.ReadUvarint(br)
		if err == io.EOF {
			break
		}
		if err != nil {
			return res, fmt.Errorf("read item length: %w", err)
		}
		if length == 0 {
			continue
		}
		// Read item bytes (may be large for IAVL leaves with big values, but
		// rare; cosmoshub leaves cap around the SDK's per-leaf max).
		buf := make([]byte, length)
		if _, err := io.ReadFull(br, buf); err != nil {
			return res, fmt.Errorf("read item: %w", err)
		}
		res.TotalItems++
		res.DecompressedBytes += uint64(length) + uint64(uvarintSize(length))

		// First byte = SnapshotItem oneof field tag (field-num<<3 | wire-type).
		tag := buf[0]
		fieldNum := tag >> 3

		switch fieldNum {
		case 1: // SnapshotStoreItem
			name := parseStoreName(buf[1:])
			res.Stores = append(res.Stores, StoreInfo{Name: name})
			curStore = &res.Stores[len(res.Stores)-1]
			curExt = nil
		case 2: // SnapshotIAVLItem — most items are these
			if curStore != nil {
				curStore.Items++
				curStore.BytesUncompressed += uint64(length) + uint64(uvarintSize(length))
			}
		case 3: // SnapshotExtensionMeta
			name, format := parseExtMeta(buf[1:])
			res.Extensions = append(res.Extensions, ExtensionInfo{Name: name, Format: format})
			curExt = &res.Extensions[len(res.Extensions)-1]
			curStore = nil
		case 4: // SnapshotExtensionPayload
			if curExt != nil {
				curExt.Payloads++
				curExt.BytesUncompressed += uint64(length) + uint64(uvarintSize(length))
			}
		default:
			res.UnknownItemTags[uint8(fieldNum)]++
		}
	}

	// Stable order: stores in encounter order (already correct), extensions same.
	if len(res.UnknownItemTags) == 0 {
		res.UnknownItemTags = nil
	}
	// Fill human-readable byte fields for each component.
	for i := range res.Stores {
		res.Stores[i].BytesHuman = humanbytes.Format(res.Stores[i].BytesUncompressed)
	}
	for i := range res.Extensions {
		res.Extensions[i].BytesHuman = humanbytes.Format(res.Extensions[i].BytesUncompressed)
	}
	return res, nil
}

// listChunks returns chunk files in numeric order.
func listChunks(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	type chunk struct {
		idx  int
		path string
	}
	var cs []chunk
	for _, e := range entries {
		n := e.Name()
		if !strings.HasPrefix(n, "chunk_") || !strings.HasSuffix(n, ".bin") {
			continue
		}
		idxStr := strings.TrimSuffix(strings.TrimPrefix(n, "chunk_"), ".bin")
		idx, err := strconv.Atoi(idxStr)
		if err != nil {
			continue
		}
		cs = append(cs, chunk{idx, filepath.Join(dir, n)})
	}
	sort.Slice(cs, func(i, j int) bool { return cs[i].idx < cs[j].idx })
	out := make([]string, len(cs))
	for i, c := range cs {
		out[i] = c.path
	}
	return out, nil
}

// parseStoreName decodes a SnapshotStoreItem sub-message (length-delimited
// inside the SnapshotItem at field 1). Returns the `name` (field 1 string).
//
// Wire layout: [outer-tag 0x0A][outer-len varint][inner-tag 0x0A][name-len varint][name bytes]
// We are passed `buf` starting at the outer-len varint (i.e. just past 0x0A).
func parseStoreName(buf []byte) string {
	// Read outer length (size of the inner sub-message).
	innerLen, n := binary.Uvarint(buf)
	if n <= 0 || int(innerLen) > len(buf)-n {
		return ""
	}
	inner := buf[n : n+int(innerLen)]
	// Inside, we want field 1 (string name) → tag 0x0A
	if len(inner) == 0 || inner[0] != 0x0A {
		return ""
	}
	nameLen, m := binary.Uvarint(inner[1:])
	if m <= 0 || 1+m+int(nameLen) > len(inner) {
		return ""
	}
	return string(inner[1+m : 1+m+int(nameLen)])
}

// parseExtMeta decodes a SnapshotExtensionMeta sub-message (field 3 of
// SnapshotItem). Returns (name, format).
func parseExtMeta(buf []byte) (string, uint32) {
	innerLen, n := binary.Uvarint(buf)
	if n <= 0 || int(innerLen) > len(buf)-n {
		return "", 0
	}
	inner := buf[n : n+int(innerLen)]
	var name string
	var format uint32
	for i := 0; i < len(inner); {
		tag := inner[i]
		i++
		switch tag {
		case 0x0A: // field 1 string name
			ln, m := binary.Uvarint(inner[i:])
			if m <= 0 || i+m+int(ln) > len(inner) {
				return name, format
			}
			i += m
			name = string(inner[i : i+int(ln)])
			i += int(ln)
		case 0x10: // field 2 varint format
			v, m := binary.Uvarint(inner[i:])
			if m <= 0 {
				return name, format
			}
			i += m
			format = uint32(v)
		default:
			// Unknown / future fields — skip best-effort by wire type.
			wire := tag & 0x07
			switch wire {
			case 0:
				_, m := binary.Uvarint(inner[i:])
				if m <= 0 {
					return name, format
				}
				i += m
			case 2:
				ln, m := binary.Uvarint(inner[i:])
				if m <= 0 || i+m+int(ln) > len(inner) {
					return name, format
				}
				i += m + int(ln)
			default:
				return name, format
			}
		}
	}
	return name, format
}

// uvarintSize returns how many bytes an unsigned varint takes.
func uvarintSize(x uint64) int {
	var b [10]byte
	return binary.PutUvarint(b[:], x)
}

// ParseChunkHashes decodes a cosmos-sdk format-3 snapshot Metadata blob:
//
//	repeated bytes chunk_hashes = 1;
//
// Returns the slice of hashes as hex strings.
func ParseChunkHashes(metadata []byte) ([]string, error) {
	out := []string{}
	i := 0
	for i < len(metadata) {
		if metadata[i] != 0x0A {
			return nil, fmt.Errorf("unexpected tag 0x%02x at offset %d", metadata[i], i)
		}
		i++
		length, n := binary.Uvarint(metadata[i:])
		if n <= 0 {
			return nil, fmt.Errorf("bad varint at offset %d", i)
		}
		i += n
		if i+int(length) > len(metadata) {
			return nil, fmt.Errorf("hash extends past metadata")
		}
		out = append(out, hex.EncodeToString(metadata[i:i+int(length)]))
		i += int(length)
	}
	return out, nil
}

