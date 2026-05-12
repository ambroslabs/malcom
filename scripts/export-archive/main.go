// Command export-archive converts a malcom snapshot directory (as produced
// by `malcom snapshot fetch`) into a portable cosmos-sdk snapshot archive
// (`.tar.gz`) that `gaiad snapshots load` accepts.
//
// This is a one-shot helper, not wired into the malcom CLI. The intended use
// is to import a malcom-fetched snapshot into a node that does not use
// pebbledb-backed state (e.g. a memiavl node), by going through the standard
// cosmos-sdk snapshot store path:
//
//	export-archive --snapshot <dir> --out <archive.tar.gz>
//	gaiad snapshots load <archive.tar.gz>
//	gaiad snapshots restore <height> <format>
//
// Archive format expected by `gaiad snapshots load` (see
// cosmos-sdk client/snapshot/load.go):
//
//   - gzip-wrapped tar
//   - first entry "_snapshot": marshaled cosmossdk.io/store/snapshots/types.Snapshot
//     proto (height, format, chunks, hash, metadata{chunk_hashes})
//   - subsequent entries "0", "1", ..., "N-1": raw chunk bytes
//
// We hand-roll the proto encoder so this script doesn't drag cosmos-sdk into
// malcom's build — matching the existing convention in
// internal/snapfetch/helpers.go and internal/snapshotinspect/inspect.go.
package main

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
)

type metaJSON struct {
	ChainID string `json:"chain_id"`
	Height  uint64 `json:"height"`
	Format  uint32 `json:"format"`
	Chunks  uint32 `json:"chunks"`
	HashHex string `json:"hash_hex"`
}

func main() {
	var (
		src      string
		out      string
		verify   bool
		noVerify bool
	)
	flag.StringVar(&src, "snapshot", "", "path to malcom snapshot dir (required)")
	flag.StringVar(&out, "out", "", "output archive path (.tar.gz); required")
	flag.BoolVar(&verify, "verify", true, "verify each chunk's SHA256 against metadata.bin while building (default true)")
	flag.BoolVar(&noVerify, "no-verify", false, "skip per-chunk SHA256 verification (faster, less safe)")
	flag.Parse()

	if src == "" || out == "" {
		fmt.Fprintln(os.Stderr, "usage: export-archive --snapshot <dir> --out <archive.tar.gz>")
		os.Exit(2)
	}
	if noVerify {
		verify = false
	}

	if err := run(src, out, verify); err != nil {
		fmt.Fprintln(os.Stderr, "export-archive:", err)
		os.Exit(1)
	}
}

func run(src, out string, verify bool) error {
	mj, err := readMetaJSON(filepath.Join(src, "meta.json"))
	if err != nil {
		return fmt.Errorf("read meta.json: %w", err)
	}

	hash, err := hex.DecodeString(mj.HashHex)
	if err != nil {
		return fmt.Errorf("decode hash_hex: %w", err)
	}

	metadataBytes, err := os.ReadFile(filepath.Join(src, "metadata.bin"))
	if err != nil {
		return fmt.Errorf("read metadata.bin: %w", err)
	}
	chunkHashes, err := parseChunkHashes(metadataBytes)
	if err != nil {
		return fmt.Errorf("parse metadata.bin: %w", err)
	}
	if uint32(len(chunkHashes)) != mj.Chunks {
		return fmt.Errorf("metadata.bin chunk_hashes=%d, meta.json chunks=%d", len(chunkHashes), mj.Chunks)
	}

	snapshotProto := encodeSnapshotProto(mj.Height, mj.Format, mj.Chunks, hash, chunkHashes)

	fOut, err := os.Create(out)
	if err != nil {
		return fmt.Errorf("create out: %w", err)
	}
	defer fOut.Close()

	gz := gzip.NewWriter(fOut)
	defer gz.Close()
	tw := tar.NewWriter(gz)
	defer tw.Close()

	// Entry 1: _snapshot
	if err := writeTarEntry(tw, "_snapshot", snapshotProto); err != nil {
		return fmt.Errorf("write _snapshot: %w", err)
	}

	// Entries 2..N+1: chunk files in numeric order.
	for i := uint32(0); i < mj.Chunks; i++ {
		chunkPath := filepath.Join(src, fmt.Sprintf("chunk_%05d.bin", i))
		if err := streamChunk(tw, chunkPath, strconv.FormatUint(uint64(i), 10), chunkHashes[i], verify); err != nil {
			return fmt.Errorf("chunk %d: %w", i, err)
		}
	}

	if err := tw.Close(); err != nil {
		return fmt.Errorf("close tar: %w", err)
	}
	if err := gz.Close(); err != nil {
		return fmt.Errorf("close gzip: %w", err)
	}
	if err := fOut.Sync(); err != nil {
		return fmt.Errorf("fsync: %w", err)
	}

	stat, _ := os.Stat(out)
	size := int64(0)
	if stat != nil {
		size = stat.Size()
	}
	fmt.Printf("wrote %s (height=%d format=%d chunks=%d bytes=%d)\n",
		out, mj.Height, mj.Format, mj.Chunks, size)
	return nil
}

func readMetaJSON(path string) (*metaJSON, error) {
	bz, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var mj metaJSON
	if err := json.Unmarshal(bz, &mj); err != nil {
		return nil, err
	}
	if mj.Height == 0 || mj.Chunks == 0 || mj.HashHex == "" {
		return nil, fmt.Errorf("incomplete meta.json: %+v", mj)
	}
	return &mj, nil
}

// parseChunkHashes decodes the cosmos-sdk format-3 snapshot Metadata blob:
//
//	repeated bytes chunk_hashes = 1;
//
// Same shape as cometbft's snapshot Metadata message — malcom's
// internal/snapfetch/helpers.go has the read-side equivalent.
func parseChunkHashes(metadata []byte) ([][]byte, error) {
	var out [][]byte
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
			return nil, fmt.Errorf("hash extends past metadata (offset=%d len=%d total=%d)",
				i, length, len(metadata))
		}
		h := make([]byte, length)
		copy(h, metadata[i:i+int(length)])
		out = append(out, h)
		i += int(length)
	}
	return out, nil
}

// encodeSnapshotProto hand-rolls the cosmossdk.io/store/snapshots/types.Snapshot
// proto encoding:
//
//	message Snapshot {
//	  uint64   height   = 1;
//	  uint32   format   = 2;
//	  uint32   chunks   = 3;
//	  bytes    hash     = 4;
//	  Metadata metadata = 5 [nullable = false];
//	}
//	message Metadata {
//	  repeated bytes chunk_hashes = 1;
//	}
func encodeSnapshotProto(height uint64, format, chunks uint32, hash []byte, chunkHashes [][]byte) []byte {
	var meta []byte
	for _, ch := range chunkHashes {
		meta = append(meta, 0x0A) // field 1 (chunk_hashes), wire type 2 (length-delim)
		meta = appendVarint(meta, uint64(len(ch)))
		meta = append(meta, ch...)
	}

	var s []byte
	s = append(s, 0x08) // field 1 height, wire type 0 (varint)
	s = appendVarint(s, height)
	s = append(s, 0x10) // field 2 format
	s = appendVarint(s, uint64(format))
	s = append(s, 0x18) // field 3 chunks
	s = appendVarint(s, uint64(chunks))
	s = append(s, 0x22) // field 4 hash, wire type 2
	s = appendVarint(s, uint64(len(hash)))
	s = append(s, hash...)
	s = append(s, 0x2A) // field 5 metadata, wire type 2 (embedded message)
	s = appendVarint(s, uint64(len(meta)))
	s = append(s, meta...)
	return s
}

func appendVarint(buf []byte, v uint64) []byte {
	for v >= 0x80 {
		buf = append(buf, byte(v)|0x80)
		v >>= 7
	}
	return append(buf, byte(v))
}

func writeTarEntry(tw *tar.Writer, name string, data []byte) error {
	hdr := &tar.Header{
		Name: name,
		Mode: 0o644,
		Size: int64(len(data)),
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return err
	}
	_, err := tw.Write(data)
	return err
}

// streamChunk copies a chunk file straight into the tar, computing its SHA256
// on the fly and (if verify) comparing it against the expected hash from
// metadata.bin. Streaming keeps peak memory bounded — chunks are ~10 MB each
// and there are hundreds of them.
func streamChunk(tw *tar.Writer, srcPath, tarName string, expected []byte, verify bool) error {
	stat, err := os.Stat(srcPath)
	if err != nil {
		return err
	}
	if err := tw.WriteHeader(&tar.Header{
		Name: tarName,
		Mode: 0o644,
		Size: stat.Size(),
	}); err != nil {
		return err
	}

	f, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer f.Close()

	if verify {
		h := sha256.New()
		w := io.MultiWriter(tw, h)
		if _, err := io.Copy(w, f); err != nil {
			return err
		}
		got := h.Sum(nil)
		if !bytesEqual(got, expected) {
			return fmt.Errorf("chunk %s SHA256 mismatch: got=%x expected=%x", tarName, got, expected)
		}
		return nil
	}
	_, err = io.Copy(tw, f)
	return err
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
