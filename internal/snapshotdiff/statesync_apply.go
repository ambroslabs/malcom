package snapshotdiff

import (
	"bufio"
	"compress/zlib"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/cockroachdb/pebble"

	malcomlog "github.com/ambroslabs/malcom/internal/log"
)

// StateSyncApplyStats summarises the result of an ApplyStateSync run.
type StateSyncApplyStats struct {
	BaseItems         uint64
	TargetItems       uint64
	Refs              uint64
	Literals          uint64
	Chunks            int
	BytesUncompressed uint64
	BytesCompressed   uint64
	MetadataBytes     int
}

// ApplyStateSync reads a CSDS-format diff at diffPath and applies it to
// baseDir, producing a state-sync-feedable snapshot at outDir.
//
// The output directory will contain:
//   - chunk_NNNNN.bin (10 MiB cosmos-sdk default) with byte-equivalent
//     content to a producer-side snapshot at target_height
//   - metadata.bin (cosmos-sdk Metadata proto with chunk hashes)
//   - meta.json (our convenience metadata)
//
// Disk requirements (under tmpDir): ~25 GB for the Pebble base index
// (each base item stored under its uint64 index for random lookup).
func ApplyStateSync(baseDir, diffPath, outDir, tmpDir string, log *slog.Logger) (StateSyncApplyStats, error) {
	var stats StateSyncApplyStats
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}

	df, err := os.Open(diffPath)
	if err != nil {
		return stats, err
	}
	defer df.Close()

	hdr, err := ReadStateSyncHeader(df)
	if err != nil {
		return stats, fmt.Errorf("read CSDS header: %w", err)
	}
	if h, err := readSnapHashFromMeta(filepath.Join(baseDir, "meta.json")); err == nil {
		if h != hdr.BaseHashHex {
			return stats, fmt.Errorf("base hash mismatch: meta=%s diff=%s", h, hdr.BaseHashHex)
		}
	}

	// Phase A: walk base, build index → bytes in Pebble.
	dbPath := filepath.Join(tmpDir, "base-by-index")
	if err := os.MkdirAll(dbPath, 0o755); err != nil {
		return stats, err
	}
	db, err := pebble.Open(dbPath, &pebble.Options{
		DisableWAL:   true,
		MemTableSize: 64 << 20,
		Cache:        pebble.NewCache(256 << 20),
		Logger:       malcomlog.PebbleShim(log.With("module", "pebble")),
	})
	if err != nil {
		return stats, fmt.Errorf("open pebble: %w", err)
	}
	defer db.Close()

	baseRdr, err := newSnapItemReader(baseDir)
	if err != nil {
		return stats, fmt.Errorf("open base: %w", err)
	}
	defer baseRdr.Close()

	batch := db.NewBatch()
	const flushEvery = 16 << 20
	var idx uint64
	for {
		item, eof, err := baseRdr.Next()
		if err != nil {
			return stats, fmt.Errorf("read base: %w", err)
		}
		if eof {
			break
		}
		var keyBuf [8]byte
		binary.BigEndian.PutUint64(keyBuf[:], idx)
		if err := batch.Set(keyBuf[:], item, nil); err != nil {
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

	// Phase B: open output, walk diff records, emit SnapshotItems.
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return stats, err
	}
	cw := newChunkWriter(outDir, CosmosSDKChunkSize)
	zw, err := zlib.NewWriterLevel(cw, zlib.DefaultCompression)
	if err != nil {
		return stats, err
	}
	bw := bufio.NewWriterSize(zw, 1<<20)

	var rawBytes uint64
	emit := func(item []byte) error {
		var lb [binary.MaxVarintLen64]byte
		n := binary.PutUvarint(lb[:], uint64(len(item)))
		if _, err := bw.Write(lb[:n]); err != nil {
			return err
		}
		if _, err := bw.Write(item); err != nil {
			return err
		}
		rawBytes += uint64(n) + uint64(len(item))
		return nil
	}

	zr, err := zlib.NewReader(df)
	if err != nil {
		return stats, fmt.Errorf("open zlib body: %w", err)
	}
	defer zr.Close()
	dr := bufio.NewReaderSize(zr, 1<<20)

	for {
		op, err := dr.ReadByte()
		if err == io.EOF {
			break
		}
		if err != nil {
			return stats, err
		}
		switch op {
		case OpRef:
			baseIdx, err := binary.ReadUvarint(dr)
			if err != nil {
				return stats, fmt.Errorf("read REF index: %w", err)
			}
			var keyBuf [8]byte
			binary.BigEndian.PutUint64(keyBuf[:], baseIdx)
			val, closer, err := db.Get(keyBuf[:])
			if err != nil {
				return stats, fmt.Errorf("base index %d not found: %w", baseIdx, err)
			}
			itemCopy := make([]byte, len(val))
			copy(itemCopy, val)
			closer.Close()
			if err := emit(itemCopy); err != nil {
				return stats, err
			}
			stats.Refs++
		case OpLiteral:
			lenU, err := binary.ReadUvarint(dr)
			if err != nil {
				return stats, fmt.Errorf("read LITERAL length: %w", err)
			}
			item := make([]byte, lenU)
			if _, err := io.ReadFull(dr, item); err != nil {
				return stats, fmt.Errorf("read LITERAL bytes: %w", err)
			}
			if err := emit(item); err != nil {
				return stats, err
			}
			stats.Literals++
		default:
			return stats, fmt.Errorf("unknown op tag 0x%02x", op)
		}
		stats.TargetItems++
	}

	if err := bw.Flush(); err != nil {
		return stats, err
	}
	if err := zw.Close(); err != nil {
		return stats, err
	}
	if err := cw.close(); err != nil {
		return stats, err
	}

	stats.Chunks = cw.chunkIdx
	stats.BytesUncompressed = rawBytes
	stats.BytesCompressed = cw.totalWritten

	// Write metadata.bin (cosmos-sdk Metadata proto).
	metaPath := filepath.Join(outDir, "metadata.bin")
	mb, err := encodeMetadataProto(cw.chunkHashes)
	if err != nil {
		return stats, err
	}
	if err := os.WriteFile(metaPath, mb, 0o644); err != nil {
		return stats, err
	}
	stats.MetadataBytes = len(mb)

	// Write meta.json (our convenience metadata).
	snapHash := computeSnapshotHash(cw.chunkHashes)
	meta := outMeta{
		Height:         hdr.TargetHeight,
		Format:         3,
		Chunks:         cw.chunkIdx,
		HashHex:        snapHash,
		BytesTotal:     cw.totalWritten,
		ChunkHashesHex: hexHashes(cw.chunkHashes),
	}
	if err := writeMetaJSON(filepath.Join(outDir, "meta.json"), meta); err != nil {
		return stats, err
	}
	// Stash the metadata length for downstream tools.
	if err := patchMetaWithMetadataLen(filepath.Join(outDir, "meta.json"), len(mb)); err != nil {
		return stats, err
	}

	return stats, nil
}

// encodeMetadataProto produces the cosmos-sdk Metadata proto binary form:
//
//	message Metadata { repeated bytes chunk_hashes = 1; }
func encodeMetadataProto(chunkHashes [][]byte) ([]byte, error) {
	out := make([]byte, 0, len(chunkHashes)*34)
	for _, h := range chunkHashes {
		out = append(out, 0x0A) // field 1, wire-type 2
		var lb [binary.MaxVarintLen64]byte
		n := binary.PutUvarint(lb[:], uint64(len(h)))
		out = append(out, lb[:n]...)
		out = append(out, h...)
	}
	return out, nil
}

// patchMetaWithMetadataLen adds the metadata_len field to the meta.json
// at path so downstream tooling matches what cosmos-snapshot-fetch
// produces.
func patchMetaWithMetadataLen(path string, metadataLen int) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}
	m["metadata_len"] = metadataLen
	out, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, out, 0o644)
}
