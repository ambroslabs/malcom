package snapserve

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/ambroslabs/malcom/internal/snapshotinspect"
	"github.com/ambroslabs/malcom/internal/statesync"
)

// loadedSnapshot is one snapshot dir prepared for serving: catalogue
// fields (Height/Format/Chunks/Hash/Metadata) ready to drop into a
// SnapshotsResponse, plus the dir path so LoadChunk knows where to
// read chunk_NNNNN.bin files. ChainID is retained for filtering by
// the dir-scan loader; the reactor itself doesn't need it.
type loadedSnapshot struct {
	Dir      string
	ChainID  string
	Height   uint64
	Format   uint32
	Chunks   uint32
	Hash     []byte
	Metadata []byte
}

// metaJSON is the subset of snapfetch's meta.json that we consume.
// Defined locally so snapserve doesn't import snapfetch (which would
// drag in the whole fetch dial loop).
type metaJSON struct {
	ChainID     string `json:"chain_id"`
	Height      uint64 `json:"height"`
	Format      uint32 `json:"format"`
	Chunks      uint32 `json:"chunks"`
	HashHex     string `json:"hash_hex"`
	MetadataLen int    `json:"metadata_len"`
}

// Store is a read-only catalogue of verified snapshot dirs that
// implements statesync.SnapshotProvider.
//
// A Store is immutable after construction. To pick up newly-landed
// snapshots, build a fresh Store (see LoadStoreFromRoot) and swap it
// onto the reactor via SetProvider — that's what Catalog does on
// rescan.
type Store struct {
	snapshots []loadedSnapshot
}

// VerifyMode controls what LoadStore checks before accepting a dir.
type VerifyMode int

const (
	// VerifyMetadataOnly does only the cheap structural checks:
	// .complete marker present, metadata.bin parses, chunk_hashes
	// count matches meta.json's Chunks. Use when you trust the
	// origin (e.g. just-completed local fetch) and don't want to
	// pay the I/O of re-hashing the snapshot at startup.
	VerifyMetadataOnly VerifyMode = iota

	// VerifyAggregateHash recomputes SHA256(chunk_0 || ... ||
	// chunk_N-1) and checks it equals meta.json's hash_hex (which
	// fetch wrote from the offer that peers signed against). Costs
	// one full read of the snapshot but doesn't decompress anything.
	VerifyAggregateHash

	// VerifyPerChunkHash also re-hashes each chunk file and checks
	// it against metadata.chunk_hashes[i]. Strictly stronger than
	// VerifyAggregateHash (the aggregate is implied) and costs the
	// same single read pass, since we hash each chunk into both a
	// per-chunk SHA256 and the rolling aggregate SHA256 in lockstep.
	VerifyPerChunkHash
)

// String returns a short label for diagnostic logs.
func (m VerifyMode) String() string {
	switch m {
	case VerifyMetadataOnly:
		return "metadata-only"
	case VerifyAggregateHash:
		return "aggregate-hash"
	case VerifyPerChunkHash:
		return "per-chunk-hash"
	default:
		return fmt.Sprintf("unknown(%d)", m)
	}
}

// LoadStore reads each explicit dir, verifies it according to mode,
// and returns a Store ready to plug into the statesync reactor.
// Returns an error on the first bad dir — explicit -snapshot flags
// represent operator intent, so we surface mismatches loudly rather
// than skipping silently.
//
// If chainID is non-empty, each snapshot's meta.json chain_id must
// match; a mismatch is a hard error (the operator probably pointed
// us at the wrong dir).
//
// Duplicate (height, format) pairs are rejected: the SnapshotsResponse
// catalogue must be unambiguous, and CometBFT requesters key
// ChunkRequest off (height, format) so two snapshots claiming the
// same key would fight for the same chunk reads.
func LoadStore(dirs []string, chainID string, mode VerifyMode, logger *slog.Logger) (*Store, error) {
	if len(dirs) == 0 {
		return nil, errors.New("no snapshot dirs provided")
	}
	loaded := make([]loadedSnapshot, 0, len(dirs))
	seen := map[string]string{}
	for _, dir := range dirs {
		t0 := time.Now()
		s, err := loadOne(dir, mode, logger)
		if err != nil {
			return nil, fmt.Errorf("snapshot %s: %w", dir, err)
		}
		if chainID != "" && s.ChainID != chainID {
			return nil, fmt.Errorf("snapshot %s: chain_id %q != expected %q",
				dir, s.ChainID, chainID)
		}
		key := fmt.Sprintf("%d_%d", s.Height, s.Format)
		if prior, ok := seen[key]; ok {
			return nil, fmt.Errorf(
				"duplicate (height=%d, format=%d) in %s and %s",
				s.Height, s.Format, prior, dir)
		}
		seen[key] = dir
		loaded = append(loaded, s)
		if logger != nil {
			logger.Info("snapshot loaded",
				"dir", dir,
				"height", s.Height,
				"format", s.Format,
				"chunks", s.Chunks,
				"verify", mode,
				"elapsed", time.Since(t0).Truncate(time.Millisecond))
		}
	}
	return newStore(loaded), nil
}

// LoadStoreFromRoot scans rootDir one level deep for snapshot dirs,
// filters to entries with chain_id == chainID (if non-empty), verifies
// each per mode, and returns a Store. Designed for the dir-watch
// workflow (Catalog) — per-dir failures are *not* fatal:
//
//   - non-directories and entries missing .complete are debug-skipped
//     (we'd see in-progress fetches and stray files here);
//   - wrong chain_id is debug-skipped (one root can hold multiple
//     chains' snapshots, each served by its own process);
//   - actual integrity errors (bad metadata, hash mismatch) are warn-
//     logged and skipped — leaving the rest of the catalogue
//     advertisable.
//
// A missing or unreadable rootDir is a hard error (operator misconfig).
// An empty rootDir yields an empty store, which is legal — the server
// runs and just doesn't advertise anything until a snapshot lands.
//
// Duplicate (height, format) pairs surviving the chain-id filter are
// rejected (warn + skip the second). The first-loaded wins; the order
// is whatever os.ReadDir returns, which on most filesystems is dir-
// order, not stable.
func LoadStoreFromRoot(rootDir, chainID string, mode VerifyMode, logger *slog.Logger) (*Store, error) {
	entries, err := os.ReadDir(rootDir)
	if err != nil {
		return nil, fmt.Errorf("scan snapshots root %s: %w", rootDir, err)
	}
	loaded := make([]loadedSnapshot, 0, len(entries))
	seen := map[string]string{}
	for _, e := range entries {
		dir := filepath.Join(rootDir, e.Name())
		// os.Stat follows symlinks — useful in practice because
		// operators often symlink finished snapshots from elsewhere
		// (large mount, network FS) into the pool dir. Lstat + IsDir
		// would refuse those.
		info, err := os.Stat(dir)
		if err != nil || !info.IsDir() {
			continue
		}
		// Cheap pre-check: skip entries that don't even look like a
		// snapshot dir, without spamming the log for stray files /
		// in-progress fetches.
		if _, err := os.Stat(filepath.Join(dir, ".complete")); err != nil {
			if logger != nil {
				logger.Debug("dir-scan: skipping (no .complete marker)", "dir", dir)
			}
			continue
		}
		t0 := time.Now()
		s, err := loadOne(dir, mode, logger)
		if err != nil {
			if logger != nil {
				logger.Warn("dir-scan: skipping (verification failed)",
					"dir", dir, "err", err)
			}
			continue
		}
		if chainID != "" && s.ChainID != chainID {
			if logger != nil {
				logger.Debug("dir-scan: skipping (wrong chain)",
					"dir", dir, "snapshot_chain_id", s.ChainID, "want", chainID)
			}
			continue
		}
		key := fmt.Sprintf("%d_%d", s.Height, s.Format)
		if prior, ok := seen[key]; ok {
			if logger != nil {
				logger.Warn("dir-scan: skipping (duplicate height/format)",
					"dir", dir, "height", s.Height, "format", s.Format,
					"already_loaded_from", prior)
			}
			continue
		}
		seen[key] = dir
		loaded = append(loaded, s)
		if logger != nil {
			logger.Info("dir-scan: snapshot loaded",
				"dir", dir,
				"height", s.Height,
				"format", s.Format,
				"chunks", s.Chunks,
				"verify", mode,
				"elapsed", time.Since(t0).Truncate(time.Millisecond))
		}
	}
	return newStore(loaded), nil
}

// newStore is the shared tail of LoadStore / LoadStoreFromRoot.
// Sorts the catalogue newest-first so the cosmos-sdk requester sees
// the highest height first on the wire.
func newStore(loaded []loadedSnapshot) *Store {
	// Newest height first. Cosmos-SDK's statesync requester picks the
	// highest offer it sees, so we advertise newest first to minimise
	// time-to-first-acceptance over an MConn that can drop tail bytes.
	sort.Slice(loaded, func(i, j int) bool {
		return loaded[i].Height > loaded[j].Height
	})
	return &Store{snapshots: loaded}
}

func loadOne(dir string, mode VerifyMode, logger *slog.Logger) (loadedSnapshot, error) {
	var zero loadedSnapshot

	if _, err := os.Stat(filepath.Join(dir, ".complete")); err != nil {
		if os.IsNotExist(err) {
			return zero, errors.New("missing .complete marker — fetch did not finish; re-run `malcom snapshot fetch` (or delete the dir and start over)")
		}
		return zero, fmt.Errorf("stat .complete: %w", err)
	}

	mdBytes, err := os.ReadFile(filepath.Join(dir, "metadata.bin"))
	if err != nil {
		return zero, fmt.Errorf("read metadata.bin: %w", err)
	}
	chunkHashesHex, err := snapshotinspect.ParseChunkHashes(mdBytes)
	if err != nil {
		return zero, fmt.Errorf("parse metadata.bin: %w", err)
	}

	rawMeta, err := os.ReadFile(filepath.Join(dir, "meta.json"))
	if err != nil {
		return zero, fmt.Errorf("read meta.json: %w", err)
	}
	var meta metaJSON
	if err := json.Unmarshal(rawMeta, &meta); err != nil {
		return zero, fmt.Errorf("parse meta.json: %w", err)
	}
	if meta.Chunks == 0 {
		return zero, errors.New("meta.json reports zero chunks")
	}
	if uint32(len(chunkHashesHex)) != meta.Chunks {
		return zero, fmt.Errorf("chunk_hashes count %d != meta.chunks %d",
			len(chunkHashesHex), meta.Chunks)
	}
	if meta.MetadataLen != 0 && meta.MetadataLen != len(mdBytes) {
		return zero, fmt.Errorf("metadata.bin len %d != meta.metadata_len %d",
			len(mdBytes), meta.MetadataLen)
	}
	expectedHash, err := hex.DecodeString(meta.HashHex)
	if err != nil {
		return zero, fmt.Errorf("decode hash_hex: %w", err)
	}
	if len(expectedHash) != sha256.Size {
		return zero, fmt.Errorf("hash_hex is %d bytes, want %d", len(expectedHash), sha256.Size)
	}

	// Decode each chunk-hash hex (only need bytes if we're per-chunk
	// verifying; cheap enough to always have them as []byte).
	chunkHashes := make([][]byte, len(chunkHashesHex))
	for i, h := range chunkHashesHex {
		b, err := hex.DecodeString(h)
		if err != nil {
			return zero, fmt.Errorf("decode chunk_hashes[%d]: %w", i, err)
		}
		chunkHashes[i] = b
	}

	if mode != VerifyMetadataOnly {
		if err := verifyChunks(dir, meta.Chunks, expectedHash, chunkHashes, mode); err != nil {
			return zero, err
		}
	}
	// The metadata-only warning used to live here, firing once per
	// snapshot per scan — fine in static mode (one scan ever), but in
	// dir-watch mode that's N×rescan-rate per hour of identical log
	// lines. The warning now lives at the catalog / RunServe layer
	// where it fires once at startup.

	return loadedSnapshot{
		Dir:      dir,
		ChainID:  meta.ChainID,
		Height:   meta.Height,
		Format:   meta.Format,
		Chunks:   meta.Chunks,
		Hash:     expectedHash,
		Metadata: mdBytes,
	}, nil
}

func verifyChunks(dir string, totalChunks uint32, wantAggregate []byte, perChunk [][]byte, mode VerifyMode) error {
	agg := sha256.New()
	buf := make([]byte, 1<<20)
	for i := uint32(0); i < totalChunks; i++ {
		path := filepath.Join(dir, fmt.Sprintf("chunk_%05d.bin", i))
		f, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("open chunk %d: %w", i, err)
		}
		var sink io.Writer = agg
		var per = sha256.New()
		if mode == VerifyPerChunkHash {
			// Stream into both agg and the per-chunk hasher in one
			// read pass — the snapshot is large and we'd rather not
			// reread the file just to compute the per-chunk SHA.
			sink = io.MultiWriter(agg, per)
		}
		if _, err := io.CopyBuffer(sink, f, buf); err != nil {
			f.Close()
			return fmt.Errorf("read chunk %d: %w", i, err)
		}
		if err := f.Close(); err != nil {
			return fmt.Errorf("close chunk %d: %w", i, err)
		}
		if mode == VerifyPerChunkHash {
			got := per.Sum(nil)
			if !bytes.Equal(got, perChunk[i]) {
				return fmt.Errorf("chunk %d hash mismatch (got %s, want %s)",
					i, hex.EncodeToString(got), hex.EncodeToString(perChunk[i]))
			}
		}
	}
	got := agg.Sum(nil)
	if !bytes.Equal(got, wantAggregate) {
		return fmt.Errorf("aggregate hash mismatch (got %s, want %s)",
			hex.EncodeToString(got), hex.EncodeToString(wantAggregate))
	}
	return nil
}

// ListSnapshots implements statesync.SnapshotProvider. The returned
// slice is freshly allocated; callers may not mutate it but the
// reactor only reads from it.
func (s *Store) ListSnapshots() []statesync.Snapshot {
	out := make([]statesync.Snapshot, len(s.snapshots))
	for i, ls := range s.snapshots {
		out[i] = statesync.Snapshot{
			Height:   ls.Height,
			Format:   ls.Format,
			Chunks:   ls.Chunks,
			Hash:     ls.Hash,
			Metadata: ls.Metadata,
		}
	}
	return out
}

// LoadChunk implements statesync.SnapshotProvider. Reads
// chunk_NNNNN.bin off disk on each call; we rely on the OS page cache
// rather than holding the full snapshot in memory. found=false means
// we either don't have a snapshot at (height, format) or the chunk
// index is past the snapshot's chunk count.
func (s *Store) LoadChunk(height uint64, format, index uint32) ([]byte, bool, error) {
	for i := range s.snapshots {
		ls := &s.snapshots[i]
		if ls.Height != height || ls.Format != format {
			continue
		}
		if index >= ls.Chunks {
			return nil, false, nil
		}
		path := filepath.Join(ls.Dir, fmt.Sprintf("chunk_%05d.bin", index))
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, false, fmt.Errorf("read chunk %d: %w", index, err)
		}
		return data, true, nil
	}
	return nil, false, nil
}

// Len returns how many snapshots the store will advertise.
func (s *Store) Len() int { return len(s.snapshots) }

// Describe returns a stable text summary for startup logs.
func (s *Store) Describe() string {
	parts := make([]string, len(s.snapshots))
	for i, ls := range s.snapshots {
		parts[i] = fmt.Sprintf("h=%d f=%d c=%d hash=%s",
			ls.Height, ls.Format, ls.Chunks, hex.EncodeToString(ls.Hash[:8]))
	}
	return "[" + joinWith(parts, ", ") + "]"
}

func joinWith(ss []string, sep string) string {
	switch len(ss) {
	case 0:
		return ""
	case 1:
		return ss[0]
	}
	n := len(sep) * (len(ss) - 1)
	for _, s := range ss {
		n += len(s)
	}
	var b bytes.Buffer
	b.Grow(n)
	b.WriteString(ss[0])
	for _, s := range ss[1:] {
		b.WriteString(sep)
		b.WriteString(s)
	}
	return b.String()
}
