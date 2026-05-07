package snapfetch

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	cmtlog "github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/p2p"
)

// TestWriteMetaWritesMetaAndCompleteAtomically exercises the finalize
// step in isolation: meta.json is valid JSON, .complete exists, and
// no .tmp files are left behind.
func TestWriteMetaWritesMetaAndCompleteAtomically(t *testing.T) {
	dir := t.TempDir()
	s := &fetchSession{log: cmtlog.NewNopLogger()}

	offer := &snapshotOffer{
		Height:   123,
		Format:   1,
		Chunks:   2,
		Hash:     []byte{0xde, 0xad, 0xbe, 0xef},
		Metadata: []byte("metadata"),
		Peers:    map[string]bool{"peer-1": true, "peer-2": true},
	}
	good := []p2p.ID{"peer-1"}

	if err := s.writeMeta(dir, offer, good, 4096); err != nil {
		t.Fatalf("writeMeta: %v", err)
	}

	// .complete present.
	if _, err := os.Stat(filepath.Join(dir, ".complete")); err != nil {
		t.Fatalf(".complete missing: %v", err)
	}
	// meta.json parseable.
	raw, err := os.ReadFile(filepath.Join(dir, "meta.json"))
	if err != nil {
		t.Fatalf("read meta.json: %v", err)
	}
	var meta savedMeta
	if err := json.Unmarshal(raw, &meta); err != nil {
		t.Fatalf("meta.json invalid JSON: %v (raw=%q)", err, raw)
	}
	if meta.Height != 123 || meta.Format != 1 || meta.Chunks != 2 {
		t.Fatalf("meta.json fields wrong: %+v", meta)
	}
	if meta.HashHex != "deadbeef" {
		t.Fatalf("hash_hex=%q, want deadbeef", meta.HashHex)
	}

	// No .tmp leftovers anywhere in the dir.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".tmp") {
			t.Fatalf("temp file leaked in snap dir: %s", e.Name())
		}
	}
}

// TestPrepareSnapshotDirAtomicMetadata exercises the metadata.bin
// write path: the file is present after prepareSnapshotDir and no
// .tmp is left behind.
func TestPrepareSnapshotDirAtomicMetadata(t *testing.T) {
	root := t.TempDir()
	s := &fetchSession{log: cmtlog.NewNopLogger(), cfg: Config{ChainID: "testchain"}}

	// One chunk hash → one byte of metadata + tag/length framing.
	// 0x0A = field-1 length-delimited, 0x04 = length 4, then 4 bytes hash.
	metadata := []byte{0x0A, 0x04, 0x01, 0x02, 0x03, 0x04}
	offer := &snapshotOffer{
		Height:   42,
		Format:   1,
		Chunks:   1,
		Hash:     []byte("h"),
		Metadata: metadata,
		Peers:    map[string]bool{},
	}

	dir, hashes, err := s.prepareSnapshotDir(root, offer)
	if err != nil {
		t.Fatalf("prepareSnapshotDir: %v", err)
	}
	if len(hashes) != 1 {
		t.Fatalf("hashes len=%d, want 1", len(hashes))
	}
	got, err := os.ReadFile(filepath.Join(dir, "metadata.bin"))
	if err != nil {
		t.Fatalf("read metadata.bin: %v", err)
	}
	if string(got) != string(metadata) {
		t.Fatalf("metadata.bin content mismatch")
	}
	if _, err := os.Stat(filepath.Join(dir, "metadata.bin.tmp")); !os.IsNotExist(err) {
		t.Fatalf("metadata.bin.tmp leaked: stat err = %v", err)
	}
}

// TestPrepareSnapshotDirReusesMatchingMetadata ensures we don't
// gratuitously rewrite metadata.bin when a prior partial fetch already
// left an identical file in place. We detect "did not rewrite" via
// inode equality, which writeFileAtomic's tmp+rename would change.
func TestPrepareSnapshotDirReusesMatchingMetadata(t *testing.T) {
	root := t.TempDir()
	s := &fetchSession{log: cmtlog.NewNopLogger(), cfg: Config{ChainID: "testchain"}}

	metadata := []byte{0x0A, 0x04, 0x01, 0x02, 0x03, 0x04}
	offer := &snapshotOffer{
		Height: 42, Format: 1, Chunks: 1,
		Hash: []byte("h"), Metadata: metadata, Peers: map[string]bool{},
	}

	// First call: writes metadata.bin from scratch.
	dir, _, err := s.prepareSnapshotDir(root, offer)
	if err != nil {
		t.Fatalf("first prepareSnapshotDir: %v", err)
	}
	mdPath := filepath.Join(dir, "metadata.bin")
	beforeStat, err := os.Stat(mdPath)
	if err != nil {
		t.Fatalf("stat metadata.bin: %v", err)
	}
	beforeIno := statIno(t, beforeStat)

	// Second call with same offer must reuse the file (no rename, so
	// inode is preserved).
	if _, _, err := s.prepareSnapshotDir(root, offer); err != nil {
		t.Fatalf("second prepareSnapshotDir: %v", err)
	}
	afterStat, err := os.Stat(mdPath)
	if err != nil {
		t.Fatalf("stat metadata.bin after reuse: %v", err)
	}
	if statIno(t, afterStat) != beforeIno {
		t.Fatalf("metadata.bin inode changed (%d → %d) — reuse path rewrote a matching file",
			beforeIno, statIno(t, afterStat))
	}

	// Third call with a different offer must rewrite (new inode).
	mismatched := &snapshotOffer{
		Height: 42, Format: 1, Chunks: 1,
		Hash:     []byte("h"),
		Metadata: []byte{0x0A, 0x04, 0xAA, 0xBB, 0xCC, 0xDD},
		Peers:    map[string]bool{},
	}
	if _, _, err := s.prepareSnapshotDir(root, mismatched); err != nil {
		t.Fatalf("third prepareSnapshotDir: %v", err)
	}
	rewrittenStat, err := os.Stat(mdPath)
	if err != nil {
		t.Fatalf("stat metadata.bin after rewrite: %v", err)
	}
	if statIno(t, rewrittenStat) == beforeIno {
		t.Fatalf("metadata.bin inode unchanged on mismatch — content was not rewritten")
	}
	got, err := os.ReadFile(mdPath)
	if err != nil {
		t.Fatalf("read metadata.bin: %v", err)
	}
	if string(got) != string(mismatched.Metadata) {
		t.Fatalf("metadata.bin content not updated on mismatch")
	}
}

func statIno(t *testing.T, fi os.FileInfo) uint64 {
	t.Helper()
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		t.Skip("inode comparison unavailable on this platform")
	}
	return st.Ino
}
