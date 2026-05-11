package snapserve

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// writeChunk writes data to <dir>/chunk_<NNNNN>.bin, the same path
// pattern fetch produces.
func writeChunk(t *testing.T, dir string, idx uint32, data []byte) {
	t.Helper()
	name := filepath.Join(dir, "chunk_"+padIdx(idx)+".bin")
	if err := os.WriteFile(name, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func padIdx(i uint32) string {
	const digits = "0123456789"
	out := []byte("00000")
	pos := 4
	v := i
	for v > 0 && pos >= 0 {
		out[pos] = digits[v%10]
		v /= 10
		pos--
	}
	return string(out)
}

// buildMetadataBin serialises chunkHashes as the format-3
// `repeated bytes chunk_hashes = 1;` proto layout: a 0x0A tag byte,
// a varint length, and the bytes — for each entry. Mirrors what
// snapfetch's metadata.bin contains.
func buildMetadataBin(chunkHashes [][]byte) []byte {
	var out []byte
	for _, h := range chunkHashes {
		out = append(out, 0x0A)
		var buf [10]byte
		n := binary.PutUvarint(buf[:], uint64(len(h)))
		out = append(out, buf[:n]...)
		out = append(out, h...)
	}
	return out
}

func makeSnapshotDir(t *testing.T, payloads [][]byte) string {
	t.Helper()
	dir := t.TempDir()

	chunkHashes := make([][]byte, len(payloads))
	agg := sha256.New()
	for i, p := range payloads {
		writeChunk(t, dir, uint32(i), p)
		h := sha256.Sum256(p)
		chunkHashes[i] = h[:]
		agg.Write(p)
	}
	if err := os.WriteFile(
		filepath.Join(dir, "metadata.bin"),
		buildMetadataBin(chunkHashes),
		0o644,
	); err != nil {
		t.Fatal(err)
	}
	mdLen := 0
	for _, h := range chunkHashes {
		mdLen += 1 + 1 + len(h) // tag + 1-byte varint (32 < 128) + payload
	}
	meta := metaJSON{
		ChainID:     "test-chain",
		Height:      12345,
		Format:      3,
		Chunks:      uint32(len(payloads)),
		HashHex:     hex.EncodeToString(agg.Sum(nil)),
		MetadataLen: mdLen,
	}
	mb, _ := json.Marshal(meta)
	if err := os.WriteFile(filepath.Join(dir, "meta.json"), mb, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".complete"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestLoadStore_HappyPath(t *testing.T) {
	dir := makeSnapshotDir(t, [][]byte{
		[]byte("hello world"),
		[]byte("second chunk payload"),
	})
	store, err := LoadStore([]string{dir}, "", VerifyPerChunkHash, nil)
	if err != nil {
		t.Fatalf("LoadStore: %v", err)
	}
	if got := store.Len(); got != 1 {
		t.Fatalf("want 1 snapshot, got %d", got)
	}
	snaps := store.ListSnapshots()
	if snaps[0].Height != 12345 || snaps[0].Format != 3 || snaps[0].Chunks != 2 {
		t.Fatalf("snapshot identifiers wrong: %+v", snaps[0])
	}
	got, found, err := store.LoadChunk(12345, 3, 1)
	if err != nil || !found {
		t.Fatalf("LoadChunk: found=%v err=%v", found, err)
	}
	if string(got) != "second chunk payload" {
		t.Fatalf("payload mismatch: %q", got)
	}
}

func TestLoadStore_MissingComplete(t *testing.T) {
	dir := makeSnapshotDir(t, [][]byte{[]byte("x")})
	if err := os.Remove(filepath.Join(dir, ".complete")); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadStore([]string{dir}, "", VerifyMetadataOnly, nil); err == nil {
		t.Fatal("expected error for missing .complete")
	}
}

func TestLoadStore_TamperedChunkDetected(t *testing.T) {
	dir := makeSnapshotDir(t, [][]byte{[]byte("original")})
	// Overwrite chunk_00000 with different content; aggregate hash
	// won't match.
	if err := os.WriteFile(
		filepath.Join(dir, "chunk_00000.bin"),
		[]byte("tampered"),
		0o644,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadStore([]string{dir}, "", VerifyAggregateHash, nil); err == nil {
		t.Fatal("expected aggregate-hash verify to detect tamper")
	}
	if _, err := LoadStore([]string{dir}, "", VerifyPerChunkHash, nil); err == nil {
		t.Fatal("expected per-chunk verify to detect tamper")
	}
}

func TestLoadStore_DuplicateHeightFormatRejected(t *testing.T) {
	dir1 := makeSnapshotDir(t, [][]byte{[]byte("a"), []byte("b")})
	dir2 := makeSnapshotDir(t, [][]byte{[]byte("c")})
	if _, err := LoadStore([]string{dir1, dir2}, "", VerifyMetadataOnly, nil); err == nil {
		t.Fatal("expected duplicate (height,format) error")
	}
}

func TestLoadChunk_OutOfRange(t *testing.T) {
	dir := makeSnapshotDir(t, [][]byte{[]byte("only one")})
	store, err := LoadStore([]string{dir}, "", VerifyPerChunkHash, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, found, err := store.LoadChunk(12345, 3, 99)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if found {
		t.Fatal("out-of-range chunk should report not found")
	}
}

func TestLoadChunk_UnknownSnapshot(t *testing.T) {
	dir := makeSnapshotDir(t, [][]byte{[]byte("x")})
	store, _ := LoadStore([]string{dir}, "", VerifyPerChunkHash, nil)
	_, found, err := store.LoadChunk(99999, 3, 0)
	if err != nil || found {
		t.Fatalf("unknown (height,format) should be not-found, no error; got found=%v err=%v", found, err)
	}
}
