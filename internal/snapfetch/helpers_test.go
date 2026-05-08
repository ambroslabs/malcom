package snapfetch

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// buildMetadataBlob serializes hashes as the cosmos-sdk format-3
// snapshot metadata: a sequence of (tag=0x0A, varint-length, bytes)
// records, mirroring parseChunkHashes's expected layout.
func buildMetadataBlob(t *testing.T, hashes [][]byte) []byte {
	t.Helper()
	var out []byte
	for _, h := range hashes {
		out = append(out, 0x0A)
		buf := make([]byte, binary.MaxVarintLen64)
		n := binary.PutUvarint(buf, uint64(len(h)))
		out = append(out, buf[:n]...)
		out = append(out, h...)
	}
	return out
}

func TestWriteFileAtomicWritesContentAndCleansTmp(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "thing.bin")

	want := []byte("durable bytes")
	if err := writeFileAtomic(path, want, 0o644); err != nil {
		t.Fatalf("writeFileAtomic: %v", err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read final: %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("content mismatch: got %q want %q", got, want)
	}

	// No leftover .tmp from a successful write.
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf(".tmp file leaked after success: stat err = %v", err)
	}
}

func TestWriteFileAtomicOverwritesAtomically(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "thing.bin")

	if err := writeFileAtomic(path, []byte("first"), 0o644); err != nil {
		t.Fatalf("first write: %v", err)
	}
	if err := writeFileAtomic(path, []byte("second"), 0o644); err != nil {
		t.Fatalf("second write: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "second" {
		t.Fatalf("overwrite content mismatch: got %q", got)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf(".tmp file leaked after overwrite: stat err = %v", err)
	}
}

func TestWriteJSONAtomicProducesValidJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "meta.json")

	type row struct {
		A int    `json:"a"`
		B string `json:"b"`
	}
	want := row{A: 7, B: "hello"}
	if err := writeJSONAtomic(path, want); err != nil {
		t.Fatalf("writeJSONAtomic: %v", err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var got row
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v (raw=%q)", err, raw)
	}
	if got != want {
		t.Fatalf("decoded mismatch: got %+v want %+v", got, want)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf(".tmp file leaked after success: stat err = %v", err)
	}
}

func TestFsyncDirOnExistingDirectory(t *testing.T) {
	dir := t.TempDir()
	if err := fsyncDir(dir); err != nil {
		t.Fatalf("fsyncDir on tempdir: %v", err)
	}
}

func TestFsyncDirOnMissingPathErrors(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "does-not-exist")
	if err := fsyncDir(missing); err == nil {
		t.Fatalf("fsyncDir on missing path: expected error, got nil")
	}
}

func TestVerifyChunkZeroAcceptsMatchingBytes(t *testing.T) {
	chunk0 := []byte("chunk-0 contents")
	chunk1 := []byte("chunk-1 contents")
	h0 := sha256.Sum256(chunk0)
	h1 := sha256.Sum256(chunk1)
	offer := &snapshotOffer{
		Chunks:   2,
		Metadata: buildMetadataBlob(t, [][]byte{h0[:], h1[:]}),
	}
	if err := verifyChunkZero(offer, chunk0); err != nil {
		t.Fatalf("verifyChunkZero on matching bytes: %v", err)
	}
}

func TestVerifyChunkZeroRejectsHashMismatch(t *testing.T) {
	chunk0 := []byte("chunk-0 contents")
	h0 := sha256.Sum256(chunk0)
	offer := &snapshotOffer{
		Chunks:   1,
		Metadata: buildMetadataBlob(t, [][]byte{h0[:]}),
	}
	if err := verifyChunkZero(offer, []byte("garbage")); err == nil {
		t.Fatalf("verifyChunkZero on mismatched bytes: expected error, got nil")
	}
}

func TestVerifyChunkZeroRejectsCountMismatch(t *testing.T) {
	chunk0 := []byte("chunk-0 contents")
	h0 := sha256.Sum256(chunk0)
	// Offer claims 5 chunks but metadata only encodes 1 hash.
	offer := &snapshotOffer{
		Chunks:   5,
		Metadata: buildMetadataBlob(t, [][]byte{h0[:]}),
	}
	if err := verifyChunkZero(offer, chunk0); err == nil {
		t.Fatalf("verifyChunkZero on chunk_hashes/Chunks mismatch: expected error, got nil")
	}
}

func TestVerifyChunkZeroRejectsMalformedMetadata(t *testing.T) {
	offer := &snapshotOffer{
		Chunks:   1,
		Metadata: []byte{0xFF, 0x01, 0x02}, // 0xFF is not the expected 0x0A tag
	}
	if err := verifyChunkZero(offer, []byte("anything")); err == nil {
		t.Fatalf("verifyChunkZero on malformed metadata: expected error, got nil")
	}
}

func TestVerifyChunkZeroRejectsEmptyMetadata(t *testing.T) {
	// Zero hashes parse cleanly but offer.Chunks=0 — defensive check
	// catches the would-be index-out-of-range on chunk_hashes[0].
	offer := &snapshotOffer{Chunks: 0, Metadata: nil}
	if err := verifyChunkZero(offer, []byte("anything")); err == nil {
		t.Fatalf("verifyChunkZero on zero-chunk offer: expected error, got nil")
	}
}
