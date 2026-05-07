package snapfetch

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

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
