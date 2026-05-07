package snapshotimport

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Import must refuse a snapshot dir that lacks the .complete marker —
// snapfetch only writes the marker after every chunk + metadata + meta.json
// is durable, so its absence means the fetch was interrupted.
func TestImportRefusesDirWithoutCompleteMarker(t *testing.T) {
	dir := t.TempDir()

	// Make the dir look superficially valid: a chunk file and meta.json,
	// but no .complete sentinel. Without the gate, openChunkDir would
	// happily start streaming this and fail deep inside the importer.
	if err := os.WriteFile(filepath.Join(dir, "chunk_00000.bin"), []byte("not a real chunk"), 0o644); err != nil {
		t.Fatalf("write chunk: %v", err)
	}

	_, err := Import(Options{
		SnapshotDir: dir,
		OutDir:      t.TempDir(),
		Height:      1,
	})
	if err == nil {
		t.Fatal("Import succeeded on dir without .complete; want error")
	}
	if !strings.Contains(err.Error(), ".complete") {
		t.Fatalf("error %q does not mention .complete marker", err)
	}
}
