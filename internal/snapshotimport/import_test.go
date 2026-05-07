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

// With .complete present, Import must pass the marker check and proceed
// to the next stage. Here the dir has the marker but no chunks, so we
// expect to hit the "no chunk_*.bin files" path — proves the gate
// doesn't over-reject a legitimate (if otherwise empty) snapshot dir.
func TestImportProceedsPastCompleteMarker(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, ".complete"), nil, 0o644); err != nil {
		t.Fatalf("write .complete: %v", err)
	}

	_, err := Import(Options{
		SnapshotDir: dir,
		OutDir:      t.TempDir(),
		Height:      1,
	})
	if err == nil {
		t.Fatal("Import succeeded on empty dir; want chunk-related error")
	}
	if strings.Contains(err.Error(), ".complete") {
		t.Fatalf("error %q mentions .complete — gate over-rejected", err)
	}
	if !strings.Contains(err.Error(), "chunk") {
		t.Fatalf("error %q does not look like a chunk-stage error", err)
	}
}

// A non-ENOENT stat error on .complete (e.g. EACCES) must surface as a
// distinct error wrapping the underlying cause — we don't want to
// silently treat it as "marker missing" or, worse, as success.
func TestImportSurfacesNonENOENTStatError(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root bypasses directory permissions")
	}
	dir := t.TempDir()
	// Strip all permissions so stat'ing children returns EACCES.
	// Restore in cleanup so t.TempDir's rm-rf can run.
	if err := os.Chmod(dir, 0o000); err != nil {
		t.Fatalf("chmod 0000: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })

	_, err := Import(Options{
		SnapshotDir: dir,
		OutDir:      t.TempDir(),
		Height:      1,
	})
	if err == nil {
		t.Fatal("Import succeeded on unreadable dir; want stat error")
	}
	// Should be the wrapped stat error, not the "missing marker" message.
	if !strings.Contains(err.Error(), "stat .complete") {
		t.Fatalf("error %q does not look like a wrapped stat error", err)
	}
}
