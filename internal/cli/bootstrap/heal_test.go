package bootstrap

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// resolveHealHeight is the load-bearing policy in `malcom heal` and
// the only piece testable without a real chain binary. Four cases
// cover the policy table in the comment on resolveHealHeight.

func TestResolveHealHeight_MarkerOnly(t *testing.T) {
	dir := t.TempDir()
	if err := WriteHeightMarker(dir, 31060000); err != nil {
		t.Fatal(err)
	}
	got, err := resolveHealHeight(dir, 0)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got != 31060000 {
		t.Fatalf("got %d, want 31060000", got)
	}
}

func TestResolveHealHeight_FlagOnly(t *testing.T) {
	dir := t.TempDir() // no marker written
	got, err := resolveHealHeight(dir, 42)
	if err != nil {
		t.Fatalf("flag-only path should succeed: %v", err)
	}
	if got != 42 {
		t.Fatalf("got %d, want 42 (flag value)", got)
	}
}

func TestResolveHealHeight_MarkerAndMatchingFlag(t *testing.T) {
	dir := t.TempDir()
	if err := WriteHeightMarker(dir, 100); err != nil {
		t.Fatal(err)
	}
	got, err := resolveHealHeight(dir, 100)
	if err != nil {
		t.Fatalf("matching values shouldn't error: %v", err)
	}
	if got != 100 {
		t.Fatalf("got %d, want 100", got)
	}
}

func TestResolveHealHeight_MarkerAndConflictingFlagErrors(t *testing.T) {
	dir := t.TempDir()
	if err := WriteHeightMarker(dir, 100); err != nil {
		t.Fatal(err)
	}
	_, err := resolveHealHeight(dir, 200)
	if err == nil {
		t.Fatal("marker 100 + flag 200 should error; got nil")
	}
	if !strings.Contains(err.Error(), "marker says height 100") {
		t.Fatalf("error message should call out the mismatch with both values; got: %v", err)
	}
}

func TestResolveHealHeight_NeitherSetErrorsWithFixHint(t *testing.T) {
	dir := t.TempDir() // no marker, no flag
	_, err := resolveHealHeight(dir, 0)
	if err == nil {
		t.Fatal("no marker + no flag should error")
	}
	// Operator should be told what to do.
	if !strings.Contains(err.Error(), "-height") {
		t.Fatalf("error message should hint at -height flag; got: %v", err)
	}
	// And the underlying marker-missing reason should be visible so
	// the operator knows whether the marker file is missing vs corrupt.
	if !errors.Is(err, ErrNoMarker) && !strings.Contains(err.Error(), ErrNoMarker.Error()) {
		t.Fatalf("error should reference ErrNoMarker; got: %v", err)
	}
}

// blockstoreLooksPopulated tests the bricked-vs-healthy signal that
// gates `-force`. cometbft's pebble blockstore writes .sst files once
// blocks land; before that, the dir has only init files (or doesn't
// exist at all).

func TestBlockstoreLooksPopulated_MissingDirIsEmpty(t *testing.T) {
	if blockstoreLooksPopulated(t.TempDir()) {
		t.Fatal("missing blockstore.db should look empty (bricked or fresh)")
	}
}

func TestBlockstoreLooksPopulated_InitFilesOnlyIsEmpty(t *testing.T) {
	// Fresh-bootstrap signature: blockstore.db exists with the
	// pebble init artifacts but no .sst data. Simulate.
	dataDir := t.TempDir()
	bs := filepath.Join(dataDir, "blockstore.db")
	if err := os.MkdirAll(bs, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, n := range []string{"CURRENT", "MANIFEST-000000", "OPTIONS-000000", "000003.log"} {
		if err := os.WriteFile(filepath.Join(bs, n), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if blockstoreLooksPopulated(dataDir) {
		t.Fatal("blockstore.db with only init files should look empty")
	}
}

func TestBlockstoreLooksPopulated_SSTFileIsPopulated(t *testing.T) {
	// One .sst file is enough to flip the verdict — that's pebble's
	// block-data file format and only appears once data has been
	// written.
	dataDir := t.TempDir()
	bs := filepath.Join(dataDir, "blockstore.db")
	if err := os.MkdirAll(bs, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bs, "000010.sst"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if !blockstoreLooksPopulated(dataDir) {
		t.Fatal("blockstore.db with a .sst file should look populated (gate against accidental nuke)")
	}
}
