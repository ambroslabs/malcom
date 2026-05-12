package snapshotfetch

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ambroslabs/malcom/internal/config"
	"github.com/ambroslabs/malcom/internal/snapshotimport"
)

// TestPipelineWriteAppDBMeta exercises the post-import appdb
// meta.json write. Standalone `malcom snapshot import` has always
// written this file; the pipelined `fetch --import` path used to
// skip it, leaving downstream `verify` / `bootstrap` commands
// without their default chain_id / height inputs.
func TestPipelineWriteAppDBMeta(t *testing.T) {
	tmp := t.TempDir()
	snapDir := filepath.Join(tmp, "snapshot_demo_42")
	appdbDir := filepath.Join(tmp, "appdb_demo_42")
	for _, d := range []string{snapDir, appdbDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}

	// Seed the snapshot's meta.json with the field we propagate.
	wantHash := "abc123deadbeef"
	snapMeta := map[string]any{
		"chain_id": "demo",
		"height":   42,
		"hash_hex": wantHash,
	}
	mb, err := json.Marshal(snapMeta)
	if err != nil {
		t.Fatalf("marshal snap meta: %v", err)
	}
	if err := os.WriteFile(filepath.Join(snapDir, "meta.json"), mb, 0o644); err != nil {
		t.Fatalf("seed snap meta: %v", err)
	}

	p := &pipelineState{
		chain:       config.Chain{ChainID: "demo"},
		snapDir:     snapDir,
		appdbOut:    appdbDir,
		appdbHeight: 42,
	}
	now := time.Now().UTC().Truncate(time.Second)

	if err := p.writeAppDBMeta(now); err != nil {
		t.Fatalf("writeAppDBMeta: %v", err)
	}

	got, err := snapshotimport.ReadAppDBMeta(appdbDir)
	if err != nil {
		t.Fatalf("ReadAppDBMeta: %v", err)
	}
	if got.ChainID != "demo" {
		t.Errorf("ChainID = %q, want %q", got.ChainID, "demo")
	}
	if got.Height != 42 {
		t.Errorf("Height = %d, want 42", got.Height)
	}
	if got.SourceSnapshotHashHex != wantHash {
		t.Errorf("SourceSnapshotHashHex = %q, want %q", got.SourceSnapshotHashHex, wantHash)
	}
	if got.DBBackend != snapshotimport.DBBackendPebble {
		t.Errorf("DBBackend = %q, want %q", got.DBBackend, snapshotimport.DBBackendPebble)
	}
	if !got.ImportedAt.Equal(now) {
		t.Errorf("ImportedAt = %v, want %v", got.ImportedAt, now)
	}
}

func TestPipelineWriteAppDBMetaMissingSnapDir(t *testing.T) {
	p := &pipelineState{appdbOut: t.TempDir()}
	if err := p.writeAppDBMeta(time.Now()); err == nil {
		t.Fatal("expected error when snapDir is unset")
	}
}

func TestPipelineWriteAppDBMetaSnapMetaMissing(t *testing.T) {
	p := &pipelineState{
		snapDir:  t.TempDir(), // empty dir; no meta.json
		appdbOut: t.TempDir(),
	}
	if err := p.writeAppDBMeta(time.Now()); err == nil {
		t.Fatal("expected error when snapshot meta.json is missing")
	}
}
