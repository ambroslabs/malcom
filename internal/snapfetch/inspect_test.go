package snapfetch

import (
	"bytes"
	"compress/zlib"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestInspectAndEnrichRewritesMetaAtomically verifies that the
// inspect-time meta.json rewrite goes through the atomic helper:
// meta.json is valid JSON enriched with the inspect-derived fields,
// and no meta.json.tmp leftover remains.
func TestInspectAndEnrichRewritesMetaAtomically(t *testing.T) {
	dir := t.TempDir()

	// Build a minimal valid snapshot dir.
	//
	// chunk_00000.bin: a zlib stream of zero bytes. Inspect reads
	// length-delimited proto items in a loop and breaks on EOF, so a
	// decompressed empty stream is a valid (empty) snapshot.
	var zbuf bytes.Buffer
	zw := zlib.NewWriter(&zbuf)
	if err := zw.Close(); err != nil {
		t.Fatalf("zlib close: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "chunk_00000.bin"), zbuf.Bytes(), 0o644); err != nil {
		t.Fatalf("write chunk: %v", err)
	}

	// metadata.bin: one chunk_hashes entry. Format-3 wire layout:
	// 0x0A = field-1 length-delimited tag, 0x04 = length 4, then 4 bytes.
	if err := os.WriteFile(
		filepath.Join(dir, "metadata.bin"),
		[]byte{0x0A, 0x04, 0xaa, 0xbb, 0xcc, 0xdd},
		0o644,
	); err != nil {
		t.Fatalf("write metadata.bin: %v", err)
	}

	// Initial meta.json — must be a JSON object so InspectAndEnrich's
	// Unmarshal-into-map succeeds.
	initial := map[string]interface{}{
		"height":        uint64(123),
		"format":        uint32(1),
		"downloaded_at": time.Now().UTC().Format(time.RFC3339Nano),
	}
	raw, err := json.MarshalIndent(initial, "", "  ")
	if err != nil {
		t.Fatalf("marshal initial: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "meta.json"), raw, 0o644); err != nil {
		t.Fatalf("write initial meta.json: %v", err)
	}

	if err := InspectAndEnrich(dir, nil); err != nil {
		t.Fatalf("InspectAndEnrich: %v", err)
	}

	// meta.json must be valid JSON and carry the enrichment fields.
	got, err := os.ReadFile(filepath.Join(dir, "meta.json"))
	if err != nil {
		t.Fatalf("read meta.json: %v", err)
	}
	var enriched map[string]interface{}
	if err := json.Unmarshal(got, &enriched); err != nil {
		t.Fatalf("meta.json invalid JSON after enrich: %v (raw=%q)", err, got)
	}
	if _, ok := enriched["inspected_at"]; !ok {
		t.Fatalf("meta.json missing inspected_at after enrich: %v", enriched)
	}
	if _, ok := enriched["chunk_hashes_hex"]; !ok {
		t.Fatalf("meta.json missing chunk_hashes_hex after enrich: %v", enriched)
	}
	// Original fields must survive (the enrich is a merge, not a replace).
	if h, ok := enriched["height"].(float64); !ok || uint64(h) != 123 {
		t.Fatalf("height not preserved: %v", enriched["height"])
	}

	// Atomic-write must not leave a tmp behind.
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
