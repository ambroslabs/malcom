package served

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSetRecordAndAllOrdering(t *testing.T) {
	dir := t.TempDir()
	s, err := New(filepath.Join(dir, "served.json"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	s.Record("id-a", "id-a@1.1.1.1:26656")
	time.Sleep(2 * time.Millisecond) // ensure distinct LastSeenAt
	s.Record("id-b", "id-b@2.2.2.2:26656")
	time.Sleep(2 * time.Millisecond)
	s.Record("id-a", "id-a@1.1.1.1:26656") // bump a back to head

	got := s.All()
	if len(got) != 2 {
		t.Fatalf("All() len=%d, want 2", len(got))
	}
	// id-a was bumped most recently, so it should be at the head.
	if got[0].ID != "id-a" {
		t.Fatalf("All()[0].ID=%q, want id-a (most recent)", got[0].ID)
	}
	if got[0].ChunksTotal != 2 {
		t.Fatalf("id-a ChunksTotal=%d, want 2", got[0].ChunksTotal)
	}
	if got[1].ID != "id-b" {
		t.Fatalf("All()[1].ID=%q, want id-b", got[1].ID)
	}
}

func TestSetSaveLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "served.json")

	s, err := New(path)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	s.Record("id-x", "id-x@5.5.5.5:26656")
	s.Record("id-y", "id-y@6.6.6.6:26656")
	s.Record("id-x", "id-x@5.5.5.5:26656") // 2nd chunk
	if err := s.Save(); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// Reload and verify
	s2, err := New(path)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	got := s2.All()
	if len(got) != 2 {
		t.Fatalf("reload All() len=%d, want 2", len(got))
	}
	for _, e := range got {
		if e.ID == "id-x" && e.ChunksTotal != 2 {
			t.Fatalf("id-x post-reload ChunksTotal=%d, want 2", e.ChunksTotal)
		}
		if e.ID == "id-y" && e.ChunksTotal != 1 {
			t.Fatalf("id-y post-reload ChunksTotal=%d, want 1", e.ChunksTotal)
		}
	}
}

func TestSetMissingFileIsEmpty(t *testing.T) {
	dir := t.TempDir()
	s, err := New(filepath.Join(dir, "does-not-exist.json"))
	if err != nil {
		t.Fatalf("New on missing path: %v", err)
	}
	if got := s.Len(); got != 0 {
		t.Fatalf("Len()=%d, want 0", got)
	}
}

func TestSetNilSafe(t *testing.T) {
	var s *Set
	s.Record("id", "id@x:0") // must not panic
	if got := s.All(); got != nil {
		t.Fatalf("nil.All()=%v, want nil", got)
	}
	if got := s.Len(); got != 0 {
		t.Fatalf("nil.Len()=%d, want 0", got)
	}
	if err := s.Save(); err != nil {
		t.Fatalf("nil.Save() err=%v, want nil", err)
	}
}

func TestSetExpiredEntriesPrunedOnLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "served.json")

	// Hand-write a file with one fresh and one ancient entry.
	old := time.Now().Add(-DefaultMaxAge - 24*time.Hour)
	new := time.Now()
	content := `{"entries":[
		{"id":"old","addr":"old@1.1.1.1:1","first_seen_at":"` + old.UTC().Format(time.RFC3339Nano) + `","last_seen_at":"` + old.UTC().Format(time.RFC3339Nano) + `","chunks_total":1},
		{"id":"new","addr":"new@2.2.2.2:2","first_seen_at":"` + new.UTC().Format(time.RFC3339Nano) + `","last_seen_at":"` + new.UTC().Format(time.RFC3339Nano) + `","chunks_total":1}
	]}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}

	s, err := New(path)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	got := s.All()
	if len(got) != 1 {
		t.Fatalf("post-prune len=%d, want 1 (old entry should have been dropped)", len(got))
	}
	if got[0].ID != "new" {
		t.Fatalf("survivor ID=%q, want new", got[0].ID)
	}
}
