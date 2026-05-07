package banlist

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNewEmptyPathErrors(t *testing.T) {
	if _, err := New(""); err == nil {
		t.Fatalf("New(\"\") = nil err, want error")
	}
}

func TestNewMissingFileIsFine(t *testing.T) {
	dir := t.TempDir()
	s, err := New(filepath.Join(dir, "nope.json"))
	if err != nil {
		t.Fatalf("New(missing) err=%v, want nil", err)
	}
	if s.Len() != 0 {
		t.Fatalf("Len=%d, want 0", s.Len())
	}
}

func TestAddSaveLoadRoundtrip(t *testing.T) {
	// Path includes a missing parent dir to also exercise Save's MkdirAll.
	path := filepath.Join(t.TempDir(), "sub", "deep", "banlist.json")
	s, err := New(path)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	s.Add("a@1.1.1.1:26656", "first")
	s.Add("a@1.1.1.1:26656", "second")
	s.Add("b@2.2.2.2:26656", "test")
	want := s.entries["a@1.1.1.1:26656"]
	if err := s.Save(); err != nil {
		t.Fatalf("Save: %v", err)
	}

	s2, err := New(path)
	if err != nil {
		t.Fatalf("reload New: %v", err)
	}
	if s2.Len() != 2 {
		t.Fatalf("Len=%d after reload, want 2", s2.Len())
	}
	if !s2.Has("b@2.2.2.2:26656") {
		t.Fatalf("entry b missing after reload")
	}
	got, ok := s2.entries["a@1.1.1.1:26656"]
	if !ok {
		t.Fatalf("entry a missing after reload")
	}
	if got.FailCount != want.FailCount {
		t.Fatalf("FailCount=%d after reload, want %d", got.FailCount, want.FailCount)
	}
	if got.Reason != want.Reason {
		t.Fatalf("Reason=%q after reload, want %q", got.Reason, want.Reason)
	}
	if !got.FirstSeenAt.Equal(want.FirstSeenAt) {
		t.Fatalf("FirstSeenAt=%v after reload, want %v", got.FirstSeenAt, want.FirstSeenAt)
	}
	if !got.LastSeenAt.Equal(want.LastSeenAt) {
		t.Fatalf("LastSeenAt=%v after reload, want %v", got.LastSeenAt, want.LastSeenAt)
	}
}

func TestAddBumpsFailCountKeepsFirstSeen(t *testing.T) {
	s, _ := New(filepath.Join(t.TempDir(), "b.json"))
	s.Add("x@1.2.3.4:26656", "first")
	first := s.entries["x@1.2.3.4:26656"].FirstSeenAt
	time.Sleep(10 * time.Millisecond)
	s.Add("x@1.2.3.4:26656", "second")
	e := s.entries["x@1.2.3.4:26656"]
	if e.FailCount != 2 {
		t.Fatalf("FailCount=%d, want 2", e.FailCount)
	}
	if !e.FirstSeenAt.Equal(first) {
		t.Fatalf("FirstSeenAt changed: %v vs %v", e.FirstSeenAt, first)
	}
	if e.Reason != "second" {
		t.Fatalf("Reason=%q, want %q", e.Reason, "second")
	}
}

func TestRemoveIdempotent(t *testing.T) {
	s, _ := New(filepath.Join(t.TempDir(), "b.json"))
	s.Add("x@1.1.1.1:26656", "")
	s.Remove("x@1.1.1.1:26656")
	if s.Has("x@1.1.1.1:26656") {
		t.Fatalf("entry still present after Remove")
	}
	// Second Remove of nonexistent: no panic.
	s.Remove("x@1.1.1.1:26656")
	s.Remove("never-was-there")
}

func TestLoadPrunesAgedEntries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "b.json")
	s, _ := New(path)
	s.Add("fresh@1.1.1.1:26656", "")
	// Manually inject an old entry past the default MaxAge.
	s.entries["old@2.2.2.2:26656"] = Entry{
		Addr:        "old@2.2.2.2:26656",
		FirstSeenAt: time.Now().Add(-2 * DefaultMaxAge),
		LastSeenAt:  time.Now().Add(-2 * DefaultMaxAge),
	}
	if err := s.Save(); err != nil {
		t.Fatalf("Save: %v", err)
	}

	s2, err := New(path)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if s2.Has("old@2.2.2.2:26656") {
		t.Fatalf("aged entry not pruned")
	}
	if !s2.Has("fresh@1.1.1.1:26656") {
		t.Fatalf("fresh entry pruned by mistake")
	}
}

func TestCapEvictsOldestByLastSeen(t *testing.T) {
	s, _ := New(filepath.Join(t.TempDir(), "b.json"))
	s.cap = 2
	s.Add("oldest@1:1", "")
	time.Sleep(5 * time.Millisecond)
	s.Add("middle@2:2", "")
	time.Sleep(5 * time.Millisecond)
	s.Add("newest@3:3", "")
	if s.Len() != 2 {
		t.Fatalf("Len=%d, want 2 (cap)", s.Len())
	}
	if s.Has("oldest@1:1") {
		t.Fatalf("oldest entry not evicted")
	}
	if !s.Has("middle@2:2") || !s.Has("newest@3:3") {
		t.Fatalf("middle/newest entries missing")
	}
}

func TestCorruptFileErrors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "b.json")
	if err := os.WriteFile(path, []byte("{not valid json"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := New(path); err == nil {
		t.Fatalf("New(corrupt) = nil err, want error")
	}
}

func TestNilSetMethodsSafe(t *testing.T) {
	var s *Set
	if s.Has("x") {
		t.Fatalf("nil Has returned true")
	}
	s.Add("x", "")
	s.Remove("x")
	if s.Len() != 0 {
		t.Fatalf("nil Len=%d, want 0", s.Len())
	}
	if err := s.Save(); err != nil {
		t.Fatalf("nil Save err=%v, want nil", err)
	}
	if got := s.Path(); got != "" {
		t.Fatalf("nil Path=%q, want empty", got)
	}
}
