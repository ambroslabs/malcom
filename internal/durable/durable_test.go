package durable

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWriteFileContentAndMode(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.bin")
	want := []byte("hello durable")
	if err := WriteFile(path, want, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("content mismatch: got %q, want %q", got, want)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("mode = %o, want 0o600", info.Mode().Perm())
	}
}

func TestWriteFileOverwritesAtomically(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.bin")
	if err := WriteFile(path, []byte("first"), 0o644); err != nil {
		t.Fatalf("first WriteFile: %v", err)
	}
	if err := WriteFile(path, []byte("second"), 0o644); err != nil {
		t.Fatalf("second WriteFile: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(got) != "second" {
		t.Fatalf("content = %q, want %q", got, "second")
	}
}

func TestWriteFileLeavesNoTmpAfterSuccess(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.bin")
	if err := WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".tmp") {
			t.Fatalf("found leftover tmp file %q after successful WriteFile", e.Name())
		}
	}
}

// errReader returns an error after n bytes. Used to exercise the
// write-error cleanup path in WriteFileFrom.
type errReader struct {
	data []byte
	n    int
	err  error
}

func (r *errReader) Read(p []byte) (int, error) {
	if r.n > 0 {
		k := copy(p, r.data[:r.n])
		r.data = r.data[r.n:]
		r.n = 0
		return k, nil
	}
	return 0, r.err
}

func TestWriteFileFromCleansUpTmpOnReadError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.bin")
	src := &errReader{data: []byte("partial"), n: 7, err: errors.New("simulated read failure")}
	err := WriteFileFrom(path, src, 0o644)
	if err == nil {
		t.Fatal("WriteFileFrom: expected error from failing reader, got nil")
	}
	if _, err := os.Stat(path); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("final path should not exist after a failed write; got Stat err = %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, e := range entries {
		t.Errorf("found leftover entry %q after failed WriteFileFrom", e.Name())
	}
}

func TestWriteFileMissingParentDirErrors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nonexistent", "out.bin")
	if err := WriteFile(path, []byte("x"), 0o644); err == nil {
		t.Fatal("WriteFile to a missing parent dir: expected error, got nil")
	}
}

func TestWriteFileFromStreamsCorrectly(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "stream.bin")
	want := bytes.Repeat([]byte("abc"), 10000)
	if err := WriteFileFrom(path, bytes.NewReader(want), 0o644); err != nil {
		t.Fatalf("WriteFileFrom: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("content mismatch: len got=%d want=%d", len(got), len(want))
	}
}

func TestCopyFilePreservesContentAndMode(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.bin")
	dst := filepath.Join(dir, "dst.bin")
	want := []byte("payload")
	if err := os.WriteFile(src, want, 0o600); err != nil {
		t.Fatalf("seed src: %v", err)
	}
	if err := CopyFile(src, dst); err != nil {
		t.Fatalf("CopyFile: %v", err)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("ReadFile dst: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("dst content = %q, want %q", got, want)
	}
	info, err := os.Stat(dst)
	if err != nil {
		t.Fatalf("Stat dst: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("dst mode = %o, want 0o600 (taken from src)", info.Mode().Perm())
	}
}

func TestCopyFileMissingSrcErrors(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "nope")
	dst := filepath.Join(dir, "dst.bin")
	if err := CopyFile(src, dst); err == nil {
		t.Fatal("CopyFile with missing src: expected error, got nil")
	}
}

func TestCloneTreeMirrorsContentAndStructure(t *testing.T) {
	src := t.TempDir()
	dst := filepath.Join(t.TempDir(), "out")

	// Build a small tree: a/b/c.txt, a/d.txt, e.txt.
	mustMkdirAll(t, filepath.Join(src, "a", "b"))
	mustWrite(t, filepath.Join(src, "a", "b", "c.txt"), "c")
	mustWrite(t, filepath.Join(src, "a", "d.txt"), "d")
	mustWrite(t, filepath.Join(src, "e.txt"), "e")

	if err := CloneTree(src, dst); err != nil {
		t.Fatalf("CloneTree: %v", err)
	}

	for rel, want := range map[string]string{
		"a/b/c.txt": "c",
		"a/d.txt":   "d",
		"e.txt":     "e",
	} {
		got, err := os.ReadFile(filepath.Join(dst, rel))
		if err != nil {
			t.Fatalf("ReadFile %s: %v", rel, err)
		}
		if string(got) != want {
			t.Errorf("%s = %q, want %q", rel, got, want)
		}
	}
}

func TestCloneTreePreservesFileModes(t *testing.T) {
	src := t.TempDir()
	dst := filepath.Join(t.TempDir(), "out")

	mustMkdirAll(t, filepath.Join(src, "sub"))
	for _, c := range []struct {
		rel  string
		mode os.FileMode
	}{
		{"private.bin", 0o600},
		{"shared.bin", 0o644},
		{"sub/exec.bin", 0o755},
	} {
		p := filepath.Join(src, c.rel)
		if err := os.WriteFile(p, []byte("x"), c.mode); err != nil {
			t.Fatalf("seed %s: %v", c.rel, err)
		}
		// os.WriteFile applies umask; force the mode explicitly.
		if err := os.Chmod(p, c.mode); err != nil {
			t.Fatalf("chmod %s: %v", c.rel, err)
		}
	}

	if err := CloneTree(src, dst); err != nil {
		t.Fatalf("CloneTree: %v", err)
	}

	for _, c := range []struct {
		rel  string
		mode os.FileMode
	}{
		{"private.bin", 0o600},
		{"shared.bin", 0o644},
		{"sub/exec.bin", 0o755},
	} {
		info, err := os.Stat(filepath.Join(dst, c.rel))
		if err != nil {
			t.Fatalf("Stat %s: %v", c.rel, err)
		}
		if got := info.Mode().Perm(); got != c.mode {
			t.Errorf("%s: mode = %o, want %o", c.rel, got, c.mode)
		}
	}
}

func TestCloneTreeEmptyDirCopies(t *testing.T) {
	src := t.TempDir()
	dst := filepath.Join(t.TempDir(), "out")
	if err := CloneTree(src, dst); err != nil {
		t.Fatalf("CloneTree on empty src: %v", err)
	}
	if _, err := os.Stat(dst); err != nil {
		t.Fatalf("Stat dst: %v", err)
	}
}

func TestRenameMovesPathAndFsyncsParent(t *testing.T) {
	base := t.TempDir()
	src := filepath.Join(base, "src")
	dst := filepath.Join(base, "dst")
	if err := os.WriteFile(src, []byte("payload"), 0o644); err != nil {
		t.Fatalf("seed src: %v", err)
	}
	if err := Rename(src, dst); err != nil {
		t.Fatalf("Rename: %v", err)
	}
	if _, err := os.Stat(src); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("src should be gone after rename; Stat err = %v", err)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("ReadFile dst: %v", err)
	}
	if string(got) != "payload" {
		t.Fatalf("dst content = %q, want %q", got, "payload")
	}
}

func TestFsyncDirOnExisting(t *testing.T) {
	if err := FsyncDir(t.TempDir()); err != nil {
		t.Fatalf("FsyncDir on tempdir: %v", err)
	}
}

func TestFsyncDirOnMissingErrors(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "nope")
	if err := FsyncDir(missing); err == nil {
		t.Fatal("FsyncDir on missing dir: expected error, got nil")
	}
}

func mustMkdirAll(t *testing.T, p string) {
	t.Helper()
	if err := os.MkdirAll(p, 0o755); err != nil {
		t.Fatalf("MkdirAll %s: %v", p, err)
	}
}

func mustWrite(t *testing.T, p, content string) {
	t.Helper()
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile %s: %v", p, err)
	}
}

