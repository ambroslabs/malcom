package bootstrap

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestWriteReadHeightMarker_Roundtrip(t *testing.T) {
	dir := t.TempDir()
	if err := WriteHeightMarker(dir, 31060000); err != nil {
		t.Fatalf("WriteHeightMarker: %v", err)
	}
	got, err := ReadHeightMarker(dir)
	if err != nil {
		t.Fatalf("ReadHeightMarker: %v", err)
	}
	if got != 31060000 {
		t.Fatalf("got %d, want 31060000", got)
	}
}

func TestWriteHeightMarker_AtomicRename(t *testing.T) {
	// WriteHeightMarker uses a tmp+rename to avoid a half-written
	// file under crash. Pin the contract: after a successful write
	// the .tmp suffix file must not exist.
	dir := t.TempDir()
	if err := WriteHeightMarker(dir, 1); err != nil {
		t.Fatal(err)
	}
	tmpPath := filepath.Join(dir, BootstrapHeightMarker+".tmp")
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Fatalf("tmp file should have been renamed away; stat err=%v", err)
	}
}

func TestWriteHeightMarker_Idempotent(t *testing.T) {
	// Writing the same height twice must succeed; the second call
	// would be the recovery path (`malcom heal` refreshes the marker
	// even when the height didn't change, just to bump mtime as a
	// breadcrumb).
	dir := t.TempDir()
	for i := 0; i < 3; i++ {
		if err := WriteHeightMarker(dir, 12345); err != nil {
			t.Fatalf("WriteHeightMarker iter %d: %v", i, err)
		}
	}
	got, err := ReadHeightMarker(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got != 12345 {
		t.Fatalf("after 3 writes, got %d, want 12345", got)
	}
}

func TestReadHeightMarker_MissingFileIsErrNoMarker(t *testing.T) {
	dir := t.TempDir()
	_, err := ReadHeightMarker(dir)
	if !errors.Is(err, ErrNoMarker) {
		t.Fatalf("err = %v, want ErrNoMarker (so heal can branch on it)", err)
	}
}

func TestReadHeightMarker_CorruptValuesFail(t *testing.T) {
	cases := map[string]string{
		"empty":         "",
		"whitespace":    "   \n\t",
		"non-numeric":   "thirty-one million",
		"zero":          "0\n",
		"negative":      "-1\n",
		"trailing junk": "31060000 oops\n",
	}
	for name, contents := range cases {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, BootstrapHeightMarker), []byte(contents), 0o644); err != nil {
				t.Fatal(err)
			}
			if _, err := ReadHeightMarker(dir); err == nil {
				t.Fatalf("corrupt marker %q should error", contents)
			}
		})
	}
}

func TestReadHeightMarker_TolerantOfWhitespace(t *testing.T) {
	// Trailing newline is expected (Write writes one); leading
	// whitespace is also tolerated so an operator copy-pasting the
	// value doesn't trip a strict parser.
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, BootstrapHeightMarker), []byte("  \n42  \n\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := ReadHeightMarker(dir)
	if err != nil {
		t.Fatalf("ReadHeightMarker should tolerate trim-able whitespace; got %v", err)
	}
	if got != 42 {
		t.Fatalf("got %d, want 42", got)
	}
}
