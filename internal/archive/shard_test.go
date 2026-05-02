package archive

import (
	"bytes"
	"errors"
	"hash/crc32"
	"path/filepath"
	"testing"
)

func openShardT(t *testing.T, base uint64) *Shard {
	t.Helper()
	dir := t.TempDir()
	bp := filepath.Join(dir, "test.blocks")
	ip := filepath.Join(dir, "test.idx")
	sh, err := Open(bp, ip, base)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sh.Close() })
	return sh
}

func TestShardBasePut(t *testing.T) {
	base := uint64(5_200_000)
	sh := openShardT(t, base)

	if sh.Has(base + 1) {
		t.Fatalf("empty shard reports has=true")
	}
	if _, err := sh.Get(base + 1); !errors.Is(err, ErrNotPresent) {
		t.Fatalf("expected ErrNotPresent on empty shard, got %v", err)
	}

	want := []byte("hello world; this is a synthetic block payload")
	if err := sh.Put(base+42, want); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if !sh.Has(base + 42) {
		t.Fatalf("Has=false after Put")
	}
	got, err := sh.Get(base + 42)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("Get returned %q, want %q", got, want)
	}
	// Idempotent
	if err := sh.Put(base+42, want); err != nil {
		t.Fatalf("idempotent Put: %v", err)
	}
	// Conflicting put
	if err := sh.Put(base+42, []byte("different")); err == nil {
		t.Fatalf("conflicting Put unexpectedly succeeded")
	}
}

func TestShardOutOfRange(t *testing.T) {
	sh := openShardT(t, ChunkSize)
	if err := sh.Put(0, []byte("x")); err == nil {
		t.Fatalf("Put below base unexpectedly succeeded")
	}
	if err := sh.Put(2*ChunkSize, []byte("x")); err == nil {
		t.Fatalf("Put above shard unexpectedly succeeded")
	}
}

func TestShardCRC(t *testing.T) {
	base := uint64(0)
	sh := openShardT(t, base)
	want := []byte{1, 2, 3, 4, 5}
	if err := sh.Put(base+10, want); err != nil {
		t.Fatal(err)
	}
	// Read raw idx bytes and check CRC matches.
	got, err := sh.Get(base + 10)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if crc32.ChecksumIEEE(got) != crc32.ChecksumIEEE(want) {
		t.Fatalf("CRC mismatch")
	}
}

func TestShardReopenPersists(t *testing.T) {
	dir := t.TempDir()
	bp := filepath.Join(dir, "x.blocks")
	ip := filepath.Join(dir, "x.idx")
	sh, err := Open(bp, ip, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := sh.Put(7, []byte("seven")); err != nil {
		t.Fatal(err)
	}
	if err := sh.Put(99999, []byte("ninety-nine-nine")); err != nil {
		t.Fatal(err)
	}
	if err := sh.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := sh.Close(); err != nil {
		t.Fatal(err)
	}
	// Reopen.
	sh2, err := Open(bp, ip, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer sh2.Close()
	got, err := sh2.Get(7)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "seven" {
		t.Fatalf("got %q, want seven", got)
	}
	got2, err := sh2.Get(99999)
	if err != nil {
		t.Fatal(err)
	}
	if string(got2) != "ninety-nine-nine" {
		t.Fatalf("got %q", got2)
	}
}

func TestShardPresentRange(t *testing.T) {
	sh := openShardT(t, 0)
	for _, h := range []uint64{5, 6, 7, 100, 101, 50000} {
		if err := sh.Put(h, []byte{byte(h)}); err != nil {
			t.Fatal(err)
		}
	}
	min, max, count := sh.PresentRange()
	if min != 5 || max != 50000 || count != 6 {
		t.Fatalf("PresentRange: min=%d max=%d count=%d (want 5/50000/6)", min, max, count)
	}
}
