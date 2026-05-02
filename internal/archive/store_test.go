package archive

import (
	"reflect"
	"testing"
)

func TestStoreRanges(t *testing.T) {
	dir := t.TempDir()
	st, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	// Two contiguous runs across a shard boundary.
	for h := uint64(99_995); h <= 100_010; h++ {
		if err := st.Put(h, []byte{byte(h)}); err != nil {
			t.Fatal(err)
		}
	}
	// A standalone entry well past the runs.
	if err := st.Put(250_000, []byte{0xff}); err != nil {
		t.Fatal(err)
	}

	ranges, total, err := st.Ranges()
	if err != nil {
		t.Fatal(err)
	}
	want := []Range{
		{Lo: 99_995, Hi: 100_010},
		{Lo: 250_000, Hi: 250_000},
	}
	if !reflect.DeepEqual(ranges, want) {
		t.Fatalf("Ranges:\n got=%+v\nwant=%+v", ranges, want)
	}
	if total != 17 {
		t.Fatalf("total=%d want 17", total)
	}
}

func TestStoreMissing(t *testing.T) {
	dir := t.TempDir()
	st, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	for _, h := range []uint64{100, 101, 102, 200, 300, 301} {
		if err := st.Put(h, []byte{1}); err != nil {
			t.Fatal(err)
		}
	}
	gaps, missing, err := st.Missing(100, 305)
	if err != nil {
		t.Fatal(err)
	}
	want := []Range{
		{Lo: 103, Hi: 199},
		{Lo: 201, Hi: 299},
		{Lo: 302, Hi: 305},
	}
	if !reflect.DeepEqual(gaps, want) {
		t.Fatalf("Missing gaps:\n got=%+v\nwant=%+v", gaps, want)
	}
	wantMissing := uint64((199 - 103 + 1) + (299 - 201 + 1) + (305 - 302 + 1))
	if missing != wantMissing {
		t.Fatalf("missing count=%d want %d", missing, wantMissing)
	}
}

func TestStoreReopen(t *testing.T) {
	dir := t.TempDir()
	st, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Put(42, []byte("hi")); err != nil {
		t.Fatal(err)
	}
	if err := st.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st2, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	got, err := st2.Get(42)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "hi" {
		t.Fatalf("got %q after reopen", got)
	}
}
