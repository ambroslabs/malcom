package compact

import (
	"os"
	"path/filepath"
	"testing"
)

// defaultCompactCacheMB's policy table. Picked the sketches from the
// docstring so anyone reading the test sees the same numbers.
func TestDefaultCompactCacheMB(t *testing.T) {
	cases := []struct {
		name           string
		memAvailableMB int64
		want           int
	}{
		// Issue #100's exact failure mode: 8 GiB DO droplet (~6 GiB
		// available after kernel + other services). 1/8 = 768 MiB
		// instead of the 8192 MiB that OOM'd the box.
		{"8 GiB host (issue #100)", 6144, 768},

		// 4 GiB tiny host
		{"4 GiB host", 3072, 384},

		// 16 GiB box hits the 1024 MiB cap
		{"16 GiB host hits cap", 12288, 1024},

		// 64 GiB box also hits the cap (more cache rarely helps bulk
		// compact — the working set is mostly metadata)
		{"64 GiB host hits cap", 50000, 1024},

		// Floor at 64 MiB so pebble's index can stay resident
		{"tiny host hits floor", 256, 64},

		// Edge: pathological zero input. Floor is the safe answer
		// (returning 0 would let pebble's library-level 8192 kick
		// in, defeating the whole point).
		{"zero input hits floor", 0, 64},

		// Edge: negative — same as zero
		{"negative input hits floor", -1, 64},

		// Boundary: exact floor value lands at floor
		{"floor boundary", int64(cacheMBFloor) * 8, cacheMBFloor},

		// Boundary: exact cap value lands at cap
		{"cap boundary", int64(cacheMBCap) * 8, cacheMBCap},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := defaultCompactCacheMB(tc.memAvailableMB); got != tc.want {
				t.Fatalf("defaultCompactCacheMB(%d) = %d, want %d", tc.memAvailableMB, got, tc.want)
			}
		})
	}
}

// TestReadMemAvailableMB just sanity-checks that we can parse the
// host's actual /proc/meminfo. Run on a Linux dev box (CI) it always
// produces a positive value; on a sandbox without /proc/meminfo we
// skip.
func TestReadMemAvailableMB_ReadsHost(t *testing.T) {
	if _, err := os.Stat("/proc/meminfo"); err != nil {
		t.Skipf("/proc/meminfo not available: %v", err)
	}
	got, err := readMemAvailableMB()
	if err != nil {
		t.Fatalf("readMemAvailableMB: %v", err)
	}
	if got <= 0 {
		t.Fatalf("MemAvailable should be positive on a healthy host, got %d", got)
	}
	// Sanity range: between 100 MiB and 10 TiB. Anything outside is
	// almost certainly a parser bug.
	if got < 100 || got > 10*1024*1024 {
		t.Fatalf("MemAvailable %d MiB is wildly out of range; parser bug?", got)
	}
}

// TestReadMemAvailableMB_ParsesFakeMeminfo verifies the parser
// independently of the host — we point it at a fake file under a tmp
// dir. Since readMemAvailableMB hardcodes /proc/meminfo, we can't
// inject the path directly; instead we exercise the underlying
// parse logic by replicating its parse loop here against a fake.
//
// Future refactor could extract a parseMemAvailable(io.Reader) for
// cleaner injection; today's API surface is small enough that this
// is fine.
func TestReadMemAvailableMB_ParsesFakeMeminfo(t *testing.T) {
	// Stand up a fake meminfo file and parse it via the same scanner
	// pattern readMemAvailableMB uses. Asserts the format expectations
	// (line prefix + units in kB).
	dir := t.TempDir()
	path := filepath.Join(dir, "meminfo")
	content := []byte(`MemTotal:        8000000 kB
MemFree:          400000 kB
MemAvailable:    7649012 kB
Buffers:          100000 kB
`)
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := parseMemAvailableMBFrom(path)
	if err != nil {
		t.Fatalf("parseMemAvailableMBFrom: %v", err)
	}
	// 7649012 kB / 1024 = 7469 MiB
	if got != 7469 {
		t.Fatalf("got %d MiB, want 7469", got)
	}
}

func TestReadMemAvailableMB_FailsOnMissingLine(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "meminfo")
	if err := os.WriteFile(path, []byte("MemTotal: 1000 kB\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := parseMemAvailableMBFrom(path); err == nil {
		t.Fatal("expected error when MemAvailable line is absent")
	}
}
