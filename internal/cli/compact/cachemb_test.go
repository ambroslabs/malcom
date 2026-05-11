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

// Cgroup detection — the "must" fix from the PR #101 review. Without
// this, /proc/meminfo's host-wide MemAvailable lies about what we can
// actually allocate inside a container / under systemd MemoryMax.

func TestParseCgroupMemMax_V2Bytes(t *testing.T) {
	dir := t.TempDir()
	v2 := filepath.Join(dir, "memory.max")
	v1 := filepath.Join(dir, "v1")
	// 2 GiB in bytes
	if err := os.WriteFile(v2, []byte("2147483648\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := parseCgroupMemMaxFrom(v2, v1)
	if err != nil {
		t.Fatalf("parseCgroupMemMaxFrom: %v", err)
	}
	if got != 2048 {
		t.Fatalf("got %d MiB, want 2048", got)
	}
}

func TestParseCgroupMemMax_V2MaxSentinel(t *testing.T) {
	dir := t.TempDir()
	v2 := filepath.Join(dir, "memory.max")
	v1 := filepath.Join(dir, "v1")
	if err := os.WriteFile(v2, []byte("max\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	got, _ := parseCgroupMemMaxFrom(v2, v1)
	if got != -1 {
		t.Fatalf(`v2 "max" should be reported as -1 (unlimited); got %d`, got)
	}
}

func TestParseCgroupMemMax_V1Bytes(t *testing.T) {
	dir := t.TempDir()
	v2 := filepath.Join(dir, "absent-v2")
	v1 := filepath.Join(dir, "memory.limit_in_bytes")
	// 512 MiB in bytes
	if err := os.WriteFile(v1, []byte("536870912\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := parseCgroupMemMaxFrom(v2, v1)
	if err != nil {
		t.Fatalf("parseCgroupMemMaxFrom: %v", err)
	}
	if got != 512 {
		t.Fatalf("got %d MiB, want 512", got)
	}
}

func TestParseCgroupMemMax_V1UnlimitedSentinel(t *testing.T) {
	// Two flavours of "unlimited" we should both treat as -1:
	cases := []struct {
		name  string
		bytes string
	}{
		{"int64-max", "9223372036854775807"}, // 2^63 - 1
		{"page-aligned-near-max", "9223372036854771712"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			v2 := filepath.Join(dir, "absent-v2")
			v1 := filepath.Join(dir, "memory.limit_in_bytes")
			if err := os.WriteFile(v1, []byte(tc.bytes+"\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			got, err := parseCgroupMemMaxFrom(v2, v1)
			if err != nil {
				t.Fatal(err)
			}
			if got != -1 {
				t.Fatalf("v1 sentinel %q should map to -1 (unlimited); got %d", tc.bytes, got)
			}
		})
	}
}

func TestParseCgroupMemMax_NeitherPathPresent(t *testing.T) {
	dir := t.TempDir()
	v2 := filepath.Join(dir, "absent-v2")
	v1 := filepath.Join(dir, "absent-v1")
	got, err := parseCgroupMemMaxFrom(v2, v1)
	if err != nil {
		t.Fatalf("missing cgroup paths should not error; got %v", err)
	}
	if got != -1 {
		t.Fatalf("missing cgroup paths should be -1 (no limit); got %d", got)
	}
}

func TestEffectiveAvailableMB(t *testing.T) {
	cases := []struct {
		name   string
		mem    int64
		cgroup int64
		want   int64
	}{
		// No cgroup limit → just use MemAvailable.
		{"no cgroup", 16000, -1, 16000},
		// Cgroup smaller than host: cgroup wins (the load-bearing
		// case for issue #100's review feedback).
		{"cgroup smaller (container on big host)", 16000, 2000, 2000},
		// Cgroup larger than what host actually has free
		// (theoretically possible under cgroup-but-no-pressure).
		{"cgroup larger than mem", 4000, 16000, 4000},
		// Cgroup of 0 — pathological but should not overflow caller.
		{"cgroup zero", 4000, 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := effectiveAvailableMB(tc.mem, tc.cgroup); got != tc.want {
				t.Fatalf("effectiveAvailableMB(%d, %d) = %d, want %d",
					tc.mem, tc.cgroup, got, tc.want)
			}
		})
	}
}

// resolveCacheMB — the CLI resolution chain, table-tested. Catches the
// "should" #3 from the review: previously the chain's order was only
// enforced by integration; now any swap is a test failure.

func TestResolveCacheMB(t *testing.T) {
	cases := []struct {
		name       string
		flagVal    int
		cfgVal     int
		memMB      int64
		memErr     error
		wantValue  int
		wantSource string
	}{
		{
			name:       "flag wins over everything",
			flagVal:    777,
			cfgVal:     888,
			memMB:      8192,
			memErr:     nil,
			wantValue:  777,
			wantSource: "flag",
		},
		{
			name:       "config used when flag is 0",
			flagVal:    0,
			cfgVal:     888,
			memMB:      8192,
			memErr:     nil,
			wantValue:  888,
			wantSource: "config",
		},
		{
			name:       "heuristic when flag and config both 0",
			flagVal:    0,
			cfgVal:     0,
			memMB:      6144, // matches issue #100's 8 GiB-host scenario
			memErr:     nil,
			wantValue:  768, // 6144/8
			wantSource: "heuristic",
		},
		{
			name:       "fallback when memErr non-nil",
			flagVal:    0,
			cfgVal:     0,
			memMB:      0,
			memErr:     errSentinel,
			wantValue:  cacheMBFallback,
			wantSource: "fallback",
		},
		{
			name:       "flag wins even with memErr",
			flagVal:    512,
			cfgVal:     0,
			memMB:      0,
			memErr:     errSentinel,
			wantValue:  512,
			wantSource: "flag",
		},
		{
			name:       "config wins even with memErr",
			flagVal:    0,
			cfgVal:     512,
			memMB:      0,
			memErr:     errSentinel,
			wantValue:  512,
			wantSource: "config",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := resolveCacheMB(tc.flagVal, tc.cfgVal, tc.memMB, tc.memErr)
			if got.Value != tc.wantValue {
				t.Fatalf("Value = %d, want %d", got.Value, tc.wantValue)
			}
			if got.Source != tc.wantSource {
				t.Fatalf("Source = %q, want %q", got.Source, tc.wantSource)
			}
		})
	}
}

type sentinelErr struct{}

func (sentinelErr) Error() string { return "test sentinel" }

var errSentinel = sentinelErr{}
