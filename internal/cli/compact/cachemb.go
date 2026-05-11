package compact

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Default-cache sizing constants. Picked from pebble's marginal-returns
// curve for bulk-compaction workloads: the hot working set is mostly
// metadata (bloom + index blocks), which fits comfortably in 1 GiB
// even for a 100+ GiB DB. Below 64 MiB pebble degrades badly because
// the index can't all stay resident.
const (
	cacheMBCap      = 1024 // upper clamp regardless of how much RAM the host has
	cacheMBFloor    = 64   // lower clamp to keep pebble's index resident
	cacheMBFallback = 256  // used when /proc/meminfo is unreadable (sandbox, non-Linux)
)

// defaultCompactCacheMB computes a memory-proportional default for the
// pebble block-cache size during compact, given the host's available
// memory in MiB. Replaces the old hardcoded 8192 MiB that was killing
// 8 GiB hosts (#100): the issue reporter's 7.75 GB RSS at OOM time
// was almost entirely the cache growing toward its cap.
//
// Heuristic: 1/8 of MemAvailable, clamped to [floor, cap]. Sketches:
//   - 4 GiB host (~3 GiB available): 384 MiB cache (vs 8192 OOM)
//   - 8 GiB host (~6 GiB available): 768 MiB cache (vs 8192 OOM)
//   - 16 GiB host: hits the 1024 MiB cap
//   - 64 GiB host: hits the 1024 MiB cap (more cache rarely helps
//     bulk compact — the working set is small)
//
// Pure function so the policy is unit-testable without /proc/meminfo.
// The 1/8 ratio is the conservative end of pebble's recommendations
// for an LSM block cache; the rest of the host's RAM is better spent
// on the OS file cache buffering sstable reads.
func defaultCompactCacheMB(memAvailableMB int64) int {
	if memAvailableMB <= 0 {
		return cacheMBFloor
	}
	eighth := memAvailableMB / 8
	if eighth > cacheMBCap {
		return cacheMBCap
	}
	if eighth < cacheMBFloor {
		return cacheMBFloor
	}
	return int(eighth)
}

// readMemAvailableMB returns the kernel's MemAvailable metric from
// /proc/meminfo, in MiB. MemAvailable is the right signal here — it
// accounts for reclaimable cached pages, so it reflects what we can
// realistically allocate without pushing the kernel into swap or OOM.
//
// Linux-only path (cosmos chains run on Linux; cometbft's p2p stack
// pulls in libraries that don't even build on darwin/windows). On a
// container/sandbox without /proc/meminfo the caller falls back to
// a hardcoded conservative value.
func readMemAvailableMB() (int64, error) {
	return parseMemAvailableMBFrom("/proc/meminfo")
}

// readCgroupMemMaxMB returns the cgroup memory limit (v2 preferred,
// v1 as fallback) in MiB, or -1 if the process isn't in a memory-
// limited cgroup. Errors are returned only on parse failures of an
// explicitly-present cgroup file; a missing cgroup file is treated
// as "no limit" (returns -1, nil) so bare-host runs work without
// special-casing.
//
// Why this matters: /proc/meminfo reports the *host* kernel's view
// of available memory. Inside a container or under `systemd
// MemoryMax=`, our cgroup's actual budget can be a tiny fraction
// of that. Using the smaller of (cgroup-limit, MemAvailable) keeps
// the heuristic honest in modern deployments.
func readCgroupMemMaxMB() (int64, error) {
	return parseCgroupMemMaxFrom(
		"/sys/fs/cgroup/memory.max",                   // v2
		"/sys/fs/cgroup/memory/memory.limit_in_bytes", // v1
	)
}

// parseCgroupMemMaxFrom tries v2-style first (the file's body is
// either "max" or a decimal byte count), then v1-style (always a
// decimal byte count, with "unlimited" represented as a near-int64-
// max sentinel — different kernels use slightly different values, so
// we treat any value above 1 PiB as unlimited). Split out for
// testability.
func parseCgroupMemMaxFrom(v2Path, v1Path string) (int64, error) {
	if data, err := os.ReadFile(v2Path); err == nil {
		s := strings.TrimSpace(string(data))
		if s == "max" {
			return -1, nil
		}
		bytes, perr := strconv.ParseInt(s, 10, 64)
		if perr != nil {
			return 0, fmt.Errorf("parse cgroup v2 limit %q: %w", s, perr)
		}
		return bytes / (1024 * 1024), nil
	}
	if data, err := os.ReadFile(v1Path); err == nil {
		s := strings.TrimSpace(string(data))
		bytes, perr := strconv.ParseInt(s, 10, 64)
		if perr != nil {
			return 0, fmt.Errorf("parse cgroup v1 limit %q: %w", s, perr)
		}
		// v1 "unlimited" varies by kernel: common sentinels are
		// 9223372036854775807 (int64 max) and 9223372036854771712
		// (page-aligned int64 max). Treating "more than 1 PiB" as
		// unlimited is a safe heuristic regardless — no realistic
		// cgroup limit sits above 1 PiB.
		const oneEiB = int64(1) << 50
		if bytes > oneEiB {
			return -1, nil
		}
		return bytes / (1024 * 1024), nil
	}
	return -1, nil
}

// effectiveAvailableMB returns the smaller of (host MemAvailable,
// cgroup limit), or just MemAvailable when there's no cgroup limit.
// Pure; takes both inputs explicitly so the policy is testable
// without /proc or /sys.
//
// cgroupMaxMB < 0 means "no cgroup limit" (bare host or unlimited
// cgroup). 0 is treated as a real (zero) limit — should never happen
// in practice but we don't want to overflow the caller's heuristic
// either.
func effectiveAvailableMB(memAvailMB, cgroupMaxMB int64) int64 {
	if cgroupMaxMB < 0 {
		return memAvailMB
	}
	if cgroupMaxMB < memAvailMB {
		return cgroupMaxMB
	}
	return memAvailMB
}

// ResolvedCacheMB is the value picked by the resolution chain along
// with metadata for the operator-facing log line.
type ResolvedCacheMB struct {
	// Value is the cache-mb value to pass to pebble.
	Value int
	// Source identifies which branch of the chain provided the
	// value: "flag", "config", "heuristic", "fallback". Stable
	// string so an operator's log-scraper can branch on it.
	Source string
	// LogKVs are additional slog key/value pairs the caller should
	// emit alongside the value (e.g. "mem_available_mb", "err").
	// Variadic any so callers can pass directly to log.Info.
	LogKVs []any
}

// resolveCacheMB encodes the CLI's flag → config → heuristic →
// fallback resolution policy for compact's cache-mb. Pure: takes
// the four observable inputs (flag value, config value, effective
// available memory in MiB, and any error reading that memory) and
// returns the chosen value with provenance.
//
// Splitting this out makes the policy table-testable without
// invoking the CLI or stubbing /proc.
func resolveCacheMB(flagVal, cfgVal int, memMB int64, memErr error) ResolvedCacheMB {
	if flagVal > 0 {
		return ResolvedCacheMB{Value: flagVal, Source: "flag"}
	}
	if cfgVal > 0 {
		return ResolvedCacheMB{Value: cfgVal, Source: "config"}
	}
	if memErr != nil {
		return ResolvedCacheMB{
			Value:  cacheMBFallback,
			Source: "fallback",
			LogKVs: []any{"err", memErr},
		}
	}
	return ResolvedCacheMB{
		Value:  defaultCompactCacheMB(memMB),
		Source: "heuristic",
		LogKVs: []any{"mem_available_mb", memMB},
	}
}

// parseMemAvailableMBFrom is readMemAvailableMB's underlying parse
// loop, split out so the parser can be unit-tested against a fake
// meminfo file without monkey-patching /proc.
func parseMemAvailableMBFrom(path string) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	for s.Scan() {
		line := s.Text()
		if !strings.HasPrefix(line, "MemAvailable:") {
			continue
		}
		// Line shape: "MemAvailable:     7649012 kB"
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return 0, fmt.Errorf("malformed MemAvailable line: %q", line)
		}
		kB, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse MemAvailable value %q: %w", fields[1], err)
		}
		return kB / 1024, nil
	}
	if err := s.Err(); err != nil {
		return 0, fmt.Errorf("scan %s: %w", path, err)
	}
	return 0, fmt.Errorf("MemAvailable not found in %s", path)
}
