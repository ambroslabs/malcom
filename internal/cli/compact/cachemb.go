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
	cacheMBCap   = 1024 // upper clamp regardless of how much RAM the host has
	cacheMBFloor = 64   // lower clamp to keep pebble's index resident
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
