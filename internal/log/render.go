package log

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// renderValue formats v according to key conventions:
//
//   - "*height" / "height" / "max_height" / "min_height" / "*_block"
//     / "count" / "*_count" → digit-grouped with underscore (Go-style):
//     31019000 → "31_019_000".
//   - "bytes" / "size" / "*_bytes" / "*_size" → humanized binary
//     (1.5 MiB, 2.76 GiB, …).
//   - "elapsed" / "duration" / "*_elapsed" / "*_duration" → short Go
//     duration (10m53s) when v is time.Duration; falls back to %v
//     when not.
//   - everything else: %v.
//
// Long results are truncated with "…" past truncateLimit chars; the
// raw value still flows through to JSON because pretty handler is the
// only consumer of this function.
const truncateLimit = 60

func renderValue(key string, v any) string {
	s := renderRaw(key, v)
	if len(s) > truncateLimit {
		// Reserve 1 byte for the ellipsis (3-byte rune); cut on a
		// rune boundary so we don't slice mid-codepoint.
		runes := []rune(s)
		if len(runes) > truncateLimit {
			s = string(runes[:truncateLimit-1]) + "…"
		}
	}
	return s
}

func renderRaw(key string, v any) string {
	lk := strings.ToLower(key)
	switch {
	case isHeightKey(lk):
		if n, ok := asUint64(v); ok {
			return groupDigits(n)
		}
	case isBytesKey(lk):
		if n, ok := asUint64(v); ok {
			return humanBytes(n)
		}
	case isDurationKey(lk):
		if d, ok := v.(time.Duration); ok {
			return d.String()
		}
	}
	return fmt.Sprintf("%v", v)
}

func isHeightKey(k string) bool {
	switch k {
	case "height", "min", "max", "count", "chunks", "items":
		return true
	}
	return strings.HasSuffix(k, "_height") ||
		strings.HasSuffix(k, "_block") ||
		strings.HasSuffix(k, "_count")
}

func isBytesKey(k string) bool {
	switch k {
	case "bytes", "size":
		return true
	}
	return strings.HasSuffix(k, "_bytes") || strings.HasSuffix(k, "_size")
}

func isDurationKey(k string) bool {
	switch k {
	case "elapsed", "duration":
		return true
	}
	return strings.HasSuffix(k, "_elapsed") || strings.HasSuffix(k, "_duration")
}

func asUint64(v any) (uint64, bool) {
	switch x := v.(type) {
	case uint64:
		return x, true
	case uint:
		return uint64(x), true
	case uint32:
		return uint64(x), true
	case uint16:
		return uint64(x), true
	case uint8:
		return uint64(x), true
	case int:
		if x >= 0 {
			return uint64(x), true
		}
	case int64:
		if x >= 0 {
			return uint64(x), true
		}
	case int32:
		if x >= 0 {
			return uint64(x), true
		}
	}
	return 0, false
}

// groupDigits renders n with underscore digit separators, the Go
// numeric-literal convention (31_019_000). Same as Python's
// "{:_}".format() but with no locale concerns.
func groupDigits(n uint64) string {
	s := strconv.FormatUint(n, 10)
	if len(s) <= 3 {
		return s
	}
	first := len(s) % 3
	if first == 0 {
		first = 3
	}
	var b strings.Builder
	b.Grow(len(s) + (len(s)-1)/3)
	b.WriteString(s[:first])
	for i := first; i < len(s); i += 3 {
		b.WriteByte('_')
		b.WriteString(s[i : i+3])
	}
	return b.String()
}

// humanBytes renders n as a binary prefix size. 1024 → "1.00 KiB",
// 2_960_000_000 → "2.76 GiB". Always 2 decimal places past KiB; no
// decimals for raw bytes.
func humanBytes(n uint64) string {
	const unit = 1024
	if n < unit {
		return strconv.FormatUint(n, 10) + "B"
	}
	div, exp := uint64(unit), 0
	for n2 := n / unit; n2 >= unit; n2 /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f%ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

// quoteIfNeeded returns s with logfmt-style quoting when s contains
// whitespace, '=', '"', or newline.
func quoteIfNeeded(s string) string {
	if strings.ContainsAny(s, " \t\"\n=") {
		return strconv.Quote(s)
	}
	return s
}
