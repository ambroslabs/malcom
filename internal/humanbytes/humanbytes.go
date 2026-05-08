// Package humanbytes formats byte counts as short human-readable strings.
package humanbytes

import "fmt"

// Format returns a short human-readable representation of n bytes.
func Format(n uint64) string {
	const (
		k = 1024
		m = k * 1024
		g = m * 1024
		t = g * 1024
	)
	switch {
	case n >= t:
		return fmt.Sprintf("%.2f TB", float64(n)/float64(t))
	case n >= g:
		return fmt.Sprintf("%.2f GB", float64(n)/float64(g))
	case n >= m:
		return fmt.Sprintf("%.1f MB", float64(n)/float64(m))
	case n >= k:
		return fmt.Sprintf("%.1f KB", float64(n)/float64(k))
	default:
		return fmt.Sprintf("%d B", n)
	}
}
