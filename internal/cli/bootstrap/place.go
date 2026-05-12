// application.db placement strategies. The output of `malcom snapshot
// import` is a complete pebble dir; bootstrap only needs to land it at
// `<chain-home>/data/application.db`. The strategy knob lets the
// operator pick between safety (copy) and speed (move).
//
// Default `copy` keeps the source pristine — useful when malcom's
// appdb is the canonical reference and you may run bootstrap multiple
// times. `move` is a single rename(2), zero-cost on the same
// filesystem; the source ceases to exist at its old path.
//
// In-place detection: if the user's appdb dir is *already* the
// destination's data dir (typical when the operator imported straight
// into the chain home), we no-op. Detected via dev+inode equality so
// path differences (relative vs. absolute, symlink vs. real) don't
// trigger an unnecessary copy.

package bootstrap

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"syscall"
)

// AppStrategy picks how application.db gets from the appdb dir to the
// chain home's data dir. Values are documented strings so the CLI flag
// can validate them up front.
type AppStrategy string

const (
	StrategyCopy AppStrategy = "copy"
	StrategyMove AppStrategy = "move"
)

// ParseAppStrategy validates a strategy name. Returns an error listing
// the supported values when the input is unrecognized.
func ParseAppStrategy(s string) (AppStrategy, error) {
	switch AppStrategy(s) {
	case StrategyCopy, StrategyMove:
		return AppStrategy(s), nil
	}
	return "", fmt.Errorf("unknown -app-strategy %q (want copy|move)", s)
}

// placeAppDB moves or copies srcAppDB → dstAppDB per strategy. It
// short-circuits when src and dst are the same on-disk directory.
//
// Move is implemented as os.Rename; cross-device renames return an
// error rather than transparently falling back to copy+delete (the
// user explicitly opted for "move"; surprising them with a long copy
// is worse than asking them to use --app-strategy=copy).
func placeAppDB(srcAppDB, dstAppDB string, strategy AppStrategy, log *slog.Logger) error {
	same, err := sameOnDisk(srcAppDB, dstAppDB)
	if err != nil {
		return fmt.Errorf("compare appdb paths: %w", err)
	}
	if same {
		log.Info("application.db already in place — skipping placement",
			"src", srcAppDB, "dst", dstAppDB)
		return nil
	}

	// Refuse to clobber an existing dst — the operator either wanted
	// in-place (handled above) or hasn't cleaned up. Surface explicitly.
	if _, err := os.Stat(dstAppDB); err == nil {
		return fmt.Errorf("destination %s already exists; remove it or pass -overwrite to wipe", dstAppDB)
	}

	if err := os.MkdirAll(filepath.Dir(dstAppDB), 0o755); err != nil {
		return fmt.Errorf("mkdir parent for %s: %w", dstAppDB, err)
	}

	switch strategy {
	case StrategyMove:
		log.Info("application.db move (rename)", "src", srcAppDB, "dst", dstAppDB)
		if err := os.Rename(srcAppDB, dstAppDB); err != nil {
			return fmt.Errorf("rename %s -> %s (cross-device renames are not supported; use -app-strategy=copy): %w",
				srcAppDB, dstAppDB, err)
		}
		return nil
	case StrategyCopy:
		log.Info("application.db copy", "src", srcAppDB, "dst", dstAppDB)
		return cloneTree(srcAppDB, dstAppDB)
	default:
		return fmt.Errorf("unsupported -app-strategy %q", strategy)
	}
}

// sameOnDisk reports whether two paths refer to the same directory on
// disk. Compares (Stat_t.Dev, Stat_t.Ino) when both exist; returns
// false (not an error) when either path is missing.
func sameOnDisk(a, b string) (bool, error) {
	ai, aErr := os.Stat(a)
	bi, bErr := os.Stat(b)
	if os.IsNotExist(aErr) || os.IsNotExist(bErr) {
		return false, nil
	}
	if aErr != nil {
		return false, aErr
	}
	if bErr != nil {
		return false, bErr
	}
	as, ok := ai.Sys().(*syscall.Stat_t)
	bs, ok2 := bi.Sys().(*syscall.Stat_t)
	if !ok || !ok2 {
		// Non-unix or unusual filesystem — fall back to absolute path
		// comparison rather than guessing.
		aa, _ := filepath.Abs(a)
		bb, _ := filepath.Abs(b)
		return aa == bb, nil
	}
	return as.Dev == bs.Dev && as.Ino == bs.Ino, nil
}
