package snapshotdiff

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// CheckFreeSpace verifies that the filesystem containing path has at least
// minBytes free. Returns a descriptive error if not.
func CheckFreeSpace(path string, minBytes uint64) error {
	var stat syscall.Statfs_t
	// statfs needs the path to exist; walk up if it doesn't.
	probe := path
	for {
		if _, err := os.Stat(probe); err == nil {
			break
		}
		next := filepath.Dir(probe)
		if next == probe {
			return fmt.Errorf("none of the parents of %s exist", path)
		}
		probe = next
	}
	if err := syscall.Statfs(probe, &stat); err != nil {
		return fmt.Errorf("statfs %s: %w", probe, err)
	}
	avail := stat.Bavail * uint64(stat.Bsize)
	if avail < minBytes {
		return fmt.Errorf("insufficient free space at %s: have %s, need %s",
			probe, HumanBytes(avail), HumanBytes(minBytes))
	}
	return nil
}

// MakeTmpDir creates a tmp dir under root with a timestamp-based unique
// name. Returns the dir path and a cleanup function. cleanup is a no-op
// if keep is true.
func MakeTmpDir(root string, keep bool) (string, func(), error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return "", nil, err
	}
	name := fmt.Sprintf("run-%d", time.Now().UnixNano())
	path := filepath.Join(root, name)
	if err := os.MkdirAll(path, 0o755); err != nil {
		return "", nil, err
	}
	cleanup := func() {
		if keep {
			return
		}
		_ = os.RemoveAll(path)
	}
	return path, cleanup, nil
}
