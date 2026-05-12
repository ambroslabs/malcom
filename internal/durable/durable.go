// Package durable provides atomic-write and directory-fsync helpers
// that survive an OS crash between the write and the next disk flush.
//
// Every entry point follows the same pattern:
//
//  1. write into a temp file in the destination's parent directory
//  2. fsync the temp file's fd so its contents are on stable storage
//  3. close the fd
//  4. (chmod if a mode override is in play)
//  5. rename temp → final path (atomic-replace on POSIX)
//  6. fsync the parent directory so the rename itself is durable
//
// Skipping any of those steps risks a window where a crash silently
// loses or corrupts data — see #108 C5/C6 for the failure modes this
// replaces.
package durable

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// WriteFile writes data to path durably and applies mode to the
// final file. The destination's parent directory must already exist
// (callers MkdirAll it themselves when in doubt).
func WriteFile(path string, data []byte, mode os.FileMode) error {
	return WriteFileFrom(path, bytes.NewReader(data), mode)
}

// WriteFileFrom is the streaming variant for large or already-open
// sources (downloads, file-to-file copies). Same durability contract.
func WriteFileFrom(path string, src io.Reader, mode os.FileMode) error {
	dir := filepath.Dir(path)
	base := filepath.Base(path)
	// Pattern keeps the original base in the tmp name so an operator
	// inspecting a post-crash leftover can tell what it was.
	tmp, err := os.CreateTemp(dir, "."+base+".*.tmp")
	if err != nil {
		return fmt.Errorf("durable: create tmp in %s: %w", dir, err)
	}
	tmpPath := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpPath) }

	if _, err := io.Copy(tmp, src); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("durable: write %s: %w", tmpPath, err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("durable: sync %s: %w", tmpPath, err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return fmt.Errorf("durable: close %s: %w", tmpPath, err)
	}
	if err := os.Chmod(tmpPath, mode); err != nil {
		cleanup()
		return fmt.Errorf("durable: chmod %s: %w", tmpPath, err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		cleanup()
		return fmt.Errorf("durable: rename %s -> %s: %w", tmpPath, path, err)
	}
	if err := FsyncDir(dir); err != nil {
		return fmt.Errorf("durable: fsync dir %s: %w", dir, err)
	}
	return nil
}

// CopyFile copies src → dst durably. The destination's mode is taken
// from the source file's permission bits.
func CopyFile(src, dst string) error {
	sf, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("durable: open src %s: %w", src, err)
	}
	defer sf.Close()
	info, err := sf.Stat()
	if err != nil {
		return fmt.Errorf("durable: stat src %s: %w", src, err)
	}
	return WriteFileFrom(dst, sf, info.Mode().Perm())
}

// CloneTree mirrors srcDir → dstDir as an independent on-disk copy
// with per-file durability. Each file lands via CopyFile (Sync +
// Rename + dir fsync); each created directory is fsync'd in
// leaf-first order at the end so all renames within it are durable.
//
// Hardlinks are intentionally avoided: pebble's LOCK file aliases
// across hardlinks, which entangles the source dir's lifetime with
// the daemon's runtime — see internal/cli/bootstrap/helpers.go
// commentary on the original cloneTree for the full reasoning.
func CloneTree(srcDir, dstDir string) error {
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		return fmt.Errorf("durable: mkdir %s: %w", dstDir, err)
	}
	dirsCreated := []string{dstDir}
	walkErr := filepath.Walk(srcDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(srcDir, path)
		if err != nil {
			return err
		}
		dst := filepath.Join(dstDir, rel)
		if info.IsDir() {
			if rel == "." {
				return nil
			}
			if err := os.MkdirAll(dst, info.Mode().Perm()); err != nil {
				return err
			}
			dirsCreated = append(dirsCreated, dst)
			return nil
		}
		return CopyFile(path, dst)
	})
	if walkErr != nil {
		return fmt.Errorf("durable: clone %s -> %s: %w", srcDir, dstDir, walkErr)
	}
	// Leaf-first fsync ensures parent dirs see their children's
	// rename-into entries durably before we fsync the parent.
	for i := len(dirsCreated) - 1; i >= 0; i-- {
		if err := FsyncDir(dirsCreated[i]); err != nil {
			return fmt.Errorf("durable: fsync %s: %w", dirsCreated[i], err)
		}
	}
	return nil
}

// Rename renames src → dst and fsyncs dst's parent directory. Use
// this when the source is already durable on disk (e.g., moving a
// directory whose contents have already been fsync'd, like the
// imported application.db tree).
func Rename(src, dst string) error {
	if err := os.Rename(src, dst); err != nil {
		return fmt.Errorf("durable: rename %s -> %s: %w", src, dst, err)
	}
	return FsyncDir(filepath.Dir(dst))
}

// FsyncDir fsyncs a directory so any prior renames into it land
// durably. On platforms where directory fsync is unsupported the
// underlying open or sync may fail; callers treat that as best-
// effort and surface the error so it can be logged.
func FsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return err
	}
	return d.Close()
}
