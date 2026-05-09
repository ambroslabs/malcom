package snapshotimport

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

// fastIngester routes f/ (fast-storage) entries through pebble's
// bulk-ingest path: one *sstable.Writer per store accumulates the
// (already byte-sorted) entries, then db.Ingest atomically links the
// finished file into the LSM. Bypasses memtable + WAL + batch-sort
// + L0→L6 compaction work for the fast-storage half of the import.
//
// Only safe for keyspaces that arrive in strictly increasing byte
// order — postorder leaves yield sorted user-keys, which is why we
// can use this for f/ but not for s/ (whose nodeKeys carry varying
// versions in the high bytes of the key).
type fastIngester struct {
	db     *pebble.DB
	tmpDir string
	log    *slog.Logger

	// Per-store state. Reset by openStore / cleared by ingestStore.
	curWriter *sstable.Writer
	curPath   string
	curName   string
	curCount  uint64 // number of f/ entries written for the current store
}

// newFastIngester returns an ingester that writes its temp SSTables
// under tmpDir. Caller is responsible for ensuring the dir exists
// and removing it when Import returns.
func newFastIngester(db *pebble.DB, tmpDir string, log *slog.Logger) *fastIngester {
	return &fastIngester{db: db, tmpDir: tmpDir, log: log}
}

// openStore starts a new SSTable Writer for store. Must be called
// after the previous store has been ingested via ingestStore (or
// the previous writer was empty / abandoned).
func (i *fastIngester) openStore(name string) error {
	if i.curWriter != nil {
		return fmt.Errorf("openStore(%q): previous store %q still open", name, i.curName)
	}
	path := filepath.Join(i.tmpDir, fmt.Sprintf("fast-%s.sst", sanitize(name)))
	// pebble's sstable.Writer wants a vfs.File (which exposes
	// Preallocate, Sync, etc. that *os.File doesn't directly satisfy).
	// vfs.Default.Create wraps os.File with the missing methods.
	f, err := vfs.Default.Create(path)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	// Match the parent DB's compression policy. The parent DB is open
	// with NoCompression on L0; ingested files land at deeper levels
	// (the LSM picks a level via key-range overlap), so they'll be
	// recompressed during the final compact regardless. NoCompression
	// here saves the snappy CPU cost on the per-store SSTable build.
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f),
		sstable.WriterOptions{
			Compression: sstable.NoCompression,
			BlockSize:   32 << 10,
		})
	i.curWriter = w
	i.curPath = path
	i.curName = name
	i.curCount = 0
	return nil
}

// set writes a fast-storage entry. Keys must arrive in strictly
// increasing byte order across calls to set within a single store.
func (i *fastIngester) set(key, value []byte) error {
	if i.curWriter == nil {
		return fmt.Errorf("fastIngester.set: no writer open (current store=%q)", i.curName)
	}
	if err := i.curWriter.Set(key, value); err != nil {
		return fmt.Errorf("fastWriter.Set: %w", err)
	}
	i.curCount++
	return nil
}

// ingestStore finalizes the current store's SSTable and atomically
// links it into the LSM. Removes the temp file on success. The
// `name` argument is for symmetry / error messages; the writer
// itself remembers what's open.
//
// No-op if openStore was called but no entries were emitted (empty
// store) — finalising an empty Writer would produce a degenerate
// file that Ingest rejects.
func (i *fastIngester) ingestStore(name string) error {
	if i.curWriter == nil {
		return nil
	}
	if i.curName != name {
		return fmt.Errorf("ingestStore(%q): current store is %q", name, i.curName)
	}
	w := i.curWriter
	path := i.curPath
	count := i.curCount
	i.curWriter = nil
	i.curPath = ""
	i.curName = ""
	i.curCount = 0

	if count == 0 {
		// Discard the empty file; Writer.Close on an empty writer
		// returns an error from sstable, so just remove without
		// closing.
		_ = os.Remove(path)
		return nil
	}
	if err := w.Close(); err != nil {
		_ = os.Remove(path)
		return fmt.Errorf("close fast SSTable: %w", err)
	}
	if err := i.db.Ingest([]string{path}); err != nil {
		_ = os.Remove(path)
		return fmt.Errorf("db.Ingest: %w", err)
	}
	// Ingest moves the file into pebble's data dir; if it returned
	// success there's nothing left to remove. Best-effort cleanup
	// in case pebble copied instead.
	_ = os.Remove(path)
	if i.log != nil {
		i.log.Debug("fast SSTable ingested", "store", name, "entries", count)
	}
	return nil
}

// cleanup releases any in-flight writer (e.g. on early error) and
// removes leftover temp files. Idempotent.
func (i *fastIngester) cleanup() {
	if i.curWriter != nil {
		_ = i.curWriter.Close()
		_ = os.Remove(i.curPath)
		i.curWriter = nil
	}
	// The caller's defer removes tmpDir wholesale; nothing else to do.
}

// sanitize replaces filesystem-unfriendly chars in a store name with
// '_'. Cosmos store names are short identifiers (bank, ibc,
// 08-light-client, …), so this is conservative.
func sanitize(s string) string {
	out := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z',
			c >= 'A' && c <= 'Z',
			c >= '0' && c <= '9',
			c == '-', c == '_', c == '.':
			out[i] = c
		default:
			out[i] = '_'
		}
	}
	return string(out)
}
