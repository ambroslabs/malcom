package snapshotimport

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/ambroslabs/malcom/internal/durable"
)

// AppDBMetaFilename is the filename of the metadata JSON written into
// an imported appdb directory (alongside application.db/ and
// extensions/). Downstream tools (verify, bootstrap, …) read this so
// the user doesn't have to repeat -chain / -height on every command.
const AppDBMetaFilename = "meta.json"

// AppDBMeta is the on-disk shape of <appdb>/meta.json.
type AppDBMeta struct {
	ChainID               string    `json:"chain_id"`
	Height                int64     `json:"height"`
	ImportedAt            time.Time `json:"imported_at"`
	SourceSnapshotHashHex string    `json:"source_snapshot_hash_hex,omitempty"`

	// DBBackend names the on-disk format of application.db. Bootstrap
	// reads this so it can patch the chain's app.toml app-db-backend
	// to match. Today only "pebbledb" is produced; future backends
	// (goleveldb, rocksdb) would land here.
	DBBackend string `json:"db_backend,omitempty"`
}

// DBBackendPebble is the value DBBackend takes when application.db is
// a pebble directory — the only backend the importer produces today.
const DBBackendPebble = "pebbledb"

// WriteAppDBMeta writes meta to <dir>/meta.json, pretty-printed.
func WriteAppDBMeta(dir string, meta AppDBMeta) error {
	path := filepath.Join(dir, AppDBMetaFilename)
	buf, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal appdb meta: %w", err)
	}
	buf = append(buf, '\n')
	if err := durable.WriteFile(path, buf, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}

// ReadAppDBMeta reads <dir>/meta.json. Returns an os.IsNotExist error
// if the file is missing — callers can treat that as "no meta, fall
// back to flags".
func ReadAppDBMeta(dir string) (AppDBMeta, error) {
	path := filepath.Join(dir, AppDBMetaFilename)
	var m AppDBMeta
	f, err := os.Open(path)
	if err != nil {
		return m, err
	}
	defer f.Close()
	if err := json.NewDecoder(f).Decode(&m); err != nil {
		return m, fmt.Errorf("decode %s: %w", path, err)
	}
	return m, nil
}
