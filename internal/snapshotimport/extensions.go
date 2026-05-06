// Extension extraction for SnapshotExtensionMeta + SnapshotExtensionPayload
// items in the snapshot stream.
//
// Cosmos-sdk modules that hold blob state (e.g. cosmwasm contract
// bytecode, IBC 08-light-client client wasm) ship that state outside
// the IAVL tree as `SnapshotExtension` items: a meta item announcing
// the extension name + format, followed by N payload items. Each
// payload is gzipped wasm bytecode for cosmwasm; the bootstrap step
// downstream reads these files, gunzips them, sha256s the inflated
// bytes, and places them at the path wasmvm expects.
//
// Output layout matches what cosmos-bootstrap-gaia consumes:
//
//	<extDir>/<extName>/payload-<index>-format<F>.bin

package snapshotimport

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// extensionWriter accumulates extension-payload writes for one or more
// extensions in a single snapshot stream. When extDir is empty, all
// methods are no-ops (extension extraction disabled).
type extensionWriter struct {
	extDir string

	curName   string
	curFormat uint32
	curIndex  uint32
}

func newExtensionWriter(extDir string) *extensionWriter {
	return &extensionWriter{extDir: extDir}
}

// openMeta starts a new extension. Resets the per-extension payload
// counter and ensures the destination directory exists.
func (e *extensionWriter) openMeta(name string, format uint32, stats *Stats, log io.Writer) error {
	stats.Extensions++
	e.curName = name
	e.curFormat = format
	e.curIndex = 0
	fmt.Fprintf(log, "[import] open extension=%q format=%d\n", name, format)
	if e.extDir == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Join(e.extDir, name), 0o755); err != nil {
		return fmt.Errorf("mkdir extension %q: %w", name, err)
	}
	return nil
}

// writePayload writes one SnapshotExtensionPayload to the per-extension
// directory. Caller is responsible for ordering (post-order against
// openMeta calls); the importer guarantees this since the wire stream
// is sequential.
func (e *extensionWriter) writePayload(payload []byte, stats *Stats) error {
	stats.ExtensionPayloads++
	if e.extDir == "" || e.curName == "" {
		e.curIndex++
		return nil
	}
	path := filepath.Join(e.extDir, e.curName,
		fmt.Sprintf("payload-%d-format%d.bin", e.curIndex, e.curFormat))
	if err := os.WriteFile(path, payload, 0o644); err != nil {
		return fmt.Errorf("write extension payload %s: %w", path, err)
	}
	e.curIndex++
	return nil
}
