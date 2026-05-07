// Package nodekey wraps cometbft's p2p.LoadOrGenNodeKey to fix two
// rough edges we can't fix upstream without forking:
//
//  1. The cometbft function calls SaveAs unconditionally on the
//     generation path, but SaveAs is just os.WriteFile — no MkdirAll.
//     If the parent dir doesn't exist (common on fresh installs), the
//     error is a cryptic "open <path>: no such file". We MkdirAll
//     before calling, but only on the generation path — loading an
//     existing key doesn't need it.
//
//  2. An empty path fails with the same cryptic OS error from inside
//     SaveAs. We reject it up front with a clearer message so
//     misconfiguration is obvious.
//
// If cometbft ever pushes the MkdirAll inside SaveAs (or accepts an
// upstream PR doing so), this package can be deleted and callers can
// go back to p2p.LoadOrGenNodeKey directly.
package nodekey

import (
	"fmt"
	"os"
	"path/filepath"

	cmtos "github.com/cometbft/cometbft/libs/os"
	"github.com/cometbft/cometbft/p2p"
)

// LoadOrGen loads the node key at path, generating and saving a fresh
// one if the file doesn't exist. Returns an explicit error on empty
// path. Creates the parent directory (mode 0700) only on the
// generation path.
func LoadOrGen(path string) (*p2p.NodeKey, error) {
	if path == "" {
		return nil, fmt.Errorf("node key path is empty")
	}
	if !cmtos.FileExists(path) {
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			return nil, fmt.Errorf("mkdir node-key dir: %w", err)
		}
	}
	return p2p.LoadOrGenNodeKey(path)
}
