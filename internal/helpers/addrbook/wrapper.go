package addrbook

import (
	"fmt"
	"os"
	"path/filepath"

	cmtlog "github.com/cometbft/cometbft/libs/log"
	pexcb "github.com/cometbft/cometbft/p2p/pex"
)

// NewAddrBook constructs and configures the runtime cometbft addrbook
// pointed at path. Wraps three pieces of boilerplate every caller had
// to repeat: empty-path validation, parent-dir MkdirAll (cometbft's
// saveToFile is os.WriteFile with no MkdirAll, so a missing parent dir
// fails the periodic save with a cryptic "open <path>: no such file"),
// and logger wiring.
//
// Hardcodes routabilityStrict=false because no current caller needs
// strict routability filtering. Parameterize if that changes.
//
// If cometbft ever folds MkdirAll into addrbook bringup (or accepts an
// upstream PR doing so), this wrapper can be deleted.
func NewAddrBook(path string, log cmtlog.Logger) (pexcb.AddrBook, error) {
	if path == "" {
		return nil, fmt.Errorf("addrbook path is empty")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("mkdir addrbook dir: %w", err)
	}
	book := pexcb.NewAddrBook(path, false)
	book.SetLogger(log)
	return book, nil
}
