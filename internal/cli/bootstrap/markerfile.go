package bootstrap

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/ambroslabs/malcom/internal/durable"
)

// BootstrapHeightMarker is the filename written under the chain home
// after `<binary> {tendermint,comet} bootstrap-state` succeeds. The
// height is the source-of-truth for `malcom bootstrap heal` recovery (see #95):
// cometbft consumes its OfflineStateSyncHeight signal on the first
// `<binary> start`, so a failed first start permanently bricks the
// home unless the height is re-applied via another `bootstrap-state`.
// Storing it in the home keeps the recovery hermetic — an operator
// who has only the chain home doesn't need to consult malcom's logs.
//
// The dot-prefix keeps it out of the way of casual `ls` and signals
// "internal metadata" to the operator. cometbft / the chain daemon
// don't read this file; it's purely malcom's tooling surface.
const BootstrapHeightMarker = ".malcom-bootstrap-height"

// ErrNoMarker means the home dir has no .malcom-bootstrap-height
// file. Distinguished from a corrupt marker so the heal subcommand
// can decide between "use the --height flag" and "the file you wrote
// is unreadable, fix it".
var ErrNoMarker = errors.New("no .malcom-bootstrap-height marker in home")

// WriteHeightMarker stores height under <home>/.malcom-bootstrap-height
// as a single decimal integer + newline. Atomic via tmp + rename so a
// crash mid-write can't leave a half-formed file. Idempotent — calling
// twice with the same height is a no-op rename of identical bytes.
func WriteHeightMarker(home string, height int64) error {
	path := filepath.Join(home, BootstrapHeightMarker)
	body := []byte(strconv.FormatInt(height, 10) + "\n")
	if err := durable.WriteFile(path, body, 0o644); err != nil {
		return fmt.Errorf("write marker: %w", err)
	}
	return nil
}

// ReadHeightMarker reads <home>/.malcom-bootstrap-height and returns
// the integer it contains. Returns ErrNoMarker (wrapped) when the
// file doesn't exist so callers can branch on "missing vs corrupt".
// Trailing whitespace / newlines are tolerated; everything else is a
// hard parse error so an operator notices that someone scribbled in
// the file.
func ReadHeightMarker(home string) (int64, error) {
	path := filepath.Join(home, BootstrapHeightMarker)
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, fmt.Errorf("%w: %s", ErrNoMarker, path)
		}
		return 0, fmt.Errorf("read marker: %w", err)
	}
	s := strings.TrimSpace(string(raw))
	if s == "" {
		return 0, fmt.Errorf("marker %s is empty", path)
	}
	h, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse marker %s: %w", path, err)
	}
	if h <= 0 {
		return 0, fmt.Errorf("marker %s holds non-positive height %d", path, h)
	}
	return h, nil
}
