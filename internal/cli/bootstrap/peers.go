// Forward fetch's peer state into the bootstrapped chain home.
//
//   - persistent_peers: top-N entries from the chain's served.json
//     (peers that successfully served us snapshot chunks), newest-first.
//     Format matches what cometbft's [p2p].persistent_peers expects:
//     comma-separated `id@host:port` strings.
//
//   - addrbook: copy the chain's addrbook.json from malcom's per-chain
//     state into <home>/config/addrbook.json so the daemon's pex starts
//     with a populated peer DB instead of from scratch.

package bootstrap

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/ambroslabs/malcom/internal/durable"
	"github.com/ambroslabs/malcom/internal/helpers/served"
)

// defaultPersistentPeerCount is how many served-peer entries we forward
// into config.toml's persistent_peers when --forward-peers=false isn't set.
// 20 is enough to give the daemon a healthy seed pool without bloating
// the line.
const defaultPersistentPeerCount = 20

// readServedPeers loads served.json at path and returns up to max
// `id@host:port` entries, newest-first. Missing file / empty set
// return (nil, nil) — callers treat that as "nothing to forward."
func readServedPeers(path string, max int) ([]string, error) {
	if path == "" {
		return nil, nil
	}
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("stat %s: %w", path, err)
	}
	set, err := served.New(path)
	if err != nil {
		return nil, fmt.Errorf("load served set: %w", err)
	}
	all := set.All()
	if max > 0 && len(all) > max {
		all = all[:max]
	}
	out := make([]string, 0, len(all))
	for _, e := range all {
		if e.Addr != "" {
			out = append(out, e.Addr)
		}
	}
	return out, nil
}

// joinPersistentPeers formats a slice of `id@host:port` strings into a
// single comma-separated value suitable for config.toml's
// [p2p].persistent_peers.
func joinPersistentPeers(peers []string) string {
	return strings.Join(peers, ",")
}

// mergePeers concatenates peer lists in order and dedupes by peer id
// (the part before `@`). Empty entries and entries without an `@`
// separator are skipped. The output preserves the relative order of
// the first list followed by any new entries from later lists, so
// callers control priority by argument order.
func mergePeers(lists ...[]string) []string {
	seen := make(map[string]struct{})
	var out []string
	for _, list := range lists {
		for _, p := range list {
			at := strings.IndexByte(p, '@')
			if p == "" || at <= 0 {
				continue
			}
			id := p[:at]
			if _, dup := seen[id]; dup {
				continue
			}
			seen[id] = struct{}{}
			out = append(out, p)
		}
	}
	return out
}

// copyAddrbook places src at <homeRoot>/config/addrbook.json. If src
// doesn't exist (operator never ran fetch, fresh chain), returns nil
// without writing.
func copyAddrbook(src, homeRoot string, log *slog.Logger) error {
	if src == "" {
		return nil
	}
	if _, err := os.Stat(src); err != nil {
		if os.IsNotExist(err) {
			log.Info("addrbook source missing — skipping copy", "src", src)
			return nil
		}
		return fmt.Errorf("stat addrbook %s: %w", src, err)
	}
	dst := filepath.Join(homeRoot, "config", "addrbook.json")
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", filepath.Dir(dst), err)
	}
	log.Info("addrbook copy", "src", src, "dst", dst)
	return durable.CopyFile(src, dst)
}
