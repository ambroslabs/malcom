// Default templates emitted by `malcom init`.

package config

import (
	"fmt"
	"strings"

	"github.com/zrbecker/cosmos-p2p/internal/registry"
)

// GlobalTemplate returns the body of the top-level config.toml: a
// default_chain pointer plus the [fetch] / [import] / [bootstrap]
// sections that are shared across chains.
func GlobalTemplate(defaultChain string) string {
	return fmt.Sprintf(`# Shared defaults. Per-chain overrides live in chains/<chain>.toml.

default_chain = %q

[fetch]
# Per-chunk download tuning. The walking algorithm uses
# per_height_timeout for snapshot selection; these knobs apply
# during the actual chunk download.
per_peer       = 2          # max in-flight chunks per peer
chunk_timeout  = "45s"      # per-chunk request timeout
max_fetch      = "30m"      # hard cap on full download
peer_fails     = 3          # missing/hash-mismatch strikes before banning
peer_redials   = 3
redial_backoff = "5s"

listen      = "tcp://0.0.0.0:0"
moniker     = "malcom-snapfetch"
extra_seeds = []

# Freshness floor: drop snapshots older than currentChainHeight -
# max_age_blocks. cli looks up current height via the chain's RPC
# at start of each fetch. 3000 ≈ 5h on cosmoshub-4 at ~6s blocks.
# 0 disables the filter.
max_age_blocks = 3000

# Block stride between snapshots — cosmoshub mints every 1000.
snapshot_interval = 1000

# Per-height probe budget. We try each target height for this long
# (waiting for any peer to serve chunk-0) before walking back to
# the next-lower height.
per_height_timeout = "10s"

# Peer-count caps. cometbft default outbound=10; we run higher to
# absorb PEX-harvested addrbooks dominated by non-snapshot peers,
# then churn out the unhelpful ones (see churn_grace below).
# pex_target_peers must be < max_outbound_peers (the gap is
# headroom for churn cycles).
max_outbound_peers = 64
pex_target_peers   = 48
pex_max_per_wave   = 8

# After a peer connects we send SnapshotsRequest immediately. Any
# peer that hasn't advertised a snapshot in [min_height, max_height]
# within churn_grace is gracefully disconnected so PEX can dial
# someone more useful from the addrbook.
churn_grace = "3s"

[import]
# Pebble bulk-load tuning. Defaults sized for an 8 GiB host with 2-4
# vCPUs; bump memtable_mb / cache_mb on bigger boxes.
memtable_mb                = 256   # x2 in-flight = ~512 MiB resident
cache_mb                   = 64
max_concurrent_compactions = 2
min_free_gb                = 20    # cosmoshub-4 import is ~14 GB after cleanup

[bootstrap]
trust_period   = "720h"   # 30d cometbft light-client trust window
app_db_backend = "pebbledb"
cmt_db_backend = "goleveldb"
place_wasm     = true     # extract wasm payloads to the gaia home's expected paths
write_configs  = true     # emit minimal app.toml/config.toml/client.toml
moniker        = "bootstrap-node"
`, defaultChain)
}

// ChainTemplate returns the body of chains/<chainID>.toml.
//
// If info is nil, the template is blank (rpcs empty, genesis empty,
// no overrides) — used in -offline mode or when chain-registry has no
// entry for the chain.
//
// If info is populated, rpcs and the [fetch].extra_seeds list are
// filled from chain-registry. genesisPath, when non-empty, is written
// as the local path to the downloaded genesis file.
func ChainTemplate(chainID string, info *registry.ChainInfo, genesisPath string) string {
	var b strings.Builder

	fmt.Fprintf(&b, "chain_id = %q\n\n", chainID)

	// Genesis.
	fmt.Fprintln(&b, "# URL or local path to genesis.json. URLs are downloaded lazily")
	fmt.Fprintln(&b, "# on `malcom bootstrap` and cached at $XDG_DATA_HOME/malcom/<chain>/.")
	fmt.Fprintln(&b, "# Relative paths resolve against this file's directory.")
	fmt.Fprintf(&b, "genesis = %q\n\n", genesisPath)

	// RPCs.
	if info != nil && len(info.RPCs) > 0 {
		fmt.Fprintln(&b, "# RPC endpoints from cosmos chain-registry. Curate as needed; entries rot.")
		fmt.Fprintln(&b, "rpcs = [")
		for _, r := range info.RPCs {
			fmt.Fprintf(&b, "  %q,\n", r)
		}
		fmt.Fprintln(&b, "]")
	} else {
		fmt.Fprintln(&b, "# Required for `malcom bootstrap` and `malcom verify`. Bootstrap needs >=2.")
		fmt.Fprintln(&b, "rpcs = [")
		fmt.Fprintln(&b, "  # \"https://...\",")
		fmt.Fprintln(&b, "]")
	}
	fmt.Fprintln(&b)

	// Path overrides.
	fmt.Fprintln(&b, "# Optional path overrides. Blank = XDG-derived:")
	fmt.Fprintln(&b, "#   node_key  → $XDG_STATE_HOME/malcom/<chain>/node_key.json")
	fmt.Fprintln(&b, "#   peer_db   → $XDG_CACHE_HOME/malcom/<chain>/peers.json")
	fmt.Fprintln(&b, "#   addrbook  → $XDG_CACHE_HOME/malcom/<chain>/addrbook.json")
	fmt.Fprintln(&b, "# node_key = \"\"")
	fmt.Fprintln(&b, "# peer_db  = \"\"")
	fmt.Fprintln(&b, "# addrbook = \"\"")
	fmt.Fprintln(&b)

	// extra_seeds populated from chain-registry's seeds + persistent_peers.
	allPeers := append([]string{}, peersUniqueSorted(info)...)
	if len(allPeers) > 0 {
		fmt.Fprintln(&b, "# Seeds + persistent_peers from cosmos chain-registry, layered into")
		fmt.Fprintln(&b, "# the snapfetch dial set. Peer entries also rot — prune as needed.")
		fmt.Fprintln(&b, "[fetch]")
		fmt.Fprintln(&b, "extra_seeds = [")
		for _, p := range allPeers {
			fmt.Fprintf(&b, "  %q,\n", p)
		}
		fmt.Fprintln(&b, "]")
	} else {
		fmt.Fprintln(&b, "# Optional: override individual fields from the top-level config.toml.")
		fmt.Fprintln(&b, "# [fetch]")
		fmt.Fprintln(&b, "# prefer_fresh = true")
		fmt.Fprintln(&b, "# [import]")
		fmt.Fprintln(&b, "# memtable_mb = 512")
	}

	return b.String()
}

// peersUniqueSorted merges info.Seeds + info.PersistentPeers and
// dedupes (some entries appear in both). Stable order: seeds first
// (chain-registry treats seeds as more reliable for first contact),
// then persistent_peers, in original order.
func peersUniqueSorted(info *registry.ChainInfo) []string {
	if info == nil {
		return nil
	}
	seen := make(map[string]struct{}, len(info.Seeds)+len(info.PersistentPeers))
	out := make([]string, 0, len(info.Seeds)+len(info.PersistentPeers))
	for _, p := range info.Seeds {
		if _, dup := seen[p]; dup {
			continue
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}
	for _, p := range info.PersistentPeers {
		if _, dup := seen[p]; dup {
			continue
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}
	return out
}
