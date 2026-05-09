// Package registry exposes a cached snapshot of cosmos chain-registry
// data: chain_id, genesis URL, RPCs, peers.
//
// Sync streams the chain-registry tarball into a cache directory
// (typically $XDG_CACHE_HOME/malcom/chain-registry/), keeping only the
// `*/chain.json` files for cosmos chains (mainnets + testnets +
// devnets, status != killed) and writing index.json mapping chain_id
// → relative directory. Lookup resolves a chain_id against the cached
// index and returns its parsed ChainInfo, sanity-checking that the
// cached chain.json's chain_id field matches the lookup key.
//
// `malcom registry refresh` forces a sync. `malcom add` auto-syncs
// when the cache is missing or older than DefaultIndexMaxAge.
package registry

import (
	"strconv"
	"time"
)

// formatMinGasPrice picks the first fee token from chain-registry's
// fees.fee_tokens and renders it as a cosmos-sdk minimum-gas-prices
// string ("<rate><denom>"). Prefers fixed_min_gas_price (the value the
// chain's protocol enforces); falls back to low_gas_price if missing.
// Returns "" when there's no usable token.
func formatMinGasPrice(tokens []feeToken) string {
	for _, t := range tokens {
		if t.Denom == "" {
			continue
		}
		rate := t.FixedMinGasPrice
		if rate == 0 {
			rate = t.LowGasPrice
		}
		return strconv.FormatFloat(rate, 'f', -1, 64) + t.Denom
	}
	return ""
}

// DefaultIndexMaxAge is how stale the cached index can be before
// callers should consider re-syncing.
const DefaultIndexMaxAge = 24 * time.Hour

// DefaultSyncTimeout bounds one Sync invocation end-to-end. Real syncs
// land in a few seconds; the cap exists so a hung TCP connection or a
// stalled download doesn't wedge `malcom add` indefinitely. Callers
// that want something different wrap their context.WithTimeout
// themselves; Sync itself never imposes a timeout.
const DefaultSyncTimeout = 5 * time.Minute

// ChainInfo is the curated subset of chain.json we expose to callers.
type ChainInfo struct {
	ChainID         string
	GenesisURL      string
	RPCs            []string // host:port URLs
	PersistentPeers []string // "id@host:port"
	Seeds           []string // "id@host:port"

	// DaemonName is the chain binary name (e.g. "gaiad", "osmosisd").
	// Bootstrap uses it for `exec.LookPath` when no explicit binary
	// path is given.
	DaemonName string

	// NodeHome is the chain's default home directory string as
	// declared in chain.json (e.g. "$HOME/.gaia"). Informational —
	// malcom always points the daemon at its own --home path.
	NodeHome string

	// RecommendedVersion is codebase.recommended_version (e.g. "v27.2.0").
	// Useful as a sanity check against the operator-supplied binary.
	RecommendedVersion string

	// CosmWasmVersion is codebase.cosmwasm.version (e.g. "v0.60.6").
	// Empty when the chain doesn't ship cosmwasm. Used to pick the
	// right wasm-extension layout (old vs. new wasmd path conventions).
	CosmWasmVersion string

	// MinGasPrice is the minimum-gas-prices string ready to drop into
	// app.toml: "<rate><denom>" (e.g. "0.005uatom"), derived from
	// fees.fee_tokens[0].fixed_min_gas_price (or low_gas_price as a
	// fallback). Empty when chain.json has no fee_tokens entry.
	MinGasPrice string
}

// chainJSON is the on-the-wire shape of cosmos chain-registry's
// chain.json. We model only the fields we read.
type chainJSON struct {
	ChainID    string `json:"chain_id"`
	Status     string `json:"status"`
	DaemonName string `json:"daemon_name"`
	NodeHome   string `json:"node_home"`
	Codebase   struct {
		RecommendedVersion string `json:"recommended_version"`
		Genesis            struct {
			GenesisURL string `json:"genesis_url"`
		} `json:"genesis"`
		CosmWasm struct {
			Version string `json:"version"`
		} `json:"cosmwasm"`
	} `json:"codebase"`
	APIs struct {
		RPC []endpoint `json:"rpc"`
	} `json:"apis"`
	Peers struct {
		Seeds           []peer `json:"seeds"`
		PersistentPeers []peer `json:"persistent_peers"`
	} `json:"peers"`
	Fees struct {
		FeeTokens []feeToken `json:"fee_tokens"`
	} `json:"fees"`
}

type feeToken struct {
	Denom            string  `json:"denom"`
	FixedMinGasPrice float64 `json:"fixed_min_gas_price"`
	LowGasPrice      float64 `json:"low_gas_price"`
}

type endpoint struct {
	Address  string `json:"address"`
	Provider string `json:"provider"`
}

type peer struct {
	ID       string `json:"id"`
	Address  string `json:"address"`
	Provider string `json:"provider"`
}

// chainInfoFromRaw extracts the curated ChainInfo subset from a parsed
// chain.json. Empty addresses and id-less peers are dropped.
func chainInfoFromRaw(raw *chainJSON) *ChainInfo {
	info := &ChainInfo{
		ChainID:            raw.ChainID,
		GenesisURL:         raw.Codebase.Genesis.GenesisURL,
		DaemonName:         raw.DaemonName,
		NodeHome:           raw.NodeHome,
		RecommendedVersion: raw.Codebase.RecommendedVersion,
		CosmWasmVersion:    raw.Codebase.CosmWasm.Version,
		MinGasPrice:        formatMinGasPrice(raw.Fees.FeeTokens),
	}
	for _, r := range raw.APIs.RPC {
		if r.Address != "" {
			info.RPCs = append(info.RPCs, r.Address)
		}
	}
	for _, p := range raw.Peers.PersistentPeers {
		if p.ID != "" && p.Address != "" {
			info.PersistentPeers = append(info.PersistentPeers, p.ID+"@"+p.Address)
		}
	}
	for _, p := range raw.Peers.Seeds {
		if p.ID != "" && p.Address != "" {
			info.Seeds = append(info.Seeds, p.ID+"@"+p.Address)
		}
	}
	return info
}
