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

import "time"

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
}

// chainJSON is the on-the-wire shape of cosmos chain-registry's
// chain.json. We model only the fields we read.
type chainJSON struct {
	ChainID  string `json:"chain_id"`
	Status   string `json:"status"`
	Codebase struct {
		Genesis struct {
			GenesisURL string `json:"genesis_url"`
		} `json:"genesis"`
	} `json:"codebase"`
	APIs struct {
		RPC []endpoint `json:"rpc"`
	} `json:"apis"`
	Peers struct {
		Seeds           []peer `json:"seeds"`
		PersistentPeers []peer `json:"persistent_peers"`
	} `json:"peers"`
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
		ChainID:    raw.ChainID,
		GenesisURL: raw.Codebase.Genesis.GenesisURL,
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
