// Package registry talks to the cosmos chain-registry
// (https://github.com/cosmos/chain-registry) — a community-curated
// store of per-chain JSON metadata: chain_id, RPC list, peer list,
// genesis URL.
//
// We use it to populate `chains/<id>.toml` defaults during
// `malcom init` so a fresh setup is ready to run without manual
// editing.
package registry

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

const baseURL = "https://raw.githubusercontent.com/cosmos/chain-registry/master"

// ChainInfo is the curated subset of chain.json we care about.
type ChainInfo struct {
	ChainID         string
	GenesisURL      string
	RPCs            []string // host:port URLs
	PersistentPeers []string // "id@host:port"
	Seeds           []string // "id@host:port"
}

// ErrNotFound is returned when chain-registry doesn't have an entry
// for the requested name (after suffix-stripping fallback).
var ErrNotFound = errors.New("chain not found in cosmos chain-registry")

// Fetch retrieves chain.json for the given chain. The chain-registry
// directory layout uses short names (cosmoshub) but our config keys
// chain_id (cosmoshub-4); we try both: first the verbatim name, then
// with the trailing `-N` suffix stripped.
func Fetch(name string) (*ChainInfo, error) {
	candidates := []string{name}
	if stripped := stripVersion(name); stripped != name {
		candidates = append(candidates, stripped)
	}

	var lastErr error
	for _, c := range candidates {
		info, err := fetchOne(c)
		if err == nil {
			return info, nil
		}
		if errors.Is(err, ErrNotFound) {
			lastErr = err
			continue
		}
		// non-404 (network etc.): bail with that error
		return nil, err
	}
	if lastErr == nil {
		lastErr = ErrNotFound
	}
	return nil, fmt.Errorf("%s: %w", name, lastErr)
}

// stripVersion drops a trailing "-N" suffix (e.g. cosmoshub-4 →
// cosmoshub). Returns name unchanged if no such suffix.
func stripVersion(name string) string {
	idx := strings.LastIndex(name, "-")
	if idx < 1 {
		return name
	}
	for _, r := range name[idx+1:] {
		if r < '0' || r > '9' {
			return name
		}
	}
	return name[:idx]
}

func fetchOne(registryName string) (*ChainInfo, error) {
	url := fmt.Sprintf("%s/%s/chain.json", baseURL, registryName)
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("GET %s: HTTP %d: %s", url, resp.StatusCode, body)
	}

	var raw chainJSON
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, fmt.Errorf("decode chain.json: %w", err)
	}

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
	return info, nil
}

// chainJSON is the on-the-wire shape of cosmos chain-registry's
// chain.json. We only model the fields we read.
type chainJSON struct {
	ChainID  string `json:"chain_id"`
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
