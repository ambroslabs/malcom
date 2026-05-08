// Package config loads malcom's TOML config and resolves a chain's
// effective settings.
//
// Layout (under $XDG_CONFIG_HOME/malcom/):
//
//	config.toml         shared defaults: default_chain, [fetch], [import], [bootstrap]
//	chains/<id>.toml    per-chain identity (chain_id, genesis, rpcs) plus optional
//	                    [fetch] / [import] / [bootstrap] sections that override defaults
//
// Resolve(id) returns a Chain with all sections layered in priority
// chain-file > config.toml > built-in defaults.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
)

// Config is the on-disk shape of config.toml: a default_chain pointer
// plus the global default tuning sections.
type Config struct {
	DefaultChain string `toml:"default_chain"`

	// Default tuning sections. Per-chain files can override individual
	// fields by including their own [fetch] / [import] / [bootstrap]
	// blocks.
	Fetch     FetchTuning     `toml:"fetch"`
	Import    ImportTuning    `toml:"import"`
	Bootstrap BootstrapTuning `toml:"bootstrap"`

	path      string // resolved config.toml path
	chainsDir string // <dir of config.toml>/chains
}

// Chain is one chain's resolved configuration. Matches the on-disk
// shape of chains/<id>.toml.
type Chain struct {
	ChainID string   `toml:"chain_id"`
	Genesis string   `toml:"genesis"`
	RPCs    []string `toml:"rpcs"`

	// Small persistent state. Blank = use XDG-derived default.
	NodeKey  string `toml:"node_key"`
	AddrBook string `toml:"addrbook"`
	Banlist  string `toml:"banlist"`

	Fetch     FetchTuning     `toml:"fetch"`
	Import    ImportTuning    `toml:"import"`
	Bootstrap BootstrapTuning `toml:"bootstrap"`
}

// FetchTuning maps onto snapfetch.Config's tuning fields.
type FetchTuning struct {
	Listen         string   `toml:"listen"`
	Moniker        string   `toml:"moniker"`
	BootstrapPeers []string `toml:"bootstrap_peers"`

	// MaxAgeBlocks is the freshness floor: any snapshot older than
	// currentChainHeight - MaxAgeBlocks is dropped from candidates.
	// Default 3000 ≈ 5h on cosmoshub-4 at 6s blocks.
	MaxAgeBlocks uint64 `toml:"max_age_blocks"`

	// SnapshotInterval is the stride between snapshot heights on the
	// chain. cosmoshub-4 mints every 1000 blocks; tune per chain.
	// The walking algorithm uses this to compute target =
	// floor(maxHeight / interval) * interval and decrement by
	// interval on each per-height probe failure.
	SnapshotInterval uint64 `toml:"snapshot_interval"`

	// PerHeightTimeout caps the time spent probing chunk-0 from
	// peers offering a given target height before walking back to
	// the next-lower height.
	PerHeightTimeout duration `toml:"per_height_timeout"`

	// MaxOutboundPeers is the hard ceiling on outbound peer
	// connections. cometbft default is 10; we run higher to absorb
	// addrbooks dominated by non-snapshot peers, then churn.
	MaxOutboundPeers int `toml:"max_outbound_peers"`

	// AllowDuplicateIP lets multiple peers share one IP. Default
	// false rejects the eclipse vector where one attacker IP fills
	// many outbound slots. Enable only when the target peer set is
	// dominated by shared egress (NAT/CGN, cloud regions) and
	// `malcom verify` is part of the operator's pipeline.
	AllowDuplicateIP bool `toml:"allow_duplicate_ip"`

	// PEXTargetPeers is the connected-outbound count our PEX dial
	// loop aims for. Must be ≤ MaxOutboundPeers; the gap is
	// headroom for churn.
	PEXTargetPeers int `toml:"pex_target_peers"`

	// PEXMaxPerWave caps parallel dials per PEX dial-loop tick.
	// Higher values find peers faster but generate more outbound
	// traffic on the network at startup.
	PEXMaxPerWave int `toml:"pex_max_per_wave"`

	// ChurnGrace is how long a freshly-connected peer has to
	// advertise a snapshot in [MinHeight, MaxHeight] before
	// being dropped as useless. Too short → drop peers before
	// they respond; too long → dead slots stick around.
	ChurnGrace duration `toml:"churn_grace"`

	// RequireStateSyncChannel bans-on-AddPeer any peer whose NodeInfo
	// doesn't advertise the state-sync snapshot channel (0x60). Turns
	// the slow churn-grace eviction of non-state-sync peers into a
	// zero-grace bench so connection slots fill faster with useful
	// candidates.
	RequireStateSyncChannel bool `toml:"require_state_sync_channel"`

	// AddrBookBanDuration is how long a misbehaving peer is barred
	// from PEX rotation via book.MarkBad.
	AddrBookBanDuration duration `toml:"addrbook_ban_duration"`

	// ProvisionalProbeStrikes is the misbehavior strike budget for a
	// peer that arrived via PEX (not on our seed list) before its
	// first verified chunk promotes it to "proven".
	ProvisionalProbeStrikes int `toml:"provisional_probe_strikes"`

	// ProvisionalProbeInflight is the in-flight chunk slot budget for
	// a still-provisional peer.
	ProvisionalProbeInflight int `toml:"provisional_probe_inflight"`

	// MaxDialFailures caps consecutive PEX dial failures against an
	// addrbook entry before it's deleted from the addrbook entirely
	// (vs the current MarkBad which cycles back in after a TTL).
	// PEX gossip will re-add the address if the peer comes back online.
	MaxDialFailures int `toml:"max_dial_failures"`

	// MaxDiskWriteFailures aborts the fetch with ErrDiskFailed once
	// chunk-write errors (ENOSPC, EIO, EROFS) hit this count. A
	// failed write leaves the chunk pending so the dispatcher
	// retries it; the cap distinguishes a one-off filesystem hiccup
	// (recovers) from a sustained disk problem (gets surfaced now,
	// not as a confusing zlib error at import).
	MaxDiskWriteFailures int `toml:"max_disk_write_failures"`

	Discover      duration `toml:"discover"`
	DialParallel  int      `toml:"dial_parallel"`
	MaxCandidates int      `toml:"max_candidates"`
	ProbeTimeout  duration `toml:"probe_timeout"`
	MinPeers      int      `toml:"min_peers"`

	PerPeer       int      `toml:"per_peer"`
	ChunkTimeout  duration `toml:"chunk_timeout"`
	MaxFetch      duration `toml:"max_fetch"`
	PeerFails     int      `toml:"peer_fails"`
	// PeerRedials caps the number of consecutive disconnect/redial
	// cycles before a peer is permanently banned for the run. 0 =
	// unlimited (legacy behaviour).
	PeerRedials   int      `toml:"peer_redials"`
	RedialBackoff duration `toml:"redial_backoff"`

	MaxRescans     int      `toml:"max_rescans"`
	RescanDiscover duration `toml:"rescan_discover"`
}

// ImportTuning maps onto snapshotimport.Options' tuning fields.
type ImportTuning struct {
	MemtableMB               int `toml:"memtable_mb"`
	CacheMB                  int `toml:"cache_mb"`
	MaxConcurrentCompactions int `toml:"max_concurrent_compactions"`
	MinFreeGB                int `toml:"min_free_gb"`
}

// BootstrapTuning collects bootstrap's tuning knobs.
type BootstrapTuning struct {
	TrustPeriod  duration `toml:"trust_period"`
	AppDBBackend string   `toml:"app_db_backend"`
	CmtDBBackend string   `toml:"cmt_db_backend"`
	PlaceWasm    bool     `toml:"place_wasm"`
	WriteConfigs bool     `toml:"write_configs"`
	Moniker      string   `toml:"moniker"`
}

// duration decodes from a TOML string like "30s" or "1h". TOML has no
// native duration type; BurntSushi calls UnmarshalText for types that
// implement it.
type duration time.Duration

func (d *duration) UnmarshalText(b []byte) error {
	s := string(b)
	if s == "" {
		*d = 0
		return nil
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("parse duration %q: %w", s, err)
	}
	*d = duration(v)
	return nil
}

// Duration returns the underlying time.Duration.
func (d duration) Duration() time.Duration { return time.Duration(d) }

// Load reads $XDG_CONFIG_HOME/malcom/config.toml. The chains/ subdir
// is resolved relative to the config file. Users isolate trees by
// setting $XDG_CONFIG_HOME (or by unsetting all XDG_*_HOME and
// pointing $HOME at a fresh directory).
//
// If config.toml is missing, the returned error tells the user to run
// `malcom init`.
func Load() (*Config, error) {
	path, err := DefaultConfigPath()
	if err != nil {
		return nil, err
	}
	var c Config
	md, err := toml.DecodeFile(path, &c)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("no config at %s — run `malcom init` to create one", path)
		}
		return nil, err
	}
	if undecoded := md.Undecoded(); len(undecoded) > 0 {
		return nil, fmt.Errorf("unknown keys in %s: %v", path, undecoded)
	}
	c.path = path
	c.chainsDir = filepath.Join(filepath.Dir(path), "chains")
	return &c, nil
}

// Resolve returns a fully-populated Chain by layering:
//  1. The global tuning sections from config.toml (lowest priority).
//  2. chains/<name>.toml on top (overrides individual fields).
//  3. Built-in defaults for any field still zero-valued.
//  4. XDG-derived defaults for blank node_key / addrbook.
func (c *Config) Resolve(name string) (Chain, error) {
	if name == "" {
		name = c.DefaultChain
	}
	if name == "" {
		return Chain{}, fmt.Errorf("no chain specified and default_chain is unset in %s", c.path)
	}

	// Pre-fill the chain struct with the global tuning defaults so
	// the per-chain decode only overwrites fields it explicitly sets.
	ch := Chain{
		Fetch:     c.Fetch,
		Import:    c.Import,
		Bootstrap: c.Bootstrap,
	}

	chainPath := filepath.Join(c.chainsDir, name+".toml")
	md, err := toml.DecodeFile(chainPath, &ch)
	if err != nil {
		return Chain{}, fmt.Errorf("read chain config %s: %w", chainPath, err)
	}
	if undecoded := md.Undecoded(); len(undecoded) > 0 {
		return Chain{}, fmt.Errorf("unknown keys in %s: %v", chainPath, undecoded)
	}

	if ch.ChainID == "" {
		ch.ChainID = name
	}

	if err := fillXDGDefaults(&ch); err != nil {
		return Chain{}, err
	}

	applyFetchDefaults(&ch.Fetch)
	applyImportDefaults(&ch.Import)
	applyBootstrapDefaults(&ch.Bootstrap)

	// Relative genesis is resolved against the chain file's directory.
	// URLs (http://, https://) are passed through unchanged — bootstrap
	// downloads them lazily.
	if ch.Genesis != "" && !isGenesisURL(ch.Genesis) && !filepath.IsAbs(ch.Genesis) {
		ch.Genesis = filepath.Join(filepath.Dir(chainPath), ch.Genesis)
	}

	return ch, nil
}

// Path returns the resolved config.toml path.
func (c *Config) Path() string { return c.path }

// ChainsDir returns the resolved chains/ directory path.
func (c *Config) ChainsDir() string { return c.chainsDir }

func fillXDGDefaults(ch *Chain) error {
	if ch.NodeKey == "" {
		d, err := StateDir()
		if err != nil {
			return err
		}
		ch.NodeKey = filepath.Join(d, ch.ChainID, "node_key.json")
	}
	if ch.AddrBook == "" {
		d, err := StateDir()
		if err != nil {
			return err
		}
		ch.AddrBook = filepath.Join(d, ch.ChainID, "addrbook.json")
	}
	if ch.Banlist == "" {
		d, err := StateDir()
		if err != nil {
			return err
		}
		ch.Banlist = filepath.Join(d, ch.ChainID, "banlist.json")
	}
	return nil
}

// DefaultChain returns a Chain with all tuning fields populated to
// built-in defaults — same values `malcom init` would write to a
// fresh chains/<id>.toml. ChainID is empty; callers fill it in.
//
// Used by subcommands as a fallback when the config file doesn't
// exist yet, so `-h` still shows real default values.
func DefaultChain() Chain {
	var ch Chain
	applyFetchDefaults(&ch.Fetch)
	applyImportDefaults(&ch.Import)
	applyBootstrapDefaults(&ch.Bootstrap)
	return ch
}

// DefaultChainID is the chain `malcom init` configures by default
// and what subcommands assume when no config is loaded.
const DefaultChainID = "cosmoshub-4"

// IsGenesisURL reports whether s is an http(s) URL (vs a local path).
// Public so subcommands can branch the same way Resolve does.
func IsGenesisURL(s string) bool { return isGenesisURL(s) }

func isGenesisURL(s string) bool {
	return strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://")
}

