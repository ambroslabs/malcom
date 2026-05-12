// Package config loads malcom's TOML config and resolves a chain's
// effective settings.
//
// Layout (under $XDG_CONFIG_HOME/malcom/):
//
//	config.toml         shared defaults: [fetch], [import], [bootstrap]
//	chains/<id>.toml    per-chain identity (chain_id, genesis, rpcs) plus optional
//	                    [fetch] / [import] / [bootstrap] sections that override defaults
//
// Resolve(id) returns a Chain with all sections layered in priority
// chain-file > config.toml > built-in defaults. Chains are added to a
// fresh tree with `malcom add <chain-id>`.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
)

// Config is the on-disk shape of config.toml: the global default
// tuning sections shared across chains.
type Config struct {
	// Default tuning sections. Per-chain files can override individual
	// fields by including their own [fetch] / [import] / [bootstrap] /
	// [log] blocks.
	Fetch     FetchTuning     `toml:"fetch"`
	Import    ImportTuning    `toml:"import"`
	Compact   CompactTuning   `toml:"compact"`
	Bootstrap BootstrapTuning `toml:"bootstrap"`
	Log       LogTuning       `toml:"log"`

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
	Served   string `toml:"served"`

	Fetch     FetchTuning     `toml:"fetch"`
	Import    ImportTuning    `toml:"import"`
	Compact   CompactTuning   `toml:"compact"`
	Bootstrap BootstrapTuning `toml:"bootstrap"`
	Log       LogTuning       `toml:"log"`
}

// CompactTuning are pebble compaction knobs. They apply to:
//
//   - the standalone `malcom compact -dir <appdb>` command, and
//   - `malcom snapshot import` when CompactDuringImport is enabled
//     (off by default — see ImportTuning.CompactDuringImport).
//
// Default import behaviour is to write the snapshot in bulk-load mode
// with auto-compactions disabled, then close the DB. The user runs
// `malcom compact` separately, or lets the daemon's pebble auto-compact
// at runtime. So these knobs typically only matter for the standalone
// compact job.
type CompactTuning struct {
	// MaxConcurrentCompactions caps the number of pebble compaction
	// goroutines. Default = runtime.NumCPU(). Compaction is largely
	// I/O-bound on cosmos-scale chains; bumping past NumCPU rarely
	// helps.
	MaxConcurrentCompactions int `toml:"max_concurrent_compactions"`

	// TargetFileSizeMB sets pebble's per-level TargetFileSize in MiB.
	// 0 = pebble defaults (~2 MiB at L0 doubling per level, ~64 MiB at
	// L6 effective). Bumping to 1024+ MiB makes the compact merge its
	// inputs into a small number of large files instead of re-fragmenting
	// them — the trade-off is wall time: bigger output files serialize
	// more of the work near the end (one big merge instead of many small
	// ones). Measured on a 147 GiB bbn appdb: default = 1822 files /
	// 25m12s; 1024 = 6 files / 47m55s. Operators who want the leanest
	// post-compact LSM can opt in here; everyone else gets fast wall.
	TargetFileSizeMB int `toml:"target_file_size_mb"`

	// CacheMB sizes the pebble block cache during the compact, in
	// MiB. 0 means "let the CLI compute a default from
	// /proc/meminfo" — see compact's defaultCompactCacheMB. The old
	// hardcoded 8192 (still the library-level fallback in
	// pebbleutil.CleanupCompact for direct API callers) OOM-killed
	// 8 GB hosts because pebble grew the cache toward the cap; the
	// CLI now picks a memory-proportional default instead. See #100.
	CacheMB int `toml:"cache_mb"`
}

// LogTuning maps onto internal/log.Tuning. Edit config.toml to surface
// a silenced module (set its entry to "debug" or remove it) — the
// -debug flag explicitly does NOT override "silent" entries so noisy
// modules stay quiet by default even under verbose runs.
type LogTuning struct {
	// Level is the global threshold ("debug"/"info"/"warn"/"error").
	// Empty = "info".
	Level string `toml:"level"`

	// Modules caps individual modules. Value is a level name; the
	// special value "silent" suppresses the module entirely.
	Modules map[string]string `toml:"modules"`
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

	// PEXDisabled puts the fetch in "curated peers only" mode: PEX
	// gossip is ignored and the connect manager draws warm-fill from
	// bootstrap_peers only — never from the cometbft addrbook.
	// Operator workflow: run with PEX enabled to discover serving
	// peers, then pin those peers in bootstrap_peers and re-run with
	// pex_disabled = true. Default false.
	PEXDisabled bool `toml:"pex_disabled"`

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
	// retries it. The counter is cumulative across the whole fetch
	// — intervening successful writes don't reset it — so the cap
	// distinguishes a one-off filesystem hiccup (recovers) from a
	// sustained disk problem (gets surfaced now, not as a confusing
	// zlib error at import). Raise on flaky cloud disks where
	// transient failures are expected over a long run.
	MaxDiskWriteFailures int `toml:"max_disk_write_failures"`

	Discover      duration `toml:"discover"`
	DialParallel  int      `toml:"dial_parallel"`
	MaxCandidates int      `toml:"max_candidates"`
	ProbeTimeout  duration `toml:"probe_timeout"`
	MinPeers      int      `toml:"min_peers"`

	PerPeer      int      `toml:"per_peer"`
	ChunkTimeout duration `toml:"chunk_timeout"`
	// PeerFails is the consecutive hash-mismatch strike budget before
	// a peer is benched for the run. Missing/empty chunks don't count
	// — they're routed to a per-peer/per-chunk decline set instead, so
	// peers with partial snapshots can still serve what they have.
	PeerFails int `toml:"peer_fails"`
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
	// MemtableMB sizes pebble's per-flush memtable arena. Default
	// 256: bulk import keeps up to MemTableStopWritesThreshold (4)
	// memtables alive, so peak resident arena bytes ≈ MemtableMB×4.
	// Lowered from 1024 in #103 — the 1024 default OOM-killed 8 GiB
	// hosts running on CGO_ENABLED=0 builds (where pebble's
	// manual.New is a plain Go make and arenas stay in the GC heap).
	// Operators on 16+ GiB boxes can raise this back toward 1024 for
	// a small write-throughput win.
	MemtableMB int `toml:"memtable_mb"`
	CacheMB    int `toml:"cache_mb"`
	MinFreeGB  int `toml:"min_free_gb"`

	// FlushSplitMB caps L0 SSTable size produced by memtable flushes.
	// 0 = pebble default (4 MiB). Setting equal to memtable_mb yields
	// ~1 SSTable per flush, which dramatically reduces L0 file count
	// when CompactDuringImport is false (the daemon sees ~50 L0 files
	// instead of thousands and auto-compacts more efficiently).
	FlushSplitMB int `toml:"flush_split_mb"`

	// CompactDuringImport enables pebble's auto-compactions while
	// the import streams. Default false (bulk-load mode): compactions
	// are deferred to a manual `malcom compact` pass or to the daemon's
	// runtime auto-compactions. Setting true trades import wall time
	// for less peak disk usage and a tighter LSM at end of import.
	CompactDuringImport bool `toml:"compact_during_import"`

	// MaxConcurrentCompactions is retained so old configs that set
	// [import].max_concurrent_compactions still parse; the field is
	// no longer consulted. Prefer [compact].max_concurrent_compactions.
	MaxConcurrentCompactions int `toml:"max_concurrent_compactions"`
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
	applyLogDefaults(&c.Log)
	return &c, nil
}

// Resolve returns a fully-populated Chain by layering:
//  1. The global tuning sections from config.toml (lowest priority).
//  2. chains/<name>.toml on top (overrides individual fields).
//  3. Built-in defaults for any field still zero-valued.
//  4. XDG-derived defaults for blank node_key / addrbook.
func (c *Config) Resolve(name string) (Chain, error) {
	if name == "" {
		return Chain{}, fmt.Errorf("no chain specified — pass -chain <id> (added via `malcom add <id>`)")
	}

	// Pre-fill the chain struct with the global tuning defaults so
	// the per-chain decode only overwrites fields it explicitly sets.
	ch := Chain{
		Fetch:     c.Fetch,
		Import:    c.Import,
		Compact:   c.Compact,
		Bootstrap: c.Bootstrap,
		Log:       c.Log,
	}

	chainPath := filepath.Join(c.chainsDir, name+".toml")
	md, err := toml.DecodeFile(chainPath, &ch)
	if err != nil {
		if os.IsNotExist(err) {
			return Chain{}, fmt.Errorf("no chain config at %s — run `malcom add %s` to add it", chainPath, name)
		}
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
	applyCompactDefaults(&ch.Compact)
	applyBootstrapDefaults(&ch.Bootstrap)
	applyLogDefaults(&ch.Log)

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
	if ch.Served == "" {
		d, err := StateDir()
		if err != nil {
			return err
		}
		ch.Served = filepath.Join(d, ch.ChainID, "served.json")
	}
	return nil
}

// IsGenesisURL reports whether s is an http(s) URL (vs a local path).
// Public so subcommands can branch the same way Resolve does.
func IsGenesisURL(s string) bool { return isGenesisURL(s) }

func isGenesisURL(s string) bool {
	return strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://")
}
