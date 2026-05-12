package snapserve

import "time"

// Config holds all knobs for RunServe. Mostly mirrors snapfetch.Config's
// p2p tuning fields — same dial loop, same PEX wiring — minus the
// fetch-only knobs (chunk timeouts, walk parameters, target heights).
//
// Pass zero values to get defaults via applyDefaults; ChainID,
// NodeKeyPath, AddrBook, Banlist, and SnapshotDirs are required.
type Config struct {
	// Identity / persistent state. ChainID is the gossip network name
	// peers compare against ours during the cometbft handshake — wrong
	// chain id, no connection.
	ChainID     string
	NodeKeyPath string
	Listen      string // default "tcp://0.0.0.0:0"
	Moniker     string // default "malcom-snapserve"
	AddrBook    string // path to cometbft PEX-managed addrbook (required)
	Banlist     string // path to cross-run banlist (required)

	// BootstrapPeers seed the dial pool. Without at least one entry (or
	// a populated AddrBook), there's no peer set to gossip with and the
	// node will sit idle behind its listen socket. Use the operator's
	// usual chain-registry seeds, or — for a back-to-back benchmark —
	// just the fetcher's listen addr.
	BootstrapPeers []string

	// SnapshotDirs are the explicit directories the server advertises
	// and reads chunks from. Each must contain a complete snapshot
	// (.complete marker + meta.json + metadata.bin + chunk_NNNNN.bin)
	// — typically produced by `malcom snapshot fetch`. Verified
	// up-front per VerifyMode; bad dirs (or chain_id mismatches against
	// ChainID) fail RunServe before the listen socket opens.
	//
	// Mutually exclusive with SnapshotsRoot. Set exactly one.
	SnapshotDirs []string

	// SnapshotsRoot is the parent directory the server scans for
	// snapshot subdirs. Each child dir is loaded with the same
	// .complete / meta.json / metadata.bin / chunk_NNNNN.bin
	// requirements as SnapshotDirs, then filtered by ChainID. Wrong-
	// chain entries and incomplete fetches are skipped (logged); only
	// matching, verified entries are served.
	//
	// On a configured RescanInterval the server re-walks the root,
	// builds a new Store, and swaps it onto the reactor without
	// dropping peers. SIGHUP at the CLI layer triggers an immediate
	// rescan.
	//
	// Mutually exclusive with SnapshotDirs.
	SnapshotsRoot string

	// RescanInterval controls how often SnapshotsRoot is re-scanned.
	// Default 30s when SnapshotsRoot is set; zero disables periodic
	// rescan (SIGHUP-only refresh).
	RescanInterval time.Duration

	// OnReloader, when non-nil, is invoked once with a function that
	// triggers an immediate catalogue rescan. CLI wires this to a
	// SIGHUP handler so an operator who just finished a fetch can
	// kick the server without waiting for RescanInterval. Only fires
	// in SnapshotsRoot mode; static-list servers have nothing to
	// reload.
	//
	// The reloader is non-blocking; multiple Trigger calls coalesce
	// into one scan if a scan is already pending.
	OnReloader func(trigger func())

	// VerifyMode controls how thoroughly each snapshot dir is checked
	// before being added to the catalogue. See VerifyMode constants.
	VerifyMode VerifyMode

	// P2P tuning (mirrors snapfetch.Config; see the comments there).
	MaxOutboundPeers    int
	AllowDuplicateIP    bool
	PEXTargetPeers      int
	PEXMaxPerWave       int
	PEXDisabled         bool
	AddrBookBanDuration time.Duration
	MaxDialFailures     int
	WarmRefreshInterval time.Duration
	PeerRedialBackoff   time.Duration
	MaxRedialBackoff    time.Duration

	// MaxRedials caps consecutive disconnect/redial cycles for a
	// pinned peer before connect.Manager auto-bans it for the run.
	// 0 = unlimited.
	//
	// Serve defaults to 0 (unlimited) — unlike fetch, where 4 is
	// reasonable for a 10-minute run, serve is a long-lived daemon
	// where a legitimate peer with intermittent connectivity would
	// hit any non-zero cap and stay banned until the next restart.
	// See #80.
	MaxRedials int

	// PersistInterval controls how often RunServe persists the
	// addrbook and banlist to disk via a background goroutine.
	// Without this, a crash/OOM/SIGKILL loses every PEX-learned peer
	// since the last clean shutdown — fine for fetch (minutes-long
	// runs) but unacceptable for serve, where each cold restart
	// would rediscover the network from bootstrap_peers. Default 5m
	// (set in applyDefaults).
	PersistInterval time.Duration

	// ShutdownDrain is how long RunServe waits for in-flight
	// ChunkResponse sends to flush after ctx is cancelled. During
	// the drain window the reactor fast-fails new ChunkRequest
	// arrivals with Missing=true so they can refetch elsewhere
	// rather than waiting for our per-chunk timeout. After the
	// drain, sw.Stop runs with its existing 1s cap.
	//
	// Default 30s (set in applyDefaults). See #82.
	ShutdownDrain time.Duration

	// ChunkRatePerPeer / ChunkBurstPerPeer cap inbound ChunkRequest
	// from any one peer.ID at a token-bucket rate. Defends against a
	// single peer cycling through every chunk index as fast as the
	// OS can read — cometbft's per-MConn SendRate bounds throughput
	// but not request rate. See #85. 0 disables.
	ChunkRatePerPeer  float64
	ChunkBurstPerPeer int

	// ChunkRateGlobal / ChunkBurstGlobal are the safety-net bucket
	// applied *across all peers* (after each peer's bucket allows).
	// Catches the case of many peers each below their per-peer cap
	// but aggregating to more than we want to serve. 0 disables.
	ChunkRateGlobal  float64
	ChunkBurstGlobal int
}

func (c *Config) applyDefaults() {
	if c.Listen == "" {
		c.Listen = "tcp://0.0.0.0:0"
	}
	if c.Moniker == "" {
		c.Moniker = "malcom-snapserve"
	}
	if c.AddrBookBanDuration == 0 {
		c.AddrBookBanDuration = time.Hour
	}
	if c.MaxDialFailures == 0 {
		c.MaxDialFailures = 3
	}
	if c.WarmRefreshInterval == 0 {
		c.WarmRefreshInterval = 5 * time.Second
	}
	if c.PeerRedialBackoff == 0 {
		c.PeerRedialBackoff = 5 * time.Second
	}
	if c.MaxRedialBackoff == 0 {
		c.MaxRedialBackoff = 5 * time.Minute
	}
	// MaxRedials intentionally has no default — 0 means unlimited at
	// the connect.Manager level, which is what serve wants. See #80.
	//
	// PersistInterval and ShutdownDrain are deliberately *not*
	// defaulted here: 0 means "off" for both (no periodic persist,
	// no drain wait). The CLI supplies sensible non-zero defaults
	// at the flag layer, so library callers passing 0 get 0 — which
	// is what the -shutdown-drain 0 help text promises and what
	// dev workflows asking for fast restart actually want.
	if c.MaxOutboundPeers == 0 {
		// Lower than fetch (64). A serve node doesn't need a wide
		// outbound mesh — its job is to be reachable, and PEX
		// participation only needs a handful of seeds.
		c.MaxOutboundPeers = 16
	}
	if c.PEXTargetPeers == 0 {
		c.PEXTargetPeers = 8
	}
	if c.PEXMaxPerWave == 0 {
		c.PEXMaxPerWave = 4
	}
}
