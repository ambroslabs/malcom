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
	Moniker     string // default "cosmos-p2p-snapserve"
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
	MaxRedials          int
}

func (c *Config) applyDefaults() {
	if c.Listen == "" {
		c.Listen = "tcp://0.0.0.0:0"
	}
	if c.Moniker == "" {
		c.Moniker = "cosmos-p2p-snapserve"
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
	if c.MaxRedials == 0 {
		c.MaxRedials = 5
	}
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
