// Built-in defaults for tuning knobs. Used when the corresponding
// field in the loaded config is zero-valued. Mirrors the historical
// CLI flag defaults so the behaviour is unchanged when init writes a
// fresh template.

package config

import "time"

// Built-in default constants. Exposed so cli help text can reference
// the same values applyFetchDefaults uses, keeping description
// strings honest if the defaults ever change.
const (
	DefaultMaxAgeBlocks = 3000 // ~5h on cosmoshub-4 at 6s blocks
)

func applyFetchDefaults(t *FetchTuning) {
	if t.Listen == "" {
		t.Listen = "tcp://0.0.0.0:0"
	}
	if t.Moniker == "" {
		t.Moniker = "malcom-snapfetch"
	}
	if t.MaxAgeBlocks == 0 {
		t.MaxAgeBlocks = DefaultMaxAgeBlocks
	}
	if t.SnapshotInterval == 0 {
		t.SnapshotInterval = 1000 // cosmoshub-4 default
	}
	if t.PerHeightTimeout.Duration() == 0 {
		t.PerHeightTimeout = duration(10 * time.Second)
	}
	if t.MaxOutboundPeers == 0 {
		t.MaxOutboundPeers = 128
	}
	if t.PEXTargetPeers == 0 {
		t.PEXTargetPeers = 96
	}
	if t.PEXMaxPerWave == 0 {
		t.PEXMaxPerWave = 12
	}
	if t.ChurnGrace.Duration() == 0 {
		t.ChurnGrace = duration(10 * time.Second)
	}
	if t.AddrBookBanDuration.Duration() == 0 {
		t.AddrBookBanDuration = duration(time.Hour)
	}
	if t.ProvisionalProbeStrikes == 0 {
		t.ProvisionalProbeStrikes = 1
	}
	if t.ProvisionalProbeInflight == 0 {
		t.ProvisionalProbeInflight = 1
	}
	if t.MaxDialFailures == 0 {
		t.MaxDialFailures = 3
	}
	// RequireStateSyncChannel is a bool — the zero value (false) is a
	// valid user-set value, so we don't override here. The template
	// sets it true; a config that omits the key gets the Go default
	// (false), which preserves the legacy "no AddPeer channel filter"
	// behaviour.
	if t.Discover.Duration() == 0 {
		t.Discover = duration(25 * time.Second)
	}
	if t.DialParallel == 0 {
		t.DialParallel = 32
	}
	if t.MaxCandidates == 0 {
		t.MaxCandidates = 5
	}
	if t.ProbeTimeout.Duration() == 0 {
		t.ProbeTimeout = duration(12 * time.Second)
	}
	if t.MinPeers == 0 {
		t.MinPeers = 1
	}
	if t.PerPeer == 0 {
		t.PerPeer = 2
	}
	if t.ChunkTimeout.Duration() == 0 {
		t.ChunkTimeout = duration(45 * time.Second)
	}
	if t.MaxFetch.Duration() == 0 {
		t.MaxFetch = duration(60 * time.Minute)
	}
	if t.PeerFails == 0 {
		t.PeerFails = 3
	}
	if t.PeerRedials == 0 {
		t.PeerRedials = 5
	}
	if t.RedialBackoff.Duration() == 0 {
		t.RedialBackoff = duration(5 * time.Second)
	}
	if t.MaxRescans == 0 {
		t.MaxRescans = 3
	}
	if t.RescanDiscover.Duration() == 0 {
		t.RescanDiscover = duration(15 * time.Second)
	}
}

func applyImportDefaults(t *ImportTuning) {
	// Defaults sized for an 8 GiB / 2-4 vCPU host. Bigger boxes can
	// bump memtable_mb / cache_mb in their config.toml; the import is
	// CPU-bound on the IAVL hashing path past ~512 MiB memtables, so
	// returns diminish quickly.
	if t.MemtableMB == 0 {
		t.MemtableMB = 256
	}
	if t.CacheMB == 0 {
		t.CacheMB = 64
	}
	if t.MaxConcurrentCompactions == 0 {
		t.MaxConcurrentCompactions = 2
	}
	if t.MinFreeGB == 0 {
		t.MinFreeGB = 20
	}
}

func applyBootstrapDefaults(t *BootstrapTuning) {
	if t.TrustPeriod.Duration() == 0 {
		t.TrustPeriod = duration(30 * 24 * time.Hour)
	}
	if t.AppDBBackend == "" {
		t.AppDBBackend = "pebbledb"
	}
	if t.CmtDBBackend == "" {
		t.CmtDBBackend = "goleveldb"
	}
	if t.Moniker == "" {
		t.Moniker = "bootstrap-node"
	}
	// PlaceWasm and WriteConfigs are bools — zero value means false.
	// Historical defaults were both true, so we set them in the
	// template rather than overriding zero values here. This way a
	// user who explicitly sets place_wasm = false in their config
	// gets that behaviour.
}
