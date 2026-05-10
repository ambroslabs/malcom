// Built-in defaults for tuning knobs. Used when the corresponding
// field in the loaded config is zero-valued. Mirrors the historical
// CLI flag defaults so the behaviour is unchanged when init writes a
// fresh template.

package config

import (
	"runtime"
	"time"
)

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
		t.MaxOutboundPeers = 192
	}
	if t.PEXTargetPeers == 0 {
		t.PEXTargetPeers = 128
	}
	if t.PEXMaxPerWave == 0 {
		t.PEXMaxPerWave = 64
	}
	if t.ChurnGrace.Duration() == 0 {
		t.ChurnGrace = duration(10 * time.Second)
	}
	if t.AddrBookBanDuration.Duration() == 0 {
		t.AddrBookBanDuration = duration(time.Hour)
	}
	if t.ProvisionalProbeStrikes == 0 {
		t.ProvisionalProbeStrikes = 2
	}
	if t.ProvisionalProbeInflight == 0 {
		t.ProvisionalProbeInflight = 1
	}
	if t.MaxDialFailures == 0 {
		t.MaxDialFailures = 3
	}
	if t.MaxDiskWriteFailures == 0 {
		t.MaxDiskWriteFailures = 3
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
		t.PeerRedials = 4
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
	// Defaults sized for the typical bootstrap host (~8 GiB box). The
	// IAVL hashing path is CPU-bound past ~2 GiB memtables, so
	// returns diminish above that. Smaller hosts should drop both
	// memtable_mb and flush_split_mb proportionally via config.
	//
	// 1024 MiB chosen over the previous 256 MiB to keep the post-
	// import L0 file count manageable: each memtable flush produces
	// roughly one SST per flush_split_mb of payload, so a 4× bigger
	// memtable cuts file count ~4× and compaction wall accordingly.
	// On bbn finality (~150 GiB s/) this drops L0 from ~70k files
	// to ~hundreds.
	if t.MemtableMB == 0 {
		t.MemtableMB = 1024
	}
	if t.CacheMB == 0 {
		t.CacheMB = 64
	}
	if t.MinFreeGB == 0 {
		t.MinFreeGB = 20
	}
	// FlushSplitMB defaults to memtable_mb so each memtable flush
	// produces ~1 L0 SSTable instead of the pebble default ~64
	// small ones. With CompactDuringImport off (the default), this
	// is what gaiad / the standalone compact will see.
	if t.FlushSplitMB == 0 {
		t.FlushSplitMB = t.MemtableMB
	}
	// CompactDuringImport zero value (false) is the default — a bool
	// can't distinguish "unset" from "set to false", so we don't
	// override here. Templates document the default.
}

// ApplyCompactDefaults is the package-internal applyCompactDefaults
// exposed for callers that work directly off the global Config (e.g.
// `malcom compact`, which doesn't go through Resolve because it takes
// no -chain argument).
func ApplyCompactDefaults(t *CompactTuning) { applyCompactDefaults(t) }

func applyCompactDefaults(t *CompactTuning) {
	if t.MaxConcurrentCompactions == 0 {
		t.MaxConcurrentCompactions = runtime.NumCPU()
		if t.MaxConcurrentCompactions < 1 {
			t.MaxConcurrentCompactions = 1
		}
	}
}

// applyLogDefaults fills in malcom's baseline log policy when both the
// global and per-chain config omit a [log] section: info threshold,
// and silence the cometbft modules that emit normal-operation events
// at Error level (peer churn) or dump packet byte counts at Debug.
//
// Operators see these by editing config.toml — `-debug` does NOT
// override an explicit "silent" entry.
func applyLogDefaults(t *LogTuning) {
	if t.Level == "" {
		t.Level = "info"
	}
	if t.Modules == nil {
		t.Modules = map[string]string{
			"addrbook":    "error",  // surface real errors, suppress info chatter
			"p2p":         "silent", // cometbft switch — peer EOFs aren't actionable
			"mconnection": "silent", // packet byte counts at Debug
			"pex":         "silent", // PEX gossip noise (covers our pex and cometbft's)
			"statesync":   "silent", // BaseService start/stop + "send queue full" at Error on slow peers — neither actionable
		}
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
