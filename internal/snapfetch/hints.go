package snapfetch

import (
	"fmt"
	"strings"
)

// Hints centralize the remediation phrasing surfaced on terminal fetch
// errors. Each terminal error in the package picks one and appends it
// to the message in the form `<what failed> — <hint>`, mirroring the
// existing `internal/cli/snapshotfetch/run.go` template.
//
// Goal: every operator-facing failure names what went wrong AND a
// concrete next step (a config field, a CLI flag, or `malcom init`).
// Keep these single-line so the CLI's `fmt.Fprintf("snapfetch: %v\n",
// err)` print stays readable.

// chainCfgRef formats a config-field reference like
// `[chains.cosmoshub-4.fetch] max_age_blocks` (or, with multiple
// fields, `[chains.cosmoshub-4.fetch] max_age_blocks/per_height_timeout`).
// Used so each hint names the *exact* TOML knob the operator needs to
// touch.
func chainCfgRef(chainID string, fields ...string) string {
	return fmt.Sprintf("[chains.%s.fetch] %s", chainID, strings.Join(fields, "/"))
}

// hintNoServable is the catch-all hint for "we walked the whole window
// and nothing served chunk-0". The likely causes span every layer
// (peer set too small, peer set churning, freshness floor too tight,
// dial budget too small, egress blocked) so the hint enumerates the
// knobs in roughly cheapest-to-try order.
func hintNoServable(chainID string) string {
	return fmt.Sprintf("no peer offered+served a snapshot in the window. Try: raise %s (or pass -max-age=), drop -target-height to widen the walk, add fresh bootstrap_peers under %s, raise %s, or check egress to peers",
		chainCfgRef(chainID, "max_age_blocks"),
		chainCfgRef(chainID, "bootstrap_peers"),
		chainCfgRef(chainID, "max_outbound_peers", "pex_target_peers"),
	)
}

// hintTargetHeightUnserved fires when -target-height is set and no
// peer served chunk-0 for that exact height in time. The walk has no
// fallback in this mode, so the remediations are narrower than
// hintNoServable.
func hintTargetHeightUnserved(chainID string) string {
	return fmt.Sprintf("drop -target-height to let the walk fall back to lower heights, raise %s, or add fresh bootstrap_peers under %s",
		chainCfgRef(chainID, "per_height_timeout"),
		chainCfgRef(chainID, "bootstrap_peers"),
	)
}

// hintEmptyTargetWindow fires when MinHeight/MaxHeight/SnapshotInterval
// produce zero candidate heights. Almost always means the freshness
// floor is too aggressive for the chain's snapshot cadence.
func hintEmptyTargetWindow(chainID string) string {
	return fmt.Sprintf("freshness floor leaves no aligned heights. Try: raise %s (or pass -max-age=), or pass -target-height directly",
		chainCfgRef(chainID, "max_age_blocks"),
	)
}

// hintMissingHeightInputs fires when the walk is invoked without
// MaxHeight or TargetHeight. Mirrors the existing CLI-side template
// at internal/cli/snapshotfetch/run.go:118.
const hintMissingHeightInputs = "pass -target-height to lock to one height, or -max-height to override the RPC lookup"

// hintNoPeerAddrs fires when buildPeerAddrs has nothing to seed: no
// bootstrap_peers configured AND addrbook.json is missing/empty.
// First-run users hit this most.
func hintNoPeerAddrs(chainID string) string {
	return fmt.Sprintf("run `malcom init` to seed peers from chain-registry, or set %s in config",
		chainCfgRef(chainID, "bootstrap_peers"),
	)
}

// hintAllPeersBanned fires mid-download when every tracked peer has
// hit its strike budget or been dropped by peerWatch. Usually means
// the peer set is dominated by misbehaving deployments OR the strike
// budgets are too tight for a flaky network. Widening the outbound
// ceiling doesn't help here — the existing peers already misbehaved;
// the cure is fresh peers or more lenient strike budgets.
func hintAllPeersBanned(chainID string) string {
	return fmt.Sprintf("every peer hit its strike budget. Try: raise %s, or add fresh bootstrap_peers under %s",
		chainCfgRef(chainID, "peer_fails", "provisional_probe_strikes"),
		chainCfgRef(chainID, "bootstrap_peers"),
	)
}

// hintHashMismatch fires when the post-download SHA256(chunks) check
// or the chunk-count metadata check fails. Both indicate a peer
// served a forged offer or tampered chunks; the only cure is a fresh
// run that picks a different offer.
const hintHashMismatch = "rerun `malcom snapshot fetch` — the misbehaving peer is now in the banlist and a different offer should win"
