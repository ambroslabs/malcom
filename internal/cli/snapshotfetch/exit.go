package snapshotfetch

import (
	"context"
	"errors"

	"github.com/zrbecker/cosmos-p2p/internal/snapfetch"
)

// Exit codes returned by Run. Documented as a stable contract so
// orchestrators (shell scripts, wrapping CLIs) can branch on the
// failure mode — e.g. retry on ExitNoPeers, pivot config on
// ExitWalkFailed, escalate on ExitDiskFailed.
//
// Map errors to codes via [mapExitCode]. Unknown errors collapse to
// [ExitGeneric], which is the documented catch-all.
const (
	ExitSuccess        = 0
	ExitGeneric        = 1
	ExitConfig         = 2
	ExitNoPeers        = 3
	ExitWalkFailed     = 4
	ExitDownloadFailed = 5
	ExitDiskFailed     = 6
	// ExitVerifyFailed — fetch + pipelined import succeeded, but the
	// post-import AppHash check against a trusted RPC didn't agree
	// with consensus. The imported db is left in place so the
	// operator can inspect; re-fetching is the typical recovery.
	// Only produced when -import is set and -no-verify isn't.
	ExitVerifyFailed = 7
	ExitInterrupted  = 130
)

// mapExitCode classifies a RunFetch error into the documented exit
// code. interrupted overrides everything else: a SIGINT/SIGTERM that
// cancelled the run is the user's intent regardless of which phase
// happened to surface ctx.Err().
func mapExitCode(err error, interrupted bool) int {
	if interrupted {
		return ExitInterrupted
	}
	if err == nil {
		return ExitSuccess
	}
	switch {
	case errors.Is(err, context.Canceled):
		return ExitInterrupted
	case errors.Is(err, snapfetch.ErrNoPeers):
		return ExitNoPeers
	case errors.Is(err, snapfetch.ErrWalkFailed):
		return ExitWalkFailed
	case errors.Is(err, snapfetch.ErrDownloadFailed):
		return ExitDownloadFailed
	case errors.Is(err, snapfetch.ErrDiskFailed):
		return ExitDiskFailed
	}
	return ExitGeneric
}
