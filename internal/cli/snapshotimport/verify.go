package snapshotimport

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/ambroslabs/malcom/internal/cli/verify"
)

// ExitVerifyFailed is returned when the post-import AppHash check
// disagrees with consensus. Keep numerically aligned with
// internal/cli/snapshotfetch/exit.go's ExitVerifyFailed so the two
// CLIs surface the same code for the same failure mode.
const ExitVerifyFailed = 7

// runPostImportVerify runs the same AppHash check `malcom verify`
// performs against the freshly imported appdb. Mirrors the helper of
// the same name in internal/cli/snapshotfetch — kept in two places
// (rather than a shared sub-package) because both files are
// thin-CLI-only, and the surface is small enough that one extra layer
// of indirection would obscure rather than help.
//
// Skip conditions and their reasoning:
//   - noVerify flag set: explicit operator opt-out.
//   - rpcs empty: chain config has no rpcs configured, no trust anchor
//     to check against. Fall back to a warning log line.
func runPostImportVerify(log *slog.Logger, appdb string, height int64, rpcs []string, noVerify bool) int {
	if noVerify {
		log.Info("WARNING: -no-verify set; application.db is not authenticated against consensus — run `malcom verify` against a trusted RPC before using this snapshot in production")
		return 0
	}
	if len(rpcs) == 0 {
		log.Info("WARNING: chain config has no rpcs; application.db is not authenticated against consensus — populate chains/<id>.toml rpcs or run `malcom verify -rpc <url>` against a trusted RPC before using this snapshot in production")
		return 0
	}
	verifyLog := log.With("module", "verify")
	verifyLog.Info("verifying apphash against rpc", "appdb", appdb, "height", height, "rpcs", len(rpcs))
	res, err := verify.CheckAppHash(appdb, height, rpcs, verifyLog)
	switch {
	case errors.Is(err, verify.ErrMismatch):
		verifyLog.Error("MISMATCH — local apphash disagrees with consensus",
			"height", res.Height,
			"local", fmt.Sprintf("%X", res.LocalHash),
			"consensus", fmt.Sprintf("%X", res.ConsensusHash),
			"rpc", res.UsedRPC,
			"action", "the imported db is left in place; re-import (or re-fetch a different snapshot) is the typical recovery")
		return ExitVerifyFailed
	case err != nil:
		// RPC unreachable or another transient failure. Don't fail
		// the import — the db on disk is fine, the user just couldn't
		// authenticate it right now.
		verifyLog.Warn("post-import verify did not complete; appdb is not authenticated",
			"err", err,
			"hint", "run `malcom verify -appdb "+appdb+"` once an rpc is reachable")
		return 0
	}
	verifyLog.Info("MATCH — application.db is consensus-correct",
		"height", res.Height,
		"apphash", fmt.Sprintf("%X", res.LocalHash),
		"rpc", res.UsedRPC)
	return 0
}
