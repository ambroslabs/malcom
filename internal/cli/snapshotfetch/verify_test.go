package snapshotfetch

import (
	"io"
	"log/slog"
	"testing"
)

// silentLogger discards all log output so tests aren't noisy.
func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

// runPostImportVerify's skip cases must all return ExitSuccess without
// touching the network. The match / mismatch paths need the real
// CheckAppHash + a fixture appdb, which lives in
// internal/cli/verify/verify_test.go — kept there because that
// package owns the load-bearing logic. Here we only assert the
// flag-routing.

func TestRunPostImportVerify_NoAppDBSkips(t *testing.T) {
	rc := runPostImportVerify(silentLogger(), "", 0, []string{"http://localhost:1234"}, false)
	if rc != ExitSuccess {
		t.Fatalf("empty appdb path should skip verify and return ExitSuccess; got %d", rc)
	}
}

func TestRunPostImportVerify_NoVerifyFlagSkips(t *testing.T) {
	// noVerify=true must short-circuit before any RPC call. Pointing
	// at a known-bad URL would fail loudly if we mistakenly reached
	// the check.
	rc := runPostImportVerify(silentLogger(), "/nonexistent", 1, []string{"http://127.0.0.1:1"}, true)
	if rc != ExitSuccess {
		t.Fatalf("noVerify=true should skip verify and return ExitSuccess; got %d", rc)
	}
}

func TestRunPostImportVerify_EmptyRPCsSkips(t *testing.T) {
	// No RPCs configured → no trust anchor → warn and continue.
	rc := runPostImportVerify(silentLogger(), "/nonexistent", 1, nil, false)
	if rc != ExitSuccess {
		t.Fatalf("empty rpcs should skip verify and return ExitSuccess; got %d", rc)
	}
}

// Sanity-check the constant so a future renumbering can't silently
// shift the operator-visible contract.
func TestExitVerifyFailedConstant(t *testing.T) {
	if ExitVerifyFailed != 7 {
		t.Fatalf("ExitVerifyFailed = %d, want 7 (stable operator-visible code)", ExitVerifyFailed)
	}
}
