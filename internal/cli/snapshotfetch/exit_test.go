package snapshotfetch

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/ambroslabs/malcom/internal/snapfetch"
)

func TestMapExitCode(t *testing.T) {
	cases := []struct {
		name        string
		err         error
		interrupted bool
		want        int
	}{
		{"nil success", nil, false, ExitSuccess},
		{"interrupted overrides nil", nil, true, ExitInterrupted},
		{"interrupted overrides error", errors.New("late error"), true, ExitInterrupted},
		{"context canceled", context.Canceled, false, ExitInterrupted},
		{"wrapped context canceled", fmt.Errorf("phase: %w", context.Canceled), false, ExitInterrupted},
		{"no peers direct", snapfetch.ErrNoPeers, false, ExitNoPeers},
		{"no peers wrapped", fmt.Errorf("session: %w", snapfetch.ErrNoPeers), false, ExitNoPeers},
		{"walk failed direct", snapfetch.ErrWalkFailed, false, ExitWalkFailed},
		{"walk failed wrapped", fmt.Errorf("%w: window [0,0]", snapfetch.ErrWalkFailed), false, ExitWalkFailed},
		{"download failed direct", snapfetch.ErrDownloadFailed, false, ExitDownloadFailed},
		{"download failed double-wrapped", fmt.Errorf("%w: %w", snapfetch.ErrDownloadFailed, errors.New("inner")), false, ExitDownloadFailed},
		{"disk failed direct", snapfetch.ErrDiskFailed, false, ExitDiskFailed},
		{"disk failed double-wrapped", fmt.Errorf("%w: write meta.json: %w", snapfetch.ErrDiskFailed, errors.New("ENOSPC")), false, ExitDiskFailed},
		{"unknown error → generic", errors.New("something else"), false, ExitGeneric},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := mapExitCode(tc.err, tc.interrupted); got != tc.want {
				t.Fatalf("mapExitCode(%v, %v) = %d; want %d", tc.err, tc.interrupted, got, tc.want)
			}
		})
	}
}

// Walk failure should win over a co-wrapped download failure if the
// walk sentinel is the outer one — first match in the switch order.
// (No current path produces both; this just guards the switch
// ordering against future regressions.)
func TestMapExitCode_FirstMatchWins(t *testing.T) {
	err := fmt.Errorf("%w: %w", snapfetch.ErrWalkFailed, snapfetch.ErrDownloadFailed)
	if got := mapExitCode(err, false); got != ExitWalkFailed {
		t.Fatalf("got %d, want %d (walk should match before download)", got, ExitWalkFailed)
	}
}
