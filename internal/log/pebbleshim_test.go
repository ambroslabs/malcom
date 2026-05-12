package log

import (
	"bytes"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"testing"
)

// TestPebbleFatalfRunsCallerDefers is the regression test for issue
// #108 finding C3: PebbleLogger.Fatalf used to call os.Exit(1)
// directly, bypassing every defer in the calling goroutine. A long-
// running daemon would lose addrbook / banlist / served state on any
// synchronous pebble assertion or detected on-disk corruption.
//
// After the fix, Fatalf panics instead. Pebble's "process must die"
// contract is preserved — unrecovered panics still terminate the
// process — but caller-side cleanup runs first.
//
// The test re-execs itself: the child registers a sentinel-printing
// defer, then calls Fatalf. On vulnerable code os.Exit(1) skips the
// defer and the sentinel never lands in the child's stderr. On the
// fixed code the panic unwinds the goroutine, the defer runs, and
// the sentinel appears.
func TestPebbleFatalfRunsCallerDefers(t *testing.T) {
	if os.Getenv("MALCOM_PEBBLE_FATALF_CHILD") == "1" {
		defer fmt.Fprintln(os.Stderr, "DEFER_RAN")
		p := PebbleShim(slog.New(slog.DiscardHandler))
		p.Fatalf("simulated pebble fatal: %s", "test")
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestPebbleFatalfRunsCallerDefers$")
	cmd.Env = append(os.Environ(), "MALCOM_PEBBLE_FATALF_CHILD=1")
	out, _ := cmd.CombinedOutput()
	if !bytes.Contains(out, []byte("DEFER_RAN")) {
		t.Fatalf("PebbleLogger.Fatalf bypassed caller defers; sentinel not found in child output.\n"+
			"--- child output ---\n%s", out)
	}
}
