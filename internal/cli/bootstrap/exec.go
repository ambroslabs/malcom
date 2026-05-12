// Subprocess primitives: locate the chain binary, invoke it, and probe
// for the bootstrap-state subcommand path (which moved from
// `<daemon> tendermint bootstrap-state` to `<daemon> comet
// bootstrap-state` somewhere in the cosmos-sdk timeline).

package bootstrap

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strings"
)

// findBinary returns the absolute path to the chain binary.
//
// Resolution order:
//  1. --binary flag (if non-empty), verified executable.
//  2. exec.LookPath(daemonName) — uses the operator's $PATH.
//
// daemonName is whatever the chain-registry's chain.json declares
// (e.g. "gaiad", "osmosisd"). Empty daemonName plus empty override is
// a hard error: malcom can't guess.
func findBinary(override, daemonName string) (string, error) {
	if override != "" {
		abs, err := exec.LookPath(override)
		if err != nil {
			return "", fmt.Errorf("-binary %q not executable: %w", override, err)
		}
		return abs, nil
	}
	if daemonName == "" {
		return "", errors.New("no -binary given and chain-registry has no daemon_name; pass -binary <path>")
	}
	abs, err := exec.LookPath(daemonName)
	if err != nil {
		return "", fmt.Errorf("daemon %q not found in $PATH (pass -binary <path>): %w", daemonName, err)
	}
	return abs, nil
}

// runDaemon invokes the chain binary, streaming stdout+stderr to the
// logger at info level (one log record per line, module=daemon). Stdin
// is /dev/null. Returns any non-zero exit status as an error wrapping
// the captured tail of stderr.
func runDaemon(ctx context.Context, log *slog.Logger, binary string, args ...string) error {
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Stdin = nil
	stderrTail := &tailBuf{cap: 4 << 10}
	cmd.Stdout = lineLogger{log: log.With("module", "daemon", "stream", "stdout")}
	cmd.Stderr = io.MultiWriter(
		lineLogger{log: log.With("module", "daemon", "stream", "stderr")},
		stderrTail,
	)
	log.Info("exec", "binary", binary, "args", args)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s %s: %w (stderr tail: %s)", binary, strings.Join(args, " "), err, stderrTail.String())
	}
	return nil
}

// probeStatesyncSubcommand returns "tendermint" or "comet" — whichever
// `<binary> {tendermint,comet} bootstrap-state --help` exits 0 on. If
// neither works, returns an error with both probes' stderr tails so the
// caller can see why.
func probeStatesyncSubcommand(ctx context.Context, binary string) (string, error) {
	for _, sub := range []string{"comet", "tendermint"} {
		cmd := exec.CommandContext(ctx, binary, sub, "bootstrap-state", "--help")
		if err := cmd.Run(); err == nil {
			return sub, nil
		}
	}
	return "", fmt.Errorf("%s has neither `tendermint bootstrap-state` nor `comet bootstrap-state` subcommand", binary)
}

// lineLogger is an io.Writer that splits its input on '\n' and emits
// one log record per non-empty line. Bytes after the last newline in
// any single Write are dropped — fine for daemon stdout/stderr where
// we don't care about the trailing partial line at process exit.
type lineLogger struct {
	log *slog.Logger
}

func (l lineLogger) Write(p []byte) (int, error) {
	// Return the original input length per io.Writer's contract.
	// We don't buffer partial trailing bytes (slog handlers don't
	// need a perfect line stream — losing the very last unfinished
	// line at process exit is fine).
	n := len(p)
	for {
		i := bytes.IndexByte(p, '\n')
		if i < 0 {
			break
		}
		line := strings.TrimRight(string(p[:i]), "\r\n")
		if line != "" {
			l.log.Debug(line)
		}
		p = p[i+1:]
	}
	return n, nil
}

// tailBuf retains the last `cap` bytes written. Used to surface a
// useful slice of stderr when a daemon invocation fails.
type tailBuf struct {
	cap int
	buf []byte
}

func (t *tailBuf) Write(p []byte) (int, error) {
	t.buf = append(t.buf, p...)
	if len(t.buf) > t.cap {
		t.buf = t.buf[len(t.buf)-t.cap:]
	}
	return len(p), nil
}

func (t *tailBuf) String() string {
	return strings.TrimSpace(string(t.buf))
}

// resolveAbs is a small helper used by callers that want a logged
// absolute path. Falls back to the input on os.Getwd failures.
func resolveAbs(p string) string {
	abs, err := os.Getwd()
	if err != nil || p == "" {
		return p
	}
	if strings.HasPrefix(p, "/") {
		return p
	}
	return abs + "/" + p
}
