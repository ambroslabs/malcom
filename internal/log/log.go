// Package log is malcom's logging foundation. It produces a
// *slog.Logger configured with one of three handlers:
//
//   - "pretty": elapsed-from-start TTY output with a colored severity
//     bar, stable per-module color, aligned msg + key=val columns,
//     and key-aware value rendering (digit-grouped heights, byte
//     sizes, durations). The default when stderr is a TTY.
//   - "text":   stdlib slog.TextHandler (logfmt). The default when
//     stderr is not a TTY — pipeable, greppable.
//   - "json":   stdlib slog.JSONHandler. Drop into Loki / Vector /
//     CloudWatch unmodified.
//
// The cometbft p2p stack wants a cmtlog.Logger; CmtShim adapts a
// *slog.Logger to that interface so output from cometbft reactors
// flows through the same handler as everything else.
package log

import (
	"io"
	"log/slog"
	"os"
	"strings"

	cmtlog "github.com/cometbft/cometbft/libs/log"
)

// Mode picks the output handler.
type Mode int

const (
	ModeAuto   Mode = iota // pretty if TTY, text otherwise
	ModePretty             // forced pretty
	ModeText               // forced logfmt
	ModeJSON               // forced JSON
)

// ParseMode accepts "auto" / "pretty" / "text" / "json"
// (case-insensitive). Empty string returns ModeAuto.
func ParseMode(s string) (Mode, bool) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "auto":
		return ModeAuto, true
	case "pretty":
		return ModePretty, true
	case "text":
		return ModeText, true
	case "json":
		return ModeJSON, true
	}
	return ModeAuto, false
}

// Options configures New.
type Options struct {
	// Writer receives the rendered output. Nil → os.Stderr.
	Writer io.Writer

	// Mode selects the handler. ModeAuto picks pretty for TTY, text
	// otherwise.
	Mode Mode

	// Level threshold. Zero value = slog.LevelInfo.
	Level slog.Level

	// ModuleLevels overrides Level on a per-module basis. The "module"
	// attr on a log record is matched exactly; modules without an
	// override fall back to Level.
	ModuleLevels map[string]slog.Level
}

// New returns a *slog.Logger using the configured handler.
func New(opts Options) *slog.Logger {
	w := opts.Writer
	if w == nil {
		w = os.Stderr
	}
	mode := opts.Mode
	if mode == ModeAuto {
		if isTTY(w) && os.Getenv("NO_COLOR") == "" {
			mode = ModePretty
		} else {
			mode = ModeText
		}
	}

	hopts := &slog.HandlerOptions{Level: opts.Level}

	var base slog.Handler
	switch mode {
	case ModePretty:
		base = newPrettyHandler(w, opts.Level, isTTY(w) && os.Getenv("NO_COLOR") == "")
	case ModeJSON:
		base = slog.NewJSONHandler(w, hopts)
	default:
		base = slog.NewTextHandler(w, hopts)
	}

	if len(opts.ModuleLevels) > 0 {
		base = &moduleFilter{
			inner:    base,
			defLevel: opts.Level,
			byModule: opts.ModuleLevels,
		}
	}
	return slog.New(base)
}

// Discard returns a logger that drops every record. Useful in tests.
func Discard() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

func isTTY(w io.Writer) bool {
	f, ok := w.(*os.File)
	if !ok {
		return false
	}
	info, err := f.Stat()
	if err != nil {
		return false
	}
	return (info.Mode() & os.ModeCharDevice) != 0
}

// Compile-time check: cmtshim implements cmtlog.Logger.
var _ cmtlog.Logger = (*cmtShim)(nil)
