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
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	cmtlog "github.com/cometbft/cometbft/libs/log"
)

// LevelSilent is a sentinel level higher than slog.LevelError used to
// silence a module entirely. A record at Error (8) is below 100, so
// the moduleFilter drops it. Used as the per-module value when the
// operator wants a noisy module (cometbft's p2p / mconnection / pex)
// suppressed: it's not a real severity, just "no record can clear
// this threshold."
const LevelSilent slog.Level = 100

// ParseLevel decodes "debug" / "info" / "warn" / "error" / "silent"
// (case-insensitive) into the corresponding slog.Level. The empty
// string returns slog.LevelInfo so a missing config value defaults
// sensibly. Returns an error on any other input.
func ParseLevel(s string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "":
		return slog.LevelInfo, nil
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	case "silent", "off":
		return LevelSilent, nil
	}
	return 0, fmt.Errorf("unknown log level %q (want debug/info/warn/error/silent)", s)
}

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

// Tuning is the operator-facing log configuration. Mirrors the
// [log] section in malcom's config.toml. Built into Options via
// BuildOptions.
type Tuning struct {
	// Level is the global threshold for modules not listed in Modules.
	// Empty string → "info".
	Level string

	// Modules caps individual modules at the named level. Records
	// emitted at a lower level pass; records at or above pass; no —
	// wait — the value is the *threshold* for that module, same
	// semantics as Level. The special value "silent" maps to a high
	// sentinel that no record can clear, dropping the module.
	Modules map[string]string
}

// BuildOptions resolves a Tuning + format mode + -debug flag into an
// Options ready for New. -debug overrides the global threshold to
// debug AND bumps every entry in Modules to debug — except entries
// explicitly set to "silent", which stay silent (operator edits config
// to surface those).
func BuildOptions(t Tuning, mode Mode, debug bool, w io.Writer) (Options, error) {
	level, err := ParseLevel(t.Level)
	if err != nil {
		return Options{}, fmt.Errorf("log.level: %w", err)
	}
	moduleLevels := make(map[string]slog.Level, len(t.Modules))
	for name, val := range t.Modules {
		lv, err := ParseLevel(val)
		if err != nil {
			return Options{}, fmt.Errorf("log.modules.%s: %w", name, err)
		}
		moduleLevels[name] = lv
	}
	if debug {
		level = slog.LevelDebug
		for name, lv := range moduleLevels {
			if lv == LevelSilent {
				continue // explicit silence wins; operator must edit config to see it
			}
			moduleLevels[name] = slog.LevelDebug
		}
	}
	return Options{
		Writer:       w,
		Mode:         mode,
		Level:        level,
		ModuleLevels: moduleLevels,
	}, nil
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
