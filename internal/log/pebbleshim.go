package log

import (
	"fmt"
	"log/slog"
)

// PebbleShim adapts a *slog.Logger to pebble's Logger interface
// (Infof + Fatalf). Set it on pebble.Options.Logger so pebble's
// startup / WAL / compaction chatter flows through the same handler
// as everything else.
//
// Fatalf preserves pebble's "process must die on this" contract by
// panicking — pebble's docs explicitly allow panic in place of the
// default logger's os.Exit. The panic lets the calling goroutine's
// defers run (addrbook saves, banlist saves, served counters) before
// the goroutine — and, if unrecovered, the process — terminates.
//
// Caveat: this only helps when Fatalf is called *synchronously* from
// a goroutine whose defers we care about (e.g., pebble.Open during
// daemon setup). Pebble's background goroutines (compaction, flush,
// table cache) calling Fatalf still take the process down without
// running the main goroutine's defers — closing that gap needs a
// shutdown-notifier hook; see #108 C3.
func PebbleShim(l *slog.Logger) *PebbleLogger {
	if l == nil {
		l = slog.Default()
	}
	return &PebbleLogger{l: l}
}

// PebbleLogger satisfies pebble.Logger (and the underlying
// pebble/internal/base.Logger interface).
type PebbleLogger struct {
	l *slog.Logger
}

// Infof formats per fmt.Sprintf and emits at slog.LevelInfo.
func (p *PebbleLogger) Infof(format string, args ...any) {
	p.l.Info(fmt.Sprintf(format, args...))
}

// Fatalf formats per fmt.Sprintf, emits at slog.LevelError, then
// panics so the calling goroutine's defers run before termination.
// Pebble's "process must die" contract is preserved: an unrecovered
// panic terminates the process the same way os.Exit would, just
// after defers get a chance to land state on disk.
func (p *PebbleLogger) Fatalf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	p.l.Error(msg)
	panic("pebble fatal: " + msg)
}
