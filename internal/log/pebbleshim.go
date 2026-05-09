package log

import (
	"fmt"
	"log/slog"
	"os"
)

// PebbleShim adapts a *slog.Logger to pebble's Logger interface
// (Infof + Fatalf). Set it on pebble.Options.Logger so pebble's
// startup / WAL / compaction chatter flows through the same handler
// as everything else.
//
// Fatalf preserves pebble's "process must die on this" contract: it
// emits an Error record then calls os.Exit(1). Pebble uses Fatalf for
// unrecoverable assertion failures and on-disk corruption — silently
// returning would leave the process running with a poisoned db.
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
// terminates the process with status 1 — matching pebble's default
// logger contract.
func (p *PebbleLogger) Fatalf(format string, args ...any) {
	p.l.Error(fmt.Sprintf(format, args...))
	os.Exit(1)
}
