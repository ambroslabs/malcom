package log

import (
	"log/slog"

	cmtlog "github.com/cometbft/cometbft/libs/log"
)

// CmtShim wraps a *slog.Logger as a cmtlog.Logger. The cometbft p2p
// stack (Switch, Reactors, AddrBook, …) wants this interface; the
// shim translates Debug/Info/Error/With into the slog equivalents so
// cometbft's output flows through the same pretty/text/json handler
// as everything else.
//
// cmtlog.Logger has no Warn level (cometbft was written before slog
// existed), so cmtlog Info/Error map straight to slog Info/Error.
func CmtShim(l *slog.Logger) cmtlog.Logger {
	if l == nil {
		l = slog.Default()
	}
	return &cmtShim{l: l}
}

type cmtShim struct {
	l *slog.Logger
}

func (s *cmtShim) Debug(msg string, kv ...any) { s.l.Debug(msg, kv...) }
func (s *cmtShim) Info(msg string, kv ...any)  { s.l.Info(msg, kv...) }
func (s *cmtShim) Error(msg string, kv ...any) { s.l.Error(msg, kv...) }

func (s *cmtShim) With(kv ...any) cmtlog.Logger {
	return &cmtShim{l: s.l.With(kv...)}
}
