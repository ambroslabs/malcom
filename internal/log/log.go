// Package log is malcom's logger: a thin implementation of
// cometbft's `libs/log.Logger` interface that produces friendlier
// output than cometbft's default TMLogger.
//
// Format:
//
//	HH:MM:SS.mmm LEVEL MODULE     msg key=val key=val
//
// Level prefix is colored when the writer is a TTY and NO_COLOR is
// unset (https://no-color.org/). Module is right-padded to a fixed
// width so columns line up across log lines.
//
// Drop-in for `cmtlog.NewTMLogger(...)` — wrap with
// `cmtlog.NewFilter(...)` exactly the same way.
package log

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	cmtlog "github.com/cometbft/cometbft/libs/log"
)

// New returns a cmtlog.Logger writing to w. If w is a TTY and
// NO_COLOR is unset, level prefixes are ANSI-colored.
func New(w io.Writer) cmtlog.Logger {
	return &logger{
		w:     w,
		color: shouldColor(w),
		mu:    &sync.Mutex{},
	}
}

const moduleColWidth = 10 // fixed-width module column so msgs align

type logger struct {
	w       io.Writer
	keyvals []any
	color   bool
	mu      *sync.Mutex // shared across With() derivatives so writes are serialized
}

// With returns a derived logger with kv prepended to every subsequent
// log line. Cometbft uses this heavily (e.g., logger.With("module",
// "fetch")).
func (l *logger) With(kv ...any) cmtlog.Logger {
	combined := make([]any, 0, len(l.keyvals)+len(kv))
	combined = append(combined, l.keyvals...)
	combined = append(combined, kv...)
	return &logger{w: l.w, keyvals: combined, color: l.color, mu: l.mu}
}

func (l *logger) Debug(msg string, kv ...any) { l.emit("DEBUG", msg, kv) }
func (l *logger) Info(msg string, kv ...any)  { l.emit("INFO ", msg, kv) }
func (l *logger) Error(msg string, kv ...any) { l.emit("ERROR", msg, kv) }

func (l *logger) emit(level, msg string, extra []any) {
	var b strings.Builder

	// Timestamp.
	b.WriteString(time.Now().Format("15:04:05.000"))
	b.WriteByte(' ')

	// Level (colored when applicable).
	if l.color {
		b.WriteString(colorize(level))
	} else {
		b.WriteString(level)
	}
	b.WriteByte(' ')

	// Module column (extracted from the merged keyvals).
	mod := pickModule(l.keyvals, extra)
	if mod == "" {
		mod = "-"
	}
	if len(mod) < moduleColWidth {
		b.WriteString(mod)
		b.WriteString(strings.Repeat(" ", moduleColWidth-len(mod)))
	} else {
		b.WriteString(mod)
	}
	b.WriteByte(' ')

	// Message.
	b.WriteString(msg)

	// Remaining key=val pairs (module already consumed).
	writeKVs(&b, l.keyvals)
	writeKVs(&b, extra)

	b.WriteByte('\n')

	l.mu.Lock()
	_, _ = l.w.Write([]byte(b.String()))
	l.mu.Unlock()
}

// pickModule extracts the module= keyval if present in either slice.
// Last value wins (matches cometbft's With semantics).
func pickModule(a, b []any) string {
	mod := ""
	for _, kvs := range [][]any{a, b} {
		for i := 0; i+1 < len(kvs); i += 2 {
			if k, ok := kvs[i].(string); ok && k == "module" {
				mod = fmt.Sprintf("%v", kvs[i+1])
			}
		}
	}
	return mod
}

func writeKVs(b *strings.Builder, kvs []any) {
	for i := 0; i+1 < len(kvs); i += 2 {
		k, ok := kvs[i].(string)
		if !ok || k == "module" || k == "" {
			continue
		}
		v := stringify(kvs[i+1])
		fmt.Fprintf(b, " %s=%s", k, v)
	}
}

// stringify renders v with logfmt-style escaping: quote when the
// value contains whitespace, '=', '"', or newline.
func stringify(v any) string {
	s := fmt.Sprintf("%v", v)
	if strings.ContainsAny(s, " \t\"\n=") {
		return strconv.Quote(s)
	}
	return s
}

func colorize(level string) string {
	switch strings.TrimSpace(level) {
	case "ERROR":
		return "\x1b[31m" + level + "\x1b[0m"
	case "INFO":
		return "\x1b[32m" + level + "\x1b[0m"
	case "DEBUG":
		return "\x1b[90m" + level + "\x1b[0m"
	}
	return level
}

// shouldColor returns true when w is a *os.File pointing at a
// character device (TTY) and NO_COLOR is unset.
func shouldColor(w io.Writer) bool {
	if os.Getenv("NO_COLOR") != "" {
		return false
	}
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
