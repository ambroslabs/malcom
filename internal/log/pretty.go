package log

import (
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// Layout (when color is enabled, ANSI codes elided):
//
//	  T+MM:SS.mmm  module    msg                  k=v  k=v
//	▍ T+MM:SS.mmm  module    msg                  …          (warn, yellow bar)
//	█ T+MM:SS.mmm  module    msg                  …          (error, red bar)
//
// Layout knobs:
//
//   - Severity bar: 1-char column, blank for info/debug, yellow ▍ for
//     warn, red █ for error. Always at the left margin so warnings
//     and errors pop visually.
//   - Elapsed timestamp: T+MM:SS.mmm from process start. Wall clock
//     is unhelpful for a single-shot CLI; "how long has it been?" is
//     the question you actually ask.
//   - Module column: 9 chars, stable color picked by FNV(name) %
//     palette. Same module → same color across runs.
//   - Message column: padded to msgColWidth so kv pairs align across
//     lines; double-spaced kv separators read better past 4 keys.
const (
	msgColWidth    = 24
	moduleColWidth = 9
)

var startTime = time.Now()

type prettyHandler struct {
	w     io.Writer
	level slog.Level
	color bool
	mu    *sync.Mutex // shared across With/WithGroup derivatives

	attrs  []slog.Attr // accumulated via WithAttrs
	groups []string    // accumulated via WithGroup
}

func newPrettyHandler(w io.Writer, level slog.Level, color bool) slog.Handler {
	return &prettyHandler{
		w:     w,
		level: level,
		color: color,
		mu:    &sync.Mutex{},
	}
}

func (h *prettyHandler) Enabled(_ context.Context, l slog.Level) bool {
	return l >= h.level
}

func (h *prettyHandler) WithAttrs(as []slog.Attr) slog.Handler {
	combined := make([]slog.Attr, 0, len(h.attrs)+len(as))
	combined = append(combined, h.attrs...)
	combined = append(combined, as...)
	return &prettyHandler{
		w:      h.w,
		level:  h.level,
		color:  h.color,
		mu:     h.mu,
		attrs:  combined,
		groups: h.groups,
	}
}

func (h *prettyHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	groups := make([]string, 0, len(h.groups)+1)
	groups = append(groups, h.groups...)
	groups = append(groups, name)
	return &prettyHandler{
		w:      h.w,
		level:  h.level,
		color:  h.color,
		mu:     h.mu,
		attrs:  h.attrs,
		groups: groups,
	}
}

func (h *prettyHandler) Handle(_ context.Context, r slog.Record) error {
	var b strings.Builder

	// Severity bar at the left margin.
	switch {
	case r.Level >= slog.LevelError:
		writeColored(&b, h.color, ansiRedBg, "█")
		b.WriteByte(' ')
	case r.Level >= slog.LevelWarn:
		writeColored(&b, h.color, ansiYellow, "▍")
		b.WriteByte(' ')
	default:
		b.WriteString("  ")
	}

	// T+MM:SS.mmm elapsed since process start.
	elapsed := time.Since(startTime)
	writeColored(&b, h.color, ansiDim, formatElapsed(elapsed))
	b.WriteString("  ")

	// Module column (stable color, fixed width).
	mod := pickModule(h.attrs, r)
	if mod == "" {
		mod = "-"
	}
	writeColored(&b, h.color, moduleColor(mod), padRight(mod, moduleColWidth))
	b.WriteByte(' ')

	// Message column, padded for alignment.
	msg := r.Message
	b.WriteString(msg)
	if len(msg) < msgColWidth {
		b.WriteString(strings.Repeat(" ", msgColWidth-len(msg)))
	} else {
		b.WriteByte(' ')
	}

	// Key=value pairs (handler-attached attrs + record attrs, module
	// already consumed). Two-space separator for readability past 4
	// keys.
	writeAttrs(&b, h.color, h.attrs, r)

	b.WriteByte('\n')

	h.mu.Lock()
	_, err := h.w.Write([]byte(b.String()))
	h.mu.Unlock()
	return err
}

func formatElapsed(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	mins := int(d / time.Minute)
	secs := int((d % time.Minute) / time.Second)
	ms := int((d % time.Second) / time.Millisecond)
	return fmt.Sprintf("T+%02d:%02d.%03d", mins, secs, ms)
}

// pickModule returns the "module" attr's string value if present,
// preferring record attrs over handler attrs (matches slog.With
// composition: With("module", "x") then logger.Info("...", "module",
// "y") emits with module=y).
func pickModule(handlerAttrs []slog.Attr, r slog.Record) string {
	mod := ""
	for _, a := range handlerAttrs {
		if a.Key == "module" {
			mod = a.Value.String()
		}
	}
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == "module" {
			mod = a.Value.String()
		}
		return true
	})
	return mod
}

func writeAttrs(b *strings.Builder, color bool, handlerAttrs []slog.Attr, r slog.Record) {
	emit := func(a slog.Attr) {
		if a.Key == "module" || a.Key == "" {
			return
		}
		v := renderValue(a.Key, a.Value.Any())
		v = quoteIfNeeded(v)
		b.WriteString("  ")
		writeColored(b, color, ansiDim, a.Key+"=")
		b.WriteString(v)
	}
	for _, a := range handlerAttrs {
		emit(a)
	}
	r.Attrs(func(a slog.Attr) bool {
		emit(a)
		return true
	})
}

func padRight(s string, n int) string {
	if len(s) >= n {
		return s
	}
	return s + strings.Repeat(" ", n-len(s))
}

// ---- color ----

const (
	ansiReset   = "\x1b[0m"
	ansiDim     = "\x1b[2m"
	ansiRedBg   = "\x1b[31;1m" // bold red glyph
	ansiYellow  = "\x1b[33m"
	ansiCyan    = "\x1b[36m"
	ansiGreen   = "\x1b[32m"
	ansiMagenta = "\x1b[35m"
	ansiBlue    = "\x1b[34m"
	ansiWhite   = "\x1b[37m"
)

// modulePalette is six visually distinct foreground colors. Six is
// enough that collisions are rare for our handful of modules and
// every entry stays distinguishable on common terminal themes.
var modulePalette = []string{
	ansiCyan,
	ansiGreen,
	ansiMagenta,
	ansiYellow,
	ansiBlue,
	ansiWhite,
}

// moduleColor picks a stable color from the palette based on the
// module name. Deterministic: the same module name always lands on
// the same color across runs.
func moduleColor(name string) string {
	if name == "" || name == "-" {
		return ansiDim
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(name))
	return modulePalette[h.Sum32()%uint32(len(modulePalette))]
}

func writeColored(b *strings.Builder, color bool, code, s string) {
	if !color {
		b.WriteString(s)
		return
	}
	b.WriteString(code)
	b.WriteString(s)
	b.WriteString(ansiReset)
}
