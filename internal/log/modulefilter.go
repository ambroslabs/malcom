package log

import (
	"context"
	"log/slog"
)

// moduleFilter wraps a handler with per-module level overrides.
// Replaces cometbft's cmtlog.NewFilter / AllowDebugWith pattern.
//
// The "module" attr is matched after WithAttrs / WithGroup
// composition: a logger created via slog.With("module", "fetch")
// takes the override for "fetch" if one is registered.
type moduleFilter struct {
	inner    slog.Handler
	defLevel slog.Level
	byModule map[string]slog.Level

	attrs  []slog.Attr
	groups []string
}

func (f *moduleFilter) Enabled(ctx context.Context, l slog.Level) bool {
	// We can't decide here without seeing the record's module attr.
	// Be permissive at Enabled and filter properly in Handle. The
	// inner handler still gates on its own level for events without
	// any module override.
	return l >= f.minLevel()
}

func (f *moduleFilter) minLevel() slog.Level {
	min := f.defLevel
	for _, l := range f.byModule {
		if l < min {
			min = l
		}
	}
	return min
}

func (f *moduleFilter) Handle(ctx context.Context, r slog.Record) error {
	mod := f.recordModule(r)
	threshold := f.defLevel
	if mod != "" {
		if lv, ok := f.byModule[mod]; ok {
			threshold = lv
		}
	}
	if r.Level < threshold {
		return nil
	}
	return f.inner.Handle(ctx, r)
}

func (f *moduleFilter) recordModule(r slog.Record) string {
	mod := ""
	for _, a := range f.attrs {
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

func (f *moduleFilter) WithAttrs(as []slog.Attr) slog.Handler {
	combined := make([]slog.Attr, 0, len(f.attrs)+len(as))
	combined = append(combined, f.attrs...)
	combined = append(combined, as...)
	return &moduleFilter{
		inner:    f.inner.WithAttrs(as),
		defLevel: f.defLevel,
		byModule: f.byModule,
		attrs:    combined,
		groups:   f.groups,
	}
}

func (f *moduleFilter) WithGroup(name string) slog.Handler {
	if name == "" {
		return f
	}
	groups := make([]string, 0, len(f.groups)+1)
	groups = append(groups, f.groups...)
	groups = append(groups, name)
	return &moduleFilter{
		inner:    f.inner.WithGroup(name),
		defLevel: f.defLevel,
		byModule: f.byModule,
		attrs:    f.attrs,
		groups:   groups,
	}
}
