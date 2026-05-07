// Package logctx attaches a cometbft-style logger to a context.Context
// so callees can pull it without threading a *logger parameter through
// every signature.
//
// The logger is stored via context.WithValue, so all standard context
// derivations (context.WithTimeout, context.WithCancel, etc.) preserve
// it automatically — no custom WithTimeout helpers needed.
//
// Two usage flavors, freely mixable:
//
//	// Free-function form (signatures stay context.Context):
//	func walk(ctx context.Context) {
//	    log := logctx.From(ctx)
//	    log.Info("walking")
//	}
//
//	// Wrapper form (when ctx.Logger() reads better):
//	func walk(ctx logctx.Context) {
//	    ctx.Logger().Info("walking")
//	}
//
// Scope by deriving a new context — never mutate:
//
//	ctx = logctx.WithFields(ctx, "module", "fetch")
//	doStuff(ctx) // sees module=snapfetch
package logctx

import (
	"context"

	cmtlog "github.com/cometbft/cometbft/libs/log"
)

type ctxKey struct{}

// With returns a context that carries log. nil log → no-op logger.
func With(parent context.Context, log cmtlog.Logger) context.Context {
	if log == nil {
		log = cmtlog.NewNopLogger()
	}
	return context.WithValue(parent, ctxKey{}, log)
}

// From returns the logger attached to ctx, or a no-op logger if none.
// Always safe to call.
func From(ctx context.Context) cmtlog.Logger {
	if v, ok := ctx.Value(ctxKey{}).(cmtlog.Logger); ok {
		return v
	}
	return cmtlog.NewNopLogger()
}

// WithFields returns a context whose logger has the given key/value
// pairs appended (via cmtlog.Logger.With). Equivalent to
// With(parent, From(parent).With(kv...)).
func WithFields(parent context.Context, kv ...any) context.Context {
	return With(parent, From(parent).With(kv...))
}

// Context is a thin wrapper around context.Context that exposes the
// attached logger via a Logger() method. Embedding makes a Context
// value satisfy the context.Context interface, so it can be passed
// to anything that takes context.Context without conversion.
type Context struct {
	context.Context
}

// Wrap turns a plain context.Context into the dot-method-friendly
// Context wrapper. The underlying ctx is unchanged.
func Wrap(ctx context.Context) Context { return Context{ctx} }

// Logger returns the logger attached to the wrapped ctx, or a no-op
// logger if none.
func (c Context) Logger() cmtlog.Logger { return From(c.Context) }

// WithFields is the wrapper-flavored equivalent of the package-level
// WithFields function.
func (c Context) WithFields(kv ...any) Context {
	return Context{WithFields(c.Context, kv...)}
}
