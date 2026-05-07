package logctx

import (
	"context"
	"testing"

	cmtlog "github.com/cometbft/cometbft/libs/log"
)

func TestFromReturnsNopWhenNotAttached(t *testing.T) {
	log := From(context.Background())
	if log == nil {
		t.Fatalf("From(empty ctx) = nil, want no-op logger")
	}
	// Calling on the no-op should not panic.
	log.Info("hi")
	log.Debug("hi")
	log.Error("hi")
}

func TestWithAttachesLogger(t *testing.T) {
	original := cmtlog.NewNopLogger()
	ctx := With(context.Background(), original)
	got := From(ctx)
	if got != original {
		t.Fatalf("From(With(ctx, log)) returned a different logger")
	}
}

func TestWithNilLoggerYieldsNopLogger(t *testing.T) {
	ctx := With(context.Background(), nil)
	got := From(ctx)
	if got == nil {
		t.Fatalf("With(nil) → From returned nil; want no-op logger")
	}
	got.Info("safe to call")
}

func TestWithFieldsChains(t *testing.T) {
	original := cmtlog.NewNopLogger()
	ctx := With(context.Background(), original)
	ctx = WithFields(ctx, "module", "fetch", "phase", "walk")
	got := From(ctx)
	if got == nil {
		t.Fatalf("From after WithFields returned nil")
	}
	// We can't introspect cometbft logger keyvals without reflection;
	// at minimum confirm no panic and the type is preserved.
	got.Info("hi")
}

func TestWrapAndContextValuesSurvive(t *testing.T) {
	original := cmtlog.NewNopLogger()
	ctx := With(context.Background(), original)
	wrapped := Wrap(ctx)
	if wrapped.Logger() == nil {
		t.Fatalf("Wrap(ctx).Logger() = nil")
	}
	if wrapped.Logger() != original {
		t.Fatalf("Wrap loses logger identity")
	}
}

func TestContextWithTimeoutPreservesLogger(t *testing.T) {
	original := cmtlog.NewNopLogger()
	ctx := With(context.Background(), original)
	timeoutCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if From(timeoutCtx) != original {
		t.Fatalf("derived ctx lost logger")
	}
}

func TestWrapWithFieldsReturnsWrapper(t *testing.T) {
	original := cmtlog.NewNopLogger()
	wrapped := Wrap(With(context.Background(), original))
	scoped := wrapped.WithFields("k", "v")
	if scoped.Logger() == nil {
		t.Fatalf("scoped.Logger() = nil")
	}
}
