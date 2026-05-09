package logctx

import (
	"context"
	"log/slog"
	"testing"
)

func TestFromReturnsDiscardWhenNotAttached(t *testing.T) {
	log := From(context.Background())
	if log == nil {
		t.Fatalf("From(empty ctx) = nil, want discard logger")
	}
	// Calling on the discard logger should not panic.
	log.Info("hi")
	log.Debug("hi")
	log.Error("hi")
}

func TestWithAttachesLogger(t *testing.T) {
	original := slog.New(slog.DiscardHandler)
	ctx := With(context.Background(), original)
	got := From(ctx)
	if got != original {
		t.Fatalf("From(With(ctx, log)) returned a different logger")
	}
}

func TestWithNilLoggerYieldsDiscardLogger(t *testing.T) {
	ctx := With(context.Background(), nil)
	got := From(ctx)
	if got == nil {
		t.Fatalf("With(nil) → From returned nil; want discard logger")
	}
	got.Info("safe to call")
}

func TestWithFieldsChains(t *testing.T) {
	original := slog.New(slog.DiscardHandler)
	ctx := With(context.Background(), original)
	ctx = WithFields(ctx, "module", "fetch", "phase", "walk")
	got := From(ctx)
	if got == nil {
		t.Fatalf("From after WithFields returned nil")
	}
	got.Info("hi")
}

func TestWrapAndContextValuesSurvive(t *testing.T) {
	original := slog.New(slog.DiscardHandler)
	ctx := With(context.Background(), original)
	wrapped := Wrap(ctx)
	if wrapped.Logger() == nil {
		t.Fatalf("Wrap(ctx).Logger() = nil")
	}
	if wrapped.Logger() != original {
		t.Fatalf("Wrap loses logger identity")
	}
}

func TestContextWithCancelPreservesLogger(t *testing.T) {
	original := slog.New(slog.DiscardHandler)
	ctx := With(context.Background(), original)
	derived, cancel := context.WithCancel(ctx)
	defer cancel()
	if From(derived) != original {
		t.Fatalf("derived ctx lost logger")
	}
}

func TestWrapWithFieldsReturnsWrapper(t *testing.T) {
	original := slog.New(slog.DiscardHandler)
	wrapped := Wrap(With(context.Background(), original))
	scoped := wrapped.WithFields("k", "v")
	if scoped.Logger() == nil {
		t.Fatalf("scoped.Logger() = nil")
	}
}
