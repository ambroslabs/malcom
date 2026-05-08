package log

import (
	"bytes"
	"log/slog"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestPrettyHandlerRendersFields(t *testing.T) {
	var buf bytes.Buffer
	logger := New(Options{Writer: &buf, Mode: ModePretty, Level: slog.LevelDebug})

	logger.With("module", "fetch").Info("freshness floor",
		"max_height", uint64(31019000),
		"min_height", uint64(31016000),
		"age", 3000,
	)
	out := buf.String()

	// Module column rendered, key-aware digit grouping applied.
	if !strings.Contains(out, "fetch") {
		t.Errorf("expected module 'fetch' in output, got: %q", out)
	}
	if !strings.Contains(out, "max_height=31_019_000") {
		t.Errorf("expected digit-grouped max_height, got: %q", out)
	}
	if !strings.Contains(out, "min_height=31_016_000") {
		t.Errorf("expected digit-grouped min_height, got: %q", out)
	}
	if !strings.Contains(out, "age=3000") {
		t.Errorf("expected non-grouped age, got: %q", out)
	}
}

func TestPrettyHandlerBytesAndDuration(t *testing.T) {
	var buf bytes.Buffer
	logger := New(Options{Writer: &buf, Mode: ModePretty, Level: slog.LevelDebug})

	logger.Info("download complete",
		"bytes", uint64(2_960_000_000),
		"elapsed", 10*time.Minute+53*time.Second,
	)
	out := buf.String()

	if !strings.Contains(out, "bytes=2.76GiB") {
		t.Errorf("expected human bytes rendering, got: %q", out)
	}
	if !strings.Contains(out, "elapsed=10m53s") {
		t.Errorf("expected duration rendering, got: %q", out)
	}
}

func TestPrettyHandlerTimestampShape(t *testing.T) {
	var buf bytes.Buffer
	logger := New(Options{Writer: &buf, Mode: ModePretty, Level: slog.LevelDebug})

	logger.Info("hi")
	out := buf.String()

	// T+MM:SS.mmm somewhere in the line.
	matched, err := regexp.MatchString(`T\+\d{2}:\d{2}\.\d{3}`, out)
	if err != nil {
		t.Fatal(err)
	}
	if !matched {
		t.Errorf("expected elapsed timestamp shape, got: %q", out)
	}
}

func TestPrettyHandlerLevels(t *testing.T) {
	var buf bytes.Buffer
	logger := New(Options{Writer: &buf, Mode: ModePretty, Level: slog.LevelDebug})

	logger.Debug("dbg")
	logger.Info("inf")
	logger.Warn("wrn")
	logger.Error("err")

	out := buf.String()
	for _, want := range []string{"dbg", "inf", "wrn", "err"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing level message %q in: %s", want, out)
		}
	}
}

func TestModuleFilter(t *testing.T) {
	var buf bytes.Buffer
	logger := New(Options{
		Writer: &buf,
		Mode:   ModeText,
		Level:  slog.LevelError,
		ModuleLevels: map[string]slog.Level{
			"fetch": slog.LevelDebug,
		},
	})

	logger.With("module", "fetch").Debug("fetch debug line")
	logger.With("module", "p2p").Debug("p2p debug line")
	logger.With("module", "p2p").Error("p2p error line")

	out := buf.String()
	if !strings.Contains(out, "fetch debug line") {
		t.Errorf("fetch debug should pass override, got: %q", out)
	}
	if strings.Contains(out, "p2p debug line") {
		t.Errorf("p2p debug should be suppressed by default level, got: %q", out)
	}
	if !strings.Contains(out, "p2p error line") {
		t.Errorf("p2p error should pass default level, got: %q", out)
	}
}

func TestCmtShimRoutesThroughHandler(t *testing.T) {
	var buf bytes.Buffer
	logger := New(Options{Writer: &buf, Mode: ModeText, Level: slog.LevelDebug})
	cmt := CmtShim(logger).With("module", "p2p")
	cmt.Info("peer dialed", "addr", "1.2.3.4:26656")

	out := buf.String()
	if !strings.Contains(out, "module=p2p") {
		t.Errorf("expected module attr on cmt-shim output, got: %q", out)
	}
	if !strings.Contains(out, "peer dialed") {
		t.Errorf("expected message on cmt-shim output, got: %q", out)
	}
	if !strings.Contains(out, `addr=1.2.3.4:26656`) {
		t.Errorf("expected addr attr, got: %q", out)
	}
}

func TestParseMode(t *testing.T) {
	cases := map[string]Mode{
		"":       ModeAuto,
		"auto":   ModeAuto,
		"AUTO":   ModeAuto,
		"pretty": ModePretty,
		"text":   ModeText,
		"json":   ModeJSON,
	}
	for in, want := range cases {
		got, ok := ParseMode(in)
		if !ok || got != want {
			t.Errorf("ParseMode(%q) = (%v, %v); want (%v, true)", in, got, ok, want)
		}
	}
	if _, ok := ParseMode("bogus"); ok {
		t.Errorf("ParseMode bogus value accepted")
	}
}
