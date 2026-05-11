package snapserve

import (
	"testing"
	"time"
)

// TestApplyDefaults_PersistAndDrainPreserveZero pins down the contract
// behind PR #94's review feedback: 0 means "off" for both
// PersistInterval and ShutdownDrain. A prior revision had
// applyDefaults override 0 → non-zero, which silently negated the
// `-shutdown-drain 0` CLI escape hatch for fast dev restarts.
//
// The CLI advertises 0-as-disabled in its help text; this test makes
// applyDefaults match that contract.
func TestApplyDefaults_PersistAndDrainPreserveZero(t *testing.T) {
	c := Config{
		PersistInterval: 0,
		ShutdownDrain:   0,
	}
	c.applyDefaults()
	if c.PersistInterval != 0 {
		t.Fatalf("PersistInterval: applyDefaults clobbered 0 → %v; 0 must mean off",
			c.PersistInterval)
	}
	if c.ShutdownDrain != 0 {
		t.Fatalf("ShutdownDrain: applyDefaults clobbered 0 → %v; 0 must mean off",
			c.ShutdownDrain)
	}
}

// TestApplyDefaults_PersistAndDrainPassNonZeroThrough confirms a
// non-zero caller value is left alone. The CLI flag's own default
// (5m / 30s) reaches Config via the flag parser, not via this
// function, so applyDefaults is now strictly a non-mutating identity
// for these two fields. Worth pinning so the next refactor doesn't
// quietly re-introduce a 0-clobber.
func TestApplyDefaults_PersistAndDrainPassNonZeroThrough(t *testing.T) {
	c := Config{
		PersistInterval: 7 * time.Minute,
		ShutdownDrain:   42 * time.Second,
	}
	c.applyDefaults()
	if c.PersistInterval != 7*time.Minute {
		t.Fatalf("PersistInterval clobbered: got %v, want 7m", c.PersistInterval)
	}
	if c.ShutdownDrain != 42*time.Second {
		t.Fatalf("ShutdownDrain clobbered: got %v, want 42s", c.ShutdownDrain)
	}
}

// TestApplyDefaults_MaxRedialsPreserveZero is the symmetric guard for
// #80: serve must default to 0 (unlimited at the connect.Manager
// level), not silently re-acquire fetch's 5-retry cap.
func TestApplyDefaults_MaxRedialsPreserveZero(t *testing.T) {
	c := Config{MaxRedials: 0}
	c.applyDefaults()
	if c.MaxRedials != 0 {
		t.Fatalf("MaxRedials: applyDefaults clobbered 0 → %d; 0 must mean unlimited",
			c.MaxRedials)
	}
}
