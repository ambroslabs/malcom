package pex

import (
	"log/slog"
	"testing"

	"github.com/cometbft/cometbft/p2p"

	malcomlog "github.com/ambroslabs/malcom/internal/log"
)

func TestNoopReactorAdvertisesChannelZero(t *testing.T) {
	r := NewNoopReactor(malcomlog.CmtShim(slog.New(slog.DiscardHandler)))
	chs := r.GetChannels()
	if len(chs) != 1 {
		t.Fatalf("GetChannels len = %d, want 1", len(chs))
	}
	if chs[0].ID != Channel {
		t.Fatalf("channel ID = 0x%02x, want 0x%02x", chs[0].ID, Channel)
	}
}

// TestNoopReactorReceiveDoesNothing exercises Receive with a synthetic
// envelope to confirm it returns cleanly — the whole point of the
// reactor is that incoming PEX bytes don't tear down the connection
// (the previous behavior with no reactor registered for channel 0x00
// gave "unknown channel 0" on cometbft's recv routine; see #118).
func TestNoopReactorReceiveDoesNothing(t *testing.T) {
	r := NewNoopReactor(malcomlog.CmtShim(slog.New(slog.DiscardHandler)))
	// Empty envelope is enough — Receive must not panic on garbage
	// input, since the whole reason it exists is to silently drop
	// whatever the peer sent.
	r.Receive(p2p.Envelope{ChannelID: Channel})
}

func TestNoopReactorPeerHooksAreNoops(t *testing.T) {
	r := NewNoopReactor(malcomlog.CmtShim(slog.New(slog.DiscardHandler)))
	// These get called by the cometbft switch on every connect /
	// disconnect; the noop reactor must accept them without side
	// effects.
	r.AddPeer(nil)
	r.RemovePeer(nil, "test")
	if got := r.InitPeer(nil); got != nil {
		t.Fatalf("InitPeer should return the peer untouched; got %v", got)
	}
}
