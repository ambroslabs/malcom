package snapserve

import (
	"testing"

	"github.com/cometbft/cometbft/p2p/conn"
)

// buildMConnConfig must inherit cometbft's DefaultMConnConfig packet
// size (1024) when given 0, and override only when given a positive
// value. The previous hardcoded 256 KiB broke interop with every
// stock cometbft peer — see #122.
func TestBuildMConnConfigPacketSize(t *testing.T) {
	defaultPacket := conn.DefaultMConnConfig().MaxPacketMsgPayloadSize
	if defaultPacket != 1024 {
		t.Fatalf("cometbft DefaultMConnConfig.MaxPacketMsgPayloadSize = %d, want 1024; the assumption this test pins has changed",
			defaultPacket)
	}

	t.Run("zero inherits cometbft default 1024", func(t *testing.T) {
		got := buildMConnConfig(0).MaxPacketMsgPayloadSize
		if got != 1024 {
			t.Fatalf("MaxPacketMsgPayloadSize = %d, want 1024 (cometbft default)", got)
		}
	})

	t.Run("positive value overrides", func(t *testing.T) {
		got := buildMConnConfig(64 * 1024).MaxPacketMsgPayloadSize
		if got != 64*1024 {
			t.Fatalf("MaxPacketMsgPayloadSize = %d, want %d", got, 64*1024)
		}
	})
}
