package snapfetch

import "testing"

// buildP2PConfig is the only choke point that can silently regress
// the AllowDuplicateIP default — a hardcoded literal here was the
// original threat-model concession. Pin both branches.
func TestBuildP2PConfigAllowDuplicateIP(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   bool
	}{
		{"false", false},
		{"true", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := buildP2PConfig(64, tc.in)
			if p.AllowDuplicateIP != tc.in {
				t.Fatalf("AllowDuplicateIP = %v, want %v", p.AllowDuplicateIP, tc.in)
			}
			if p.MaxNumOutboundPeers != 64 {
				t.Fatalf("MaxNumOutboundPeers = %d, want 64", p.MaxNumOutboundPeers)
			}
		})
	}
}
