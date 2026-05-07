package addrbook

import (
	"github.com/cometbft/cometbft/p2p"
	pexcb "github.com/cometbft/cometbft/p2p/pex"
)

// Source labels record where a PeerAddr came from. Populate applies
// different banlist semantics per source.
const (
	SourceBootstrap = "bootstrap" // user-supplied (chain-registry, CSV bootstrap_peers)
	SourceAddrbook  = "addrbook"  // loaded from a prior addrbook.json
)

// PeerAddr is a candidate dial address with its provenance. NOT a
// connection — just an endpoint we might try.
type PeerAddr struct {
	Addr   string
	Source string
}

// Banlist is the subset of *internal/helpers/banlist.Set methods that
// Populate uses. Defined as an interface so this package stays free
// of a hard dependency on the banlist package.
type Banlist interface {
	Has(addr string) bool
	Remove(addr string)
}

// PopulateResult reports the outcome of a Populate call.
type PopulateResult struct {
	Added         int
	SkippedBanned int
}

// Populate seeds the runtime addrbook with the supplied addrs. Per
// source, banlist semantics differ:
//
//   - SourceBootstrap: trusted (user explicitly named them). Any prior
//     banlist entry is cleared (explicit re-allow), then added.
//   - SourceAddrbook: filtered against bans — entries in the banlist
//     are skipped (counted in SkippedBanned).
//
// Each successful add uses src=na ("the peer told us about itself").
// NetAddress parse failures are silently skipped. bans may be nil to
// disable banlist behavior entirely.
func Populate(book pexcb.AddrBook, bans Banlist, addrs []PeerAddr) PopulateResult {
	var r PopulateResult
	for _, p := range addrs {
		switch p.Source {
		case SourceAddrbook:
			if bans != nil && bans.Has(p.Addr) {
				r.SkippedBanned++
				continue
			}
		case SourceBootstrap:
			if bans != nil {
				bans.Remove(p.Addr)
			}
		}
		na, err := p2p.NewNetAddressString(p.Addr)
		if err != nil {
			continue
		}
		if err := book.AddAddress(na, na); err == nil {
			r.Added++
		}
	}
	return r
}
