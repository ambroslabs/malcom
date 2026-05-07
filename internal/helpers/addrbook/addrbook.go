// Package addrbook reads cometbft's addrbook.json file format and
// returns a sorted list of seed addresses for the initial dial pool.
//
// This package is a READ-ONLY parser, not a runtime address book. The
// live address book — bucket management, TTL'd bans, on-disk
// persistence — is owned by cometbft's p2p/pex.AddrBook (imported as
// pexcb in our code). We hand-roll a parallel reader of the same JSON
// file because pexcb's public API exposes neither:
//
//	(a) enumerate-all-entries (GetSelection caps at ~250 random entries), nor
//	(b) sort-by-LastSuccess (GetSelection randomizes via Fisher-Yates).
//
// cometbft's Fisher-Yates sampling is the right choice for validator
// nodes: it spreads each node's connections evenly across the known
// network so no peer-set is disproportionately relied on, which is
// what a decentralized chain needs for resilience and eclipse
// resistance. Our goal is the opposite. We are a short-lived snapshot
// fetcher with one objective — maximize the probability of connecting
// to a working peer right now. We don't care about network-wide
// diversity; we just want the historically-good peers first. So we
// read the file directly and prioritize by LastSuccess.
//
// All WRITES to addrbook.json go through pexcb.AddrBook.Save() — never
// this package.
package addrbook

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sort"
	"strings"
	"time"
)

type AddrBook struct {
	Key   string         `json:"key"`
	Addrs []AddrBookItem `json:"addrs"`
}

type AddrBookItem struct {
	Addr struct {
		ID   string `json:"id"`
		IP   string `json:"ip"`
		Port int    `json:"port"`
	} `json:"addr"`
	Buckets     []int     `json:"buckets"`
	LastAttempt time.Time `json:"last_attempt"`
	LastSuccess time.Time `json:"last_success"`
	Attempts    int       `json:"attempts"`
}

// String formats the peer as nodeID@host:port, bracketing IPv6 literals so
// cometbft's NewNetAddressString can parse it.
func (i AddrBookItem) String() string {
	host := i.Addr.IP
	if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	return fmt.Sprintf("%s@%s:%d", i.Addr.ID, host, i.Addr.Port)
}

// IsIPv6 reports whether the address is an IPv6 literal.
func (i AddrBookItem) IsIPv6() bool {
	ip := net.ParseIP(i.Addr.IP)
	return ip != nil && ip.To4() == nil
}

// Load is a one-shot read of a cometbft addrbook.json from disk. It
// does not retain a file handle, manage state, or write back. The
// returned entries are syntactically valid (IPv4 and IPv6) and sorted
// by LastSuccess descending — most-recently-reachable first.
//
// To modify the address book at runtime, use pexcb.AddrBook
// (constructed with pexcb.NewAddrBook). This function is for boot-time
// enumeration only.
func Load(path string) ([]AddrBookItem, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var ab AddrBook
	if err := json.NewDecoder(f).Decode(&ab); err != nil {
		return nil, fmt.Errorf("decode addrbook: %w", err)
	}

	out := make([]AddrBookItem, 0, len(ab.Addrs))
	for _, a := range ab.Addrs {
		if a.Addr.ID == "" || a.Addr.Port == 0 || a.Addr.IP == "" {
			continue
		}
		if net.ParseIP(a.Addr.IP) == nil {
			continue
		}
		out = append(out, a)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].LastSuccess.After(out[j].LastSuccess)
	})
	return out, nil
}

// FreshTop returns up to n addresses with a non-zero LastSuccess.
func FreshTop(items []AddrBookItem, n int) []string {
	out := make([]string, 0, n)
	for _, it := range items {
		if it.LastSuccess.IsZero() {
			continue
		}
		s := it.String()
		if !strings.Contains(s, "@") {
			continue
		}
		out = append(out, s)
		if len(out) >= n {
			break
		}
	}
	return out
}

// All returns every entry in the addrbook (both fresh and stale), formatted
// as nodeID@host:port. Order matches Load (LastSuccess desc).
func All(items []AddrBookItem) []string {
	out := make([]string, 0, len(items))
	for _, it := range items {
		s := it.String()
		if !strings.Contains(s, "@") {
			continue
		}
		out = append(out, s)
	}
	return out
}
