// Package peers loads and ranks peer addresses from a CometBFT-style
// addrbook.json (e.g. the snapshot Polkachu publishes for cosmoshub).
package peers

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

// Load reads a Polkachu-style addrbook.json and returns all syntactically
// valid entries (both IPv4 and IPv6), sorted by LastSuccess descending so
// the most-recently-reachable peers come first.
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
