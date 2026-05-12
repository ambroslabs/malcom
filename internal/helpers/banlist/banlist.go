// Package banlist maintains a persistent set of peer addresses that
// have been verified unreachable and should not be redialed across
// restarts.
//
// The set complements cometbft's addrbook, which has no cross-run
// memory of banned peers — addrbook.json only persists peers in
// addrLookup, and MarkBad'd / RemoveAddress'd entries vanish on save.
// So when PEX re-gossips a permanently-unreachable address, a fresh
// process re-learns it's bad from scratch. This package mimics the in-memory
// ban behavior cometbft has at runtime, but persistently across runs.
//
// Entries are keyed by NetAddress.String() (id@ip:port). A peer that
// changes IP gets a fresh chance, since the (id, ip:port) tuple no
// longer matches. Entries auto-prune on load if older than MaxAge so a
// long-offline peer occasionally gets retried in case its operator
// brought it back. The set is also capped at Cap entries; the oldest
// (by LastSeenAt) are evicted when over.
package banlist

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/ambroslabs/malcom/internal/durable"
)

const (
	DefaultCap    = 50000
	DefaultMaxAge = 30 * 24 * time.Hour
)

// Entry is the on-disk record for one banned peer address.
type Entry struct {
	Addr        string    `json:"addr"`
	FirstSeenAt time.Time `json:"first_seen_at"`
	LastSeenAt  time.Time `json:"last_seen_at"`
	Reason      string    `json:"reason"`
	FailCount   int       `json:"fail_count"`
}

type fileShape struct {
	Entries []Entry `json:"entries"`
}

// Set is a thread-safe, persistent banlist of peer addresses. All
// methods are nil-safe so callers can pass a nil *Set to disable the
// feature without conditional plumbing.
type Set struct {
	mu      sync.Mutex
	path    string
	cap     int
	maxAge  time.Duration
	entries map[string]Entry
}

// New returns a Set bound to path, populated from disk if the file
// exists. A missing file is fine — returns an empty set (we'll write
// one on Save). Errors on empty path or unreadable/malformed file:
// callers should treat those as configuration errors and surface them
// rather than silently rebuild from scratch.
//
// Save writes the current state back atomically and MkdirAll's the
// parent dir on demand, so callers don't need to pre-create it.
func New(path string) (*Set, error) {
	if path == "" {
		return nil, fmt.Errorf("banlist path is empty")
	}
	s := &Set{
		path:    path,
		cap:     DefaultCap,
		maxAge:  DefaultMaxAge,
		entries: map[string]Entry{},
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

// load replaces the in-memory set with the contents of path, dropping
// entries older than MaxAge and enforcing Cap. Returns nil (and leaves
// the set empty) if the file does not exist.
func (s *Set) load() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.entries = map[string]Entry{}
	b, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read banlist file %s: %w", s.path, err)
	}
	var f fileShape
	if err := json.Unmarshal(b, &f); err != nil {
		return fmt.Errorf("parse banlist file %s: %w", s.path, err)
	}
	cutoff := time.Now().Add(-s.maxAge)
	for _, e := range f.Entries {
		if e.Addr == "" {
			continue
		}
		if e.LastSeenAt.Before(cutoff) {
			continue
		}
		s.entries[e.Addr] = e
	}
	s.enforceCapLocked()
	return nil
}

// Save atomically writes the set to disk, sorted newest-first by
// LastSeenAt for stable diffs. Creates parent dirs as needed.
func (s *Set) Save() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return fmt.Errorf("mkdir banlist dir: %w", err)
	}
	entries := make([]Entry, 0, len(s.entries))
	for _, e := range s.entries {
		entries = append(entries, e)
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].LastSeenAt.After(entries[j].LastSeenAt)
	})
	b, err := json.MarshalIndent(fileShape{Entries: entries}, "", "\t")
	if err != nil {
		return fmt.Errorf("marshal banlist: %w", err)
	}
	if err := durable.WriteFile(s.path, b, 0o644); err != nil {
		return fmt.Errorf("write banlist: %w", err)
	}
	return nil
}

// Has reports whether addr is in the set.
func (s *Set) Has(addr string) bool {
	if s == nil || addr == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.entries[addr]
	return ok
}

// Add records addr as banned. New entries get FirstSeenAt = now;
// existing entries bump FailCount. LastSeenAt is always set to now
// and Reason is updated when non-empty.
func (s *Set) Add(addr, reason string) {
	if s == nil || addr == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	e, ok := s.entries[addr]
	if !ok {
		e = Entry{Addr: addr, FirstSeenAt: now}
	}
	e.LastSeenAt = now
	e.FailCount++
	if reason != "" {
		e.Reason = reason
	}
	s.entries[addr] = e
	s.enforceCapLocked()
}

// Remove drops addr from the set if present. Used when a caller has
// fresh evidence the address is alive (e.g. an explicit bootstrap-seed
// override).
func (s *Set) Remove(addr string) {
	if s == nil || addr == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.entries, addr)
}

// Len returns the current entry count.
func (s *Set) Len() int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.entries)
}

// Path returns the on-disk path bound to this set.
func (s *Set) Path() string {
	if s == nil {
		return ""
	}
	return s.path
}

func (s *Set) enforceCapLocked() {
	if s.cap <= 0 || len(s.entries) <= s.cap {
		return
	}
	type kv struct {
		k string
		t time.Time
	}
	arr := make([]kv, 0, len(s.entries))
	for k, e := range s.entries {
		arr = append(arr, kv{k: k, t: e.LastSeenAt})
	}
	sort.Slice(arr, func(i, j int) bool { return arr[i].t.Before(arr[j].t) })
	drop := len(s.entries) - s.cap
	for i := 0; i < drop; i++ {
		delete(s.entries, arr[i].k)
	}
}
