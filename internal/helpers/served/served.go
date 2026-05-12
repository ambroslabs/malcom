// Package served maintains a persistent list of peers that have
// served us verified snapshot chunks across runs. It is the positive
// counterpart to banlist: where banlist remembers "do not redial",
// served remembers "prioritize redialing".
//
// The list is keyed by NetAddress.String() (id@ip:port). Each entry
// records first/last seen times and a cumulative chunks count. On
// startup, snapfetch loads the file, prepends entries to the manager's
// dial pool, and Pins each one so the manager actively redials them
// from t=0 — bypassing the multi-minute discovery cycle a fresh PEX
// session would otherwise need to find chunk-servers.
//
// Sort order on disk is newest-first by LastSeenAt so the most-recently
// useful peers are at the head of the list.
package served

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
	DefaultCap    = 5000
	DefaultMaxAge = 30 * 24 * time.Hour
)

// Entry is the on-disk record for one chunk-serving peer.
type Entry struct {
	ID           string    `json:"id"`
	Addr         string    `json:"addr"`
	FirstSeenAt  time.Time `json:"first_seen_at"`
	LastSeenAt   time.Time `json:"last_seen_at"`
	ChunksTotal  int       `json:"chunks_total"`
}

type fileShape struct {
	Entries []Entry `json:"entries"`
}

// Set is a thread-safe, persistent list of chunk-serving peers. All
// methods are nil-safe so callers can pass a nil *Set to disable the
// feature without conditional plumbing.
type Set struct {
	mu      sync.Mutex
	path    string
	cap     int
	maxAge  time.Duration
	entries map[string]Entry // keyed by Addr
}

// New returns a Set bound to path, populated from disk if the file
// exists. A missing file is fine — returns an empty set.
func New(path string) (*Set, error) {
	if path == "" {
		return nil, fmt.Errorf("served list path is empty")
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

func (s *Set) load() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.entries = map[string]Entry{}
	b, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read served file %s: %w", s.path, err)
	}
	var f fileShape
	if err := json.Unmarshal(b, &f); err != nil {
		return fmt.Errorf("parse served file %s: %w", s.path, err)
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
// LastSeenAt.
func (s *Set) Save() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return fmt.Errorf("mkdir served dir: %w", err)
	}
	entries := s.allLocked()
	b, err := json.MarshalIndent(fileShape{Entries: entries}, "", "\t")
	if err != nil {
		return fmt.Errorf("marshal served: %w", err)
	}
	if err := durable.WriteFile(s.path, b, 0o644); err != nil {
		return fmt.Errorf("write served: %w", err)
	}
	return nil
}

// Record bumps the entry for (id, addr): creates a new one with
// FirstSeenAt=now if absent, otherwise updates LastSeenAt and
// increments ChunksTotal. Idempotent and concurrent-safe.
func (s *Set) Record(id, addr string) {
	if s == nil || addr == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	e, ok := s.entries[addr]
	if !ok {
		e = Entry{ID: id, Addr: addr, FirstSeenAt: now}
	}
	e.LastSeenAt = now
	e.ChunksTotal++
	s.entries[addr] = e
	s.enforceCapLocked()
}

// All returns entries sorted newest-first by LastSeenAt. Caller-owned
// copy; safe to mutate.
func (s *Set) All() []Entry {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.allLocked()
}

func (s *Set) allLocked() []Entry {
	out := make([]Entry, 0, len(s.entries))
	for _, e := range s.entries {
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].LastSeenAt.After(out[j].LastSeenAt)
	})
	return out
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
