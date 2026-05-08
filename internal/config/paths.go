// XDG Base Directory paths for malcom.
//
// We honour the standard env vars (XDG_CONFIG_HOME, XDG_STATE_HOME,
// XDG_CACHE_HOME). When unset, we fall back to the spec defaults:
// $HOME/.config, $HOME/.local/state, $HOME/.cache. If $HOME isn't
// set either (rare — happens in some sandboxed CI), we error rather
// than guessing — better to surface "no HOME" than to silently write
// to the wrong place.

package config

import (
	"fmt"
	"os"
	"path/filepath"
)

// AppName is the directory name used inside each XDG root.
const AppName = "malcom"

// ConfigDir returns $XDG_CONFIG_HOME/malcom (or $HOME/.config/malcom).
// User-edited config lives here. Default config.toml path is
// ConfigDir() + "/config.toml".
func ConfigDir() (string, error) {
	return xdgDir("XDG_CONFIG_HOME", ".config")
}

// StateDir returns $XDG_STATE_HOME/malcom (or $HOME/.local/state/malcom).
// Persistent runtime state: node keys (security-relevant), session
// markers, etc. Survives reboots; not safe to wipe casually.
func StateDir() (string, error) {
	return xdgDir("XDG_STATE_HOME", filepath.Join(".local", "state"))
}

// CacheDir returns $XDG_CACHE_HOME/malcom (or $HOME/.cache/malcom).
// Regenerable cached files: peer DBs, addrbook downloads. Safe to
// wipe; the next operation will rebuild them.
func CacheDir() (string, error) {
	return xdgDir("XDG_CACHE_HOME", ".cache")
}

// DataDir returns $XDG_DATA_HOME/malcom (or $HOME/.local/share/malcom).
// Read-mostly reference data: chain genesis files, etc. Persistent
// across runs; re-downloadable but expensive enough we don't put it
// in CacheDir.
func DataDir() (string, error) {
	return xdgDir("XDG_DATA_HOME", filepath.Join(".local", "share"))
}

// DefaultConfigPath returns the canonical config file path:
// ConfigDir() + "/config.toml".
func DefaultConfigPath() (string, error) {
	d, err := ConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(d, "config.toml"), nil
}

// RegistryCacheDir returns the on-disk cache root for the
// chain-registry tarball + index. Lives under CacheDir() so it
// participates in `malcom clean`.
func RegistryCacheDir() (string, error) {
	d, err := CacheDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(d, "chain-registry"), nil
}

func xdgDir(envVar, homeRel string) (string, error) {
	if v := os.Getenv(envVar); v != "" {
		return filepath.Join(v, AppName), nil
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return "", fmt.Errorf("$%s unset and $HOME unavailable: %w", envVar, err)
	}
	return filepath.Join(home, homeRel, AppName), nil
}
