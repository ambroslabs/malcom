// Package cleancmd is the `malcom clean` subcommand: wipe malcom's
// state across $XDG_{CONFIG,STATE,CACHE}_HOME/malcom/. Default backs
// up each existing dir to <dir>.bak.<UTC-ts>; -clobber removes them
// outright.
//
// Useful during config-schema iteration: re-test from a clean slate
// without losing hand-edited values (filled-in genesis paths, RPC
// lists) by accident.
package cleancmd

import (
	"errors"
	"flag"
	"fmt"
	iofs "io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/zrbecker/cosmos-p2p/internal/config"
)

// Run is the malcom subcommand entry point.
func Run(args []string) int {
	fs := flag.NewFlagSet("malcom clean", flag.ContinueOnError)
	clobber := fs.Bool("clobber", false, "delete instead of backing up to <dir>.bak.<ts>")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	cfgPath, err := config.DefaultConfigPath()
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolve config path: %v\n", err)
		return 1
	}
	configRoot := filepath.Dir(cfgPath)

	stateDir, err := config.StateDir()
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolve state dir: %v\n", err)
		return 1
	}
	cacheDir, err := config.CacheDir()
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolve cache dir: %v\n", err)
		return 1
	}

	targets := []string{configRoot, stateDir, cacheDir}
	ts := time.Now().UTC().Format("20060102-150405")

	rc := 0
	nothing := true
	for _, p := range targets {
		info, err := os.Stat(p)
		if errors.Is(err, iofs.ErrNotExist) {
			fmt.Printf("[clean] %s: not present, skipping\n", p)
			continue
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "stat %s: %v\n", p, err)
			rc = 1
			continue
		}
		if !info.IsDir() {
			fmt.Fprintf(os.Stderr, "%s is not a directory; refusing to act\n", p)
			rc = 1
			continue
		}
		nothing = false

		if *clobber {
			if err := os.RemoveAll(p); err != nil {
				fmt.Fprintf(os.Stderr, "remove %s: %v\n", p, err)
				rc = 1
				continue
			}
			fmt.Printf("[clean] %s -> deleted\n", p)
			continue
		}

		backup := uniqueBackupName(p, ts)
		if err := os.Rename(p, backup); err != nil {
			fmt.Fprintf(os.Stderr, "backup %s -> %s: %v\n", p, backup, err)
			rc = 1
			continue
		}
		fmt.Printf("[clean] %s -> %s\n", p, backup)
	}

	if nothing && rc == 0 {
		fmt.Println("[clean] nothing to clean")
	}
	return rc
}

// uniqueBackupName returns "<p>.bak.<ts>" — or "<p>.bak.<ts>-N" if
// that already exists (sub-second re-run collision).
func uniqueBackupName(p, ts string) string {
	base := p + ".bak." + ts
	if _, err := os.Stat(base); errors.Is(err, iofs.ErrNotExist) {
		return base
	}
	for i := 1; ; i++ {
		name := fmt.Sprintf("%s-%d", base, i)
		if _, err := os.Stat(name); errors.Is(err, iofs.ErrNotExist) {
			return name
		}
	}
}
