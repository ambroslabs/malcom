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
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/ambroslabs/malcom/internal/config"
	malcomlog "github.com/ambroslabs/malcom/internal/log"
)

// Run is the malcom subcommand entry point.
func Run(args []string) int {
	fs := flag.NewFlagSet("malcom clean", flag.ContinueOnError)
	clobber := fs.Bool("clobber", false, "delete instead of backing up to <dir>.bak.<ts>")
	logMode := fs.String("log", "", "log output: auto (default), pretty, text, json")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	mode, ok := malcomlog.ParseMode(*logMode)
	if !ok {
		fmt.Fprintf(os.Stderr, "invalid -log %q (want auto/pretty/text/json)\n", *logMode)
		return 2
	}
	log := malcomlog.New(malcomlog.Options{
		Writer: os.Stderr, Mode: mode, Level: slog.LevelInfo,
	}).With("module", "clean")

	cfgPath, err := config.DefaultConfigPath()
	if err != nil {
		log.Error("resolve config path", "err", err)
		return 1
	}
	configRoot := filepath.Dir(cfgPath)

	stateDir, err := config.StateDir()
	if err != nil {
		log.Error("resolve state dir", "err", err)
		return 1
	}
	cacheDir, err := config.CacheDir()
	if err != nil {
		log.Error("resolve cache dir", "err", err)
		return 1
	}

	targets := []string{configRoot, stateDir, cacheDir}
	ts := time.Now().UTC().Format("20060102-150405")

	rc := 0
	nothing := true
	for _, p := range targets {
		info, err := os.Stat(p)
		if errors.Is(err, iofs.ErrNotExist) {
			log.Info("not present, skipping", "path", p)
			continue
		}
		if err != nil {
			log.Error("stat failed", "path", p, "err", err)
			rc = 1
			continue
		}
		if !info.IsDir() {
			log.Error("not a directory; refusing to act", "path", p)
			rc = 1
			continue
		}
		nothing = false

		if *clobber {
			if err := os.RemoveAll(p); err != nil {
				log.Error("remove failed", "path", p, "err", err)
				rc = 1
				continue
			}
			log.Info("deleted", "path", p)
			continue
		}

		backup := uniqueBackupName(p, ts)
		if err := os.Rename(p, backup); err != nil {
			log.Error("backup failed", "path", p, "backup", backup, "err", err)
			rc = 1
			continue
		}
		log.Info("backed up", "path", p, "backup", backup)
	}

	if nothing && rc == 0 {
		log.Info("nothing to clean")
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
