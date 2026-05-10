// `malcom snapshot verify-fast --appdb <dir>` — count-only sanity
// check on an already-imported appdb. Reads CommitInfo to discover
// store names + version, then iterates each store's f/ namespace
// and compares the count to the leaves found in the same store's
// s/ namespace. Reports the first mismatch.

package snapshotverifyfast

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/pebble"

	malcomlog "github.com/zrbecker/cosmos-p2p/internal/log"
	"github.com/zrbecker/cosmos-p2p/internal/snapshotimport"
)

func Run(args []string) int {
	fs := flag.NewFlagSet("malcom snapshot verify-fast", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), `usage: malcom snapshot verify-fast --appdb <dir>

Count-only sanity check on an imported appdb. For each store, counts
f/ entries and compares against leaves discovered in the s/ tree.
Catches silent drops/duplicates, not value corruption.`)
		fs.PrintDefaults()
	}
	appdb := fs.String("appdb", "", "path to the imported application.db (= <out>/application.db from `malcom snapshot import`)")
	logMode := fs.String("log", "", "log output: auto (default), pretty, text, json")
	debug := fs.Bool("debug", false, "verbose logging")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *appdb == "" {
		fs.Usage()
		return 2
	}
	mode, ok := malcomlog.ParseMode(*logMode)
	if !ok {
		fmt.Fprintf(os.Stderr, "invalid -log %q (want auto/pretty/text/json)\n", *logMode)
		return 2
	}
	logOpts, err := malcomlog.BuildOptions(malcomlog.Tuning{}, mode, *debug, os.Stderr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "build log options: %v\n", err)
		return 1
	}
	logger := malcomlog.New(logOpts).With("module", "verify-fast-cli")

	t0 := time.Now()
	dbDir := *appdb
	// Accept either the application.db dir directly or its parent (the
	// `<out>` from `malcom snapshot import`).
	if _, err := os.Stat(filepath.Join(dbDir, "CURRENT")); err != nil {
		alt := filepath.Join(dbDir, "application.db")
		if _, err := os.Stat(filepath.Join(alt, "CURRENT")); err == nil {
			dbDir = alt
		}
	}
	db, err := pebble.Open(dbDir, &pebble.Options{ReadOnly: true})
	if err != nil {
		logger.Error("open appdb", "path", dbDir, "err", err)
		return 1
	}
	defer db.Close()

	// Read latest_version → CommitInfo at that version → store names.
	lvBytes, closer, err := db.Get(snapshotimport.LatestVersionKey)
	if err != nil {
		logger.Error("read latest_version", "err", err)
		return 1
	}
	version, err := snapshotimport.DecodeLatestVersion(lvBytes)
	closer.Close()
	if err != nil {
		logger.Error("decode latest_version", "err", err)
		return 1
	}
	ciBytes, closer, err := db.Get(snapshotimport.CommitInfoKey(version))
	if err != nil {
		logger.Error("read commit_info", "version", version, "err", err)
		return 1
	}
	names, err := snapshotimport.DecodeStoreNames(ciBytes)
	closer.Close()
	if err != nil {
		logger.Error("decode commit_info", "err", err)
		return 1
	}
	logger.Info("verify-fast", "appdb", dbDir, "version", version, "stores", len(names))

	// VerifyFast computes leaf counts on-the-fly when LeafCount==0,
	// so we can pass StoreInfo just-named-here.
	stores := make([]snapshotimport.StoreInfo, len(names))
	for i, n := range names {
		stores[i].Name = n
	}
	if err := snapshotimport.VerifyFast(db, stores, logger); err != nil {
		logger.Error("verify-fast failed", "err", err)
		return 1
	}
	logger.Info("verify-fast passed", "elapsed", time.Since(t0).Truncate(time.Millisecond))
	return 0
}
