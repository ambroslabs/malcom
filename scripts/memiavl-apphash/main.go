// Command memiavl-apphash opens a memiavl directory read-only and prints the
// cosmos-sdk app_hash (Merkle root of the commit info) at the loaded version.
//
// Useful for verifying a memiavl-imported snapshot against a public RPC's
// canonical app_hash for the same height *before* starting the node:
//
//	memiavl-apphash --memiavl-db /root/.gaia/data/memiavl.db
//	-> version=31076000 app_hash=DEADBEEF...
//
// Compare to:
//
//	curl 'https://cosmos-rpc.publicnode.com/block?height=31076001' \
//	    | jq -r '.result.block.header.app_hash'
//
// (Note: header.app_hash at height N+1 is the result of finalizing block N,
// so a snapshot taken at height N should match block N+1's header.app_hash.)
//
// Lives under scripts/ with its own go.mod so the cronos-store/memiavl
// dependency stays out of malcom's main module graph — matching the
// existing convention that malcom proper does not import cosmos-sdk.
package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"os"

	"github.com/crypto-org-chain/cronos-store/memiavl"

	storetypes "cosmossdk.io/store/types"
)

func main() {
	var (
		dir         string
		targetVer   uint
		printStores bool
	)
	flag.StringVar(&dir, "memiavl-db", "", "path to the memiavl directory, e.g. /root/.gaia/data/memiavl.db (required)")
	flag.UintVar(&targetVer, "version", 0, "target version to load; 0 means latest")
	flag.BoolVar(&printStores, "stores", false, "also print per-store hash + version")
	flag.Parse()

	if dir == "" {
		fmt.Fprintln(os.Stderr, "usage: memiavl-apphash --memiavl-db <dir> [--version <h>] [--stores]")
		os.Exit(2)
	}

	if err := run(dir, uint32(targetVer), printStores); err != nil {
		fmt.Fprintln(os.Stderr, "memiavl-apphash:", err)
		os.Exit(1)
	}
}

func run(dir string, targetVersion uint32, printStores bool) error {
	db, err := memiavl.Load(dir, memiavl.Options{
		ReadOnly:      true,
		TargetVersion: targetVersion,
	}, "" /* chainId only used when creating new DB */)
	if err != nil {
		return fmt.Errorf("open memiavl: %w", err)
	}
	defer db.Close()

	memCI := db.LastCommitInfo()
	if memCI == nil {
		return fmt.Errorf("no commit info available; db may be empty (version=%d)", db.Version())
	}

	ci := convertCommitInfo(memCI)
	hash := ci.Hash()

	fmt.Printf("version=%d app_hash=%s\n", ci.Version, hex.EncodeToString(hash))

	if printStores {
		for _, si := range ci.StoreInfos {
			fmt.Printf("  %-32s version=%d hash=%s\n", si.Name, si.CommitId.Version, hex.EncodeToString(si.CommitId.Hash))
		}
	}
	return nil
}

// convertCommitInfo mirrors cronos-store/store/rootmulti.convertCommitInfo,
// which is package-private there. Field-for-field copy from memiavl's CommitInfo
// proto type into cosmos-sdk's. The cosmos-sdk CommitInfo.Hash() method computes
// the Merkle root that nodes use as the chain's app_hash.
func convertCommitInfo(ci *memiavl.CommitInfo) *storetypes.CommitInfo {
	storeInfos := make([]storetypes.StoreInfo, len(ci.StoreInfos))
	for i, si := range ci.StoreInfos {
		storeInfos[i] = storetypes.StoreInfo{
			Name: si.Name,
			CommitId: storetypes.CommitID{
				Version: si.CommitId.Version,
				Hash:    si.CommitId.Hash,
			},
		}
	}
	return &storetypes.CommitInfo{
		Version:    ci.Version,
		StoreInfos: storeInfos,
	}
}
