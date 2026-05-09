// malcom is the cosmos-p2p-toolkit CLI for managing the gaiad
// snapshot→bootstrap workflow:
//
//	malcom init               seed XDG dirs + shared config.toml
//	malcom add <chain-id>     register a chain (chain config + node key)
//	malcom registry refresh   re-fetch the cached chain-registry snapshot
//	malcom snapshot fetch     download a state-sync snapshot
//	malcom snapshot import    snapshot dir → application.db + extensions/
//	malcom bootstrap          assemble a runnable gaiad home dir
//	malcom verify             check the imported AppHash against a cometbft RPC
//	malcom compact            full-keyspace pebble compaction (post-import slack)
//
// Each subcommand owns its own flag set; see `malcom <sub> -h` for
// per-command flags. malcom itself does not parse global flags.
package main

import (
	"fmt"
	"os"

	"github.com/zrbecker/cosmos-p2p/internal/cli/addcmd"
	"github.com/zrbecker/cosmos-p2p/internal/cli/bootstrap"
	"github.com/zrbecker/cosmos-p2p/internal/cli/cleancmd"
	"github.com/zrbecker/cosmos-p2p/internal/cli/compact"
	"github.com/zrbecker/cosmos-p2p/internal/cli/initcmd"
	"github.com/zrbecker/cosmos-p2p/internal/cli/registrycmd"
	"github.com/zrbecker/cosmos-p2p/internal/cli/snapshotfetch"
	"github.com/zrbecker/cosmos-p2p/internal/cli/snapshotimport"
	"github.com/zrbecker/cosmos-p2p/internal/cli/snapshotindex"
	"github.com/zrbecker/cosmos-p2p/internal/cli/verify"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	rest := os.Args[2:]
	switch os.Args[1] {
	case "init":
		os.Exit(initcmd.Run(rest))
	case "add":
		os.Exit(addcmd.Run(rest))
	case "registry":
		os.Exit(registrycmd.Run(rest))
	case "clean":
		os.Exit(cleancmd.Run(rest))
	case "snapshot":
		os.Exit(snapshotDispatch(rest))
	case "bootstrap":
		os.Exit(bootstrap.Run(rest))
	case "verify":
		os.Exit(verify.Run(rest))
	case "compact":
		os.Exit(compact.Run(rest))
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "malcom: unknown command %q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}
}

func snapshotDispatch(args []string) int {
	if len(args) < 1 {
		snapshotUsage()
		return 2
	}
	rest := args[1:]
	switch args[0] {
	case "fetch":
		return snapshotfetch.Run(rest)
	case "import":
		return snapshotimport.Run(rest)
	case "index":
		return snapshotindex.Run(rest)
	case "-h", "--help", "help":
		snapshotUsage()
		return 0
	default:
		fmt.Fprintf(os.Stderr, "malcom snapshot: unknown subcommand %q\n\n", args[0])
		snapshotUsage()
		return 2
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `malcom — gaiad bootstrap toolkit

usage: malcom <command> [args...]

commands:
  init               seed XDG dirs + shared config.toml
  add <chain-id>     register a chain (chain config + node key + per-chain dirs)
  registry refresh   re-fetch the cached chain-registry snapshot
  clean              back up (or -clobber) the XDG malcom dirs
  snapshot fetch     download a state-sync snapshot
  snapshot import    convert a snapshot dir into application.db + extensions/
  bootstrap          assemble a runnable gaiad home directory
  verify             check the imported AppHash against a cometbft RPC
  compact            full-keyspace pebble compaction (reclaim slack post-import)

run 'malcom <command> -h' for command-specific flags.`)
}

func snapshotUsage() {
	fmt.Fprintln(os.Stderr, `malcom snapshot — snapshot lifecycle subcommands

usage: malcom snapshot <subcommand> [args...]

subcommands:
  fetch    download a snapshot via state-sync P2P
  import   convert a downloaded snapshot dir into application.db + extensions/`)
}
