// malcom is a CLI for managing the cosmos-sdk snapshot→bootstrap
// workflow:
//
//	malcom init                   seed XDG dirs + shared config.toml
//	malcom add <chain-id>         register a chain (chain config + node key)
//	malcom registry refresh       re-fetch the cached chain-registry snapshot
//	malcom snapshot fetch         download a state-sync snapshot
//	malcom snapshot import        snapshot dir → application.db + extensions/
//	malcom snapshot serve         advertise a local snapshot over state-sync P2P
//	malcom bootstrap              assemble a runnable chain home dir
//	malcom bootstrap heal         recover a chain home bricked by a failed first start
//	malcom verify                 check the imported AppHash against a cometbft RPC
//	malcom compact                full-keyspace pebble compaction (post-import slack)
//
// CLI parsing is cobra/pflag. Run `malcom <sub> -h` for per-command flags.
package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/ambroslabs/malcom/internal/cli/addcmd"
	"github.com/ambroslabs/malcom/internal/cli/bootstrap"
	"github.com/ambroslabs/malcom/internal/cli/cleancmd"
	"github.com/ambroslabs/malcom/internal/cli/cliexit"
	"github.com/ambroslabs/malcom/internal/cli/compact"
	"github.com/ambroslabs/malcom/internal/cli/initcmd"
	"github.com/ambroslabs/malcom/internal/cli/registrycmd"
	"github.com/ambroslabs/malcom/internal/cli/snapshotfetch"
	"github.com/ambroslabs/malcom/internal/cli/snapshotimport"
	"github.com/ambroslabs/malcom/internal/cli/snapshotindex"
	"github.com/ambroslabs/malcom/internal/cli/snapshotserve"
	"github.com/ambroslabs/malcom/internal/cli/verify"
)

func main() {
	root := newRoot()
	err := root.Execute()
	if err == nil {
		return
	}
	var ec *cliexit.Error
	if errors.As(err, &ec) {
		os.Exit(ec.Code)
	}
	// Argument / flag parse error or other unexpected — cobra has
	// already silenced its own "Error: ..." printing (SilenceErrors),
	// so surface it here and exit 2 to match the pre-cobra usage-
	// error convention.
	fmt.Fprintln(os.Stderr, "malcom:", err)
	os.Exit(2)
}

func newRoot() *cobra.Command {
	root := &cobra.Command{
		Use:           "malcom",
		Short:         "cosmos-sdk chain bootstrap toolkit",
		Long:          "malcom is a CLI for managing the cosmos-sdk snapshot→bootstrap workflow.",
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	// Group: snapshot (fetch / import / index / serve).
	snapshotGroup := &cobra.Command{
		Use:   "snapshot",
		Short: "snapshot lifecycle subcommands",
		Long:  "Subcommands managing snapshot fetch / import / index / serve.",
	}
	snapshotGroup.AddCommand(snapshotfetch.NewCmd())
	snapshotGroup.AddCommand(snapshotimport.NewCmd())
	snapshotGroup.AddCommand(snapshotindex.NewCmd())
	snapshotGroup.AddCommand(snapshotserve.NewCmd())

	// bootstrap is a command that also has `heal` as a subcommand.
	bootstrapCmd := bootstrap.NewCmd()
	bootstrapCmd.AddCommand(bootstrap.NewHealCmd())

	root.AddCommand(
		initcmd.NewCmd(),
		addcmd.NewCmd(),
		registrycmd.NewCmd(),
		cleancmd.NewCmd(),
		snapshotGroup,
		bootstrapCmd,
		verify.NewCmd(),
		compact.NewCmd(),
	)
	return root
}
