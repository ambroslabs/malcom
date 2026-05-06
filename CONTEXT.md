# cosmos-p2p — session context

A running notebook for what we've built, tried, and learned in this thread,
so we can pick up on a fresh machine without losing the thread.

## What this repo does

A cosmoshub-4 ingestion pipeline. The maintained core flow lives in
`cmd/malcom/` as a single CLI; legacy / research / diagnostic
binaries live in `experimental/cmd/`.

**Subcommands:**

- `init` — seed XDG dirs (config.toml + chains/<id>.toml), generate
  p2p node key, populate rpcs/peers from cosmos chain-registry.
  Idempotent; `-force` overwrites templates (never the node key).
  `-offline` skips the registry fetch.
- `clean` — wipe the three malcom XDG roots
  ($XDG_{CONFIG,STATE,CACHE}_HOME/malcom). Backup-by-default (renames
  to `<dir>.bak.<ts>`); `-clobber` for destructive remove. Iteration
  helper.
- `snapshot fetch` — discover and download a state-sync snapshot.
  Walks back from `floor(currentHeight / interval) * interval` (also
  jumps up if a peer offers a fresher one). Custom PEX reactor on
  cometbft AddrBook with active churn for non-snapshot-serving peers.
- `snapshot import` — read chunks, write gaiad-compatible
  `application.db/` (pebble) + `extensions/`. Single-goroutine
  stack-based importer (no iavl, no wave-parallel — OOMed on 2 vCPUs).
  Pre-populates IAVL fast-storage inline so gaiad skips its
  `upgradeToFastStorageGc1_1_0` pass on first start.
- `bootstrap` — assemble runnable gaia home: offline state-sync via
  cometbft, copy application.db, place wasm bytecode for cosmwasm +
  08-light-client, write minimal app.toml/config.toml/client.toml.
  Genesis is downloaded lazily from the URL in chain config if not
  already cached.
- `verify` — read CommitInfo from imported `application.db`, compute
  local AppHash, fetch consensus AppHash from cometbft RPC at H+1,
  match/mismatch.
- `compact` — full-keyspace pebble compaction to reclaim slack
  outside the import flow.

## Config model (XDG, no `-config` flag)

malcom respects the XDG Base Directory Specification end-to-end:

    $XDG_CONFIG_HOME/malcom/config.toml          shared defaults: default_chain, [fetch], [import], [bootstrap]
    $XDG_CONFIG_HOME/malcom/chains/<id>.toml     per-chain identity (chain_id, genesis URL, rpcs, bootstrap_peers)
    $XDG_STATE_HOME/malcom/<id>/node_key.json    cometbft p2p ed25519 identity
    $XDG_CACHE_HOME/malcom/<id>/                 peer DB, addrbook
    $XDG_DATA_HOME/malcom/<id>/                  genesis cache

There is no `-config <path>` flag. To isolate a tree:

- Different config: `XDG_CONFIG_HOME=/tmp/foo malcom …`
- Whole footprint: `env -u XDG_CONFIG_HOME -u XDG_STATE_HOME -u XDG_CACHE_HOME -u XDG_DATA_HOME HOME=/tmp/foo malcom …`
  (XDG defaults all derive from `$HOME` when unset; the spec deliberately doesn't define a single `XDG_HOME`).

`config.Load()` reads `$XDG_CONFIG_HOME/malcom/config.toml`, sets
`chainsDir = <its dir>/chains`, and on missing file returns
"run `malcom init` to create one". `cfg.Resolve(name)` then layers
chains/<id>.toml on top of the global defaults to produce a fully
populated `Chain`.

## CLI flag conventions

CLI flags that have config counterparts (`-chain`, `-max-age`, `-rpc`)
use the **sentinel pattern**: register with the type's zero value,
and resolution treats `""` / `0` as "fall back to config." This
suppresses stdlib's auto `(default …)` line. The real default goes
in the description string via `fmt.Sprintf` against
`config.DefaultChainID` and `config.DefaultMaxAgeBlocks`, e.g.:

    -chain string
        chain id (default "cosmoshub-4"; override in config.default_chain)
    -max-age uint
        freshness floor in blocks (default 3000; override in config.fetch.max_age_blocks)

This avoids the chicken-egg of "show resolved config in help" while
keeping a single source of truth for the value (one constant, used
both as the description text and as the fallback in
`applyFetchDefaults`). No `seen` map / `fs.Visit` machinery needed.

We considered viper/kong/cobra — none solve the help-text issue
either; they all show the registered flag default, not the resolved
post-config value.

**Block archive (experimental, stable):**

- `experimental/cmd/cosmos-archive download` peers up via cometbft p2p,
  fetches missing block heights into `<archive>/shards/`, writes a
  CRC-checked store.
- `experimental/cmd/cosmos-archive {ranges,missing,stats,status,verify,
  fsck,verify-chain,verify-genesis,verify-anchor}` query/audit the
  archive.
- The archive pebble store sits at `/mnt/data/cosmos-archive/cosmoshub-4/`.

## End-to-end numbers (cosmoshub-4 height 30,950,000, 2 vCPU box)

| Phase | Time |
|---|---|
| Snapshot fetch (download + inspect) | ~10 min |
| Snapshot import | ~10 min |
| Bootstrap (light client + place) | ~5 s |
| gaiad blocksync to live tip | ~6 min |
| **Total: snapshot to live tip** | **~27 min** |

AppHash at 30,950,000: `0F22F949B584C1C0ABBDB1A045C5B02B46151675FD64C4ACD4A208B937C9DDB5`. Verified against polkachu/publicnode RPC. Independent test command:

    ./build/malcom verify -appdb /mnt/data/cosmos-archive/appdb-out/30950000 -height 30950000

## Hardware on this 2-vCPU box

- Intel Xeon Gold 6548N, **2 vCPUs**, 8 GB RAM, 8 GB swap.
- Single 1 TB ext4 volume on /dev/sda (cloud volume; reports rotational but probably SSD-backed).

## What lives where

- Repo: `/mnt/data/home/zrbecker/code/cosmos-p2p-toolkit/`
- gaia v27.2.0 source: `/mnt/data/home/zrbecker/code/refs/gaia-v27.2.0/`
- gaiad binary: `/mnt/data/home/zrbecker/bin/gaiad-v27.2.0-linux-amd64` (sha256 `8c086f59…ce3f`, official prebuilt; `bin/gaiad → that`).
- Genesis: `/mnt/data/home/zrbecker/genesis.cosmoshub-4.json`
- Snapshot: `/mnt/data/cosmos-archive/cosmoshub-4/snapshots/30950000_3/` (2.76 GB compressed, format 3)
- Imported appdb: `/mnt/data/cosmos-archive/appdb-out/30950000/application.db/` (~13.55 GB after cleanup)
- gaia bootstrap: `/mnt/data/cosmos-archive/gaia-bootstrap/30950000/`
- Polkachu addrbook: `https://snapshots.polkachu.com/addrbook/cosmos/addrbook.json` — drop into `<home>/config/addrbook.json` while gaiad is stopped, then start.
- Go toolchain: `/mnt/data/home/zrbecker/sdk/go/bin/go` (1.23.4). `go` is NOT on default PATH — use the absolute path.

## Builds without CGO

Cosmos SDK / cometbft pull in `github.com/supranational/blst` and
`github.com/herumi/bls-eth-go-binary` for BLS12-381 keys (added in
SDK v0.50+). Both are CGO-only — no pure-Go fallback. So gaiad
cannot be built with `CGO_ENABLED=0` on a host without gcc. Our
runtime uses the **official prebuilt** `gaiad-v27.2.0-linux-amd64`
which works regardless: cometbft-db v1.0+ registers pebble without a
build tag, so `db_backend = "pebbledb"` in app.toml is honored even
though the prebuilt was compiled with `netgo,ledger,static_wasm`.

`malcom` itself has no CGO requirement (pebble + cometbft v0.38 + a
hand-rolled snapshot importer).

## Useful commands cheat sheet

    # build
    go build -o build/malcom ./cmd/malcom

    # one-time setup: writes config.toml + chains/cosmoshub-4.toml
    # (rpcs + peers populated from cosmos chain-registry).
    ./build/malcom init

    # full pipeline (chain + tuning come from config; only run-instance
    # paths/heights are flags).
    ./build/malcom snapshot fetch -out /mnt/data/cosmos-archive/cosmoshub-4/snapshots
    ./build/malcom snapshot import \
        -snapshot /mnt/data/cosmos-archive/cosmoshub-4/snapshots/snapshot_cosmoshub-4_<H> \
        -out      /mnt/data/cosmos-archive/appdb-out \
        -height   <H>
    ./build/malcom bootstrap \
        -appdb    /mnt/data/cosmos-archive/appdb-out/appdb_cosmoshub-4_<H> \
        -out      /mnt/data/cosmos-archive/gaia-bootstrap \
        -height   <H>
    cp /tmp/polkachu-addrbook.json \
        /mnt/data/cosmos-archive/gaia-bootstrap/gaia_cosmoshub-4_<H>/config/addrbook.json
    nohup /mnt/data/home/zrbecker/bin/gaiad start \
        --home /mnt/data/cosmos-archive/gaia-bootstrap/gaia_cosmoshub-4_<H> \
        </dev/null \
        >/tmp/gaiad-<H>.log 2>&1 &

    # AppHash sanity-check
    ./build/malcom verify \
        -appdb /mnt/data/cosmos-archive/appdb-out/appdb_cosmoshub-4_<H> \
        -height <H>

    # pebble manual compaction (slack reclaim outside the import flow)
    ./build/malcom compact -dir /path/to/application.db

    # iteration: reset the malcom XDG tree (keeps backups by default)
    ./build/malcom clean
    ./build/malcom clean -clobber   # destructive, no backup

## Open follow-ons

- **Proto regeneration** for the snapshot wire types
  (`internal/snapshotimport/snapproto.go`). The hand-rolled decoder
  was a step too far on the "zero cosmos deps" goal; regen from
  copied + simplified `cosmos-sdk/snapshots/types/*.proto`.
- Snapshot fetch indexing: pre-decompress per-store byte ranges so
  multiple readers can decompress in parallel. Today the zlib stream
  is single-threaded.
- `experimental/cmd/cosmos-rapid-bootstrap` rework. Currently a
  serial fetch → import → bootstrap → start orchestrator using the
  in-tree CLI packages; the pipelined fetch+import overlap from the
  old wave-parallel path is gone. Refresh design or fold into a
  `malcom run` subcommand.
- `malcom config show` (long-term, only if config knobs proliferate):
  prints resolved values after layering. The kubectl/git pattern that
  sidesteps the "what would `--help` show?" problem.
