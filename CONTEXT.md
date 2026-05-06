# cosmos-p2p — session context

A running notebook for what we've built, tried, and learned in this thread,
so we can pick up on a fresh machine without losing the thread.

## What this repo does

A cosmoshub-4 ingestion pipeline. The maintained core flow lives in
`cmd/malcom/` as a single CLI with five subcommands; legacy / research /
diagnostic binaries live in `experimental/cmd/`.

**Snapshot → gaiad (the maintained `malcom` flow):**

1. `malcom snapshot fetch -prefer-fresh` discovers and downloads the
   newest format-3 cosmos-sdk state-sync snapshot. Phases: peer
   discovery → chunk-0 probe → parallel chunk fetch with redial.
2. `malcom snapshot import` reads the downloaded chunks and writes a
   gaiad-compatible `application.db/` (pebble) plus `extensions/` for
   wasm/08-wasm payloads. Atop `internal/snapshotimport`, the simple
   single-goroutine stack-based importer (no iavl, no wave-parallel
   path — it OOMed on 2 vCPUs). Pre-populates IAVL fast-storage
   inline so gaiad doesn't run its `upgradeToFastStorageGc1_1_0` pass
   on first start.
3. `malcom bootstrap` writes a runnable gaia home dir: cometbft
   offline state-sync bootstrap, copies application.db, places wasm
   bytecode for cosmwasm + 08-light-client, generates minimal
   app.toml/config.toml/client.toml.
4. Run gaiad against the bootstrap home.
5. `malcom verify` (optional) reads the imported CommitInfo from
   `<out>/application.db`, computes the local AppHash, fetches the
   consensus AppHash from a cometbft RPC at H+1, and reports match/
   mismatch.
6. `malcom compact` (optional) runs a full-keyspace pebble compaction
   to reclaim slack outside the import flow.

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

    # full pipeline (assumes binary built, snapshot will be downloaded)
    ./build/malcom snapshot fetch -prefer-fresh
    ./build/malcom snapshot import \
        -snapshot /mnt/data/cosmos-archive/cosmoshub-4/snapshots/<H>_3 \
        -out      /mnt/data/cosmos-archive/appdb-out/<H> \
        -height   <H>
    ./build/malcom bootstrap \
        -appdb    /mnt/data/cosmos-archive/appdb-out/<H> \
        -out      /mnt/data/cosmos-archive/gaia-bootstrap/<H> \
        -genesis  /mnt/data/home/zrbecker/genesis.cosmoshub-4.json \
        -height   <H> \
        -rpc      "https://cosmos-rpc.polkachu.com,https://cosmos-rpc.publicnode.com" \
        -write-configs
    cp /tmp/polkachu-addrbook.json \
        /mnt/data/cosmos-archive/gaia-bootstrap/<H>/config/addrbook.json
    nohup /mnt/data/home/zrbecker/bin/gaiad start \
        --home /mnt/data/cosmos-archive/gaia-bootstrap/<H> \
        </dev/null \
        >/mnt/data/cosmos-archive/gaia-bootstrap/<H>/gaiad.log 2>&1 &

    # AppHash sanity-check
    ./build/malcom verify \
        -appdb /mnt/data/cosmos-archive/appdb-out/<H> \
        -height <H>

    # pebble manual compaction (slack reclaim outside the import flow)
    ./build/malcom compact -dir /path/to/application.db

## Open follow-ons

- **Proto regeneration** for the snapshot wire types
  (`internal/snapshotimport/snapproto.go`). The hand-rolled decoder
  was a step too far on the "zero cosmos deps" goal; regen from
  copied + simplified `cosmos-sdk/snapshots/types/*.proto`.
- Snapshot fetch indexing: pre-decompress per-store byte ranges so
  multiple readers can decompress in parallel. Would unlock real
  reader-side parallelism (today the zlib stream is single-threaded).
- `experimental/cmd/cosmos-rapid-bootstrap` rework. Currently a
  serial fetch → import → bootstrap → start orchestrator using the
  in-tree CLI packages; the pipelined fetch+import overlap from the
  old wave-parallel path is gone. Refresh design or fold into a
  `malcom run` subcommand.
