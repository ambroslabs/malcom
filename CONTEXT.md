# cosmos-p2p — session context

A running notebook for what we've built, tried, and learned in this thread,
so we can pick up on a fresh machine without losing the thread.

## What this repo does

A cosmoshub-4 ingestion pipeline. Two main flows:

**Block archive (existing, stable):**
- `cosmos-archive download` peers up via cometbft p2p, fetches missing
  block heights into `<archive>/shards/`, writes a CRC-checked store.
- `cosmos-archive {ranges,missing,stats,status,verify,fsck,verify-chain,verify-genesis,verify-anchor}` query/audit the archive.
- The archive pebble store sits at `/mnt/data/cosmos-archive/cosmoshub-4/`.

**Snapshot → gaiad (new in this session):**
1. `cosmos-snapshot-fetch -prefer-fresh` discovers and downloads the
   newest format-3 cosmos-sdk state-sync snapshot. Phases: peer
   discovery → chunk-0 probe → parallel chunk fetch with redial.
2. `cosmos-snapshot-to-appdb -backend pebbledb` imports the snapshot
   into a pebble-formatted `application.db` that gaiad reads
   directly. Pre-populates IAVL fast-storage during import (Path A:
   inline `f`-prefixed entries + `m+"storage_version"="1.1.0-<H>"`)
   so gaiad doesn't run its `upgradeToFastStorageGc1_1_0` pass on
   first start.
3. `cosmos-bootstrap-gaia` writes a runnable gaia home dir: cometbft
   offline state-sync bootstrap, copies application.db, places wasm
   bytecode for cosmwasm + 08-light-client, generates minimal
   app.toml/config.toml/client.toml.
4. Run gaiad against the bootstrap home.

## End-to-end numbers (cosmoshub-4 height 30,950,000, 2 vCPU box)

| Phase | Time |
|---|---|
| Snapshot fetch (download + inspect) | ~10 min |
| snapshot-to-appdb | ~10 min |
| bootstrap-gaia (light client + place) | ~5 s |
| gaiad blocksync to live tip | ~6 min |
| **Total: snapshot to live tip** | **~27 min** |

AppHash at 30,950,000: `0F22F949B584C1C0ABBDB1A045C5B02B46151675FD64C4ACD4A208B937C9DDB5`. Verified against polkachu/publicnode RPC. Independent test command:

    /tmp/cosmos-apphash-verify -appdb /mnt/data/cosmos-archive/appdb-out/30950000 -height 30950000

## Hardware on this 2-vCPU box

- Intel Xeon Gold 6548N, **2 vCPUs**, 8 GB RAM, 8 GB swap.
- Single 1 TB ext4 volume on /dev/sda (cloud volume; reports rotational but probably SSD-backed).

## What lives where

- Repo: `/mnt/data/home/zrbecker/code/cosmos-p2p/`
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

## Optimizations applied (in commit order)

Each commit is small + atomic so we can read history later. `git log --oneline`:

| Commit | What |
|---|---|
| `9c0eacb` | snapshotappdb: stream LSM metrics during pebble compactions; PebbleCleanupCompact reclaims orphan SSTs (~11 GB on cosmoshub) |
| `4bbcce7` | gitignore: build/, nohup.out, cosmos-bootstrap-gaia |
| `2913bbb` | bootstrap-gaia: real copy of application.db (not hardlink) — flock aliasing + lifetime entanglement issues |
| `2fbc396` | snapshot-to-appdb: cleanup pass before "complete" line so the summary reflects post-cleanup state |
| `1d8ce27` | snapshotappdb: per-store workers, pipeline next-store reads behind prev-store commit |
| `e4f1b42` | snapshotappdb: 3-stage intra-store pipeline (iavl.Add ‖ fast-write ‖ async pebble flush) |
| `59e97ff` | iavl: vendor v1.3.6 under third_party/iavl/, replace directive in go.mod, 2-way parallel sibling hashing in Importer.Add (proved correctness — AppHash matches) |
| **(uncommitted)** | iavl: wave-parallel rewrite — defer all hashing to a worker pool with dependency-graph dispatch |

## Performance comparison on 2 vCPU (snapshot import for h=30,950,000)

Stream phase = open-first-store → last-store commit.

| Variant | Stream | Final compact | Cleanup | Total |
|---|---|---|---|---|
| Serial (pre-1d8ce27) | 8m31s | 2m4s | 16s | ~10m24s |
| 1d8ce27 inter-store only | 8m28s | 2m9s | 16s | ~10m55s |
| e4f1b42 + intra-store 3-stage | 8m18s | 2m8s | 16s | ~10m44s |
| 59e97ff iavl 2-way fork | ~9m5s | 2m9s | 16s | ~11m54s |
| Wave-parallel (uncommitted) | OOM at bank | — | — | — |

**Reading:** none of the parallel designs win on 2 vCPUs. Each adds
goroutine + channel + GC overhead while the iavl.Importer's `Add`
work is fundamentally CPU-bound on `_hash` + `writeBytes`, and we
only have 2 cores. The 3-stage intra-store split bought ~13s of
stream time but lost ~30s elsewhere.

## Wave-parallel rewrite — current status

Lives in `third_party/iavl/import.go` (uncommitted). Design:

1. `Importer.Add` no longer hashes anything. It builds the tree
   shape, sets atomic state on each Node (`importPending`,
   `importEvents`, `importBuilt`, `importSubmitted`, `importParent`).
2. Leaves are submitted to a **dispatcher** goroutine via an
   unbounded `pending` slice. The dispatcher pumps into a bounded
   `ready` channel. Workers consume from `ready`.
3. Workers (default `min(NumCPU, 8)`) hash + serialise + send to a
   `writeQ` channel.
4. A single **writer** goroutine drains `writeQ` and calls
   `pebble.Batch.Set`, with async flushes at `maxBatchSize` (10000).
5. After hashing, worker calls `eventDone(node)` which atomically
   increments `node.events`. When events hits 2 (worker contributed
   "hashed" + main contributed "parent linked"), the parent's
   `pending` is decremented. If pending hits 0 AND the parent has
   been popped (`importBuilt`), it gets submitted to ready.
6. Root never has `importBuilt` set (it's never popped). Workers
   never submit it. `Commit` drains the pool, then synchronously
   hashes + writes the root with `nonce=1`.

### Bugs encountered (and fixed)

1. **Synchronous nil-out of children's leftNode/rightNode in Add raced workers' `_hash`.** Upstream nils these AFTER calling writeNode (which hashes synchronously); we deferred hashing but kept the nil-out, so workers could read `node.leftNode == nil` and `_hash` would silently return nil → corrupted serialized bytes → AppHash mismatch on iavl unit tests. Fix: nil-out moved into worker after hashing.
2. **Worker-as-producer-and-consumer deadlock on `ready` channel.** When the bounded `ready` filled, workers blocked on submit while no one was draining (because the only consumers were the workers themselves). Fix: dispatcher goroutine pattern. Workers and main both append to an unbounded `pending` slice (non-blocking under mutex); the dispatcher is the only producer to `ready`.
3. **Memory exhaustion on bank during the 2-vCPU run.** With 9M+ leaves all sitting in `pending` until 2 workers can drain them, the live key+value bytes are several GB. Hit 4.4 GB swap, throughput dropped from 270k items/s → 30k items/s. Mitigated by dropping `key/value/leftNodeKey/rightNodeKey` after `hashAndSerialize` (frees ~80% of leaf memory). Also reduced snapshotappdb concurrency to 1 to keep only one Importer alive at a time. Memory still grew unboundedly — workers can't drain fast enough on 2 cores.

### iavl unit tests

`go test -run "TestExporter_Import" -count=1 ./third_party/iavl/...`
passes — proves consistency on basic / sized / random small trees.

### What's still unknown

- AppHash on full cosmoshub h=30,950,000 — couldn't finish on 2 vCPUs
  due to OOM. Need a bigger box.
- Wall-clock speedup with N≥4 cores — design was sized for it.
- Whether to keep wave-parallel as the always-on path or fall back to
  the upstream serial path under a NumCPU threshold.

## Plan when we resume on the 32 vCPU box

1. Pull this branch (currently main, tip `59e97ff` committed + the
   wave-parallel diff in working tree).
2. Optionally `git stash` the wave-parallel patch first if you want
   the 2-way-fork baseline run again.
3. Re-run `cosmos-snapshot-to-appdb -backend pebbledb` against the
   already-fetched snapshot at
   `/mnt/data/cosmos-archive/cosmoshub-4/snapshots/30950000_3/`.
4. Verify with `cosmos-apphash-verify -appdb … -height 30950000`.
5. Compare wall-clock to the 2-vCPU serial baseline (~10m24s
   total, ~8m30s stream).
6. **Only if wave-parallel still loses on 32 cores or hits memory
   limits**: introduce the NumCPU threshold and keep the upstream
   serial path as a fallback.
7. Likely follow-on: bound the `pending` slice with main-side
   backpressure + worker-side overflow list, so memory is bounded
   regardless of core count.

## Open follow-ons (lower priority)

- Snapshot fetch indexing: pre-decompress per-store byte ranges so
  multiple readers can decompress in parallel. Would unlock real
  reader-side parallelism (today the zlib stream is single-threaded).
- iavl.Importer's per-store internal `inflightCommit` could grow to
  >1 batch in flight to overlap pebble I/O — currently we await
  prev before starting next.
- bootstrap-gaia could be made resumable (today it wipes data/ on
  -overwrite and re-bootstraps).

## Useful commands cheat sheet

    # full pipeline (assumes binaries built, snapshot present)
    ./cosmos-snapshot-fetch -prefer-fresh
    ./cosmos-snapshot-to-appdb -backend pebbledb \
        -snapshot /mnt/data/cosmos-archive/cosmoshub-4/snapshots/<H>_3 \
        -out      /mnt/data/cosmos-archive/appdb-out/<H>
    ./cosmos-bootstrap-gaia \
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
    /tmp/cosmos-apphash-verify \
        -appdb /mnt/data/cosmos-archive/appdb-out/<H> \
        -height <H>

    # pebble manual compaction (slack reclaim outside the import flow)
    ./pebble-compact -dir /path/to/application.db
