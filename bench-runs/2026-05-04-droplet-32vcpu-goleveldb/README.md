# 2026-05-04 droplet 32vCPU goleveldb run

## Setup

- Host: DigitalOcean droplet `cosmos-perf-test`, c-32-intel (32 vCPU, 64 GiB, NVMe), sfo3
- gaiad: v27.2.0 with the `cosmossdk.io/store` fork (per-chunk-restored log line — `loggedChunkReader` in `refs/store-v1.1.2-fork/snapshots/`)
- bench: `cmd/cosmos-statesync-bench` with `-fresh -peers-limit 25 -max-outbound 25`
- DB backend: **goleveldb** (cometbft default; no `app-db-backend` override in app.toml)
- Chain: cosmoshub-4
- Snapshot: format=3, height=30,955,000, 296 chunks
- Trusted RPCs: lavenderfive, publicnode, tendermintrpc.lava, stakin-nodes, kjnodes, easy2stake

## Files

- `gaiad.log` — full gaiad output through state-sync + initial blocksync
- `bench.out` — bench harness's event-driven phase output

## Notable observations

1. **Goleveldb backend**, not pebble. Cosmos-sdk's `GetAppDBBackend` returns `GoLevelDBBackend` when neither `app-db-backend` (app.toml) nor `db_backend` (config.toml) is explicitly set to pebble. The bench's `-fresh` re-runs `gaiad init` which writes `db_backend = "goleveldb"`. Confirmed via `.ldb` files in `application.db/` (vs pebble's `.sst`).

2. **Chunk-124 stall**: the per-chunk "Restored snapshot chunk" log shows a ~17-minute gap between chunk 123 (7:52 AM) and chunk 124 (8:09 AM), then resumes at full speed. Diagnosis: goleveldb compaction backpressure. The LSM hit `L0StopWritesThreshold` and stalled the IAVL writer goroutine; once L0→L1 compactions caught up, the pipeline resumed. 4439 SSTables at ~2 MiB each at peak.

3. **Fast-storage upgrade timing**: 25 "Upgrading IAVL storage..." log lines fired at 7:30 AM (gaiad startup, before restore) — all on **empty stores**, so they're effectively no-ops that just bump `storage_version` to `1.1.0-0`. After restore completed at 8:38 AM, no `Upgrading IAVL storage` lines fired. Need to investigate whether the post-restore `LoadLatestVersion` triggers `shouldForceFastStorageUpgrade` (storage_version `1.1.0-0` mismatching the newly-imported `latestVersion=30955000`) — if not, the restored DB has branch nodes only and reads will fall through to the slow tree-walk path until something forces a re-upgrade.

4. **Caught up successfully**: see end of `gaiad.log` for first `executed block height=30955001` lines (8:38 AM, immediately after `Snapshot restored`).

## Phase timing summary (from bench.out)

- State sync located: +27s
- First chunk fetched: +27s
- Chunk 100 restored: +17m44s
- Chunk 125 restored: +39m46s (the chunk-124 stall is in here)
- Verified ABCI app + Snapshot restored: ~+68m (8:38 AM, 7:30 AM start)
- Caught up: shortly after, gaiad started executing blocks immediately

## Why this run is worth keeping

- Establishes the goleveldb baseline. Next run with `app-db-backend = "pebbledb"` is the comparison point.
- Demonstrates the chunk-124 stall as a reproducible compaction-backpressure signature.
- Provides post-restore log evidence for the "fast-storage upgrade is missing/skipped" investigation.

## Related

- Issue #6 (parallelism), #7 (cache layers), #8 (Rust port)
- `third_party/iavl/FORK_NOTES.md` — wave-parallel design (not exercised in this run; gaiad uses upstream iavl)
- `refs/store-v1.1.2-fork/` — local fork that adds the per-chunk log line
