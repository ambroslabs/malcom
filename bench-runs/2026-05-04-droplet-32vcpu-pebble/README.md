# 2026-05-04 droplet 32vCPU pebble run

## Setup

- Host: DigitalOcean droplet `cosmos-perf-test`, c-32-intel (32 vCPU, 64 GiB, NVMe), sfo3
- gaiad: v27.2.0 with **two** local forks of cosmos-sdk via `replace` directives:
  - `cosmossdk.io/store` → `refs/store-v1.1.2-fork/` — adds `loggedChunkReader` (per-chunk-restored log) and `IAVL storage upgrade complete` line in `LoadStoreWithOpts`.
  - `github.com/cosmos/iavl` → `refs/iavl-v1.2.6-fork/` — adds five `logger.Info` lines around `enableFastStorageAndCommit` (mutable_tree.go) and `Importer.Commit`'s LoadVersion call (import.go). Pure observability, no semantic change.
- bench: `cmd/cosmos-statesync-bench` with `-fresh -peers-limit 25 -max-outbound 25 -app-db-backend pebbledb`
- DB backend: **pebbledb** (overridden via the new `-app-db-backend` bench flag)
- Chain: cosmoshub-4
- Snapshot: format=3, height=30,957,000, 297 chunks
- Trusted RPCs: lavenderfive, publicnode, tendermintrpc.lava, stakin-nodes, kjnodes, easy2stake

## Files

- `gaiad.log` — full gaiad output through state-sync + initial blocksync (ANSI-stripped). Includes the new `IAVL importer.Commit: invoking LoadVersion` / `LoadVersion returned` / `IAVL fast-storage upgrade: starting` / `complete` / `write phase` lines that are unique to this run.
- `bench.out` — bench harness's per-chunk + phase event log.

## Phase timing summary

| Phase | Time | Cumulative from start |
|---|---|---|
| Bench start | 10:29:13 UTC | +0s |
| First chunk fetched | 10:29:38 | +25s |
| State sync located | (early) | +27s |
| Snapshot restored | 12:07:03 | **+1h37m50s** |
| Blocksync starts | 12:07:03 | +1h37m50s |
| Caught up | 12:09:55 | **+1h40m45s** |

**Total wall: ~1h41m** for fresh state-sync + catchup to mainnet on pebbledb.

For comparison: the goleveldb run archived at `../2026-05-04-droplet-32vcpu-goleveldb/` finished restore at **+1h09m** (no catchup measurement; killed mid-blocksync). Pebble was **~30 minutes slower** on the state-sync phase but completed catchup.

## Per-store IAVL fast-storage upgrade timings

Captured by the new `IAVL fast-storage upgrade: write phase` log lines. Times are `iterate_and_batch_duration` — the full tree iteration + per-leaf `SaveFastNodeNoCache` + final batch.Commit.

| Store | Leaves | Duration | Rate (leaves/s) |
|---|---|---|---|
| 08-wasm | 4 | 21µs | — |
| acc | 7,143,182 | 6m45s | 17,600 |
| authz | 113,790 | 3.94s | 28,900 |
| **bank** | 28,332,261 | **36m04s** | **13,100** |
| consensus | 1 | 8µs | — |
| distribution | 3,059,279 | 3m46s | 13,500 |
| evidence | 4 | 18µs | — |
| feegrant | 4,474 | 56ms | 80,000 |
| feemarket | 2 | 9µs | — |
| gov | 1,503 | 25ms | 60,000 |
| **ibc** | 20,453,246 | **22m56s** | **14,860** |
| icacontroller | 86 | 0.34ms | — |
| icahost | 480 | 1.7ms | — |
| liquid | 8,628 | 182ms | 47,000 |
| mint | 2 | 9µs | — |
| packetfowardmiddleware | 8,974 | 6.6s | 1,360 |
| params | 70 | 0.28ms | — |
| **provider** | 5,988,570 | **3m52s** | **25,800** |
| ratelimit | 8 | 40µs | — |
| slashing | 3,488 | 40ms | 87,000 |
| **staking** | 4,736,980 | **6m32s** | **12,070** |
| tokenfactory | 11 | 67µs | — |
| transfer | 3,373 | 52ms | 65,000 |
| upgrade | 65 | 0.30ms | — |
| **wasm** | 4,543,054 | **4m42s** | **16,100** |

Total fast-nodes written: **~74M**. Total upgrade time spent inside `Importer.Commit` calls: **~1h28m**, spread across the chunk-stream window (each store's upgrade fires synchronously at the chunk boundary where its `SnapshotItem_Store` marker appears in the protobuf stream — NOT as a single post-restore pass).

## Key observations

1. **Per-store upgrade is interleaved with chunk fetching.** This was a session-long question — does the bulk fast-storage upgrade run as a single end-of-restore phase, or distributed during restore? Answer: **distributed**. The Importer for each store calls `tree.LoadVersion(version)` at its `Commit()` point (iavl@v1.2.6/import.go:232), which silently triggers `enableFastStorageAndCommitIfNotEnabled`. So the chunk pipeline freezes for tens-of-minutes at every store boundary that contains a major store like bank/ibc. The `Snapshot restored` log fires AFTER all upgrades complete.

2. **Bank dominates: 36m04s for 28M leaves.** That's ~13,100 leaves/sec on pebble with stock cosmos-db config (4 MiB memtable, 8 MiB cache, L0StopWritesThreshold=12). The bottleneck is not iavl-side but pebble-side: small memtable forces ~hundreds of L0 flushes during a single store's upgrade, hitting the L0 stall threshold repeatedly.

3. **Per-store throughput varies 2× across runs.** Provider's 25,800/s vs bank's 13,100/s — same backend, same hardware. Difference is pebble's compaction state: by the time provider ran (after bank cleared L0), L0 was less full and compactions weren't bottlenecking writes. This argues that pebble's behavior is highly state-dependent for this workload.

4. **Catchup throughput on pebble was good.** 1884 blocks in 2m52s = ~10.9 blk/s sustained. Faster than the goleveldb run's catchup rate (~6 blk/s avg over its 5-min window). Pebble's read-side optimization (block cache, layered compaction) helps blocksync once the LSM is fully built.

5. **No headline `Upgrading IAVL storage...` log fires post-restore for the real upgrade.** That log lives in `cosmossdk.io/store/iavl/store.go:71` (LoadStoreWithOpts), which only fires for stores opened via the cosmos-sdk wrapper — i.e., when `LoadLatestVersion` is called after restore. By that point storage_version has already been bumped by the upgrade fired inside `Importer.Commit`, so `IsUpgradeable()` returns false and the log stays silent. Without our iavl fork's `IAVL fast-storage upgrade: starting/complete` lines, this entire 1h28m of work would be invisible to operators.

## Why this run is worth keeping

- Establishes the **pebble baseline for state-sync upgrade timing** with stock cosmos-db config — the configuration ~all cosmoshub operators run.
- First run with the iavl observability fork wired in. Future state-sync runs (different chains, different hardware, different backend tunings) can be compared directly to this per-store timing table.
- Pairs with `../2026-05-04-droplet-32vcpu-goleveldb/` to demonstrate the storage-backend trade: pebble slower for the upgrade phase, faster for steady-state catchup.
- Demonstrates issue #9's actionable thread (cosmos-db pebble defaults are unsized for production) with empirical wall-clock numbers.

## Related

- Issue #6 — extending wave-parallel iavl to block execution
- Issue #7 — reconsidering cosmos-sdk's cache layers
- Issue #8 — Rust port of cosmos-snapshot-to-appdb
- Issue #9 — cosmos-sdk's pebble/goleveldb tuning is locked behind abstractions
- `refs/store-v1.1.2-fork/` and `refs/iavl-v1.2.6-fork/` — the two local forks that made this run's per-store timings observable
- `internal/snapshotappdb/pebble_adapter.go` — reference for tuned pebble.Options that the offline tool uses (1 GiB memtable, 2 GiB cache, deferred L0 compaction); none of these are reachable through running gaiad

## Quick commands

To re-run an equivalent bench:

```sh
./cosmos-statesync-bench \
  -home /root/statesync-test \
  -gaiad /root/gaiad \
  -chain-id cosmoshub-4 \
  -genesis /root/genesis.cosmoshub-4.json \
  -rpcs <comma-separated-rpcs> \
  -addrbook https://snapshots.polkachu.com/addrbook/cosmos/addrbook.json \
  -peers-file /root/peers-cumulative.json \
  -peers-limit 25 -max-outbound 25 \
  -app-db-backend pebbledb \
  -fresh
```
