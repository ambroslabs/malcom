# 2026-05-04 droplet 32vCPU rapid-bootstrap run

## Setup

- Host: DigitalOcean droplet `cosmos-perf-test`, c-32-intel (32 vCPU, 64 GiB, NVMe), sfo3
- New tool: **`cmd/cosmos-rapid-bootstrap`** — orchestrates a fast snapshot → caught-up flow as a single command, mirroring the bench harness's structured event log.
- gaiad: v27.2.0 with all three forks wired in:
  - `cosmossdk.io/store` fork (per-chunk-restored log)
  - `github.com/cosmos/iavl@v1.2.6` fork (silent-upgrade observability — not exercised here since rapid-bootstrap pre-populates fast-nodes)
  - `github.com/cosmos/cosmos-db@v1.1.3` fork (2 GiB MemTableSize for gaiad runtime)
- Pipeline binaries: `cosmos-snapshot-fetch`, `cosmos-snapshot-to-appdb` (via internal package), `cosmos-bootstrap-gaia` (with new `-skip-app-copy` flag)
- DB backend: pebbledb everywhere
- Chain: cosmoshub-4
- Snapshot: format=3, height=30,958,000, 297 chunks (~2.96 GiB compressed, ~13.82 GiB uncompressed)

## Pipeline

1. **`cosmos-snapshot-fetch`** — discovers + races + downloads chunks via raw cometbft p2p (no RPC dependency). Writes to `<home>/snapshots-staging/`.
2. **`snapshotappdb.Import`** — wave-parallel iavl + bulk-load-tuned pebble (1 GiB memtable, deferred L0 compaction). Writes application.db **directly to `<home>/data/application.db`** — no intermediate location, no copy.
3. **`snapshotappdb.PebbleCleanupCompact`** — full-keyspace compaction to reclaim slack from bulk-load mode.
4. **`cosmos-bootstrap-gaia -skip-app-copy`** — writes `state.db` + `blockstore.db` (via cometbft `node.BootstrapState`) + minimal `config.toml`, `app.toml`, `client.toml`. The new `-skip-app-copy` flag bypasses the 1.5-min `cloneTree` since application.db is already in place.
5. **`gaiad init`** — generates `node_key.json` + `priv_validator_key.json` + `priv_validator_state.json` (via temp home + cherry-pick).
6. **Config edits** — set `persistent_peers` from cumulative.json, `app-db-backend = "pebbledb"`, `statesync.enable = false`.
7. **`gaiad start`** — pure blocksync to mainnet tip (no state-sync at runtime; data is already on disk).
8. **RPC poll** until `catching_up: false`.

## Phase timing summary

| Phase | Duration | Cumulative |
|---|---|---|
| Setup (wipe + mkdirs) | <1s | +0s |
| Snapshot fetch (discover + race + 297-chunk download) | **3m14s** | +3m14s |
| `snapshotappdb.Import` (148.8M items, 13.82 GB uncompressed, 25 stores) | **3m38s** | +6m53s |
| Pebble cleanup compaction | 5s | +6m58s |
| Cometbft `BootstrapState` (state.db + blockstore.db) | 9s | +7m07s |
| `gaiad init` (cherry-picked files) | <1s | +7m08s |
| Configure peers + backend | <1s | +7m08s |
| `gaiad start` + blocksync to tip (1576 blocks at ~10 blk/s) | **2m45s** | +9m53s |
| **CAUGHT UP** | — | **+9m53s** |

## Comparison: rapid-bootstrap vs gaiad state-sync

Same hardware, same chain, same snapshot height. Total wall to caught-up node:

| Approach | Backend | Total wall | Speedup |
|---|---|---|---|
| gaiad state-sync | goleveldb | 1h09m+ (restore only; killed mid-catchup) | 1× |
| gaiad state-sync | pebbledb (stock) | **1h41m** | 1× |
| **cosmos-rapid-bootstrap** | pebbledb (tuned) | **9m53s** | **~10×** |

## Why so much faster

The big-ticket attribution:

1. **Bulk-load-tuned pebble during import** (`internal/snapshotappdb/pebble_adapter.go`):
   - `MemTableSize: 1 << 30` (1 GiB) vs cosmos-db stock's 4 MiB
   - `DisableAutomaticCompactions: true` — write everything to L0, single big compact at the end
   - `L0CompactionThreshold: math.MaxInt32`, `L0StopWritesThreshold: math.MaxInt32` — no L0 backpressure stalls
   - `MaxConcurrentCompactions: 4` (8 during cleanup) vs stock 3
   - `Cache: 2 << 30` (2 GiB) vs stock 8 MiB
   
   These options are **unreachable through gaiad's stock cosmos-db wrapper** (only `maxopenfiles` is exposed). For the 74M-leaf bulk fast-storage upgrade workload this is the dominant lever.

2. **Pre-populated fast-nodes during import** (Path A in `snapshotappdb`):
   - Each leaf gets its fast-node entry written **alongside** its branch node, in the same bulk-load flow.
   - When gaiad opens application.db, `IsUpgradeable()` returns false (storage_version already set during import). The 1h28m-ish silent post-restore upgrade pass that we measured in `../2026-05-04-droplet-32vcpu-pebble/` simply doesn't happen.

3. **Wave-parallel iavl** (`third_party/iavl`):
   - Defers hashing to a worker pool + atomic-events scheduler.
   - Speeds up the import phase's CPU-bound work; doesn't help the LSM-bound parts.

4. **Skip the application.db copy**:
   - Stock `cosmos-bootstrap-gaia` does a `cloneTree` step (~1.5 min for 14 GiB on local NVMe). New `-skip-app-copy` flag bypasses it because `snapshotappdb.Import` writes directly to the destination.

5. **No state-sync overhead in gaiad**:
   - The state-sync runs paid for: chunk fetching via cometbft's reactor, IAVL Importer with stock pebble defaults (the 36-min bank upgrade), and the silent fast-storage upgrade chain interleaved with chunks. **All of this is replaced** by our offline tool calling pebble directly with bulk-load options.

The catchup phase is roughly the same speed in both worlds (~10 blk/s), since steady-state block execution is read-dominated and the storage-tuning levers don't help much there. The win is entirely in the import phase.

## Files

- `bench.out` — rapid-bootstrap orchestrator's event log: phase boundaries, sub-process output prefixed with `[snap-fetch]` / `[bootstrap]` / `[appdb]`, RPC poll lines.
- `gaiad.log` — gaiad's own log from `gaiad start` until catchup (ANSI-stripped).

## Pairs with

- `../2026-05-04-droplet-32vcpu-goleveldb/` — gaiad state-sync on goleveldb (1h09m+)
- `../2026-05-04-droplet-32vcpu-pebble/` — gaiad state-sync on pebble (1h41m)
- `../2026-05-04-droplet-32vcpu-memdb-oom/` — partial memdb run, established workload-bound minimum

## Caveats

- **gaiad uses 2 GiB memtable** (cosmos-db fork) during catchup — but as the run shows, that doesn't change blocksync rate. Steady-state block execution isn't memtable-bound; the lever there would be block cache size + iavl LRU. The 2 GiB memtable is benign for this comparison.
- **Catchup is short** (1576 blocks, 2m45s). A longer catchup window would smooth out the rate measurement.
- **Trust hash via single RPC** in cosmos-bootstrap-gaia. Earlier we hit transient timeouts on a couple of RPCs; this run trimmed to 4 reliable ones.

## Quick command

```sh
./cosmos-rapid-bootstrap \
  -home /root/rapid-bootstrap-test \
  -gaiad /root/gaiad \
  -chain-id cosmoshub-4 \
  -genesis /root/genesis.cosmoshub-4.json \
  -rpcs https://cosmos-rpc.publicnode.com:443,https://cosmoshub.rpc.stakin-nodes.com,https://rpc.lavenderfive.com:443/cosmoshub,https://cosmos-rpc.easy2stake.com \
  -addrbook https://snapshots.polkachu.com/addrbook/cosmos/addrbook.json \
  -peers-file /root/peers-cumulative.json \
  -peers-limit 25 -max-outbound 25 \
  -fresh \
  -snapshot-fetch /root/cosmos-p2p/cosmos-snapshot-fetch \
  -bootstrap-bin /root/cosmos-p2p/cosmos-bootstrap-gaia
```
