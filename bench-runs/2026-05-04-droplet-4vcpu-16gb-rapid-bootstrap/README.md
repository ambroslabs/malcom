# 2026-05-04 droplet 4vCPU 16GB rapid-bootstrap run

## Setup

- Host: DigitalOcean droplet `cosmos-perf-test-16gb`, **g-4vcpu-16gb** (4 vCPU, 16 GiB, NVMe), sfo3
- Tool: `cmd/cosmos-rapid-bootstrap` after the **adaptive-RAM refactor** committed in this session:
  - **`PebbleProfile` auto-detection** in `internal/snapshotappdb/pebble_adapter.go`. Reads `/proc/meminfo:MemAvailable`, picks one of `high|mid|low|tiny`. On this 16 GiB host: **`low` profile** → 256 MiB memtable, 256 MiB cache, automatic compactions ON, `concurrency=1` (sequential per-store).
  - **Bounded reorder buffer** in `internal/snapshotappdb/stream.go`. `defaultMaxAhead=4` chunks → ~40 MiB cap. Snapfetch back-pressures when the importer falls behind.
  - **GOMEMLIMIT auto-set** via `runtime/debug.SetMemoryLimit` to 75% of host MemAvailable (~12 GiB on this box). Forces aggressive GC near the limit.
  - **Memory instrumentation**: `[mem]` lines every 30s break down `go=heap:NMiB sys:NMiB stack:NMiB GCs=N pebble=memtable:NMiB×N cache:NMiB L0:N files`.
  - **Cleaner per-store / per-chunk progress**: `[chunks]` queue depth every 15s, `[appdb]` items/s every 30s (was every 5s), `[snapfetch]` one-line progress every 15s.
- Chain: cosmoshub-4. Snapshot height **30960000**, 297 chunks (~3 GiB compressed, 13.82 GiB uncompressed).

## Result: 14m37s end-to-end on 16 GB

| Phase | Duration | Cumulative |
|---|---|---|
| Setup + first peer dial | <30s | +30s |
| Snapfetch + ImportStream (interleaved, low profile) | **12m10s** | +12m10s |
| Pebble cleanup compaction | 0s (final compact ran inside import) | +12m10s |
| Cometbft `BootstrapState` | 10s | +12m21s |
| `gaiad init` + config edits | 1s | +12m22s |
| `gaiad start` + blocksync to tip | 2m15s | +14m37s |
| **CAUGHT UP** at 30960189 | — | **+14m37s** |

## Comparison: same workload across hosts

All on cosmoshub-4, same day, same orchestrator:

| Host | RAM | vCPU | Profile | Total wall | Notes |
|---|---|---|---|---|---|
| cosmos-perf-test | 64 GiB | 32 | high (default) | **6m47s** | streaming + prefer-fresh |
| cosmos-perf-test | 64 GiB | 32 | high | 9m53s | first version, serial fetch+import |
| **cosmos-perf-test-16gb** | **16 GiB** | **4** | **low** | **14m37s** | **same code, smaller machine** |
| (gaiad pebble state-sync, baseline) | 64 GiB | 32 | (cometbft default) | **1h41m** | for reference |

vs the 64 GiB run: **2.15× wall time on 25% the RAM and 12% the cores.**
vs gaiad pebble state-sync: **~7× speedup, on a host 4× smaller.**

## Memory profile observed

The `[mem]` log lines captured the heap dynamics live during the run. Headline points:

- **Profile-load idle (acc/authz)**: heap ~2.5 GiB, pebble memtable 1 GiB.
- **Bank import peak**: heap climbed steadily 2.5 → 5 → 7 → 9 → 11 GiB. **GOMEMLIMIT (12 GiB) kicked in** — heap pinned at ~10–11 GiB while GC ran 6× as often. Pebble memtable stayed at 256 MiB × 2 (capped). Sys peaked at 15.78 GiB / 16 GiB.
- **Bank Commit**: heap collapsed to **14 MiB** on bank's `Importer.Commit` returning. The wave-parallel iavl frontier really was reclaimable; GC just needed pressure to claim it.
- **Subsequent stores (ibc, provider, etc.)**: heap oscillated 5–10 GiB through bank-equivalent peaks, never breached GOMEMLIMIT.
- **L0 file count**: ballooned to **868 SSTs** at peak (vs `L0CompactionThreshold=8`/`L0StopWritesThreshold=24`). Pebble's stop-writes throttles `db.Apply()` but compactions can't keep up at our write rate. Pebble runs the final compact at end-of-import to tidy.

## What back-pressure chain works through

1. Snapfetch → channel sink → reorder buffer (capped at 4 chunks).
2. When buffer is full, `OnChunk` blocks → snapfetch's per-peer-limit-2 inflight throttles each peer's request rate.
3. Reader (zlib + proto + iavl `Importer.Add`) pulls from the buffer at the rate iavl's `Add` accepts.
4. iavl `Add` → wave-parallel hash workers → writeQ (cap 4096) → batched pebble.Apply.
5. pebble.Apply blocks when memtable backlog hits `MemTableStopWritesThreshold=2` or L0 hits `L0StopWritesThreshold=24`.
6. **Net effect**: the slowest link absorbs the back-pressure. On this host the slowest link is **pebble** (low profile = small memtable + active compactions). Snapfetch ends up waiting on the chain's slowest gear.

## What broke before this commit (failure modes)

For history (also archived in `../2026-05-04-droplet-4vcpu-16gb-rapid-bootstrap-thrash-history.txt` if useful):

1. **Run 1 (high profile)**: peers timed out under bank-import slow rate → all 5 peers banned → snapfetch rescan exhausted → import-stream returned `commit store "bank": invalid node structure, found stack size 18` mid-bank. Death by network detachment.
2. **Run 2 (low profile, unbounded reorder buffer)**: heap pegged at 99.7% RAM, items/s collapsed 1M → 10/s, GC thrash. Buffer held the entire snapshot (~3 GiB) waiting for slow consumer. Death by GC starvation.
3. **Run 3 (low profile, bounded buffer, no GOMEMLIMIT)**: bank items processed fine until ~50M, then heap kept climbing past 12 GiB → again pegged at 99.7% RAM. Iavl frontier expanded behind slow pebble writes. Death by un-paced GC.
4. **Run 4 (low profile, bounded buffer, GOMEMLIMIT=12GiB)** ← **this run**. GOMEMLIMIT forced GC under pressure, heap stayed pinned at ~10 GiB during peak, sys peaked at 15.78 GiB but never OOMd. Successful end-to-end.

The lesson: **bounded reorder buffer + tuned pebble profile + GOMEMLIMIT** is the combo. None of the three alone is sufficient.

## Files

- `bench.out` — orchestrator's full event log including `[mem]`, `[chunks]`, `[appdb]`, `[snapfetch]`, `>>> PHASE` lines.
- `gaiad.log` — gaiad's own log from `gaiad start` until catchup.

## Pairs with

- `../2026-05-04-droplet-32vcpu-rapid-bootstrap/` — first rapid-bootstrap (serial, 64 GiB, 9m53s)
- `../2026-05-04-droplet-32vcpu-rapid-bootstrap-streaming-fresh/` — streaming + prefer-fresh (64 GiB, 6m47s)
- `../2026-05-04-droplet-32vcpu-pebble/` — gaiad state-sync baseline (64 GiB, 1h41m)

## Quick command

```sh
GOMEMLIMIT=12GiB ./cosmos-rapid-bootstrap \
  -home /root/rapid-bootstrap-test \
  -gaiad /root/gaiad \
  -chain-id cosmoshub-4 \
  -genesis /root/genesis.cosmoshub-4.json \
  -rpcs https://cosmos-rpc.publicnode.com:443,https://cosmoshub.rpc.stakin-nodes.com,https://rpc.lavenderfive.com:443/cosmoshub,https://cosmos-rpc.easy2stake.com \
  -addrbook https://snapshots.polkachu.com/addrbook/cosmos/addrbook.json \
  -peers-file /root/peers-cumulative.json \
  -peers-limit 25 -max-outbound 25 \
  -fresh \
  -bootstrap-bin /root/cosmos-p2p/cosmos-bootstrap-gaia
  # -pebble-profile auto (default; will detect low on a 16 GiB host)
  # -mem-limit-pct 0.75 (default; auto-applies GOMEMLIMIT to 75% of MemAvailable)
```

The env var `GOMEMLIMIT` is now redundant — the binary applies it automatically based on host RAM. Kept above for explicitness.
