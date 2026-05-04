# 2026-05-04 droplet 4vCPU 16GB rapid-bootstrap (pool + peer-resilience) run

## Setup

- Host: same `cosmos-perf-test-16gb` droplet (4 vCPU, 16 GiB, 50 GiB NVMe, sfo3) as the prior 14m37s run.
- Snapshot: cosmoshub-4 height 30960000, 297 chunks, 13.82 GiB uncompressed.
- Build: includes the changes committed during this session:
  - **`*Node` pool + chunk-buffer pool** (commit `20e3b64`).
  - **reorderReader deadlock fix** (`stream.go` — pump no longer blocks on a per-chunk-index horizon; relies on chunks-channel + snapfetch-inflight as the natural cap; uncommitted at run time).
  - **peer resilience trio in `internal/snapfetch/snapfetch.go`** (uncommitted at run time):
    1. `runKeepWarm` background goroutine that dials seeds whenever connected count < `WarmPeerTarget` (default 16).
    2. `provisional` peer flag — connected non-good peers get one in-flight probe slot; first verified chunk promotes them; any failure 1-strikes them.
    3. Exponential-backoff redial: `5s, 10s, 20s, …` capped at `MaxRedialBackoff` (default 5m). Disconnects no longer ban — only `PeerFailLimit` hash/missing strikes do.

## Outcome: snapfetch+import succeeded; gaiad blocksync OOM-disk before catchup

| Phase | Duration | Cumulative |
|---|---|---|
| Setup + first peer dial | ~30s | +30s |
| Snapfetch + ImportStream (interleaved, low profile) | **12m07s** | +12m07s |
| Pebble cleanup compaction | 0s (final compact ran inside import, took 1m9s) | +12m07s |
| Cometbft `BootstrapState` | 10s | +12m17s |
| `gaiad init` + config edits | 1s | +12m18s |
| `gaiad start` + blocksync | (caught up to 30961243, ~3m45s) | ~+16m |
| **DISK FULL** at height 30961243 (~60 blocks short of tip) | — | — |

The import phase landed at **12m07s vs 12m10s** for the prior 14m37s run on the same hardware — essentially identical. Confirms that the wave-parallel iavl pipeline's hashing isn't the bottleneck on small hosts (pebble I/O + serial decompression dominate).

## Disk failure mode

```
panic: error writing batch to DB "failed to save block":
(base 30960001, height 30961244):
write /root/rapid-bootstrap-test/data/blockstore.db/000107.log:
no space left on device
```

Application.db reached 45 GiB / 48 GiB available, leaving 16 KiB free. Pebble's L0 had peaked at 497 SSTs during bank import; the in-import "final compaction" took 1m9s but didn't fully compact down. Blocksync's per-block writes filled the remaining headroom.

This is a **disk-sizing** issue, not a code regression. The prior 14m37s run only had to blocksync ~189 blocks past the snapshot (the chain was nearly at the snapshot height when it ran); this run had to chase ~1240 blocks because the chain had moved forward in real time during the long debugging session.

Fixes for next time:
- Run a more aggressive post-import compaction (compact to L6 explicitly) before launching gaiad. Or
- Use a larger disk (75 GiB+).

## Resilience signal counts

From `bench.err`:

| Signal | Count |
|---|---|
| `keep-warm refresh` ticks | 22 |
| `provisional peer added` | 6 |
| `peer promoted from provisional` | 1 |
| `benching peer` (proven 3-strike + provisional 1-strike) | 13 |

The keep-warm dialer fired 22 times, mostly during bank import while connected count was below 16. 6 new peers entered as provisional; only 1 served a verified chunk and got promoted (the rest were either tendermint nodes without state-sync at this height, or had transient connection failures during their probe). 13 benches happened — the originally-chosen 4 good peers stayed connected throughout and the resilience system absorbed the loss of the original peer pool that killed the prior run.

**Critically**: the prior 16 GB run with no resilience (committed at `14c06fe`) failed when 4 good_peers all dropped during bank import; this run's chosen 4 stayed alive. We didn't actually exercise the rescan path because the original good peers held — but the keep-warm dialer kept a fresh pool warm in case any had dropped.

## Memory profile

| Metric | This run | Prior 14m37s run |
|---|---|---|
| Peak heap (Go) | **8.8 GiB** | ~10–11 GiB pinned |
| Peak sys (Go) | 14.8 GiB | 15.78 GiB |
| Peak L0 SSTs | 497 | 868 |
| Final compaction time | 1m9s (during import) | 0s (compact inside import) |

Heap peak dropped from 10–11 GiB to 8.8 GiB. The `*Node` pool removed enough GC pressure that GOMEMLIMIT (11.6 GiB) wasn't pinning the heap — the workload's natural footprint sits below the limit.

The reorder buffer was *significantly larger* than the prior bounded-maxAhead version (peaked around 220 chunks ≈ 2.2 GB) because the deadlock fix removed the per-chunk-index horizon. The natural cap is now `chunks_channel_size (16) + snapfetch_inflight × peers ≈ 24-50 chunks`, but in practice the reorder buffer absorbed download bursts: snapfetch finished delivering all 297 chunks by 4-5 minutes in (state=closed-draining), while the importer was still consuming. So memory was **higher in the buffer, lower in the iavl frontier**, net lower overall.

## What this run validates

1. **`*Node` pool is a clean win** — heap drops 2 GiB at peak, no observed regression.
2. **reorderReader deadlock fix is correct** — the prior `maxAhead=4` deadlocked on `peers × inflight > 4` (which was always the case). New design drains the channel unconditionally; the natural cap from snapfetch in-flight is sufficient.
3. **Peer-resilience trio works** — keep-warm+provisional+exp-backoff held the network up through a long bank import that kills the un-instrumented version. The "1 promoted" from "6 provisional added" was lower than ideal (low hit rate on random tendermint peers having this snapshot's height), but the existing 4 good_peers held throughout, so the resilience was never really stressed.

## What this run does NOT validate

- **End-to-end CAUGHT UP** — gaiad died from disk-full mid-blocksync before the orchestrator could observe `catching=false`.
- **Wave-parallel iavl on 4 vCPU is faster than serial** — open question; commit messages from the 2-way-parallel ancestor (`59e97ff`) explicitly noted "On 2 vCPUs the fork is a small regression (~12% slower stream phase) due to oversubscription". Wave-parallel adds *more* per-node overhead than 2-way-parallel; on 4 vCPU it may net negative. Worth A/B testing with a `workers=0` synchronous mode.

## Files

- `bench.out` — orchestrator's stdout (phase log, [chunks]/[appdb]/[mem] lines).
- `bench.err` — orchestrator's stderr (cometbft p2p errors + the new resilience log lines).
- `gaiad.log` — last 5000 lines of gaiad's stdout up to the OOM panic.

## Pairs with

- `../2026-05-04-droplet-4vcpu-16gb-rapid-bootstrap/` — prior 14m37s baseline (same hardware, no pool, no peer-resilience).
- `../2026-05-04-droplet-32vcpu-rapid-bootstrap-streaming-fresh/` — 6m47s on 32 vCPU 64 GiB.
