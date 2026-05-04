# 2026-05-04 droplet 32vCPU memdb run (OOM at chunk 170/297)

## Setup

- Host: DigitalOcean droplet `cosmos-perf-test`, c-32-intel (32 vCPU, **64 GiB**, NVMe), sfo3
- gaiad: v27.2.0 with both local forks (`cosmossdk.io/store` + `github.com/cosmos/iavl@v1.2.6`) for per-chunk + per-store upgrade observability
- bench: `cosmos-statesync-bench -fresh -peers-limit 25 -max-outbound 25 -app-db-backend memdb`
- DB backend: **memdb** (`google/btree` under sync.Mutex, no persistence, no compaction, no fsync)
- Chain: cosmoshub-4
- Snapshot: format=3, height=30,958,000, 297 chunks

## Outcome: OOM-killed at chunk 170/297

```
oom-kill: ... task=gaiad,pid=97260
Out of memory: Killed process 97260 (gaiad)
total-vm:72104340kB, anon-rss:64729820kB
```

gaiad's resident set hit **64.7 GB** — essentially all 64 GB of physical RAM — and the kernel OOM-killer terminated it. Bench reported `gaiad exited unexpectedly: signal: killed` at +11m30s.

The intent was to establish a **workload-bound minimum** for the post-state-sync IAVL fast-storage upgrade by running with zero storage-backend overhead. We got far enough to capture timings for the first six big stores in alphabetical order before OOM.

## Why memdb on cosmoshub doesn't fit in 64 GB

iavl `Node` structs in Go memory are much larger than their on-disk encoded bytes:

- Encoded leaf: ~50–100 bytes (varint(height) + varint(size) + bytes(key) + bytes(value))
- In-memory `*Node`: pointer + key/value byte slices (24 b each) + leftHash/rightHash byte slices + leftNodeKey/rightNodeKey byte slices + height (1 b) + size (8 b) + version (8 b) + Go GC headers
- Effective: **~200–400 bytes per node** in memory after deserialization

cosmoshub-4 at this height: ~74M leaves + ~74M inner nodes = ~150M total nodes. At 250 b avg = ~37 GB just for branch nodes. Plus:
- ~74M fast-node entries: ~3–5 GB
- iavl LRU cache (default 781,250 entries × ~250 b = ~200 MB; bounded but cycling)
- cosmos-db memdb btree internal nodes (degree 32; ~5–10% overhead on stored size)
- Snapshot chunks staged on disk (10 MB × ~170 = ~1.7 GB) — disk, not RAM
- Working tree DAG, immutable tree clone, mempool, runtime

Total RAM working set easily exceeds 64 GB. The memdb baseline test on cosmoshub-4 needs **128+ GB** to complete; ~192 GB to run with safe headroom.

## What we captured before OOM

Per-store fast-storage upgrade timings (first six big stores plus a handful of trivial ones):

| Store | Leaves | Duration (memdb) | Rate (leaves/s) | Pebble equivalent | Speedup |
|---|---|---|---|---|---|
| 08-wasm | 4 | 21µs | — | 21µs | — |
| **acc** | 7,143,224 | **1m07.6s** | **107,000** | 6m45s | **6.0×** |
| **authz** | 113,783 | **0.49s** | **232,000** | 3.94s | **8.0×** |
| **bank** | 28,332,342 | **4m14.1s** | **111,500** | 36m04s | **8.5×** |
| consensus | 1 | ~10µs | — | 8µs | — |
| **distribution** | 3,059,261 | **36.86s** | **83,000** | 3m46s | **6.1×** |
| evidence | 4 | 15µs | — | 18µs | — |
| feegrant | 4,474 | 14.7ms | 304,000 | 56ms | 3.8× |
| (gov) | 1,504 | 8.27ms | 182,000 | 25ms | 3× |

OOM hit immediately after — chunk pipeline reached chunk 170/297 (~57% through the snapshot's chunk-stream), then ibc was likely starting its upgrade (20M leaves) when memory exhausted.

## Headline finding (what's enough to take away from this partial run)

**Workload-bound rate: ~110k leaves/sec on this hardware**, dominated by:

1. iavl iterator's per-leaf `GetNode` (hits LRU cache mutex, deserializes node bytes, allocates `*Node`)
2. `saveFastNodeUnlocked`'s `bytes.Buffer` + `WriteBytes` per leaf
3. cosmos-db memdb's `btree.ReplaceOrInsert` per leaf under sync.Mutex
4. GC overhead from ~25 allocations per leaf × hundreds of millions

Pebble's effective rate during big-store writes was ~13k leaves/sec. The ratio gives an attribution:

- **~12% of total fast-storage upgrade time on pebble is workload-bound** (iavl + cosmos-sdk overhead)
- **~88% is pebble-specific overhead** (4 MiB memtable rotations, L0 stalls, compaction backpressure, fsync)

This is the single most actionable measurement against issue #9 — even with perfect storage, gaiad's IAVL fast-storage upgrade has a hard ~10 minute floor on cosmoshub-4 with the current iavl code. The other ~1h28m of pebble's upgrade time is recoverable through proper pebble.Options.

## Files

- `gaiad.log` — full gaiad output up to the OOM kill (ANSI-stripped)
- `bench.out` — bench harness's per-chunk + phase event log; ends with `!!! gaiad exited unexpectedly: signal: killed`
- `dmesg-oom.txt` — kernel OOM-killer record showing gaiad's RSS at kill time

## Pairs with

- `../2026-05-04-droplet-32vcpu-goleveldb/` — full run on goleveldb (1h09m to restore)
- `../2026-05-04-droplet-32vcpu-pebble/` — full run on pebble (1h41m to caught-up); per-store timing table is comparable
