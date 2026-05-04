# 2026-05-04 droplet 32vCPU rapid-bootstrap (streaming + prefer-fresh) run

## Setup

- Host: same droplet as previous runs (cosmos-perf-test, c-32-intel, 64 GiB).
- Tool: `cmd/cosmos-rapid-bootstrap` with **two improvements over the first run**:
  1. **Streaming pipeline** (refactored `cmd/cosmos-snapshot-fetch` → `internal/snapfetch` library + new `snapshotappdb.ImportStream`). Chunks are streamed via channel from snapfetch into the importer's reorder buffer; the importer starts processing chunk 0 the moment it lands. Fetch + import overlap.
  2. **prefer-fresh selection fix** (`internal/snapfetch/snapfetch.go`'s `raceProbe`). Previously, the post-race tiebreaker was peer count, so an older snapshot with one more peer could beat the freshest. Now with `-prefer-fresh=true` (the rapid-bootstrap default), we pick the **freshest candidate that meets the min-peers threshold**, regardless of whether older candidates have more peers.
- Chain: cosmoshub-4.
- Snapshot chosen: format=3, height=**30,959,000** (the freshest available — 4 good peers).
- Backend: pebbledb everywhere.

## Result: 6m47s end-to-end

| Phase | Duration | Cumulative |
|---|---|---|
| Setup (wipe + mkdirs) | <1s | +0s |
| Snapfetch + ImportStream (interleaved) | **4m45s** | +4m45s |
| Pebble cleanup compaction | 5s | +4m52s |
| Cometbft `BootstrapState` | 9s | +5m02s |
| `gaiad init` (cherry-picked files) | <1s | +5m02s |
| Configure peers + backend | <1s | +5m02s |
| `gaiad start` + blocksync to tip (~1100 blocks @ ~10.5 blk/s) | 1m45s | +6m47s |
| **CAUGHT UP** | — | **+6m47s** |

## Comparison ladder (all on the same hardware/chain/day)

| Approach | Backend | Snapshot height | Total wall | Speedup vs gaiad pebble |
|---|---|---|---|---|
| gaiad state-sync | goleveldb | 30,955,000 | 1h09m+ (killed mid-catchup) | ≈ |
| gaiad state-sync | pebbledb (stock) | 30,957,000 | **1h41m** | 1× |
| rapid-bootstrap (serial fetch→import) | pebbledb (tuned) | 30,958,000 | **9m53s** | 10.3× |
| rapid-bootstrap (streaming, peer-tiebreak picker) | pebbledb (tuned) | 30,956,000 (stale, picked older) | 11m41s | 8.6× |
| **rapid-bootstrap (streaming + prefer-fresh)** | pebbledb (tuned) | **30,959,000** (freshest) | **6m47s** | **15.0×** |

## What changed between runs

- **Serial → streaming**: saved ~2:14 by overlapping fetch with import (instead of 3m14s fetch + 3m38s import sequentially, both happen in 4m45s).
- **Peer-tiebreak → prefer-fresh selector**: saved ~5 minutes of blocksync because we picked snapshot 30,959,000 instead of 30,956,000. With ~10 blk/s blocksync rate, every 1000 blocks of staleness = ~1.5 minutes.

The two improvements **compound**: streaming alone got us to 11m41s (with the bad picker); prefer-fresh alone would have kept the 9m53s serial baseline. Together they hit 6m47s.

## Selection logic — what changed

`internal/snapfetch/snapfetch.go` `raceProbe`:

**Before** (sort key after race):
```go
sort.Slice(cands, func(i, j int) bool {
    gi, gj := len(cands[i].good), len(cands[j].good)
    if gi != gj {
        return gi > gj   // primary: peer count desc
    }
    return cands[i].offer.Height > cands[j].offer.Height  // tiebreak: height
})
```
Picked top-1 from this sorted list.

**After** (with `-prefer-fresh=true`):
```go
sort.Slice(cands, func(i, j int) bool {
    return cands[i].offer.Height > cands[j].offer.Height  // by height desc
})
for _, c := range cands {
    if len(c.good) >= minGood {
        return c.offer, c.good   // pick freshest meeting threshold
    }
}
```

So with `prefer-fresh`, height is the primary key and `min-peers` is a hard threshold rather than a tiebreaker. The non-prefer-fresh path retains the old behavior unchanged.

## RAM usage during the run

Peak RSS observed: **~50 GiB** during the import phase (matches the serial run). Dropped to ~4 GiB after import returned. Breakdown (approximate):

- Pebble bulk-load memtable backlog (1 GiB × up to 8 in-flight, `DisableAutomaticCompactions=true`): 4–8 GiB
- iavl Importer's mid-store frontier (live `*Node` allocations awaiting wave-hashing): 5–10 GiB during bank/ibc
- Pebble `Cache(2 GiB)` (pre-allocated): 2 GiB
- Go runtime GC slack: ~2× active heap
- Reorder buffer: ~100 MiB (bounded by `peers × per-peer × chunk-size`)

A lower-RAM host (16–32 GiB) would need tuned-down bulk-load options (smaller `MemTableSize`, smaller `Cache`, `concurrency=1`) to avoid OOM. This profile targets 64 GiB+.

## Files

- `bench.out` — rapid-bootstrap orchestrator's event log (subprocess output is prefixed `[snap-fetch]`/`[bootstrap]`/`[appdb]`).
- `gaiad.log` — gaiad's own log from `gaiad start` until catchup. ANSI-clean (the droplet's `TERM` produces no escape codes).

## Pairs with

- `../2026-05-04-droplet-32vcpu-goleveldb/`
- `../2026-05-04-droplet-32vcpu-pebble/`
- `../2026-05-04-droplet-32vcpu-memdb-oom/`
- `../2026-05-04-droplet-32vcpu-rapid-bootstrap/` — first version (serial fetch→import)

## Caveats / open items

- **gaiad uses cosmos-db fork with 2 GiB MemTableSize** at runtime. As we measured earlier, this doesn't change blocksync rate, so the comparison is apples-to-apples on catchup speed.
- **The reorder buffer is unbounded in code**; in practice peer-side concurrency limits cap memory. A pathological adversary could exhaust memory by feeding very-large indexes early. Worth bounding for production.
- **`channelSink.OnChosen` only supports a single call**; if snapfetch's internal rescan path triggers a second `OnChosen` (rare — would only happen if all peers fail mid-stream), the streaming sink errors out and `RunFetch` exits.

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
  -bootstrap-bin /root/cosmos-p2p/cosmos-bootstrap-gaia
```

`-prefer-fresh=true` is the default; pass `-prefer-fresh=false` to revert to the peer-count tiebreaker.
