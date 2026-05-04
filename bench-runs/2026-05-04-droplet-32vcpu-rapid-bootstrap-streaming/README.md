# 2026-05-04 droplet 32vCPU rapid-bootstrap (streaming, suboptimal pick) run

## Setup

- Host: same droplet as previous runs (cosmos-perf-test, c-32-intel, 64 GiB).
- Tool: `cmd/cosmos-rapid-bootstrap` after the streaming refactor (see commit history): chunks stream via channel from `internal/snapfetch.RunFetch` into `snapshotappdb.ImportStream`'s reorder buffer; fetch and import overlap.
- **At this point in the session, the post-race candidate selector still tiebroke on peer count.** That picked snapshot **30,956,000 with 5 peers** even though **30,959,000 with 4 peers was available**. The fix that picks freshest-meeting-threshold landed after this run; see `../2026-05-04-droplet-32vcpu-rapid-bootstrap-streaming-fresh/`.

## Result: 11m41s end-to-end

| Phase | Duration | Cumulative |
|---|---|---|
| Snapfetch + ImportStream (interleaved) | **4m39s** | +4m39s |
| Pebble cleanup compaction | 5s | +4m46s |
| Cometbft bootstrap-state | 8s | +4m54s |
| `gaiad init` + config edits | <1s | +4m56s |
| `gaiad start` + blocksync to tip (~3,943 blocks @ ~10 blk/s) | 6m45s | +11m41s |
| **CAUGHT UP** at 30,959,943 | — | **+11m41s** |

The streaming pipeline genuinely worked — fetch + import overlapped, saving ~2:14 vs the previous 9m53s serial run's import phase. **But** picking the older 30,956,000 instead of 30,959,000 added ~3,000 extra blocks to blocksync, which at ~10 blk/s costs **~5 minutes**. Net effect: ~3 minutes slower than the serial run with a fresher snapshot.

This is the empirical evidence that motivated the prefer-fresh fix.

## Why the picker chose the stale snapshot

`raceProbe`'s post-race sort was:

```go
sort.Slice(cands, func(i, j int) bool {
    gi, gj := len(cands[i].good), len(cands[j].good)
    if gi != gj { return gi > gj }    // primary: peer count desc
    return cands[i].offer.Height > cands[j].offer.Height
})
```

So one extra good peer at an older height beat the freshest. From the discovery output:

| Height | good peers in race |
|---|---|
| 30,959,000 | 4 |
| 30,958,000 | 4 |
| 30,957,000 | 3 |
| **30,956,000 (chosen)** | **5** |
| 30,955,000 | 3 |

The fix landed in `internal/snapfetch/snapfetch.go` between this run and the next (paired README): with `-prefer-fresh=true`, height becomes the primary key and `min-peers` is a hard threshold.

## Files

- `bench.out` — rapid-bootstrap event log
- `gaiad.log` — gaiad's catchup log

## Pairs with

- `../2026-05-04-droplet-32vcpu-rapid-bootstrap/` — serial version (9m53s)
- `../2026-05-04-droplet-32vcpu-rapid-bootstrap-streaming-fresh/` — streaming + fix (6m47s)
- `../2026-05-04-droplet-32vcpu-pebble/` — gaiad pebble state-sync (1h41m)
