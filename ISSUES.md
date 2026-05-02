# ISSUES

One-line backlog for the cosmos-p2p experiment (talking the CometBFT P2P protocol to a Cosmos Hub node and pulling a recent block).

## Plan (parts)

- [x] Part 1 — Go module bootstrap + this ISSUES.md
- [x] Part 2 — Generate/persist Ed25519 node key (`p2p.LoadOrGenNodeKey` → `data/node_key.json`)
- [x] Part 3 — Resolve a reachable cosmoshub-4 peer (`internal/peers` parses Polkachu addrbook, ranks by `last_success`)
- [x] Part 4 — Establish P2P transport: `MultiplexTransport.Listen` + `Switch.DialPeerWithAddress` (secret handshake + NodeInfo handled by cometbft)
- [x] Part 5 — Block-sync reactor (`internal/blocksync`) sends `StatusRequest` on AddPeer, then `BlockRequest` once StatusResponse arrives
- [x] Part 6 — `types.BlockFromProto` decodes the response; main prints height/hash/time/txs/proposer

## What a Cosmos Hub node needs to see before it will hand us a block

- TCP dial to its P2P port (default `26656`).
- Successful **secret handshake** (X25519 ECDH + Ed25519 auth, ChaCha20-Poly1305 framing).
- **NodeInfo** that matches the peer's expectations:
  - same `Network` chain ID (`cosmoshub-4`)
  - compatible `ProtocolVersion{P2P, Block, App}`
  - `Channels` byte-set advertising the block-sync channel `0x40`
  - non-empty `Moniker`, valid `ListenAddr`, unique `DefaultNodeID`
- An MConnection up on channel `0x40`; only then will the peer accept `BlockRequest`/`StatusRequest`.
- Peer must have block pruning that still keeps the height we ask for — for an archive-ish full node this is everything, for a default node it's only the recent window. We use `StatusResponse.{base, height}` to pick a height in range.

## Open issues / blockers

- [x] cometbft pinned to `v0.38.22` (cosmoshub-4 / gaia v27.2.0 runs this) — `ProtocolVersion{P2P:8, Block:11}`.
- [x] Peer source decided — Polkachu's `https://snapshots.polkachu.com/addrbook/cosmos/addrbook.json` (1500+ peers with `last_success` timestamps). Chain-registry `persistent_peers` is the static fallback.
- [x] Block-sync proto path confirmed — `github.com/cometbft/cometbft/proto/tendermint/blocksync` (channel `0x40`, msgs `StatusRequest`/`StatusResponse`/`BlockRequest`/`BlockResponse`/`NoBlockResponse` wrapped in `bcproto.Message`).
- [x] Seeds vs full nodes — using Polkachu addrbook entries (full nodes) by recency of `last_success`; chain-registry `seeds` would have only run PEX and dropped us.
- [x] Built-in `blocksync.Reactor` skipped — wrote a minimal custom Reactor at `internal/blocksync/reactor.go` that just speaks channel `0x40` (StatusRequest/StatusResponse/BlockRequest/BlockResponse/NoBlockResponse).
- [x] Outbound TCP works in this env — first 2 candidates timed out (firewalled), 3rd connected on first try (`148.251.6.198:26630`).
- [ ] Tx decoding deferred — block envelope prints fine via `types.Block`. To decode tx contents we'd add `cosmossdk.io/x/tx` or `github.com/cosmos/cosmos-sdk/types/tx` and the registered Msg types.

## Confirmed working

Live run output (truncated):
```
[status]   peer=9d2cd8c2e3  base=25224989  height=30922899
[block]    peer=9d2cd8c2e3  height=30922799  hash=60049B151C8B2E1727EB6CD43F3665D617EDB8788348A4897E64EA53F6CF4EB3
           time=2026-05-02T02:36:52Z  txs=6  proposer=FB4FB25A61B493A5BF8E3CD4B5E8F5B404DA8E23
```

## Possible follow-ups

- [ ] Fetch a *range* of recent blocks (loop `BlockRequest` from `height-N` to `height`).
- [ ] Verify the block by also fetching height+1 and checking `LastCommit` quorum against the validator set (would need the `state-sync` channel or an RPC fallback for the validator set at that height).
- [ ] Decode tx payloads using cosmos-sdk codec.
- [ ] Add a PEX reactor to crawl additional peers ourselves instead of depending on Polkachu's snapshot.
- [ ] Try IPv6 entries (currently filtered out in `peers.Load`).

## Notes — wire-protocol cheat sheet

- Channel `0x40` carries the wrapped oneof `bcproto.Message`. We register one `ChannelDescriptor{ID: 0x40, MessageType: &bcproto.Message{}}`; `peer.Send()` auto-wraps inner messages via the `p2p.Wrapper` interface, and inbound bytes are auto-unwrapped before reaching `Reactor.Receive()`.
- `DefaultNodeInfo.CompatibleWith` checks: same `ProtocolVersion.Block`, same `Network`, ≥1 shared channel — that's all that gates the handshake from the peer's side.
- `MultiplexTransport.Listen()` must be called (it binds a TCP listener and starts an accept goroutine) before `Switch.Start()` works — listen on `0.0.0.0:0` to avoid port conflicts; we don't actually serve incoming peers.

## Notes

- Go installed at `$HOME/sdk/go/bin/go` (1.23.4). Not on PATH; invoke by absolute path or `PATH=$HOME/sdk/go/bin:$PATH go ...`.
- Module path: `github.com/zrbecker/cosmos-p2p`.
