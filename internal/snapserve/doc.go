// Package snapserve runs malcom in state-sync server mode: stand up the
// same p2p/PEX/state-sync stack snapfetch builds, but with the
// statesync reactor in serving mode (responding to inbound
// SnapshotsRequest / ChunkRequest) instead of probing mode (firing
// SnapshotsRequest at every new peer).
//
// Use cases:
//
//   - reproducible state-sync fetch benchmarks (one node serves a
//     curated snapshot, the other times its fetch);
//   - bootstrap aid for known peers (put the server on a public IP and
//     it'll show up in PEX gossip just like a real seed);
//   - integration tests for snapfetch against the real wire format
//     without depending on flaky mainnet peers.
//
// Out of scope: consensus / block sync / RPC. Serve is wire-only — it
// answers SnapshotsRequest from a local snapshot directory and that's
// it. There is no application.db read path here, so a serving node
// can't be a full node.
package snapserve
