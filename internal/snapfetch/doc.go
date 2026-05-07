// Package snapfetch is the reusable core behind `malcom snapshot fetch`.
// It walks candidate snapshot heights against state-sync peers, picks
// the freshest offer that any peer will serve, downloads every chunk
// in parallel, verifies each against the per-chunk SHA256 in the
// snapshot Metadata blob, and writes everything under a per-snapshot
// directory ready for `malcom snapshot import` to consume.
package snapfetch
