package snapfetch

import (
	"encoding/hex"
	"fmt"
	"time"

	"github.com/zrbecker/cosmos-p2p/internal/statesync"
)

// Config holds all knobs for RunFetch. Field defaults are documented in
// the comments — pass zero values to opt into the defaults via Defaults().
type Config struct {
	ChainID           string
	NodeKeyPath       string
	Listen            string // default "tcp://0.0.0.0:0"
	Moniker           string // default "cosmos-p2p-snapfetch"
	AddrBook          string // path to cometbft PEX-managed addrbook (required)
	DeadPeers         string // path to cross-run dead-peer set (required)
	BootstrapPeers []string

	DiscoverFor       time.Duration // default 25s
	DialParallel      int           // default 32
	MaxCandidates     int           // default 5
	ProbeTimeout      time.Duration // default 12s
	MinGoodPeers      int           // default 1
	PerPeerLimit      int           // default 2
	ChunkTimeout      time.Duration // default 45s
	MaxFetchTime      time.Duration // default 30m
	PeerFailLimit     int           // default 3 (hash-mismatch / missing-chunk strikes before ban)
	MaxRedials        int           // default 5 (consecutive disconnect/redial cycles before benching). 0 = unlimited.
	PeerRedialBackoff time.Duration // default 5s — base backoff between redial attempts; doubles on each retry up to MaxRedialBackoff

	// MaxRedialBackoff caps the exponential backoff between redial
	// attempts to a connected peer. We never permanently ban peers for
	// being temporarily disconnected; only hash-mismatch / missing-chunk
	// strikes (PeerFailLimit) ban a peer.
	MaxRedialBackoff time.Duration // default 5m

	// WarmPeerTarget is the connected-peer count below which the
	// background refresher keeps dialing peer addrs during phase 3.
	// Without this, peer attrition during a long bank-store import can
	// starve snapfetch even though plenty of addrs are still reachable.
	WarmPeerTarget int // default 16

	// WarmRefreshInterval is how often the background refresher checks
	// the connected count and dials more peer addrs if needed.
	WarmRefreshInterval time.Duration // default 5s

	TargetHeight uint64

	// MaxHeight is the upper bound for snapshot selection. The cli
	// resolves it to the chain's current height via RPC by default.
	// Walking starts from floor(MaxHeight, SnapshotInterval).
	// Required (the walking algorithm has no useful behavior without it).
	MaxHeight uint64

	// MinHeight is the freshness floor: walking stops once the
	// candidate target height drops below this. Typically
	// MaxHeight - MaxAgeBlocks. Zero disables the floor (walks
	// all the way to height 1 — usually undesirable).
	MinHeight uint64

	// SnapshotInterval is the chain's snapshot stride (cosmoshub
	// mints every 1000 blocks). Walking decrements target by this on
	// each per-height failure.
	SnapshotInterval uint64

	// PerHeightTimeout is how long to wait for a peer to serve
	// chunk-0 at the current target height before walking back.
	PerHeightTimeout time.Duration

	// MaxOutboundPeers is the hard cap on the cometbft Switch's
	// outbound connection count.
	MaxOutboundPeers int

	// PEXTargetPeers / PEXMaxPerWave control our PEX auto-dial
	// reactor's pace. TargetPeers should be < MaxOutboundPeers.
	PEXTargetPeers int
	PEXMaxPerWave  int

	// ChurnGrace is how long a connected peer has to advertise a
	// useful snapshot before being dropped. See walkBackward.
	ChurnGrace time.Duration

	// RequireStateSyncChannel bans-on-AddPeer any peer whose NodeInfo
	// lacks the snapshot channel (0x60). When false, those peers are
	// caught later via churn-grace.
	RequireStateSyncChannel bool

	// AddrBookBanDuration is the TTL passed to book.MarkBad when
	// banning a misbehaving peer. Default 1h.
	AddrBookBanDuration time.Duration

	// ProvisionalProbeStrikes / ProvisionalProbeInflight bound a
	// PEX-arrived peer's trial period before it serves its first
	// verified chunk and gets promoted to "proven".
	ProvisionalProbeStrikes  int // default 1
	ProvisionalProbeInflight int // default 1

	// MaxDialFailures caps consecutive PEX dial failures against an
	// addrbook entry before AutoReactor deletes it from the addrbook.
	// 0 disables; the address keeps cycling through MarkBad TTLs.
	MaxDialFailures int

	MaxRescans        int           // default 3
	RescanDiscoverFor time.Duration // default 15s
}

// applyDefaults fills in zero-valued fields with defaults. Mutates cfg.
func (c *Config) applyDefaults() {
	if c.Listen == "" {
		c.Listen = "tcp://0.0.0.0:0"
	}
	if c.Moniker == "" {
		c.Moniker = "cosmos-p2p-snapfetch"
	}
	if c.DiscoverFor == 0 {
		c.DiscoverFor = 25 * time.Second
	}
	if c.DialParallel == 0 {
		c.DialParallel = 32
	}
	if c.MaxCandidates == 0 {
		c.MaxCandidates = 5
	}
	if c.ProbeTimeout == 0 {
		c.ProbeTimeout = 12 * time.Second
	}
	if c.MinGoodPeers == 0 {
		c.MinGoodPeers = 1
	}
	if c.PerPeerLimit == 0 {
		c.PerPeerLimit = 2
	}
	if c.ChunkTimeout == 0 {
		c.ChunkTimeout = 45 * time.Second
	}
	if c.MaxFetchTime == 0 {
		c.MaxFetchTime = 30 * time.Minute
	}
	if c.PeerFailLimit == 0 {
		c.PeerFailLimit = 3
	}
	if c.MaxRedials == 0 {
		c.MaxRedials = 5
	}
	if c.PeerRedialBackoff == 0 {
		c.PeerRedialBackoff = 5 * time.Second
	}
	if c.MaxRedialBackoff == 0 {
		c.MaxRedialBackoff = 5 * time.Minute
	}
	if c.AddrBookBanDuration == 0 {
		c.AddrBookBanDuration = time.Hour
	}
	if c.ProvisionalProbeStrikes == 0 {
		c.ProvisionalProbeStrikes = 1
	}
	if c.ProvisionalProbeInflight == 0 {
		c.ProvisionalProbeInflight = 1
	}
	if c.MaxDialFailures == 0 {
		c.MaxDialFailures = 3
	}
	if c.WarmPeerTarget == 0 {
		c.WarmPeerTarget = 16
	}
	if c.WarmRefreshInterval == 0 {
		c.WarmRefreshInterval = 5 * time.Second
	}
	if c.MaxRescans == 0 {
		c.MaxRescans = 3
	}
	if c.RescanDiscoverFor == 0 {
		c.RescanDiscoverFor = 15 * time.Second
	}
	if c.SnapshotInterval == 0 {
		c.SnapshotInterval = 1000
	}
	if c.PerHeightTimeout == 0 {
		c.PerHeightTimeout = 10 * time.Second
	}
	if c.MaxOutboundPeers == 0 {
		c.MaxOutboundPeers = 64
	}
	if c.PEXTargetPeers == 0 {
		c.PEXTargetPeers = 48
	}
	if c.PEXMaxPerWave == 0 {
		c.PEXMaxPerWave = 8
	}
	if c.ChurnGrace == 0 {
		c.ChurnGrace = 3 * time.Second
	}
}

// peerAddr is one candidate dial address with provenance — pulled
// from a previous addrbook.json or from the user's bootstrap_peers
// CSV. NOT a connection; just an endpoint we might try. Internal.
type peerAddr struct{ addr, source string }

// snapshotOffer is one (height, format, hash) tuple advertised by ≥1
// peer. Internal.
type snapshotOffer struct {
	Height   uint64
	Format   uint32
	Chunks   uint32
	Hash     []byte
	Metadata []byte
	Peers    map[string]bool
}

// savedMeta is the JSON shape written to <dir>/meta.json after a
// successful fetch.
type savedMeta struct {
	Height          uint64    `json:"height"`
	Format          uint32    `json:"format"`
	Chunks          uint32    `json:"chunks"`
	HashHex         string    `json:"hash_hex"`
	MetadataLen     int       `json:"metadata_len"`
	GoodPeers       []string  `json:"good_peers"`
	OfferedBy       []string  `json:"offered_by"`
	DownloadedAt    time.Time `json:"downloaded_at"`
	BytesTotal      uint64    `json:"bytes_total"`
	BytesTotalHuman string    `json:"bytes_total_human"`
}

func snapKey(s *statesync.Snapshot) string {
	return fmt.Sprintf("%d_%d_%s", s.Height, s.Format, hex.EncodeToString(s.Hash))
}
