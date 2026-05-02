// Package observer is a passive p2p reactor that advertises (but does not
// participate in) the cometbft mempool, consensus, and evidence channels so
// peers will gossip those messages to us. We count by message type and
// expose a snapshot for stat logging. We never send anything outbound on
// these channels — we're a sink, not a relay or a validator.
package observer

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/p2p/conn"
	cmtcons "github.com/cometbft/cometbft/proto/tendermint/consensus"
	protomem "github.com/cometbft/cometbft/proto/tendermint/mempool"
	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
	gogoproto "github.com/cosmos/gogoproto/proto"
)

// Channel IDs (matching cometbft conventions).
const (
	StateChannel       = byte(0x20) // ConsensusState — NewRoundStep, HasVote, etc.
	DataChannel        = byte(0x21) // ConsensusData — Proposal, BlockPart
	VoteChannel        = byte(0x22) // ConsensusVote — Vote
	VoteSetBitsChannel = byte(0x23) // VoteSetBits
	MempoolChannel     = byte(0x30) // Mempool — Txs (transaction gossip)
	EvidenceChannel    = byte(0x38) // Evidence — DuplicateVote, LightClientAttack
)

// Counters is a snapshot of per-message-type counts. Plain int64 values for
// caller convenience; the live atomics inside Reactor get loaded into here.
type Counters struct {
	// Consensus
	NewRoundSteps  int64
	NewValidBlocks int64
	Proposals      int64
	ProposalPols   int64
	BlockParts     int64
	Votes          int64
	HasVotes       int64
	VoteSetMaj23   int64
	VoteSetBits    int64

	// Mempool
	TxBatches int64
	Txs       int64
	UniqueTxs int64
	TxBytes   int64

	// Relay
	VotesRelayed      int64
	BlockPartsRelayed int64
	ProposalsRelayed  int64
	TxsRelayed        int64

	// Evidence
	EvidenceItems int64
}

// MaxSeenTx is the cap on unique-tx-hash dedup entries. After this we stop
// growing the dedup set; counters keep advancing but UniqueTxs tops out.
const MaxSeenTx = 200_000

type Reactor struct {
	p2p.BaseReactor
	logger log.Logger

	// All hot-path counters are atomic int64 keyed by channel ID into a
	// fixed-size array. Avoids the lock contention that starves MConn
	// ping/pong responses under high inbound traffic.
	bytesRecv [256]int64
	bytesSent [256]int64

	// Message-type counters (also atomic; one Receive event = one increment).
	cNewRoundSteps  int64
	cNewValidBlocks int64
	cProposals      int64
	cProposalPols   int64
	cBlockParts     int64
	cVotes          int64
	cHasVotes       int64
	cVoteSetMaj23   int64
	cVoteSetBits    int64
	cTxBatches      int64
	cTxs            int64
	cUniqueTxs      int64
	cTxBytes        int64
	cEvidenceItems  int64

	// Relay activity (atomic; only meaningful when RelayConsensus / RelayTxs).
	cVotesRelayed      int64
	cBlockPartsRelayed int64
	cProposalsRelayed  int64
	cTxsRelayed        int64
	stepSent           int64

	// SeenTx is a map and needs a mutex; same for the ring buffer.
	muSeen          sync.Mutex
	seenTx          map[[32]byte]struct{}
	lastTxRing      [32]string
	lastTxRingHead  int
	lastTxRingCount int

	// SampleEvery, if > 0, logs roughly 1 in N tx hashes.
	SampleEvery int

	// tipFn returns the height of the latest block in our cache. We claim
	// that we're working on tipFn()+1 in NewRoundStep messages so peers
	// gate-keeping mempool / vote / blockpart / proposal gossip on
	// peerState.GetHeight() will treat us as caught up. We never sign or
	// vote — purely an announcement.
	tipFn func() int64
	// HeartbeatInterval is how often we re-broadcast our NewRoundStep so
	// peers' tracking advances as our cache does.
	HeartbeatInterval time.Duration

	// RelayTxs, when true, forwards each unique mempool tx (deduped via
	// seenTx) to all connected peers except the sender. We do NOT run
	// CheckTx — chain-agnostic. Receivers run CheckTx themselves and drop
	// invalid txs, so the worst-case cost of relay is N CheckTx invocations
	// per bad tx (no further amplification because the receivers don't
	// re-gossip what they reject).
	RelayTxs bool

	// RelayConsensus, when true, forwards every received Vote / BlockPart /
	// Proposal to all connected peers except the sender. WITHOUT a per-vote
	// dedup, this is N²-amplifying: each unique vote that all N peers send
	// us once gets relayed N×(N-1) times in total. At cosmoshub's ~60
	// unique votes/sec × 50 peers, that's ~150K send events/sec ≈ 30 MB/s
	// outbound just from votes, plus block parts and proposals.
	// Default OFF; only enable if you have bandwidth to spend.
	RelayConsensus bool
}

func NewReactor(logger log.Logger) *Reactor {
	r := &Reactor{
		logger:            logger,
		seenTx:            make(map[[32]byte]struct{}, 4096),
		SampleEvery:       0,
		HeartbeatInterval: 3 * time.Second,
		RelayTxs:          false,
		RelayConsensus:    false,
	}
	r.BaseReactor = *p2p.NewBaseReactor("observer", r)
	r.BaseReactor.SetLogger(logger)
	return r
}

// SetTipFn provides a callback returning the height of the latest block we
// have. We claim height = tip+1 in NewRoundStep so peers gate-keeping mempool
// and consensus gossip on peerState.GetHeight() let traffic through.
// Call before Start; not safe to change at runtime.
func (r *Reactor) SetTipFn(fn func() int64) {
	r.tipFn = fn
}

func (r *Reactor) GetChannels() []*conn.ChannelDescriptor {
	// Generous receive buffers; cosmoshub mempool batches can be sizeable.
	const (
		bigRecv  = 1 << 24 // 16 MiB
		smallRecv = 1 << 20 // 1 MiB
	)
	return []*conn.ChannelDescriptor{
		{ID: StateChannel, Priority: 6, SendQueueCapacity: 100, RecvBufferCapacity: smallRecv, RecvMessageCapacity: smallRecv, MessageType: &cmtcons.Message{}},
		{ID: DataChannel, Priority: 10, SendQueueCapacity: 100, RecvBufferCapacity: bigRecv, RecvMessageCapacity: bigRecv, MessageType: &cmtcons.Message{}},
		{ID: VoteChannel, Priority: 7, SendQueueCapacity: 100, RecvBufferCapacity: smallRecv, RecvMessageCapacity: smallRecv, MessageType: &cmtcons.Message{}},
		{ID: VoteSetBitsChannel, Priority: 1, SendQueueCapacity: 2, RecvBufferCapacity: smallRecv, RecvMessageCapacity: smallRecv, MessageType: &cmtcons.Message{}},
		{ID: MempoolChannel, Priority: 5, SendQueueCapacity: 100, RecvBufferCapacity: bigRecv, RecvMessageCapacity: bigRecv, MessageType: &protomem.Message{}},
		{ID: EvidenceChannel, Priority: 6, SendQueueCapacity: 100, RecvBufferCapacity: smallRecv, RecvMessageCapacity: smallRecv, MessageType: &cmtproto.EvidenceList{}},
	}
}

func (r *Reactor) AddPeer(peer p2p.Peer) {
	r.sendStateClaim(peer)
}

func (r *Reactor) RemovePeer(p2p.Peer, any) {}

// sendStateClaim broadcasts our claimed (height, round, step) to one peer.
// Peers gate Vote/Proposal/BlockPart and (importantly) mempool Tx gossip on
// our claimed height matching their consensus height.
func (r *Reactor) sendStateClaim(peer p2p.Peer) {
	if r.tipFn == nil {
		return
	}
	tip := r.tipFn()
	if tip <= 0 {
		return
	}
	msg := &cmtcons.NewRoundStep{
		Height:                tip + 1,
		Round:                 0,
		Step:                  1,
		SecondsSinceStartTime: 0,
		LastCommitRound:       0,
	}
	if peer.TrySend(p2p.Envelope{ChannelID: StateChannel, Message: msg}) {
		atomic.AddInt64(&r.stepSent, 1)
		atomic.AddInt64(&r.bytesSent[StateChannel], int64(gogoproto.Size(msg)))
	}
}

// Heartbeat re-sends our state claim to all connected peers on every tick
// so peers' tracking advances as the chain advances.
func (r *Reactor) Heartbeat(stop <-chan struct{}) {
	t := time.NewTicker(r.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			if r.Switch == nil {
				continue
			}
			for _, p := range r.Switch.Peers().List() {
				r.sendStateClaim(p)
			}
		}
	}
}

// relayConsensus broadcasts a consensus message to every connected peer
// except the sender. These messages are signature-verifiable end-to-end, so
// relaying without "running" them is safe — recipients verify themselves.
func (r *Reactor) relayConsensus(env p2p.Envelope, ch byte, msg gogoproto.Message) {
	if r.Switch == nil {
		return
	}
	senderID := env.Src.ID()
	wire := int64(gogoproto.Size(msg))
	sent := int64(0)
	for _, p := range r.Switch.Peers().List() {
		if p.ID() == senderID {
			continue
		}
		// TrySend so a slow peer doesn't block our hot path.
		if p.TrySend(p2p.Envelope{ChannelID: ch, Message: msg}) {
			sent++
		}
	}
	atomic.AddInt64(&r.bytesSent[ch], wire*sent)
}

// relayTxs forwards a batch of mempool txs to every connected peer except
// the sender. We do NOT run CheckTx — recipients verify themselves and
// reject invalid txs (cometbft's mempool reactor does this). Each tx is
// relayed only once (dedup happens in Receive via seenTx). Bandwidth cost
// to us: tx_size × (peers−1) per unique tx. Recipients' worst-case cost
// for a bad tx: one CheckTx invocation, then they drop it (no further
// amplification, because they don't re-gossip what they reject).
func (r *Reactor) relayTxs(sender p2p.ID, txs [][]byte) {
	if r.Switch == nil || len(txs) == 0 {
		return
	}
	msg := &protomem.Txs{Txs: txs}
	wire := int64(gogoproto.Size(msg))
	sent := int64(0)
	for _, p := range r.Switch.Peers().List() {
		if p.ID() == sender {
			continue
		}
		if p.TrySend(p2p.Envelope{ChannelID: MempoolChannel, Message: msg}) {
			sent++
		}
	}
	atomic.AddInt64(&r.bytesSent[MempoolChannel], wire*sent)
}

func (r *Reactor) Receive(env p2p.Envelope) {
	// Cometbft auto-unwraps oneof Messages before handing them to Receive,
	// so env.Message is the *inner* type (e.g. *Vote, *Proposal, *Txs).
	//
	// Hot path is lock-free: all counters are atomic int64. We only take a
	// mutex around the seenTx/lastTxRing update for mempool dedup. This
	// avoids serializing every inbound message on a shared lock and starving
	// MConn ping/pongs (which caused mass disconnects under cosmoshub load).
	if pm, ok := env.Message.(gogoproto.Message); ok {
		atomic.AddInt64(&r.bytesRecv[env.ChannelID], int64(gogoproto.Size(pm)))
	}

	switch m := env.Message.(type) {

	case *cmtcons.NewRoundStep:
		atomic.AddInt64(&r.cNewRoundSteps, 1)
	case *cmtcons.NewValidBlock:
		atomic.AddInt64(&r.cNewValidBlocks, 1)
	case *cmtcons.Proposal:
		atomic.AddInt64(&r.cProposals, 1)
		if r.RelayConsensus {
			atomic.AddInt64(&r.cProposalsRelayed, 1)
			r.relayConsensus(env, DataChannel, m)
		}
	case *cmtcons.ProposalPOL:
		atomic.AddInt64(&r.cProposalPols, 1)
	case *cmtcons.BlockPart:
		atomic.AddInt64(&r.cBlockParts, 1)
		if r.RelayConsensus {
			atomic.AddInt64(&r.cBlockPartsRelayed, 1)
			r.relayConsensus(env, DataChannel, m)
		}
	case *cmtcons.Vote:
		atomic.AddInt64(&r.cVotes, 1)
		if r.RelayConsensus {
			atomic.AddInt64(&r.cVotesRelayed, 1)
			r.relayConsensus(env, VoteChannel, m)
		}
	case *cmtcons.HasVote:
		atomic.AddInt64(&r.cHasVotes, 1)
	case *cmtcons.VoteSetMaj23:
		atomic.AddInt64(&r.cVoteSetMaj23, 1)
	case *cmtcons.VoteSetBits:
		atomic.AddInt64(&r.cVoteSetBits, 1)

	case *protomem.Txs:
		if m == nil {
			return
		}
		atomic.AddInt64(&r.cTxBatches, 1)
		// Hash all txs first (CPU work) then dedup under muSeen briefly.
		hashes := make([][32]byte, len(m.Txs))
		for i, tx := range m.Txs {
			hashes[i] = sha256.Sum256(tx)
			atomic.AddInt64(&r.cTxs, 1)
			atomic.AddInt64(&r.cTxBytes, int64(len(tx)))
		}
		var freshTxs [][]byte
		r.muSeen.Lock()
		for i, h := range hashes {
			if len(r.seenTx) < MaxSeenTx {
				if _, seen := r.seenTx[h]; !seen {
					r.seenTx[h] = struct{}{}
					hh := hex.EncodeToString(h[:])
					r.lastTxRing[r.lastTxRingHead] = hh
					r.lastTxRingHead = (r.lastTxRingHead + 1) % len(r.lastTxRing)
					if r.lastTxRingCount < len(r.lastTxRing) {
						r.lastTxRingCount++
					}
					atomic.AddInt64(&r.cUniqueTxs, 1)
					if r.RelayTxs {
						freshTxs = append(freshTxs, m.Txs[i])
					}
				}
			} else if _, seen := r.seenTx[h]; !seen {
				atomic.AddInt64(&r.cUniqueTxs, 1)
				if r.RelayTxs {
					freshTxs = append(freshTxs, m.Txs[i])
				}
			}
		}
		r.muSeen.Unlock()
		if len(freshTxs) > 0 {
			atomic.AddInt64(&r.cTxsRelayed, int64(len(freshTxs)))
			r.relayTxs(env.Src.ID(), freshTxs)
		}

	case *cmtproto.EvidenceList:
		atomic.AddInt64(&r.cEvidenceItems, int64(len(m.Evidence)))
	}
}

func goTypeName(v interface{}) string {
	if v == nil {
		return "nil"
	}
	t := fmt.Sprintf("%T", v)
	return t
}

// Snapshot returns a consistent copy of all counters via atomic loads.
func (r *Reactor) Snapshot() Counters {
	return Counters{
		NewRoundSteps:     atomic.LoadInt64(&r.cNewRoundSteps),
		NewValidBlocks:    atomic.LoadInt64(&r.cNewValidBlocks),
		Proposals:         atomic.LoadInt64(&r.cProposals),
		ProposalPols:      atomic.LoadInt64(&r.cProposalPols),
		BlockParts:        atomic.LoadInt64(&r.cBlockParts),
		Votes:             atomic.LoadInt64(&r.cVotes),
		HasVotes:          atomic.LoadInt64(&r.cHasVotes),
		VoteSetMaj23:      atomic.LoadInt64(&r.cVoteSetMaj23),
		VoteSetBits:       atomic.LoadInt64(&r.cVoteSetBits),
		TxBatches:         atomic.LoadInt64(&r.cTxBatches),
		Txs:               atomic.LoadInt64(&r.cTxs),
		UniqueTxs:         atomic.LoadInt64(&r.cUniqueTxs),
		TxBytes:           atomic.LoadInt64(&r.cTxBytes),
		EvidenceItems:     atomic.LoadInt64(&r.cEvidenceItems),
		VotesRelayed:      atomic.LoadInt64(&r.cVotesRelayed),
		BlockPartsRelayed: atomic.LoadInt64(&r.cBlockPartsRelayed),
		ProposalsRelayed:  atomic.LoadInt64(&r.cProposalsRelayed),
		TxsRelayed:        atomic.LoadInt64(&r.cTxsRelayed),
	}
}

// BytesPerChannel returns recv/sent byte counters keyed by channel ID.
func (r *Reactor) BytesPerChannel() (recv, sent map[byte]int64) {
	recv = make(map[byte]int64, 8)
	sent = make(map[byte]int64, 8)
	for ch := 0; ch < 256; ch++ {
		rb := atomic.LoadInt64(&r.bytesRecv[ch])
		sb := atomic.LoadInt64(&r.bytesSent[ch])
		if rb > 0 {
			recv[byte(ch)] = rb
		}
		if sb > 0 {
			sent[byte(ch)] = sb
		}
	}
	return
}

// RecentTxHashes returns up to 32 most recently seen unique tx hashes.
func (r *Reactor) RecentTxHashes() []string {
	r.muSeen.Lock()
	defer r.muSeen.Unlock()
	out := make([]string, 0, r.lastTxRingCount)
	for i := 0; i < r.lastTxRingCount; i++ {
		idx := (r.lastTxRingHead - 1 - i + len(r.lastTxRing)) % len(r.lastTxRing)
		if r.lastTxRing[idx] != "" {
			out = append(out, r.lastTxRing[idx])
		}
	}
	return out
}

// suppress unused-import warning when SampleEvery isn't wired up.
var _ = time.Now
