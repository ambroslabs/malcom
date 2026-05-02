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
	"time"

	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/p2p/conn"
	cmtcons "github.com/cometbft/cometbft/proto/tendermint/consensus"
	protomem "github.com/cometbft/cometbft/proto/tendermint/mempool"
	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
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

// Counters is a snapshot of per-message-type counts.
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
	TxBatches int64 // number of envelope batches received (each may carry many txs)
	Txs       int64 // total individual txs received
	UniqueTxs int64 // unique tx hashes seen this run
	TxBytes   int64 // total tx bytes seen

	// Evidence
	EvidenceItems int64

	// Unhandled message types — for debugging which types peers actually
	// send us. Keyed by Go type name (e.g. "*types.Vote").
	unknownTypes map[string]int64
}

// MaxSeenTx is the cap on unique-tx-hash dedup entries. After this we stop
// growing the dedup set; counters keep advancing but UniqueTxs tops out.
const MaxSeenTx = 200_000

type Reactor struct {
	p2p.BaseReactor
	logger log.Logger

	mu       sync.Mutex
	counters Counters
	seenTx   map[[32]byte]struct{}

	// SampleEvery, if > 0, logs roughly 1 in N tx hashes for inspection.
	SampleEvery int
	// LastTxHashes is a small ring of the most recently observed tx hashes
	// (deduplicated), surfaced for diagnostics. Capacity 32.
	lastTxRing      [32]string
	lastTxRingHead  int
	lastTxRingCount int
}

func NewReactor(logger log.Logger) *Reactor {
	r := &Reactor{
		logger:      logger,
		seenTx:      make(map[[32]byte]struct{}, 4096),
		SampleEvery: 0,
		counters:    Counters{unknownTypes: make(map[string]int64, 16)},
	}
	r.BaseReactor = *p2p.NewBaseReactor("observer", r)
	r.BaseReactor.SetLogger(logger)
	return r
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

func (r *Reactor) AddPeer(p2p.Peer)           {}
func (r *Reactor) RemovePeer(p2p.Peer, any)   {}

func (r *Reactor) Receive(env p2p.Envelope) {
	// Cometbft auto-unwraps oneof Messages before handing them to Receive,
	// so env.Message is the *inner* type (e.g. *Vote, *Proposal, *Txs).
	r.mu.Lock()
	defer r.mu.Unlock()
	switch m := env.Message.(type) {

	// Consensus messages.
	case *cmtcons.NewRoundStep:
		r.counters.NewRoundSteps++
	case *cmtcons.NewValidBlock:
		r.counters.NewValidBlocks++
	case *cmtcons.Proposal:
		r.counters.Proposals++
	case *cmtcons.ProposalPOL:
		r.counters.ProposalPols++
	case *cmtcons.BlockPart:
		r.counters.BlockParts++
	case *cmtcons.Vote:
		r.counters.Votes++
	case *cmtcons.HasVote:
		r.counters.HasVotes++
	case *cmtcons.VoteSetMaj23:
		r.counters.VoteSetMaj23++
	case *cmtcons.VoteSetBits:
		r.counters.VoteSetBits++

	// Mempool transactions.
	case *protomem.Txs:
		if m == nil {
			return
		}
		r.counters.TxBatches++
		for _, tx := range m.Txs {
			r.counters.Txs++
			r.counters.TxBytes += int64(len(tx))
			h := sha256.Sum256(tx)
			if len(r.seenTx) < MaxSeenTx {
				if _, seen := r.seenTx[h]; !seen {
					r.seenTx[h] = struct{}{}
					r.counters.UniqueTxs++
					hh := hex.EncodeToString(h[:])
					r.lastTxRing[r.lastTxRingHead] = hh
					r.lastTxRingHead = (r.lastTxRingHead + 1) % len(r.lastTxRing)
					if r.lastTxRingCount < len(r.lastTxRing) {
						r.lastTxRingCount++
					}
				}
			} else if _, seen := r.seenTx[h]; !seen {
				r.counters.UniqueTxs++
			}
		}

	// Evidence (sent as a list, not via Message wrapper).
	case *cmtproto.EvidenceList:
		r.counters.EvidenceItems += int64(len(m.Evidence))

	default:
		// Helpful when a message type isn't matched.
		r.counters.unknownTypes[goTypeName(m)]++
	}
}

func goTypeName(v interface{}) string {
	if v == nil {
		return "nil"
	}
	t := fmt.Sprintf("%T", v)
	return t
}

func (r *Reactor) Snapshot() Counters {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.counters
}

// UnknownTypes returns a copy of the unknown-message-type histogram.
func (r *Reactor) UnknownTypes() map[string]int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]int64, len(r.counters.unknownTypes))
	for k, v := range r.counters.unknownTypes {
		out[k] = v
	}
	return out
}

// RecentTxHashes returns up to 32 most recently seen unique tx hashes.
func (r *Reactor) RecentTxHashes() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
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
