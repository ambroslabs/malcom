// Package crawler walks the CometBFT P2P gossip graph using PEX and asks
// each reachable peer for its block range via StatusRequest.
//
// One Switch is shared across the run. Workers pull addresses from a queue
// and call Switch.DialPeerWithAddress. Successful peers fire AddPeer on the
// PEX and BLOCKSYNC reactors, which push results back into this crawler.
// Once we've recorded a peer's status we drop the connection so the slot
// can be reused for another candidate.
package crawler

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/p2p"

	"github.com/zrbecker/cosmos-p2p/internal/blocksync"
	"github.com/zrbecker/cosmos-p2p/internal/pex"
)

type PeerRecord struct {
	Addr         string    `json:"addr"`
	NodeID       string    `json:"node_id"`
	Host         string    `json:"host"`
	Port         int       `json:"port"`
	Source       string    `json:"source,omitempty"`
	Connected    bool      `json:"connected"`
	Moniker      string    `json:"moniker,omitempty"`
	Network      string    `json:"network,omitempty"`
	Version      string    `json:"version,omitempty"`
	BaseHeight   int64     `json:"base_height,omitempty"`
	LatestHeight int64     `json:"latest_height,omitempty"`
	DialErr      string    `json:"dial_err,omitempty"`
	DialedAt     time.Time `json:"dialed_at,omitempty"`
	GotStatusAt  time.Time `json:"got_status_at,omitempty"`
}

type Crawler struct {
	sw     *p2p.Switch
	self   p2p.ID
	pex    *pex.Reactor
	bs     *blocksync.Reactor
	logger log.Logger

	mu       sync.Mutex
	seen     map[string]*PeerRecord // by node ID
	queue    chan string
	parallel int

	// PostStatusGrace is how long we keep a connection alive after the
	// peer's StatusResponse arrives, so its PexAddrs reply has time to come
	// back before we disconnect.
	PostStatusGrace time.Duration

	// RetryInterval, if > 0, requeues every peer-without-status plus
	// AlwaysRetry on this cadence. Catches transient timeouts and pulls
	// fresh PEX batches from seed-mode peers we previously talked to.
	RetryInterval time.Duration
	// AlwaysRetry holds addresses (typically chain-registry seeds) that are
	// re-queued every retry tick regardless of whether they've responded.
	AlwaysRetry []string
}

func New(sw *p2p.Switch, self p2p.ID, pexR *pex.Reactor, bsR *blocksync.Reactor, logger log.Logger, parallel int) *Crawler {
	if parallel < 1 {
		parallel = 1
	}
	return &Crawler{
		sw:              sw,
		self:            self,
		pex:             pexR,
		bs:              bsR,
		logger:          logger,
		seen:            map[string]*PeerRecord{},
		queue:           make(chan string, 65536),
		parallel:        parallel,
		PostStatusGrace: 3 * time.Second,
		RetryInterval:   60 * time.Second,
	}
}

// Seed enqueues the initial peer addresses (e.g. addrbook entries).
func (c *Crawler) Seed(addrs []string) {
	for _, a := range addrs {
		c.enqueue(a, "seed")
	}
}

// Preload installs PeerRecords from a previous run into the seen-set and
// queues their addresses for redial. The records keep their previously-
// observed base/height/moniker so a single bad re-dial doesn't erase known
// history. Call before Seed.
func (c *Crawler) Preload(records []PeerRecord) {
	c.mu.Lock()
	queued := make([]string, 0, len(records))
	for _, r := range records {
		if r.NodeID == "" || p2p.ID(r.NodeID) == c.self {
			continue
		}
		if _, ok := c.seen[r.NodeID]; ok {
			continue
		}
		cp := r
		// Reset transient flags; we'll re-discover live state during the run.
		cp.Connected = false
		cp.DialedAt = time.Time{}
		c.seen[r.NodeID] = &cp
		if r.Addr != "" {
			queued = append(queued, r.Addr)
		}
	}
	c.mu.Unlock()

	for _, addr := range queued {
		select {
		case c.queue <- addr:
		default:
			c.logger.Error("preload: queue full", "queued_so_far", len(queued))
			return
		}
	}
}

// enqueue records a new peer (if unseen) and pushes it into the dial queue.
// Returns true if it was actually new.
func (c *Crawler) enqueue(addr, source string) bool {
	parts := strings.SplitN(addr, "@", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return false
	}
	nodeID := parts[0]
	if p2p.ID(nodeID) == c.self {
		return false
	}
	c.mu.Lock()
	if _, ok := c.seen[nodeID]; ok {
		c.mu.Unlock()
		return false
	}
	host, portStr, _ := strings.Cut(parts[1], ":")
	port, _ := strconv.Atoi(portStr)
	c.seen[nodeID] = &PeerRecord{
		Addr: addr, NodeID: nodeID, Host: host, Port: port, Source: source,
	}
	c.mu.Unlock()

	select {
	case c.queue <- addr:
		return true
	default:
		c.logger.Error("dial queue full; dropping peer", "addr", addr)
		return false
	}
}

// Run drives the crawl until ctx is cancelled. It blocks until then.
func (c *Crawler) Run(ctx context.Context) {
	var wg sync.WaitGroup

	// PEX consumer: incoming address batches → enqueue.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case ev := <-c.pex.Out:
				added := 0
				for _, na := range ev.Addrs {
					addr := fmt.Sprintf("%s@%s:%d", string(na.ID), na.IP, na.Port)
					if c.enqueue(addr, ev.Source) {
						added++
					}
				}
				if added > 0 {
					c.logger.Info("PEX batch", "from", ev.Source, "new", added, "of", len(ev.Addrs))
				}
			}
		}
	}()

	// Status consumer: record (base, height), then disconnect to free slot.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case res := <-c.bs.Done:
				if res.Status == nil {
					continue
				}
				c.mu.Lock()
				rec, ok := c.seen[res.PeerID]
				if ok {
					rec.BaseHeight = res.Status.Base
					rec.LatestHeight = res.Status.Height
					rec.GotStatusAt = time.Now()
					rec.Connected = true
				}
				c.mu.Unlock()
				if !ok {
					continue
				}
				peer := c.sw.Peers().Get(p2p.ID(res.PeerID))
				if peer == nil {
					continue
				}
				if di, ok := peer.NodeInfo().(p2p.DefaultNodeInfo); ok {
					c.mu.Lock()
					rec.Moniker = di.Moniker
					rec.Network = di.Network
					rec.Version = di.Version
					c.mu.Unlock()
				}
				// Don't disconnect yet — give PEX a moment to respond. We
				// already sent PexRequest in pex.AddPeer; the peer's reply
				// is asynchronous and was being clobbered by an immediate
				// StopPeerGracefully here.
				peerID := p2p.ID(res.PeerID)
				time.AfterFunc(c.PostStatusGrace, func() {
					if p := c.sw.Peers().Get(peerID); p != nil {
						c.sw.StopPeerGracefully(p)
					}
				})
			}
		}
	}()

	// Dial workers.
	for i := 0; i < c.parallel; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case addr := <-c.queue:
					c.dial(ctx, addr)
				}
			}
		}()
	}

	// Periodic progress.
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				disc, conn, status := c.counts()
				c.logger.Info("progress",
					"discovered", disc,
					"connected", conn,
					"with_status", status,
					"queued", len(c.queue))
			}
		}
	}()

	// Retry tick: requeue peers without status + always-retry list (seeds).
	if c.RetryInterval > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			t := time.NewTicker(c.RetryInterval)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					c.requeueRetry()
				}
			}
		}()
	}

	<-ctx.Done()
	// Reactors hold no resources we need to drain; the Switch.Stop in main
	// will tear them down. We return immediately rather than wg.Wait()
	// because the goroutines block on channels we don't drain at shutdown.
}

func (c *Crawler) dial(ctx context.Context, addr string) {
	na, err := p2p.NewNetAddressString(addr)
	if err != nil {
		c.recordErr(addr, fmt.Errorf("parse: %w", err))
		return
	}
	c.mu.Lock()
	if rec, ok := c.seen[string(na.ID)]; ok {
		rec.DialedAt = time.Now()
	}
	c.mu.Unlock()

	if err := c.sw.DialPeerWithAddress(na); err != nil {
		c.recordErr(addr, err)
	}
}

func (c *Crawler) recordErr(addr string, err error) {
	parts := strings.SplitN(addr, "@", 2)
	if len(parts) == 0 {
		return
	}
	nodeID := parts[0]
	c.mu.Lock()
	defer c.mu.Unlock()
	if rec, ok := c.seen[nodeID]; ok {
		// Don't overwrite a successful connect with a noisy "duplicate" or
		// "self" error from a later attempt.
		if rec.LatestHeight == 0 && rec.DialErr == "" {
			rec.DialErr = err.Error()
		}
	}
}

// requeueRetry pushes addresses back into the dial queue for another try.
// We re-dial every peer that hasn't given us a status yet, plus the
// AlwaysRetry list (seeds). The Switch dedups currently-connected and
// currently-dialing peers, so this is safe.
func (c *Crawler) requeueRetry() {
	c.mu.Lock()
	addrs := make([]string, 0, len(c.seen)+len(c.AlwaysRetry))
	for _, r := range c.seen {
		if r.LatestHeight == 0 && r.Addr != "" {
			addrs = append(addrs, r.Addr)
		}
	}
	c.mu.Unlock()
	addrs = append(addrs, c.AlwaysRetry...)

	queued := 0
	for _, a := range addrs {
		select {
		case c.queue <- a:
			queued++
		default:
			c.logger.Error("retry: queue full", "queued", queued, "of", len(addrs))
			return
		}
	}
	c.logger.Info("retry tick", "requeued", queued, "candidates", len(addrs))
}

func (c *Crawler) counts() (discovered, connected, withStatus int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	discovered = len(c.seen)
	for _, r := range c.seen {
		if r.Connected {
			connected++
		}
		if r.LatestHeight > 0 {
			withStatus++
		}
	}
	return
}

// Snapshot returns a copy of the peer records, sorted by latest_height desc.
func (c *Crawler) Snapshot() []PeerRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]PeerRecord, 0, len(c.seen))
	for _, r := range c.seen {
		out = append(out, *r)
	}
	// Sort by base_height ascending (deepest history first); peers with no
	// status (base==0) bubble to the end.
	sort.Slice(out, func(i, j int) bool {
		ai, aj := out[i].BaseHeight, out[j].BaseHeight
		if ai == 0 && aj == 0 {
			return out[i].NodeID < out[j].NodeID
		}
		if ai == 0 {
			return false
		}
		if aj == 0 {
			return true
		}
		if ai != aj {
			return ai < aj
		}
		return out[i].LatestHeight > out[j].LatestHeight
	})
	return out
}
