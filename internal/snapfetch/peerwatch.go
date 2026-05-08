package snapfetch

import (
	"context"
	"sync"
	"time"

	cmtlog "github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/p2p"
	pexcb "github.com/cometbft/cometbft/p2p/pex"

	"github.com/zrbecker/cosmos-p2p/internal/logctx"
	"github.com/zrbecker/cosmos-p2p/internal/statesync"
)

// peerWatch is the long-lived churn loop spanning walk + download. It
// listens for SnapshotsResponse events; marks each peer "useful" if it
// ever advertised a snapshot at height >= minHeight. On a 1s tick,
// peers connected for ≥ grace with no useful flag get
// StopPeerGracefully'd AND MarkBad'd in the addrbook (banDuration TTL)
// so PEX won't re-dial them this run.
//
// minHeight = 0 disables churning (peerWatch still subscribes; just
// never drops anyone).

// watchSwitch is the subset of *p2p.Switch peerWatch needs.
// Defined as an interface so tests can supply a fake.
type watchSwitch interface {
	Peers() p2p.IPeerSet
	StopPeerGracefully(p2p.Peer)
}

// watchManager is the subset of *connect.Manager peerWatch needs.
type watchManager interface {
	Ban(pid p2p.ID, reason string)
}

type peerWatch struct {
	sw                      watchSwitch
	book                    pexcb.AddrBook
	mgr                     watchManager // notified on every eviction so it stops redialing
	minHeight               uint64
	grace                   time.Duration
	banDuration             time.Duration
	requireStateSyncChannel bool
	log                     cmtlog.Logger // snapshot of logctx.From(ctx) at construction

	mu        sync.Mutex
	firstSeen map[p2p.ID]time.Time
	useful    map[p2p.ID]bool
	// banned is the set of peers we've benched — by peerWatch's own
	// tick (channel filter or no-useful-offer-in-window) or by
	// download's misbehavior path (probe timeout, hash mismatch,
	// max-redials hit). Queried via isBanned() from
	// download.tryRedial so the redial loop doesn't keep
	// re-establishing connections to peers we've already kicked.
	banned map[p2p.ID]bool
}

func newPeerWatch(ctx context.Context, sw watchSwitch, book pexcb.AddrBook, mgr watchManager, minHeight uint64, grace, banDuration time.Duration, requireStateSyncChannel bool) *peerWatch {
	return &peerWatch{
		sw:                      sw,
		book:                    book,
		mgr:                     mgr,
		minHeight:               minHeight,
		grace:                   grace,
		banDuration:             banDuration,
		requireStateSyncChannel: requireStateSyncChannel,
		log:                     logctx.From(ctx),
		firstSeen:               map[p2p.ID]time.Time{},
		useful:                  map[p2p.ID]bool{},
		banned:                  map[p2p.ID]bool{},
	}
}

// isBanned reports whether the peer was benched by any path —
// peerWatch's tick or download's misbehavior handling.
func (w *peerWatch) isBanned(id p2p.ID) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.banned[id]
}

// markUseful records that the named peer offered something inside
// our freshness window. Safe to call from any goroutine.
func (w *peerWatch) markUseful(peerID p2p.ID) {
	w.mu.Lock()
	w.useful[peerID] = true
	w.mu.Unlock()
}

// banPeer is the one-stop eviction for a connected peer: disconnect,
// addrbook-MarkBad with banDuration TTL, and flag in `banned` so
// download.tryRedial stops attempting to redial. Called from
// peerWatch's own tick (channel filter / churn-grace) AND from
// download (misbehavior).
func (w *peerWatch) banPeer(peer p2p.Peer, reason string) {
	addr := peer.SocketAddr()
	w.log.Debug("evicting peer", "peer", string(peer.ID()), "reason", reason)
	w.sw.StopPeerGracefully(peer)
	if w.book != nil {
		w.book.MarkBad(addr, w.banDuration)
	}
	if w.mgr != nil {
		w.mgr.Ban(peer.ID(), reason)
	}
	w.mu.Lock()
	delete(w.firstSeen, peer.ID())
	w.banned[peer.ID()] = true
	w.mu.Unlock()
}

// markBannedByID is the disconnected-peer counterpart to banPeer.
// Used by download() when MaxRedials is hit — the peer isn't
// currently connected so we can't StopPeerGracefully, but we still
// want to addrbook-MarkBad and flag in `banned` so tryRedial stops.
func (w *peerWatch) markBannedByID(id p2p.ID, addr string, reason string) {
	w.log.Debug("benching peer (no connection)", "peer", string(id), "reason", reason)
	if w.book != nil && addr != "" {
		if na, err := p2p.NewNetAddressString(addr); err == nil {
			w.book.MarkBad(na, w.banDuration)
		}
	}
	if w.mgr != nil {
		w.mgr.Ban(id, reason)
	}
	w.mu.Lock()
	w.banned[id] = true
	w.mu.Unlock()
}

// onConnect handles a Connected event by recording firstSeen. The
// channel filter is applied on the next tick, not here, so PEX-only
// seeds get a window to reply to AutoReactor's PexRequest before we
// disconnect them — without that window the addrbook never enriches
// past the static bootstrap list.
func (w *peerWatch) onConnect(id p2p.ID) {
	w.mu.Lock()
	if _, ok := w.firstSeen[id]; !ok {
		w.firstSeen[id] = time.Now()
	}
	w.mu.Unlock()
}

// onDisconnect handles a Removed event. Clears firstSeen so a
// reconnect gets a fresh grace window.
func (w *peerWatch) onDisconnect(id p2p.ID) {
	w.mu.Lock()
	delete(w.firstSeen, id)
	w.mu.Unlock()
}

// tick sweeps firstSeen entries for two eviction reasons: peers that
// don't advertise the snapshot channel, and peers that have been
// connected past their grace window without offering anything useful.
// Channel-filter eviction is deferred from onConnect to here so peers
// have ≥1 tick to send a PexAddrs reply before we disconnect them.
//
// Also drops firstSeen entries whose peer is gone (covers a dropped
// Removed event).
func (w *peerWatch) tick() {
	if w.minHeight == 0 {
		return
	}
	now := time.Now()
	w.mu.Lock()
	type evict struct {
		id     p2p.ID
		reason string
	}
	pending := make([]evict, 0)
	for id, first := range w.firstSeen {
		if w.sw.Peers().Get(id) == nil {
			delete(w.firstSeen, id)
			continue
		}
		if w.useful[id] {
			continue
		}
		if w.requireStateSyncChannel {
			if peer := w.sw.Peers().Get(id); peer != nil {
				di, ok := peer.NodeInfo().(p2p.DefaultNodeInfo)
				if !ok {
					pending = append(pending, evict{id, "non-default NodeInfo"})
					continue
				}
				if !di.HasChannel(statesync.SnapshotChannel) {
					pending = append(pending, evict{id, "no state-sync channel"})
					continue
				}
			}
		}
		if now.Sub(first) < w.grace {
			continue
		}
		pending = append(pending, evict{id, "no useful offer in window"})
	}
	w.mu.Unlock()
	for _, e := range pending {
		if peer := w.sw.Peers().Get(e.id); peer != nil {
			w.banPeer(peer, e.reason)
		}
	}
}

// run is the watcher's main loop. Subscribes to ctrl-only events (the
// caller supplies the channel so subscriber lifecycle matches
// peerWatch's). Returns when ctx is cancelled.
func (w *peerWatch) run(ctx context.Context, ctrl <-chan statesync.Event) {
	t := time.NewTicker(1 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			w.tick()
		case ev, ok := <-ctrl:
			if !ok {
				return
			}
			switch {
			case ev.Connected:
				w.onConnect(p2p.ID(ev.PeerID))
			case ev.Removed:
				w.onDisconnect(p2p.ID(ev.PeerID))
			case ev.Snapshot != nil:
				if w.minHeight == 0 || ev.Snapshot.Height >= w.minHeight {
					w.markUseful(p2p.ID(ev.PeerID))
				}
			}
		}
	}
}
