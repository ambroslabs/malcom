package snapfetch

import (
	"context"
	"sync"
	"time"

	cmtlog "github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/p2p"
	pexcb "github.com/cometbft/cometbft/p2p/pex"

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
type peerWatch struct {
	sw                      *p2p.Switch
	book                    pexcb.AddrBook
	minHeight               uint64
	grace                   time.Duration
	banDuration             time.Duration
	requireStateSyncChannel bool
	log                     cmtlog.Logger

	mu               sync.Mutex
	firstSeen        map[p2p.ID]time.Time
	useful           map[p2p.ID]bool
	externallyBanned map[p2p.ID]bool // signaled by download(); skip in tryRedial
}

func newPeerWatch(sw *p2p.Switch, book pexcb.AddrBook, minHeight uint64, grace, banDuration time.Duration, requireStateSyncChannel bool, log cmtlog.Logger) *peerWatch {
	return &peerWatch{
		sw:                      sw,
		book:                    book,
		minHeight:               minHeight,
		grace:                   grace,
		banDuration:             banDuration,
		requireStateSyncChannel: requireStateSyncChannel,
		log:                     log,
		firstSeen:               map[p2p.ID]time.Time{},
		useful:                  map[p2p.ID]bool{},
		externallyBanned:        map[p2p.ID]bool{},
	}
}

// isBanned reports whether the peer was banned (by peerWatch's own
// tick or via download()'s misbehavior path). Used by tryRedial to
// avoid the legacy "banned-but-still-redialed" loop.
func (w *peerWatch) isBanned(id p2p.ID) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.externallyBanned[id]
}

// markUseful records that the named peer offered something inside
// our freshness window. Safe to call from any goroutine.
func (w *peerWatch) markUseful(peerID p2p.ID) {
	w.mu.Lock()
	w.useful[peerID] = true
	w.mu.Unlock()
}

// banPeer is the one-stop shop for "this peer is useless; evict it":
// disconnect + addrbook-ban + signal download() so its tryRedial
// stops dialing this peer for the rest of the run.
func (w *peerWatch) banPeer(peer p2p.Peer, reason string) {
	addr := peer.SocketAddr()
	w.log.Debug("evicting peer", "peer", string(peer.ID()), "reason", reason)
	w.sw.StopPeerGracefully(peer)
	if w.book != nil {
		w.book.MarkBad(addr, w.banDuration)
	}
	w.mu.Lock()
	delete(w.firstSeen, peer.ID())
	w.externallyBanned[peer.ID()] = true
	w.mu.Unlock()
}

// markBannedByID is the disconnected-peer counterpart to banPeer.
// Used by download() when MaxRedials is hit — the peer isn't
// currently connected so we can't StopPeerGracefully, but we still
// want to addrbook-MarkBad and signal tryRedial to stop trying.
func (w *peerWatch) markBannedByID(id p2p.ID, addr string, reason string) {
	w.log.Debug("benching peer (no connection)", "peer", string(id), "reason", reason)
	if w.book != nil && addr != "" {
		if na, err := p2p.NewNetAddressString(addr); err == nil {
			w.book.MarkBad(na, w.banDuration)
		}
	}
	w.mu.Lock()
	w.externallyBanned[id] = true
	w.mu.Unlock()
}

func (w *peerWatch) tick() {
	if w.minHeight == 0 {
		return
	}
	now := time.Now()
	for _, peer := range w.sw.Peers().List() {
		// Channel filter: bans peers whose handshake NodeInfo doesn't
		// advertise the snapshot channel (0x60). Catches relayers and
		// blocksync-only nodes immediately rather than after grace.
		if w.requireStateSyncChannel && !peer.NodeInfo().(p2p.DefaultNodeInfo).HasChannel(statesync.SnapshotChannel) {
			w.banPeer(peer, "no state-sync channel")
			continue
		}
		id := peer.ID()
		w.mu.Lock()
		if _, ok := w.firstSeen[id]; !ok {
			w.firstSeen[id] = now
			w.mu.Unlock()
			continue
		}
		if w.useful[id] {
			w.mu.Unlock()
			continue
		}
		first := w.firstSeen[id]
		w.mu.Unlock()
		if now.Sub(first) < w.grace {
			continue
		}
		w.banPeer(peer, "no useful offer in window")
	}
}

// run is the watcher's main loop. Subscribes to evs (the caller
// supplies the mux subscription so subscriber lifecycle matches
// peerWatch's). Returns when ctx is cancelled.
func (w *peerWatch) run(ctx context.Context, evs chan statesync.Event) {
	t := time.NewTicker(1 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			w.tick()
		case ev, ok := <-evs:
			if !ok {
				return
			}
			if ev.Snapshot == nil {
				continue
			}
			if w.minHeight == 0 || ev.Snapshot.Height >= w.minHeight {
				w.markUseful(p2p.ID(ev.PeerID))
			}
		}
	}
}
