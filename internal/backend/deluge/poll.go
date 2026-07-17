// The poll/event loop: reconnection, hot/cold status polling, idle
// sweep, and backend event synthesis (04-deluge-backend.md).
package deluge

import (
	"context"
	"strings"
	"time"

	"github.com/jonny-gm/torrentseek/internal/backend"
)

// --- poll / event loop ---

func (b *Backend) loop() {
	defer close(b.done)
	hot := time.NewTicker(b.cfg.HotPoll)
	defer hot.Stop()
	cold := time.NewTicker(b.cfg.ColdPoll)
	defer cold.Stop()

	backoff := b.cfg.HotPoll
	for {
		b.mu.Lock()
		cur := b.c
		b.mu.Unlock()

		if cur == nil {
			// Reconnect with exponential backoff; sent-state was already
			// cleared when the connection died, so the first flush after
			// reconnect re-asserts everything.
			select {
			case <-b.quit:
				return
			case <-time.After(backoff):
			}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			c, err := dial(ctx, b.cfg.Addr, b.cfg.Username, b.cfg.Password)
			cancel()
			if err != nil {
				b.cfg.Log.Warn("deluge: reconnect failed", "err", err)
				backoff = min(backoff*2, 30*time.Second)
				continue
			}
			b.cfg.Log.Info("deluge: reconnected")
			backoff = b.cfg.HotPoll
			b.mu.Lock()
			b.c = c
			b.mu.Unlock()
			continue
		}

		select {
		case <-b.quit:
			return
		case <-cur.done:
			b.mu.Lock()
			b.c = nil
			for _, ts := range b.torrents {
				ts.sentPieces = nil
				ts.sentWanted = ""
			}
			b.mu.Unlock()
			b.cfg.Log.Warn("deluge: connection lost")
		case ev := <-cur.events:
			b.handleDaemonEvent(ev)
		case <-hot.C:
			b.pollHot(cur)
			b.idleSweep(cur)
		case <-cold.C:
			b.pollCold(cur)
		}
	}
}

func (b *Backend) handleDaemonEvent(ev event) {
	getID := func() (backend.ID, bool) {
		if len(ev.args) == 0 {
			return "", false
		}
		id, err := backend.ParseID("btih:" + strings.ToLower(asString(ev.args[0])))
		return id, err == nil
	}
	switch ev.name {
	case "TorrentAddedEvent":
		if id, ok := getID(); ok {
			b.emit(backend.Event{Type: backend.EventTorrentAdded, Torrent: id})
		}
	case "TorrentRemovedEvent":
		if id, ok := getID(); ok {
			b.mu.Lock()
			delete(b.torrents, id)
			b.mu.Unlock()
			b.emit(backend.Event{Type: backend.EventTorrentRemoved, Torrent: id})
		}
	case "TorrentFileCompletedEvent", "TorrentFinishedEvent":
		// Piece-level state is poll-derived; just make the next hot tick
		// pick this torrent up.
		if id, ok := getID(); ok {
			b.mu.Lock()
			if ts, ok := b.torrents[id]; ok {
				ts.hotUntil = time.Now().Add(b.cfg.HotWindow)
			}
			b.mu.Unlock()
		}
	}
}

// pollHot refreshes torrents with active interest in one batched call and
// re-flushes any whose sent-state was cleared by a failure or reconnect —
// or that the fresh snapshot shows paused while pieces are still wanted
// (flush's wake then resumes them; see the wake comment for how a torrent
// with active interest can end up paused).
func (b *Backend) pollHot(c *conn) {
	b.mu.Lock()
	now := time.Now()
	var ids []any
	for id, ts := range b.torrents {
		if now.Before(ts.hotUntil) {
			ids = append(ids, rawID(id))
		}
	}
	b.mu.Unlock()
	if len(ids) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	all, err := b.fetchAll(ctx, c, map[any]any{"id": ids})
	if err != nil {
		b.cfg.Log.Warn("deluge: hot poll failed", "err", err)
		return
	}
	b.ingest(all)

	// Decide re-asserts against the post-ingest snapshot so the paused
	// check sees current state, not the pre-poll cache.
	b.mu.Lock()
	var reassert []backend.ID
	for id, ts := range b.torrents {
		if now.Before(ts.hotUntil) && len(ts.desired) > 0 &&
			(ts.sentPieces == nil || ts.info.State == backend.StateStopped) {
			reassert = append(reassert, id)
		}
	}
	b.mu.Unlock()

	for _, id := range reassert {
		if err := b.flush(ctx, id); err != nil {
			b.cfg.Log.Warn("deluge: re-assert failed", "torrent", id, "err", err)
		}
	}
}

// idleSweep pauses own-add torrents whose interest ended more than
// IdleGrace ago (see flush's idle-grace comment: pausing on a momentary
// zero-interest gap would disconnect the very swarm a media player's next
// request needs milliseconds later). The pause lands first — a graceful
// disconnect — and only then are file priorities zeroed, so the torrent
// is never active with every file unwanted, a state that presents to the
// swarm as a seed and gets peer connections dropped and backoff-banned.
func (b *Backend) idleSweep(c *conn) {
	b.mu.Lock()
	now := time.Now()
	type target struct {
		id backend.ID
		n  int
	}
	var expired []target
	for id, ts := range b.torrents {
		if ts.ownAdd && !ts.idleSince.IsZero() && now.Sub(ts.idleSince) >= b.cfg.IdleGrace {
			expired = append(expired, target{id, len(ts.files)})
		}
	}
	b.mu.Unlock()
	if len(expired) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, t := range expired {
		if _, err := c.callTimeout(ctx, "core.pause_torrent",
			[]any{rawID(t.id)}, nil); err != nil {
			b.cfg.Log.Warn("deluge: idle pause failed", "torrent", t.id, "err", err)
			continue // leave idleSince set; retried next sweep
		}
		prios := make([]any, t.n)
		for i := range prios {
			prios[i] = 0
		}
		if _, err := c.callTimeout(ctx, "core.set_torrent_options",
			[]any{[]any{rawID(t.id)}, map[any]any{"file_priorities": prios}}, nil); err != nil {
			b.cfg.Log.Warn("deluge: idle file-zeroing failed", "torrent", t.id, "err", err)
		}
		b.mu.Lock()
		if ts, ok := b.torrents[t.id]; ok {
			ts.idleSince = time.Time{}
			ts.sentWanted = "[]" // next interest triggers a full wake
		}
		b.mu.Unlock()
	}
}

// pollCold sweeps every torrent, noticing external adds/removes and
// metadata resolution the push events may have missed.
func (b *Backend) pollCold(c *conn) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	all, err := b.fetchAll(ctx, c, nil)
	if err != nil {
		b.cfg.Log.Warn("deluge: cold poll failed", "err", err)
		return
	}
	seen := b.ingest(all)

	b.mu.Lock()
	var removed []backend.ID
	for id := range b.torrents {
		if !seen[id] {
			removed = append(removed, id)
			delete(b.torrents, id)
		}
	}
	b.mu.Unlock()
	for _, id := range removed {
		b.emit(backend.Event{Type: backend.EventTorrentRemoved, Torrent: id})
	}
}

// ingest merges a get_torrents_status result into the cache and pauses
// magnet torrents this backend added whose metadata just resolved with no
// interest registered — magnets have to run to fetch metadata, but once
// resolved they idle paused like any other add (externally-added torrents
// are never touched).
func (b *Backend) ingest(all map[any]any) map[backend.ID]bool {
	seen := make(map[backend.ID]bool, len(all))
	var toPause []backend.ID

	b.mu.Lock()
	for k, v := range all {
		st, ok := v.(map[any]any)
		if !ok {
			continue
		}
		id, err := backend.ParseID("btih:" + strings.ToLower(asString(k)))
		if err != nil {
			continue
		}
		seen[id] = true
		wasPending := false
		if prev, ok := b.torrents[id]; ok {
			wasPending = prev.pending
		}
		ts := b.applyLocked(id, st)
		if wasPending && !ts.pending && ts.ownAdd && len(ts.desired) == 0 {
			toPause = append(toPause, id)
		}
	}
	b.mu.Unlock()

	if len(toPause) > 0 {
		c, err := b.conn()
		if err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			for _, id := range toPause {
				if _, err := c.callTimeout(ctx, "core.pause_torrent",
					[]any{rawID(id)}, nil); err != nil {
					b.cfg.Log.Warn("deluge: pausing resolved magnet failed", "torrent", id, "err", err)
				}
			}
		}
	}
	return seen
}

func (b *Backend) emit(e backend.Event) {
	select {
	case b.events <- e:
	default:
		// Slow consumer: drop. The scheduler's repoll safety net
		// re-snapshots piece state (backend.Event contract).
	}
}
