// Write coalescing: diffing desired serving state against last-sent
// and pushing it to the daemon in bulk (04-deluge-backend.md).
package deluge

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/jonny-gm/torrentseek/internal/backend"
	"github.com/jonny-gm/torrentseek/internal/pieces"
)

// Prioritize records the desired ranges and flushes the resulting piece
// priorities/deadlines as a diff (04-deluge-backend.md, Scheduling
// strategy). Every range gets real concurrent per-piece treatment; rank
// scales only the deadline tier.
func (b *Backend) Prioritize(ctx context.Context, id backend.ID, ranges []backend.PriorityRange) error {
	if _, err := b.ensure(ctx, id); err != nil {
		return err
	}
	b.mu.Lock()
	ts := b.torrents[id]
	ts.desired = slices.Clone(ranges)
	ts.hotUntil = time.Now().Add(b.cfg.HotWindow)
	b.mu.Unlock()
	return b.flush(ctx, id)
}

// desiredDeadlines maps the desired ranges to per-piece deadlines:
// tier by each range's rank, staggered piece by piece within the range,
// tightest deadline winning where ranges overlap. The stagger means a
// sliding window rewrites deadlines every flush (each piece's offset
// from the range start shrinks as the cursor approaches), but deadline
// writes are one bulk RPC so the flush cost doesn't grow with it.
func desiredDeadlines(desired []backend.PriorityRange, pieceCount int) map[int]int64 {
	want := make(map[int]int64)
	for _, r := range desired {
		base := int64(deadlineBaseMS + r.Rank*deadlineStepMS)
		for p := max(r.Begin, 0); p < min(r.End, pieceCount); p++ {
			d := base + int64(p-r.Begin)*deadlineIntraStepMS
			if d > deadlineMaxMS {
				d = deadlineMaxMS
			}
			if cur, ok := want[p]; !ok || d < cur {
				want[p] = d
			}
		}
	}
	return want
}

// wantedFiles returns the indices of files overlapping any desired range.
func wantedFiles(desired []backend.PriorityRange, files []backend.FileInfo, pieceSize int64) []int {
	var out []int
	for _, f := range files {
		fr := pieces.FromByteRange(f.Offset, pieceSize, 0, f.Length)
		for _, r := range desired {
			if r.Begin < fr.End && fr.Begin < r.End {
				out = append(out, f.Index)
				break
			}
		}
	}
	return out
}

// flush pushes the diff between desired and last-sent state: file wanted
// flags first (a file at priority 0 zeroes its pieces, so piece writes
// must land after), then piece priorities and deadlines in bulk, then
// releases for pieces no longer in any window.
func (b *Backend) flush(ctx context.Context, id backend.ID) error {
	c, err := b.conn()
	if err != nil {
		return err
	}

	b.mu.Lock()
	ts, ok := b.torrents[id]
	if !ok || ts.pending {
		b.mu.Unlock()
		return nil
	}
	want := desiredDeadlines(ts.desired, ts.info.PieceCount)

	wanted := wantedFiles(ts.desired, ts.files, ts.info.PieceSize)
	wantedKey := fmt.Sprint(wanted)
	sendWanted := wantedKey != ts.sentWanted && len(ts.files) > 0
	// A torrent going from no interest to some interest needs peers *now*.
	// Idle torrents are kept paused (see Add), so the transition resumes
	// the torrent — which announces immediately — plus a best-effort
	// forced reannounce for the case where the torrent was already
	// running. Besides the no-interest→interest transition, wake also
	// fires whenever the last status snapshot shows the torrent paused
	// while pieces are wanted: Deluge re-pauses asynchronously after a
	// forced recheck (the add-time recheck records the paused state and
	// restores it when checking completes), which can silently undo a
	// wake that raced it — the state check makes wake self-healing on the
	// next flush instead of one-shot.
	wake := len(wanted) > 0 && (ts.sentWanted == "" || ts.sentWanted == "[]" ||
		ts.info.State == backend.StateStopped)
	// The reverse transition idles the torrent again — but not immediately.
	// Media players constantly close and reopen streams (container
	// probing, seeks), and pausing on a momentary zero-interest gap
	// disconnects every peer; the reopen milliseconds later then faces an
	// empty peer table plus reconnect backoff on the peers just dropped,
	// turning every seek into a 30–60s swarm rebuild. Instead, interest
	// ending only starts the idle-grace clock: the torrent keeps running
	// with its last wanted set (a genuine leecher, so no seed-drop
	// poisoning; at worst it prefetches a few minutes of the file the user
	// was just streaming). The poll's idle sweep does the actual
	// zero-files-and-pause once the grace expires with no new interest.
	if len(wanted) > 0 {
		ts.idleSince = time.Time{}
	} else if ts.sentWanted != "" && ts.sentWanted != "[]" {
		if ts.idleSince.IsZero() {
			ts.idleSince = time.Now()
		}
		// Keep the previous wanted files during the grace window.
		sendWanted = false
	}
	fileCount := len(ts.files)

	setPrio := map[any]any{}     // pieces newly High
	setDeadline := map[any]any{} // pieces with new/changed deadlines
	var clearPieces []int        // pieces released from all windows
	// Pieces already complete need no urgency (libtorrent drops a
	// time-critical entry when its piece finishes), so filter them out of
	// both the writes and the release diff. In a healthy stream the window
	// slides forward because pieces complete — without this filter every
	// slide would re-write deadlines for done pieces and issue a
	// clear-per-piece RPC for each departed one, dominating the flush.
	for p := range want {
		if p < ts.have.Len() && ts.have.Has(p) {
			delete(want, p)
		}
	}
	for p, d := range want {
		if _, had := ts.sentPieces[p]; !had {
			setPrio[int64(p)] = int64(piecePriorityHigh)
		}
		if ts.sentPieces[p] != d {
			setDeadline[int64(p)] = d
		}
	}
	for p := range ts.sentPieces {
		if _, still := want[p]; !still {
			if p < ts.have.Len() && ts.have.Has(p) {
				continue // completed: its deadline died with it
			}
			clearPieces = append(clearPieces, p)
		}
	}
	raw := rawID(id)
	b.mu.Unlock()

	if sendWanted {
		prios := make([]any, fileCount)
		for i := range prios {
			prios[i] = 0
		}
		for _, fi := range wanted {
			if fi >= 0 && fi < fileCount {
				prios[fi] = filePriorityNormal
			}
		}
		if _, err := c.callTimeout(ctx, "core.set_torrent_options",
			[]any{[]any{raw}, map[any]any{"file_priorities": prios}}, nil); err != nil {
			return b.flushFailed(id, err)
		}
	}
	if wake {
		if _, err := c.callTimeout(ctx, "core.resume_torrent", []any{raw}, nil); err != nil {
			return b.flushFailed(id, err)
		}
		// Best-effort: a failed reannounce shouldn't fail the flush — the
		// priorities/deadlines are the load-bearing writes.
		if _, err := c.callTimeout(ctx, "core.force_reannounce",
			[]any{[]any{raw}}, nil); err != nil {
			b.cfg.Log.Debug("deluge: force_reannounce failed", "torrent", id, "err", err)
		}
	}
	if len(setPrio) > 0 {
		if _, err := c.callTimeout(ctx, "piecepriority.set_piece_priorities",
			[]any{raw, setPrio}, nil); err != nil {
			return b.flushFailed(id, err)
		}
	}
	if len(setDeadline) > 0 {
		if _, err := c.callTimeout(ctx, "piecepriority.set_piece_deadlines",
			[]any{raw, setDeadline}, nil); err != nil {
			return b.flushFailed(id, err)
		}
	}
	if len(clearPieces) > 0 {
		// Deadlines first, priorities second: libtorrent restores a
		// cleared time-critical piece's priority to low (1), so a reset
		// written before the clear would be clobbered and released pieces
		// would rank *below* untouched ones instead of rejoining them.
		if len(want) == 0 {
			// Last window on the torrent closed: clear everything in one
			// call rather than one RPC per piece.
			if _, err := c.callTimeout(ctx, "piecepriority.clear_piece_deadlines",
				[]any{raw}, nil); err != nil {
				return b.flushFailed(id, err)
			}
		} else {
			for _, p := range clearPieces {
				if _, err := c.callTimeout(ctx, "piecepriority.clear_piece_deadline",
					[]any{raw, int64(p)}, nil); err != nil {
					return b.flushFailed(id, err)
				}
			}
		}
		reset := map[any]any{}
		for _, p := range clearPieces {
			reset[int64(p)] = int64(piecePriorityDefault)
		}
		if _, err := c.callTimeout(ctx, "piecepriority.set_piece_priorities",
			[]any{raw, reset}, nil); err != nil {
			return b.flushFailed(id, err)
		}
	}
	b.mu.Lock()
	if ts, ok := b.torrents[id]; ok {
		ts.sentPieces = want
		if sendWanted {
			ts.sentWanted = wantedKey
		}
		if wake && ts.info.State == backend.StateStopped {
			// Optimistic: the next status poll gives the real state; this
			// just stops back-to-back flushes re-issuing resume/reannounce.
			ts.info.State = backend.StateDownloading
		}
	}
	b.mu.Unlock()
	return nil
}

// flushFailed clears sent-state so the next Prioritize/poll tick
// re-asserts the full desired set (04-deluge-backend.md, Failure
// handling).
func (b *Backend) flushFailed(id backend.ID, err error) error {
	b.mu.Lock()
	if ts, ok := b.torrents[id]; ok {
		ts.sentPieces = nil
		ts.sentWanted = ""
	}
	b.mu.Unlock()
	return err
}
