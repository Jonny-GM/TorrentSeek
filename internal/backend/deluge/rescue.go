// Straggler rescue: the escalating rescue_piece kick
// (04-deluge-backend.md, "Rescue escalation").
package deluge

import (
	"context"
	"fmt"
	"time"

	"github.com/jonny-gm/torrentseek/internal/backend"
)

// Rescue escalation ladder (04-deluge-backend.md): rescue_piece only
// kicks holders downloading below a speed bar, and the first attempt's
// bar catches only obvious deadbeats — kicking a flowing peer costs
// real bandwidth. But a holder above the bar can still hostage a piece
// indefinitely: trickling it at tens of KiB/s, or serving other pieces
// fast while this piece's requests sit deep in its queue (observed in
// the field: three rescues no-opped over a 34-second stall because the
// holders cleared 8 KiB/s). A repeat rescue for the same piece is
// proof the previous bar was too low, so each attempt raises it, and
// the third drops the bar entirely — by then the piece has been stuck
// through two gentler attempts and a re-request from any other peer
// beats waiting.
const (
	rescueBanSeconds     = 10
	rescueMinSpeedFirst  = 8 << 10  // bytes/s: deadbeats only
	rescueMinSpeedSecond = 64 << 10 // bytes/s: anything slower than real service
	rescueMinSpeedAll    = 1 << 40  // no bar: kick every holder
	// rescueEscalationWindow separates one stall's attempts from the
	// next: a repeat inside it escalates, a rescue after it starts the
	// ladder over. Must exceed the scheduler's per-piece repeat interval
	// (StallRescueAfter, seconds); a piece re-stuck half a minute later
	// is a new stall, not the same one.
	rescueEscalationWindow = 30 * time.Second
)

// rescueKey/rescueTrack track per-piece rescue attempts for the
// escalation ladder, pruned as entries age past the window.
type rescueKey struct {
	id    backend.ID
	piece int
}

type rescueTrack struct {
	attempts int
	last     time.Time
}

// escalateRescue records one more rescue attempt for the piece and
// returns the min_speed bar that attempt should use.
func (b *Backend) escalateRescue(id backend.ID, piece int) int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	for k, t := range b.rescues {
		if now.Sub(t.last) > rescueEscalationWindow {
			delete(b.rescues, k)
		}
	}
	k := rescueKey{id: id, piece: piece}
	t := b.rescues[k]
	t.attempts++
	t.last = now
	b.rescues[k] = t
	switch t.attempts {
	case 1:
		return rescueMinSpeedFirst
	case 2:
		return rescueMinSpeedSecond
	default:
		return rescueMinSpeedAll
	}
}

// RescuePiece implements the optional backend.PieceRescuer capability
// via piecepriority.rescue_piece: the plugin IP-bans the peers holding
// the piece's requested blocks for a few seconds (libtorrent
// disconnects filtered peers immediately, requeueing the blocks) and
// the piece's still-active deadline re-requests them from healthy
// peers. Repeat attempts for a still-stuck piece escalate the kick
// (see the rescue escalation ladder above).
func (b *Backend) RescuePiece(ctx context.Context, id backend.ID, piece int) error {
	if _, err := b.ensure(ctx, id); err != nil {
		return err
	}
	c, err := b.conn()
	if err != nil {
		return err
	}
	minSpeed := b.escalateRescue(id, piece)
	b.cfg.Log.Debug("deluge: rescue attempt", "torrent", id, "piece", piece, "min_speed", minSpeed)
	v, err := c.callTimeout(ctx, "piecepriority.rescue_piece",
		[]any{rawID(id), int64(piece), int64(rescueBanSeconds), minSpeed}, nil)
	if err != nil {
		return err
	}
	if kicked, ok := v.([]any); ok && len(kicked) > 0 {
		b.cfg.Log.Info("deluge: rescued piece by kicking hostage peers",
			"torrent", id, "piece", piece, "min_speed", minSpeed, "peers", fmt.Sprint(kicked))
	}
	return nil
}
