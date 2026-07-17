package deluge

// Live tests against a real deluged, gated on DELUGE_LIVE_ADDR (plus
// DELUGE_LIVE_USER / DELUGE_LIVE_PASS). Skipped otherwise, so `make test`
// stays client-free; the live harness (test/live/run.sh) sets the
// variables and runs these against the daemon it starts.

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jonny-gm/torrentseek/internal/backend"
	"github.com/jonny-gm/torrentseek/internal/pieces"
)

func liveConn(t *testing.T) *conn {
	t.Helper()
	addr := os.Getenv("DELUGE_LIVE_ADDR")
	if addr == "" {
		t.Skip("DELUGE_LIVE_ADDR not set; skipping live deluged test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	c, err := dial(ctx, addr, os.Getenv("DELUGE_LIVE_USER"), os.Getenv("DELUGE_LIVE_PASS"))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { c.close(nil) })
	return c
}

func TestLiveTransport(t *testing.T) {
	c := liveConn(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	info, err := c.call(ctx, "daemon.info", nil, nil)
	if err != nil {
		t.Fatalf("daemon.info: %v", err)
	}
	if _, ok := info.(string); !ok {
		t.Fatalf("daemon.info returned %T, want version string", info)
	}
	t.Logf("daemon.info = %v", info)

	st, err := c.call(ctx, "core.get_torrents_status",
		[]any{map[any]any{}, []any{"name", "progress", "num_pieces"}}, nil)
	if err != nil {
		t.Fatalf("get_torrents_status: %v", err)
	}
	torrents, ok := st.(map[any]any)
	if !ok {
		t.Fatalf("get_torrents_status returned %T, want dict", st)
	}
	t.Logf("%d torrents", len(torrents))

	// The plugin's RPC surface must be reachable through this transport.
	for id := range torrents {
		pr, err := c.call(ctx, "piecepriority.get_piece_priorities", []any{id}, nil)
		if err != nil {
			t.Fatalf("piecepriority.get_piece_priorities(%v): %v", id, err)
		}
		if _, ok := pr.([]any); !ok {
			t.Fatalf("get_piece_priorities returned %T, want list", pr)
		}
		t.Logf("priorities(%v): %d pieces", id, len(pr.([]any)))
		break
	}

	// An invalid method must surface as a clean rpcError, not a hang.
	_, err = c.call(ctx, "core.no_such_method", nil, nil)
	if err == nil {
		t.Fatal("call to invalid method succeeded, want error")
	}
	t.Logf("invalid method error (expected): %v", err)
}

// TestLiveBackend drives the full Backend implementation against the real
// daemon: idempotent add, files, piece state, prioritize (verifying the
// piece priorities actually landed daemon-side), read, and remove. It
// needs DELUGE_LIVE_TORRENT pointing at a .torrent file whose data the
// daemon can't already have (a fresh harness-generated one).
func TestLiveBackend(t *testing.T) {
	addr := os.Getenv("DELUGE_LIVE_ADDR")
	torrentPath := os.Getenv("DELUGE_LIVE_TORRENT")
	if addr == "" || torrentPath == "" {
		t.Skip("DELUGE_LIVE_ADDR / DELUGE_LIVE_TORRENT not set; skipping live backend test")
	}
	metainfo, err := os.ReadFile(torrentPath)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	b, err := New(ctx, Config{
		Addr:     addr,
		Username: os.Getenv("DELUGE_LIVE_USER"),
		Password: os.Getenv("DELUGE_LIVE_PASS"),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer b.Close()

	res, err := b.Add(ctx, backend.AddRequest{Metainfo: metainfo})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	if res.Existing {
		t.Log("torrent already present (rerun); continuing")
	}
	id := res.ID
	t.Logf("added %s", id)
	defer b.Remove(context.Background(), id, true)

	dup, err := b.Add(ctx, backend.AddRequest{Metainfo: metainfo})
	if err != nil {
		t.Fatalf("duplicate Add: %v", err)
	}
	if !dup.Existing || dup.ID != id {
		t.Fatalf("duplicate Add = %+v, want Existing with same id", dup)
	}

	files, err := b.Files(ctx, id)
	if err != nil {
		t.Fatalf("Files: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("Files returned none")
	}
	info, err := b.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	t.Logf("%d files, %d pieces of %d bytes", len(files), info.PieceCount, info.PieceSize)

	state, err := b.PieceState(ctx, id)
	if err != nil {
		t.Fatalf("PieceState: %v", err)
	}
	if state.Len() != info.PieceCount {
		t.Fatalf("PieceState len %d, want %d", state.Len(), info.PieceCount)
	}

	// Prioritize two ranges and verify daemon-side piece priorities moved.
	end := min(3, info.PieceCount)
	if err := b.Prioritize(ctx, id, []backend.PriorityRange{{Range: pieces.Range{Begin: 0, End: end}}}); err != nil {
		t.Fatalf("Prioritize: %v", err)
	}
	c, err := b.conn()
	if err != nil {
		t.Fatal(err)
	}
	pr, err := c.call(ctx, "piecepriority.get_piece_priorities", []any{rawID(id)}, nil)
	if err != nil {
		t.Fatalf("get_piece_priorities: %v", err)
	}
	prios := pr.([]any)
	for i := 0; i < end; i++ {
		if got := prios[i].(int64); got != piecePriorityHigh {
			t.Errorf("piece %d priority = %d after Prioritize, want %d", i, got, piecePriorityHigh)
		}
	}
	t.Logf("daemon-side priorities after Prioritize: %v...", prios[:end])

	// Releasing all windows resets priorities and clears deadlines.
	if err := b.Prioritize(ctx, id, nil); err != nil {
		t.Fatalf("Prioritize(nil): %v", err)
	}
	pr, err = c.call(ctx, "piecepriority.get_piece_priorities", []any{rawID(id)}, nil)
	if err != nil {
		t.Fatalf("get_piece_priorities: %v", err)
	}
	if got := pr.([]any)[0].(int64); got != piecePriorityDefault {
		t.Errorf("piece 0 priority after release = %d, want %d", got, piecePriorityDefault)
	}

	if err := b.Remove(ctx, id, true); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if _, err := b.Get(ctx, id); !errors.Is(err, backend.ErrTorrentNotFound) {
		t.Fatalf("Get after Remove = %v, want ErrTorrentNotFound", err)
	}
}
