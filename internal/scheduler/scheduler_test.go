package scheduler

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/jonny-gm/torrentseek/internal/backend"
	"github.com/jonny-gm/torrentseek/internal/backend/fake"
	"github.com/jonny-gm/torrentseek/internal/pieces"
)

var ctx = context.Background()

// The canonical fake layout: 16-byte pieces, sample.txt [0,40),
// movie.mkv [40,240), 15 pieces.
func movieSpec() fake.TorrentSpec {
	return fake.TorrentSpec{
		Name:      "Some.Movie.2024",
		PieceSize: 16,
		Files: []fake.FileSpec{
			{Path: "Some.Movie.2024/sample.txt", Length: 40},
			{Path: "Some.Movie.2024/movie.mkv", Length: 200},
		},
		Seed: 42,
	}
}

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func newSched(t *testing.T, clk *clock) (*Scheduler, *fake.Fake, backend.ID) {
	t.Helper()
	f := fake.New()
	id, magnet := f.Register(movieSpec())
	if _, err := f.Add(ctx, backend.AddRequest{Magnet: magnet}); err != nil {
		t.Fatal(err)
	}
	cfg := Config{
		NowWindowBytes:     32, // 2 pieces
		BootstrapHeadBytes: 16, // 1 piece worth
		BootstrapTailBytes: 16,
		PrepareTTL:         time.Minute,
		RepollInterval:     5 * time.Millisecond,
	}
	if clk != nil {
		cfg.Now = clk.now
	}
	s := New(f, cfg)
	t.Cleanup(s.Close)
	t.Cleanup(func() { f.Close() })
	return s, f, id
}

// TestPieceSettleTimeGatesReads verifies the opt-in diagnostic knob: with
// PieceSettleTime set, a wait on a piece that just completed must not
// resolve until the settle window has elapsed, and once it has, the
// completedAt entry is forgotten (bounded memory).
func TestPieceSettleTimeGatesReads(t *testing.T) {
	clk := &clock{t: time.Unix(1000, 0)}
	f := fake.New()
	defer f.Close()
	id, magnet := f.Register(movieSpec())
	if _, err := f.Add(ctx, backend.AddRequest{Magnet: magnet}); err != nil {
		t.Fatal(err)
	}
	s := New(f, Config{
		NowWindowBytes: 32, BootstrapHeadBytes: 16, BootstrapTailBytes: 16,
		PrepareTTL: time.Minute, RepollInterval: 5 * time.Millisecond,
		PieceSettleTime: 500 * time.Millisecond,
		Now:             clk.now,
	})
	defer s.Close()

	done := make(chan error, 1)
	go func() { done <- s.WaitBytes(ctx, id, 1, 0, 16) }() // pieces [2,4)
	time.Sleep(20 * time.Millisecond)                      // let the waiter register

	f.CompleteRange(id, pieces.Range{Begin: 2, End: 4}) // real event fires promptly
	select {
	case err := <-done:
		t.Fatalf("wait resolved before settle time elapsed (err=%v)", err)
	case <-time.After(50 * time.Millisecond):
	}

	clk.advance(500 * time.Millisecond) // settle window elapses
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("wait after settling: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("wait never resolved after settle time elapsed")
	}
}

func TestPieceSettleTimeOffByDefaultIsInstant(t *testing.T) {
	// Same scenario, PieceSettleTime unset (zero value): must behave exactly
	// like the pre-existing instant-wake path, no waiting required.
	s, f, id := newSched(t, nil)
	done := make(chan error, 1)
	go func() { done <- s.WaitBytes(ctx, id, 1, 0, 16) }()
	time.Sleep(20 * time.Millisecond)
	f.CompleteRange(id, pieces.Range{Begin: 2, End: 4})
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("wait never resolved with settle time off")
	}
}

func TestOpenStreamIssuesWindowAndBootstrap(t *testing.T) {
	s, f, id := newSched(t, nil)

	// Stream over movie.mkv (file offset 40) starting at byte 0.
	st, err := s.OpenStream(ctx, id, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	want := []backend.PriorityRange{
		{Range: pieces.Range{Begin: 2, End: 5}, Rank: 0},   // now-window: bytes [40,72)
		{Range: pieces.Range{Begin: 2, End: 4}, Rank: 1},   // head bootstrap: bytes [40,56)
		{Range: pieces.Range{Begin: 14, End: 15}, Rank: 2}, // tail bootstrap: bytes [224,240)
	}
	waitPrioritized(t, f, id, "open-stream window and bootstraps", func(got []backend.PriorityRange) bool {
		return slices.Equal(got, want)
	})
}

func TestWindowSlidesOnAdvance(t *testing.T) {
	s, f, id := newSched(t, nil)
	st, err := s.OpenStream(ctx, id, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	st.Advance(32) // window now bytes [72,104) → pieces [4,7)
	before := waitPrioritized(t, f, id, "window slide to [4,7)", func(got []backend.PriorityRange) bool {
		return len(got) > 0 && got[0] == (backend.PriorityRange{Range: pieces.Range{Begin: 4, End: 7}})
	})

	// Advancing within the same pieces must not re-flush a different set.
	st.Advance(33)
	time.Sleep(20 * time.Millisecond) // a (wrong) re-flush would land within this
	after := f.PrioritizedRanges(id)
	if !slices.Equal(before, after) {
		t.Errorf("sub-piece advance changed priorities: %v → %v", before, after)
	}
}

func TestCloseDropsWindow(t *testing.T) {
	s, f, id := newSched(t, nil)
	st, err := s.OpenStream(ctx, id, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	st.Close()
	// Only bootstraps remain.
	want := []backend.PriorityRange{
		{Range: pieces.Range{Begin: 2, End: 4}, Rank: 0},
		{Range: pieces.Range{Begin: 14, End: 15}, Rank: 1},
	}
	waitPrioritized(t, f, id, "close to drop the window", func(got []backend.PriorityRange) bool {
		return slices.Equal(got, want)
	})
	st.Close() // idempotent
}

func TestUrgencyOrdering(t *testing.T) {
	s, f, id := newSched(t, nil)

	// Stream A at byte 0 of movie.mkv; give its window runway by
	// completing its pieces. Stream B at byte 128 has none.
	a, err := s.OpenStream(ctx, id, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	f.CompleteRange(id, pieces.Range{Begin: 2, End: 5})
	// Wait until the scheduler's cached state has seen the completions, so
	// B's open computes runways against them.
	if err := s.WaitBytes(ctx, id, 1, 0, 32); err != nil {
		t.Fatal(err)
	}

	b, err := s.OpenStream(ctx, id, 1, 128) // bytes [168,200) → pieces [10,13)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	waitPrioritized(t, f, id, "starving stream's window first at rank 0", func(got []backend.PriorityRange) bool {
		return len(got) > 0 && got[0] == (backend.PriorityRange{Range: pieces.Range{Begin: 10, End: 13}})
	})
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for !cond() {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for " + what)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// waitPrioritized polls the fake's most recent Prioritize set until ok
// accepts it — flush delivery runs on a per-torrent goroutine
// (deliverFlushes), so tests can't read it synchronously.
func waitPrioritized(t *testing.T, f *fake.Fake, id backend.ID, what string, ok func([]backend.PriorityRange) bool) []backend.PriorityRange {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		got := f.PrioritizedRanges(id)
		if ok(got) {
			return got
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %s; last prioritized = %v", what, got)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// TestBlockedStreamsTieAtTopUrgency: every blocked stream's window ranks
// 0 simultaneously (03-streaming.md, "Concurrent windows, ranked
// urgency") — the backend pursues them all at its tightest deadline tier
// concurrently, so no blocked stream can be starved by another and no
// rotation bookkeeping exists to get wrong.
func TestBlockedStreamsTieAtTopUrgency(t *testing.T) {
	s, f, id := newSched(t, nil)

	// waiterCount peeks at scheduler internals (same package) so the test
	// can synchronize on "the goroutine's WaitBytes has registered".
	waiterCount := func() int {
		s.mu.Lock()
		defer s.mu.Unlock()
		if ts, ok := s.torrents[id]; ok {
			return len(ts.waiters)
		}
		return 0
	}

	// A (older) at byte 0 → window pieces [2,5); B (newer) at byte 128 →
	// window pieces [10,13). Neither blocked yet: newest-first tie-break
	// puts B ahead at rank 0.
	a, err := s.OpenStream(ctx, id, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := s.OpenStream(ctx, id, 1, 128)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	waitPrioritized(t, f, id, "newest stream leading unblocked ties", func(got []backend.PriorityRange) bool {
		return len(got) > 0 && got[0] == (backend.PriorityRange{Range: pieces.Range{Begin: 10, End: 13}})
	})

	// Both block. Their windows must both carry rank 0 — tied at top
	// urgency — with everything else ranked strictly below.
	aDone := make(chan error, 1)
	go func() { aDone <- a.WaitBytes(ctx, 0, 16) }()
	bDone := make(chan error, 1)
	go func() { bDone <- b.WaitBytes(ctx, 128, 16) }()
	waitFor(t, "both waiters to register", func() bool { return waiterCount() == 2 })

	rankOf := func(got []backend.PriorityRange, r pieces.Range) int {
		for _, pr := range got {
			if pr.Range == r {
				return pr.Rank
			}
		}
		return -1
	}
	got := waitPrioritized(t, f, id, "both blocked windows tied at rank 0", func(got []backend.PriorityRange) bool {
		return rankOf(got, pieces.Range{Begin: 2, End: 5}) == 0 &&
			rankOf(got, pieces.Range{Begin: 10, End: 13}) == 0
	})
	for _, pr := range got[2:] {
		if pr.Rank == 0 {
			t.Fatalf("prioritized = %v, want only blocked windows at rank 0", got)
		}
	}

	// Serving each blocked range wakes its stream; neither wait depends on
	// the other being served first.
	f.CompleteRange(id, pieces.Range{Begin: 10, End: 13})
	if err := <-bDone; err != nil {
		t.Fatal(err)
	}
	f.CompleteRange(id, pieces.Range{Begin: 2, End: 4})
	if err := <-aDone; err != nil {
		t.Fatal(err)
	}
}

func TestWaitBytes(t *testing.T) {
	s, f, id := newSched(t, nil)

	// Already-complete range returns immediately.
	f.CompleteRange(id, pieces.Range{Begin: 2, End: 4})
	if err := s.WaitBytes(ctx, id, 1, 0, 16); err != nil {
		t.Fatalf("complete range: %v", err)
	}

	// Blocked wait wakes when the pieces complete.
	done := make(chan error, 1)
	go func() { done <- s.WaitBytes(ctx, id, 1, 60, 30) }() // pieces [6,9)
	select {
	case err := <-done:
		t.Fatalf("wait returned early: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	f.CompleteRange(id, pieces.Range{Begin: 6, End: 9})
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("wait: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("wait did not wake after completion")
	}

	// Context timeout surfaces.
	tctx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if err := s.WaitBytes(tctx, id, 1, 180, 10); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("timed-out wait: %v, want DeadlineExceeded", err)
	}
}

func TestWaitBytesFailsOnRemove(t *testing.T) {
	s, f, id := newSched(t, nil)
	done := make(chan error, 1)
	go func() { done <- s.WaitBytes(ctx, id, 1, 60, 30) }()
	time.Sleep(20 * time.Millisecond) // let the waiter register
	if err := f.Remove(ctx, id, false); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, backend.ErrTorrentNotFound) {
			t.Errorf("wait after remove: %v, want ErrTorrentNotFound", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("wait did not fail after torrent removal")
	}
}

func TestPrepareWindowAndTTL(t *testing.T) {
	clk := &clock{t: time.Unix(1000, 0)}
	s, f, id := newSched(t, clk)

	ready, avail, err := s.Prepare(ctx, id, 1, 10, 16) // pieces [3,5)
	if err != nil || ready || avail != 0 {
		t.Fatalf("prepare: ready=%v avail=%d err=%v", ready, avail, err)
	}
	waitPrioritized(t, f, id, "prepare window [3,5)", func(got []backend.PriorityRange) bool {
		return slices.Equal(got, []backend.PriorityRange{{Range: pieces.Range{Begin: 3, End: 5}}})
	})

	// Whole-file default (negative length).
	if _, _, err := s.Prepare(ctx, id, 1, 0, -1); err != nil {
		t.Fatal(err)
	}
	waitPrioritized(t, f, id, "[3,5) then whole file [2,15)", func(got []backend.PriorityRange) bool {
		return len(got) == 2 && got[1] == (backend.PriorityRange{Range: pieces.Range{Begin: 2, End: 15}, Rank: 1})
	})

	// After the TTL, expired windows drop at the next recompute.
	clk.advance(2 * time.Minute)
	if _, _, err := s.Prepare(ctx, id, 0, 0, 8); err != nil { // pieces [0,1)
		t.Fatal(err)
	}
	waitPrioritized(t, f, id, "only [0,1) after TTL", func(got []backend.PriorityRange) bool {
		return slices.Equal(got, []backend.PriorityRange{{Range: pieces.Range{Begin: 0, End: 1}}})
	})
}

func TestPrepareValidation(t *testing.T) {
	s, _, id := newSched(t, nil)
	if _, _, err := s.Prepare(ctx, id, 9, 0, 1); !errors.Is(err, backend.ErrFileNotFound) {
		t.Errorf("bad index: %v", err)
	}
	if _, _, err := s.Prepare(ctx, id, 1, 500, 1); !errors.Is(err, backend.ErrOutOfRange) {
		t.Errorf("offset past EOF: %v", err)
	}
	if _, _, err := s.Prepare(ctx, id, 1, 190, 20); !errors.Is(err, backend.ErrOutOfRange) {
		t.Errorf("range past EOF: %v", err)
	}
	unknown := backend.ID("btih:00000000000000000000ffffffffffffffffffff")
	if _, _, err := s.Prepare(ctx, unknown, 0, 0, 1); !errors.Is(err, backend.ErrTorrentNotFound) {
		t.Errorf("unknown torrent: %v", err)
	}
}

func TestWaiterWakesAcrossFiles(t *testing.T) {
	// A waiter on file 0 (whose pieces overlap file 1's first piece) wakes
	// via events or the repoll safety net — whichever fires first.
	s, f, id := newSched(t, nil)
	done := make(chan error, 1)
	go func() { done <- s.WaitBytes(ctx, id, 0, 0, 40) }()
	time.Sleep(10 * time.Millisecond)
	f.CompleteRange(id, pieces.Range{Begin: 0, End: 3})
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter never woke")
	}
}

// reannouncingFake decorates the fake with the optional
// backend.Reannouncer capability so stall-nudge behavior is testable.
type reannouncingFake struct {
	*fake.Fake
	mu     sync.Mutex
	nudged int
}

func (r *reannouncingFake) Reannounce(_ context.Context, _ backend.ID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nudged++
	return nil
}

func (r *reannouncingFake) nudges() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.nudged
}

// TestStallNudgeReannouncesBlockedTorrents: a waiter blocked past
// StallNudgeAfter triggers exactly one reannounce per interval, and a
// wait shorter than the threshold triggers none.
func TestStallNudgeReannouncesBlockedTorrents(t *testing.T) {
	clk := &clock{t: time.Unix(1000, 0)}
	f := &reannouncingFake{Fake: fake.New()}
	defer f.Close()
	id, magnet := f.Register(movieSpec())
	if _, err := f.Add(ctx, backend.AddRequest{Magnet: magnet}); err != nil {
		t.Fatal(err)
	}
	s := New(f, Config{
		NowWindowBytes: 32, BootstrapHeadBytes: 16, BootstrapTailBytes: 16,
		PrepareTTL: time.Minute, RepollInterval: 5 * time.Millisecond,
		StallNudgeAfter: 10 * time.Second,
		Now:             clk.now,
	})
	defer s.Close()

	done := make(chan error, 1)
	go func() { done <- s.WaitBytes(ctx, id, 1, 0, 16) }()
	waitFor(t, "waiter registration", func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		ts, ok := s.torrents[id]
		return ok && len(ts.waiters) > 0
	})

	time.Sleep(50 * time.Millisecond) // many repolls, no clock movement
	if n := f.nudges(); n != 0 {
		t.Fatalf("nudged %d times before the stall threshold", n)
	}

	clk.advance(10 * time.Second) // waiter now stalled past the threshold
	waitFor(t, "first stall nudge", func() bool { return f.nudges() >= 1 })
	time.Sleep(50 * time.Millisecond) // rate limit: one per interval
	if n := f.nudges(); n != 1 {
		t.Fatalf("nudged %d times within one interval, want 1", n)
	}

	clk.advance(10 * time.Second) // still stalled: next interval re-nudges
	waitFor(t, "second stall nudge", func() bool { return f.nudges() >= 2 })

	f.CompleteRange(id, pieces.Range{Begin: 2, End: 4})
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("wait never resolved")
	}
}

// TestClosedStreamWindowLingers: with WindowLinger set, closing a stream
// parks its window at a low rank instead of dropping it — a reopen at
// the same position finds the pieces still prioritized (no
// teardown/rebuild) — and the parked window expires on the clock.
func TestClosedStreamWindowLingers(t *testing.T) {
	clk := &clock{t: time.Unix(1000, 0)}
	f := fake.New()
	defer f.Close()
	id, magnet := f.Register(movieSpec())
	if _, err := f.Add(ctx, backend.AddRequest{Magnet: magnet}); err != nil {
		t.Fatal(err)
	}
	s := New(f, Config{
		NowWindowBytes: 32, BootstrapHeadBytes: 16, BootstrapTailBytes: 16,
		PrepareTTL: time.Minute, RepollInterval: 5 * time.Millisecond,
		WindowLinger: 10 * time.Second,
		Now:          clk.now,
	})
	defer s.Close()

	st, err := s.OpenStream(ctx, id, 1, 0) // window pieces [2,5)
	if err != nil {
		t.Fatal(err)
	}
	window := pieces.Range{Begin: 2, End: 5}
	st.Close()

	// The window survives the close (ranked after nothing here but the
	// bootstraps), so a player reopening milliseconds later would find
	// its deadlines intact.
	waitPrioritized(t, f, id, "lingering window after close", func(got []backend.PriorityRange) bool {
		return slices.ContainsFunc(got, func(r backend.PriorityRange) bool { return r.Range == window })
	})

	// Past the linger, the next recompute drops it.
	clk.advance(11 * time.Second)
	if _, _, err := s.Prepare(ctx, id, 0, 0, 8); err != nil { // any flush trigger
		t.Fatal(err)
	}
	waitPrioritized(t, f, id, "linger expiry", func(got []backend.PriorityRange) bool {
		return !slices.ContainsFunc(got, func(r backend.PriorityRange) bool { return r.Range == window })
	})
}

// TestCloseReopenChurnKeepsWindowContinuously: the exact churn a player
// produces — close and immediately reopen the same position — must never
// pass through a state where the window is absent from the desired set.
func TestCloseReopenChurnKeepsWindowContinuously(t *testing.T) {
	clk := &clock{t: time.Unix(1000, 0)}
	f := fake.New()
	defer f.Close()
	id, magnet := f.Register(movieSpec())
	if _, err := f.Add(ctx, backend.AddRequest{Magnet: magnet}); err != nil {
		t.Fatal(err)
	}
	s := New(f, Config{
		NowWindowBytes: 32, BootstrapHeadBytes: 16, BootstrapTailBytes: 16,
		PrepareTTL: time.Minute, RepollInterval: 5 * time.Millisecond,
		WindowLinger: 10 * time.Second,
		Now:          clk.now,
	})
	defer s.Close()

	window := pieces.Range{Begin: 2, End: 5}
	for i := 0; i < 5; i++ {
		st, err := s.OpenStream(ctx, id, 1, 0)
		if err != nil {
			t.Fatal(err)
		}
		st.Close()
		// Inspect the scheduler's own desired set (not the fake's, which
		// the async deliverer may lag): the window must be present in
		// every recomputed set, open or closed.
		s.mu.Lock()
		last := s.torrents[id].lastFlushed
		s.mu.Unlock()
		if !slices.ContainsFunc(last, func(r backend.PriorityRange) bool { return r.Range == window }) {
			t.Fatalf("churn round %d: window %v missing from desired set %v", i, window, last)
		}
	}
}

// rescueFake adds the straggler-rescue capabilities to the fake: swarm
// states are scripted per piece, and rescue calls are recorded.
type rescueFake struct {
	*fake.Fake
	mu      sync.Mutex
	states  []backend.PieceSwarmState
	rescued []int
}

func (r *rescueFake) PieceSwarmStates(context.Context, backend.ID) ([]backend.PieceSwarmState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.states), nil
}

func (r *rescueFake) RescuePiece(_ context.Context, _ backend.ID, piece int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rescued = append(r.rescued, piece)
	return nil
}

func (r *rescueFake) rescues() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.rescued)
}

// TestStallRescueKicksOnlyDownloadingPieces: a waiter stuck past the
// threshold triggers a rescue for the first missing piece of its range —
// but only when that piece is actively downloading (hostage blocks);
// unavailable pieces are left alone, and repeats are rate-limited.
func TestStallRescueKicksOnlyDownloadingPieces(t *testing.T) {
	clk := &clock{t: time.Unix(1000, 0)}
	f := &rescueFake{Fake: fake.New()}
	defer f.Close()
	id, magnet := f.Register(movieSpec())
	if _, err := f.Add(ctx, backend.AddRequest{Magnet: magnet}); err != nil {
		t.Fatal(err)
	}
	// 15 pieces; the waiter below waits on pieces [2,4).
	f.mu.Lock()
	f.states = make([]backend.PieceSwarmState, 15)
	f.states[2] = backend.PieceSwarmUnavailable
	f.mu.Unlock()

	s := New(f, Config{
		NowWindowBytes: 32, BootstrapHeadBytes: 16, BootstrapTailBytes: 16,
		PrepareTTL: time.Minute, RepollInterval: 5 * time.Millisecond,
		StallRescueAfter: 5 * time.Second,
		Now:              clk.now,
	})
	defer s.Close()

	done := make(chan error, 1)
	go func() { done <- s.WaitBytes(ctx, id, 1, 0, 16) }() // pieces [2,4)
	waitFor(t, "waiter registration", func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		ts, ok := s.torrents[id]
		return ok && len(ts.waiters) > 0
	})

	// Past the threshold but the piece is unavailable: never rescued —
	// kicking peers can't help a piece nobody has.
	clk.advance(6 * time.Second)
	time.Sleep(50 * time.Millisecond)
	if got := f.rescues(); len(got) != 0 {
		t.Fatalf("rescued %v while the piece was unavailable", got)
	}

	// The piece turns 'available' (a peer has it; a fully dead holder
	// never gets it marked 'downloading'): rescued, once per interval.
	f.mu.Lock()
	f.states[2] = backend.PieceSwarmAvailable
	f.mu.Unlock()
	waitFor(t, "first rescue", func() bool { return len(f.rescues()) >= 1 })
	if got := f.rescues(); got[0] != 2 {
		t.Fatalf("rescued piece %d, want 2", got[0])
	}
	time.Sleep(50 * time.Millisecond) // rate limit: no repeat within the interval
	if got := f.rescues(); len(got) != 1 {
		t.Fatalf("rescued %d times within one interval, want 1", len(got))
	}
	clk.advance(6 * time.Second) // still stuck: next interval re-rescues
	waitFor(t, "second rescue", func() bool { return len(f.rescues()) >= 2 })

	f.CompleteRange(id, pieces.Range{Begin: 2, End: 4})
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
