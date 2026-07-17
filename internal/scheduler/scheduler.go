// Package scheduler implements the seek-aware piece scheduling of
// docs/spec/03-streaming.md: it turns open streams and prepare calls into
// priority windows, keeps the backend's priorities in sync as cursors move,
// and lets the streamer block until the bytes at a cursor exist.
//
// Every window progresses concurrently on a capable backend; the scheduler
// ranks windows by urgency (blocked streams all tie at rank 0, the rest by
// smallest runway) and leaves the mechanism to the backend.
package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/jonny-gm/torrentseek/internal/backend"
	"github.com/jonny-gm/torrentseek/internal/pieces"
)

// Config carries the tuning knobs from 03-streaming.md. Zero values fall
// back to the spec defaults.
type Config struct {
	NowWindowBytes     int64         // top-priority window past each cursor (default 32 MiB)
	BootstrapHeadBytes int64         // file head prioritized on first open (default 8 MiB)
	BootstrapTailBytes int64         // file tail prioritized on first open (default 8 MiB)
	PrepareTTL         time.Duration // idle decay for prepare windows (default 120 s)
	RepollInterval     time.Duration // safety-net piece-state re-snapshot (default 1 s)
	// PieceSettleTime, when nonzero, delays trusting a piece for reads until
	// this long after it was first observed complete. Zero means off (the
	// library default, so tests read the instant a piece completes), but
	// the shipped binary defaults it ON: libtorrent 2.x hashes blocks out
	// of its in-memory store buffer and reports the piece finished before
	// the disk thread necessarily writes those blocks out, so a read
	// racing the report can see stale file contents. Not hypothetical —
	// the live harness caught a fast loopback stream serving a block of
	// zeros exactly this way (03-streaming.md, "Piece settle time").
	PieceSettleTime time.Duration
	// StallNudgeAfter, when nonzero and the backend implements
	// backend.Reannouncer, forces a tracker reannounce on any torrent
	// whose oldest blocked waiter has been waiting this long, repeating
	// at the same interval while the torrent stays blocked. Deadlines
	// steer the peers a client already has; a wait this long means no
	// connected peer is supplying the piece, and hunting for new peers
	// is the only lever left. Zero means off — the default everywhere,
	// including the shipped binary: repeated forced reannounces are
	// impolite to trackers, so this is opt-in (-stall-nudge) for swarms
	// with genuinely poor piece availability.
	StallNudgeAfter time.Duration
	// StallRescueAfter, when nonzero and the backend implements both
	// backend.PieceRescuer and backend.SwarmObserver, rescues the piece a
	// stream has been blocked on for this long *when that piece is
	// actively downloading*: its blocks are parked in a stalled peer's
	// queue, and the backend client's own request timeout (tuned for
	// downloads, not deadlines) won't re-request them for a minute or
	// more — observed in the field as one piece hostage for 79 seconds
	// while 130 peers delivered 8 MB/s of everything else. The rescue
	// kicks exactly the hostage-holding peers; unavailable pieces (no
	// peer has them) are never rescued, so a genuinely starved swarm
	// doesn't get its few peers kicked, and the kick itself is a no-op
	// for pieces with no requested blocks outstanding. Zero
	// means off (the library default); the shipped binary defaults it on
	// via -stall-rescue. Repeats per piece at the same interval while
	// still stuck.
	StallRescueAfter time.Duration
	// WindowLinger keeps a closed stream's priority window alive this
	// long after the stream goes away (at a rank below every live
	// window). Media players churn streams constantly — closing and
	// reopening the same position for container probing, track reads,
	// and reload-style recovery — and without a linger every close tears
	// the window's piece deadlines down and every reopen rebuilds them,
	// resetting the backend client's time-critical bookkeeping at the
	// exact pieces it was chasing. Zero drops windows immediately (the
	// library default; the shipped binary defaults it on via
	// -window-linger).
	WindowLinger time.Duration
	Now          func() time.Time
	Log          *slog.Logger
}

func (c Config) withDefaults() Config {
	if c.NowWindowBytes <= 0 {
		c.NowWindowBytes = 32 << 20
	}
	if c.BootstrapHeadBytes <= 0 {
		c.BootstrapHeadBytes = 8 << 20
	}
	if c.BootstrapTailBytes <= 0 {
		c.BootstrapTailBytes = 8 << 20
	}
	if c.PrepareTTL <= 0 {
		c.PrepareTTL = 120 * time.Second
	}
	if c.RepollInterval <= 0 {
		c.RepollInterval = time.Second
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	if c.Log == nil {
		c.Log = slog.Default()
	}
	return c
}

// Scheduler tracks priority windows per torrent and mediates all
// Prioritize traffic to the backend. Create with New, stop with Close
// (Close does not close the backend).
type Scheduler struct {
	b   backend.Backend
	cfg Config
	// nudger is b's optional Reannouncer capability (nil when absent);
	// see Config.StallNudgeAfter.
	nudger backend.Reannouncer
	// rescuer/swarmObs are b's optional straggler-rescue capabilities
	// (nil when absent); see Config.StallRescueAfter.
	rescuer  backend.PieceRescuer
	swarmObs backend.SwarmObserver

	mu       sync.Mutex
	torrents map[backend.ID]*torrentState

	quit chan struct{}
	done chan struct{}

	streamSeq uint64 // monotonic stream-creation counter
}

type torrentState struct {
	info  backend.TorrentInfo
	files []backend.FileInfo
	state pieces.Bitfield

	// completedAt records when a piece was first observed to transition
	// missing->complete during this process's lifetime, for PieceSettleTime
	// gating (see Config). Pieces already complete when a torrent is first
	// seen (e.g. resumed from a prior run) get no entry and are treated as
	// pre-settled — there is no fresh-write race for data that has been
	// sitting on disk since before this process started. Entries are
	// deleted once they settle, so this only grows with recent activity.
	completedAt map[int]time.Time

	streams         []*Stream
	bootstraps      []pieces.Range
	bootstrapIssued map[int]bool
	prepares        []prepareEntry
	lingers         []prepareEntry // closed streams' windows, kept until expiry (Config.WindowLinger)
	waiters         map[*waiter]struct{}
	lastFlushed     []backend.PriorityRange
	removed         bool

	// Flush delivery. Prioritize is a network round trip, so it runs off
	// the scheduler mutex: flushLocked parks the newest desired set here
	// and a single per-torrent goroutine delivers it. Intermediate sets
	// are obsolete the moment a newer one exists, so only the latest is
	// ever sent.
	pendingFlush []backend.PriorityRange
	hasPending   bool
	flushing     bool

	// lastNudge rate-limits stall nudges (Config.StallNudgeAfter): at most
	// one reannounce per interval while the torrent stays blocked.
	lastNudge time.Time
	// lastRescue rate-limits straggler rescues per piece
	// (Config.StallRescueAfter): kicking the same hostage holders again
	// before the re-requested blocks had a chance to arrive helps nobody.
	lastRescue map[int]time.Time
}

type prepareEntry struct {
	r       pieces.Range
	expires time.Time
}

type waiter struct {
	r       pieces.Range
	ch      chan error
	created time.Time // when the wait began, for stall-nudge age
}

// Stream is a scheduler handle for one open HTTP stream: a cursor with a
// now-window ahead of it.
type Stream struct {
	s      *Scheduler
	id     backend.ID
	file   backend.FileInfo
	cursor int64
	seq    uint64 // creation order; newer streams win runway ties
	closed bool

	// waiting is set while a WaitBytes on this stream is pending. Every
	// blocked stream's window ties at rank 0 — the backend pursues them
	// all concurrently at top urgency (03-streaming.md).
	waiting bool
}

// New starts a scheduler over b, consuming b's event channel. There must be
// only one consumer of a backend's events; the scheduler is it.
func New(b backend.Backend, cfg Config) *Scheduler {
	s := &Scheduler{
		b:        b,
		cfg:      cfg.withDefaults(),
		torrents: make(map[backend.ID]*torrentState),
		quit:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	s.nudger, _ = b.(backend.Reannouncer)
	s.rescuer, _ = b.(backend.PieceRescuer)
	s.swarmObs, _ = b.(backend.SwarmObserver)
	go s.loop()
	return s
}

// Close stops the event loop. Blocked WaitBytes calls are woken with an
// error.
func (s *Scheduler) Close() {
	close(s.quit)
	<-s.done
}

// OpenStream registers a stream over file fileIndex of id with its cursor
// at offset, issuing head/tail bootstrap windows on the file's first open.
func (s *Scheduler) OpenStream(ctx context.Context, id backend.ID, fileIndex int, offset int64) (*Stream, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ts, err := s.ensureTorrentLocked(ctx, id)
	if err != nil {
		return nil, err
	}
	if fileIndex < 0 || fileIndex >= len(ts.files) {
		return nil, backend.ErrFileNotFound
	}
	file := ts.files[fileIndex]

	s.streamSeq++
	st := &Stream{s: s, id: id, file: file, cursor: offset, seq: s.streamSeq}
	ts.streams = append(ts.streams, st)

	if !ts.bootstrapIssued[fileIndex] {
		ts.bootstrapIssued[fileIndex] = true
		head := min(s.cfg.BootstrapHeadBytes, file.Length)
		if r := pieces.FromByteRange(file.Offset, ts.info.PieceSize, 0, head); !r.Empty() {
			ts.bootstraps = append(ts.bootstraps, r)
		}
		tail := min(s.cfg.BootstrapTailBytes, file.Length)
		if r := pieces.FromByteRange(file.Offset, ts.info.PieceSize, file.Length-tail, tail); !r.Empty() {
			ts.bootstraps = append(ts.bootstraps, r)
		}
	}
	s.flushLocked(id, ts)
	return st, nil
}

// Advance moves the stream's cursor (bytes actually written to the
// consumer). The window slides with it; priorities are re-flushed only when
// the desired piece ranges actually change.
func (st *Stream) Advance(cursor int64) {
	st.s.mu.Lock()
	defer st.s.mu.Unlock()
	if st.closed {
		return
	}
	st.cursor = cursor
	if ts, ok := st.s.torrents[st.id]; ok {
		st.s.flushLocked(st.id, ts)
	}
}

// Close drops the stream's window. Idempotent.
func (st *Stream) Close() {
	st.s.mu.Lock()
	defer st.s.mu.Unlock()
	if st.closed {
		return
	}
	st.closed = true
	if ts, ok := st.s.torrents[st.id]; ok {
		ts.streams = slices.DeleteFunc(ts.streams, func(o *Stream) bool { return o == st })
		if lg := st.s.cfg.WindowLinger; lg > 0 {
			if r := st.windowLocked(ts); !r.Empty() && !ts.state.HasRange(r) {
				expires := st.s.cfg.Now().Add(lg)
				if i := slices.IndexFunc(ts.lingers, func(p prepareEntry) bool { return p.r == r }); i >= 0 {
					ts.lingers[i].expires = expires // refresh, don't duplicate
				} else {
					ts.lingers = append(ts.lingers, prepareEntry{r: r, expires: expires})
				}
			}
		}
		st.s.flushLocked(st.id, ts)
	}
}

// windowLocked is the stream's current now-window as a piece range —
// the same window flushLocked computes for it while it is open.
func (st *Stream) windowLocked(ts *torrentState) pieces.Range {
	remaining := st.file.Length - st.cursor
	if remaining <= 0 {
		return pieces.Range{}
	}
	length := min(st.s.cfg.NowWindowBytes, remaining)
	return pieces.FromByteRange(st.file.Offset, ts.info.PieceSize, st.cursor, length)
}

// Prepare adds a TTL'd priority window for the byte range and reports its
// readiness: whether every backing piece is complete, and how many bytes of
// the range are available now. A negative length means "to end of file".
func (s *Scheduler) Prepare(ctx context.Context, id backend.ID, fileIndex int, off, length int64) (ready bool, bytesAvailable int64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ts, err := s.ensureTorrentLocked(ctx, id)
	if err != nil {
		return false, 0, err
	}
	if fileIndex < 0 || fileIndex >= len(ts.files) {
		return false, 0, backend.ErrFileNotFound
	}
	file := ts.files[fileIndex]
	if off < 0 || off > file.Length {
		return false, 0, backend.ErrOutOfRange
	}
	if length < 0 {
		length = file.Length - off
	}
	if off+length > file.Length {
		return false, 0, backend.ErrOutOfRange
	}

	// Prepare doubles as the readiness poll (02-http-api.md), so report
	// fresh piece state instead of racing the event loop's cached view.
	if state, err := s.b.PieceState(ctx, id); err == nil {
		ts.state = state
	}

	want := pieces.FromByteRange(file.Offset, ts.info.PieceSize, off, length)
	expires := s.cfg.Now().Add(s.cfg.PrepareTTL)
	if i := slices.IndexFunc(ts.prepares, func(p prepareEntry) bool { return p.r == want }); i >= 0 {
		ts.prepares[i].expires = expires // refresh, don't duplicate
	} else {
		ts.prepares = append(ts.prepares, prepareEntry{r: want, expires: expires})
	}
	s.flushLocked(id, ts)

	return ts.state.HasRange(want),
		pieces.AvailableBytes(ts.state, file.Offset+off, ts.info.PieceSize, length),
		nil
}

// WaitBytes blocks until the pieces backing bytes [off, off+length) of this
// stream's file are complete, marking the stream as blocked so the
// scheduler's ordering serves it FIFO among blocked streams.
func (st *Stream) WaitBytes(ctx context.Context, off, length int64) error {
	s := st.s
	s.mu.Lock()
	ts, err := s.ensureTorrentLocked(ctx, st.id)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	want := pieces.FromByteRange(st.file.Offset, ts.info.PieceSize, off, length)
	if s.hasSettledRangeLocked(ts, want) {
		s.mu.Unlock()
		return nil
	}
	st.waiting = true
	w := &waiter{r: want, ch: make(chan error, 1), created: s.cfg.Now()}
	ts.waiters[w] = struct{}{}
	s.flushLocked(st.id, ts) // re-rank: this stream is now blocked
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		st.waiting = false
		if cur, ok := s.torrents[st.id]; ok {
			delete(cur.waiters, w)
			s.flushLocked(st.id, cur)
		}
		s.mu.Unlock()
	}()
	select {
	case err := <-w.ch:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// WaitBytes blocks until the pieces backing bytes [off, off+length) of the
// file are complete, the context is done, or the torrent goes away.
func (s *Scheduler) WaitBytes(ctx context.Context, id backend.ID, fileIndex int, off, length int64) error {
	s.mu.Lock()
	ts, err := s.ensureTorrentLocked(ctx, id)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	if fileIndex < 0 || fileIndex >= len(ts.files) {
		s.mu.Unlock()
		return backend.ErrFileNotFound
	}
	file := ts.files[fileIndex]
	want := pieces.FromByteRange(file.Offset, ts.info.PieceSize, off, length)
	if s.hasSettledRangeLocked(ts, want) {
		s.mu.Unlock()
		return nil
	}
	w := &waiter{r: want, ch: make(chan error, 1), created: s.cfg.Now()}
	ts.waiters[w] = struct{}{}
	s.mu.Unlock()

	select {
	case err := <-w.ch:
		return err
	case <-ctx.Done():
		s.mu.Lock()
		if ts, ok := s.torrents[id]; ok {
			delete(ts.waiters, w)
		}
		s.mu.Unlock()
		return ctx.Err()
	}
}

// --- internals ---

// ensureTorrentLocked returns cached torrent state, snapshotting info,
// files, and piece state from the backend on first touch.
func (s *Scheduler) ensureTorrentLocked(ctx context.Context, id backend.ID) (*torrentState, error) {
	if ts, ok := s.torrents[id]; ok && !ts.removed {
		return ts, nil
	}
	info, err := s.b.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	files, err := s.b.Files(ctx, id)
	if err != nil {
		return nil, err
	}
	state, err := s.b.PieceState(ctx, id)
	if err != nil {
		return nil, err
	}
	ts := &torrentState{
		info:            info,
		files:           files,
		state:           state,
		bootstrapIssued: make(map[int]bool),
		waiters:         make(map[*waiter]struct{}),
	}
	s.torrents[id] = ts
	return ts, nil
}

// flushLocked recomputes the torrent's desired ranges (most-urgent first)
// and pushes them to the backend if they changed.
func (s *Scheduler) flushLocked(id backend.ID, ts *torrentState) {
	now := s.cfg.Now()
	ts.prepares = slices.DeleteFunc(ts.prepares, func(p prepareEntry) bool {
		return now.After(p.expires) || ts.state.HasRange(p.r)
	})
	ts.bootstraps = slices.DeleteFunc(ts.bootstraps, func(r pieces.Range) bool {
		return ts.state.HasRange(r)
	})
	ts.lingers = slices.DeleteFunc(ts.lingers, func(p prepareEntry) bool {
		return now.After(p.expires) || ts.state.HasRange(p.r)
	})

	// Stream now-windows, ranked by urgency (03-streaming.md, "Concurrent
	// windows, ranked urgency"). Every blocked stream ties at rank 0 —
	// with real concurrent piece control there is no serialization to
	// fairly queue for; they all get the backend's top urgency and
	// progress in parallel. Non-blocked windows follow at increasing
	// ranks, smallest runway first (closest to stalling gets the
	// next-tightest tier), newest stream winning ties (a new stream is
	// the freshest expression of user intent). Bootstraps and prepares
	// rank after the stream windows.
	type win struct {
		r       pieces.Range
		blocked bool
		runway  int64
		seq     uint64
	}
	var wins []win
	for _, st := range ts.streams {
		remaining := st.file.Length - st.cursor
		if remaining <= 0 {
			continue
		}
		length := min(s.cfg.NowWindowBytes, remaining)
		r := pieces.FromByteRange(st.file.Offset, ts.info.PieceSize, st.cursor, length)
		wins = append(wins, win{
			r:       r,
			blocked: st.waiting,
			runway:  pieces.AvailableBytes(ts.state, st.file.Offset+st.cursor, ts.info.PieceSize, length),
			seq:     st.seq,
		})
	}
	slices.SortStableFunc(wins, func(a, b win) int {
		switch {
		case a.blocked != b.blocked:
			if a.blocked {
				return -1
			}
			return 1
		case a.blocked: // both blocked: rank ties at 0, order irrelevant
			return 0
		case a.runway != b.runway:
			if a.runway < b.runway {
				return -1
			}
			return 1
		default: // newest stream first
			if a.seq > b.seq {
				return -1
			}
			if a.seq < b.seq {
				return 1
			}
			return 0
		}
	})

	// Blocked windows all tie at rank 0; every window after them (the sort
	// puts blocked first) takes the next rank in turn.
	desired := make([]backend.PriorityRange, 0, len(wins)+len(ts.lingers)+len(ts.bootstraps)+len(ts.prepares))
	nextRank := 0
	for _, w := range wins {
		if w.blocked {
			desired = append(desired, backend.PriorityRange{Range: w.r, Rank: 0})
			nextRank = 1
			continue
		}
		desired = append(desired, backend.PriorityRange{Range: w.r, Rank: nextRank})
		nextRank++
	}
	// Lingering windows of just-closed streams rank below every live
	// window: a reopen at the same position finds the pieces still
	// deadlined (no teardown/rebuild), while genuinely abandoned windows
	// only cost a few seconds of loose-deadline prefetch before expiry.
	for _, p := range ts.lingers {
		desired = append(desired, backend.PriorityRange{Range: p.r, Rank: nextRank})
		nextRank++
	}
	for _, r := range ts.bootstraps {
		desired = append(desired, backend.PriorityRange{Range: r, Rank: nextRank})
		nextRank++
	}
	for _, p := range ts.prepares {
		desired = append(desired, backend.PriorityRange{Range: p.r, Rank: nextRank})
		nextRank++
	}

	if slices.Equal(desired, ts.lastFlushed) {
		return
	}
	ts.lastFlushed = desired
	ts.pendingFlush = desired
	ts.hasPending = true
	if !ts.flushing {
		ts.flushing = true
		go s.deliverFlushes(id)
	}
}

// deliverFlushes sends parked desired-range sets to the backend without
// holding the scheduler mutex. Holding it across Prioritize (several RPC
// round trips on a real backend) would serialize piece-event handling,
// waiter wakeups, and cursor advances behind network latency — measured
// live, that capped a loopback stream at ~7 MiB/s because every piece
// boundary paid a full flush before the reader could continue.
func (s *Scheduler) deliverFlushes(id backend.ID) {
	for {
		s.mu.Lock()
		ts, ok := s.torrents[id]
		if !ok {
			s.mu.Unlock()
			return
		}
		if !ts.hasPending {
			ts.flushing = false
			s.mu.Unlock()
			return
		}
		desired := ts.pendingFlush
		ts.hasPending = false
		s.mu.Unlock()

		if err := s.b.Prioritize(context.Background(), id, desired); err != nil {
			s.cfg.Log.Warn("prioritize failed", "torrent", id, "err", err)
		}
	}
}

func (s *Scheduler) loop() {
	defer close(s.done)
	ticker := time.NewTicker(s.cfg.RepollInterval)
	defer ticker.Stop()
	for {
		select {
		case e, ok := <-s.b.Events():
			if !ok {
				s.failAllWaiters(errors.New("backend closed"))
				return
			}
			s.handleEvent(e)
		case <-ticker.C:
			s.repoll()
		case <-s.quit:
			s.failAllWaiters(errors.New("scheduler closed"))
			return
		}
	}
}

func (s *Scheduler) handleEvent(e backend.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ts, ok := s.torrents[e.Torrent]
	if !ok {
		return
	}
	switch e.Type {
	case backend.EventPieceCompleted:
		if e.Piece >= 0 && e.Piece < ts.state.Len() && !ts.state.Has(e.Piece) {
			if s.cfg.PieceSettleTime > 0 {
				if ts.completedAt == nil {
					ts.completedAt = make(map[int]time.Time)
				}
				ts.completedAt[e.Piece] = s.cfg.Now()
			}
			ts.state.Set(e.Piece)
			s.wakeWaitersLocked(ts)
		}
	case backend.EventTorrentRemoved:
		ts.removed = true
		for w := range ts.waiters {
			w.ch <- backend.ErrTorrentNotFound
		}
		delete(s.torrents, e.Torrent)
	case backend.EventMetadataResolved:
		// Cached info/files are stale (fetched while pending is refused, so
		// this is mostly defensive); drop and refetch on next touch.
		delete(s.torrents, e.Torrent)
	}
}

// repoll re-snapshots piece state for torrents that have blocked waiters,
// guarding against lost events (the Events contract allows drops), and
// stall-nudges torrents whose waiters have been blocked too long.
func (s *Scheduler) repoll() {
	s.mu.Lock()
	var ids []backend.ID
	for id, ts := range s.torrents {
		if len(ts.waiters) > 0 {
			ids = append(ids, id)
		}
	}
	s.mu.Unlock()

	for _, id := range ids {
		state, err := s.b.PieceState(context.Background(), id)
		nudge := false
		s.mu.Lock()
		if ts, ok := s.torrents[id]; ok {
			if err == nil {
				s.stampNewlyCompleteLocked(ts, state)
				ts.state = state
				s.wakeWaitersLocked(ts)
				nudge = s.wantsNudgeLocked(ts)
			} else if errors.Is(err, backend.ErrTorrentNotFound) {
				ts.removed = true
				for w := range ts.waiters {
					w.ch <- backend.ErrTorrentNotFound
				}
				delete(s.torrents, id)
			}
		}
		s.mu.Unlock()
		if nudge {
			s.cfg.Log.Debug("stall nudge: forcing reannounce", "torrent", id)
			if err := s.nudger.Reannounce(context.Background(), id); err != nil {
				s.cfg.Log.Warn("stall nudge failed", "torrent", id, "err", err)
			}
		}
		s.rescueStalledPieces(id)
	}
}

// rescueStalledPieces finds pieces that blocked streams have been stuck
// on past StallRescueAfter, confirms against fresh swarm state that each
// is actively downloading (its blocks are assigned to peers — the
// hostage condition; unavailable or idle pieces are left alone), and
// asks the backend to kick the holders. See Config.StallRescueAfter.
func (s *Scheduler) rescueStalledPieces(id backend.ID) {
	if s.rescuer == nil || s.swarmObs == nil || s.cfg.StallRescueAfter <= 0 {
		return
	}

	s.mu.Lock()
	ts, ok := s.torrents[id]
	if !ok || len(ts.waiters) == 0 {
		s.mu.Unlock()
		return
	}
	now := s.cfg.Now()
	var candidates []int
	for w := range ts.waiters {
		if now.Sub(w.created) < s.cfg.StallRescueAfter {
			continue
		}
		for p := w.r.Begin; p < w.r.End; p++ {
			if p >= 0 && p < ts.state.Len() && !ts.state.Has(p) {
				if last, ok := ts.lastRescue[p]; !ok || now.Sub(last) >= s.cfg.StallRescueAfter {
					candidates = append(candidates, p)
				}
				break // only the first missing piece blocks the read
			}
		}
	}
	s.mu.Unlock()
	if len(candidates) == 0 {
		return
	}

	swarm, err := s.swarmObs.PieceSwarmStates(context.Background(), id)
	if err != nil {
		return
	}
	for _, p := range candidates {
		// Rescue anything except unavailable (kicking peers can't help a
		// piece nobody has) and have (done). "Downloading" is the classic
		// hostage; "available" covers a holder that never transmits at
		// all — the client only marks a piece downloading while bytes
		// are actually arriving, so a fully dead peer's hostage piece
		// reads as merely available. The rescue itself is a no-op when
		// no blocks of the piece are in the requested state, so an
		// idle-but-truly-unrequested piece kicks nobody.
		if p >= len(swarm) || swarm[p] == backend.PieceSwarmUnavailable || swarm[p] == backend.PieceSwarmHave {
			continue
		}
		s.mu.Lock()
		if ts, ok := s.torrents[id]; ok {
			if ts.lastRescue == nil {
				ts.lastRescue = make(map[int]time.Time)
			}
			ts.lastRescue[p] = s.cfg.Now()
		}
		s.mu.Unlock()
		s.cfg.Log.Info("straggler rescue: kicking peers holding blocked piece", "torrent", id, "piece", p)
		if err := s.rescuer.RescuePiece(context.Background(), id, p); err != nil {
			s.cfg.Log.Warn("straggler rescue failed", "torrent", id, "piece", p, "err", err)
		}
	}
}

// wantsNudgeLocked reports whether the torrent's oldest still-blocked
// waiter has exceeded StallNudgeAfter, at most once per interval (see
// Config.StallNudgeAfter). It advances lastNudge when it fires.
func (s *Scheduler) wantsNudgeLocked(ts *torrentState) bool {
	if s.nudger == nil || s.cfg.StallNudgeAfter <= 0 || len(ts.waiters) == 0 {
		return false
	}
	now := s.cfg.Now()
	if now.Sub(ts.lastNudge) < s.cfg.StallNudgeAfter {
		return false
	}
	for w := range ts.waiters {
		if now.Sub(w.created) >= s.cfg.StallNudgeAfter {
			ts.lastNudge = now
			return true
		}
	}
	return false
}

func (s *Scheduler) wakeWaitersLocked(ts *torrentState) {
	for w := range ts.waiters {
		if s.hasSettledRangeLocked(ts, w.r) {
			w.ch <- nil
			delete(ts.waiters, w)
		}
	}
}

// hasSettledRangeLocked is HasRange plus, when PieceSettleTime is set, a
// grace period since each piece's completion was first observed. See
// Config.PieceSettleTime.
func (s *Scheduler) hasSettledRangeLocked(ts *torrentState, r pieces.Range) bool {
	if !ts.state.HasRange(r) {
		return false
	}
	if s.cfg.PieceSettleTime <= 0 || len(ts.completedAt) == 0 {
		return true
	}
	now := s.cfg.Now()
	for i := r.Begin; i < r.End; i++ {
		if t, tracked := ts.completedAt[i]; tracked {
			if now.Sub(t) < s.cfg.PieceSettleTime {
				return false
			}
			delete(ts.completedAt, i) // permanently settled; stop tracking
		}
	}
	return true
}

// stampNewlyCompleteLocked records the completion time of every piece that
// is set in next but not in ts.state, ahead of ts.state being updated to
// next. No-op when settle tracking is off.
func (s *Scheduler) stampNewlyCompleteLocked(ts *torrentState, next pieces.Bitfield) {
	if s.cfg.PieceSettleTime <= 0 {
		return
	}
	now := s.cfg.Now()
	for i := 0; i < next.Len(); i++ {
		if next.Has(i) && !ts.state.Has(i) {
			if ts.completedAt == nil {
				ts.completedAt = make(map[int]time.Time)
			}
			ts.completedAt[i] = now
		}
	}
}

func (s *Scheduler) failAllWaiters(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ts := range s.torrents {
		for w := range ts.waiters {
			w.ch <- err
		}
		ts.waiters = make(map[*waiter]struct{})
	}
}
