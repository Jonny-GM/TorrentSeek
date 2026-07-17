// Package deluge implements backend.Backend over Deluge's daemon RPC plus
// the deluge-piece-priority plugin, per docs/spec/04-deluge-backend.md.
// Reads go directly against Deluge's download location on disk, so
// TorrentSeek must run co-located with deluged (01-backends.md).
package deluge

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/jonny-gm/torrentseek/internal/backend"
	"github.com/jonny-gm/torrentseek/internal/pieces"
)

// Config configures the Deluge backend.
type Config struct {
	// Addr is deluged's RPC endpoint, host:port.
	Addr     string
	Username string
	Password string

	// HotPoll is the piece-state poll interval for torrents with active
	// interest (default 500 ms); ColdPoll sweeps everything (default 10 s).
	HotPoll  time.Duration
	ColdPoll time.Duration
	// HotWindow is how long a torrent stays hot after a Prioritize call
	// (default 30 s).
	HotWindow time.Duration
	// IdleGrace is how long a torrent keeps running after its last
	// stream/window closes before it is paused (default 5 min). Pausing
	// disconnects every peer, so pausing on a momentary zero-interest gap
	// — a media player closing one range request before issuing the next —
	// would force a full swarm rebuild on the reopen.
	IdleGrace time.Duration
	Log       *slog.Logger
}

func (c Config) withDefaults() Config {
	if c.Addr == "" {
		c.Addr = "127.0.0.1:58846"
	}
	if c.HotPoll <= 0 {
		c.HotPoll = 500 * time.Millisecond
	}
	if c.ColdPoll <= 0 {
		c.ColdPoll = 10 * time.Second
	}
	if c.HotWindow <= 0 {
		c.HotWindow = 30 * time.Second
	}
	if c.IdleGrace <= 0 {
		c.IdleGrace = 5 * time.Minute
	}
	if c.Log == nil {
		c.Log = slog.Default()
	}
	return c
}

// Deadline tiers (04-deluge-backend.md, Scheduling strategy): a range at
// rank r gets deadline base + r*step, plus an intra-range stagger so the
// piece at the range start (the read cursor) is strictly hottest and
// urgency decays piece by piece toward the window's far edge. Without the
// stagger every piece in a window shares one deadline and libtorrent has
// no reason to finish the piece the blocked reader is sitting on before
// the 31 MiB behind it — observed live as a stream stalled minutes on a
// single piece while the swarm poured data into the rest of the window.
// Capped. Starting values to tune against real swarms.
const (
	deadlineBaseMS      = 500
	deadlineStepMS      = 1500
	deadlineIntraStepMS = 100
	deadlineMaxMS       = 30000

	// piecePriorityHigh/Default are libtorrent's 0-7 scale: 7 for pieces
	// in an active window, 4 (the libtorrent default) when released.
	piecePriorityHigh    = 7
	piecePriorityDefault = 4

	// filePriorityNormal marks a file wanted; 0 unwanted. Piece deadlines
	// carry the actual urgency, so wanted files need no more than normal.
	filePriorityNormal = 4
)

// Backend implements backend.Backend over deluged's RPC.
type Backend struct {
	cfg Config

	mu       sync.Mutex
	c        *conn // nil while disconnected
	torrents map[backend.ID]*tState
	rescues  map[rescueKey]rescueTrack

	events chan backend.Event
	quit   chan struct{}
	done   chan struct{}
}

// tState is the cached view of one torrent, updated by the poller.
type tState struct {
	info             backend.TorrentInfo
	files            []backend.FileInfo
	rawPaths         []string // Deluge file paths, relative to downloadLocation
	downloadLocation string
	have             pieces.Bitfield
	swarm            []backend.PieceSwarmState // per-piece availability (Deluge pieces field)
	pending          bool                      // metadata not yet resolved

	// Desired serving state (from Prioritize) and what was last sent, for
	// write coalescing. sentPieces maps piece -> deadline ms last sent
	// (piece priority High implied); sentWanted is the last-sent wanted
	// file set, as a canonical string.
	desired    []backend.PriorityRange
	hotUntil   time.Time
	idleSince  time.Time
	sentPieces map[int]int64
	sentWanted string
	// ownAdd marks torrents this backend added (as opposed to ones added
	// externally, whose priorities and pause state are never touched
	// outside a flush).
	ownAdd bool
}

// New connects to deluged (verifying credentials work) and starts the
// poll/event loop.
func New(ctx context.Context, cfg Config) (*Backend, error) {
	cfg = cfg.withDefaults()
	b := &Backend{
		cfg:      cfg,
		torrents: make(map[backend.ID]*tState),
		rescues:  make(map[rescueKey]rescueTrack),
		events:   make(chan backend.Event, 1024),
		quit:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	c, err := dial(ctx, cfg.Addr, cfg.Username, cfg.Password)
	switch {
	case err == nil:
		b.c = c
	case isDaemonRejection(err):
		// The daemon answered and said no (bad credentials, bad request):
		// retrying the same config forever cannot succeed, so this is the
		// one startup failure that should be loud and fatal.
		return nil, err
	default:
		// Unreachable (not running yet, wrong port, network blip) is a
		// transient condition, not a configuration error: start anyway,
		// serve ErrBackendUnavailable from every operation, and let the
		// loop's reconnect backoff pick the daemon up whenever it appears.
		cfg.Log.Warn("deluge: daemon unreachable; starting anyway and retrying in the background",
			"addr", cfg.Addr, "err", err)
	}
	go b.loop()
	return b, nil
}

// isDaemonRejection reports whether err is an explicit RPC-level error
// response from a live daemon (e.g. BadLoginError), as opposed to a
// connection-level failure to reach one.
func isDaemonRejection(err error) bool {
	var re *rpcError
	return errors.As(err, &re)
}

// Connected reports whether the backend currently holds a live daemon
// connection. Purely informational (startup logging, health); operations
// fail with backend.ErrBackendUnavailable while false and recover on
// their own once the reconnect loop succeeds.
func (b *Backend) Connected() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.c != nil
}

// conn returns the live connection, or an error while disconnected (the
// loop is responsible for redialing; callers just fail fast and retry).
func (b *Backend) conn() (*conn, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.c == nil {
		return nil, fmt.Errorf("%w: deluged at %s (reconnecting in the background)",
			backend.ErrBackendUnavailable, b.cfg.Addr)
	}
	return b.c, nil
}

// --- Backend interface ---

var alreadyInSessionRe = regexp.MustCompile(`already (?:in session|being added) \(([0-9a-fA-F]{40})\)`)

// Add implements idempotent add: Deluge raises AddTorrentError naming the
// existing torrent's id, which maps to Existing rather than an error.
func (b *Backend) Add(ctx context.Context, req backend.AddRequest) (backend.AddResult, error) {
	c, err := b.conn()
	if err != nil {
		return backend.AddResult{}, err
	}

	var v any
	switch {
	case req.Magnet != "":
		// A paused torrent can't fetch magnet metadata, so magnets start
		// running; ingest pauses them at metadata resolution if no
		// interest has arrived by then.
		v, err = c.callTimeout(ctx, "core.add_torrent_magnet",
			[]any{req.Magnet, map[any]any{"add_paused": false}}, nil)
	case len(req.Metainfo) > 0:
		// Metainfo adds idle paused until first interest: an unpaused
		// all-files-unwanted torrent presents to the swarm as a seed, and
		// peers contacted in that state get dropped as redundant and then
		// sit in libtorrent's reconnect backoff — poisoning exactly the
		// peers a stream will need moments later.
		dump := base64.StdEncoding.EncodeToString(req.Metainfo)
		v, err = c.callTimeout(ctx, "core.add_torrent_file",
			[]any{"added.torrent", dump, map[any]any{"add_paused": true}}, nil)
	default:
		return backend.AddResult{}, errors.New("deluge: add: no magnet or metainfo")
	}

	if err != nil {
		var re *rpcError
		if errors.As(err, &re) {
			if m := alreadyInSessionRe.FindStringSubmatch(re.Args); m != nil {
				id, perr := backend.ParseID("btih:" + strings.ToLower(m[1]))
				if perr == nil {
					return backend.AddResult{ID: id, Existing: true}, nil
				}
			}
		}
		return backend.AddResult{}, err
	}

	hash, ok := v.(string)
	if !ok || hash == "" {
		return backend.AddResult{}, fmt.Errorf("deluge: add returned %v", v)
	}
	id, err := backend.ParseID("btih:" + strings.ToLower(hash))
	if err != nil {
		return backend.AddResult{}, fmt.Errorf("deluge: add returned unparseable id %q", hash)
	}

	// A paused add never checks data already on disk, so a torrent whose
	// payload is already complete would report progress 0 until its first
	// resume. piecepriority.verify rechecks without joining the swarm
	// (libtorrent's stop_when_ready: the torrent lands back paused the
	// moment checking completes, with no announce and no peer contact).
	// Deluge's own core.force_recheck is NOT a substitute — it resumes
	// the handle and re-pauses only when the checked alert is processed,
	// and connections formed in that window get torn down and
	// backoff-banned for ~60s: exactly the peers the first stream needs.
	// Best effort — a missed recheck only delays accurate progress until
	// the first resume.
	if len(req.Metainfo) > 0 {
		if _, err := c.callTimeout(ctx, "piecepriority.verify",
			[]any{strings.ToLower(hash)}, nil); err != nil {
			b.cfg.Log.Warn("deluge: verify after add failed", "torrent", hash, "err", err)
		}
	}

	// File priorities keep Deluge's wanted-by-default until the first
	// flush writes the real set (04-deluge-backend.md, Add semantics).
	// Pausing, not file-zeroing, is what prevents background downloading —
	// and deliberately so: an active torrent with every file unwanted is
	// vacuously "finished", so if the torrent is ever briefly active
	// outside this backend's control (a user recheck or resume from
	// Deluge's own UI), every peer it touches drops the connection as
	// seed-to-seed redundant and backoff-bans it for ~60s — exactly the
	// peers the first stream needs. Wanted-by-default makes such a window
	// an ordinary leecher contact instead. Magnet adds have no file list
	// yet — the poll pauses them when metadata resolves, if nothing wants
	// them by then (ownAdd marks them as ours; externally-added torrents
	// are never touched).
	if ts, err := b.refresh(ctx, id); err == nil {
		b.mu.Lock()
		ts.ownAdd = true
		b.mu.Unlock()
	}
	return backend.AddResult{ID: id}, nil
}

// rawID strips the btih: prefix: Deluge's torrent ids are the bare
// lowercase hex info-hash.
func rawID(id backend.ID) string { return strings.TrimPrefix(string(id), "btih:") }

func (b *Backend) Remove(ctx context.Context, id backend.ID, deleteData bool) error {
	c, err := b.conn()
	if err != nil {
		return err
	}
	_, err = c.callTimeout(ctx, "core.remove_torrent", []any{rawID(id), deleteData}, nil)
	if err != nil {
		if isNotFound(err) {
			return backend.ErrTorrentNotFound
		}
		return err
	}
	b.mu.Lock()
	delete(b.torrents, id)
	b.mu.Unlock()
	b.emit(backend.Event{Type: backend.EventTorrentRemoved, Torrent: id})
	return nil
}

// isNotFound matches Deluge's InvalidTorrentError (and the KeyError shape
// some paths raise for unknown ids).
func isNotFound(err error) bool {
	var re *rpcError
	if !errors.As(err, &re) {
		return false
	}
	return re.ExcType == "InvalidTorrentError" || re.ExcType == "KeyError"
}

func (b *Backend) List(ctx context.Context) ([]backend.TorrentInfo, error) {
	c, err := b.conn()
	if err != nil {
		return nil, err
	}
	all, err := b.fetchAll(ctx, c, nil)
	if err != nil {
		return nil, err
	}
	infos := make([]backend.TorrentInfo, 0, len(all))
	b.mu.Lock()
	defer b.mu.Unlock()
	for k, v := range all {
		st, ok := v.(map[any]any)
		if !ok {
			continue
		}
		id, err := backend.ParseID("btih:" + strings.ToLower(asString(k)))
		if err != nil {
			continue
		}
		ts := b.applyLocked(id, st)
		infos = append(infos, ts.info)
	}
	slices.SortFunc(infos, func(a, x backend.TorrentInfo) int {
		return strings.Compare(string(a.ID), string(x.ID))
	})
	return infos, nil
}

func (b *Backend) Get(ctx context.Context, id backend.ID) (backend.TorrentInfo, error) {
	ts, err := b.ensure(ctx, id)
	if err != nil {
		return backend.TorrentInfo{}, err
	}
	return ts.info, nil
}

func (b *Backend) Files(ctx context.Context, id backend.ID) ([]backend.FileInfo, error) {
	ts, err := b.refresh(ctx, id)
	if err != nil {
		return nil, err
	}
	if ts.pending {
		return nil, backend.ErrMetadataPending
	}
	return slices.Clone(ts.files), nil
}

// PieceSwarmStates implements the optional backend.SwarmObserver
// capability from Deluge's pieces status field (see parseStatus). The
// snapshot is fresh: like PieceState, every call re-fetches status.
func (b *Backend) PieceSwarmStates(ctx context.Context, id backend.ID) ([]backend.PieceSwarmState, error) {
	ts, err := b.refresh(ctx, id)
	if err != nil {
		return nil, err
	}
	if ts.pending {
		return nil, backend.ErrMetadataPending
	}
	return slices.Clone(ts.swarm), nil
}

func (b *Backend) PieceState(ctx context.Context, id backend.ID) (pieces.Bitfield, error) {
	ts, err := b.refresh(ctx, id)
	if err != nil {
		return pieces.Bitfield{}, err
	}
	return ts.have.Clone(), nil
}

// Reannounce implements the optional backend.Reannouncer capability:
// force an immediate tracker reannounce so a torrent whose connected
// peers cannot supply a wanted piece hunts for fresh ones.
func (b *Backend) Reannounce(ctx context.Context, id backend.ID) error {
	if _, err := b.ensure(ctx, id); err != nil {
		return err
	}
	c, err := b.conn()
	if err != nil {
		return err
	}
	_, err = c.callTimeout(ctx, "core.force_reannounce", []any{[]any{rawID(id)}}, nil)
	return err
}

// ReadAt is a direct disk read from the torrent's download location; the
// caller is responsible for only reading completed ranges
// (01-backends.md).
func (b *Backend) ReadAt(ctx context.Context, id backend.ID, fileIndex int, p []byte, off int64) (int, error) {
	ts, err := b.ensure(ctx, id)
	if err != nil {
		return 0, err
	}
	if ts.pending {
		return 0, backend.ErrMetadataPending
	}
	if fileIndex < 0 || fileIndex >= len(ts.files) {
		return 0, backend.ErrFileNotFound
	}
	fi := ts.files[fileIndex]
	if off < 0 || off > fi.Length {
		return 0, backend.ErrOutOfRange
	}
	if off+int64(len(p)) > fi.Length {
		p = p[:fi.Length-off]
	}
	if len(p) == 0 {
		return 0, nil
	}

	path := filepath.Join(ts.downloadLocation, filepath.FromSlash(ts.rawPaths[fileIndex]))
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, backend.ErrIncomplete
		}
		return 0, err
	}
	defer f.Close()
	n, err := f.ReadAt(p, off)
	if errors.Is(err, io.EOF) && n < len(p) {
		// The file exists but is shorter than the completed range claims:
		// bytes not yet written through.
		return n, backend.ErrIncomplete
	}
	return n, err
}

func (b *Backend) Events() <-chan backend.Event { return b.events }

func (b *Backend) Close() error {
	close(b.quit)
	<-b.done
	b.mu.Lock()
	if b.c != nil {
		b.c.close(nil)
		b.c = nil
	}
	b.mu.Unlock()
	close(b.events)
	return nil
}

// --- cache maintenance ---

// ensure returns cached state, fetching on first touch.
func (b *Backend) ensure(ctx context.Context, id backend.ID) (*tState, error) {
	b.mu.Lock()
	ts, ok := b.torrents[id]
	b.mu.Unlock()
	if ok {
		return ts, nil
	}
	return b.refresh(ctx, id)
}

// refresh fetches one torrent's status and updates the cache.
func (b *Backend) refresh(ctx context.Context, id backend.ID) (*tState, error) {
	c, err := b.conn()
	if err != nil {
		return nil, err
	}
	v, err := c.callTimeout(ctx, "core.get_torrent_status", []any{rawID(id), statusFields}, nil)
	if err != nil {
		if isNotFound(err) {
			return nil, backend.ErrTorrentNotFound
		}
		return nil, err
	}
	st, ok := v.(map[any]any)
	if !ok || len(st) == 0 {
		// get_torrent_status returns an empty dict for unknown ids rather
		// than erroring.
		return nil, backend.ErrTorrentNotFound
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.applyLocked(id, st), nil
}

// applyLocked merges a status snapshot into the cache, emitting
// piece-completed and metadata-resolved events for observed transitions.
func (b *Backend) applyLocked(id backend.ID, st map[any]any) *tState {
	next := parseStatus(id, st)
	prev, existed := b.torrents[id]
	if existed {
		// Carry serving state across snapshots.
		next.desired = prev.desired
		next.hotUntil = prev.hotUntil
		next.idleSince = prev.idleSince
		next.sentPieces = prev.sentPieces
		next.sentWanted = prev.sentWanted
		next.ownAdd = prev.ownAdd

		if prev.pending && !next.pending {
			b.emit(backend.Event{Type: backend.EventMetadataResolved, Torrent: id})
		}
		if prev.have.Len() == next.have.Len() {
			for i := 0; i < next.have.Len(); i++ {
				if next.have.Has(i) && !prev.have.Has(i) {
					b.emit(backend.Event{Type: backend.EventPieceCompleted, Torrent: id, Piece: i})
				}
			}
		}
	}
	b.torrents[id] = next
	return next
}
