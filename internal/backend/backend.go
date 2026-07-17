// Package backend defines the pluggable torrent-client interface specified
// in docs/spec/01-backends.md. The streaming core talks to torrent clients
// exclusively through this contract; nothing client-specific may leak above
// it.
package backend

import (
	"context"
	"errors"
	"regexp"
	"strings"

	"github.com/jonny-gm/torrentseek/internal/pieces"
)

// ID is a torrent identity: the info-hash, presented as "btih:<hex>".
type ID string

var btihRe = regexp.MustCompile(`^btih:[0-9a-f]{40}$`)

// ParseID validates and normalizes an id string into an ID.
func ParseID(s string) (ID, error) {
	s = strings.ToLower(s)
	if !btihRe.MatchString(s) {
		return "", ErrInvalidID
	}
	return ID(s), nil
}

// State is the lifecycle state of a torrent as seen by consumers.
type State string

const (
	// StateMetadataPending means the torrent was added from a magnet link
	// and its metadata (file list, piece layout) is not yet resolved.
	StateMetadataPending State = "metadata_pending"
	StateDownloading     State = "downloading"
	StateSeeding         State = "seeding"
	StateStopped         State = "stopped"
)

// TorrentInfo describes a torrent. PieceSize, PieceCount, and TotalSize are
// exact once metadata is resolved, even at zero bytes downloaded.
type TorrentInfo struct {
	ID         ID
	Name       string
	TotalSize  int64
	PieceSize  int64
	PieceCount int
	// Progress is completion in [0,1].
	Progress float64
	State    State
	// RateDownload is the client's current download speed for this torrent
	// in bytes/second; Peers the connected peer count. Both are
	// observability data (zero where a backend can't report them).
	RateDownload int64
	Peers        int
}

// FileInfo describes one file inside a torrent. Offset is the byte offset
// of the file within the torrent payload (files are laid out sequentially),
// which is what maps file byte ranges onto torrent pieces.
type FileInfo struct {
	Index  int
	Path   string
	Length int64
	Offset int64
}

// AddRequest adds a torrent from exactly one source: a magnet URI or raw
// .torrent file bytes.
type AddRequest struct {
	Magnet   string
	Metainfo []byte
}

// AddResult reports the torrent's id and whether it already existed.
// Re-adding a known torrent is not an error (idempotent add).
type AddResult struct {
	ID       ID
	Existing bool
}

// EventType enumerates backend event kinds.
type EventType string

const (
	EventPieceCompleted   EventType = "piece_completed"
	EventMetadataResolved EventType = "metadata_resolved"
	EventTorrentAdded     EventType = "torrent_added"
	EventTorrentRemoved   EventType = "torrent_removed"
)

// Event is a backend notification. Piece is meaningful only for
// EventPieceCompleted. Backends without a push channel synthesize events by
// polling; the interface hides this.
type Event struct {
	Type    EventType
	Torrent ID
	Piece   int
}

// PriorityRange is one desired piece range with its relative urgency.
// Rank 0 is most urgent; multiple ranges may share a rank (every blocked
// stream ties at 0 — see 03-streaming.md). A backend with real per-piece
// control maps rank to how aggressively it pursues the range (e.g. a
// deadline tier); a backend limited to coarser control serves ranges in
// list order, which is sorted by rank.
type PriorityRange struct {
	pieces.Range
	Rank int
}

// Sentinel errors. Backends must return these (possibly wrapped) so the
// layers above can map them to API error codes.
var (
	ErrInvalidID       = errors.New("invalid torrent id")
	ErrTorrentNotFound = errors.New("torrent not found")
	ErrFileNotFound    = errors.New("file not found")
	ErrMetadataPending = errors.New("torrent metadata not yet resolved")
	ErrOutOfRange      = errors.New("read out of range")
	// ErrIncomplete is returned by ReadAt when the requested bytes are not
	// fully downloaded. Per the interface contract the caller must wait on
	// PieceState/Events before reading; hitting this indicates a scheduler
	// bug, so it is loud rather than blocking.
	ErrIncomplete = errors.New("requested bytes not yet complete")
	// ErrBackendUnavailable means the torrent client cannot currently be
	// reached (not running, or the connection dropped and reconnection is
	// still in progress). It is a transient condition, not a TorrentSeek
	// failure: the daemon stays up, keeps retrying, and operations start
	// working again the moment the client is back.
	ErrBackendUnavailable = errors.New("torrent client unreachable")
)

// Backend is the minimal contract the streaming core needs from a torrent
// client. Implementations must be safe for concurrent use.
type Backend interface {
	// Add is idempotent: adding a torrent that already exists returns the
	// existing id with Existing set, not an error.
	Add(ctx context.Context, req AddRequest) (AddResult, error)

	Remove(ctx context.Context, id ID, deleteData bool) error

	List(ctx context.Context) ([]TorrentInfo, error)
	Get(ctx context.Context, id ID) (TorrentInfo, error)

	// Files reports exact lengths once metadata is resolved. It returns
	// ErrMetadataPending while a magnet add is still resolving.
	Files(ctx context.Context, id ID) ([]FileInfo, error)

	// PieceState returns a snapshot bitfield of completed pieces.
	PieceState(ctx context.Context, id ID) (pieces.Bitfield, error)

	// Prioritize asks the client to make the pieces backing these ranges
	// arrive soon, every range concurrently, with per-range urgency given
	// by Rank (sorted ascending). It is called frequently as read cursors
	// move: implementations must coalesce redundant calls rather than
	// round-tripping each one.
	Prioritize(ctx context.Context, id ID, ranges []PriorityRange) error

	// ReadAt reads completed bytes of file fileIndex at offset off. It
	// never returns incomplete data: callers wait on PieceState/Events
	// first, and ReadAt fails with ErrIncomplete otherwise.
	ReadAt(ctx context.Context, id ID, fileIndex int, p []byte, off int64) (int, error)

	// Events delivers backend notifications. The channel is closed by
	// Close. Slow consumers may lose events; consumers requiring exact
	// state must re-snapshot via PieceState.
	Events() <-chan Event

	Close() error
}

// Reannouncer is an optional capability: force an immediate tracker
// reannounce to hunt for fresh peers. Deadlines and priorities steer the
// peers a client already has, but they cannot conjure a piece no
// connected peer holds — when a stream stays blocked, acquiring new
// peers is the only lever left. The scheduler nudges backends that
// implement this when a blocked wait exceeds its stall threshold
// (03-streaming.md, "Stall nudge").
type Reannouncer interface {
	Reannounce(ctx context.Context, id ID) error
}

// PieceSwarmState is a piece's relationship to the connected swarm — not
// just "do I have it" but "can I even get it": a blocked stream over an
// Unavailable piece is starving on availability (only new peers help),
// while one over an Available piece is starving on scheduling or peer
// throughput. Surfacing the difference is what makes long stalls
// diagnosable from a log instead of a packet capture.
type PieceSwarmState uint8

const (
	// PieceSwarmUnavailable: not downloaded and no connected peer has it.
	PieceSwarmUnavailable PieceSwarmState = iota
	// PieceSwarmAvailable: a connected peer has it; no transfer active.
	PieceSwarmAvailable
	// PieceSwarmDownloading: being downloaded from a peer right now.
	PieceSwarmDownloading
	// PieceSwarmHave: complete locally.
	PieceSwarmHave
)

func (s PieceSwarmState) String() string {
	switch s {
	case PieceSwarmUnavailable:
		return "unavailable"
	case PieceSwarmAvailable:
		return "available"
	case PieceSwarmDownloading:
		return "downloading"
	case PieceSwarmHave:
		return "have"
	}
	return "unknown"
}

// PieceRescuer is an optional capability: kick the peers holding a
// piece's outstanding block requests hostage, so the blocks requeue and
// the piece's still-active urgency re-requests them from healthy peers.
// A piece can sit "downloading" for a minute-plus while its blocks are
// parked in a stalled peer's queue — the client's own request timeout is
// tuned for downloads, not deadlines (observed in the field: one piece
// hostage 79 seconds while 130 peers delivered 8 MB/s of everything
// else). The scheduler invokes this for a blocked stream stuck on an
// actively-downloading piece (03-streaming.md, "Straggler rescue").
type PieceRescuer interface {
	RescuePiece(ctx context.Context, id ID, piece int) error
}

// SwarmObserver is an optional capability: per-piece swarm availability.
// Backends whose client exposes piece availability (Deluge's status
// carries it) implement this; callers must treat absence as "unknown",
// never as unavailable.
type SwarmObserver interface {
	PieceSwarmStates(ctx context.Context, id ID) ([]PieceSwarmState, error)
}
