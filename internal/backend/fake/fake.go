// Package fake is an in-memory backend.Backend with scriptable piece
// availability, per docs/spec/01-backends.md ("Testing posture"). The
// streaming core and scheduler are developed and tested against it, with no
// torrent client or swarm involved.
//
// Tests script the world explicitly: Register a torrent spec, Add it through
// the normal interface, then drive availability with CompleteRange /
// ServePrioritized. Nothing completes on its own, so tests are fully
// deterministic. File contents are generated from the spec's seed, so reads
// are verifiable without storing payloads.
package fake

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/jonny-gm/torrentseek/internal/backend"
	"github.com/jonny-gm/torrentseek/internal/pieces"
)

// FileSpec describes one file in a scripted torrent.
type FileSpec struct {
	Path   string
	Length int64
}

// TorrentSpec describes a scripted torrent. Its identity is derived from
// Name and Files, so equal specs collide like equal info-hashes would.
type TorrentSpec struct {
	Name      string
	PieceSize int64
	Files     []FileSpec
	// MetadataPending makes Add land in StateMetadataPending until the test
	// calls ResolveMetadata, mimicking a magnet whose metadata is fetching.
	MetadataPending bool
	// Seed drives content generation; distinct seeds give distinct bytes.
	Seed int64
}

func (s TorrentSpec) totalSize() int64 {
	var t int64
	for _, f := range s.Files {
		t += f.Length
	}
	return t
}

func (s TorrentSpec) pieceCount() int {
	if s.PieceSize <= 0 {
		return 0
	}
	return int((s.totalSize() + s.PieceSize - 1) / s.PieceSize)
}

type torrent struct {
	spec            TorrentSpec
	id              backend.ID
	added           bool
	metadataPending bool
	have            pieces.Bitfield
	// prioritized is the most recent Prioritize call, sorted by rank
	// (most-urgent first). Each call replaces the previous state
	// (coalescing semantics).
	prioritized []backend.PriorityRange
}

// Fake is the in-memory backend. Safe for concurrent use.
type Fake struct {
	mu       sync.Mutex
	torrents map[backend.ID]*torrent
	events   chan backend.Event
	closed   bool
}

// New returns an empty Fake with no torrents registered.
func New() *Fake {
	return &Fake{
		torrents: make(map[backend.ID]*torrent),
		events:   make(chan backend.Event, 256),
	}
}

// Register makes a torrent spec known to the fake (the fake's "universe of
// torrents that exist"). It does not add the torrent; tests Add it through
// the normal interface using the returned magnet or id.
func (f *Fake) Register(spec TorrentSpec) (backend.ID, string) {
	h := sha1.New()
	fmt.Fprintf(h, "%s/%d", spec.Name, spec.PieceSize)
	for _, file := range spec.Files {
		fmt.Fprintf(h, "/%s:%d", file.Path, file.Length)
	}
	id := backend.ID("btih:" + hex.EncodeToString(h.Sum(nil)))

	f.mu.Lock()
	defer f.mu.Unlock()
	// Same spec means same identity: re-registering must not clobber the
	// existing torrent's state, just like re-announcing an info-hash.
	if _, ok := f.torrents[id]; !ok {
		f.torrents[id] = &torrent{
			spec:            spec,
			id:              id,
			metadataPending: spec.MetadataPending,
			have:            pieces.NewBitfield(spec.pieceCount()),
		}
	}
	return id, "magnet:?xt=urn:btih:" + strings.TrimPrefix(string(id), "btih:")
}

var magnetRe = regexp.MustCompile(`(?i)urn:btih:([0-9a-f]{40})`)

func (f *Fake) Add(_ context.Context, req backend.AddRequest) (backend.AddResult, error) {
	var raw string
	switch {
	case req.Magnet != "" && req.Metainfo == nil:
		m := magnetRe.FindStringSubmatch(req.Magnet)
		if m == nil {
			return backend.AddResult{}, backend.ErrInvalidID
		}
		raw = "btih:" + strings.ToLower(m[1])
	case req.Metainfo != nil && req.Magnet == "":
		// The fake's "metainfo format" is simply the id string; real
		// backends parse bencoded .torrent bytes here.
		raw = string(req.Metainfo)
	default:
		return backend.AddResult{}, fmt.Errorf("add: exactly one of magnet or metainfo required")
	}
	id, err := backend.ParseID(raw)
	if err != nil {
		return backend.AddResult{}, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	t, ok := f.torrents[id]
	if !ok {
		return backend.AddResult{}, fmt.Errorf("fake: torrent %s not registered: %w", id, backend.ErrTorrentNotFound)
	}
	if t.added {
		return backend.AddResult{ID: id, Existing: true}, nil
	}
	t.added = true
	f.emitLocked(backend.Event{Type: backend.EventTorrentAdded, Torrent: id})
	return backend.AddResult{ID: id, Existing: false}, nil
}

// MetainfoFor returns bytes that Add accepts as a .torrent upload for a
// registered torrent.
func (f *Fake) MetainfoFor(id backend.ID) []byte { return []byte(id) }

func (f *Fake) Remove(_ context.Context, id backend.ID, _ bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	t, err := f.addedLocked(id)
	if err != nil {
		return err
	}
	t.added = false
	t.have = pieces.NewBitfield(t.spec.pieceCount())
	t.prioritized = nil
	f.emitLocked(backend.Event{Type: backend.EventTorrentRemoved, Torrent: id})
	return nil
}

func (f *Fake) List(_ context.Context) ([]backend.TorrentInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []backend.TorrentInfo
	for _, t := range f.torrents {
		if t.added {
			out = append(out, t.infoLocked())
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (f *Fake) Get(_ context.Context, id backend.ID) (backend.TorrentInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	t, err := f.addedLocked(id)
	if err != nil {
		return backend.TorrentInfo{}, err
	}
	return t.infoLocked(), nil
}

func (f *Fake) Files(_ context.Context, id backend.ID) ([]backend.FileInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	t, err := f.addedLocked(id)
	if err != nil {
		return nil, err
	}
	if t.metadataPending {
		return nil, backend.ErrMetadataPending
	}
	out := make([]backend.FileInfo, len(t.spec.Files))
	var off int64
	for i, file := range t.spec.Files {
		out[i] = backend.FileInfo{Index: i, Path: file.Path, Length: file.Length, Offset: off}
		off += file.Length
	}
	return out, nil
}

func (f *Fake) PieceState(_ context.Context, id backend.ID) (pieces.Bitfield, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	t, err := f.addedLocked(id)
	if err != nil {
		return pieces.Bitfield{}, err
	}
	if t.metadataPending {
		return pieces.Bitfield{}, backend.ErrMetadataPending
	}
	return t.have.Clone(), nil
}

func (f *Fake) Prioritize(_ context.Context, id backend.ID, ranges []backend.PriorityRange) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	t, err := f.addedLocked(id)
	if err != nil {
		return err
	}
	if t.metadataPending {
		return backend.ErrMetadataPending
	}
	t.prioritized = append([]backend.PriorityRange(nil), ranges...)
	return nil
}

func (f *Fake) ReadAt(_ context.Context, id backend.ID, fileIndex int, p []byte, off int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	t, err := f.addedLocked(id)
	if err != nil {
		return 0, err
	}
	if t.metadataPending {
		return 0, backend.ErrMetadataPending
	}
	if fileIndex < 0 || fileIndex >= len(t.spec.Files) {
		return 0, backend.ErrFileNotFound
	}
	var fileOff int64
	for i := 0; i < fileIndex; i++ {
		fileOff += t.spec.Files[i].Length
	}
	fileLen := t.spec.Files[fileIndex].Length
	if off < 0 || off > fileLen {
		return 0, backend.ErrOutOfRange
	}
	n := int64(len(p))
	atEOF := false
	if off+n >= fileLen {
		n = fileLen - off
		atEOF = true
	}
	need := pieces.FromByteRange(fileOff, t.spec.PieceSize, off, n)
	if !t.have.HasRange(need) {
		return 0, fmt.Errorf("pieces %v missing: %w", need, backend.ErrIncomplete)
	}
	for i := int64(0); i < n; i++ {
		p[i] = contentByte(t.spec.Seed, fileOff+off+i)
	}
	if atEOF {
		return int(n), io.EOF
	}
	return int(n), nil
}

func (f *Fake) Events() <-chan backend.Event { return f.events }

func (f *Fake) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.closed {
		f.closed = true
		close(f.events)
	}
	return nil
}

// --- test-scripting helpers (not part of backend.Backend) ---

// ResolveMetadata completes a pending magnet's metadata fetch.
func (f *Fake) ResolveMetadata(id backend.ID) {
	f.mu.Lock()
	defer f.mu.Unlock()
	t, err := f.addedLocked(id)
	if err != nil || !t.metadataPending {
		return
	}
	t.metadataPending = false
	f.emitLocked(backend.Event{Type: backend.EventMetadataResolved, Torrent: id})
}

// CompleteRange marks every piece in r complete, emitting an event per
// newly completed piece.
func (f *Fake) CompleteRange(id backend.ID, r pieces.Range) {
	f.mu.Lock()
	defer f.mu.Unlock()
	t, err := f.addedLocked(id)
	if err != nil || t.metadataPending {
		return
	}
	for i := r.Begin; i < r.End; i++ {
		f.completePieceLocked(t, i)
	}
}

// ServePrioritized completes up to n missing pieces in prioritized order
// (the most recent Prioritize call, most-urgent range first), simulating a
// swarm delivering what was asked for. It returns how many pieces it
// completed; fewer than n means the priority set had nothing left missing.
func (f *Fake) ServePrioritized(id backend.ID, n int) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	t, err := f.addedLocked(id)
	if err != nil || t.metadataPending {
		return 0
	}
	done := 0
	for _, r := range t.prioritized {
		for i := r.Begin; i < r.End && done < n; i++ {
			if i >= 0 && i < t.have.Len() && !t.have.Has(i) {
				f.completePieceLocked(t, i)
				done++
			}
		}
		if done >= n {
			break
		}
	}
	return done
}

// PrioritizedRanges returns the most recent Prioritize call's ranges, for
// asserting scheduler behavior.
func (f *Fake) PrioritizedRanges(id backend.ID) []backend.PriorityRange {
	f.mu.Lock()
	defer f.mu.Unlock()
	t, err := f.addedLocked(id)
	if err != nil {
		return nil
	}
	return append([]backend.PriorityRange(nil), t.prioritized...)
}

// FileContent returns the expected bytes of a file range, for verifying
// reads end-to-end. It ignores piece availability.
func (f *Fake) FileContent(id backend.ID, fileIndex int, off, length int64) []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	t, ok := f.torrents[id]
	if !ok || fileIndex < 0 || fileIndex >= len(t.spec.Files) {
		return nil
	}
	var fileOff int64
	for i := 0; i < fileIndex; i++ {
		fileOff += t.spec.Files[i].Length
	}
	out := make([]byte, length)
	for i := range out {
		out[i] = contentByte(t.spec.Seed, fileOff+off+int64(i))
	}
	return out
}

// --- internals ---

func (f *Fake) addedLocked(id backend.ID) (*torrent, error) {
	t, ok := f.torrents[id]
	if !ok || !t.added {
		return nil, backend.ErrTorrentNotFound
	}
	return t, nil
}

func (f *Fake) completePieceLocked(t *torrent, i int) {
	if i < 0 || i >= t.have.Len() || t.have.Has(i) {
		return
	}
	t.have.Set(i)
	f.emitLocked(backend.Event{Type: backend.EventPieceCompleted, Torrent: t.id, Piece: i})
}

func (f *Fake) emitLocked(e backend.Event) {
	if f.closed {
		return
	}
	select {
	case f.events <- e:
	default: // slow consumers lose events, per the interface contract
	}
}

func (t *torrent) infoLocked() backend.TorrentInfo {
	info := backend.TorrentInfo{
		ID:         t.id,
		Name:       t.spec.Name,
		TotalSize:  t.spec.totalSize(),
		PieceSize:  t.spec.PieceSize,
		PieceCount: t.spec.pieceCount(),
		State:      backend.StateDownloading,
	}
	if t.metadataPending {
		info.State = backend.StateMetadataPending
		return info
	}
	if n := t.have.Len(); n > 0 {
		info.Progress = float64(t.have.Count()) / float64(n)
	}
	if info.Progress == 1 {
		info.State = backend.StateSeeding
	}
	return info
}

// contentByte generates the deterministic payload byte at torrent offset i
// (SplitMix64 mix, one byte per offset).
func contentByte(seed, i int64) byte {
	x := uint64(i)*0x9E3779B97F4A7C15 + uint64(seed)*0xBF58476D1CE4E5B9 + 0x94D049BB133111EB
	x ^= x >> 30
	x *= 0xBF58476D1CE4E5B9
	x ^= x >> 27
	return byte(x)
}
