package fake

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/jonny-gm/torrentseek/internal/backend"
	"github.com/jonny-gm/torrentseek/internal/pieces"
)

var ctx = context.Background()

// movieSpec: two files over 16-byte pieces; file 1 starts mid-piece.
//
//	sample.txt  40 bytes  → torrent offset 0,  pieces 0..2 (piece 2 shared)
//	movie.mkv  200 bytes  → torrent offset 40, pieces 2..15
func movieSpec() TorrentSpec {
	return TorrentSpec{
		Name:      "Some.Movie.2024",
		PieceSize: 16,
		Files: []FileSpec{
			{Path: "Some.Movie.2024/sample.txt", Length: 40},
			{Path: "Some.Movie.2024/movie.mkv", Length: 200},
		},
		Seed: 42,
	}
}

func addMovie(t *testing.T, f *Fake) backend.ID {
	t.Helper()
	_, magnet := f.Register(movieSpec())
	res, err := f.Add(ctx, backend.AddRequest{Magnet: magnet})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	if res.Existing {
		t.Fatal("first Add reported Existing")
	}
	return res.ID
}

func TestAddIsIdempotent(t *testing.T) {
	f := New()
	defer f.Close()
	id := addMovie(t, f)

	_, magnet := f.Register(movieSpec()) // same spec, same identity
	again, err := f.Add(ctx, backend.AddRequest{Magnet: magnet})
	if err != nil {
		t.Fatalf("re-Add: %v", err)
	}
	if !again.Existing || again.ID != id {
		t.Errorf("re-Add = %+v, want Existing=true ID=%s", again, id)
	}

	viaMetainfo, err := f.Add(ctx, backend.AddRequest{Metainfo: f.MetainfoFor(id)})
	if err != nil {
		t.Fatalf("metainfo Add: %v", err)
	}
	if !viaMetainfo.Existing || viaMetainfo.ID != id {
		t.Errorf("metainfo Add = %+v, want Existing=true ID=%s", viaMetainfo, id)
	}
}

func TestAddRejectsBadInput(t *testing.T) {
	f := New()
	defer f.Close()
	if _, err := f.Add(ctx, backend.AddRequest{}); err == nil {
		t.Error("empty request should error")
	}
	if _, err := f.Add(ctx, backend.AddRequest{Magnet: "not a magnet"}); !errors.Is(err, backend.ErrInvalidID) {
		t.Errorf("garbage magnet: err = %v, want ErrInvalidID", err)
	}
	unknown := "magnet:?xt=urn:btih:00000000000000000000ffffffffffffffffffff"
	if _, err := f.Add(ctx, backend.AddRequest{Magnet: unknown}); !errors.Is(err, backend.ErrTorrentNotFound) {
		t.Errorf("unregistered magnet: err = %v, want ErrTorrentNotFound", err)
	}
}

func TestFilesLayoutAndInfo(t *testing.T) {
	f := New()
	defer f.Close()
	id := addMovie(t, f)

	info, err := f.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	// 240 payload bytes over 16-byte pieces = 15 pieces.
	if info.TotalSize != 240 || info.PieceCount != 15 || info.State != backend.StateDownloading || info.Progress != 0 {
		t.Errorf("info = %+v", info)
	}

	files, err := f.Files(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 {
		t.Fatalf("got %d files", len(files))
	}
	if files[0].Offset != 0 || files[0].Length != 40 || files[1].Offset != 40 || files[1].Length != 200 {
		t.Errorf("file layout wrong: %+v", files)
	}
}

func TestMetadataPendingLifecycle(t *testing.T) {
	f := New()
	defer f.Close()
	spec := movieSpec()
	spec.MetadataPending = true
	_, magnet := f.Register(spec)
	res, err := f.Add(ctx, backend.AddRequest{Magnet: magnet})
	if err != nil {
		t.Fatal(err)
	}

	if info, _ := f.Get(ctx, res.ID); info.State != backend.StateMetadataPending {
		t.Errorf("state = %s, want metadata_pending", info.State)
	}
	if _, err := f.Files(ctx, res.ID); !errors.Is(err, backend.ErrMetadataPending) {
		t.Errorf("Files err = %v, want ErrMetadataPending", err)
	}
	if _, err := f.PieceState(ctx, res.ID); !errors.Is(err, backend.ErrMetadataPending) {
		t.Errorf("PieceState err = %v, want ErrMetadataPending", err)
	}

	f.ResolveMetadata(res.ID)
	if info, _ := f.Get(ctx, res.ID); info.State != backend.StateDownloading {
		t.Errorf("state after resolve = %s, want downloading", info.State)
	}
	if _, err := f.Files(ctx, res.ID); err != nil {
		t.Errorf("Files after resolve: %v", err)
	}
}

func TestReadAtRefusesIncompleteThenServes(t *testing.T) {
	f := New()
	defer f.Close()
	id := addMovie(t, f)

	// movie.mkv bytes [10,26) sit at torrent offset [50,66) → pieces 3..5.
	buf := make([]byte, 16)
	if _, err := f.ReadAt(ctx, id, 1, buf, 10); !errors.Is(err, backend.ErrIncomplete) {
		t.Fatalf("read before completion: err = %v, want ErrIncomplete", err)
	}

	f.CompleteRange(id, pieces.Range{Begin: 3, End: 5})
	n, err := f.ReadAt(ctx, id, 1, buf, 10)
	if err != nil || n != 16 {
		t.Fatalf("read after completion: n=%d err=%v", n, err)
	}
	if want := f.FileContent(id, 1, 10, 16); !bytes.Equal(buf, want) {
		t.Errorf("read bytes differ from expected content")
	}
}

func TestReadAtBoundaries(t *testing.T) {
	f := New()
	defer f.Close()
	id := addMovie(t, f)
	f.CompleteRange(id, pieces.Range{Begin: 0, End: 15})

	// Read straddling EOF of file 0 (40 bytes long).
	buf := make([]byte, 16)
	n, err := f.ReadAt(ctx, id, 0, buf, 32)
	if n != 8 || err != io.EOF {
		t.Errorf("EOF read: n=%d err=%v, want 8, io.EOF", n, err)
	}
	if want := f.FileContent(id, 0, 32, 8); !bytes.Equal(buf[:8], want) {
		t.Error("EOF read bytes differ from expected content")
	}

	if _, err := f.ReadAt(ctx, id, 0, buf, 41); !errors.Is(err, backend.ErrOutOfRange) {
		t.Errorf("past-EOF offset: err = %v, want ErrOutOfRange", err)
	}
	if _, err := f.ReadAt(ctx, id, 0, buf, -1); !errors.Is(err, backend.ErrOutOfRange) {
		t.Errorf("negative offset: err = %v, want ErrOutOfRange", err)
	}
	if _, err := f.ReadAt(ctx, id, 5, buf, 0); !errors.Is(err, backend.ErrFileNotFound) {
		t.Errorf("bad file index: err = %v, want ErrFileNotFound", err)
	}
}

func TestFileContentIsDistinctAcrossFilesAndSeeds(t *testing.T) {
	f := New()
	defer f.Close()
	id := addMovie(t, f)
	a := f.FileContent(id, 0, 0, 32)
	b := f.FileContent(id, 1, 0, 32)
	if bytes.Equal(a, b) {
		t.Error("different files should have different content")
	}

	other := movieSpec()
	other.Name = "Other.Movie"
	other.Seed = 7
	oid, _ := f.Register(other)
	if bytes.Equal(a, f.FileContent(oid, 0, 0, 32)) {
		t.Error("different seeds should give different content")
	}
}

func TestPrioritizeAndServePrioritized(t *testing.T) {
	f := New()
	defer f.Close()
	id := addMovie(t, f)

	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(f.Prioritize(ctx, id, []backend.PriorityRange{{Range: pieces.Range{Begin: 8, End: 10}}, {Range: pieces.Range{Begin: 0, End: 2}, Rank: 1}}))
	// A later call replaces the earlier one (coalescing semantics).
	must(f.Prioritize(ctx, id, []backend.PriorityRange{{Range: pieces.Range{Begin: 8, End: 10}}, {Range: pieces.Range{Begin: 14, End: 15}, Rank: 1}}))

	got := f.PrioritizedRanges(id)
	want := []backend.PriorityRange{{Range: pieces.Range{Begin: 8, End: 10}}, {Range: pieces.Range{Begin: 14, End: 15}, Rank: 1}}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("PrioritizedRanges = %v, want %v", got, want)
	}

	// The swarm delivers urgent-first: pieces 8, 9, then 14.
	if n := f.ServePrioritized(id, 2); n != 2 {
		t.Fatalf("ServePrioritized(2) = %d", n)
	}
	state, _ := f.PieceState(ctx, id)
	if !state.HasRange(pieces.Range{Begin: 8, End: 10}) || state.Has(14) {
		t.Error("first serve should complete [8,10) only")
	}
	if n := f.ServePrioritized(id, 5); n != 1 {
		t.Errorf("second serve = %d, want 1 (only piece 14 left prioritized)", n)
	}
	if n := f.ServePrioritized(id, 5); n != 0 {
		t.Errorf("exhausted serve = %d, want 0", n)
	}
}

func TestEventsAndRemove(t *testing.T) {
	f := New()
	defer f.Close()
	id := addMovie(t, f)

	f.CompleteRange(id, pieces.Range{Begin: 2, End: 4})
	if err := f.Remove(ctx, id, false); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Get(ctx, id); !errors.Is(err, backend.ErrTorrentNotFound) {
		t.Errorf("Get after remove: %v, want ErrTorrentNotFound", err)
	}

	var got []backend.Event
	f.Close()
	for e := range f.Events() {
		got = append(got, e)
	}
	want := []backend.Event{
		{Type: backend.EventTorrentAdded, Torrent: id},
		{Type: backend.EventPieceCompleted, Torrent: id, Piece: 2},
		{Type: backend.EventPieceCompleted, Torrent: id, Piece: 3},
		{Type: backend.EventTorrentRemoved, Torrent: id},
	}
	if len(got) != len(want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("event[%d] = %v, want %v", i, got[i], want[i])
		}
	}
}

func TestSeedingStateAtFullCompletion(t *testing.T) {
	f := New()
	defer f.Close()
	id := addMovie(t, f)
	f.CompleteRange(id, pieces.Range{Begin: 0, End: 15})
	info, _ := f.Get(ctx, id)
	if info.State != backend.StateSeeding || info.Progress != 1 {
		t.Errorf("info = %+v, want seeding at progress 1", info)
	}
}

// Interface compliance.
var _ backend.Backend = (*Fake)(nil)
