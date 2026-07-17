package deluge

// Unit tests for the backend's pure parsing/diff logic — status dicts as
// the daemon actually shapes them (verified against a live deluged; see
// live_test.go for the end-to-end path).

import (
	"reflect"
	"testing"
	"time"

	"github.com/jonny-gm/torrentseek/internal/backend"
	"github.com/jonny-gm/torrentseek/internal/pieces"
)

const testID = backend.ID("btih:bd899eea84440a6d204c8eeb0278d176598a1121")

// downloadingStatus mirrors a real get_torrent_status result for a
// part-complete torrent (field shapes captured from a live daemon).
func downloadingStatus() map[any]any {
	return map[any]any{
		"hash":              "bd899eea84440a6d204c8eeb0278d176598a1121",
		"name":              "testfile.bin",
		"total_size":        int64(2097152),
		"num_pieces":        int64(8),
		"piece_length":      int64(262144),
		"progress":          45.3125,
		"state":             "Downloading",
		"download_location": "/downloads",
		"files": []any{
			map[any]any{"index": int64(0), "path": "testfile.bin", "size": int64(2097152), "offset": int64(0)},
		},
		"pieces":                []any{int64(0), int64(1), int64(2), int64(3), int64(3), int64(1), int64(0), int64(3)},
		"download_payload_rate": int64(1234),
		"num_peers":             int64(2),
		"num_seeds":             int64(3),
	}
}

func TestParseStatusDownloading(t *testing.T) {
	ts := parseStatus(testID, downloadingStatus())

	if ts.info.Name != "testfile.bin" || ts.info.TotalSize != 2097152 {
		t.Errorf("info = %+v", ts.info)
	}
	if ts.info.State != backend.StateDownloading {
		t.Errorf("state = %v", ts.info.State)
	}
	if ts.info.Progress < 0.45 || ts.info.Progress > 0.46 {
		t.Errorf("progress = %v, want ~0.453 (Deluge reports 0-100)", ts.info.Progress)
	}
	if ts.info.RateDownload != 1234 || ts.info.Peers != 5 { // peers + seeds
		t.Errorf("observability pair = %d, %d", ts.info.RateDownload, ts.info.Peers)
	}
	if ts.pending {
		t.Error("pending = true for resolved torrent")
	}
	if len(ts.files) != 1 || ts.files[0].Length != 2097152 || ts.files[0].Offset != 0 {
		t.Errorf("files = %+v", ts.files)
	}
	for i, want := range []bool{false, false, false, true, true, false, false, true} {
		if ts.have.Has(i) != want {
			t.Errorf("piece %d have = %v, want %v", i, ts.have.Has(i), want)
		}
	}
}

func TestParseStatusSwarmStates(t *testing.T) {
	// Deluge's pieces field is availability, not just completion:
	// 0 = no connected peer has it, 1 = available from a peer,
	// 2 = downloading, 3 = have. parseStatus keeps the raw states for
	// the SwarmObserver capability alongside the collapsed bitfield.
	ts := parseStatus(testID, downloadingStatus())
	want := []backend.PieceSwarmState{
		backend.PieceSwarmUnavailable, backend.PieceSwarmAvailable,
		backend.PieceSwarmDownloading, backend.PieceSwarmHave,
		backend.PieceSwarmHave, backend.PieceSwarmAvailable,
		backend.PieceSwarmUnavailable, backend.PieceSwarmHave,
	}
	if !reflect.DeepEqual(ts.swarm, want) {
		t.Errorf("swarm = %v, want %v", ts.swarm, want)
	}

	seeding := downloadingStatus()
	seeding["state"] = "Seeding"
	seeding["progress"] = float64(100)
	seeding["pieces"] = nil
	ts = parseStatus(testID, seeding)
	for i, st := range ts.swarm {
		if st != backend.PieceSwarmHave {
			t.Fatalf("seeding piece %d swarm = %v, want have", i, st)
		}
	}
}

func TestParseStatusSeedingNilPieces(t *testing.T) {
	st := downloadingStatus()
	st["state"] = "Seeding"
	st["progress"] = float64(100)
	st["pieces"] = nil // Deluge stops tracking per-piece state once seeding

	ts := parseStatus(testID, st)

	if ts.info.State != backend.StateSeeding {
		t.Errorf("state = %v", ts.info.State)
	}
	if got := ts.have.Count(); got != 8 {
		t.Errorf("have.Count() = %d, want all 8 for a seeding torrent", got)
	}
}

func TestParseStatusMetadataPending(t *testing.T) {
	st := downloadingStatus()
	st["files"] = []any{}
	st["num_pieces"] = int64(0)
	st["pieces"] = nil
	st["progress"] = float64(0)

	ts := parseStatus(testID, st)

	if !ts.pending || ts.info.State != backend.StateMetadataPending {
		t.Errorf("pending = %v, state = %v", ts.pending, ts.info.State)
	}
	if ts.have.Count() != 0 {
		t.Error("pending torrent must have no complete pieces")
	}
}

func TestDesiredDeadlinesTiersStaggerAndOverlap(t *testing.T) {
	got := desiredDeadlines([]backend.PriorityRange{
		{Range: pieces.Range{Begin: 0, End: 3}, Rank: 0}, // 500 + 100/piece
		// Rank-1 tier (2000 + 100/piece); its piece 2 (2000) loses to the
		// rank-0 range's staggered 700 — overlap keeps the tightest.
		{Range: pieces.Range{Begin: 2, End: 5}, Rank: 1},
		{Range: pieces.Range{Begin: 90, End: 92}, Rank: 2}, // 3500 + 100/piece
	}, 100)

	want := map[int]int64{
		0: 500, 1: 600, 2: 700,
		3: 2100, 4: 2200,
		90: 3500, 91: 3600,
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("desiredDeadlines = %v, want %v", got, want)
	}
}

func TestDesiredDeadlinesCapAndClamp(t *testing.T) {
	// Rank 30 would be 45500ms uncapped; a long rank-0 range staggers past
	// the cap too; and ranges beyond pieceCount clamp rather than produce
	// out-of-range pieces.
	desired := make([]backend.PriorityRange, 31)
	for i := range desired {
		desired[i] = backend.PriorityRange{Range: pieces.Range{Begin: i, End: i + 1}, Rank: i}
	}
	desired[30] = backend.PriorityRange{Range: pieces.Range{Begin: 98, End: 105}, Rank: 30}

	got := desiredDeadlines(desired, 100)

	if got[98] != deadlineMaxMS {
		t.Errorf("rank-30 deadline = %d, want capped %d", got[98], deadlineMaxMS)
	}
	for p := range got {
		if p < 0 || p >= 100 {
			t.Errorf("out-of-range piece %d in desired set", p)
		}
	}

	// A rank-0 range long enough that intra-range stagger alone crosses
	// the cap: at 100ms/piece the cap is reached at piece (30000-500)/100.
	got = desiredDeadlines([]backend.PriorityRange{
		{Range: pieces.Range{Begin: 0, End: 400}, Rank: 0},
	}, 400)
	if got[294] != deadlineMaxMS-deadlineIntraStepMS {
		t.Errorf("piece 294 deadline = %d, want %d", got[294], deadlineMaxMS-deadlineIntraStepMS)
	}
	if got[295] != deadlineMaxMS || got[399] != deadlineMaxMS {
		t.Errorf("staggered deadlines = %d/%d at pieces 295/399, want capped %d",
			got[295], got[399], deadlineMaxMS)
	}
}

func TestWantedFiles(t *testing.T) {
	files := []backend.FileInfo{
		{Index: 0, Offset: 0, Length: 1000},    // pieces 0-0
		{Index: 1, Offset: 1000, Length: 3000}, // pieces 0-3 (1000..4000, 1KiB pieces)
		{Index: 2, Offset: 4000, Length: 2048}, // pieces 3-5
	}
	got := wantedFiles([]backend.PriorityRange{{Range: pieces.Range{Begin: 4, End: 6}}}, files, 1024)
	if !reflect.DeepEqual(got, []int{2}) {
		t.Errorf("wantedFiles = %v, want [2]", got)
	}
	got = wantedFiles([]backend.PriorityRange{{Range: pieces.Range{Begin: 0, End: 1}}}, files, 1024)
	if !reflect.DeepEqual(got, []int{0, 1}) {
		t.Errorf("wantedFiles = %v, want [0 1] (piece 0 spans both)", got)
	}
	if got = wantedFiles(nil, files, 1024); got != nil {
		t.Errorf("wantedFiles(no ranges) = %v, want none", got)
	}
}

func TestAlreadyInSessionRe(t *testing.T) {
	// Message format from deluge/core/torrentmanager.py.
	for _, msg := range []string{
		"[Torrent already in session (bd899eea84440a6d204c8eeb0278d176598a1121).]",
		"[Torrent already being added (bd899eea84440a6d204c8eeb0278d176598a1121).]",
	} {
		m := alreadyInSessionRe.FindStringSubmatch(msg)
		if m == nil || m[1] != "bd899eea84440a6d204c8eeb0278d176598a1121" {
			t.Errorf("regex failed on %q: %v", msg, m)
		}
	}
}

func TestEscalateRescueLadder(t *testing.T) {
	b := &Backend{rescues: make(map[rescueKey]rescueTrack)}

	// Repeats for the same stuck piece climb the ladder and stay at the
	// top; a different piece is its own independent ladder.
	want := []int64{rescueMinSpeedFirst, rescueMinSpeedSecond, rescueMinSpeedAll, rescueMinSpeedAll}
	for i, w := range want {
		if got := b.escalateRescue(testID, 649); got != w {
			t.Fatalf("attempt %d: min_speed = %d, want %d", i+1, got, w)
		}
	}
	if got := b.escalateRescue(testID, 650); got != int64(rescueMinSpeedFirst) {
		t.Fatalf("other piece: min_speed = %d, want %d (fresh ladder)", got, rescueMinSpeedFirst)
	}

	// An attempt past the escalation window is a new stall: the ladder
	// restarts (and the stale entry is pruned, not accumulated).
	k := rescueKey{id: testID, piece: 649}
	tr := b.rescues[k]
	tr.last = tr.last.Add(-rescueEscalationWindow - time.Second)
	b.rescues[k] = tr
	if got := b.escalateRescue(testID, 649); got != int64(rescueMinSpeedFirst) {
		t.Fatalf("after window: min_speed = %d, want %d (fresh ladder)", got, rescueMinSpeedFirst)
	}
	if b.rescues[k].attempts != 1 {
		t.Fatalf("after window: attempts = %d, want 1", b.rescues[k].attempts)
	}
}
