package httpapi

import (
	"bytes"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/jonny-gm/torrentseek/internal/pieces"
)

func TestParseRange(t *testing.T) {
	const fileLen = 200
	tests := []struct {
		name, header      string
		want              byteRange
		partial, unsatisf bool
	}{
		{"no header", "", byteRange{0, 200}, false, false},
		{"closed", "bytes=10-25", byteRange{10, 16}, true, false},
		{"open ended", "bytes=190-", byteRange{190, 10}, true, false},
		{"suffix", "bytes=-16", byteRange{184, 16}, true, false},
		{"suffix larger than file", "bytes=-500", byteRange{0, 200}, true, false},
		{"end clamped", "bytes=190-999", byteRange{190, 10}, true, false},
		{"single byte", "bytes=0-0", byteRange{0, 1}, true, false},
		{"last byte", "bytes=199-199", byteRange{199, 1}, true, false},
		{"start at eof", "bytes=200-", byteRange{}, false, true},
		{"start past eof", "bytes=500-900", byteRange{}, false, true},
		{"empty suffix", "bytes=-0", byteRange{}, false, true},
		{"multi-range ignored", "bytes=0-1,5-9", byteRange{0, 200}, false, false},
		{"garbage ignored", "bytes=abc-def", byteRange{0, 200}, false, false},
		{"inverted ignored", "bytes=50-10", byteRange{0, 200}, false, false},
		{"wrong unit ignored", "pieces=0-1", byteRange{0, 200}, false, false},
		{"negative start ignored", "bytes=-5-10", byteRange{0, 200}, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, partial, unsatisf := parseRange(tt.header, fileLen)
			if r != tt.want || partial != tt.partial || unsatisf != tt.unsatisf {
				t.Errorf("parseRange(%q) = %+v/%v/%v, want %+v/%v/%v",
					tt.header, r, partial, unsatisf, tt.want, tt.partial, tt.unsatisf)
			}
		})
	}
}

// streamCfg keeps chunks smaller than a piece so the chunk loop, cursor
// advancing, and window sliding all get exercised on the tiny test torrent.
func streamCfg() Config {
	return Config{ChunkBytes: 8, ReadTimeout: 2 * time.Second}
}

// feed keeps delivering prioritized pieces (urgent-first) until stop is
// closed, simulating a swarm that honors the scheduler's priorities.
func (fx *fixture) feed(t *testing.T) (stop func()) {
	t.Helper()
	quit := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-quit:
				return
			default:
				if fx.f.ServePrioritized(fx.id, 1) == 0 {
					time.Sleep(time.Millisecond)
				}
			}
		}
	}()
	return func() { close(quit); <-done }
}

func (fx *fixture) get(t *testing.T, path string, header http.Header) *http.Response {
	t.Helper()
	req, err := http.NewRequest("GET", fx.srv.URL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	for k, vs := range header {
		req.Header[k] = vs
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestStreamFullFile(t *testing.T) {
	fx := newFixture(t, streamCfg())
	fx.addMovie(t)
	fx.f.CompleteRange(fx.id, pieces.Range{Begin: 0, End: 15})

	resp := fx.get(t, "/v1/stream/"+string(fx.id)+"/1", nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if resp.Header.Get("Accept-Ranges") != "bytes" || resp.ContentLength != 200 {
		t.Errorf("headers: Accept-Ranges=%q Content-Length=%d", resp.Header.Get("Accept-Ranges"), resp.ContentLength)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if want := fx.f.FileContent(fx.id, 1, 0, 200); !bytes.Equal(body, want) {
		t.Error("body differs from file content")
	}
}

func TestStreamRangeRequests(t *testing.T) {
	fx := newFixture(t, streamCfg())
	fx.addMovie(t)
	fx.f.CompleteRange(fx.id, pieces.Range{Begin: 0, End: 15})

	cases := []struct {
		name, rangeHeader, wantContentRange string
		wantStatus                          int
		wantOff, wantLen                    int64
	}{
		{"closed range", "bytes=10-25", "bytes 10-25/200", 206, 10, 16},
		{"open ended", "bytes=190-", "bytes 190-199/200", 206, 190, 10},
		{"suffix", "bytes=-16", "bytes 184-199/200", 206, 184, 16},
		{"multi-range ignored", "bytes=0-1,5-9", "", 200, 0, 200},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := fx.get(t, "/v1/stream/"+string(fx.id)+"/1", http.Header{"Range": {tc.rangeHeader}})
			defer resp.Body.Close()
			if resp.StatusCode != tc.wantStatus {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tc.wantStatus)
			}
			if got := resp.Header.Get("Content-Range"); got != tc.wantContentRange {
				t.Errorf("Content-Range = %q, want %q", got, tc.wantContentRange)
			}
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			if want := fx.f.FileContent(fx.id, 1, tc.wantOff, tc.wantLen); !bytes.Equal(body, want) {
				t.Errorf("body (%d bytes) differs from file[%d:%d]", len(body), tc.wantOff, tc.wantOff+tc.wantLen)
			}
		})
	}
}

func TestStreamUnsatisfiableRange(t *testing.T) {
	fx := newFixture(t, streamCfg())
	fx.addMovie(t)

	resp := fx.get(t, "/v1/stream/"+string(fx.id)+"/1", http.Header{"Range": {"bytes=200-"}})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("status = %d, want 416", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Range"); got != "bytes */200" {
		t.Errorf("Content-Range = %q, want \"bytes */200\"", got)
	}
}

func TestStreamBlocksUntilSwarmDelivers(t *testing.T) {
	fx := newFixture(t, streamCfg())
	fx.addMovie(t)
	stop := fx.feed(t)
	defer stop()

	// Nothing is downloaded when the request starts; the feeder delivers
	// pieces only as the scheduler prioritizes them.
	resp := fx.get(t, "/v1/stream/"+string(fx.id)+"/1", http.Header{"Range": {"bytes=100-"}})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if want := fx.f.FileContent(fx.id, 1, 100, 100); !bytes.Equal(body, want) {
		t.Error("body differs from file content")
	}
}

func TestStreamSeekReprioritizes(t *testing.T) {
	fx := newFixture(t, streamCfg())
	fx.addMovie(t)

	// A mid-file request with nothing downloaded: the scheduler must ask
	// for the cursor's pieces first (plus bootstraps), not the file start.
	resp := fx.get(t, "/v1/stream/"+string(fx.id)+"/1", http.Header{"Range": {"bytes=100-"}})
	defer resp.Body.Close()

	deadline := time.After(2 * time.Second)
	for {
		got := fx.f.PrioritizedRanges(fx.id)
		// Cursor at file byte 100 → torrent bytes [140,172) → pieces [8,11).
		if len(got) > 0 && got[0].Range == (pieces.Range{Begin: 8, End: 11}) {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("prioritized = %v, want cursor window [8,11) first", got)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestStreamHEADTriggersNothing(t *testing.T) {
	fx := newFixture(t, streamCfg())
	fx.addMovie(t)

	req, _ := http.NewRequest("HEAD", fx.srv.URL+"/v1/stream/"+string(fx.id)+"/1", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.ContentLength != 200 {
		t.Errorf("HEAD: status=%d Content-Length=%d", resp.StatusCode, resp.ContentLength)
	}
	if got := fx.f.PrioritizedRanges(fx.id); len(got) != 0 {
		t.Errorf("HEAD prioritized %v, want nothing", got)
	}
}

func TestStreamTimeoutSeversConnection(t *testing.T) {
	cfg := streamCfg()
	cfg.ReadTimeout = 50 * time.Millisecond
	fx := newFixture(t, cfg)
	fx.addMovie(t)

	// Dead swarm: nothing ever arrives. Headers come immediately, then the
	// body must fail rather than hang.
	resp := fx.get(t, "/v1/stream/"+string(fx.id)+"/1", nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if _, err := io.ReadAll(resp.Body); err == nil {
		t.Fatal("body read succeeded on a dead swarm; want severed connection")
	}
}

func TestDeleteAbortsOpenStream(t *testing.T) {
	fx := newFixture(t, streamCfg())
	fx.addMovie(t)

	resp := fx.get(t, "/v1/stream/"+string(fx.id)+"/1", nil)
	defer resp.Body.Close()

	// Wait for the stream to open (its window shows up), then delete.
	deadline := time.After(2 * time.Second)
	for len(fx.f.PrioritizedRanges(fx.id)) == 0 {
		select {
		case <-deadline:
			t.Fatal("stream window never appeared")
		case <-time.After(5 * time.Millisecond):
		}
	}
	if del := fx.do(t, "DELETE", "/v1/torrents/"+string(fx.id), nil, nil); del.StatusCode != http.StatusNoContent {
		t.Fatalf("delete: %d", del.StatusCode)
	}

	readDone := make(chan error, 1)
	go func() {
		_, err := io.ReadAll(resp.Body)
		readDone <- err
	}()
	select {
	case err := <-readDone:
		if err == nil {
			t.Fatal("stream completed after torrent deletion; want severed connection")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("stream not aborted after torrent deletion")
	}
}
