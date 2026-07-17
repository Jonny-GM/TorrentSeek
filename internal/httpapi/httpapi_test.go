package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jonny-gm/torrentseek/internal/backend"
	"github.com/jonny-gm/torrentseek/internal/backend/fake"
	"github.com/jonny-gm/torrentseek/internal/pieces"
	"github.com/jonny-gm/torrentseek/internal/scheduler"
)

// movieSpec matches the fake package's canonical layout: 16-byte pieces,
// sample.txt at [0,40), movie.mkv at [40,240), 15 pieces total.
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

type fixture struct {
	f      *fake.Fake
	srv    *httptest.Server
	id     backend.ID
	magnet string
}

func newFixture(t *testing.T, cfg Config) *fixture {
	t.Helper()
	return newFixtureSpec(t, cfg, movieSpec())
}

func newFixtureSpec(t *testing.T, cfg Config, spec fake.TorrentSpec) *fixture {
	t.Helper()
	f := fake.New()
	id, magnet := f.Register(spec)
	sched := scheduler.New(f, scheduler.Config{
		// Small windows so tests exercise sliding on a 15-piece torrent.
		NowWindowBytes:     32,
		BootstrapHeadBytes: 16,
		BootstrapTailBytes: 16,
		RepollInterval:     10 * time.Millisecond,
	})
	srv := httptest.NewServer(New(f, sched, cfg))
	t.Cleanup(srv.Close)
	t.Cleanup(sched.Close)
	t.Cleanup(func() { f.Close() })
	return &fixture{f: f, srv: srv, id: id, magnet: magnet}
}

func (fx *fixture) do(t *testing.T, method, path string, body io.Reader, into any) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, fx.srv.URL+path, body)
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if into != nil {
		if err := json.NewDecoder(resp.Body).Decode(into); err != nil {
			t.Fatalf("%s %s: decoding response: %v", method, path, err)
		}
	}
	return resp
}

func (fx *fixture) addMovie(t *testing.T) {
	t.Helper()
	var res struct {
		ID       string `json:"id"`
		Existing bool   `json:"existing"`
	}
	body := strings.NewReader(fmt.Sprintf(`{"magnet":%q}`, fx.magnet))
	resp := fx.do(t, "POST", "/v1/torrents/add", body, &res)
	if resp.StatusCode != http.StatusOK || res.ID != string(fx.id) || res.Existing {
		t.Fatalf("add: status=%d res=%+v", resp.StatusCode, res)
	}
}

func errCode(t *testing.T, resp *http.Response, body map[string]map[string]string) string {
	t.Helper()
	return body["error"]["code"]
}

func TestAddListGetDelete(t *testing.T) {
	fx := newFixture(t, Config{})
	fx.addMovie(t)

	// Re-add is idempotent.
	var again struct {
		ID       string `json:"id"`
		Existing bool   `json:"existing"`
	}
	body := strings.NewReader(fmt.Sprintf(`{"magnet":%q}`, fx.magnet))
	if resp := fx.do(t, "POST", "/v1/torrents/add", body, &again); resp.StatusCode != http.StatusOK || !again.Existing {
		t.Fatalf("re-add: status=%d existing=%v", resp.StatusCode, again.Existing)
	}

	var list []map[string]any
	if resp := fx.do(t, "GET", "/v1/torrents", nil, &list); resp.StatusCode != http.StatusOK || len(list) != 1 {
		t.Fatalf("list: status=%d len=%d", resp.StatusCode, len(list))
	}
	if list[0]["id"] != string(fx.id) || list[0]["state"] != "downloading" {
		t.Errorf("list[0] = %v", list[0])
	}

	var detail map[string]any
	if resp := fx.do(t, "GET", "/v1/torrents/"+string(fx.id), nil, &detail); resp.StatusCode != http.StatusOK {
		t.Fatalf("get: status=%d", resp.StatusCode)
	}
	if detail["piece_size"] != float64(16) || detail["piece_count"] != float64(15) || detail["total_size"] != float64(240) {
		t.Errorf("detail = %v", detail)
	}

	if resp := fx.do(t, "DELETE", "/v1/torrents/"+string(fx.id), nil, nil); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("delete: status=%d", resp.StatusCode)
	}
	var errBody map[string]map[string]string
	resp := fx.do(t, "GET", "/v1/torrents/"+string(fx.id), nil, &errBody)
	if resp.StatusCode != http.StatusNotFound || errCode(t, resp, errBody) != "torrent_not_found" {
		t.Errorf("get after delete: status=%d body=%v", resp.StatusCode, errBody)
	}
}

func TestAddViaMultipart(t *testing.T) {
	fx := newFixture(t, Config{})

	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	fw, err := mw.CreateFormFile("torrent", "movie.torrent")
	if err != nil {
		t.Fatal(err)
	}
	fw.Write(fx.f.MetainfoFor(fx.id))
	mw.Close()

	req, _ := http.NewRequest("POST", fx.srv.URL+"/v1/torrents/add", &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var res struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || res.ID != string(fx.id) {
		t.Fatalf("multipart add: status=%d id=%s", resp.StatusCode, res.ID)
	}
}

func TestAddErrors(t *testing.T) {
	fx := newFixture(t, Config{})
	cases := []struct {
		name, body string
		wantStatus int
		wantCode   string
	}{
		{"empty body", ``, http.StatusBadRequest, "bad_request"},
		{"no magnet", `{}`, http.StatusBadRequest, "bad_request"},
		{"garbage magnet", `{"magnet":"nope"}`, http.StatusBadRequest, "invalid_id"},
		{"unknown torrent", `{"magnet":"magnet:?xt=urn:btih:00000000000000000000ffffffffffffffffffff"}`, http.StatusNotFound, "torrent_not_found"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var errBody map[string]map[string]string
			resp := fx.do(t, "POST", "/v1/torrents/add", strings.NewReader(tc.body), &errBody)
			if resp.StatusCode != tc.wantStatus || errCode(t, resp, errBody) != tc.wantCode {
				t.Errorf("status=%d code=%q, want %d %q", resp.StatusCode, errCode(t, resp, errBody), tc.wantStatus, tc.wantCode)
			}
		})
	}
}

func TestFilesReportsAvailability(t *testing.T) {
	fx := newFixture(t, Config{})
	fx.addMovie(t)

	// Piece 2 spans the file boundary: bytes [32,48) = 8 of each file.
	fx.f.CompleteRange(fx.id, pieces.Range{Begin: 2, End: 3})

	var files []map[string]any
	if resp := fx.do(t, "GET", "/v1/torrents/"+string(fx.id)+"/files", nil, &files); resp.StatusCode != http.StatusOK {
		t.Fatalf("files: status=%d", resp.StatusCode)
	}
	if len(files) != 2 {
		t.Fatalf("got %d files", len(files))
	}
	if files[0]["length"] != float64(40) || files[0]["bytes_available"] != float64(8) {
		t.Errorf("file 0 = %v, want 8 of 40 bytes available", files[0])
	}
	if files[1]["length"] != float64(200) || files[1]["bytes_available"] != float64(8) {
		t.Errorf("file 1 = %v, want 8 of 200 bytes available", files[1])
	}
	if files[1]["file_index"] != float64(1) {
		t.Errorf("file 1 index = %v", files[1]["file_index"])
	}
}

// waitPrioritized polls the fake's most recent Prioritize set until it
// matches want — the scheduler delivers flushes asynchronously
// (scheduler.deliverFlushes), so tests can't read it synchronously.
func waitPrioritized(t *testing.T, f *fake.Fake, id backend.ID, want []backend.PriorityRange) {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		got := f.PrioritizedRanges(id)
		if len(got) == len(want) {
			match := true
			for i := range want {
				if got[i] != want[i] {
					match = false
					break
				}
			}
			if match {
				return
			}
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for prioritize; last = %v, want %v", f.PrioritizedRanges(id), want)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestPrepare(t *testing.T) {
	fx := newFixture(t, Config{})
	fx.addMovie(t)

	// Prepare movie.mkv bytes [10,26) → torrent bytes [50,66) → pieces [3,5).
	var res struct {
		Ready          bool  `json:"ready"`
		BytesAvailable int64 `json:"bytes_available"`
	}
	body := strings.NewReader(`{"offset":10,"length":16}`)
	resp := fx.do(t, "POST", "/v1/torrents/"+string(fx.id)+"/files/1/prepare", body, &res)
	if resp.StatusCode != http.StatusAccepted || res.Ready || res.BytesAvailable != 0 {
		t.Fatalf("prepare: status=%d res=%+v", resp.StatusCode, res)
	}
	waitPrioritized(t, fx.f, fx.id, []backend.PriorityRange{{Range: pieces.Range{Begin: 3, End: 5}}})

	// The swarm delivers; prepare now reports ready.
	fx.f.ServePrioritized(fx.id, 2)
	body = strings.NewReader(`{"offset":10,"length":16}`)
	resp = fx.do(t, "POST", "/v1/torrents/"+string(fx.id)+"/files/1/prepare", body, &res)
	if resp.StatusCode != http.StatusAccepted || !res.Ready || res.BytesAvailable != 16 {
		t.Fatalf("prepare after serve: status=%d res=%+v", resp.StatusCode, res)
	}

	// No body means the whole file: pieces [2,15).
	resp = fx.do(t, "POST", "/v1/torrents/"+string(fx.id)+"/files/1/prepare", nil, &res)
	if resp.StatusCode != http.StatusAccepted || res.Ready {
		t.Fatalf("whole-file prepare: status=%d res=%+v", resp.StatusCode, res)
	}
	waitPrioritized(t, fx.f, fx.id, []backend.PriorityRange{{Range: pieces.Range{Begin: 2, End: 15}}})
}

func TestPrepareValidation(t *testing.T) {
	fx := newFixture(t, Config{})
	fx.addMovie(t)

	cases := []struct {
		name, path, body string
		wantStatus       int
		wantCode         string
	}{
		{"bad file index", "/files/9/prepare", ``, http.StatusNotFound, "file_not_found"},
		{"negative index", "/files/-1/prepare", ``, http.StatusBadRequest, "bad_request"},
		{"offset past EOF", "/files/1/prepare", `{"offset":500}`, http.StatusBadRequest, "bad_request"},
		{"range past EOF", "/files/1/prepare", `{"offset":190,"length":20}`, http.StatusBadRequest, "bad_request"},
		{"negative offset", "/files/1/prepare", `{"offset":-1,"length":5}`, http.StatusBadRequest, "bad_request"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var body io.Reader
			if tc.body != "" {
				body = strings.NewReader(tc.body)
			}
			var errBody map[string]map[string]string
			resp := fx.do(t, "POST", "/v1/torrents/"+string(fx.id)+tc.path, body, &errBody)
			if resp.StatusCode != tc.wantStatus || errCode(t, resp, errBody) != tc.wantCode {
				t.Errorf("status=%d code=%q, want %d %q", resp.StatusCode, errCode(t, resp, errBody), tc.wantStatus, tc.wantCode)
			}
		})
	}
}

func TestMetadataPendingMapsTo409(t *testing.T) {
	spec := movieSpec()
	spec.MetadataPending = true
	fx := newFixtureSpec(t, Config{}, spec)
	id := fx.id
	fx.addMovie(t)

	var errBody map[string]map[string]string
	resp := fx.do(t, "GET", "/v1/torrents/"+string(id)+"/files", nil, &errBody)
	if resp.StatusCode != http.StatusConflict || errCode(t, resp, errBody) != "metadata_pending" {
		t.Errorf("files while pending: status=%d body=%v", resp.StatusCode, errBody)
	}

	var detail map[string]any
	if resp := fx.do(t, "GET", "/v1/torrents/"+string(id), nil, &detail); resp.StatusCode != http.StatusOK || detail["state"] != "metadata_pending" {
		t.Errorf("get while pending: status=%d state=%v", resp.StatusCode, detail["state"])
	}
}

func TestInvalidIDRejected(t *testing.T) {
	fx := newFixture(t, Config{})
	var errBody map[string]map[string]string
	resp := fx.do(t, "GET", "/v1/torrents/not-an-id", nil, &errBody)
	if resp.StatusCode != http.StatusBadRequest || errCode(t, resp, errBody) != "invalid_id" {
		t.Errorf("status=%d body=%v", resp.StatusCode, errBody)
	}
}

func TestAuth(t *testing.T) {
	fx := newFixture(t, Config{Token: "s3cret"})

	get := func(path string, decorate func(*http.Request)) int {
		req, _ := http.NewRequest("GET", fx.srv.URL+path, nil)
		if decorate != nil {
			decorate(req)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return resp.StatusCode
	}

	if got := get("/v1/torrents", nil); got != http.StatusUnauthorized {
		t.Errorf("no token: %d, want 401", got)
	}
	if got := get("/v1/torrents", func(r *http.Request) {
		r.Header.Set("Authorization", "Bearer wrong")
	}); got != http.StatusUnauthorized {
		t.Errorf("wrong token: %d, want 401", got)
	}
	if got := get("/v1/torrents", func(r *http.Request) {
		r.Header.Set("Authorization", "Bearer s3cret")
	}); got != http.StatusOK {
		t.Errorf("bearer token: %d, want 200", got)
	}
	// Query param works for players that can't set headers.
	if got := get("/v1/torrents?token=s3cret", nil); got != http.StatusOK {
		t.Errorf("query token: %d, want 200", got)
	}

	// Without a configured token, everything is open (loopback default).
	open := newFixture(t, Config{})
	req, _ := http.NewRequest("GET", open.srv.URL+"/v1/torrents", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("open server: %d, want 200", resp.StatusCode)
	}
}

// unavailableBackend simulates a torrent client that can't be reached:
// every operation fails with ErrBackendUnavailable, as the deluge
// backend does while disconnected.
type unavailableBackend struct{ *fake.Fake }

func (u unavailableBackend) List(context.Context) ([]backend.TorrentInfo, error) {
	return nil, backend.ErrBackendUnavailable
}

func (u unavailableBackend) Get(context.Context, backend.ID) (backend.TorrentInfo, error) {
	return backend.TorrentInfo{}, backend.ErrBackendUnavailable
}

func (u unavailableBackend) Add(context.Context, backend.AddRequest) (backend.AddResult, error) {
	return backend.AddResult{}, backend.ErrBackendUnavailable
}

// TestBackendUnavailableMapsTo503: an unreachable torrent client is a
// transient condition the daemon reports, not one it dies from — the
// API stays up and answers with a stable backend_unavailable code.
func TestBackendUnavailableMapsTo503(t *testing.T) {
	f := fake.New()
	u := unavailableBackend{Fake: f}
	sched := scheduler.New(u, scheduler.Config{})
	srv := httptest.NewServer(New(u, sched, Config{}))
	t.Cleanup(srv.Close)
	t.Cleanup(sched.Close)
	t.Cleanup(func() { f.Close() })

	for _, path := range []string{"/v1/torrents", "/v1/torrents/btih:" + strings.Repeat("a", 40)} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		var body struct {
			Error struct{ Code, Message string }
		}
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusServiceUnavailable || body.Error.Code != "backend_unavailable" {
			t.Errorf("%s: status=%d code=%q, want 503 backend_unavailable", path, resp.StatusCode, body.Error.Code)
		}
	}
}
