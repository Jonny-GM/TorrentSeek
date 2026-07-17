package httpapi

import (
	"bytes"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/jonny-gm/torrentseek/internal/backend/fake"
	"github.com/jonny-gm/torrentseek/internal/pieces"
)

// noRedirect returns the raw 3xx response instead of following it.
var noRedirect = &http.Client{
	CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
}

func (fx *fixture) play(t *testing.T, query string) *http.Response {
	t.Helper()
	resp, err := noRedirect.Get(fx.srv.URL + "/v1/play?" + query)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func TestPlayRedirectsToLargestVideoFile(t *testing.T) {
	// notes.txt is the largest file overall; movie.mkv must win anyway.
	spec := fake.TorrentSpec{
		Name:      "Some.Movie.2024",
		PieceSize: 16,
		Files: []fake.FileSpec{
			{Path: "Some.Movie.2024/notes.txt", Length: 300},
			{Path: "Some.Movie.2024/movie.mkv", Length: 200},
		},
		Seed: 1,
	}
	fx := newFixtureSpec(t, streamCfg(), spec)

	resp := fx.play(t, "magnet="+url.QueryEscape(fx.magnet))
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("status = %d, want 302", resp.StatusCode)
	}
	want := "/v1/stream/" + string(fx.id) + "/1"
	if got := resp.Header.Get("Location"); got != want {
		t.Fatalf("Location = %q, want %q", got, want)
	}

	// The redirect target actually plays.
	fx.f.CompleteRange(fx.id, pieces.Range{Begin: 0, End: 32})
	full, err := http.Get(fx.srv.URL + resp.Header.Get("Location"))
	if err != nil {
		t.Fatal(err)
	}
	defer full.Body.Close()
	body, _ := io.ReadAll(full.Body)
	if !bytes.Equal(body, fx.f.FileContent(fx.id, 1, 0, 200)) {
		t.Error("streamed bytes differ from file content")
	}
}

func TestPlayFallsBackToLargestFile(t *testing.T) {
	spec := fake.TorrentSpec{
		Name:      "No.Video.Here",
		PieceSize: 16,
		Files: []fake.FileSpec{
			{Path: "No.Video.Here/small.dat", Length: 40},
			{Path: "No.Video.Here/big.dat", Length: 200},
		},
		Seed: 2,
	}
	fx := newFixtureSpec(t, streamCfg(), spec)
	resp := fx.play(t, "magnet="+url.QueryEscape(fx.magnet))
	if want := "/v1/stream/" + string(fx.id) + "/1"; resp.Header.Get("Location") != want {
		t.Errorf("Location = %q, want %q (largest file)", resp.Header.Get("Location"), want)
	}
}

func TestPlayByExistingID(t *testing.T) {
	fx := newFixture(t, streamCfg())
	fx.addMovie(t)
	resp := fx.play(t, "id="+string(fx.id))
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("status = %d, want 302", resp.StatusCode)
	}
}

func TestPlayWaitsForMetadata(t *testing.T) {
	spec := movieSpec()
	spec.MetadataPending = true
	fx := newFixtureSpec(t, streamCfg(), spec)

	go func() {
		time.Sleep(100 * time.Millisecond)
		fx.f.ResolveMetadata(fx.id)
	}()
	start := time.Now()
	resp := fx.play(t, "magnet="+url.QueryEscape(fx.magnet))
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("status = %d, want 302 after metadata resolves", resp.StatusCode)
	}
	if time.Since(start) > 3*time.Second {
		t.Error("play took implausibly long after metadata resolved")
	}
}

func TestPlayMetadataTimeout(t *testing.T) {
	cfg := streamCfg()
	cfg.ReadTimeout = 100 * time.Millisecond
	spec := movieSpec()
	spec.MetadataPending = true // never resolved
	fx := newFixtureSpec(t, cfg, spec)

	resp := fx.play(t, "magnet="+url.QueryEscape(fx.magnet))
	if resp.StatusCode != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504", resp.StatusCode)
	}
}

func TestPlayPropagatesToken(t *testing.T) {
	cfg := streamCfg()
	cfg.Token = "s3cret"
	fx := newFixtureSpec(t, cfg, movieSpec())

	resp := fx.play(t, "magnet="+url.QueryEscape(fx.magnet)+"&token=s3cret")
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("status = %d, want 302", resp.StatusCode)
	}
	loc, err := url.Parse(resp.Header.Get("Location"))
	if err != nil || loc.Query().Get("token") != "s3cret" {
		t.Errorf("Location %q should carry the token", resp.Header.Get("Location"))
	}
}

func TestPlayValidation(t *testing.T) {
	fx := newFixture(t, streamCfg())
	cases := []struct {
		name, query string
		wantStatus  int
	}{
		{"no params", "", http.StatusBadRequest},
		{"both params", "magnet=x&id=y", http.StatusBadRequest},
		{"bad id", "id=nope", http.StatusBadRequest},
		{"unknown magnet", "magnet=" + url.QueryEscape("magnet:?xt=urn:btih:00000000000000000000ffffffffffffffffffff"), http.StatusNotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if resp := fx.play(t, tc.query); resp.StatusCode != tc.wantStatus {
				t.Errorf("status = %d, want %d", resp.StatusCode, tc.wantStatus)
			}
		})
	}
}
