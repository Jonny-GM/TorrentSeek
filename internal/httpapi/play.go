package httpapi

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/jonny-gm/torrentseek/internal/backend"
)

// videoExts are the extensions handlePlay prefers when picking the file to
// stream (02-http-api.md "Convenience: play").
var videoExts = map[string]bool{
	".mkv": true, ".mp4": true, ".avi": true, ".webm": true, ".mov": true,
	".m4v": true, ".ts": true, ".mpg": true, ".mpeg": true, ".wmv": true,
	".flv": true,
}

// metadataPollInterval is how often play re-checks a magnet whose metadata
// is still resolving.
const metadataPollInterval = 250 * time.Millisecond

// handlePlay turns a magnet into a playing stream: add (idempotent), wait
// for metadata, pick the largest video file, redirect to its stream URL.
func (s *server) handlePlay(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	magnet, idParam := q.Get("magnet"), q.Get("id")

	var id backend.ID
	switch {
	case magnet != "" && idParam == "":
		res, err := s.b.Add(r.Context(), backend.AddRequest{Magnet: magnet})
		if err != nil {
			s.writeBackendError(w, err)
			return
		}
		id = res.ID
	case idParam != "" && magnet == "":
		parsed, err := backend.ParseID(idParam)
		if err != nil {
			s.writeError(w, http.StatusBadRequest, "invalid_id", `torrent ids look like "btih:<40 hex chars>"`)
			return
		}
		id = parsed
	default:
		s.writeError(w, http.StatusBadRequest, "bad_request", `exactly one of "magnet" or "id" is required`)
		return
	}

	// Magnets resolve metadata from the swarm; poll under the same budget
	// streams get for stalled reads.
	deadline := time.Now().Add(s.readTimeout)
	var files []backend.FileInfo
	for {
		var err error
		files, err = s.b.Files(r.Context(), id)
		if err == nil {
			break
		}
		if !errors.Is(err, backend.ErrMetadataPending) {
			s.writeBackendError(w, err)
			return
		}
		if time.Now().After(deadline) {
			s.writeError(w, http.StatusGatewayTimeout, "metadata_timeout",
				fmt.Sprintf("torrent metadata did not resolve within %s; the swarm may be dead — retry, or check /v1/torrents/%s", s.readTimeout, id))
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-time.After(metadataPollInterval):
		}
	}
	if len(files) == 0 {
		s.writeError(w, http.StatusUnprocessableEntity, "no_files", "torrent contains no files")
		return
	}

	pick, pickVideo := files[0], backend.FileInfo{Length: -1}
	for _, f := range files {
		if f.Length > pick.Length {
			pick = f
		}
		if videoExts[strings.ToLower(path.Ext(f.Path))] && f.Length > pickVideo.Length {
			pickVideo = f
		}
	}
	if pickVideo.Length >= 0 {
		pick = pickVideo
	}

	target := fmt.Sprintf("/v1/stream/%s/%d", id, pick.Index)
	// Players can't attach auth headers while following redirects, so carry
	// the query token through.
	if token := q.Get("token"); token != "" {
		target += "?token=" + url.QueryEscape(token)
	}
	http.Redirect(w, r, target, http.StatusFound)
}
