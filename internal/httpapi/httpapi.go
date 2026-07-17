// Package httpapi implements the /v1 HTTP API from docs/spec/02-http-api.md
// over a backend.Backend: torrent management and prepare. The /stream
// endpoint arrives with the streaming engine (03-streaming.md).
package httpapi

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jonny-gm/torrentseek/internal/backend"
	"github.com/jonny-gm/torrentseek/internal/pieces"
	"github.com/jonny-gm/torrentseek/internal/scheduler"
)

// maxAddBody bounds add-request bodies; .torrent files are small (a very
// large one is a few MB of piece hashes).
const maxAddBody = 16 << 20

// Config configures the API handler.
type Config struct {
	// Token, when non-empty, is required on every request as
	// "Authorization: Bearer <token>" or "?token=<token>".
	Token string
	// ReadTimeout is the maximum mid-body stall while waiting for pieces
	// before a stream is severed (default 60 s, per 03-streaming.md).
	ReadTimeout time.Duration
	// ChunkBytes is the streamer's read/write granularity (default 256 KiB).
	ChunkBytes int64
	Log        *slog.Logger
}

// New returns the /v1 API handler over b. Streams and prepares are
// scheduled through sched, which must be built over the same backend.
func New(b backend.Backend, sched *scheduler.Scheduler, cfg Config) http.Handler {
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	if cfg.ReadTimeout <= 0 {
		cfg.ReadTimeout = 60 * time.Second
	}
	if cfg.ChunkBytes <= 0 {
		cfg.ChunkBytes = 256 << 10
	}
	s := &server{
		b:           b,
		sched:       sched,
		token:       cfg.Token,
		readTimeout: cfg.ReadTimeout,
		chunkBytes:  cfg.ChunkBytes,
		log:         cfg.Log,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/torrents/add", s.handleAdd)
	mux.HandleFunc("GET /v1/torrents", s.handleList)
	mux.HandleFunc("GET /v1/torrents/{id}", s.handleGet)
	mux.HandleFunc("DELETE /v1/torrents/{id}", s.handleDelete)
	mux.HandleFunc("GET /v1/torrents/{id}/files", s.handleFiles)
	mux.HandleFunc("POST /v1/torrents/{id}/files/{file_index}/prepare", s.handlePrepare)
	mux.HandleFunc("GET /v1/stream/{id}/{file_index}", s.handleStream)
	mux.HandleFunc("GET /v1/play", s.handlePlay)
	return s.requireAuth(mux)
}

type server struct {
	b           backend.Backend
	sched       *scheduler.Scheduler
	token       string
	readTimeout time.Duration
	chunkBytes  int64
	log         *slog.Logger
}

// --- wire types (snake_case per spec) ---

type torrentJSON struct {
	ID        string  `json:"id"`
	Name      string  `json:"name"`
	TotalSize int64   `json:"total_size"`
	Progress  float64 `json:"progress"`
	State     string  `json:"state"`
}

type torrentDetailJSON struct {
	torrentJSON
	PieceSize  int64 `json:"piece_size"`
	PieceCount int   `json:"piece_count"`
	// Observability: current download speed (bytes/s) and connected peers,
	// so "why is my stream slow" is answerable from this API alone.
	RateDownload int64 `json:"rate_download"`
	Peers        int   `json:"peers"`
}

type fileJSON struct {
	FileIndex      int    `json:"file_index"`
	Path           string `json:"path"`
	Length         int64  `json:"length"`
	BytesAvailable int64  `json:"bytes_available"`
}

type addRequestJSON struct {
	Magnet string `json:"magnet"`
}

type addResponseJSON struct {
	ID       string `json:"id"`
	Existing bool   `json:"existing"`
}

type prepareRequestJSON struct {
	Offset int64  `json:"offset"`
	Length *int64 `json:"length"` // nil means "to end of file"
}

type prepareResponseJSON struct {
	Ready bool `json:"ready"`
	// BytesAvailable counts completed bytes within the requested range.
	BytesAvailable int64 `json:"bytes_available"`
}

// --- handlers ---

func (s *server) handleAdd(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxAddBody)
	req, err := parseAddRequest(r)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	res, err := s.b.Add(r.Context(), req)
	if err != nil {
		s.writeBackendError(w, err)
		return
	}
	s.writeJSON(w, http.StatusOK, addResponseJSON{ID: string(res.ID), Existing: res.Existing})
}

func parseAddRequest(r *http.Request) (backend.AddRequest, error) {
	ct, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
	switch {
	case ct == "multipart/form-data":
		file, _, err := r.FormFile("torrent")
		if err != nil {
			return backend.AddRequest{}, fmt.Errorf(`multipart add requires a "torrent" file field`)
		}
		defer file.Close()
		metainfo, err := io.ReadAll(file)
		if err != nil {
			return backend.AddRequest{}, fmt.Errorf("reading torrent upload: %w", err)
		}
		return backend.AddRequest{Metainfo: metainfo}, nil
	default:
		var body addRequestJSON
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			return backend.AddRequest{}, fmt.Errorf("invalid JSON body: %v", err)
		}
		if body.Magnet == "" {
			return backend.AddRequest{}, fmt.Errorf(`"magnet" is required (or upload a .torrent as multipart)`)
		}
		return backend.AddRequest{Magnet: body.Magnet}, nil
	}
}

func (s *server) handleList(w http.ResponseWriter, r *http.Request) {
	infos, err := s.b.List(r.Context())
	if err != nil {
		s.writeBackendError(w, err)
		return
	}
	out := make([]torrentJSON, 0, len(infos))
	for _, info := range infos {
		out = append(out, toTorrentJSON(info))
	}
	s.writeJSON(w, http.StatusOK, out)
}

func (s *server) handleGet(w http.ResponseWriter, r *http.Request) {
	id, ok := s.pathID(w, r)
	if !ok {
		return
	}
	info, err := s.b.Get(r.Context(), id)
	if err != nil {
		s.writeBackendError(w, err)
		return
	}
	s.writeJSON(w, http.StatusOK, torrentDetailJSON{
		torrentJSON:  toTorrentJSON(info),
		PieceSize:    info.PieceSize,
		PieceCount:   info.PieceCount,
		RateDownload: info.RateDownload,
		Peers:        info.Peers,
	})
}

func (s *server) handleDelete(w http.ResponseWriter, r *http.Request) {
	id, ok := s.pathID(w, r)
	if !ok {
		return
	}
	deleteData := r.URL.Query().Get("delete_data") == "true"
	if err := s.b.Remove(r.Context(), id, deleteData); err != nil {
		s.writeBackendError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) handleFiles(w http.ResponseWriter, r *http.Request) {
	id, ok := s.pathID(w, r)
	if !ok {
		return
	}
	files, info, state, err := s.fileState(r, id)
	if err != nil {
		s.writeBackendError(w, err)
		return
	}
	out := make([]fileJSON, 0, len(files))
	for _, f := range files {
		out = append(out, fileJSON{
			FileIndex:      f.Index,
			Path:           f.Path,
			Length:         f.Length,
			BytesAvailable: pieces.AvailableBytes(state, f.Offset, info.PieceSize, f.Length),
		})
	}
	s.writeJSON(w, http.StatusOK, out)
}

func (s *server) handlePrepare(w http.ResponseWriter, r *http.Request) {
	id, ok := s.pathID(w, r)
	if !ok {
		return
	}
	fileIndex, err := strconv.Atoi(r.PathValue("file_index"))
	if err != nil || fileIndex < 0 {
		s.writeError(w, http.StatusBadRequest, "bad_request", "file_index must be a non-negative integer")
		return
	}

	// Body is optional; absent or empty means "the whole file".
	req := prepareRequestJSON{}
	if r.ContentLength != 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
			s.writeError(w, http.StatusBadRequest, "bad_request", "invalid JSON body: "+err.Error())
			return
		}
	}
	length := int64(-1) // scheduler treats negative as "to end of file"
	if req.Length != nil {
		length = *req.Length
		if length < 0 {
			s.writeError(w, http.StatusBadRequest, "bad_request", "length must be non-negative")
			return
		}
	}

	ready, avail, err := s.sched.Prepare(r.Context(), id, fileIndex, req.Offset, length)
	if err != nil {
		s.writeBackendError(w, err)
		return
	}
	s.writeJSON(w, http.StatusAccepted, prepareResponseJSON{Ready: ready, BytesAvailable: avail})
}

// fileState gathers the per-torrent context most endpoints need: file
// layout, torrent info, and a piece-state snapshot.
func (s *server) fileState(r *http.Request, id backend.ID) ([]backend.FileInfo, backend.TorrentInfo, pieces.Bitfield, error) {
	files, err := s.b.Files(r.Context(), id)
	if err != nil {
		return nil, backend.TorrentInfo{}, pieces.Bitfield{}, err
	}
	info, err := s.b.Get(r.Context(), id)
	if err != nil {
		return nil, backend.TorrentInfo{}, pieces.Bitfield{}, err
	}
	state, err := s.b.PieceState(r.Context(), id)
	if err != nil {
		return nil, backend.TorrentInfo{}, pieces.Bitfield{}, err
	}
	return files, info, state, nil
}

// --- plumbing ---

func toTorrentJSON(info backend.TorrentInfo) torrentJSON {
	return torrentJSON{
		ID:        string(info.ID),
		Name:      info.Name,
		TotalSize: info.TotalSize,
		Progress:  info.Progress,
		State:     string(info.State),
	}
}

func (s *server) pathID(w http.ResponseWriter, r *http.Request) (backend.ID, bool) {
	id, err := backend.ParseID(r.PathValue("id"))
	if err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid_id", `torrent ids look like "btih:<40 hex chars>"`)
		return "", false
	}
	return id, true
}

func (s *server) requireAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.token != "" {
			presented := r.URL.Query().Get("token")
			if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
				presented = strings.TrimPrefix(h, "Bearer ")
			}
			if subtle.ConstantTimeCompare([]byte(presented), []byte(s.token)) != 1 {
				s.writeError(w, http.StatusUnauthorized, "unauthorized", "missing or invalid API token")
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// writeBackendError maps backend sentinel errors onto API statuses and
// stable error codes.
func (s *server) writeBackendError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, backend.ErrTorrentNotFound):
		s.writeError(w, http.StatusNotFound, "torrent_not_found", "no such torrent")
	case errors.Is(err, backend.ErrFileNotFound):
		s.writeError(w, http.StatusNotFound, "file_not_found", "no such file in torrent")
	case errors.Is(err, backend.ErrInvalidID):
		s.writeError(w, http.StatusBadRequest, "invalid_id", err.Error())
	case errors.Is(err, backend.ErrOutOfRange):
		s.writeError(w, http.StatusBadRequest, "bad_request", "byte range outside file")
	case errors.Is(err, backend.ErrMetadataPending):
		s.writeError(w, http.StatusConflict, "metadata_pending", "torrent metadata is still resolving; retry shortly")
	case errors.Is(err, backend.ErrBackendUnavailable):
		s.writeError(w, http.StatusServiceUnavailable, "backend_unavailable",
			"the torrent client is unreachable (is deluged running?); TorrentSeek keeps retrying and recovers on its own once it is back")
	default:
		s.log.Error("backend error", "err", err)
		s.writeError(w, http.StatusInternalServerError, "internal", "internal error")
	}
}

func (s *server) writeError(w http.ResponseWriter, status int, code, msg string) {
	type errBody struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	s.writeJSON(w, status, map[string]errBody{"error": {Code: code, Message: msg}})
}

func (s *server) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		s.log.Error("encoding response", "err", err)
	}
}
