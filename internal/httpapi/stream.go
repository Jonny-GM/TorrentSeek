package httpapi

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/jonny-gm/torrentseek/internal/backend"
	"github.com/jonny-gm/torrentseek/internal/scheduler"
)

// byteRange is a resolved request range: [start, start+length) of a file.
type byteRange struct {
	start, length int64
}

// parseRange resolves a Range header against a file of the given length,
// per 03-streaming.md: single ranges only; multi-range and malformed
// headers are ignored (returning the full file with 200, as RFC 9110
// permits); syntactically valid but unsatisfiable ranges yield
// unsatisfiable=true (416).
func parseRange(header string, fileLength int64) (r byteRange, partial, unsatisfiable bool) {
	full := byteRange{0, fileLength}
	spec, ok := strings.CutPrefix(header, "bytes=")
	if header == "" || !ok || strings.Contains(spec, ",") {
		return full, false, false
	}
	first, last, ok := strings.Cut(strings.TrimSpace(spec), "-")
	if !ok {
		return full, false, false
	}

	if first == "" { // suffix form: bytes=-N, the last N bytes
		n, err := strconv.ParseInt(last, 10, 64)
		if err != nil {
			return full, false, false
		}
		if n <= 0 {
			return byteRange{}, false, true
		}
		n = min(n, fileLength)
		return byteRange{fileLength - n, n}, true, false
	}

	start, err := strconv.ParseInt(first, 10, 64)
	if err != nil || start < 0 {
		return full, false, false
	}
	if start >= fileLength {
		return byteRange{}, false, true
	}
	end := fileLength - 1 // open-ended: bytes=N-
	if last != "" {
		end, err = strconv.ParseInt(last, 10, 64)
		if err != nil || end < start {
			return full, false, false
		}
		end = min(end, fileLength-1)
	}
	return byteRange{start, end - start + 1}, true, false
}

// heartbeatInterval bounds how long a stall runs before its cause (swarm
// throughput, peer count, overall progress) is logged. A stall shorter than
// this never logs a heartbeat — only the final "stream stalled" summary.
const heartbeatInterval = 10 * time.Second

// waitWithHeartbeat wraps Stream.WaitBytes with the same total deadline
// (s.readTimeout) but logs the torrent's swarm state partway through a long
// stall, so "why is this stuck" is answerable from the log alone rather
// than inferred after the fact: e.g. rate_download=0 peers=0 means no
// swarm; nonzero rate with the wait still unmet means this specific piece
// is unavailable from the peers currently delivering other data.
func (s *server) waitWithHeartbeat(ctx context.Context, st *scheduler.Stream, id backend.ID, fileIndex int, off, n int64) error {
	deadline := time.Now().Add(s.readTimeout)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return context.DeadlineExceeded
		}
		step := min(remaining, heartbeatInterval)
		stepCtx, cancel := context.WithTimeout(ctx, step)
		err := st.WaitBytes(stepCtx, off, n)
		cancel()
		if err == nil || ctx.Err() != nil || !errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		if info, ierr := s.b.Get(ctx, id); ierr == nil {
			args := []any{"torrent", id, "file", fileIndex, "offset", off,
				"progress", fmt.Sprintf("%.3f", info.Progress), "state", info.State,
				"rate_download", info.RateDownload, "peers", info.Peers}
			// The blocked piece's swarm availability is the heart of the
			// diagnosis: "unavailable" means no connected peer has it
			// (only new peers can help), "available"/"downloading" means
			// the swarm has it and the stall is scheduling or peer
			// throughput. The piece named is the first *missing* piece of
			// the awaited range — a read straddling a piece boundary
			// waits on the far piece, and naming the (complete) piece at
			// the read offset instead turns the log into false
			// reassurance (observed in the field: every boundary-
			// straddling stall reported "have"). Best-effort — backends
			// without the capability just omit it.
			if piece, state, ok := s.blockedPieceSwarmState(ctx, id, fileIndex, off, n, info.PieceSize); ok {
				args = append(args, "piece", piece, "piece_swarm", state)
			}
			s.log.Info("stream still waiting", args...)
		}
	}
}

// blockedPieceSwarmState resolves the first missing piece backing bytes
// [off, off+n) of the file and its swarm availability, when the backend
// can report it. If every backing piece is complete (the wait resolved
// racily), the last one is reported.
func (s *server) blockedPieceSwarmState(ctx context.Context, id backend.ID, fileIndex int, off, n, pieceSize int64) (int, backend.PieceSwarmState, bool) {
	so, ok := s.b.(backend.SwarmObserver)
	if !ok || pieceSize <= 0 || n <= 0 {
		return 0, 0, false
	}
	files, err := s.b.Files(ctx, id)
	if err != nil || fileIndex < 0 || fileIndex >= len(files) {
		return 0, 0, false
	}
	states, err := so.PieceSwarmStates(ctx, id)
	if err != nil {
		return 0, 0, false
	}
	first := int((files[fileIndex].Offset + off) / pieceSize)
	last := int((files[fileIndex].Offset + off + n - 1) / pieceSize)
	if first < 0 || first >= len(states) {
		return 0, 0, false
	}
	last = min(last, len(states)-1)
	for p := first; p <= last; p++ {
		if states[p] != backend.PieceSwarmHave {
			return p, states[p], true
		}
	}
	return last, states[last], true
}

func (s *server) handleStream(w http.ResponseWriter, r *http.Request) {
	id, ok := s.pathID(w, r)
	if !ok {
		return
	}
	fileIndex, err := strconv.Atoi(r.PathValue("file_index"))
	if err != nil || fileIndex < 0 {
		s.writeError(w, http.StatusBadRequest, "bad_request", "file_index must be a non-negative integer")
		return
	}
	files, err := s.b.Files(r.Context(), id)
	if err != nil {
		s.writeBackendError(w, err)
		return
	}
	if fileIndex >= len(files) {
		s.writeBackendError(w, backend.ErrFileNotFound)
		return
	}
	file := files[fileIndex]

	ctype := mime.TypeByExtension(path.Ext(file.Path))
	if ctype == "" {
		ctype = "application/octet-stream"
	}
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Type", ctype)

	rng, partial, unsatisfiable := parseRange(r.Header.Get("Range"), file.Length)
	if unsatisfiable {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", file.Length))
		s.writeError(w, http.StatusRequestedRangeNotSatisfiable, "range_not_satisfiable",
			fmt.Sprintf("file is %d bytes", file.Length))
		return
	}
	w.Header().Set("Content-Length", strconv.FormatInt(rng.length, 10))
	status := http.StatusOK
	if partial {
		status = http.StatusPartialContent
		w.Header().Set("Content-Range",
			fmt.Sprintf("bytes %d-%d/%d", rng.start, rng.start+rng.length-1, file.Length))
	}

	// HEAD probes size/type without triggering downloads or priorities.
	if r.Method == http.MethodHead {
		w.WriteHeader(status)
		return
	}

	st, err := s.sched.OpenStream(r.Context(), id, fileIndex, rng.start)
	if err != nil {
		s.writeBackendError(w, err)
		return
	}
	defer st.Close()

	// Headers never depend on downloaded bytes: send them now (flushed past
	// the server's buffering), stall mid-body if pieces are missing.
	rc := http.NewResponseController(w)
	w.WriteHeader(status)
	rc.Flush()

	start := time.Now()
	var stalled time.Duration
	stalls := 0
	buf := make([]byte, s.chunkBytes)
	cursor, end := rng.start, rng.start+rng.length
	s.log.Debug("stream open", "torrent", id, "file", fileIndex, "offset", rng.start, "length", rng.length)
	defer func() {
		elapsed := time.Since(start)
		sent := cursor - rng.start
		rate := float64(sent) / max(elapsed.Seconds(), 0.001) / (1 << 20)
		s.log.Info("stream closed", "torrent", id, "file", fileIndex,
			"sent", sent, "elapsed", elapsed.Round(time.Millisecond),
			"mibps", fmt.Sprintf("%.1f", rate),
			"stalls", stalls, "stalled", stalled.Round(time.Millisecond))
	}()

	for cursor < end {
		n := min(int64(len(buf)), end-cursor)

		waitStart := time.Now()
		err := s.waitWithHeartbeat(r.Context(), st, id, fileIndex, cursor, n)
		if d := time.Since(waitStart); d > 100*time.Millisecond {
			stalls++
			stalled += d
			s.log.Debug("stream stalled on pieces", "torrent", id, "file", fileIndex, "offset", cursor, "waited", d.Round(time.Millisecond))
		}
		if err != nil {
			// The swarm didn't deliver in time, the torrent was removed, or
			// the client left: fail visibly by severing the connection
			// mid-body rather than hanging or serving zeros.
			s.log.Warn("stream aborted", "torrent", id, "file", fileIndex, "offset", cursor, "err", err)
			panic(http.ErrAbortHandler)
		}

		read, err := s.b.ReadAt(r.Context(), id, fileIndex, buf[:n], cursor)
		if err != nil && !(errors.Is(err, io.EOF) && read > 0) {
			s.log.Warn("stream read failed", "torrent", id, "file", fileIndex, "offset", cursor, "err", err)
			panic(http.ErrAbortHandler)
		}
		if _, err := w.Write(buf[:read]); err != nil {
			return // client hung up; defer closes the window
		}
		rc.Flush()

		cursor += int64(read)
		st.Advance(cursor)
	}
}
