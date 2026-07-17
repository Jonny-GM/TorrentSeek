// Command torrentprobe is a scriptable, fully-logged stand-in for a media
// player, built to isolate TorrentSeek's serving correctness from
// player-side behavior (caching, reconnects, seek heuristics).
//
// It has two subcommands:
//
//	torrentprobe fetch  -magnet <magnet> -out capture.bin
//	torrentprobe verify -against capture.bin
//
// fetch reads from the /v1/stream endpoint the way a player does — a
// sequential GET, optionally with one deliberate simulated seek — and
// writes every byte it receives to a local capture file at the exact
// offset it was received, plus a manifest recording which byte ranges
// were captured. Every chunk is logged with a timestamp.
//
// verify re-reads those same byte ranges fresh from the server (run this
// once the torrent has settled, e.g. after "Force Re-check" in Deluge)
// and compares them byte-for-byte against the capture,
// reporting the exact offset of the first mismatch if any. If verify
// passes, TorrentSeek served correct bytes during the download and any
// corruption is downstream (player-side); if it fails, this pinpoints the
// exact wrong offset for a targeted fix.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "fetch":
		cmdFetch(os.Args[2:])
	case "verify":
		cmdVerify(os.Args[2:])
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "torrentprobe: unknown command %q\n\n", os.Args[1])
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `torrentprobe: isolate TorrentSeek serving correctness from player behavior.

Usage:
  torrentprobe fetch  -magnet <magnet> -out capture.bin [options]
  torrentprobe verify -against capture.bin [options]

Run "torrentprobe fetch -h" or "torrentprobe verify -h" for the full option list.`)
	os.Exit(2)
}

func logf(msg string, kv ...any) {
	ts := time.Now().Format("2006-01-02T15:04:05.000Z07:00")
	fmt.Fprintf(os.Stderr, "time=%s msg=%q", ts, msg)
	for i := 0; i+1 < len(kv); i += 2 {
		fmt.Fprintf(os.Stderr, " %v=%v", kv[i], kv[i+1])
	}
	fmt.Fprintln(os.Stderr)
}

func must(err error) {
	if err != nil {
		logf("fatal", "err", err)
		os.Exit(1)
	}
}

func fatal(format string, a ...any) {
	logf("fatal", "msg", fmt.Sprintf(format, a...))
	os.Exit(1)
}

// --- fetch ---

type segment struct{ Start, End int64 } // [Start, End) actually captured

func cmdFetch(args []string) {
	fs := flag.NewFlagSet("fetch", flag.ExitOnError)
	server := fs.String("server", "http://127.0.0.1:3480", "TorrentSeek base URL")
	token := fs.String("token", "", "API token, if the server requires one")
	magnet := fs.String("magnet", "", "magnet URI; resolved via /v1/play (add + wait for metadata + pick the largest video file), same as a player would")
	id := fs.String("id", "", "existing torrent id (btih:...), used with -file instead of -magnet")
	fileIndex := fs.Int("file", -1, "file index; only used with -id")
	out := fs.String("out", "", "output capture file (required)")
	start := fs.Int64("start", 0, "starting byte offset of the first segment")
	maxBytes := fs.Int64("max-bytes", 512<<20, "stop the first segment after this many bytes (0 = read to EOF)")
	seekAfter := fs.Int64("seek-after", 0, "if >0, stop the first segment after exactly this many bytes and open a second segment at -seek-to — a deliberate, logged, single seek under our control (mimics a player jumping to a new position)")
	seekTo := fs.Int64("seek-to", 0, "byte offset for the simulated seek; used with -seek-after")
	seekMaxBytes := fs.Int64("seek-max-bytes", 64<<20, "stop the post-seek segment after this many bytes (0 = read to EOF)")
	reqTimeout := fs.Duration("req-timeout", 5*time.Minute, "safety-net timeout per HTTP request; should exceed the server's -read-timeout so it never fires before the server's own timeout would")
	fs.Parse(args)

	if *out == "" {
		fatal("-out is required")
	}
	if *magnet == "" && *id == "" {
		fatal("need -magnet or -id")
	}

	streamPath, err := resolveStream(*server, *token, *magnet, *id, *fileIndex)
	must(err)
	logf("resolved stream", "path", streamPath)

	f, err := os.Create(*out)
	must(err)
	defer f.Close()

	seg1Cap := *maxBytes
	if *seekAfter > 0 {
		seg1Cap = *seekAfter
	}
	logf("segment 1: starting", "offset", *start, "cap_bytes", seg1Cap)
	seg1, err := fetchSegment(*server, *token, streamPath, f, *start, seg1Cap, *reqTimeout)
	must(err)
	segments := []segment{seg1}

	if *seekAfter > 0 {
		logf("simulated seek", "from_received", seg1.End-seg1.Start, "to_offset", *seekTo)
		seg2, err := fetchSegment(*server, *token, streamPath, f, *seekTo, *seekMaxBytes, *reqTimeout)
		must(err)
		segments = append(segments, seg2)
	}

	must(writeManifest(*out+".manifest", streamPath, segments))
	total := int64(0)
	for _, s := range segments {
		total += s.End - s.Start
	}
	logf("fetch complete", "segments", len(segments), "total_bytes", total, "manifest", *out+".manifest")
}

// resolveStream turns a magnet or an existing id into a /v1/stream path.
// For a magnet, it goes through /v1/play — the same add+wait+pick-file
// path a player hitting that URL takes — so the probe exercises exactly
// what a player exercises, not a hand-rolled shortcut.
func resolveStream(server, token, magnet, id string, fileIndex int) (string, error) {
	if magnet != "" {
		u := server + "/v1/play?magnet=" + url.QueryEscape(magnet)
		if token != "" {
			u += "&token=" + url.QueryEscape(token)
		}
		client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return "", err
		}
		resp, err := client.Do(req)
		if err != nil {
			return "", fmt.Errorf("play: %w", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusFound {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			return "", fmt.Errorf("play: status %d: %s", resp.StatusCode, body)
		}
		loc := resp.Header.Get("Location")
		if loc == "" {
			return "", fmt.Errorf("play: 302 with no Location header")
		}
		return loc, nil
	}
	if id == "" {
		return "", fmt.Errorf("need -magnet or -id")
	}
	if fileIndex < 0 {
		fileIndex = 0
	}
	return fmt.Sprintf("/v1/stream/%s/%d", id, fileIndex), nil
}

// fetchSegment issues one GET with Range: bytes=start-, writes every
// received byte to out at its true file offset, and logs progress. A
// server-side or network abort mid-read is logged and treated as the
// segment simply ending where it ended — not a fatal error — since the
// point is to capture exactly what arrived, abort or not.
func fetchSegment(server, token, streamPath string, out *os.File, start, capBytes int64, reqTimeout time.Duration) (segment, error) {
	req, err := http.NewRequest(http.MethodGet, server+streamPath, nil)
	if err != nil {
		return segment{}, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-", start))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	ctx, cancel := context.WithTimeout(context.Background(), reqTimeout)
	defer cancel()
	reqStart := time.Now()
	resp, err := http.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		return segment{}, fmt.Errorf("request at offset %d: %w", start, err)
	}
	defer resp.Body.Close()
	logf("segment opened", "offset", start, "status", resp.StatusCode, "content_length", resp.ContentLength)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return segment{}, fmt.Errorf("segment at offset %d: unexpected status %d: %s", start, resp.StatusCode, body)
	}

	buf := make([]byte, 256<<10)
	var written int64
	lastLog := time.Now()
	for capBytes <= 0 || written < capBytes {
		toRead := len(buf)
		if capBytes > 0 {
			if remaining := capBytes - written; remaining < int64(toRead) {
				toRead = int(remaining)
			}
		}
		n, rerr := resp.Body.Read(buf[:toRead])
		if n > 0 {
			if _, werr := out.WriteAt(buf[:n], start+written); werr != nil {
				return segment{start, start + written}, fmt.Errorf("writing capture file: %w", werr)
			}
			written += int64(n)
			if time.Since(lastLog) > time.Second {
				elapsed := time.Since(reqStart)
				mibps := float64(written) / elapsed.Seconds() / (1 << 20)
				logf("chunk", "offset", start, "received", written, "elapsed_ms", elapsed.Milliseconds(), "mibps", fmt.Sprintf("%.2f", mibps))
				lastLog = time.Now()
			}
		}
		if rerr != nil {
			if rerr != io.EOF {
				logf("segment ended early", "offset", start, "received", written, "err", rerr)
			}
			break
		}
	}
	logf("segment closed", "offset", start, "received", written)
	return segment{start, start + written}, nil
}

func writeManifest(path, streamPath string, segs []segment) error {
	var b strings.Builder
	b.WriteString(streamPath + "\n")
	for _, s := range segs {
		fmt.Fprintf(&b, "%d %d\n", s.Start, s.End)
	}
	return os.WriteFile(path, []byte(b.String()), 0o644)
}

func readManifest(path string) (string, []segment, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", nil, err
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) < 1 || lines[0] == "" {
		return "", nil, fmt.Errorf("empty manifest %s", path)
	}
	streamPath := lines[0]
	var segs []segment
	for _, l := range lines[1:] {
		var s, e int64
		if _, err := fmt.Sscanf(l, "%d %d", &s, &e); err != nil {
			return "", nil, fmt.Errorf("bad manifest line %q: %w", l, err)
		}
		segs = append(segs, segment{s, e})
	}
	return streamPath, segs, nil
}

// --- verify ---

func cmdVerify(args []string) {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	server := fs.String("server", "http://127.0.0.1:3480", "TorrentSeek base URL")
	token := fs.String("token", "", "API token, if the server requires one")
	against := fs.String("against", "", "capture file previously written by fetch (required; reads <against>.manifest alongside it)")
	fs.Parse(args)

	if *against == "" {
		fatal("-against is required")
	}
	streamPath, segments, err := readManifest(*against + ".manifest")
	must(err)
	capture, err := os.Open(*against)
	must(err)
	defer capture.Close()

	logf("verifying", "segments", len(segments), "stream", streamPath)
	mismatches := 0
	for i, seg := range segments {
		logf("verifying segment", "index", i, "start", seg.Start, "end", seg.End, "bytes", seg.End-seg.Start)
		if err := verifySegment(*server, *token, streamPath, capture, seg); err != nil {
			mismatches++
			logf("MISMATCH", "segment", i, "err", err)
		} else {
			logf("segment OK", "index", i)
		}
	}
	if mismatches == 0 {
		fmt.Println("VERIFY OK: every captured byte matches a fresh read from the server")
		return
	}
	fmt.Printf("VERIFY FAILED: %d of %d segment(s) mismatched\n", mismatches, len(segments))
	os.Exit(1)
}

// verifySegment re-reads seg fresh from the server and compares it
// byte-for-byte, in bounded-memory chunks, against the capture file.
func verifySegment(server, token, streamPath string, capture *os.File, seg segment) error {
	length := seg.End - seg.Start
	if length <= 0 {
		return nil
	}
	req, err := http.NewRequest(http.MethodGet, server+streamPath, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", seg.Start, seg.End-1))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	resp, err := http.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %d", resp.StatusCode)
	}

	fresh := bufio.NewReaderSize(resp.Body, 1<<20)
	old := bufio.NewReaderSize(io.NewSectionReader(capture, seg.Start, length), 1<<20)

	bufA := make([]byte, 64<<10)
	bufB := make([]byte, 64<<10)
	var pos int64
	for pos < length {
		want := int64(len(bufA))
		if remaining := length - pos; remaining < want {
			want = remaining
		}
		na, erra := io.ReadFull(fresh, bufA[:want])
		nb, errb := io.ReadFull(old, bufB[:want])
		if erra != nil && erra != io.EOF && erra != io.ErrUnexpectedEOF {
			return fmt.Errorf("reading fresh data at offset %d: %w", seg.Start+pos, erra)
		}
		if errb != nil && errb != io.EOF && errb != io.ErrUnexpectedEOF {
			return fmt.Errorf("reading captured data at offset %d: %w", seg.Start+pos, errb)
		}
		if na != nb {
			return fmt.Errorf("short read at offset %d: fresh=%d captured=%d bytes", seg.Start+pos, na, nb)
		}
		for i := 0; i < na; i++ {
			if bufA[i] != bufB[i] {
				lo, hi := max(0, i-8), min(na, i+8)
				return fmt.Errorf("byte mismatch at offset %d: fresh=% x captured=% x",
					seg.Start+pos+int64(i), bufA[lo:hi], bufB[lo:hi])
			}
		}
		pos += int64(na)
		if na == 0 {
			break
		}
	}
	return nil
}
