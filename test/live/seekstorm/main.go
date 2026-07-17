// Command seekstorm drives a player-like seek workload against a running
// torrentseek daemon and traces piece-level behavior while it runs, for
// the live harness (test/live/run.sh, seek-storm stage).
//
// It issues a scripted sequence of Range requests (each one a "seek"),
// measures time-to-first-byte and read throughput per seek, and verifies
// every byte against the source file. Concurrently it samples the
// leecher's piece bitfield straight from deluged, logging where the
// download frontier is relative to the read cursor — the trace that shows
// whether a blocked read is waiting on the piece under the cursor while
// the rest of the window fills (the starvation signature), without
// needing a GUI.
//
// The first seek is a warmup: it runs while the swarm is still
// bootstrapping (the harness connect-peer nudge lands mid-read), so it
// gets a loose TTFB bound. Every later seek is asserted strictly.
//
// -player switches to a player simulation instead of the seek list: a
// paced forward read (a video player draining at its bitrate), a
// concurrent tail reader issuing a fresh request every second (an MP4
// player pairing every playhead read with moov/subtitle-table reads at
// the file end — a real mpv session logged 595 tail opens out of 1200),
// and scheduled mid-playback seeks. Every read pause above a second is
// recorded as a rebuffer event; the run fails if any single stall
// exceeds -player-max-stall.
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jonny-gm/torrentseek/internal/backend"
	"github.com/jonny-gm/torrentseek/internal/backend/deluge"
	"github.com/jonny-gm/torrentseek/internal/units"
)

type options struct {
	api        string
	id         string
	fileIndex  int
	src        string
	delugeAddr string
	delugeUser string
	delugePass string
	seeks      string
	readLen    int64
	window     int64
	ttfbMax    time.Duration
	warmupTTFB time.Duration
	sample     time.Duration

	player         time.Duration
	playerStart    float64
	playerRate     int64
	playerSeeks    string
	playerMaxStall time.Duration
	tailEvery      time.Duration
}

func main() {
	var o options
	flag.StringVar(&o.api, "api", "http://127.0.0.1:3480", "torrentseek API base URL")
	flag.StringVar(&o.id, "id", "", "torrent id (btih:...)")
	flag.IntVar(&o.fileIndex, "file", 0, "file index to stream")
	flag.StringVar(&o.src, "src", "", "source file for byte verification")
	flag.StringVar(&o.delugeAddr, "deluge-addr", "", "leecher deluged RPC endpoint for piece-state sampling")
	flag.StringVar(&o.delugeUser, "deluge-user", "", "leecher daemon username")
	flag.StringVar(&o.delugePass, "deluge-pass", "", "leecher daemon password")
	flag.StringVar(&o.seeks, "seeks", "0,0.5,0.25,0.75,0.52,0.9", "comma-separated seek positions as fractions of file length")
	sizeFlag(&o.readLen, 8<<20, "read", "bytes to read per seek")
	sizeFlag(&o.window, 32<<20, "window", "now-window size the daemon runs with (trace annotation only)")
	flag.DurationVar(&o.ttfbMax, "ttfb-max", 15*time.Second, "max time-to-first-byte per seek (after warmup)")
	flag.DurationVar(&o.warmupTTFB, "warmup-ttfb", 90*time.Second, "max time-to-first-byte for the warmup seek")
	flag.DurationVar(&o.sample, "sample", time.Second, "piece-state sampling interval")
	flag.DurationVar(&o.player, "player", 0, "run a player simulation for this long instead of the seek list")
	flag.Float64Var(&o.playerStart, "player-start", 0.02, "player mode: starting position as a fraction of file length")
	sizeFlag(&o.playerRate, 1310720, "player-rate", "player mode: paced playback read rate per second (a video bitrate)")
	flag.StringVar(&o.playerSeeks, "player-seeks", "", "player mode: scheduled seeks as time:delta pairs, e.g. 30s:+0.10,55s:-0.05")
	flag.DurationVar(&o.playerMaxStall, "player-max-stall", 10*time.Second, "player mode: fail if any single rebuffer exceeds this")
	flag.DurationVar(&o.tailEvery, "tail-every", time.Second, "player mode: interval between tail-read requests (0 disables)")
	flag.Parse()

	if err := run(o); err != nil {
		fmt.Fprintln(os.Stderr, "seekstorm: FAIL:", err)
		os.Exit(1)
	}
}

type sizeValue struct{ p *int64 }

func (v sizeValue) String() string {
	if v.p == nil {
		return ""
	}
	return units.FormatBytes(*v.p)
}

func (v sizeValue) Set(s string) error {
	n, err := units.ParseBytes(s)
	if err != nil {
		return err
	}
	*v.p = n
	return nil
}

func sizeFlag(p *int64, def int64, name, usage string) {
	*p = def
	flag.Var(sizeValue{p}, name, usage)
}

func run(o options) error {
	id, err := backend.ParseID(o.id)
	if err != nil {
		return fmt.Errorf("-id: %w", err)
	}
	src, err := os.Open(o.src)
	if err != nil {
		return err
	}
	defer src.Close()

	// Read-only backend instance for observation: it never adds,
	// prioritizes, or pauses anything — PieceState/Get/Files round-trip to
	// deluged on demand, giving the trace a fresh piece bitfield per
	// sample without going through the daemon under test.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	obs, err := deluge.New(ctx, deluge.Config{
		Addr:     o.delugeAddr,
		Username: o.delugeUser,
		Password: o.delugePass,
		Log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	cancel()
	if err != nil {
		return fmt.Errorf("observer connect: %w", err)
	}
	defer obs.Close()

	info, err := obs.Get(context.Background(), id)
	if err != nil {
		return err
	}
	files, err := obs.Files(context.Background(), id)
	if err != nil {
		return err
	}
	if o.fileIndex < 0 || o.fileIndex >= len(files) {
		return fmt.Errorf("file index %d out of range", o.fileIndex)
	}
	file := files[o.fileIndex]
	fmt.Printf("seekstorm: %s file=%d len=%d pieces=%d piece_size=%d window=%s read/seek=%s\n",
		id, o.fileIndex, file.Length, info.PieceCount, info.PieceSize,
		units.FormatBytes(o.window), units.FormatBytes(o.readLen))

	// cursor is the absolute in-torrent byte offset the stream is
	// currently reading; the sampler reads it to anchor each trace line.
	var cursor atomic.Int64
	var seekNo atomic.Int64
	stopSampler := make(chan struct{})
	samplerDone := make(chan struct{})
	go func() {
		defer close(samplerDone)
		start := time.Now()
		tick := time.NewTicker(o.sample)
		defer tick.Stop()
		lastLogged := time.Time{}
		for {
			select {
			case <-stopSampler:
				return
			case <-tick.C:
			}
			// Swarm states, not just the bitfield: the trace answers
			// both "where is the frontier" and "could the swarm even
			// deliver the cursor piece" in one line.
			swarm, err := obs.PieceSwarmStates(context.Background(), id)
			if err != nil || len(swarm) != info.PieceCount {
				fmt.Printf("trace t=%+.1fs piece-state error: %v\n", time.Since(start).Seconds(), err)
				continue
			}
			hasP := func(p int) bool { return swarm[p] == backend.PieceSwarmHave }
			cur := cursor.Load()
			cp := int(cur / info.PieceSize)
			if cp >= info.PieceCount {
				cp = info.PieceCount - 1
			}
			firstMissing := -1
			for p := cp; p < info.PieceCount; p++ {
				if !hasP(p) {
					firstMissing = p
					break
				}
			}
			winPieces := int(o.window / info.PieceSize)
			winHave, total := 0, 0
			winEnd := min(cp+winPieces, info.PieceCount)
			for p := 0; p < info.PieceCount; p++ {
				if hasP(p) {
					total++
					if p >= cp && p < winEnd {
						winHave++
					}
				}
			}
			// The frontier piece missing is the interesting condition (a
			// reader would be blocked on it); log those samples always,
			// everything else at a low duty cycle to keep the trace short.
			blockedAtCursor := firstMissing == cp
			if !blockedAtCursor && time.Since(lastLogged) < 5*time.Second {
				continue
			}
			lastLogged = time.Now()
			fmt.Printf("trace t=%+7.1fs seek=%d cursor_piece=%d cursor_have=%v cursor_swarm=%s first_missing=%s window=%d/%d total=%d/%d\n",
				time.Since(start).Seconds(), seekNo.Load(), cp, !blockedAtCursor, swarm[cp],
				missingLabel(firstMissing, cp), winHave, winEnd-cp, total, info.PieceCount)
		}
	}()
	defer func() { close(stopSampler); <-samplerDone }()

	if o.player > 0 {
		return runPlayer(o, client(), id, file, src, &cursor, &seekNo)
	}

	fracs, err := parseSeeks(o.seeks)
	if err != nil {
		return err
	}

	type result struct {
		frac float64
		off  int64
		ttfb time.Duration
		took time.Duration
	}
	var results []result
	httpc := client()
	for i, frac := range fracs {
		off := int64(frac * float64(file.Length))
		readLen := min(o.readLen, file.Length-off)
		seekNo.Store(int64(i))
		cursor.Store(file.Offset + off)

		budget := o.ttfbMax
		if i == 0 {
			budget = o.warmupTTFB
		}
		fmt.Printf("seek %d: offset %d (%.0f%%), reading %s, ttfb budget %s\n",
			i, off, frac*100, units.FormatBytes(readLen), budget)

		ctx, cancel := context.WithTimeout(context.Background(), budget+60*time.Second)
		req, err := http.NewRequestWithContext(ctx, "GET",
			fmt.Sprintf("%s/v1/stream/%s/%d", o.api, id, o.fileIndex), nil)
		if err != nil {
			cancel()
			return err
		}
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", off))
		t0 := time.Now()
		resp, err := httpc.Do(req)
		if err != nil {
			cancel()
			return fmt.Errorf("seek %d: %w", i, err)
		}
		if resp.StatusCode != http.StatusPartialContent {
			resp.Body.Close()
			cancel()
			return fmt.Errorf("seek %d: status %s", i, resp.Status)
		}

		var ttfb time.Duration
		var got int64
		buf := make([]byte, 256<<10)
		want := make([]byte, 256<<10)
		for got < readLen {
			n, err := resp.Body.Read(buf)
			if n > 0 {
				if ttfb == 0 {
					ttfb = time.Since(t0)
				}
				if _, rerr := src.ReadAt(want[:n], off+got); rerr != nil {
					resp.Body.Close()
					cancel()
					return fmt.Errorf("seek %d: source read: %v", i, rerr)
				}
				if !bytes.Equal(buf[:n], want[:n]) {
					resp.Body.Close()
					cancel()
					return fmt.Errorf("seek %d: bytes differ at offset %d", i, off+got)
				}
				got += int64(n)
				cursor.Store(file.Offset + off + got)
			}
			if err != nil {
				resp.Body.Close()
				cancel()
				return fmt.Errorf("seek %d: read after %s: %v", i, units.FormatBytes(got), err)
			}
		}
		took := time.Since(t0)
		resp.Body.Close() // early close = player seeking away
		cancel()

		results = append(results, result{frac: frac, off: off, ttfb: ttfb, took: took})
		fmt.Printf("seek %d: ttfb=%s read %s in %s (%.1f MiB/s)\n",
			i, ttfb.Round(time.Millisecond), units.FormatBytes(got), took.Round(time.Millisecond),
			float64(got)/(1<<20)/took.Seconds())

		if i == 0 && ttfb > o.warmupTTFB {
			return fmt.Errorf("warmup seek ttfb %s exceeds %s", ttfb, o.warmupTTFB)
		}
	}

	fmt.Println("seek storm summary:")
	failed := false
	for i, r := range results {
		verdict := "ok"
		if i > 0 && r.ttfb > o.ttfbMax {
			verdict = fmt.Sprintf("FAIL (> %s)", o.ttfbMax)
			failed = true
		}
		fmt.Printf("  seek %d @ %3.0f%%: ttfb %8s  %s\n", i, r.frac*100, r.ttfb.Round(time.Millisecond), verdict)
	}
	if failed {
		return fmt.Errorf("time-to-first-byte exceeded %s after a seek — see the piece trace above for where the frontier sat", o.ttfbMax)
	}
	return nil
}

func missingLabel(firstMissing, cursorPiece int) string {
	if firstMissing < 0 {
		return "none"
	}
	return "+" + strconv.Itoa(firstMissing-cursorPiece)
}

func parseSeeks(s string) ([]float64, error) {
	var out []float64
	for _, part := range strings.Split(s, ",") {
		f, err := strconv.ParseFloat(strings.TrimSpace(part), 64)
		if err != nil || f < 0 || f >= 1 {
			return nil, fmt.Errorf("-seeks: %q is not a fraction in [0,1)", part)
		}
		out = append(out, f)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("-seeks: empty")
	}
	return out, nil
}

func client() *http.Client { return &http.Client{} }

type playerSeek struct {
	at    time.Duration
	delta float64
}

func parsePlayerSeeks(s string) ([]playerSeek, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}
	var out []playerSeek
	for _, part := range strings.Split(s, ",") {
		at, delta, ok := strings.Cut(strings.TrimSpace(part), ":")
		if !ok {
			return nil, fmt.Errorf("-player-seeks: %q is not time:delta", part)
		}
		d, err := time.ParseDuration(at)
		if err != nil {
			return nil, fmt.Errorf("-player-seeks: %q: %v", part, err)
		}
		f, err := strconv.ParseFloat(delta, 64)
		if err != nil {
			return nil, fmt.Errorf("-player-seeks: %q: %v", part, err)
		}
		out = append(out, playerSeek{at: d, delta: f})
	}
	return out, nil
}

// runPlayer simulates a video player against the stream endpoint: a
// paced forward read, a concurrent once-a-second tail reader, and
// scheduled seeks. Any read pause over a second is a rebuffer event.
func runPlayer(o options, httpc *http.Client, id backend.ID, file backend.FileInfo, src *os.File, cursor, seekNo *atomic.Int64) error {
	seeks, err := parsePlayerSeeks(o.playerSeeks)
	if err != nil {
		return err
	}
	fmt.Printf("player: %s at %s/s from %.0f%%, tail read every %s, %d scheduled seeks, max stall %s\n",
		o.player, units.FormatBytes(o.playerRate), o.playerStart*100, o.tailEvery, len(seeks), o.playerMaxStall)

	// Tail reader: a fresh request-and-close each tick, like an MP4
	// player fetching moov/subtitle tables from the file end alongside
	// every stretch of playback.
	stopTail := make(chan struct{})
	tailDone := make(chan struct{})
	var tailReads, tailErrs atomic.Int64
	go func() {
		defer close(tailDone)
		if o.tailEvery <= 0 {
			return
		}
		tick := time.NewTicker(o.tailEvery)
		defer tick.Stop()
		tailOff := max(file.Length-1<<20, 0)
		for {
			select {
			case <-stopTail:
				return
			case <-tick.C:
			}
			req, err := http.NewRequest("GET",
				fmt.Sprintf("%s/v1/stream/%s/%d", o.api, id, o.fileIndex), nil)
			if err != nil {
				tailErrs.Add(1)
				continue
			}
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", tailOff, min(tailOff+256<<10, file.Length)-1))
			resp, err := httpc.Do(req)
			if err != nil {
				tailErrs.Add(1)
				continue
			}
			if _, err := io.Copy(io.Discard, resp.Body); err != nil {
				tailErrs.Add(1)
			}
			resp.Body.Close()
			tailReads.Add(1)
		}
	}()
	defer func() { close(stopTail); <-tailDone }()

	type stall struct {
		at  time.Duration
		off int64
		dur time.Duration
	}
	var stalls []stall
	var played int64
	var severed int
	pos := int64(o.playerStart * float64(file.Length))
	start := time.Now()
	buf := make([]byte, 256<<10)
	want := make([]byte, 256<<10)
	nextSeek := 0
	seekCount := 0

	for time.Since(start) < o.player {
		// (Re)open at pos — a fresh Range request, exactly what a player
		// does after every seek or reload.
		req, err := http.NewRequest("GET",
			fmt.Sprintf("%s/v1/stream/%s/%d", o.api, id, o.fileIndex), nil)
		if err != nil {
			return err
		}
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", pos))
		resp, err := httpc.Do(req)
		if err != nil {
			return fmt.Errorf("player open at %d: %w", pos, err)
		}
		if resp.StatusCode != http.StatusPartialContent {
			resp.Body.Close()
			return fmt.Errorf("player open at %d: status %s", pos, resp.Status)
		}

	reading:
		for time.Since(start) < o.player {
			// Seek when one is due: close, jump, reopen.
			if nextSeek < len(seeks) && time.Since(start) >= seeks[nextSeek].at {
				delta := int64(seeks[nextSeek].delta * float64(file.Length))
				pos = max(0, min(pos+delta, file.Length-1))
				nextSeek++
				seekCount++
				seekNo.Store(int64(seekCount))
				cursor.Store(file.Offset + pos)
				fmt.Printf("player t=%+6.1fs seek #%d to offset %d (%.0f%%)\n",
					time.Since(start).Seconds(), seekCount, pos, float64(pos)/float64(file.Length)*100)
				break reading
			}
			// Pace to the playback rate: never read ahead of it, like a
			// player with a bounded demuxer cache.
			target := int64(time.Since(start).Seconds() * float64(o.playerRate))
			if played >= target {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			tRead := time.Now()
			n, err := resp.Body.Read(buf)
			if d := time.Since(tRead); d >= time.Second {
				stalls = append(stalls, stall{at: time.Since(start) - d, off: pos, dur: d})
				fmt.Printf("player t=%+6.1fs REBUFFER %.1fs at offset %d\n",
					(time.Since(start) - d).Seconds(), d.Seconds(), pos)
			}
			if n > 0 {
				if _, rerr := src.ReadAt(want[:n], pos); rerr != nil {
					resp.Body.Close()
					return fmt.Errorf("player: source read at %d: %v", pos, rerr)
				}
				if !bytes.Equal(buf[:n], want[:n]) {
					resp.Body.Close()
					return fmt.Errorf("player: bytes differ at offset %d", pos)
				}
				pos += int64(n)
				played += int64(n)
				cursor.Store(file.Offset + pos)
			}
			if err != nil {
				resp.Body.Close()
				if pos >= file.Length {
					fmt.Println("player: reached end of file")
					goto done
				}
				// The server severs streams whose stall exceeds its
				// read-timeout — exactly what triggers mpv's reload
				// script in the field. Reload: reopen at the same
				// position and keep playing. The blocking read that hit
				// the severance was already recorded as a rebuffer
				// above, so the outage still counts against max-stall.
				severed++
				fmt.Printf("player t=%+6.1fs stream severed at offset %d (%v); reloading\n",
					time.Since(start).Seconds(), pos, err)
				break reading
			}
		}
		resp.Body.Close()
	}
done:

	fmt.Printf("player summary: played %s in %s, %d tail reads (%d errors), %d rebuffers, %d severed streams\n",
		units.FormatBytes(played), o.player, tailReads.Load(), tailErrs.Load(), len(stalls), severed)
	var worst time.Duration
	var total time.Duration
	for _, st := range stalls {
		fmt.Printf("  t=%+6.1fs offset=%d stalled %s\n", st.at.Seconds(), st.off, st.dur.Round(time.Millisecond))
		total += st.dur
		if st.dur > worst {
			worst = st.dur
		}
	}
	fmt.Printf("  worst=%s total_stalled=%s\n", worst.Round(time.Millisecond), total.Round(time.Millisecond))
	if worst > o.playerMaxStall {
		return fmt.Errorf("worst rebuffer %s exceeds %s — see the piece trace above", worst.Round(time.Millisecond), o.playerMaxStall)
	}
	return nil
}
