// Command torrentseek is the TorrentSeek daemon: it serves the /v1 HTTP API
// (docs/spec/02-http-api.md) over a torrent client backend — Deluge by
// default, or an in-memory fake for development.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/jonny-gm/torrentseek/internal/backend"
	"github.com/jonny-gm/torrentseek/internal/backend/deluge"
	"github.com/jonny-gm/torrentseek/internal/backend/fake"
	"github.com/jonny-gm/torrentseek/internal/httpapi"
	"github.com/jonny-gm/torrentseek/internal/scheduler"
	"github.com/jonny-gm/torrentseek/internal/units"
)

var version = "dev"

type options struct {
	listen, token          string
	delugeAddr             string
	delugeUser, delugePass string
	readTimeout            time.Duration
	prepareTTL             time.Duration
	idleGrace              time.Duration
	pieceSettle            time.Duration
	stallNudge             time.Duration
	stallRescue            time.Duration
	windowLinger           time.Duration
	nowWindow              int64
	bootstrapHead          int64
	bootstrapTail          int64
	devFake                bool
	debug                  bool
}

// sizeValue is a flag.Value for byte sizes ("32MiB", "512k", "1048576").
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

func main() {
	var opts options
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.StringVar(&opts.listen, "listen", "127.0.0.1:3480", "address to bind the HTTP API")
	flag.StringVar(&opts.token, "token", "", "API token; required for non-loopback binds")
	flag.StringVar(&opts.delugeAddr, "deluge-addr", "127.0.0.1:58846", "deluged RPC endpoint (host:port)")
	flag.StringVar(&opts.delugeUser, "deluge-user", "", "Deluge daemon username (from Deluge's auth file)")
	flag.StringVar(&opts.delugePass, "deluge-pass", "", "Deluge daemon password")
	flag.DurationVar(&opts.readTimeout, "read-timeout", 60*time.Second, "max mid-body stall waiting for pieces before a stream is severed")
	flag.DurationVar(&opts.prepareTTL, "prepare-ttl", 120*time.Second, "idle decay for prepare-created priority windows")
	flag.DurationVar(&opts.idleGrace, "idle-grace", 5*time.Minute, "how long a torrent keeps its swarm alive after the last stream closes before it is paused")
	flag.DurationVar(&opts.pieceSettle, "piece-settle", time.Second, "delay trusting a piece for reads this long after completion, covering the gap between the backend reporting a piece finished and its disk write becoming visible (0 disables; see docs/spec/03-streaming.md)")
	flag.DurationVar(&opts.windowLinger, "window-linger", 10*time.Second, "keep a closed stream's priority window alive this long, so a player's constant close-and-reopen churn doesn't tear down and rebuild piece deadlines (0 disables)")
	flag.DurationVar(&opts.stallRescue, "stall-rescue", 6*time.Second, "when a stream has been blocked this long on a piece that is actively downloading, kick the peers holding its blocks hostage so they re-request from healthy peers (0 disables)")
	flag.DurationVar(&opts.stallNudge, "stall-nudge", 0, "force a tracker reannounce when a stream has been blocked this long, hunting for peers that have the missing piece (off by default — repeated forced reannounces are impolite to trackers; enable only for swarms with poor piece availability)")
	sizeFlag(&opts.nowWindow, 32<<20, "now-window", "top-priority window past each stream cursor")
	sizeFlag(&opts.bootstrapHead, 8<<20, "bootstrap-head", "file head prioritized on first open (container probing)")
	sizeFlag(&opts.bootstrapTail, 8<<20, "bootstrap-tail", "file tail prioritized on first open (container probing)")
	flag.BoolVar(&opts.devFake, "dev-fake", false, "serve the API over the in-memory fake backend (development)")
	flag.BoolVar(&opts.debug, "debug", false, "log stream opens, stalls, and scheduling detail")
	flag.Parse()

	if *showVersion {
		fmt.Println("torrentseek", version)
		return
	}
	if err := run(opts); err != nil {
		fmt.Fprintln(os.Stderr, "torrentseek:", err)
		os.Exit(1)
	}
}

// requiresToken reports whether a listen address is reachable from off-host
// and therefore must be protected (docs/spec/00-overview.md: non-loopback
// binding requires the token and is explicitly opt-in).
func requiresToken(listen string) bool {
	host, _, err := net.SplitHostPort(listen)
	if err != nil {
		return true // unparseable: refuse to assume it's safe
	}
	if host == "" {
		return true // ":3480" binds all interfaces
	}
	if host == "localhost" {
		return false
	}
	ip := net.ParseIP(host)
	return ip == nil || !ip.IsLoopback()
}

func run(opts options) error {
	level := slog.LevelInfo
	if opts.debug {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	if opts.token == "" && requiresToken(opts.listen) {
		return fmt.Errorf("listen address %q is not loopback: set -token to expose the API beyond this host (TorrentSeek is not designed to face the internet)", opts.listen)
	}

	var b backend.Backend
	if opts.devFake {
		b = fake.New()
		log.Warn("running with the in-memory fake backend; torrents must be pre-registered, this is for development only")
	} else {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		db, err := deluge.New(ctx, deluge.Config{
			Addr:      opts.delugeAddr,
			Username:  opts.delugeUser,
			Password:  opts.delugePass,
			IdleGrace: opts.idleGrace,
			Log:       log,
		})
		cancel()
		if err != nil {
			return err
		}
		b = db
		if db.Connected() {
			log.Info("connected to deluged", "addr", opts.delugeAddr)
		} else {
			log.Warn("deluged is not reachable; torrents and streams will report backend_unavailable until it comes up (retrying in the background)",
				"addr", opts.delugeAddr)
		}
	}
	defer b.Close()

	sched := scheduler.New(b, scheduler.Config{
		NowWindowBytes:     opts.nowWindow,
		BootstrapHeadBytes: opts.bootstrapHead,
		BootstrapTailBytes: opts.bootstrapTail,
		PrepareTTL:         opts.prepareTTL,
		PieceSettleTime:    opts.pieceSettle,
		StallNudgeAfter:    opts.stallNudge,
		StallRescueAfter:   opts.stallRescue,
		WindowLinger:       opts.windowLinger,
		Log:                log,
	})
	defer sched.Close()

	handler := httpapi.New(b, sched, httpapi.Config{Token: opts.token, ReadTimeout: opts.readTimeout, Log: log})
	log.Info("torrentseek listening", "addr", opts.listen, "auth", opts.token != "")
	return http.ListenAndServe(opts.listen, handler)
}
