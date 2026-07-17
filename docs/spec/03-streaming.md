# Spec: Seek-Aware Streaming

**Status:** Draft

This is the core competency of TorrentSeek: serving HTTP Range requests over
files whose bytes may not exist locally yet, and steering the torrent client
so the bytes a consumer wants next are the bytes downloaded next.

## Range semantics

`GET /stream/{id}/{file_index}`:

- **`Accept-Ranges: bytes`** is always advertised.
- **`Content-Length` is always correct**, even at 0% downloaded — file length
  is known from torrent metadata.
- A request with a single-range `Range` header returns **`206 Partial
  Content`** with the exact requested range. Open-ended ranges (`bytes=X-`)
  are supported — players use them constantly.
- No `Range` header → `200` with the whole file.
- Unsatisfiable ranges → `416` with `Content-Range: bytes */<length>`.
- **Multi-range requests (`bytes=0-1,500-999`) are ignored**: the `Range`
  header is disregarded and the response is a `200` with the full file, which
  RFC 9110 explicitly permits an origin server to do. TorrentSeek never
  produces `multipart/byteranges` bodies. No real-world player sends
  multi-range requests; anything that does still gets a correct, playable
  response.
- `Content-Type` from filename extension; `HEAD` returns headers only and
  triggers no downloads and no prioritization.

## Blocking read model

If the bytes at the read cursor are not yet downloaded, the response
**stalls** (backpressure) while the scheduler prioritizes the missing pieces.
It does not error and does not serve zeros.

- A configurable per-read timeout (default 60 s) aborts the connection if
  the swarm cannot deliver, so a dead torrent fails visibly rather than
  hanging consumers forever.
- The stall happens mid-body: headers (status, lengths) are sent immediately
  since they never depend on downloaded bytes.
- Multiple concurrent streams (same or different files) are supported; each
  open stream maintains its own cursor and priority window.

## Piece scheduling

For every open stream (and every `prepare` call), the scheduler maintains a
priority window derived from the read cursor:

1. **Now-window:** the pieces backing the next N MB after the cursor get top
   priority, roughly sequential. The window is **fixed-size** (v1 makes no
   attempt to scale it with observed consumer read rate — revisit only with
   evidence from real swarms).
2. **Header/tail bootstrap:** on first open of a file, the first and last few
   pieces of the file are prioritized so container-format probing (MP4 moov
   atoms, MKV cues) works immediately.
3. **Seek = cursor jump:** a new `Range` request re-centers the window at the
   new offset; the old window's boost is dropped.
4. Everything outside active windows falls back to the torrent's normal
   priorities.

The scheduler talks to the backend only through `prioritize()` /
`piece_state()` / `events()` ([01-backends.md](01-backends.md)); it never
sees client-specific concepts.

### Concurrent windows, ranked urgency

The reference backend can advance every active window on a torrent
concurrently — real per-piece priority and deadlines, not a single movable
cursor (see [01-backends.md](01-backends.md),
[04-deluge-backend.md](04-deluge-backend.md)). The scheduler still computes
one **ordered** list of desired ranges (most-urgent-first) rather than an
unordered set, because order carries real information the backend uses:
the reference backend scales each range's piece deadlines by its rank, so
front-of-list ranges are pursued more aggressively than back-of-list ones
even though all of them get real, immediate priority. List order is a
*relative urgency* signal, not a serialization queue — nothing here
requires the backend to finish one range before starting the next.

- Correctness never depends on window order — a blocked read waits on
  `piece_state`/`events` regardless of which window fills first.
- Ranking **on the same torrent**: every **blocked** stream (one with a read
  actively waiting on bytes) ranks ahead of every non-blocked one, tied at
  equal top urgency — with real concurrent piece control there is no
  serialization to fairly queue for and no FIFO tie-break needed to keep
  blocked streams from starving each other; they simply all get the
  backend's tightest deadline tier and progress in parallel, limited only
  by genuine swarm bandwidth rather than by scheduler-imposed ordering.
  Non-blocked windows follow, ranked smallest runway (available bytes ahead
  of the cursor) first, newest stream winning ties, so the window closest
  to stalling gets the next-tightest deadline tier. Streams on different
  torrents are independent by construction.

### Cursor tracking

The cursor is the offset of the next byte the consumer will read: it starts
at the `Range` start and advances as body bytes are actually written to the
socket (not as they are read from disk), so a paused player stops pulling
and the window stops advancing with it.

### Asynchronous priority delivery

Recomputing desired ranges is cheap and happens inline, but pushing them
to the backend is a network round trip and must never gate the read path:
the scheduler parks the newest desired set per torrent and a delivery
goroutine sends it, skipping straight to the latest set when several
recomputes pile up (an intermediate set is obsolete the moment a newer one
exists). Serving a piece boundary therefore never waits on backend RPCs —
a scheduler that flushes synchronously caps streaming throughput at
piece-size ÷ RPC-latency, far below what the swarm can deliver.

### Stall nudge

Priorities and deadlines steer the peers the client is already connected
to, but they cannot conjure a piece no connected peer holds — in a fresh
or sparse swarm a stream can sit blocked on one piece while the swarm
delivers everything else at full speed, because piece *availability*, not
scheduling, is the bottleneck. When that happens, acquiring new peers is
the only lever left.

If a blocked wait exceeds a configurable threshold, the scheduler forces
a tracker reannounce on the torrent, repeating at the same interval while
the stream stays blocked. This is an optional backend capability
(`reannounce(id)`); the scheduler detects support at construction and
does nothing against a backend without it.

The nudge is **off by default**: repeated forced reannounces are impolite
to trackers (they exist to be polled on the announce interval, not
hammered when a client is unhappy), and in a swarm whose peers do have
the piece, a reannounce does nothing but add tracker load. Enable it
(`-stall-nudge 15s`) only when streams observably starve on piece
availability — a fresh swarm of mostly-leechers with spotty coverage.

### Straggler rescue

The nastiest stall is neither availability nor bandwidth: a piece's
requested blocks sit parked in a stalled peer's queue, and the client
won't re-request them elsewhere until its own request timeout snubs
that peer — a timeout tuned for downloads, not deadlines. A large,
healthy swarm makes this *worse*, not better: the rest of the window
pours in at megabytes per second while the one piece the reader needs
sits hostage for a minute or more.

When a stream has been blocked past a configurable threshold (default
6 s) on a piece the swarm demonstrably has (not `unavailable`, not yet
`have`), the scheduler asks the backend to rescue it: kick exactly the
stalled peers holding that piece's requested blocks, so the blocks
requeue and the piece's still-active urgency re-requests them from
healthy peers. Holders that are actually delivering are never kicked —
a kicked peer takes its bandwidth with it, so an indiscriminate kick
trades one stall for a worse one — and unavailable pieces are never
rescued, so a starved swarm keeps its few peers. Requires the backend's
optional
`rescue_piece` and `piece_swarm_states` capabilities; repeats per piece
at the threshold interval while still stuck. `0` disables.

Rescues for a piece that stays stuck **escalate**: each repeat is proof
the previous attempt's notion of "stalled holder" was too forgiving —
a holder can trickle the piece at tens of KiB/s, or serve other pieces
fast while this piece's requests sit deep in its queue, and either way
the piece never arrives while every holder clears a bar set low enough
to protect healthy peers. How the kick escalates is the backend's
business (see
04-deluge-backend.md for the Deluge ladder); the scheduler's contract
is only that it keeps re-invoking `rescue_piece` at the threshold
interval until the piece completes or the stream goes away.

### Window teardown

When a stream closes (client disconnect, timeout, or completion), its
window boost is not dropped immediately: it **lingers** for a
configurable grace (default 10 s) at a rank below every live window,
then decays. Media players churn streams constantly — closing and
reopening the same position for container probing, track reads, and
reload-style recovery (a real-world session logged 1,200 stream opens
in eight minutes) — and dropping the window on every close would tear
down the backend's piece deadlines and rebuild them on the reopen
milliseconds later, resetting the client's time-critical bookkeeping at
exactly the pieces it was chasing. A reopen inside the linger finds the
window still in force; a genuinely abandoned window costs at most a few
seconds of loose-deadline prefetch before it expires. `prepare` windows
decay after their own configurable idle TTL since `prepare` has no
connection whose closure can signal teardown.

## Readiness reporting

There is no per-range readiness endpoint. Consumers that want buffering UI
poll `prepare` with the range they care about and read `bytes_available`
from its response ([02-http-api.md](02-http-api.md)); the same call doubles
as the pre-buffer hint, so polling readiness and requesting it are one
operation.

## Configuration knobs

| Knob | Default | Meaning |
|---|---|---|
| `now_window_bytes` | 32 MiB | Size of the top-priority window past the cursor |
| `bootstrap_head_bytes` | 8 MiB | File head prioritized on first open |
| `bootstrap_tail_bytes` | 8 MiB | File tail prioritized on first open |
| `read_timeout` | 60 s | Max stall before aborting a blocked read |
| `prepare_ttl` | 120 s | Idle decay for prepare-created windows |
| `piece_settle` | 1 s | Delay before a freshly-completed piece is trusted for reads |
| `stall_nudge` | off | Blocked-stream age that triggers a forced tracker reannounce (opt-in) |
| `window_linger` | 10 s | How long a closed stream's window stays in force (survives player churn) |
| `stall_rescue` | 6 s | Blocked-stream age that triggers kicking the stalled peers holding the piece's blocks |

## Piece settle time

Some backends report a piece "have" before the bytes are actually visible
through the file the backend exposes. The reference backend is one of
them: libtorrent 2.x hashes a piece's blocks out of its in-memory store
buffer and posts piece-finished as soon as the hash verifies, while the
disk thread writes those blocks out asynchronously — so a read racing the
report can see stale file contents (typically a block of zeros mid-file
where the write hasn't landed yet). If a stream reads the
instant such a piece completes, it can serve corrupt bytes even though
the backend's own state says the piece is done.

`piece_settle` closes the race by refusing to serve a piece until it has
been observed complete for the configured duration. This shifts stream
latency at the download frontier by up to the settle time but does not
cap throughput — pieces settle concurrently, so a reader chasing the
frontier runs the settle time behind it at full speed. Pieces already
complete when a torrent is first observed are treated as settled (data
sitting on disk since before the process started has no fresh-write
race), so pre-seeded torrents serve immediately.

Setting it to `0` disables the gate; that is only safe against a backend
whose completion signal is already write-visible.

Defaults are starting points to be tuned against real swarms. Every knob is
a daemon flag — `-now-window`, `-bootstrap-head`, `-bootstrap-tail`,
`-read-timeout`, `-prepare-ttl`, `-piece-settle`, `-stall-nudge`,
`-window-linger`, `-stall-rescue` — with sizes accepting
`KiB`/`MiB`/`GiB` suffixes.
