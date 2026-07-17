# TorrentSeek
Seek-aware torrent streaming with pluggable torrent client backends.

TorrentSeek is a local daemon that turns torrents into seekable files: it
controls a torrent client (Deluge 2.0+ first) and exposes every file in
every torrent as an HTTP stream with full `Range` support. Reads and seeks
are translated into real per-piece priorities and deadlines, so the bytes a
consumer wants next are the bytes downloaded next and you can jump into the
middle of a file before the torrent is complete.

TorrentSeek controls your own torrent client and downloads nothing by
itself; use it only with content you have the right to download and share.

## Install

Linux and macOS:

```bash
curl -fsSL https://raw.githubusercontent.com/Jonny-GM/TorrentSeek/main/install.sh | bash
```

Windows (PowerShell):

```powershell
irm https://raw.githubusercontent.com/Jonny-GM/TorrentSeek/main/install.ps1 | iex
```

Both detect your platform and install just the `torrentseek` binary — Deluge and the
`deluge-piece-priority` plugin are installed separately (see
[its own install instructions](https://github.com/Jonny-GM/deluge-piece-priority)).

## Quickstart

Requirements: **Deluge 2.0+**, with `deluged` running and the
[`deluge-piece-priority`](https://github.com/Jonny-GM/deluge-piece-priority)
plugin installed and enabled — that plugin is what exposes the per-piece
control seeking needs; stock Deluge RPC doesn't have it. `deluged` must be
on the same host (or its download directory visible at the same path),
since TorrentSeek reads torrent bytes directly from disk.

```bash
torrentseek -deluge-user <user> -deluge-pass <pass>
```

Play a magnet in one line — `/v1/play` adds the torrent, waits for
metadata, picks the largest video file, and redirects to its stream (quote
the URL: magnets are full of `&`):

```bash
mpv "http://127.0.0.1:3480/v1/play?magnet=magnet:?xt=urn:btih:..."
```

Or drive the API directly:

```bash
curl -d '{"magnet":"magnet:?xt=urn:btih:..."}' http://127.0.0.1:3480/v1/torrents/add
{"id":"btih:...","existing":false}

curl http://127.0.0.1:3480/v1/torrents/btih:.../files
[{"file_index":0,"path":"Movie/movie.mkv","length":1469771776,"bytes_available":0}]

mpv http://127.0.0.1:3480/v1/stream/btih:.../0
```

### Flags

| Flag | Default | What it does |
|---|---|---|
| `-listen` | `127.0.0.1:3480` | Address to bind the HTTP API |
| `-token` | unset | API auth token — **required for non-loopback binds**; players may pass it as `?token=` |
| `-deluge-addr` | `127.0.0.1:58846` | `deluged` RPC endpoint |
| `-deluge-user` / `-deluge-pass` | unset | Daemon credentials, from Deluge's `auth` file |
| `-read-timeout` | `60s` | Max mid-body stall waiting for the swarm before a stream is severed |
| `-now-window` | `32MiB` | Top-priority window past each stream cursor |
| `-bootstrap-head` / `-bootstrap-tail` | `8MiB` | File head/tail prioritized on first open (container probing) |
| `-prepare-ttl` | `120s` | Idle decay for `/prepare`-created priority windows |
| `-piece-settle` | `1s` | Grace between a piece completing and its bytes being trusted on disk (`0` disables) |
| `-idle-grace` | `5m` | How long a torrent keeps its swarm alive after its last stream closes, before it is paused |
| `-window-linger` | `10s` | How long a closed stream's priority window stays in force, riding out player close-and-reopen churn (`0` disables) |
| `-stall-rescue` | `6s` | A stream blocked this long on an actively-downloading piece gets the piece's stalled block-holders kicked, re-requesting the blocks from healthy peers (`0` disables) |
| `-stall-nudge` | off | A stream blocked this long forces a tracker reannounce to hunt for new peers — opt-in, since forced reannounces add tracker load |
| `-debug` | off | Log stream opens, stalls, and scheduling detail |

Sizes accept `KiB`/`MiB`/`GiB` suffixes; durations use Go syntax (`90s`,
`5m`). The scheduling knobs and their reasoning live in
[03-streaming.md](docs/spec/03-streaming.md).

## Debugging playback issues

Run the daemon with `-debug` for stream open/close/stall logs, backend
piece-priority calls, and individual piece completions.

To isolate whether TorrentSeek served correct bytes — independent of any
player's own caching, reconnect, or seek behavior — use `torrentprobe`
(ships alongside `torrentseek` in every release):

```bash
torrentprobe fetch -magnet "magnet:?xt=urn:btih:..." -out capture.bin
# ... let it run past the point where playback misbehaves, then once the
# torrent has settled (fully downloaded, or verified complete in Deluge):
torrentprobe verify -against capture.bin
VERIFY OK: every captured byte matches a fresh read from the server
```

`fetch` reads sequentially from `/v1/stream` like a player does (optionally
with one simulated seek via `-seek-after`/`-seek-to`) and captures every
byte it receives, with a full timestamped log, to a local file. `verify`
re-reads those exact byte ranges later and compares byte-for-byte, reporting
the precise offset of any mismatch. If `verify` passes, TorrentSeek's
serving is proven correct for that run and any corruption is downstream
(player-side); if it fails, the reported offset is a concrete lead. Run
`torrentprobe fetch -h` / `-h verify` for all options.

## Development

Building from source needs Go 1.24+; `make build` produces
`./bin/torrentseek`.

```bash
make test          # vet + unit/integration tests (race detector)
make live          # live validation against a real deluged
SWARM=1 make live  # adds a tracker+seeder swarm stage
```

Unit and integration tests need no torrent client: the streaming core is
developed against a scriptable in-memory backend, and the Deluge backend
against a real `deluged` process directly (no container — `deluged` is a
plain process, and Docker is unavailable in this project's CI environment).
The live harness (`test/live/`) is the real thing, and runs in CI via the
manually-triggered "Live Deluge test" workflow.

## Spec docs

- [Overview](docs/spec/00-overview.md) — scope, goals, non-goals, process model
- [Backends](docs/spec/01-backends.md) — pluggable torrent client interface
- [HTTP API](docs/spec/02-http-api.md) — the API contract
- [Streaming](docs/spec/03-streaming.md) — Range semantics and seek-aware piece scheduling
- [Deluge backend](docs/spec/04-deluge-backend.md) — reference backend: Deluge over its native daemon RPC
