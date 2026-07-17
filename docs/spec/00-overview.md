# TorrentSeek — Overview

**Status:** Draft

TorrentSeek turns torrents into seekable files. It controls a torrent client
through a pluggable backend and exposes each file inside each torrent as a
seekable byte stream over HTTP. A consumer adds a torrent, picks a file, and
starts reading immediately — including jumping to an arbitrary offset — while
the torrent client fetches the needed pieces on demand.

TorrentSeek is a systems component: stable, boring, testable. It is the
reusable primitive that higher-level products (media UIs, mpv scripts,
Stremio/Kodi add-ons, download managers) build on. Those products are
separate projects; TorrentSeek only knows about torrents, files, pieces, and
byte ranges.

## What it answers

- Add this magnet / `.torrent`.
- Which files are inside?
- Give me a stream URL for file index 3.
- Ensure byte range 8 GB–8.1 GB becomes available.

## Goals (v1)

- Add torrents from magnet links and `.torrent` files.
- Enumerate the files inside a torrent.
- Serve any file as an HTTP stream supporting `Range` requests, so consumers
  can seek to arbitrary offsets before the torrent is complete.
- Translate reads/seeks into piece priorities on the backend, so requested
  byte ranges become available quickly.
- Pluggable torrent client backends; **Deluge (2.0+, via `deluged`'s RPC
  plus the `deluge-piece-priority` plugin) is the reference backend**,
  since it's the one with real per-piece priority and deadline control —
  see [01-backends.md](01-backends.md).

## Non-goals

TorrentSeek has **no** users, profiles, posters, metadata matching, watch
state, subtitle preferences, or "watched" concept. It does not search
indexers or resolve names to torrents. It does not transcode. Anything
product-shaped lives in separate projects that consume its API.

It should never know whether a file is "S02E04", who watched it, or which
subtitle language is secondary.

## Process model

- Single local daemon, binding `127.0.0.1` on a configurable port by default.
- No accounts or multi-tenancy. Optional shared-secret token (header or query
  param) for the API, off by default on loopback.
- Non-loopback binding requires the token and is explicitly opt-in;
  TorrentSeek is not designed to face the internet.
- TorrentSeek holds no durable state: the torrent client owns everything
  that must survive restarts (known torrents, file selection, progress), and
  the daemon rebuilds its in-memory view from the backend on startup. A
  local store would only be justified by serving-level settings the client
  cannot hold; none exist today.

## Design principles

- **Local-only by default.** Anything user-facing lives above it.
- **Backends are pluggable.** The backend interface must not leak any
  specific torrent client's concepts upward. See
  [01-backends.md](01-backends.md).
- **The API is the product.** Consumers only ever see the HTTP API; keep it
  small, uniform, and versionable. See [02-http-api.md](02-http-api.md).
- **Seek-aware serving is the core competency.** The piece scheduler is what
  makes this more than a file server. See [03-streaming.md](03-streaming.md).

## Deferred (explicitly out of scope for now)

- **Virtual file tree / mounts** (WebDAV, rclone, WinFsp/davfs2): exposing
  torrent files as a browsable/mountable tree is a natural future surface of
  the same serving core, but all design work on it is deferred until the
  HTTP Range streaming path is solid.
- **Event stream** (SSE for torrent/availability changes): post-v1
  nice-to-have; consumers poll until then.

## Spec index

| Doc | Contents |
|---|---|
| [01-backends.md](01-backends.md) | Pluggable torrent client backend interface |
| [02-http-api.md](02-http-api.md) | HTTP API contract |
| [03-streaming.md](03-streaming.md) | Range semantics and seek-aware piece scheduling |
| [04-deluge-backend.md](04-deluge-backend.md) | Reference backend: Deluge over its native daemon RPC |
