# Spec: HTTP API

**Status:** Draft

All endpoints are JSON unless noted. The daemon binds `127.0.0.1` by default;
see [00-overview.md](00-overview.md) for the process/security model.

## Versioning

All endpoints live under a **`/v1` path prefix** from the first release
(`/v1/torrents`, `/v1/stream/...`). The prefix is omitted in the rest of this
document for brevity. Breaking changes require a new prefix; additive changes
(new fields, new endpoints) do not.

## Authentication

When the API token is enabled (mandatory off-loopback, optional on loopback):

- Preferred: `Authorization: Bearer <token>` header.
- Also accepted: `?token=<token>` query parameter — media players generally
  cannot set request headers on a URL they are handed, and stream URLs must
  be playable as-is.

Requests failing auth get `401` with the standard error envelope.

## Errors

Errors use a uniform envelope with an appropriate HTTP status:

```json
{"error": {"code": "torrent_not_found", "message": "..."}}
```

Codes are stable snake_case strings; messages are human-readable and not
contractual.

One code deserves calling out: `backend_unavailable` (HTTP 503) means
the torrent client cannot currently be reached — not running yet, or
restarting. TorrentSeek does not exit when this happens: it keeps
serving the API, reports this code from operations that need the
client, and recovers on its own once the client is back (streams over
already-downloaded data keep working throughout, since reads never
touch the client's RPC).

## Torrent management

```
POST /torrents/add
  body: {"magnet": "..."}  or multipart .torrent upload
  → 200 {"id": "btih:...", "existing": false}
```

Idempotent: adding an already-known torrent returns `"existing": true` with
the same id. For magnet links, the torrent may be returned in a
`metadata_pending` state before its file list is known.

```
GET  /torrents
  → [{id, name, total_size, progress, state}]

GET  /torrents/{id}
  → detail incl. piece_size, piece_count, backend state, and the
    observability pair rate_download (bytes/s) and peers (connected peer
    count), zero where a backend cannot report them — so "why is my stream
    slow" is answerable from this API alone

DELETE /torrents/{id}?delete_data=false

GET  /torrents/{id}/files
  → [{file_index, path, length, bytes_available}]
```

`DELETE` **aborts open streams immediately**: their connections are closed
as soon as the torrent is removed from the backend. Consumers that need
graceful drain must stop their readers first — the serving layer does not
hold torrents alive for stragglers.

`length` is exact once metadata is resolved, even at 0% downloaded.
`bytes_available` counts completed bytes anywhere in the file and lets
consumers display availability without understanding pieces.

## Preparing

```
POST /torrents/{id}/files/{file_index}/prepare
  body (optional): {"offset": 0, "length": ...}
  → 202 {"ready": false, "bytes_available": ...}
```

`prepare` warms a file (or byte range) without an open stream: it bumps piece
priorities so a subsequent read of that range starts instantly. It is
advisory and idempotent. Repeated `prepare` calls with a moving offset are
also the supported way to poll readiness and pre-buffer — there is no
separate readiness endpoint.

## Streaming

```
GET  /stream/{id}/{file_index}
HEAD /stream/{id}/{file_index}
```

The core endpoint: a seekable byte stream over the file. Full semantics —
Range handling, blocking-read model, timeouts, and how open streams drive
piece scheduling — are specified in [03-streaming.md](03-streaming.md).

Contract summary:

- `Accept-Ranges: bytes`, correct `Content-Length` before download completes.
- Single-range `Range` request → `206 Partial Content`; no `Range` (or an
  ignored multi-range) → `200` full file.
- `Content-Type` inferred from the filename extension.
- `HEAD` supported so consumers can probe size/type without triggering
  downloads.
- Reads for not-yet-downloaded bytes stall (with a timeout) rather than
  erroring or serving zeros.

## Convenience: play

```
GET /v1/play?magnet=<url-encoded magnet>
GET /v1/play?id=btih:...
```

One URL that turns a magnet into a playing video, so
`mpv "http://127.0.0.1:3480/v1/play?magnet=..."` works with nothing else:

1. With `magnet`, adds the torrent (idempotent — an already-known torrent is
   fine); with `id`, uses an existing torrent. Exactly one of the two.
2. Waits for magnet metadata to resolve, bounded by the daemon's read
   timeout; expiry yields `504` with code `metadata_timeout`.
3. Picks the **largest file with a video extension** (`mkv`, `mp4`, `avi`,
   `webm`, `mov`, `m4v`, `ts`, `mpg`, `mpeg`, `wmv`, `flv`), falling back to
   the largest file of any kind.
4. Responds `302 Found` to that file's `/v1/stream/{id}/{file_index}` URL. A
   `token` query parameter on the request is propagated to the redirect
   target, since players can't add auth headers when following redirects.

Anything smarter — file pickers, episode selection, quality preferences —
belongs in consumers, not here.
