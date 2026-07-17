# Spec: Torrent Client Backends

**Status:** Draft

The torrent client is pluggable. **Deluge (2.0+), via `deluged`'s own RPC
plus the `deluge-piece-priority` plugin, is the reference backend**,
specified in [04-deluge-backend.md](04-deluge-backend.md). The interface
here is the minimal contract the streaming core needs; nothing
client-specific may leak above it.

Transmission's RPC has no piece-level control at all — only a per-torrent
sequential-order toggle with a movable start pointer, which forces a
streaming core built on it to serialize scheduling into a single window
per torrent even though nothing about seek-aware streaming inherently
requires that (see
`docs/investigations/2026-07-10-sequential-download-scattering.md` for a
concrete failure mode this caused under multi-peer load). `deluge-piece-
priority` exposes libtorrent's real `piece_priority`/`set_piece_deadline`
primitives, so the reference backend can express exactly what the
scheduler wants instead of approximating it.

## Interface

A backend must provide:

| Capability | Notes |
|---|---|
| `add(magnet \| torrent_bytes) → torrent_id` | Idempotent: re-adding returns the existing id |
| `remove(torrent_id, delete_data)` | |
| `list()` / `get(torrent_id)` | Name, size, state, progress, piece size |
| `files(torrent_id)` | Index, path, length per file |
| `piece_state(torrent_id)` | Which pieces are complete (bitfield) |
| `prioritize(torrent_id, ranked_ranges)` | Make the pieces backing these ranges arrive soon; each range carries an explicit urgency rank. Mechanism is backend-specific |
| `read(torrent_id, file_index, offset, length)` | Read completed bytes from local storage |
| `events()` | Piece-completed notifications, or polling fallback |

This list is unchanged by the reference-backend switch — the interface was
already designed not to know about pointers, priorities, or deadlines as
concepts. What changed is the *contract* `prioritize` makes, below.

Three **optional capabilities** sit alongside the required set:

- `reannounce(torrent_id)` forces an immediate tracker reannounce to hunt
  for fresh peers. Prioritization steers the peers a client already has,
  but it cannot conjure a piece no connected peer holds; the scheduler
  uses this capability — when a backend offers it — to nudge a torrent
  whose blocked stream has been starving (03-streaming.md, "Stall
  nudge"). A backend without it simply never gets nudged; nothing above
  degrades.
- `piece_swarm_states(torrent_id)` reports each piece's relationship to
  the connected swarm: `unavailable` (no connected peer has it),
  `available`, `downloading`, or `have`. This is pure observability — it
  drives no scheduling decisions — but it is what makes a long stall
  diagnosable from the log alone: a blocked stream over an *unavailable*
  piece is starving on availability (only new peers can help), one over
  an *available* piece is starving on scheduling or peer throughput.
  The stall heartbeat log includes it when the backend offers it;
  callers must treat absence as "unknown", never as unavailable.
- `rescue_piece(torrent_id, piece)` kicks the stalled peers holding the
  piece's outstanding block requests, requeueing the blocks so the
  piece's still-active urgency re-requests them from healthy peers. The
  scheduler invokes it for a blocked stream stuck on a piece the swarm
  has (03-streaming.md, "Straggler rescue"); a backend without it just
  waits out the client's own request timeout.

## Identity

Torrent identity is the info-hash, presented as `btih:<hex>` throughout the
API. Backend-internal ids (e.g. Deluge's own torrent-id strings, which
happen to already be the info-hash but aren't guaranteed to stay that way)
never appear above the backend boundary.

## Semantics

- **`add` is idempotent.** Adding a torrent that already exists (same
  info-hash) returns the existing `torrent_id` with a flag, not an error.
- **File lengths are known from metadata.** Once a torrent's metadata is
  resolved, `files()` reports exact lengths even when zero bytes are
  downloaded. For magnet links, metadata resolution may take time; the
  backend must expose a `metadata_pending` state and `files()` returns empty
  until it clears.
- **`prioritize` expresses real, concurrent per-piece urgency.** The
  scheduler calls it frequently as read cursors move; backends must
  coalesce redundant calls rather than round-tripping each one. Each range
  carries an explicit rank — 0 is most urgent, and **multiple ranges may
  share a rank** (every blocked stream ties at 0; see 03-streaming.md),
  which is why rank is a field rather than implied by list position. A
  capable backend (Deluge) turns each range into an actual per-piece
  priority/deadline assignment scaled by its rank — multiple ranges on the
  same torrent are expected to progress concurrently, not serialize behind
  one cursor. The list is also sorted by rank so a backend that can only
  express coarser urgency (e.g. a future client with nothing better than a
  sequential-order toggle) still has a well-defined fallback: serve ranges
  in list order. That fallback is the degraded case, not the baseline the
  core scheduler designs around.
- **`read` never returns incomplete bytes.** Callers (the streamer) are
  responsible for waiting on `piece_state`/`events` before reading a range;
  the backend only validates that the file exists and the offset is in
  range.
- **`read` is a local disk read.** Backends read directly from the client's
  download directory; TorrentSeek deploys co-located with the torrent client
  (same host or a shared filesystem view of the download directory). Remote,
  RPC-tunneled reads are out of scope for v1 — the interface signature
  permits a future backend to implement them, but no current design work
  accommodates it.
- **`events` may be a poll.** Clients without a push channel for
  piece-level events poll piece state and synthesize piece-completed
  events; the interface hides this. (Deluge has a real push-event channel
  for torrent-level lifecycle changes but not per-piece completion — see
  [04-deluge-backend.md](04-deluge-backend.md) for which parts of `events()`
  it can push versus must poll.)

## Testing posture

The interface gets a fake in-memory implementation with scriptable piece
availability and timing, so the streamer and scheduler are testable without
a real torrent client or swarm. The Deluge backend is tested against a real
`deluged` process directly (no container needed — `deluged` is a plain
process, and Docker is unavailable in this project's CI environment
anyway); the live harness (`test/live/`, `make live`) starts one alongside a
tracker and a real seeding peer, run in CI via a manually triggered
workflow.

Until a second real backend exists, the fake backend is what keeps the
interface honest. No second backend is currently planned; qBittorrent's Web
API — the other obvious candidate — exposes the same class of limitation
as Transmission (file-level priority and a first/last-piece toggle, no
genuine per-piece control), so it would reintroduce the problem Deluge was
chosen to avoid, not complement it.
