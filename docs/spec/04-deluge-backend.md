# Spec: Deluge Backend

**Status:** Draft

The reference implementation of the backend interface
([01-backends.md](01-backends.md)), targeting **Deluge 2.0+**, over
`deluged`'s own native daemon RPC plus the `deluge-piece-priority` plugin
(a separate project, `github.com/Jonny-GM/deluge-piece-priority`) for the
per-piece priority/deadline control Deluge's stock RPC doesn't expose. This
spec maps each interface capability onto real RPC calls and defines how
seek-aware scheduling uses genuine per-piece control.

## Transport

Deluge's daemon RPC is a custom binary protocol over a raw TCP socket —
not JSON-RPC or anything HTTP-shaped (confirmed against
`deluge/core/rpcserver.py` and `deluge/transfer.py` in Deluge's own
source):

- **Framing:** each message is `<version:1 byte><length:uint32 BE><body>`.
  `version` is always `1`. `body` is `zlib.compress(rencode.dumps(payload))`
  — rencode (a bencode-like compact serialization) first, then zlib.
- **TLS is mandatory.** `deluged` only listens via `listenSSL`, with a
  self-signed certificate it generates itself on first run. The Go client
  connects with `InsecureSkipVerify: true` — there is no CA to verify
  against, matching what Deluge's own client does for the same daemon.
- **Requests** are a rencoded tuple of one or more `(request_id, method,
  args, kwargs)` 4-tuples — batching multiple calls in one message is part
  of the wire format, though this backend only ever sends one call per
  message (batching is an optimization not needed yet; see "Write
  coalescing" below for where round trips are actually saved).
- **Responses** are `(1, request_id, return_value)` (`RPC_RESPONSE`),
  `(2, request_id, exc_type_name, exc_args, exc_kwargs, traceback_str)`
  (`RPC_ERROR`), or `(3, event_name, event_args)` (`RPC_EVENT`, pushed
  unprompted for events this session subscribed to — see "Polling and
  events").
- **Auth:** `daemon.login(username, password)` is a specially-dispatched
  method (works before any other call succeeds) returning an auth level;
  credentials come from Deluge's own `auth` file
  (`<config-dir>/auth`, `username:password:level` per line) or explicit
  configuration. `daemon.info` (also special-cased) returns the daemon's
  version string, useful as a connectivity/version check before login.

### Rencode codec: implemented in-house, not `go-rencode`

`github.com/gdm85/go-rencode` is the existing Go rencode implementation,
but it's **GPL-2.0-licensed**; TorrentSeek is MIT, and statically linking a
GPLv2 package into a distributed MIT binary would make the combined
binary's license terms GPLv2's, not a decision to make implicitly by
picking a dependency. rencode's format is small (an extension of bencode
with more compact tags for small ints/lists/dicts) and this backend only
needs the subset Deluge's RPC actually round-trips — ints, floats,
strings/bytes, lists, tuples-as-lists, dicts, bools, and `None` — so
`internal/backend/deluge/rencode/` is a from-scratch, MIT-licensed
encoder/decoder scoped to exactly that subset, not a general-purpose
port.

## Capability mapping

| Interface capability | Deluge RPC |
|---|---|
| `add(magnet)` | `core.add_torrent_magnet(uri, options)` |
| `add(torrent_bytes)` | `core.add_torrent_file(filename, base64(bytes), options)` |
| `remove(id, delete_data)` | `core.remove_torrent(torrent_id, remove_data)` |
| `list()` / `get(id)` | `core.get_torrents_status({}, fields)` / `core.get_torrent_status(id, fields)` |
| `files(id)` | Same call, `files` field |
| `piece_state(id)` | Same call, `pieces` field (see below) |
| `prioritize(id, ranges)` | `piecepriority.set_piece_priorities` + `piecepriority.set_piece_deadlines` (bulk — see "Scheduling strategy") |
| `read(id, file_index, off, len)` | Direct disk read from the torrent's download location — no RPC |
| `events()` | `daemon.set_event_interest` (push, lifecycle events) + polling (piece bitfields) — see "Polling and events" |
| `reannounce(id)` (optional capability) | `core.force_reannounce([id])` — backs the scheduler's stall nudge (03-streaming.md, "Stall nudge") |
| `piece_swarm_states(id)` (optional capability) | Same status call, `pieces` field — the raw 0/1/2/3 states, not collapsed (see below) |
| `rescue_piece(id, piece)` (optional capability) | `piecepriority.rescue_piece` — IP-bans the piece's stalled block-holders for a few seconds (libtorrent disconnects filtered peers immediately, requeueing the blocks); repeat rescues escalate the `min_speed` bar (see "Rescue escalation") |

Standing `get_torrent_status`/`get_torrents_status` field set:
`hash`, `name`, `total_size`, `num_pieces`, `piece_length`,
`progress`, `state`, `download_location`, `files`, `pieces` (per-piece
state, verified against Deluge's source `_get_pieces_info`:
`0`=missing-from-swarm — no connected peer has it, `1`=a connected peer
has it but no transfer is active, `2`=being downloaded, `3`=have —
collapsed to a completed/not bitfield for `piece_state()`, and exposed
raw through the `piece_swarm_states()` capability for stall diagnosis),
`download_payload_rate`, `num_peers`.

## Identity

Deluge's `torrent_id` string is already the lowercase info-hash, so
`ID` conversion is direct (`btih:` + the string) with no separate lookup
table — but this backend still treats it as an opaque backend id and
converts at the boundary rather than assuming the format, in case a future
Deluge version changes it.

## Add semantics

- `core.add_torrent_magnet`/`core.add_torrent_file` return the new
  torrent's id; adding a magnet/file whose info-hash Deluge already knows
  raises `DelugeError` (`"Torrent already in session"` class of message) —
  the backend catches this specific case and maps it to the interface's
  idempotent-add contract (`existing: true`), not an error.
- Magnet adds have no `files` until Deluge resolves metadata. The backend
  reports `metadata_pending` while `core.get_torrent_status`'s `files`
  field is empty; a `TorrentAddedEvent`/subsequent status poll clears it
  once metadata resolves (Deluge does not have a distinct
  "metadata resolved" event of its own to push, unlike the interface's
  `EventMetadataResolved`, which this backend synthesizes from the poll).
- Torrents this backend adds idle **paused** while nothing wants them —
  pausing, not file-priority zeroing, is what keeps a fresh season pack
  from downloading tens of gigabytes in the background. Concretely:
  - Metainfo adds use `add_paused: true`, immediately followed by
    `piecepriority.verify` so data already on disk is verified and
    progress reported accurately (a paused torrent never checks on its
    own). `verify` rechecks without joining the swarm — no announce, no
    peer contact, paused again the instant checking completes. Deluge's
    own `core.force_recheck` is not a substitute: it resumes the handle
    and re-pauses only when the checked alert is processed, and peer
    connections formed in that window get torn down and parked in
    libtorrent's ~60s reconnect backoff — exactly the peers the first
    stream needs.
  - Magnet adds use `add_paused: false` — a paused torrent can't fetch
    metadata — and are paused at metadata resolution if no stream has
    shown interest by then.
  - On the first flush that wants a paused torrent, the backend resumes it
    and issues `core.force_reannounce` to rejoin the swarm promptly. A
    status snapshot showing a torrent paused while pieces are wanted also
    triggers a resume — Deluge's deferred post-recheck re-pause can
    otherwise silently undo a wake that raced it.
  - When the last interest ends, the torrent is **not** paused
    immediately: pausing disconnects every peer, and media players
    constantly close and reopen streams (container probing, seeks), so an
    immediate pause would force a 30–60s swarm rebuild on the very next
    request — the reconnect is further slowed by libtorrent's backoff on
    the peers just dropped. Instead the torrent keeps running with its
    last wanted file set (a genuine leecher, so no seed-drop poisoning;
    at worst it prefetches a few minutes of the file that was just
    streaming) for a configurable idle grace (default 5 min, `IdleGrace`/
    `-idle-grace`). The poll's idle sweep then pauses first — a graceful
    disconnect — and zeroes file priorities second, so the torrent is
    never active with every file unwanted. Torrents added outside this
    backend are never paused/resumed by it.
- File priorities keep Deluge's wanted-by-default until the first
  `prepare`/stream open writes the real wanted set (streamed files at
  normal priority, everything else `0`). Deliberately so: a torrent whose
  files are all unwanted counts as vacuously *finished*, and if it is
  ever briefly active in that state — say a user rechecks or resumes it
  from Deluge's own UI — every peer it touches drops the connection as
  seed-to-seed redundant and backoff-bans it for about a minute,
  poisoning exactly the peers the first stream needs. With
  wanted-by-default files such a window is an ordinary short-lived
  leecher contact, and the pause guarantees nothing actually transfers
  outside it.

## Scheduling strategy (real per-piece priority and deadlines)

`deluge-piece-priority` exposes libtorrent's
`piece_priority()`/`set_piece_deadline()` directly, so this backend can
express exactly the concurrent per-range urgency
[01-backends.md](01-backends.md) and [03-streaming.md](03-streaming.md)
assume as the baseline.

1. The file being served gets Deluge file priority `Normal` (not `High` —
   piece-level deadlines are the actual urgency signal; file priority only
   needs to be non-zero so the file downloads at all).
2. On every scheduler flush, the backend receives a rank-sorted list of
   piece ranges, each carrying its urgency rank (rank 0 may be shared by
   several ranges — every blocked stream ties at top urgency). For each
   range with rank `rank`:
   - every piece in the range gets Deluge piece priority `High` (`7`);
   - every piece in the range gets a deadline of
     `500ms + rank * 1500ms + offset * 100ms`, capped at `30000ms`, where
     `offset` is the piece's position from the range's start —
     blocked-stream ranges (rank tied at `0` per 03-streaming.md's
     ranking) get the tightest tier, less urgent windows progressively
     looser ones, and *within* a range urgency decays piece by piece away
     from the range start. The intra-range stagger matters: a range
     starts at a read cursor, and without it every piece in a 32 MiB
     window shares one deadline, giving libtorrent no reason to finish
     the piece the blocked reader is actually sitting on before the
     31 MiB behind it — a stream can stall for minutes on a single piece
     while the swarm pours data into the rest of the window. These are
     starting values to tune against real swarms, same status as
     03-streaming.md's other knobs.
   - a piece appearing in more than one active range (overlapping
     bootstrap/now-window near a file boundary) gets the tightest
     (minimum) deadline among the ranges it's in;
   - pieces the backend already knows are complete are excluded from the
     writes entirely — libtorrent drops a time-critical entry when its
     piece finishes, so urgency on a done piece is a no-op, and in a
     healthy stream the window slides forward precisely because pieces
     complete. Without this exclusion every slide re-writes deadlines for
     finished pieces and issues per-piece release RPCs for them, and that
     churn — not the swarm — becomes the throughput ceiling.
3. Pieces that were prioritized on a previous flush but are absent from the
   new desired set — and are still incomplete (a seek away, a closed
   stream; completed pieces released themselves) — have their deadline
   cleared (`piecepriority.clear_piece_deadline`) and priority reset to
   Deluge's default (`4`) — tracked as a diff against desired state, same
   pattern as the write-coalescing described below.
4. When the last stream/window on a torrent closes, the file's priority
   returns to `Normal`/unwanted per the add-semantics default and all its
   deadlines are cleared in one `piecepriority.clear_piece_deadlines`
   call.

There is no cross-window arbitration: every range in the list gets real,
concurrent piece-level treatment in the same flush. See 03-streaming.md's
"Concurrent windows, ranked urgency" for what the scheduler does with
that.

### Dependency: bulk deadline setter

`deluge-piece-priority` v1's RPC surface has `set_piece_priorities` (bulk)
but only a single-piece `set_piece_deadline` — its own spec calls this out
explicitly as a gap to close "if a real caller needs it." A scheduler flush
can touch dozens of pieces across several windows; one RPC round trip per
piece per flush tick is the kind of overhead the interface's "coalesce
redundant calls" contract exists to avoid. **This backend requires
`piecepriority.set_piece_deadlines(torrent_id, {piece: deadline_ms})`
(bulk) to exist** — adding it to `deluge-piece-priority` is a prerequisite
for this backend's scheduling strategy, not optional follow-up work.

### Rescue escalation

`piecepriority.rescue_piece` takes a `min_speed` bar (bytes/s) and only
kicks block-holders downloading slower than it — kicking a flowing peer
costs real bandwidth. But one bar can't be right twice: the value that
correctly spares healthy peers on the first attempt is exactly the value
a hostage-taker hides behind on the next, trickling the piece at tens of
KiB/s or serving other pieces fast while this piece's requests sit deep
in its queue — every rescue no-ops while the piece stays stuck.

So the backend keeps a per-`(torrent, piece)` attempt count and raises
the bar each time the scheduler re-invokes `rescue_piece` for the same
still-stuck piece:

| Attempt | `min_speed` | Meaning |
|---|---|---|
| 1 | 8 KiB/s | Obvious deadbeats only |
| 2 | 64 KiB/s | Anything slower than real service |
| 3+ | unlimited | Kick every holder — the piece has been stuck through two gentler attempts; re-requesting from the rest of the swarm beats waiting, and kicked healthy peers reconnect in seconds |

Attempts more than 30 s apart are separate stalls: the count resets and
the ladder starts over (the window must exceed the scheduler's
per-piece repeat interval, which is seconds). The ladder lives entirely
in this backend — the plugin RPC is stateless and the scheduler just
re-invokes on its interval (03-streaming.md, "Straggler rescue").

## Reads

`read()` is a direct disk read: `download_location` (from
`get_torrent_status`) + the file's path, at the requested offset. No RPC
round-trip is on the read path, which requires TorrentSeek to run
co-located with `deluged` (same host or a shared filesystem view of
`download_location`) — a stated v1 deployment requirement
([01-backends.md](01-backends.md)). Incomplete-piece safety is the
caller's job per the interface contract; the backend only checks that the
file exists and the offset is in range.

## Polling and events

Deluge has a real server-push event channel (`RPC_EVENT` messages, see
"Transport"). This backend uses it for what it covers and polls for what
it doesn't:

- **Pushed via `daemon.set_event_interest`:** `TorrentAddedEvent`,
  `TorrentRemovedEvent`, `TorrentFileCompletedEvent`,
  `TorrentFinishedEvent`. These map directly to
  `EventTorrentAdded`/`EventTorrentRemoved` and (for the metadata-pending
  case) trigger a status refetch.
- **Not available as events, so polled:** per-piece completion. Deluge's
  event set (confirmed by reading `deluge/event.py`) has no
  piece-level event — `TorrentFileCompletedEvent` is whole-file, not
  per-piece, and `deluge-piece-priority` explicitly documents that it
  forwards no libtorrent alerts over RPC either. So this part is
  poll-synthesized:
  - **Hot poll:** torrents with active streams, prepares, or blocked reads
    are polled (`get_torrent_status` for `pieces`) every **500 ms**.
  - **Cold poll:** all other known torrents every **10 s**, as a safety
    net for state changes missed between pushed events (connection blips,
    etc.).
  - The poller diffs successive `pieces` bitfields and emits
    `EventPieceCompleted` for newly-set bits.
- All fields for all hot-polled torrents are fetched in **one batched
  `core.get_torrents_status` call per tick**, not one call per torrent
  (the call takes a filter dict, so the batch is "all ids matching this
  filter").

## Write coalescing

Desired piece priorities and deadlines are tracked as backend-local state
(what was last sent, not what the scheduler last asked for — the two
differ during a failed-write retry) and flushed as a diff, at most once
per scheduling tick per torrent. Repeated `prioritize` calls that don't
change the desired set are absorbed with no RPC traffic.

## Failure handling

- **Daemon absent at startup:** an unreachable daemon is a transient
  condition, not a configuration error — TorrentSeek starts anyway,
  serves `backend_unavailable` from every operation, and the reconnect
  loop picks the daemon up whenever it appears. The one startup failure
  that *is* fatal is an explicit RPC-level rejection from a live daemon
  (bad credentials): retrying the same config forever cannot succeed,
  so that fails loudly instead of hiding a typo behind endless retries.
- **Connection loss:** polling and event delivery both depend on the one
  TCP+TLS connection; on disconnect, the backend reconnects with
  exponential backoff (hot-poll interval × 2ⁿ, capped at 30 s), re-runs
  `daemon.login` and `daemon.set_event_interest`, and resets backoff on
  first success. Streams over already-downloaded bytes keep serving
  through the outage (reads are direct disk I/O, no RPC on the read
  path); streams needing new pieces block under the normal read-timeout
  budget, and new operations fail fast with `backend_unavailable`.
- **Daemon restart / lost settings:** a daemon restart necessarily drops
  the RPC connection, and reconnecting clears all sent-state, so the
  first flush after any reconnect re-asserts file priority and every
  active piece priority/deadline from scratch. (Piece deadlines are
  session-scoped daemon state — a restarted daemon has lost them by
  definition, and the reconnect is the reliable signal that it happened.)
- **Failed writes:** a failed `piecepriority.*` call clears the sent-state
  for the affected torrent so the next tick retries the full desired set;
  failed reads surface as backend errors to the core.
