# Why sequential download scatters under a real swarm

Field report: a stream open on a real 5.3 GB, ~1269-piece torrent (41-50
peers, 8-16 MB/s aggregate) sent `sequential_download=true,
sequential_download_from_piece=0` to Transmission once, then stalled for the
full 60s read-timeout. The daemon log showed dozens of pieces completing in
the 500-1268 range — including the torrent's literal last piece — while
piece 0 through piece 65 never completed at all. `torrentprobe` confirmed
TorrentSeek itself served zero bytes and zero corruption during that stall
(it blocked correctly and reported `received=0` truthfully); the question
was why Transmission never delivered the one piece the read was blocked on.

This is not a TorrentSeek bug, not an RPC field-name/encoding bug (verified
against the real RPC spec and a real daemon), and — contrary to an earlier
hypothesis in this investigation — not simply "piece 0 was unavailable from
every connected peer." That theory doesn't survive the fact that the torrent
went on to complete fully and quickly. The actual mechanism is in
Transmission's own piece-picker and is straightforward once you read it.

## Method

Docker (used by this project's own `test/live` harness) was unavailable:
the org's egress policy blocks the relevant registries (`lscr.io`,
`production.cloudfront.docker.com`) outright, confirmed via the proxy's
`recentRelayFailures`. Per the proxy's own rule ("do not retry or route
around policy denials"), Docker was abandoned rather than worked around.

Instead: Transmission **4.1.3** was built from source
(`github.com/transmission/transmission`, tag `4.1.3`, daemon-only CMake
build, no GTK/Qt) and run as two real `transmission-daemon` processes,
driven with raw RPC calls — no TorrentSeek code involved anywhere in these
experiments, isolating Transmission's own behavior.

First attempt bound both daemons to loopback (127.0.0.1 / 127.0.0.2) on one
host. Result: **zero peer connections**, ever, despite the tracker
correctly handing back both peers. Reading `libtransmission/net.cc`
explained why:

```cpp
// is_martian_addr_helpers, net.cc:452
auto const loopback_allowed =
    from == TR_PEER_FROM_INCOMING || from == TR_PEER_FROM_LPD || from == TR_PEER_FROM_RESUME;
return ... || (!loopback_allowed && (addr.is_ipv4_loopback() || addr.is_ipv6_loopback())) || ...
```

Transmission treats a loopback address learned **from a tracker** as
"martian" and silently discards it as a peer candidate — by design, not a
bug. (This is exactly why the project's existing `test/live` Docker harness
works: containers get real non-loopback bridge IPs.) Docker being
unavailable, two Linux network namespaces joined by a veth pair
(`10.99.0.1` / `10.99.0.2` — genuinely routable, non-loopback) were used
instead. That fixed peer connectivity immediately.

## Finding 1 — sequential download works, with one built-in quirk

With one healthy, fully-seeded peer (300 MiB, 1 MiB pieces = 300 pieces,
seeder throttled to 1.5 MiB/s to get readable timing), issuing the exact
fields TorrentSeek issues —

```json
{"method":"torrent_set","params":{"ids":["<hash>"],"files_wanted":[0],
 "priority_high":[0],"sequential_download":true,
 "sequential_download_from_piece":0}}
```

— produced piece completions in **strict ascending order**, one roughly
every second: `0, 1, 2, 3, 4, 5, ... 66`, zero gaps, zero reordering. The
one exception: piece **299** (the torrent's last piece) arrived right after
piece 1, unprompted — before piece 2. That matches Transmission's own
source exactly:

```cpp
// peer-mgr-wishlist.cc:328, Impl::get_salt()
// Download first and last piece first
if (piece == 0U) { return 0U; }
if (piece == n_pieces - 1U) { return 1U; }
```

Confirms the RPC field names, casing, and request construction TorrentSeek
uses are all correct and functionally honored by a real 4.1.3 daemon.

## Finding 2 — the actual mechanism: sequential order is the *last* tiebreaker

`libtransmission/peer-mgr-wishlist.cc`, `Candidate::compare()` — this is
the sort order Transmission uses to rank which piece to fetch next:

```cpp
int Wishlist::Impl::Candidate::compare(Candidate const& that) const noexcept
{
    // prefer pieces closer to completion
    if (auto const val = tr_compare_3way(std::size(unrequested), std::size(that.unrequested)); val != 0)
        return val;
    // prefer higher priority
    if (auto const val = tr_compare_3way(priority, that.priority); val != 0)
        return -val;
    // prefer rarer pieces
    if (auto const val = tr_compare_3way(replication, that.replication); val != 0)
        return val;
    return tr_compare_3way(salt, that.salt);   // <- sequential order lives here, LAST
}
```

`salt` is where `sequential_download_from_piece` actually lives (see
`get_salt()` above). It is consulted **only** once "closer to completion,"
file priority, and piece rarity are all tied. In a single-peer test, those
three never meaningfully differ between not-yet-started pieces, so the sort
degenerates to pure sequential order — which is exactly why Finding 1 looked
clean. In a real swarm with 40+ simultaneous peer connections, many pieces
are concurrently partially-requested (different peers mid-flight on
different pieces) and have different replication counts across the swarm,
so "closer to completion" and "rarer piece" routinely **outrank** the
sequential salt. Transmission ends up fetching whatever is closest to done
or rarest across dozens of concurrent connections, not strictly the next
piece after the pointer — which is precisely the 500-1268-while-0-65-starve
pattern from the field report. This is Transmission's piece-picker working
as designed; `sequential_download` is a soft ordering hint, not a hard
constraint, and it gets weaker exactly when swarm activity is highest.

## Finding 3 — seeks pay an "in-flight drain" tax

Simulating a seek: after 15s of downloading from piece 0 (single peer),
`sequential_download_from_piece` was re-pointed to 150 mid-download — the
same `torrent_set` call TorrentSeek issues on every `Range` request:

```
before seek: piece 0, 1, 299, 2, 3   (t=12s .. t=14s)
seek issued: sequential_download_from_piece=150
after seek:  piece 0,1,2,3,4,299 (already-complete, t=0s baseline)
             5,6,7,8,...,19               (t=1s .. t=10s  — NOT 150!)
             150,151,152,...,191          (t=11s .. t=39s — strictly sequential)
```

Pieces already in flight *before* the seek (5 through 19 — the front of the
file, still draining from the pre-seek window) kept being serviced for
**~11 seconds** after the seek before piece 150 ever appeared. That's
`Candidate::compare()`'s "closer to completion" rule again: an
already-partially-downloaded piece outranks a freshly-pointed-to piece with
zero progress, salt or no salt. Once the pre-seek backlog drained, piece
150 onward arrived in clean sequential order with no further scattering.

With one peer, 11 seconds is a small fraction of TorrentSeek's 60s
read-timeout. Under a busy multi-peer swarm — where there's a much larger
population of concurrently in-flight, partially-complete pieces plus the
rarity tiebreak from Finding 2 — this drain tax is plausibly far larger,
and stacks with Finding 2's general order-scattering. This was not
reproduced under multi-peer contention (see below); it's a mechanism, not
a measured worst case.

## What this rules out / confirms

- **Not** a TorrentSeek RPC bug: field names, casing, and request shape are
  correct and functionally verified against a real 4.1.3 daemon.
- **Not** simple piece unavailability: the field-report torrent finished
  downloading fully and quickly, which is inconsistent with piece 0 being
  genuinely absent from the swarm — it was present but consistently
  outranked by Transmission's own selection heuristics.
- **Is** an inherent characteristic of Transmission's sequential-download
  feature: it is a last-resort tiebreaker in the piece-picker, not a hard
  directive, and it visibly weakens under real multi-peer load and across
  seeks.

## Not tested

- A live multi-peer/busy-swarm repro with varying per-peer piece
  availability (would need several partial-availability peers to generate
  real `replication` variance) — Findings 2 and 3's swarm-scale severity is
  inferred from source, not measured.
- Whether the user's actual production Transmission build behaves
  identically to this from-source 4.1.3 (version/build assumed equivalent,
  not independently confirmed).

## Possible directions (not implemented here)

- TorrentSeek could track how long a stream has been blocked on a specific
  piece and escalate (e.g. re-assert the pointer, or accept this as a
  documented limitation of the Transmission backend under heavy swarm
  load) — out of scope for this investigation; recorded here for the next
  round of work on the streaming scheduler.
