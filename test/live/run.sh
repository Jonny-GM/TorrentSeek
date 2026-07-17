#!/usr/bin/env bash
# Live validation of TorrentSeek against a real deluged.
#
# Stage 0 (always): the Go live tests (transport + backend) against the
#   leecher daemon — wire protocol, login, plugin RPC reachability,
#   idempotent add, prioritize round trip.
# Stage 1 (always): single daemon, pre-seeded data — add/verify lifecycle,
#   disk reads, full-file and Range streaming, dead-swarm timeout severing.
# Stage 2 (SWARM=1): tracker + a second seeding deluged — the leecher
#   starts empty and mid-file bytes arrive via real per-piece
#   priority/deadline steering.
# Stage 3 (SWARM=1): cold-stream throughput — streaming at the download
#   frontier must run at swarm speed, not be throttled by the serving path.
# Stage 4 (SWARM=1): seek storm — seeder capped at a realistic swarm rate,
#   scripted seeks with per-seek time-to-first-byte assertions and a piece
#   frontier trace sampled straight from deluged (test/live/seekstorm).
# Stage 5 (SWARM=1): player simulation — a heterogeneous four-seeder swarm
#   of rate-capped goseed instances (test/live/goseed; fast to slow, so
#   frontier blocks can straggle on slow peers, each dialing in from its
#   own loopback IP) under an mpv-shaped workload: paced playback reads, a
#   tail read every second, scheduled mid-playback seeks. Recreates the
#   field conditions behind real-world rebuffers; fails on any single
#   stall over 10s.
#
# Requirements: deluged (plus Deluge's python library for delugectl.py),
# go, jq, curl, cmp. No containers — deluged is a plain process.
#
# Environment:
#   DELUGED            deluged binary (default: deluged on PATH)
#   DELUGE_PYTHON      python with the deluge module (default: python3)
#   PIECEPRIORITY_EGG  prebuilt PiecePriority .egg to install
#   PIECEPRIORITY_SRC  deluge-piece-priority checkout to build the egg from
#                      (used when PIECEPRIORITY_EGG is unset; one of the
#                      two must be provided)
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
WORK="${WORK:-/tmp/torrentseek-live}"
SWARM="${SWARM:-0}"
API=http://127.0.0.1:3480
DELUGED="${DELUGED:-deluged}"
DELUGE_PYTHON="${DELUGE_PYTHON:-python3}"
LEECHER_PORT=58900
SEEDER_PORT=58901
SEEDER_PEER_PORT=16901

log() { printf '\n== %s\n' "$*"; }
fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }

cleanup() {
  set +e
  [[ -n "${TS_PID:-}" ]] && kill "$TS_PID" 2>/dev/null
  [[ -n "${TRACKER_PID:-}" ]] && kill "$TRACKER_PID" 2>/dev/null
  [[ -n "${LEECHER_PID:-}" ]] && kill "$LEECHER_PID" 2>/dev/null
  [[ -n "${SEEDER_PID:-}" ]] && kill "$SEEDER_PID" 2>/dev/null
  for pid in ${GOSEED_PIDS:-}; do kill "$pid" 2>/dev/null; done
}
trap cleanup EXIT

wait_until() { # wait_until <seconds> <description> <command...>
  local deadline=$(( $(date +%s) + $1 )); shift
  local what="$1"; shift
  until "$@"; do
    (( $(date +%s) < deadline )) || fail "timed out waiting for $what"
    sleep 1
  done
}

# write_core_conf <config-dir> <daemon-port> <peer-port> <download-dir> <plugins-json>
# Deluge config files are two concatenated JSON objects (version header +
# payload); missing keys fall back to daemon defaults.
write_core_conf() {
  mkdir -p "$1/plugins"
  cat > "$1/core.conf" <<EOF
{
    "file": 1,
    "format": 1
}{
    "allow_remote": false,
    "daemon_port": $2,
    "listen_ports": [$3, $3],
    "random_port": false,
    "dht": false,
    "upnp": false,
    "natpmp": false,
    "utpex": false,
    "lsd": false,
    "new_release_check": false,
    "download_location": "$4",
    "enabled_plugins": $5
}
EOF
}

deluge_auth() { # deluge_auth <config-dir> → "user:pass"
  awk -F: 'NR==1{print $1":"$2}' "$1/auth"
}

dctl() { # dctl <port> <config> <cmd> [args...]
  "$DELUGE_PYTHON" "$HERE/delugectl.py" --port "$1" --config "$2" "${@:3}"
}

# --- setup -------------------------------------------------------------------

command -v "$DELUGED" >/dev/null || fail "deluged not found (set DELUGED)"
command -v jq >/dev/null || fail "jq is required"

log "workspace $WORK"
rm -rf "$WORK"
mkdir -p "$WORK/leecher-dl" "$WORK/source/Live.Movie.2026"

log "locating PiecePriority egg"
EGG="${PIECEPRIORITY_EGG:-}"
if [[ -z "$EGG" ]]; then
  [[ -n "${PIECEPRIORITY_SRC:-}" ]] || fail "set PIECEPRIORITY_EGG or PIECEPRIORITY_SRC"
  PYBIN="$DELUGE_PYTHON"
  ( cd "$PIECEPRIORITY_SRC" && rm -rf build dist && \
    "$PYBIN" -c "from setuptools import setup; setup()" bdist_egg >/dev/null 2>&1 )
  EGG=$(ls "$PIECEPRIORITY_SRC"/dist/PiecePriority-*.egg)
fi
[[ -f "$EGG" ]] || fail "PiecePriority egg not found at $EGG"
echo "egg: $EGG"

log "starting deluged (leecher, daemon port $LEECHER_PORT)"
write_core_conf "$WORK/config-leecher" "$LEECHER_PORT" 16900 "$WORK/leecher-dl" '["PiecePriority"]'
cp "$EGG" "$WORK/config-leecher/plugins/"
"$DELUGED" -c "$WORK/config-leecher" -d -p "$LEECHER_PORT" \
  -L info -l "$WORK/deluged-leecher.log" &
LEECHER_PID=$!
wait_until 60 "leecher deluged RPC port" bash -c "exec 3<>/dev/tcp/127.0.0.1/$LEECHER_PORT" 2>/dev/null
wait_until 30 "leecher auth file" test -s "$WORK/config-leecher/auth"

LEECHER_AUTH=$(deluge_auth "$WORK/config-leecher")
LEECHER_USER="${LEECHER_AUTH%%:*}"
LEECHER_PASS="${LEECHER_AUTH#*:}"

log "generating content (32 MiB movie + sample)"
head -c $((32 * 1024 * 1024)) /dev/urandom > "$WORK/source/Live.Movie.2026/movie.mkv"
head -c 4096 /dev/urandom > "$WORK/source/Live.Movie.2026/sample.txt"

log "building torrentseek"
( cd "$REPO" && go build -o "$WORK/torrentseek" ./cmd/torrentseek )
( cd "$REPO" && go build -o "$WORK/mktorrent" ./test/live/mktorrent )

# --- stage 0: Go live tests against the real daemon ---------------------------

log "stage 0: Go live tests (transport + backend)"
"$WORK/mktorrent" -root "$WORK/source/Live.Movie.2026" -out "$WORK/gotest.torrent" > /dev/null
( cd "$REPO" && \
  DELUGE_LIVE_ADDR="127.0.0.1:$LEECHER_PORT" \
  DELUGE_LIVE_USER="$LEECHER_USER" \
  DELUGE_LIVE_PASS="$LEECHER_PASS" \
  DELUGE_LIVE_TORRENT="$WORK/gotest.torrent" \
  go test ./internal/backend/deluge/ -run 'TestLive' -count=1 -v ) \
  || fail "Go live tests failed"

# --- start the daemon under test ----------------------------------------------

# read-timeout must exceed BitTorrent's ~10s unchoke cycle: a fresh peer's
# first piece legitimately takes up to one rechoke tick to start flowing.
# TS_FLAGS: extra torrentseek flags for A/B experiment runs.
"$WORK/torrentseek" -listen 127.0.0.1:3480 -read-timeout 15s -debug \
  -deluge-addr "127.0.0.1:$LEECHER_PORT" \
  -deluge-user "$LEECHER_USER" -deluge-pass "$LEECHER_PASS" \
  ${TS_FLAGS:-} \
  > "$WORK/torrentseek.log" 2>&1 &
TS_PID=$!
wait_until 30 "torrentseek API" bash -c "curl -sf $API/v1/torrents >/dev/null"

# --- stage 1: pre-seeded single daemon -----------------------------------------

log "stage 1: pre-seeded torrent"
cp -r "$WORK/source/Live.Movie.2026" "$WORK/leecher-dl/"
"$WORK/mktorrent" -root "$WORK/source/Live.Movie.2026" -out "$WORK/movie.torrent" > "$WORK/movie.hash"
HASH=$(cat "$WORK/movie.hash")
echo "info-hash: $HASH"

ADD=$(curl -sf -F "torrent=@$WORK/movie.torrent" "$API/v1/torrents/add")
ID=$(jq -r .id <<<"$ADD")
[[ "$ID" == "btih:$HASH" ]] || fail "add returned $ADD, expected btih:$HASH"

wait_until 120 "verification to complete" bash -c \
  "curl -sf $API/v1/torrents/$ID | jq -e '.progress == 1' >/dev/null"
echo "torrent verified complete"

FILES=$(curl -sf "$API/v1/torrents/$ID/files")
IDX=$(jq -r '.[] | select(.path | endswith("movie.mkv")) | .file_index' <<<"$FILES")
LEN=$(jq -r '.[] | select(.path | endswith("movie.mkv")) | .length' <<<"$FILES")
[[ "$LEN" == "$((32 * 1024 * 1024))" ]] || fail "movie length $LEN"

log "HEAD probe"
curl -sfI "$API/v1/stream/$ID/$IDX" | grep -qi "accept-ranges: bytes" || fail "no Accept-Ranges"

log "full-file stream"
curl -sf "$API/v1/stream/$ID/$IDX" -o "$WORK/full.bin"
cmp "$WORK/full.bin" "$WORK/source/Live.Movie.2026/movie.mkv" || fail "full stream bytes differ"

log "range requests"
check_range() { # check_range <range-header-value> <skip> <count>
  curl -sf -H "Range: bytes=$1" "$API/v1/stream/$ID/$IDX" -o "$WORK/part.bin"
  dd status=none if="$WORK/source/Live.Movie.2026/movie.mkv" bs=1 skip="$2" count="$3" of="$WORK/want.bin"
  cmp "$WORK/part.bin" "$WORK/want.bin" || fail "range $1 bytes differ"
  echo "range $1 ok"
}
check_range "1000000-1999999" 1000000 1000000
check_range "$((LEN - 65536))-" $((LEN - 65536)) 65536
check_range "-65536" $((LEN - 65536)) 65536

log "dead-swarm timeout"
mkdir -p "$WORK/source/Dead.Torrent"
head -c $((1024 * 1024)) /dev/urandom > "$WORK/source/Dead.Torrent/void.bin"
"$WORK/mktorrent" -root "$WORK/source/Dead.Torrent" -out "$WORK/dead.torrent" > "$WORK/dead.hash"
DEAD_ID="btih:$(cat "$WORK/dead.hash")"
curl -sf -F "torrent=@$WORK/dead.torrent" "$API/v1/torrents/add" >/dev/null
START=$(date +%s)
if curl -s --max-time 60 "$API/v1/stream/$DEAD_ID/0" -o /dev/null; then
  fail "dead-swarm stream succeeded?!"
fi
ELAPSED=$(( $(date +%s) - START ))
echo "dead stream severed after ${ELAPSED}s (read-timeout 15s)"
(( ELAPSED >= 10 && ELAPSED <= 40 )) || fail "severing took ${ELAPSED}s, expected ~15s"
curl -sf "$API/v1/torrents" >/dev/null || fail "API unhealthy after severed stream"

# --- stage 2: real swarm --------------------------------------------------------

if [[ "$SWARM" == "1" ]]; then
  log "stage 2: tracker + seeding deluged"
  ( cd "$REPO" && go build -o "$WORK/tracker" ./test/live/tracker )
  "$WORK/tracker" -listen 127.0.0.1:6969 &
  TRACKER_PID=$!

  log "starting deluged (seeder, daemon port $SEEDER_PORT)"
  mkdir -p "$WORK/seeder-dl"
  write_core_conf "$WORK/config-seeder" "$SEEDER_PORT" "$SEEDER_PEER_PORT" "$WORK/seeder-dl" '[]'
  "$DELUGED" -c "$WORK/config-seeder" -d -p "$SEEDER_PORT" \
    -L info -l "$WORK/deluged-seeder.log" &
  SEEDER_PID=$!
  wait_until 60 "seeder deluged RPC port" bash -c "exec 3<>/dev/tcp/127.0.0.1/$SEEDER_PORT" 2>/dev/null
  wait_until 30 "seeder auth file" test -s "$WORK/config-seeder/auth"

  # Distinct content from stage 1 — same content would mean the same
  # info-hash, and the leecher would already have it.
  log "generating swarm content (256 MiB, seeder only)"
  mkdir -p "$WORK/source/Swarm.Movie.2026"
  head -c $((256 * 1024 * 1024)) /dev/urandom > "$WORK/source/Swarm.Movie.2026/movie.mkv"
  cp -r "$WORK/source/Swarm.Movie.2026" "$WORK/seeder-dl/"
  "$WORK/mktorrent" -root "$WORK/source/Swarm.Movie.2026" \
    -out "$WORK/swarm.torrent" -announce http://127.0.0.1:6969/announce > "$WORK/swarm.hash"
  SWARM_HASH=$(cat "$WORK/swarm.hash")
  SWARM_ID="btih:$SWARM_HASH"

  # The seeder's upload is capped (per-torrent, 10 MiB/s) so the
  # progress-after-seek assertion below stays meaningful: uncapped
  # loopback moves the whole 256 MiB in seconds, making "seeked to the
  # middle" and "downloaded everything" indistinguishable.
  dctl "$SEEDER_PORT" "$WORK/config-seeder" --max-up 10240 add-seed "$WORK/swarm.torrent" "$WORK/seeder-dl" >/dev/null
  dctl "$SEEDER_PORT" "$WORK/config-seeder" wait-complete "$SWARM_HASH" 120 >/dev/null
  echo "seeder is seeding"

  SWARM_ADD=$(curl -sf -F "torrent=@$WORK/swarm.torrent" "$API/v1/torrents/add")
  jq -e '.existing == false' <<<"$SWARM_ADD" >/dev/null || fail "swarm torrent was already known to the leecher: $SWARM_ADD"
  SIDX=$(curl -sf "$API/v1/torrents/$SWARM_ID/files" | jq -r '.[] | select(.path | endswith("movie.mkv")) | .file_index')

  log "cold mid-file seek over the real swarm"
  MID=$((128 * 1024 * 1024))
  START=$(date +%s)
  ok=""
  for attempt in 1 2 3; do
    # The connect-peer nudge lands while the stream is open and blocked, so
    # the leecher is interested when the peer link comes up — a peer
    # connected while the torrent wants nothing gets dropped as
    # uninterested (the backend also force-reannounces on the
    # no-interest→interest transition; the nudge just removes tracker
    # timing from the test).
    curl -sf --max-time 180 -H "Range: bytes=$MID-$((MID + 2 * 1024 * 1024 - 1))" \
        "$API/v1/stream/$SWARM_ID/$SIDX" -o "$WORK/swarm-part.bin" &
    CURL_PID=$!
    sleep 2
    dctl "$LEECHER_PORT" "$WORK/config-leecher" connect-peer "$SWARM_HASH" 127.0.0.1 "$SEEDER_PEER_PORT" >/dev/null || true
    if wait "$CURL_PID"; then
      ok=1; break
    fi
    echo "attempt $attempt severed (peers may still be connecting); retrying"
    sleep 3
  done
  [[ -n "$ok" ]] || fail "swarm range request failed after 3 attempts"
  TTLB=$(( $(date +%s) - START ))
  dd status=none if="$WORK/source/Swarm.Movie.2026/movie.mkv" bs=1M skip=128 count=2 of="$WORK/swarm-want.bin"
  cmp "$WORK/swarm-part.bin" "$WORK/swarm-want.bin" || fail "swarm range bytes differ"
  echo "2 MiB from the middle of a cold 256 MiB torrent in ${TTLB}s"
  (( TTLB <= 90 )) || fail "cold seek took ${TTLB}s"

  # The point of seek-aware streaming: we got mid-file bytes without
  # downloading the whole torrent first.
  PROGRESS=$(curl -sf "$API/v1/torrents/$SWARM_ID" | jq -r .progress)
  echo "leecher progress right after seek-read: $PROGRESS"
  jq -e '.progress < 0.9' <<<"$(curl -sf "$API/v1/torrents/$SWARM_ID")" >/dev/null \
    || fail "leecher progress $PROGRESS suggests the read was served by a full download, not a seek"

  # --- stage 3: cold-stream throughput -----------------------------------------
  # Streaming an incomplete torrent must deliver at ~swarm speed, not be
  # throttled by the server's piece-completion wake-up path. On loopback
  # deluge-to-deluge moves tens of MB/s; the stream reading right at the
  # download frontier must keep up.
  if [[ -n "${SKIP_TP:-}" ]]; then
    log "stage 3 skipped (SKIP_TP set)"
  else
  log "stage 3: cold-stream throughput (512 MiB, streamed while downloading)"
  mkdir -p "$WORK/source/Throughput.Test.2026"
  head -c $((512 * 1024 * 1024)) /dev/urandom > "$WORK/source/Throughput.Test.2026/movie.mkv"
  cp -r "$WORK/source/Throughput.Test.2026" "$WORK/seeder-dl/"
  "$WORK/mktorrent" -root "$WORK/source/Throughput.Test.2026" \
    -out "$WORK/tp.torrent" -announce http://127.0.0.1:6969/announce > "$WORK/tp.hash"
  TP_HASH=$(cat "$WORK/tp.hash")
  TP_ID="btih:$TP_HASH"

  dctl "$SEEDER_PORT" "$WORK/config-seeder" add-seed "$WORK/tp.torrent" "$WORK/seeder-dl" >/dev/null
  dctl "$SEEDER_PORT" "$WORK/config-seeder" wait-complete "$TP_HASH" 180 >/dev/null

  curl -sf -F "torrent=@$WORK/tp.torrent" "$API/v1/torrents/add" >/dev/null
  START=$(date +%s%N)
  curl -sf --max-time 300 "$API/v1/stream/$TP_ID/0" -o "$WORK/tp.out" &
  CURL_PID=$!
  sleep 2
  dctl "$LEECHER_PORT" "$WORK/config-leecher" connect-peer "$TP_HASH" 127.0.0.1 "$SEEDER_PEER_PORT" >/dev/null || true
  wait "$CURL_PID" || fail "cold throughput stream failed (see torrentseek.log)"
  ELAPSED_MS=$(( ($(date +%s%N) - START) / 1000000 ))
  cmp "$WORK/tp.out" "$WORK/source/Throughput.Test.2026/movie.mkv" || fail "throughput stream bytes differ"
  SPEED_MIBPS=$(( 512 * 1000 / ELAPSED_MS ))
  RATE=$(curl -sf "$API/v1/torrents/$TP_ID" | jq -r '"\(.rate_download) B/s from \(.peers) peer(s)"')
  echo "cold 512 MiB streamed in ${ELAPSED_MS}ms = ~${SPEED_MIBPS} MiB/s (deluge reported: $RATE at end)"
  grep "stream closed" "$WORK/torrentseek.log" | tail -3 || true
  (( SPEED_MIBPS >= 8 )) || fail "cold-stream throughput ${SPEED_MIBPS} MiB/s is far below LAN swarm speed — the serving path is throttling"
  fi

  # --- stage 4: seek storm at realistic swarm speed -----------------------------
  # Stages 2-3 run at uncapped loopback speed, which hides scheduling
  # problems: when the whole window arrives in a second, it doesn't matter
  # which piece libtorrent finished first. This stage caps the seeder at a
  # typical healthy-swarm rate so pieces genuinely compete for bandwidth,
  # then seeks around the file like a player while tracing the piece
  # frontier straight from deluged — if the piece under the cursor starves
  # while the window fills behind it, the trace shows it and the per-seek
  # time-to-first-byte assertion fails.
  log "stage 4: seek storm (512 MiB, seeder capped at 8 MiB/s)"
  ( cd "$REPO" && go build -o "$WORK/seekstorm" ./test/live/seekstorm )
  mkdir -p "$WORK/source/Seek.Storm.2026"
  head -c $((512 * 1024 * 1024)) /dev/urandom > "$WORK/source/Seek.Storm.2026/movie.mkv"
  cp -r "$WORK/source/Seek.Storm.2026" "$WORK/seeder-dl/"
  "$WORK/mktorrent" -root "$WORK/source/Seek.Storm.2026" \
    -out "$WORK/storm.torrent" -announce http://127.0.0.1:6969/announce > "$WORK/storm.hash"
  STORM_HASH=$(cat "$WORK/storm.hash")
  STORM_ID="btih:$STORM_HASH"

  dctl "$SEEDER_PORT" "$WORK/config-seeder" --max-up 8192 add-seed "$WORK/storm.torrent" "$WORK/seeder-dl" >/dev/null
  dctl "$SEEDER_PORT" "$WORK/config-seeder" wait-complete "$STORM_HASH" 180 >/dev/null

  curl -sf -F "torrent=@$WORK/storm.torrent" "$API/v1/torrents/add" >/dev/null
  STORM_IDX=$(curl -sf "$API/v1/torrents/$STORM_ID/files" | jq -r '.[] | select(.path | endswith("movie.mkv")) | .file_index')

  "$WORK/seekstorm" -id "$STORM_ID" -file "$STORM_IDX" \
    -src "$WORK/source/Seek.Storm.2026/movie.mkv" \
    -deluge-addr "127.0.0.1:$LEECHER_PORT" \
    -deluge-user "$LEECHER_USER" -deluge-pass "$LEECHER_PASS" &
  STORM_PID=$!
  sleep 2
  # Same connect-peer nudge as stage 2: it lands while the warmup seek is
  # blocked, taking tracker announce timing out of the test.
  dctl "$LEECHER_PORT" "$WORK/config-leecher" connect-peer "$STORM_HASH" 127.0.0.1 "$SEEDER_PEER_PORT" >/dev/null || true
  wait "$STORM_PID" || fail "seek storm failed (see trace above and torrentseek.log)"

  # --- stage 4b: close/reopen churn --------------------------------------------
  # Players close and reopen streams constantly (probing, track reads,
  # reload-style recovery). Model forward playback as one seek per 4 MiB
  # step, each a fresh HTTP request: every step tears down the previous
  # stream and immediately needs the pieces right after it, so this is
  # the workload that punishes deadline teardown/rebuild on stream
  # churn. The same per-seek TTFB bound applies to every step.
  log "stage 4b: close/reopen churn (12 forward steps of 4 MiB)"
  CHURN_SEEKS=$(awk 'BEGIN{for(i=0;i<12;i++)printf "%s%.7f",(i?",":""),0.6+i*0.0078125}')
  "$WORK/seekstorm" -id "$STORM_ID" -file "$STORM_IDX" \
    -src "$WORK/source/Seek.Storm.2026/movie.mkv" \
    -deluge-addr "127.0.0.1:$LEECHER_PORT" \
    -deluge-user "$LEECHER_USER" -deluge-pass "$LEECHER_PASS" \
    -seeks "$CHURN_SEEKS" -read 4MiB -warmup-ttfb 30s \
    || fail "churn stage failed (see trace above and torrentseek.log)"

  # --- stage 5: player simulation over a heterogeneous swarm --------------------
  # One fast seeder can't reproduce real-world rebuffers: with a single
  # uniform peer, window blocks never straggle. Four rate-capped goseed
  # instances (2 MiB/s down to 512 KiB/s) make libtorrent spread window
  # requests across peers of very different speeds — the field condition
  # where the piece under the playhead waits on a slow peer's queue while
  # aggregate bandwidth looks healthy. Each goseed dials in from its own
  # loopback IP: libtorrent allows one peer connection per IP per
  # torrent, so four local seeders presenting as 127.0.0.1 would silently
  # collapse to a single link (observed; that dead end is why the swarm
  # is goseeds rather than more deluged instances). The client is
  # mpv-shaped: paced playback reads (a bitrate, not a drain), a fresh
  # tail request every second (moov/subtitle tables — 595 of 1,200 opens
  # in a real session), and seeks mid-playback. 4 MiB pieces match the
  # field torrent.
  log "stage 5: player sim (4 goseeds at 2048/1024/512/512 KiB/s, 512 MiB, 4 MiB pieces)"
  # Isolate the experiment: earlier stages' torrents are still leeching
  # under idle-grace and would silently compete for bandwidth.
  for tid in "$SWARM_ID" "$STORM_ID"; do
    curl -sf -X DELETE "$API/v1/torrents/$tid?delete_data=true" >/dev/null || true
  done
  ( cd "$REPO" && go build -o "$WORK/goseed" ./test/live/goseed )
  mkdir -p "$WORK/source/Player.Sim.2026"
  head -c $((512 * 1024 * 1024)) /dev/urandom > "$WORK/source/Player.Sim.2026/movie.mkv"
  "$WORK/mktorrent" -root "$WORK/source/Player.Sim.2026" -piece-len $((4 * 1024 * 1024)) \
    -out "$WORK/sim.torrent" > "$WORK/sim.hash"
  SIM_ID="btih:$(cat "$WORK/sim.hash")"

  curl -sf -F "torrent=@$WORK/sim.torrent" "$API/v1/torrents/add" >/dev/null
  SIM_IDX=$(curl -sf "$API/v1/torrents/$SIM_ID/files" | jq -r '.[] | select(.path | endswith("movie.mkv")) | .file_index')

  GOSEED_PIDS=""
  gorates=(2048 1024 512 512)
  for i in 0 1 2 3; do
    "$WORK/goseed" -torrent "$WORK/sim.torrent" -data "$WORK/source/Player.Sim.2026/movie.mkv" \
      -dial 127.0.0.1:16900 -bind "127.0.0.8$i" -rate "${gorates[$i]}" \
      > "$WORK/goseed$i.log" 2>&1 &
    GOSEED_PIDS="$GOSEED_PIDS $!"
  done

  "$WORK/seekstorm" -id "$SIM_ID" -file "$SIM_IDX" \
    -src "$WORK/source/Player.Sim.2026/movie.mkv" \
    -deluge-addr "127.0.0.1:$LEECHER_PORT" \
    -deluge-user "$LEECHER_USER" -deluge-pass "$LEECHER_PASS" \
    -player 90s -player-start 0.02 -player-rate 1280KiB \
    -player-seeks "30s:+0.10,55s:-0.05,75s:+0.15" -player-max-stall 10s &
  SIM_PID=$!
  # Swarm-shape probes mid-sim: the verdict is meaningless if the
  # intended heterogeneous swarm didn't materialize.
  ( sleep 20; echo "-- peers at t+20s:"; dctl "$LEECHER_PORT" "$WORK/config-leecher" peers "${SIM_ID#btih:}" || true
    sleep 30; echo "-- peers at t+50s:"; dctl "$LEECHER_PORT" "$WORK/config-leecher" peers "${SIM_ID#btih:}" || true ) &
  PROBE_PID=$!
  wait "$SIM_PID" || { kill "$PROBE_PID" 2>/dev/null; fail "player sim failed (see rebuffer report and torrentseek.log)"; }
  wait "$PROBE_PID" 2>/dev/null || true
fi

log "ALL LIVE TESTS PASSED"
