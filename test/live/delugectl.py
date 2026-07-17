#!/usr/bin/env python3
"""Harness-side Deluge daemon control (test/live/run.sh).

Uses Deluge's own client library (present wherever deluged is installed)
for the operations the harness needs against the *seeder* daemon — the
leecher side is always driven through TorrentSeek's HTTP API, since that's
the code under test.

Usage:
  delugectl.py --port N [--config DIR] [--max-up KIBPS] add-seed <torrent> <download_dir>
  delugectl.py --port N [--config DIR] wait-complete <torrent_id> <timeout_s>
  delugectl.py --port N [--config DIR] connect-peer <torrent_id> <host> <peer_port>
  delugectl.py --port N [--config DIR] listen-port
  delugectl.py --port N [--config DIR] peers <torrent_id>
  delugectl.py --port N [--config DIR] peer-debug <torrent_id>
  delugectl.py --port N [--config DIR] rescue <torrent_id> <piece> [ban_s] [min_speed]
"""

import argparse
import base64
import sys
import time

from twisted.internet import defer, reactor

from deluge.ui.client import client


def auth_from(config_dir: str) -> tuple[str, str]:
    with open(f'{config_dir}/auth') as f:
        user, password = f.readline().strip().split(':')[:2]
    return user, password


@defer.inlineCallbacks
def run(args):
    user, password = auth_from(args.config)
    yield client.connect(host='127.0.0.1', port=args.port, username=user, password=password)

    if args.cmd == 'add-seed':
        with open(args.args[0], 'rb') as f:
            dump = base64.b64encode(f.read()).decode()
        options = {'download_location': args.args[1]}
        if args.max_up:
            # Per-torrent upload cap (KiB/s). Loopback swarms move fast
            # enough that "did we seek or just download everything" is
            # indistinguishable without one.
            options['max_upload_speed'] = args.max_up
        tid = yield client.core.add_torrent_file('seed.torrent', dump, options)
        print(tid)

    elif args.cmd == 'wait-complete':
        tid, timeout = args.args[0], float(args.args[1])
        deadline = time.time() + timeout
        while True:
            st = yield client.core.get_torrent_status(tid, ['progress', 'state'])
            if st.get('progress', 0) >= 100:
                print('complete')
                break
            if time.time() > deadline:
                print(f'TIMEOUT at {st}', file=sys.stderr)
                reactor.stop()
                sys.exit(1)
            d = defer.Deferred()
            reactor.callLater(1, d.callback, None)
            yield d

    elif args.cmd == 'connect-peer':
        tid, host, peer_port = args.args[0], args.args[1], int(args.args[2])
        yield client.core.connect_peer(tid, host, peer_port)
        print('ok')

    elif args.cmd == 'listen-port':
        port = yield client.core.get_listen_port()
        print(port)

    elif args.cmd == 'peer-debug':
        # Wire-level choke/queue state via the PiecePriority plugin — the
        # data that splits a frozen transfer into "stopped requesting"
        # vs "stopped answering".
        rows = yield client.piecepriority.get_peer_debug(args.args[0])
        for r in rows:
            print(
                f"  {r['ip']} q_down={r['download_queue_length']} "
                f"q_up={r['upload_queue_length']} piece={r['downloading_piece_index']} "
                f"interesting={r['interesting']} choked_by_us={r['choked']} "
                f"remote_interested={r['remote_interested']} choking_us={r['remote_choked']} "
                f"down={r['down_speed']} up={r['up_speed']}"
            )
        if not rows:
            print('  (no peers)')

    elif args.cmd == 'rescue':
        # Direct piecepriority.rescue_piece invocation, for exercising the
        # kick (and its min_speed bar) outside the scheduler's timing.
        tid, piece = args.args[0], int(args.args[1])
        ban_s = int(args.args[2]) if len(args.args) > 2 else 10
        min_speed = int(args.args[3]) if len(args.args) > 3 else 8192
        banned = yield client.piecepriority.rescue_piece(tid, piece, ban_s, min_speed)
        print(list(banned))

    elif args.cmd == 'peers':
        # Per-peer connection detail — who is actually connected and how
        # fast each link runs, for diagnosing swarm shape in the harness.
        st = yield client.core.get_torrent_status(
            args.args[0], ['peers', 'num_peers', 'num_seeds', 'download_payload_rate']
        )
        print(
            f"num_peers={st.get('num_peers')} num_seeds={st.get('num_seeds')} "
            f"rate={st.get('download_payload_rate')}"
        )
        for peer in st.get('peers') or []:
            print(
                f"  {peer.get('ip')} seed={peer.get('seed')} "
                f"down={peer.get('down_speed')} up={peer.get('up_speed')} "
                f"client={peer.get('client')!r}"
            )

    client.disconnect()
    reactor.stop()


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--port', type=int, required=True)
    p.add_argument('--config', required=True)
    p.add_argument('--max-up', type=int, default=0,
                   help='per-torrent upload cap in KiB/s (add-seed only)')
    p.add_argument('cmd', choices=['add-seed', 'wait-complete', 'connect-peer', 'listen-port', 'peers', 'peer-debug', 'rescue'])
    p.add_argument('args', nargs='*')
    args = p.parse_args()

    d = run(args)

    def failed(f):
        print(f'ERROR: {f.getErrorMessage()}', file=sys.stderr)
        reactor.stop()
        sys.exit(1)

    d.addErrback(failed)
    reactor.run()


if __name__ == '__main__':
    main()
