// Command goseed is a minimal rate-capped BitTorrent seeder for the live
// harness's player-simulation stage. It holds a complete copy of one
// torrent's payload and serves piece requests over the wire protocol —
// nothing else: no tracker, no extensions, no downloading.
//
// Its two properties are exactly the ones a heterogeneous test swarm
// needs and real clients make hard:
//
//   - a precise per-connection upload cap (token pacing per block), so a
//     swarm of goseeds has *known* fast and slow peers;
//   - an explicit local bind address for the outgoing connection, so each
//     seeder dials the leecher from its own 127.0.0.x — libtorrent allows
//     one peer connection per IP per torrent, and a swarm of local peers
//     that all present as 127.0.0.1 silently collapses to one link
//     (observed live; Deluge exposes no way around it on the accepting
//     side).
//
// goseed dials INTO the leecher (torrent clients accept inbound peers on
// their listen port) and redials with backoff whenever the connection
// drops, so it needs no tracker and survives the leecher pausing and
// resuming the torrent.
package main

import (
	"crypto/rand"
	"crypto/sha1"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"time"
)

func main() {
	torrentPath := flag.String("torrent", "", "path to the .torrent file")
	dataPath := flag.String("data", "", "path to the complete payload file (single-file torrents only)")
	dial := flag.String("dial", "", "leecher peer address to connect to (host:port)")
	bind := flag.String("bind", "", "local address to dial from (gives this seeder its own peer IP)")
	rate := flag.Int64("rate", 1024, "upload cap in KiB/s")
	blackhole := flag.Int("blackhole-piece", -1, "accept but never answer requests for this piece (-2: for every piece — a completely dead peer) — a stalled peer holding blocks hostage, for straggler-rescue tests")
	flag.Parse()
	if *torrentPath == "" || *dataPath == "" || *dial == "" {
		fmt.Fprintln(os.Stderr, "usage: goseed -torrent FILE -data FILE -dial HOST:PORT [-bind IP] [-rate KIBPS]")
		os.Exit(2)
	}
	if err := run(*torrentPath, *dataPath, *dial, *bind, *rate*1024, *blackhole); err != nil {
		log.Fatal(err)
	}
}

func run(torrentPath, dataPath, dial, bind string, rateBytes int64, blackhole int) error {
	infoHash, pieceLen, total, err := parseTorrent(torrentPath)
	if err != nil {
		return err
	}
	data, err := os.Open(dataPath)
	if err != nil {
		return err
	}
	defer data.Close()
	if st, err := data.Stat(); err != nil || st.Size() != total {
		return fmt.Errorf("payload is %v bytes, torrent says %d", stSize(data), total)
	}
	numPieces := int((total + pieceLen - 1) / pieceLen)

	peerID := make([]byte, 20)
	copy(peerID, "-GS0001-")
	if _, err := rand.Read(peerID[8:]); err != nil {
		return err
	}

	log.Printf("goseed: %x, %d pieces of %d, dialing %s from %q at %d KiB/s (blackhole piece %d)",
		infoHash, numPieces, pieceLen, dial, bind, rateBytes/1024, blackhole)
	for {
		if err := serveOnce(infoHash, peerID, numPieces, pieceLen, data, dial, bind, rateBytes, blackhole); err != nil {
			log.Printf("goseed: session ended: %v (redialing)", err)
		}
		time.Sleep(2 * time.Second)
	}
}

func stSize(f *os.File) int64 {
	st, err := f.Stat()
	if err != nil {
		return -1
	}
	return st.Size()
}

func serveOnce(infoHash, peerID []byte, numPieces int, pieceLen int64, data *os.File, dial, bind string, rateBytes int64, blackhole int) error {
	d := net.Dialer{Timeout: 10 * time.Second}
	if bind != "" {
		d.LocalAddr = &net.TCPAddr{IP: net.ParseIP(bind)}
	}
	conn, err := d.Dial("tcp", dial)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Handshake: pstrlen, pstr, reserved, info_hash, peer_id — then the
	// same back from the leecher.
	hs := make([]byte, 0, 68)
	hs = append(hs, 19)
	hs = append(hs, "BitTorrent protocol"...)
	hs = append(hs, make([]byte, 8)...)
	hs = append(hs, infoHash...)
	hs = append(hs, peerID...)
	if _, err := conn.Write(hs); err != nil {
		return err
	}
	theirs := make([]byte, 68)
	if _, err := io.ReadFull(conn, theirs); err != nil {
		return fmt.Errorf("handshake: %w", err)
	}
	if string(theirs[28:48]) != string(infoHash) {
		return fmt.Errorf("handshake for wrong info-hash")
	}

	// Full bitfield, then unchoke — a seed with nothing to hide has no
	// reason to make the leecher wait a choke round-trip.
	bits := make([]byte, (numPieces+7)/8)
	for p := 0; p < numPieces; p++ {
		bits[p/8] |= 0x80 >> (p % 8)
	}
	if err := writeMsg(conn, 5, bits); err != nil {
		return err
	}
	if err := writeMsg(conn, 1, nil); err != nil {
		return err
	}

	// Serve requests forever, pacing piece payloads to the rate cap with
	// a budget clock: each block "costs" its transmission time at the
	// cap, and sends sleep until the clock allows them.
	budget := time.Now()
	hdr := make([]byte, 4)
	for {
		conn.SetReadDeadline(time.Now().Add(4 * time.Minute))
		if _, err := io.ReadFull(conn, hdr); err != nil {
			return err
		}
		length := binary.BigEndian.Uint32(hdr)
		if length == 0 {
			continue // keep-alive
		}
		if length > 1<<17+16 {
			return fmt.Errorf("oversized message (%d)", length)
		}
		msg := make([]byte, length)
		if _, err := io.ReadFull(conn, msg); err != nil {
			return err
		}
		if msg[0] != 6 || len(msg) != 13 { // only requests need answering
			continue
		}
		index := binary.BigEndian.Uint32(msg[1:5])
		begin := binary.BigEndian.Uint32(msg[5:9])
		size := binary.BigEndian.Uint32(msg[9:13])
		if size > 1<<17 {
			return fmt.Errorf("oversized request (%d)", size)
		}
		if int(index) == blackhole || blackhole == -2 {
			// Swallow the request: the block stays parked in this peer's
			// queue on the leecher side, exactly like a stalled peer in a
			// real swarm — the condition straggler rescue exists for.
			continue
		}

		cost := time.Duration(float64(size) / float64(rateBytes) * float64(time.Second))
		if now := time.Now(); budget.Before(now) {
			budget = now
		}
		budget = budget.Add(cost)
		time.Sleep(time.Until(budget))

		block := make([]byte, 8+size)
		binary.BigEndian.PutUint32(block[0:4], index)
		binary.BigEndian.PutUint32(block[4:8], begin)
		if _, err := data.ReadAt(block[8:], pieceLen*int64(index)+int64(begin)); err != nil {
			return fmt.Errorf("payload read: %w", err)
		}
		if err := writeMsg(conn, 7, block); err != nil {
			return err
		}
	}
}

func writeMsg(conn net.Conn, id byte, payload []byte) error {
	out := make([]byte, 5+len(payload))
	binary.BigEndian.PutUint32(out[0:4], uint32(1+len(payload)))
	out[4] = id
	copy(out[5:], payload)
	_, err := conn.Write(out)
	return err
}

// parseTorrent pulls info_hash, piece length, and total size out of a
// single-file .torrent — a tiny purpose-built bencode walk, not a general
// parser.
func parseTorrent(path string) (infoHash []byte, pieceLen, total int64, err error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, 0, 0, err
	}
	// The info dict's exact byte span: find "4:info" at the top level and
	// bdecode-skip one value from there.
	idx := indexInfo(raw)
	if idx < 0 {
		return nil, 0, 0, fmt.Errorf("no info dict in %s", path)
	}
	end, err := bskip(raw, idx)
	if err != nil {
		return nil, 0, 0, err
	}
	sum := sha1.Sum(raw[idx:end])
	infoHash = sum[:]

	pieceLen = findInt(raw[idx:end], "12:piece length")
	total = findInt(raw[idx:end], "6:length")
	if pieceLen <= 0 || total <= 0 {
		return nil, 0, 0, fmt.Errorf("piece length/total not found (single-file torrents only)")
	}
	return infoHash, pieceLen, total, nil
}

func indexInfo(raw []byte) int {
	for i := 0; i+6 < len(raw); i++ {
		if string(raw[i:i+6]) == "4:info" {
			return i + 6
		}
	}
	return -1
}

// bskip returns the index just past the bencode value starting at i.
func bskip(raw []byte, i int) (int, error) {
	if i >= len(raw) {
		return 0, fmt.Errorf("truncated bencode")
	}
	switch c := raw[i]; {
	case c == 'i':
		for j := i + 1; j < len(raw); j++ {
			if raw[j] == 'e' {
				return j + 1, nil
			}
		}
		return 0, fmt.Errorf("unterminated int")
	case c == 'l' || c == 'd':
		j := i + 1
		for j < len(raw) && raw[j] != 'e' {
			var err error
			if j, err = bskip(raw, j); err != nil {
				return 0, err
			}
		}
		return j + 1, nil
	case c >= '0' && c <= '9':
		n := 0
		j := i
		for ; j < len(raw) && raw[j] != ':'; j++ {
			n = n*10 + int(raw[j]-'0')
		}
		return j + 1 + n, nil
	}
	return 0, fmt.Errorf("bad bencode at %d", i)
}

func findInt(raw []byte, key string) int64 {
	for i := 0; i+len(key) < len(raw); i++ {
		if string(raw[i:i+len(key)]) == key && raw[i+len(key)] == 'i' {
			var n int64
			for j := i + len(key) + 1; j < len(raw) && raw[j] != 'e'; j++ {
				n = n*10 + int64(raw[j]-'0')
			}
			return n
		}
	}
	return -1
}
