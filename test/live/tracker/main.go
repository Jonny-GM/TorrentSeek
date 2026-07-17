// Command tracker is a minimal BitTorrent HTTP tracker for the live test
// harness: it remembers who announced each info-hash and hands the peer list
// back in compact form. Just enough for the two deluged processes on
// localhost to find each other; not for real-world use.
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type peer struct {
	ip   net.IP
	port uint16
	seen time.Time
}

var (
	mu     sync.Mutex
	swarms = map[string]map[string]peer{} // info_hash → addr → peer
)

func main() {
	listen := flag.String("listen", ":6969", "address to listen on")
	flag.Parse()
	http.HandleFunc("/announce", announce)
	log.Printf("tracker listening on %s", *listen)
	log.Fatal(http.ListenAndServe(*listen, nil))
}

func announce(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	infoHash := q.Get("info_hash")
	port, err := strconv.ParseUint(q.Get("port"), 10, 16)
	if infoHash == "" || err != nil {
		http.Error(w, "bad announce", http.StatusBadRequest)
		return
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		http.Error(w, "bad remote addr", http.StatusBadRequest)
		return
	}
	ip := net.ParseIP(host).To4()
	if ip == nil {
		http.Error(w, "ipv4 only", http.StatusBadRequest)
		return
	}
	mu.Lock()
	swarm := swarms[infoHash]
	if swarm == nil {
		swarm = map[string]peer{}
		swarms[infoHash] = swarm
	}
	self := fmt.Sprintf("%s:%d", ip, port)
	if q.Get("event") == "stopped" {
		delete(swarm, self)
	} else {
		swarm[self] = peer{ip: ip, port: uint16(port), seen: time.Now()}
	}
	var compact []byte
	for addr, p := range swarm {
		if time.Since(p.seen) > 10*time.Minute {
			delete(swarm, addr)
			continue
		}
		// Never hand a peer its own address back. Every peer here shares
		// 127.0.0.1, and libtorrent reacts to a tracker-induced
		// self-connection by banning the IP — which on localhost bans
		// every other peer in the swarm too.
		if addr == self {
			continue
		}
		compact = append(compact, p.ip...)
		compact = append(compact, byte(p.port>>8), byte(p.port))
	}
	mu.Unlock()

	log.Printf("announce %x from %s (event=%q): %d peers", infoHash, self, q.Get("event"), len(compact)/6)
	resp := fmt.Sprintf("d8:intervali15e5:peers%d:", len(compact))
	w.Write(append(append([]byte(resp), compact...), 'e'))
}
