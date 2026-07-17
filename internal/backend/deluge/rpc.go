// Deluge daemon RPC transport per docs/spec/04-deluge-backend.md:
// rencode-serialized, zlib-compressed messages with a 5-byte header
// (version byte + big-endian uint32 body length) over mandatory TLS.
package deluge

import (
	"bytes"
	"compress/zlib"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/jonny-gm/torrentseek/internal/backend/deluge/rencode"
)

const (
	protocolVersion = 1
	headerSize      = 5

	// maxBodySize bounds a single message body (a full piece bitfield for
	// a huge torrent is well under 1 MiB; 64 MiB leaves room without
	// letting a corrupt length prefix allocate unbounded memory).
	maxBodySize = 64 << 20

	// clientVersion is sent in daemon.login's mandatory client_version
	// kwarg. The daemon only checks presence, not value.
	clientVersion = "torrentseek"
)

// rpcError is an RPC_ERROR response from the daemon, carrying the Python
// exception's type name and stringified arguments.
type rpcError struct {
	ExcType string
	Args    string
}

func (e *rpcError) Error() string {
	return fmt.Sprintf("deluge rpc: %s: %s", e.ExcType, e.Args)
}

// conn is one authenticated connection to deluged. It is single-use:
// on any transport error every pending and future call fails and the
// owner discards it and dials a fresh one (reconnect policy lives in the
// backend, not here).
type conn struct {
	tcp net.Conn

	writeMu sync.Mutex // one frame writer at a time

	mu      sync.Mutex
	nextID  int64
	pending map[int64]chan result
	closed  bool
	err     error

	events chan event
	done   chan struct{}
}

type result struct {
	value any
	err   error
}

// event is a server-pushed RPC_EVENT.
type event struct {
	name string
	args []any
}

// dial connects, logs in, and subscribes to the lifecycle events the
// backend consumes. The TLS config skips verification by design: deluged
// generates its own self-signed certificate with no CA to verify against
// (docs/spec/04-deluge-backend.md, Transport).
func dial(ctx context.Context, addr, username, password string) (*conn, error) {
	d := tls.Dialer{Config: &tls.Config{InsecureSkipVerify: true}}
	tcp, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("deluge: dial %s: %w", addr, err)
	}
	c := &conn{
		tcp:     tcp,
		pending: make(map[int64]chan result),
		events:  make(chan event, 64),
		done:    make(chan struct{}),
	}
	go c.readLoop()

	// A call made before daemon.login succeeds is silently dropped by the
	// daemon (no error response), so login must complete first.
	if _, err := c.call(ctx, "daemon.login", []any{username, password},
		map[any]any{"client_version": clientVersion}); err != nil {
		c.close(err)
		return nil, fmt.Errorf("deluge: login: %w", err)
	}
	if _, err := c.call(ctx, "daemon.set_event_interest", []any{[]any{
		"TorrentAddedEvent", "TorrentRemovedEvent",
		"TorrentFileCompletedEvent", "TorrentFinishedEvent",
	}}, nil); err != nil {
		c.close(err)
		return nil, fmt.Errorf("deluge: set_event_interest: %w", err)
	}
	return c, nil
}

// call performs one RPC round trip.
func (c *conn) call(ctx context.Context, method string, args []any, kwargs map[any]any) (any, error) {
	if args == nil {
		args = []any{}
	}
	if kwargs == nil {
		kwargs = map[any]any{}
	}

	c.mu.Lock()
	if c.closed {
		err := c.err
		c.mu.Unlock()
		return nil, err
	}
	id := c.nextID
	c.nextID++
	ch := make(chan result, 1)
	c.pending[id] = ch
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
	}()

	if err := c.writeFrame([]any{[]any{id, method, args, kwargs}}); err != nil {
		return nil, err
	}
	select {
	case r := <-ch:
		return r.value, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.done:
		c.mu.Lock()
		err := c.err
		c.mu.Unlock()
		return nil, err
	}
}

func (c *conn) writeFrame(payload any) error {
	enc, err := rencode.Encode(payload)
	if err != nil {
		return fmt.Errorf("deluge: encode: %w", err)
	}
	var body bytes.Buffer
	zw := zlib.NewWriter(&body)
	if _, err := zw.Write(enc); err != nil {
		return fmt.Errorf("deluge: compress: %w", err)
	}
	if err := zw.Close(); err != nil {
		return fmt.Errorf("deluge: compress: %w", err)
	}

	frame := make([]byte, headerSize+body.Len())
	frame[0] = protocolVersion
	binary.BigEndian.PutUint32(frame[1:5], uint32(body.Len()))
	copy(frame[headerSize:], body.Bytes())

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if _, err := c.tcp.Write(frame); err != nil {
		c.close(err)
		return fmt.Errorf("deluge: write: %w", err)
	}
	return nil
}

func (c *conn) readLoop() {
	for {
		msg, err := c.readFrame()
		if err != nil {
			c.close(err)
			return
		}
		c.dispatch(msg)
	}
}

func (c *conn) readFrame() (any, error) {
	header := make([]byte, headerSize)
	if _, err := io.ReadFull(c.tcp, header); err != nil {
		return nil, err
	}
	if header[0] != protocolVersion {
		return nil, fmt.Errorf("deluge: protocol version %d, want %d", header[0], protocolVersion)
	}
	n := binary.BigEndian.Uint32(header[1:5])
	if n > maxBodySize {
		return nil, fmt.Errorf("deluge: message body %d bytes exceeds limit", n)
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(c.tcp, body); err != nil {
		return nil, err
	}
	zr, err := zlib.NewReader(bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("deluge: decompress: %w", err)
	}
	raw, err := io.ReadAll(io.LimitReader(zr, maxBodySize+1))
	if err != nil {
		return nil, fmt.Errorf("deluge: decompress: %w", err)
	}
	if len(raw) > maxBodySize {
		return nil, errors.New("deluge: decompressed body exceeds limit")
	}
	return rencode.Decode(raw)
}

func (c *conn) dispatch(msg any) {
	parts, ok := msg.([]any)
	if !ok || len(parts) < 3 {
		return // not a message shape we know; drop
	}
	kind, ok := parts[0].(int64)
	if !ok {
		return
	}
	switch kind {
	case 1: // RPC_RESPONSE: (1, request_id, value)
		id, ok := parts[1].(int64)
		if !ok {
			return
		}
		c.deliver(id, result{value: parts[2]})
	case 2: // RPC_ERROR: (2, request_id, exc_type, exc_args, exc_kwargs, traceback)
		id, ok := parts[1].(int64)
		if !ok {
			return
		}
		e := &rpcError{}
		if s, ok := parts[2].(string); ok {
			e.ExcType = s
		}
		if len(parts) > 3 {
			e.Args = fmt.Sprintf("%v", parts[3])
		}
		c.deliver(id, result{err: e})
	case 3: // RPC_EVENT: (3, event_name, args)
		name, ok := parts[1].(string)
		if !ok {
			return
		}
		args, _ := parts[2].([]any)
		select {
		case c.events <- event{name: name, args: args}:
		default: // slow consumer: drop; poll safety-net re-snapshots state
		}
	}
}

func (c *conn) deliver(id int64, r result) {
	c.mu.Lock()
	ch, ok := c.pending[id]
	c.mu.Unlock()
	if ok {
		ch <- r
	}
}

// close tears the connection down, failing all pending calls with err.
// Idempotent; only the first error wins.
func (c *conn) close(err error) {
	if err == nil {
		err = errors.New("deluge: connection closed")
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	c.err = err
	pending := c.pending
	c.pending = make(map[int64]chan result)
	c.mu.Unlock()

	c.tcp.Close()
	close(c.done)
	for _, ch := range pending {
		ch <- result{err: err}
	}
}

// callTimeout wraps call with a default deadline so a daemon that stops
// responding (but keeps the TCP session up) cannot wedge a caller that
// passed context.Background().
func (c *conn) callTimeout(ctx context.Context, method string, args []any, kwargs map[any]any) (any, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	return c.call(ctx, method, args, kwargs)
}
