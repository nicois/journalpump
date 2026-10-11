// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

// Package websocket implements the websocket sender: the port of
// journalpump/senders/websocket.py (plan Task 24). It delivers journal
// batches as binary websocket frames to a remote endpoint, optionally
// snappy-compressing each frame and optionally applying the
// permessage-deflate extension.
//
// Python runs the connection as an asyncio event loop inside a Thread
// with a dummy consumer draining inbound messages and submits sends
// with run_coroutine_threadsafe. Go collapses that to ONE runner
// goroutine that owns the connection, dials/reconnects, drains inbound
// frames and serializes outbound frames; SendMessages hands a frame to
// the runner and waits for the write result (Python's send() returns
// through the event loop the same way).
//
// Two independent compression layers (websocket.py:26-34, 195):
//
//   - app-level `compression` (default "snappy"): each outgoing frame is
//     snappy-compress()'d before send. The connect header
//     Journalpump-Compression carries the value when it is not "none".
//   - protocol-level `websocket_compression` (default "none"): "deflate"
//     requests the permessage-deflate extension.
//
// Batching (websocket.py:66-73, 95-98): when max_batch_size > 0 the
// messages of one batch are NUL-joined into a SINGLE frame; the connect
// header Journalpump-Batch-Size carries the configured max_batch_size.
// When max_batch_size == 0 batching is disabled and each message is its
// own frame.
//
// Reconnect (websocket.py:76, 261-262, 295-305): a jittered exponential
// backoff base 20 s factor 1.8 cap 90 s, reset after a connection has
// been stable for more than 60 s.
package websocket

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/snappy"
	"github.com/gorilla/websocket"
	"golang.org/x/net/proxy"

	"github.com/Aiven-Open/journalpump/internal/sender"
	"github.com/Aiven-Open/journalpump/internal/tagged"
	"github.com/Aiven-Open/journalpump/internal/version"
)

// Ported constants (websocket.py).
const (
	// defaultMaxBatchSize is the config default max_batch_size (1024**2,
	// websocket.py:346).
	defaultMaxBatchSize = 1024 * 1024
	// defaultMaxSendInterval is config.get("max_send_interval", 1.0)
	// (websocket.py:315).
	defaultMaxSendInterval = 1.0
	// connectTimeout is websocket_connect's default timeout=30
	// (websocket.py:211).
	connectTimeout = 30 * time.Second
	// stableConnectionReset is the "connection was up long enough" window
	// (websocket.py:261).
	stableConnectionReset = 60 * time.Second
	// closeTimeout is close_timeout=20 (websocket.py:206).
	closeTimeout = 20 * time.Second
	// inboundLimit is max_size=MAX_KAFKA_MESSAGE_SIZE*2 (websocket.py:207).
	inboundLimit = 2 * 1024 * 1024
	// backoffBase/backoffFactor/backoffMax are the runner's ExponentialBackoff
	// parameters (base=20, factor=1.8, maximum=90, jitter=True,
	// websocket.py:76).
	backoffBase   = 20.0
	backoffFactor = 1.8
	backoffMax    = 90.0
)

// errNotConnected is the send failure when no connection is established
// within sendWaitConnected (websocket.py async_send returning False).
var errNotConnected = errors.New("websocket: not connected")

// sendWaitConnected is async_send's wait for the connected event
// (websocket.py:91). A variable so tests can shorten it.
var sendWaitConnected = 20 * time.Second

// errStopped is the send failure when the runner is shutting down.
var errStopped = errors.New("websocket: runner stopped")

// exceptionStats is the statsd seam for the sender's own
// unexpected_exception emission (websocket.py:386).
type exceptionStats interface {
	UnexpectedException(ex error, where string, tags map[string]string)
}

type compression string

const (
	compressionNone   compression = "none"
	compressionSnappy compression = "snappy"
)

type wsCompression string

const (
	wsCompressionNone    wsCompression = "none"
	wsCompressionDeflate wsCompression = "deflate"
)

// config is the decoded sender config block.
type config struct {
	uri                  string
	socks5Proxy          string
	ssl                  bool
	ca                   string
	keyfile              string
	certfile             string
	websocketCompression wsCompression
	compression          compression
	maxBatchSize         int
	maxSendInterval      float64
}

// Sender is the websocket transport (websocket.py WebsocketSender).
type Sender struct {
	cfg     config
	dialer  *websocket.Dialer
	headers http.Header
	stats   exceptionStats
	tags    *tagged.Tagged
	name    string
	log     sender.Logger

	// jitter returns a uniform value in [0, m) for the reconnect backoff;
	// tests inject a deterministic source.
	jitter func(m float64) float64

	runnerMu sync.Mutex
	runner   *runner
}

// New is the wiring-map constructor for output_type "websocket"
// (journalpump/senders/__init__.py).
func New(args sender.ConstructorArgs) (sender.Instance, error) {
	cfg, err := parseConfig(args.Config)
	if err != nil {
		return sender.Instance{}, err
	}
	s := &Sender{
		cfg:    cfg,
		stats:  args.Stats,
		tags:   args.Tags,
		name:   args.Name,
		log:    args.Log,
		jitter: func(m float64) float64 { return rand.Float64() * m },
	}
	s.headers = s.handshakeHeaders()
	d, err := s.buildDialer()
	if err != nil {
		return sender.Instance{}, err
	}
	s.dialer = d

	inst := sender.Instance{
		Transport:       s,
		MaxSendInterval: time.Duration(cfg.maxSendInterval * float64(time.Second)),
		MessageOverhead: 1, // plan: websocket overrides the shared overhead to 1 (websocket.py:73)
		Type:            "WebsocketSender",
	}
	// Framework batch slicing limit: the configured max_batch_size when
	// batching is enabled (the frame NUL-joins the whole slice), else the
	// framework default 1 MiB.
	if cfg.maxBatchSize > 0 {
		inst.MaxBatchSize = cfg.maxBatchSize
	}
	return inst, nil
}

// parseConfig decodes and validates the sender config block. Unlike
// Python (which reads every key lazily inside send_messages and retries
// forever on a bad value), the Go sender validates structurally at
// construction so a misconfiguration surfaces once instead of being
// retried silently (strict-at-load; flagged as a deliberate deviation).
func parseConfig(raw map[string]any) (config, error) {
	var cfg config

	uri, present := raw["websocket_uri"]
	if !present {
		// Python: config["websocket_uri"] raises KeyError in _init_websocket.
		return cfg, errors.New("config key 'websocket_uri' missing")
	}
	s, ok := uri.(string)
	if !ok {
		return cfg, fmt.Errorf("websocket_uri: %v is not a string", uri)
	}
	cfg.uri = s

	var err error
	if cfg.socks5Proxy, err = optString(raw, "socks5_proxy"); err != nil {
		return cfg, err
	}
	if cfg.ssl, err = optBool(raw, "ssl"); err != nil {
		return cfg, err
	}
	if cfg.ca, err = optString(raw, "ca"); err != nil {
		return cfg, err
	}
	if cfg.keyfile, err = optString(raw, "keyfile"); err != nil {
		return cfg, err
	}
	if cfg.certfile, err = optString(raw, "certfile"); err != nil {
		return cfg, err
	}

	if cfg.websocketCompression, err = optEnum(raw, "websocket_compression",
		[]wsCompression{wsCompressionNone, wsCompressionDeflate}, wsCompressionNone); err != nil {
		return cfg, err
	}
	if cfg.compression, err = optEnum(raw, "compression",
		[]compression{compressionNone, compressionSnappy}, compressionSnappy); err != nil {
		return cfg, err
	}

	cfg.maxBatchSize = defaultMaxBatchSize
	if v, present := raw["max_batch_size"]; present {
		n, ok := intValue(v)
		if !ok {
			return cfg, fmt.Errorf("max_batch_size: %v is not an integer", v)
		}
		cfg.maxBatchSize = n
	}

	cfg.maxSendInterval = defaultMaxSendInterval
	if v, present := raw["max_send_interval"]; present {
		f, ok := floatValue(v)
		if !ok {
			return cfg, fmt.Errorf("max_send_interval: %v is not a number", v)
		}
		cfg.maxSendInterval = f
	}

	// websocket.py:160-161 raises when ssl is on and any of the triple is
	// missing (lazily at connect time). Validate at load.
	if cfg.ssl && (cfg.ca == "" || cfg.keyfile == "" || cfg.certfile == "") {
		return cfg, errors.New("ca, keyfile, and certfile are required when ssl is enabled")
	}
	return cfg, nil
}

// optString returns the string value of an optional key, or "" when the key
// is absent or null, erroring when present with a non-string value.
func optString(raw map[string]any, key string) (string, error) {
	v, present := raw[key]
	if !present || v == nil {
		return "", nil
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("%s: %v is not a string", key, v)
	}
	return s, nil
}

// optBool returns the boolean value of an optional key, or false when the key
// is absent or null, erroring when present with a non-boolean value.
func optBool(raw map[string]any, key string) (bool, error) {
	v, present := raw[key]
	if !present || v == nil {
		return false, nil
	}
	b, ok := v.(bool)
	if !ok {
		return false, fmt.Errorf("%s: %v is not a boolean", key, v)
	}
	return b, nil
}

// optEnum returns the value of an optional enum key: the default when absent,
// the matching allowed value when present, else an error naming the choices.
func optEnum[T ~string](raw map[string]any, key string, allowed []T, def T) (T, error) {
	v, present := raw[key]
	if !present || v == nil {
		return def, nil
	}
	s, ok := v.(string)
	if !ok {
		return def, fmt.Errorf("%s: %v is not a string", key, v)
	}
	for _, a := range allowed {
		if T(s) == a {
			return a, nil
		}
	}
	names := make([]string, len(allowed))
	for i, a := range allowed {
		names[i] = string(a)
	}
	return def, fmt.Errorf("%s: %q is not one of %s", key, s, strings.Join(names, ", "))
}

// handshakeHeaders builds the connect headers (websocket.py:166-171).
func (s *Sender) handshakeHeaders() http.Header {
	h := http.Header{}
	h.Set("User-Agent", "journalpump/"+version.Version)
	if s.batching() {
		h.Set("Journalpump-Batch-Size", strconv.Itoa(s.cfg.maxBatchSize))
	}
	if s.cfg.compression != compressionNone {
		h.Set("Journalpump-Compression", string(s.cfg.compression))
	}
	return h
}

// batching reports whether frames are NUL-joined (websocket.py:69-72).
func (s *Sender) batching() bool { return s.cfg.maxBatchSize > 0 }

// buildDialer constructs the gorilla dialer: TLS when ssl is enabled,
// SOCKS5 when socks5_proxy is set, permessage-deflate when requested.
// Python only supports a SOCKS5 proxy (never HTTP_PROXY), so the
// dialer's HTTP proxy function is disabled.
func (s *Sender) buildDialer() (*websocket.Dialer, error) {
	d := &websocket.Dialer{
		HandshakeTimeout: connectTimeout,
		Proxy:            nil, // Python honors only socks5_proxy, not HTTP_PROXY
	}
	if s.cfg.websocketCompression == wsCompressionDeflate {
		d.EnableCompression = true
	}
	if s.cfg.ssl {
		tlsCfg, err := s.tlsConfig()
		if err != nil {
			return nil, err
		}
		d.TLSClientConfig = tlsCfg
	}
	if s.cfg.socks5Proxy != "" {
		sd, err := newSocks5Dialer(s.cfg.socks5Proxy)
		if err != nil {
			return nil, err
		}
		d.NetDialContext = sd.DialContext
	}
	return d, nil
}

// tlsConfig builds the client TLS config: the ca file is the verification
// root and certfile/keyfile is the client certificate
// (websocket.py:162-164 load_cert_chain + load_verify_locations).
func (s *Sender) tlsConfig() (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(s.cfg.certfile, s.cfg.keyfile)
	if err != nil {
		return nil, fmt.Errorf("websocket tls: %w", err)
	}
	pem, err := os.ReadFile(s.cfg.ca)
	if err != nil {
		return nil, fmt.Errorf("websocket tls: ca: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("websocket tls: ca %q contains no PEM certificates", s.cfg.ca)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
	}, nil
}

// frames turns one batch into the frames to send (websocket.py:95-105):
// NUL-join when batching, then snappy-compress each frame when app-level
// compression is on.
func (s *Sender) frames(batch [][]byte) [][]byte {
	msgs := batch
	if s.batching() {
		msgs = [][]byte{bytes.Join(batch, []byte{0})}
	}
	out := make([][]byte, len(msgs))
	for i, m := range msgs {
		if s.cfg.compression == compressionSnappy {
			out[i] = snappy.Encode(nil, m)
		} else {
			out[i] = m
		}
	}
	return out
}

// SendMessages implements sender.Sender (websocket.py:366-391). A failure
// is returned to the shared loop, which owns mark_disconnected/backoff
// (Python's send_messages catches its own exceptions and returns False,
// then calls _backoff itself; the Go framework does both).
func (s *Sender) SendMessages(ctx context.Context, batch [][]byte, _ string) error {
	r := s.ensureRunner()
	if err := r.submit(ctx, s.frames(batch)); err != nil {
		if !errors.Is(err, errNotConnected) && !errors.Is(err, errStopped) && ctx.Err() == nil {
			// Python's generic except path (websocket.py:383-386) emits
			// stats.unexpected_exception for an unexpected send failure.
			if s.stats != nil {
				s.stats.UnexpectedException(err, "sender", s.makeTags())
			}
		}
		return err
	}
	return nil
}

// makeTags mirrors self.make_tags({"app": "journalpump"}) (websocket.py:386):
// sender tags plus app=journalpump.
func (s *Sender) makeTags() map[string]string {
	if s.tags == nil {
		return map[string]string{"sender": s.name, "app": "journalpump"}
	}
	return s.tags.MakeTags(map[string]string{"sender": s.name, "app": "journalpump"})
}

// MaintenanceOperations implements sender.Sender; websocket has none.
func (s *Sender) MaintenanceOperations(context.Context) error { return nil }

// Close implements io.Closer: stop the runner goroutine and release the
// connection. The supervisor calls it at shutdown and on reload.
func (s *Sender) Close() error {
	s.runnerMu.Lock()
	r := s.runner
	s.runner = nil
	s.runnerMu.Unlock()
	if r != nil {
		r.stop()
	}
	return nil
}

// ensureRunner starts the runner goroutine on first use (Python's
// _init_websocket starts the thread lazily from send_messages).
func (s *Sender) ensureRunner() *runner {
	s.runnerMu.Lock()
	defer s.runnerMu.Unlock()
	if s.runner == nil {
		r := newRunner(s)
		s.runner = r
		go r.run()
	}
	return s.runner
}

func (s *Sender) logf(format string, args ...any) {
	if s.log != nil {
		s.log.Printf(format, args...)
	}
}

// liveConn is one established connection: the websocket plus the channel
// the runner reads outbound requests from.
type liveConn struct {
	ws     *websocket.Conn
	sendCh chan sendRequest
	closed chan struct{}
}

type sendRequest struct {
	frames [][]byte
	result chan error
}

// runner owns the connection lifecycle: dial, drain inbound, serve
// outbound, reconnect with the jittered backoff.
type runner struct {
	s   *Sender
	log sender.Logger

	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}

	mu          sync.Mutex
	conn        *liveConn
	connectedCh chan struct{} // closed when a connection is established
	backoff     *exponentialBackoff

	// dialFn establishes one connection; a field so tests can drive the
	// reconnect loop without a network (testing/synctest).
	dialFn func() (*websocket.Conn, error)
}

func newRunner(s *Sender) *runner {
	r := &runner{
		s:           s,
		log:         s.log,
		stopCh:      make(chan struct{}),
		doneCh:      make(chan struct{}),
		connectedCh: make(chan struct{}),
		backoff:     newExponentialBackoff(backoffBase, backoffFactor, backoffMax, s.jitter),
	}
	r.dialFn = r.dial
	return r
}

func (r *runner) run() {
	defer close(r.doneCh)
	for {
		select {
		case <-r.stopCh:
			return
		default:
		}
		conn, err := r.dialFn()
		if err != nil {
			r.s.logf("websocket connection error: %v; retrying", err)
			if !r.sleepBackoff() {
				return
			}
			continue
		}
		established := time.Now()
		lc := &liveConn{ws: conn, sendCh: make(chan sendRequest), closed: make(chan struct{})}
		r.setConnected(lc)
		r.s.logf("websocket connected to %s", r.s.cfg.uri)

		serveErr := r.serve(lc)
		close(lc.closed)
		r.setDisconnected(lc)
		r.closeConn(conn, serveErr)
		r.s.logf("websocket connection closed after %.0f s", time.Since(established).Seconds())

		if time.Since(established) > stableConnectionReset {
			r.backoff.reset()
		}
		if !r.sleepBackoff() {
			return
		}
	}
}

// dial establishes one connection under connectTimeout, cancelable by stop.
func (r *runner) dial() (*websocket.Conn, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stopWatch := make(chan struct{})
	go func() {
		select {
		case <-r.stopCh:
			cancel()
		case <-stopWatch:
		}
	}()
	defer close(stopWatch)

	ctx, timeoutCancel := context.WithTimeout(ctx, connectTimeout)
	defer timeoutCancel()

	conn, resp, err := r.s.dialer.DialContext(ctx, r.s.cfg.uri, r.s.headers)
	if err != nil {
		if resp != nil {
			_ = resp.Body.Close()
		}
		return nil, err
	}
	if r.s.cfg.websocketCompression == wsCompressionDeflate {
		conn.EnableWriteCompression(true)
	}
	conn.SetReadLimit(inboundLimit)
	return conn, nil
}

// serve drains inbound while serializing the outbound requests until the
// connection dies, the drain fails, or stop fires.
func (r *runner) serve(lc *liveConn) error {
	readerErr := make(chan error, 1)
	go func() { readerErr <- drain(lc.ws) }()
	for {
		select {
		case <-r.stopCh:
			return errStopped
		case err := <-readerErr:
			return err
		case req := <-lc.sendCh:
			if err := writeFrames(lc.ws, req.frames); err != nil {
				req.result <- err
				return err
			}
			req.result <- nil
		}
	}
}

// drain reads and discards inbound frames (websocket.py:84-87); its exit
// means the connection is dead.
func drain(conn *websocket.Conn) error {
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			return err
		}
	}
}

// writeFrames writes each frame as a binary message (websocket.py:103-114).
func writeFrames(conn *websocket.Conn, frames [][]byte) error {
	for _, frame := range frames {
		if err := conn.WriteMessage(websocket.BinaryMessage, frame); err != nil {
			return err
		}
	}
	return nil
}

// closeConn sends a close frame (bounded by closeTimeout) then closes the
// socket (websocket.py close_timeout=20).
func (r *runner) closeConn(conn *websocket.Conn, _ error) {
	_ = conn.WriteControl(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
		time.Now().Add(closeTimeout),
	)
	_ = conn.Close()
}

func (r *runner) setConnected(lc *liveConn) {
	r.mu.Lock()
	r.conn = lc
	close(r.connectedCh)
	r.mu.Unlock()
}

func (r *runner) setDisconnected(lc *liveConn) {
	r.mu.Lock()
	if r.conn == lc {
		r.conn = nil
		r.connectedCh = make(chan struct{})
	}
	r.mu.Unlock()
}

// waitConn returns the current connection, waiting up to sendWaitConnected
// for one to be established (websocket.py async_send's 20 s wait).
func (r *runner) waitConn(ctx context.Context) *liveConn {
	r.mu.Lock()
	if r.conn != nil {
		c := r.conn
		r.mu.Unlock()
		return c
	}
	ch := r.connectedCh
	r.mu.Unlock()

	timer := time.NewTimer(sendWaitConnected)
	defer timer.Stop()
	select {
	case <-ch:
		r.mu.Lock()
		c := r.conn
		r.mu.Unlock()
		return c
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return nil
	case <-r.stopCh:
		return nil
	}
}

// submit sends one batch's frames and waits for the write result.
func (r *runner) submit(ctx context.Context, frames [][]byte) error {
	lc := r.waitConn(ctx)
	if lc == nil {
		return errNotConnected
	}
	req := sendRequest{frames: frames, result: make(chan error, 1)}
	select {
	case lc.sendCh <- req:
	case <-lc.closed:
		return errNotConnected
	case <-ctx.Done():
		return ctx.Err()
	case <-r.stopCh:
		return errStopped
	}
	select {
	case err := <-req.result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-r.stopCh:
		return errStopped
	}
}

// sleepBackoff waits the next jittered backoff interval; false means stop.
func (r *runner) sleepBackoff() bool {
	d := r.backoff.next()
	r.s.logf("websocket retrying connection in %.1f s", d.Seconds())
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-r.stopCh:
		return false
	}
}

// stop asks the runner to exit and waits for it.
func (r *runner) stop() {
	r.stopOnce.Do(func() { close(r.stopCh) })
	<-r.doneCh
}

// exponentialBackoff is journalpump.util.ExponentialBackoff with full
// jitter (util.py:42-74): the sleep is jitter(0, base*factor**attempts),
// capped at maximum; the attempt counter stops growing at the cap and is
// reset once a connection is stable long enough.
type exponentialBackoff struct {
	base    float64
	factor  float64
	maximum float64
	jitter  func(float64) float64

	mu       sync.Mutex
	attempts int
}

func newExponentialBackoff(base, factor, maximum float64, jitter func(float64) float64) *exponentialBackoff {
	return &exponentialBackoff{base: base, factor: factor, maximum: maximum, jitter: jitter}
}

// next returns the next sleep interval.
func (b *exponentialBackoff) next() time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	result := b.base * math.Pow(b.factor, float64(b.attempts))
	if result <= b.maximum {
		b.attempts++
	} else {
		result = b.maximum
	}
	seconds := b.jitter(result)
	return time.Duration(seconds * float64(time.Second))
}

// reset clears the attempt counter (util.py:73-74).
func (b *exponentialBackoff) reset() {
	b.mu.Lock()
	b.attempts = 0
	b.mu.Unlock()
}

// intValue decodes an integer config value (JSON numbers arrive as
// float64 or json.Number).
func intValue(v any) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case int64:
		return int(n), true
	case float64:
		if n != math.Trunc(n) {
			return 0, false
		}
		return int(n), true
	case json.Number:
		i, err := n.Int64()
		return int(i), err == nil
	default:
		return 0, false
	}
}

// floatValue decodes a float config value.
func floatValue(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	default:
		return 0, false
	}
}

// socks5ProxyDialer dials the target through a SOCKS5 proxy. socks5
// resolves the target hostname locally (and hands the proxy an IP) while
// socks5h defers the name to the proxy (aiohttp-socks Proxy.from_url);
// x/net resolves names remotely in both cases, so a local resolver is
// layered in for socks5.
type socks5ProxyDialer struct {
	inner   proxy.Dialer
	timeout time.Duration
	resolve bool
}

func newSocks5Dialer(address string) (*socks5ProxyDialer, error) {
	u, err := url.Parse(address)
	if err != nil {
		return nil, fmt.Errorf("socks5_proxy: %w", err)
	}
	if u.Scheme != "socks5" && u.Scheme != "socks5h" {
		return nil, fmt.Errorf("socks5_proxy: unsupported scheme %q", u.Scheme)
	}
	inner, err := proxy.FromURL(u, proxy.Direct)
	if err != nil {
		return nil, fmt.Errorf("socks5_proxy: %w", err)
	}
	return &socks5ProxyDialer{inner: inner, timeout: connectTimeout, resolve: u.Scheme == "socks5"}, nil
}

// DialContext dials addr under min(ctx, timeout).
func (d *socks5ProxyDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()
	if d.resolve {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}
		if net.ParseIP(host) == nil {
			ips, err := net.DefaultResolver.LookupHost(ctx, host)
			if err != nil {
				return nil, err
			}
			if len(ips) == 0 {
				return nil, fmt.Errorf("socks5_proxy: no addresses for %s", host)
			}
			addr = net.JoinHostPort(ips[0], port)
		}
	}
	if cd, ok := d.inner.(proxy.ContextDialer); ok {
		return cd.DialContext(ctx, network, addr)
	}
	return d.inner.Dial(network, addr)
}
