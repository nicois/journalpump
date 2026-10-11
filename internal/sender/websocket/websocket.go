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
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	// stableConnectionReset is the "connection was up long enough" window
	// (websocket.py:261).
	stableConnectionReset = 60 * time.Second
	// inboundLimit is max_size=MAX_KAFKA_MESSAGE_SIZE*2 (websocket.py:207).
	// It bounds the DECOMPRESSED message size (websockets' max_size applies
	// after permessage-deflate expansion), not the compressed wire bytes.
	inboundLimit = 2 * 1024 * 1024
	// maxRedirects mirrors websockets MAX_REDIRECTS (=10,
	// WEBSOCKETS_MAX_REDIRECTS defaults to 10; websockets/asyncio/client.py:37).
	maxRedirects = 10
	// backoffBase/backoffFactor/backoffMax are the runner's ExponentialBackoff
	// parameters (base=20, factor=1.8, maximum=90, jitter=True,
	// websocket.py:76).
	backoffBase   = 20.0
	backoffFactor = 1.8
	backoffMax    = 90.0
)

// Timing vars: websockets' client defaults. Vars so tests can shorten them.
var (
	// connectTimeout is websocket_connect's outer timeout=30
	// (websocket.py:211), covering the whole connect incl. SOCKS.
	connectTimeout = 30 * time.Second
	// openTimeout is websockets' open_timeout default (10 s), the inner
	// handshake timeout (websockets/asyncio/client.py connect signature).
	openTimeout = 10 * time.Second
	// closeTimeout is close_timeout=20 (websocket.py:206).
	closeTimeout = 20 * time.Second
	// pingInterval/pingTimeout are websockets' keepalive defaults (20 s /
	// 20 s): a ping every pingInterval, and a pong expected within
	// pingTimeout (websockets/asyncio/connection.py keepalive).
	pingInterval = 20 * time.Second
	pingTimeout  = 20 * time.Second
	// writeWait bounds a single control-frame write.
	writeWait = 10 * time.Second
)

// httpsProxyTLSConfig returns the TLS client config used for the TLS hop to an
// HTTPS proxy (item #2). It is a var so a test can trust a local proxy's
// certificate; production uses the system roots.
var httpsProxyTLSConfig = func() *tls.Config {
	return &tls.Config{MinVersion: tls.VersionTLS12}
}

// errNotConnected is the send failure when no connection is established
// within sendWaitConnected (websocket.py async_send returning False).
var errNotConnected = errors.New("websocket: not connected")

// transportError wraps an expected transport failure (a failed write / a
// dropped connection). Python's async_send catches these and returns False,
// so they never reach the unexpected-exception handler (websocket.py:106-114,
// 383-386); the Go sender mirrors that by not emitting the metric for them.
type transportError struct{ err error }

func (e *transportError) Error() string { return e.err.Error() }
func (e *transportError) Unwrap() error { return e.err }

// sendWaitConnected is async_send's wait for the connected event
// (websocket.py:91). A variable so tests can shorten it.
var sendWaitConnected = 20 * time.Second

// errStopped is the send failure when the runner is shutting down.
var errStopped = errors.New("websocket: runner stopped")

// errInboundTooLarge is the drain failure when a frame expands past
// inboundLimit after decompression (websockets max_size).
var errInboundTooLarge = errors.New("websocket: inbound message exceeds the 2 MiB limit")

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
	headers http.Header
	stats   exceptionStats
	tags    *tagged.Tagged
	name    string
	log     sender.Logger

	// tlsCfg is the client TLS config for wss (certfile/keyfile + ca), built
	// once at construction; nil when ssl is off.
	tlsCfg *tls.Config

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
	if cfg.ssl {
		tc, err := s.tlsConfig()
		if err != nil {
			return sender.Instance{}, err
		}
		s.tlsCfg = tc
	}

	inst := sender.Instance{
		Transport:       s,
		MaxSendInterval: time.Duration(cfg.maxSendInterval * float64(time.Second)),
		// Batch slicing keeps the LogSender base defaults (base.py:130-131):
		// a 30-byte overhead and a 1 MiB limit, INDEPENDENT of the configured
		// max_batch_size. Python passes max_batch_size to the WebsocketRunner,
		// which stores it on ITSELF (websocket.py:71) only to emit the
		// Journalpump-Batch-Size header (websocket.py:169); the runner's
		// batch_message_overhead = 1 (websocket.py:73) is never read, and the
		// LogSender that actually slices (base.py:262-266) keeps the base
		// defaults. So the configured value changes ONLY batching (the
		// NUL-join) and the header — see batching().
		MaxBatchSize:    sender.DefaultMaxBatchSize,
		MessageOverhead: sender.DefaultMessageOverhead,
		Type:            "WebsocketSender",
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

	// websockets rejects an ssl argument with a ws:// URI
	// (websockets/asyncio/client.py:297-298, "ssl argument is incompatible
	// with a ws:// URI"). Without this the cert triple would be silently
	// ignored and the connection sent in PLAINTEXT.
	u, err := url.Parse(cfg.uri)
	if err != nil {
		return cfg, fmt.Errorf("websocket_uri: %w", err)
	}
	switch u.Scheme {
	case "ws", "wss":
	default:
		return cfg, fmt.Errorf("websocket_uri: unsupported scheme %q (want ws or wss)", u.Scheme)
	}
	if cfg.ssl && u.Scheme == "ws" {
		return cfg, errors.New("ssl is incompatible with a ws:// websocket_uri (would send plaintext)")
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

// optEnum returns the value of an optional enum key: the default when the key
// is ABSENT, the matching allowed value when present, else an error naming the
// choices. A present null is an error, not a default: Python's
// config.get(key, default) returns None for a present null and then
// WebsocketCompression(None)/JournalPumpMessageCompression(None) raises.
func optEnum[T ~string](raw map[string]any, key string, allowed []T, def T) (T, error) {
	v, present := raw[key]
	if !present {
		return def, nil
	}
	if v == nil {
		return def, fmt.Errorf("%s: null is not one of %s", key, enumNames(allowed))
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
	return def, fmt.Errorf("%s: %q is not one of %s", key, s, enumNames(allowed))
}

// enumNames renders the allowed enum values for an error message.
func enumNames[T ~string](allowed []T) string {
	names := make([]string, len(allowed))
	for i, a := range allowed {
		names[i] = string(a)
	}
	return strings.Join(names, ", ")
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

// resolveProxy returns the proxy URL to use for a given target URI. An
// explicit socks5_proxy (websocket.py:44-46 builds a python_socks Proxy,
// bypassing the environment) always wins; otherwise websockets defaults
// proxy=True and selects an environment proxy via get_proxy
// (websockets/proxy.py:97-131). The proxy is re-resolved for EVERY redirect
// destination (item #4): the scheme and the no_proxy match can change.
func (s *Sender) resolveProxy(rawURI string) string {
	if s.cfg.socks5Proxy != "" {
		return s.cfg.socks5Proxy
	}
	return selectEnvProxy(rawURI)
}

// preconnect establishes the TCP (and any proxy tunnel) connection to the
// target under the OUTER connectTimeout budget (websocket.py's 30 s
// websocket_connect timeout), leaving the HTTP/TLS opening handshake to the
// separate 10 s open_timeout phase (item #8). The returned net.Conn is handed
// to gorilla so no further socket work happens under the handshake timeout.
func (r *runner) preconnect(ctx context.Context, rawURI string) (net.Conn, error) {
	u, err := url.Parse(rawURI)
	if err != nil {
		return nil, fmt.Errorf("websocket_uri: %w", err)
	}
	target := hostPort(u)
	proxyURL := r.s.resolveProxy(rawURI)
	if proxyURL == "" {
		// The ctx deadline bounds the dial (the caller sets it to the opening
		// or outer budget as appropriate).
		nd := &net.Dialer{KeepAlive: 30 * time.Second}
		return nd.DialContext(ctx, "tcp", target)
	}
	pu, err := url.Parse(proxyURL)
	if err != nil {
		return nil, fmt.Errorf("proxy: %w", err)
	}
	switch {
	case strings.HasPrefix(pu.Scheme, "socks"):
		// SOCKS proxy: dial the TARGET through the proxy (remote DNS).
		sd, err := newSocks5Dialer(pu.String())
		if err != nil {
			return nil, err
		}
		return sd.DialContext(ctx, "tcp", target)
	case pu.Scheme == "http" || pu.Scheme == "https":
		// HTTP/HTTPS proxy: establish a CONNECT tunnel ourselves, so the TLS
		// hop to an HTTPS proxy is verified here rather than being handed to
		// gorilla (which supports only http:// proxies).
		return connectTunnel(ctx, pu, target)
	default:
		return nil, fmt.Errorf("proxy: unsupported scheme %q", pu.Scheme)
	}
}

// hostPort returns the host:port dial target for a ws/wss URI (default port
// 80/443).
func hostPort(u *url.URL) string {
	host := u.Hostname()
	port := u.Port()
	if port == "" {
		if u.Scheme == "wss" {
			port = "443"
		} else {
			port = "80"
		}
	}
	return net.JoinHostPort(host, port)
}

// connectTunnel dials an HTTP/HTTPS proxy and issues a CONNECT to target,
// returning the established tunnel. An https:// proxy is reached over TLS
// (verified against httpsProxyTLSConfig). The WHOLE exchange is bounded by the
// ctx deadline and by an explicit socket deadline, and the connection is
// closed when ctx is cancelled, so a proxy that withholds its CONNECT response
// cannot block shutdown (item #2). Proxy URL credentials become an HTTP Basic
// Proxy-Authorization header (item #3); it is sent only to the proxy and never
// to the destination.
func connectTunnel(ctx context.Context, proxy *url.URL, target string) (net.Conn, error) {
	secure := proxy.Scheme == "https"
	proxyAddr := proxy.Host
	if proxy.Port() == "" {
		if secure {
			proxyAddr += ":443"
		} else {
			proxyAddr += ":80"
		}
	}
	c, err := (&net.Dialer{}).DialContext(ctx, "tcp", proxyAddr)
	if err != nil {
		return nil, err
	}
	raw := c // closed on cancellation; c may be reassigned to the TLS conn below
	// Abort the exchange if the context is cancelled (e.g. runner stop), and
	// bound it with an explicit deadline as well.
	stopWatch := context.AfterFunc(ctx, func() { _ = raw.Close() })
	defer stopWatch()
	deadline := time.Now().Add(connectTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if err := c.SetDeadline(deadline); err != nil {
		_ = c.Close()
		return nil, err
	}
	if secure {
		cfg := httpsProxyTLSConfig().Clone()
		if cfg.ServerName == "" {
			cfg.ServerName = proxy.Hostname()
		}
		tc := tls.Client(c, cfg)
		if err := tc.HandshakeContext(ctx); err != nil {
			_ = c.Close()
			return nil, err
		}
		c = tc
	}
	headers := http.Header{}
	if proxy.User != nil {
		if pass, ok := proxy.User.Password(); ok {
			token := base64.StdEncoding.EncodeToString([]byte(proxy.User.Username() + ":" + pass))
			headers.Set("Proxy-Authorization", "Basic "+token)
		}
	}
	req := &http.Request{
		Method: "CONNECT",
		URL:    &url.URL{Opaque: target},
		Host:   target,
		Header: headers,
	}
	if err := req.Write(c); err != nil {
		_ = c.Close()
		return nil, err
	}
	br := bufio.NewReader(c)
	resp, err := http.ReadResponse(br, req)
	if err != nil {
		_ = c.Close()
		return nil, err
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_ = c.Close()
		return nil, fmt.Errorf("proxy CONNECT %s: %s", target, resp.Status)
	}
	// Clear the tunnel-exchange deadline; the caller manages I/O deadlines from
	// here on.
	_ = c.SetDeadline(time.Time{})
	if br.Buffered() > 0 {
		return &bufferedConn{Conn: c, r: br}, nil
	}
	return c, nil
}

// bufferedConn preserves any bytes the CONNECT response reader buffered past
// the header.
type bufferedConn struct {
	net.Conn
	r *bufio.Reader
}

func (b *bufferedConn) Read(p []byte) (int, error) { return b.r.Read(p) }

// dialedConn is one established websocket connection plus the RAW net.Conn
// underneath it. gorilla's Conn.SetWriteDeadline only stores a field and
// applies it at the START of each frame write, so cancellation of a blocked
// write must close the raw conn (item #1).
type dialedConn struct {
	ws     *websocket.Conn
	raw    net.Conn
	pongCh chan string
}

// envProxyKeys is the get_proxy priority order per URI security
// (websockets/proxy.py:117-119).
var (
	envProxyKeysPlain = []string{"ws", "socks", "https", "http"}
	envProxyKeysTLS   = []string{"wss", "socks", "https"}
)

// selectEnvProxy reproduces websockets get_proxy (websockets/proxy.py:97-131):
// consult no_proxy/no_proxy bypass first, then pick the first present
// environment proxy in priority order; a "socks" entry beginning with
// "http://" is rewritten to "socks5h://". Returns "" when no proxy applies.
func selectEnvProxy(rawURI string) string {
	u, err := url.Parse(rawURI)
	if err != nil {
		return ""
	}
	host, port := u.Hostname(), u.Port()
	if port == "" {
		if u.Scheme == "wss" {
			port = "443"
		} else {
			port = "80"
		}
	}
	if hostBypassed(host, port) {
		return ""
	}
	keys := envProxyKeysPlain
	if u.Scheme == "wss" {
		keys = envProxyKeysTLS
	}
	for _, k := range keys {
		if v := envValue(k + "_proxy"); v != "" {
			if k == "socks" && strings.HasPrefix(v, "http://") {
				v = "socks5h://" + strings.TrimPrefix(v, "http://")
			}
			return v
		}
	}
	return ""
}

// envValue returns the effective value of a proxy env var following urllib's
// getproxies_environment (urllib/request.py): the lowercase key is checked
// first, and a PRESENT lowercase key wins — including when it is EMPTY, which
// REMOVES the setting even if the uppercase key is set. Only when the
// lowercase key is absent does the uppercase key apply (item #3).
func envValue(key string) string {
	if v, ok := os.LookupEnv(strings.ToLower(key)); ok {
		return v
	}
	return os.Getenv(strings.ToUpper(key))
}

// hostBypassed reproduces urllib's proxy_bypass_environment for a host:port
// (urllib/request.py): a no_proxy value that is ENTIRELY "*" bypasses
// everything (a "*" among other entries does not); an entry may carry
// host:port; a leading-dot or bare domain matches the host and its subdomains
// (item #3).
func hostBypassed(host, port string) bool {
	noProxy := envValue("no_proxy")
	if noProxy == "" {
		return false
	}
	// urllib compares the COMPLETE value to "*" (no trimming), so " * " does
	// NOT bypass an ordinary host (item #5).
	if noProxy == "*" {
		return true
	}
	hostPort := net.JoinHostPort(host, port)
	host = strings.ToLower(host)
	for _, entry := range strings.Split(noProxy, ",") {
		entry = strings.ToLower(strings.TrimSpace(entry))
		if entry == "" {
			continue
		}
		entryHost, entryPort := entry, ""
		if h, p, err := net.SplitHostPort(entry); err == nil {
			entryHost, entryPort = h, p
		}
		if entryPort != "" && entryPort != port && entry != hostPort {
			continue
		}
		dom := strings.TrimPrefix(entryHost, ".")
		if host == dom || strings.HasSuffix(host, "."+dom) {
			return true
		}
	}
	return false
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
		// Python's async_send swallows ordinary transport failures and
		// returns False (websocket.py:106-114), so only an UNEXPECTED error
		// reaches the stats.unexpected_exception handler (websocket.py:383-386).
		if shouldReport(ctx, err) && s.stats != nil {
			s.stats.UnexpectedException(err, "sender", s.makeTags())
		}
		return err
	}
	return nil
}

// shouldReport reports whether a send failure is UNEXPECTED (and so warrants
// stats.unexpected_exception). Expected transport failures, a not-connected
// result, a stopped runner and a cancelled caller are all swallowed, matching
// Python where async_send catches them and returns False.
func shouldReport(ctx context.Context, err error) bool {
	var te *transportError
	if errors.As(err, &te) {
		return false
	}
	if errors.Is(err, errNotConnected) || errors.Is(err, errStopped) {
		return false
	}
	return ctx.Err() == nil
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
// the runner reads outbound requests from. raw is the underlying net.Conn,
// used to force I/O deadlines (gorilla's Conn.SetWriteDeadline cannot
// interrupt an in-flight write).
type liveConn struct {
	ws     *websocket.Conn
	raw    net.Conn
	sendCh chan sendRequest
	closed chan struct{}
	// pongCh receives the payloads of inbound pongs; the heartbeat matches
	// them against the ping it sent (item #5).
	pongCh    chan string
	abortOnce sync.Once
}

// abort irreversibly tears the connection down by closing the RAW socket. A
// closed socket cannot be re-armed by a later gorilla write, so a blocked
// write or read returns promptly and shutdown is bounded (item #1).
func (lc *liveConn) abort() {
	lc.abortOnce.Do(func() {
		if lc.raw != nil {
			_ = lc.raw.Close()
		}
	})
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
	stopped     bool          // set with conn read under mu; blocks late publication
	connectedCh chan struct{} // closed when a connection is established
	backoff     *exponentialBackoff

	// dialFn establishes one connection; a field so tests can drive the
	// reconnect loop without a network (testing/synctest).
	dialFn func() (*dialedConn, error)
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
		dc, err := r.dialFn()
		if err != nil {
			r.s.logf("websocket connection error: %v; retrying", err)
			if !r.sleepBackoff() {
				return
			}
			continue
		}
		established := time.Now()
		lc := &liveConn{ws: dc.ws, raw: dc.raw, sendCh: make(chan sendRequest), closed: make(chan struct{}), pongCh: dc.pongCh}
		if !r.setConnected(lc) {
			// A dial that completed AFTER stop must not be served: stop() saw no
			// published connection and therefore installed no watchdog, so a
			// write here could block shutdown forever (round-4 item #1).
			_ = lc.ws.Close()
			return
		}
		r.s.logf("websocket connected to %s", r.s.cfg.uri)

		_ = r.serve(lc)
		close(lc.closed)
		r.setDisconnected(lc)
		_ = lc.ws.Close()
		r.s.logf("websocket connection closed after %.0f s", time.Since(established).Seconds())

		if time.Since(established) > stableConnectionReset {
			r.backoff.reset()
		}
		if !r.sleepBackoff() {
			return
		}
	}
}

// dial establishes one connection, cancelable by stop, following up to
// maxRedirects supported handshake redirects
// (websockets/asyncio/client.py:448-559).
//
// Phase separation matches Python (item #4): for an EXPLICIT socks5_proxy the
// preconnection is under the outer connectTimeout (30 s) and only the opening
// handshake is under openTimeout; otherwise ONE opening deadline (open_timeout,
// 10 s) covers ordinary TCP, environment-proxy establishment AND the whole
// redirect sequence. The proxy is re-resolved per redirect target (item #4),
// and userinfo is stripped from EVERY dial URL while the logical URI keeps it
// for Authorization (item #7).
func (r *runner) dial() (*dialedConn, error) {
	outerCtx, cancel := context.WithCancel(context.Background())
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

	outerCtx, timeoutCancel := context.WithTimeout(outerCtx, connectTimeout)
	defer timeoutCancel()

	logical := r.s.cfg.uri
	headers := r.s.headers.Clone()
	applyBasicAuth(logical, headers)

	if r.s.cfg.socks5Proxy != "" {
		// Explicit SOCKS proxy: the preconnection gets the full outer budget,
		// then a single opening deadline for the handshake. Redirects are
		// refused for a preexisting socket (redirectTarget).
		preconn, err := r.preconnect(outerCtx, logical)
		if err != nil {
			return nil, err
		}
		openCtx, openCancel := context.WithTimeout(outerCtx, openTimeout)
		defer openCancel()
		pongCh := make(chan string, 16)
		conn, resp, err := r.handshake(openCtx, logical, headers, preconn, pongCh)
		if err == nil {
			return &dialedConn{ws: conn, raw: preconn, pongCh: pongCh}, nil
		}
		_ = preconn.Close()
		if resp != nil {
			_ = resp.Body.Close()
		}
		if _, rerr := redirectTarget(logical, resp, true); rerr != nil {
			return nil, rerr
		}
		return nil, err
	}

	// Ordinary path: ONE opening deadline across establishment and every
	// redirect attempt (Python's open_timeout is not restarted per redirect).
	openCtx, openCancel := context.WithTimeout(outerCtx, openTimeout)
	defer openCancel()

	var lastErr error
	for attempt := 0; attempt < maxRedirects; attempt++ {
		preconn, err := r.preconnect(openCtx, logical)
		if err != nil {
			return nil, err
		}
		pongCh := make(chan string, 16)
		conn, resp, err := r.handshake(openCtx, logical, headers, preconn, pongCh)
		if err == nil {
			return &dialedConn{ws: conn, raw: preconn, pongCh: pongCh}, nil
		}
		lastErr = err
		_ = preconn.Close()
		// A failed handshake returns the HTTP response for a redirect
		// (gorilla does not follow redirects itself).
		next, rerr := redirectTarget(logical, resp, false)
		if resp != nil {
			_ = resp.Body.Close()
		}
		if rerr != nil {
			return nil, rerr
		}
		if next == "" {
			return nil, lastErr
		}
		// Cross-origin redirects drop credentials (websockets:493-510).
		if crossOrigin(logical, next) {
			headers = stripCredentials(headers)
		}
		headers = headers.Clone()
		logical = next
		applyBasicAuth(logical, headers)
	}
	return nil, fmt.Errorf("websocket: more than %d redirects", maxRedirects)
}

// handshake runs the HTTP upgrade (and TLS for wss) over the preconnected
// socket. The OPENING DEADLINE comes from ctx (gorilla's HandshakeTimeout is
// disabled so a shared/redirect-wide deadline is not restarted per attempt).
// The dial URL is the logical URI with userinfo stripped (item #7).
func (r *runner) handshake(ctx context.Context, logical string, headers http.Header, preconn net.Conn, pongCh chan string) (*websocket.Conn, *http.Response, error) {
	d := &websocket.Dialer{
		TLSClientConfig: r.s.tlsCfg,
		NetDialContext: func(context.Context, string, string) (net.Conn, error) {
			return preconn, nil
		},
	}
	if r.s.cfg.websocketCompression == wsCompressionDeflate {
		d.EnableCompression = true
	}
	conn, resp, err := d.DialContext(ctx, stripUserinfo(logical), headers)
	if err != nil {
		return nil, resp, err
	}
	if r.s.cfg.websocketCompression == wsCompressionDeflate {
		conn.EnableWriteCompression(true)
	}
	// SetReadLimit bounds the COMPRESSED frame; the decompressed bound is
	// enforced by drain's bounded reader (item #8). The pong handler feeds the
	// heartbeat's matching-pong check (item #5).
	conn.SetReadLimit(inboundLimit)
	conn.SetPongHandler(func(msg string) error {
		select {
		case pongCh <- msg:
		default:
		}
		return conn.SetReadDeadline(time.Now().Add(readWait()))
	})
	if err := conn.SetReadDeadline(time.Now().Add(readWait())); err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	return conn, resp, nil
}

// readWait is the read deadline window: a ping every pingInterval and a pong
// expected within pingTimeout, so a silent peer trips the deadline after
// pingInterval+pingTimeout.
func readWait() time.Duration { return pingInterval + pingTimeout }

// stripUserinfo removes any userinfo from a URI: gorilla rejects a URL with
// userinfo, so the credentials are carried in the Authorization header
// instead (applyBasicAuth) and the dial URL is clean.
func stripUserinfo(rawURI string) string {
	u, err := url.Parse(rawURI)
	if err != nil || u.User == nil {
		return rawURI
	}
	u.User = nil
	return u.String()
}

// applyBasicAuth adds HTTP Basic credentials from the URI userinfo
// (websockets/client.py:111-112), where the username/password are
// URL-unquoted. A username without a password is rejected by websockets'
// parse_uri (uri.py:91-93) at config load; here it is skipped.
func applyBasicAuth(rawURI string, h http.Header) {
	u, err := url.Parse(rawURI)
	if err != nil || u.User == nil {
		return
	}
	user := u.User.Username()
	pass, ok := u.User.Password()
	if !ok {
		return
	}
	token := base64.StdEncoding.EncodeToString([]byte(user + ":" + pass))
	h.Set("Authorization", "Basic "+token)
}

// stripCredentials drops the headers websockets removes on a cross-origin
// redirect (websockets/asyncio/client.py:511-517).
func stripCredentials(h http.Header) http.Header {
	out := http.Header{}
	for k, vs := range h {
		switch strings.ToLower(k) {
		case "authorization", "cookie", "proxy-authorization":
			continue
		}
		out[k] = vs
	}
	return out
}

// redirectTarget returns the redirect target for a handshake response, or ""
// when the response is not a supported redirect (websockets process_redirect,
// websockets/asyncio/client.py:448-517). A non-nil error is a security
// refusal (TLS downgrade, preexisting socket, cross-origin with explicit
// host/port).
func redirectTarget(fromURI string, resp *http.Response, sockPreexisting bool) (string, error) {
	if resp == nil {
		return "", nil
	}
	switch resp.StatusCode {
	case 300, 301, 302, 303, 307, 308:
	default:
		return "", nil
	}
	loc := resp.Header.Get("Location")
	if loc == "" {
		return "", nil
	}
	base, err := url.Parse(fromURI)
	if err != nil {
		return "", nil
	}
	ref, err := url.Parse(loc)
	if err != nil {
		return "", nil
	}
	target := base.ResolveReference(ref)

	// A preexisting socket (the explicit socks5_proxy path) cannot be reused.
	if sockPreexisting {
		return "", fmt.Errorf("websocket: cannot follow redirect to %s with a preexisting socket", target)
	}
	oldSecure := base.Scheme == "wss"
	newSecure := target.Scheme == "wss"
	if oldSecure && !newSecure {
		return "", fmt.Errorf("websocket: cannot follow redirect to non-secure URI %s", target)
	}
	return target.String(), nil
}

// crossOrigin reports whether two URIs differ in scheme/host/port.
func crossOrigin(a, b string) bool {
	ua, errA := url.Parse(a)
	ub, errB := url.Parse(b)
	if errA != nil || errB != nil {
		return true
	}
	return ua.Scheme != ub.Scheme || ua.Host != ub.Host
}

// serve drains inbound while serializing the outbound requests until the
// connection dies, the drain fails, or stop fires.
//
// On a normal stop it performs a graceful closing handshake WHILE the drain
// goroutine is still alive, so the peer's close response can be awaited
// (item #6). On any other exit, or once the close handshake is done or times
// out, it force-closes the raw socket (item #1) so every blocked read/write
// returns and both worker goroutines join.
func (r *runner) serve(lc *liveConn) error {
	readerErr := make(chan error, 1)
	go func() { readerErr <- drain(lc.ws) }()

	stopHB := make(chan struct{})
	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		r.heartbeat(lc, stopHB)
	}()

	var serveErr error
	readerJoined := false
loop:
	for {
		select {
		case <-r.stopCh:
			serveErr = errStopped
			break loop
		case err := <-readerErr:
			readerJoined = true
			serveErr = err
			break loop
		case req := <-lc.sendCh:
			if err := writeFrames(lc.ws, req.frames); err != nil {
				// An ordinary transport failure: Python's async_send would
				// have caught it and returned False (item #12).
				req.result <- &transportError{err}
				serveErr = err
				break loop
			}
			req.result <- nil
		}
	}

	close(stopHB)
	<-hbDone

	if serveErr == errStopped {
		// Graceful closing handshake. drain is still reading, so the peer's
		// close response ends it within closeTimeout (item #6).
		_ = lc.ws.WriteControl(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
			time.Now().Add(closeTimeout),
		)
		select {
		case <-readerErr:
			readerJoined = true
		case <-time.After(closeTimeout):
		}
	}
	// Irreversible teardown: after this no gorilla write can re-arm a
	// deadline and block shutdown (item #1).
	lc.abort()
	if !readerJoined {
		<-readerErr
	}
	return serveErr
}

// heartbeat runs websockets' keepalive (ping_interval=20 s, ping_timeout=20 s):
// it pings every pingInterval and requires a PONG carrying the SAME payload
// within pingTimeout. Matching the payload (rather than accepting any inbound
// traffic as proof of life) means a peer that keeps sending data but never
// pongs is still detected and reconnected (item #5).
func (r *runner) heartbeat(lc *liveConn, stop chan struct{}) {
	t := time.NewTicker(pingInterval)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-lc.closed:
			return
		case <-r.stopCh:
			return
		case <-t.C:
		}
		payload := pingPayload()
		if err := lc.ws.WriteControl(websocket.PingMessage, payload, time.Now().Add(writeWait)); err != nil {
			lc.abort()
			return
		}
		if !r.waitPong(lc, payload, stop) {
			r.s.logf("websocket: no pong within %s; reconnecting", pingTimeout)
			lc.abort()
			return
		}
	}
}

// waitPong waits up to pingTimeout for a pong carrying payload. It returns
// true when the matching pong arrived or the runner is shutting down, and
// false on a real pong timeout.
func (r *runner) waitPong(lc *liveConn, payload []byte, stop chan struct{}) bool {
	timer := time.NewTimer(pingTimeout)
	defer timer.Stop()
	for {
		select {
		case got := <-lc.pongCh:
			if got == string(payload) {
				return true
			}
		case <-timer.C:
			return false
		case <-stop:
			return true
		case <-r.stopCh:
			return true
		}
	}
}

// pingPayload returns a fresh 8-byte ping payload so a pong can be matched to
// its ping.
func pingPayload() []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, rand.Uint64())
	return b
}

// drain reads and discards inbound frames (websocket.py:84-87); its exit
// means the connection is dead. The read deadline (kept fresh by incoming
// frames and pongs) trips after pingInterval+pingTimeout of silence. The
// payload is bounded AFTER decompression: gorilla's SetReadLimit bounds the
// COMPRESSED wire bytes, so a small permessage-deflate frame could otherwise
// expand past the 2 MiB max_size (item #8).
func drain(conn *websocket.Conn) error {
	for {
		_ = conn.SetReadDeadline(time.Now().Add(readWait()))
		typ, r, err := conn.NextReader()
		if err != nil {
			return err
		}
		if typ != websocket.BinaryMessage && typ != websocket.TextMessage {
			continue
		}
		n, err := io.Copy(io.Discard, io.LimitReader(r, inboundLimit+1))
		if err != nil {
			return err
		}
		if n > inboundLimit {
			return errInboundTooLarge
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

// setConnected publishes lc as the live connection. It returns false (and
// publishes nothing) when the runner has already stopped, so a dial that
// completes after stop is rejected rather than served without a watchdog
// (round-4 item #1).
func (r *runner) setConnected(lc *liveConn) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return false
	}
	r.conn = lc
	close(r.connectedCh)
	return true
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

// stop asks the runner to exit and waits for it. It nudges an in-flight write
// with a one-shot write deadline so the common case unblocks fast, but that
// deadline is reversible (gorilla re-arms it at the start of the next frame
// write), so a watchdog INDEPENDENT of the serve loop irreversibly aborts the
// connection after closeTimeout even if writeFrames never returns (item #1).
// The socket stays open long enough for serve to perform its graceful closing
// handshake in the common case (item #6).
func (r *runner) stop() {
	r.stopOnce.Do(func() {
		r.mu.Lock()
		// Mark stopped and close stopCh together with reading conn, so a dial
		// completing concurrently either was published (and gets the watchdog)
		// or is rejected by setConnected (round-4 item #1).
		r.stopped = true
		close(r.stopCh)
		lc := r.conn
		r.mu.Unlock()
		if lc == nil {
			return
		}
		if lc.raw != nil {
			_ = lc.raw.SetWriteDeadline(time.Now())
		}
		watchdog := closeTimeout // read on the caller's goroutine, see below
		go func() {
			timer := time.NewTimer(watchdog)
			defer timer.Stop()
			select {
			case <-timer.C:
				lc.abort()
			case <-r.doneCh:
			}
		}()
	})
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

// socks5ProxyDialer dials the target through a SOCKS5 proxy. The target
// hostname is sent to the PROXY for remote resolution: Python's
// aiohttp_socks Proxy (backed by python_socks) defaults SOCKS5 to remote DNS
// (python_socks/_connectors/socks5_async.py:20 sets rdns=True), and x/net's
// SOCKS5 dialer likewise passes the name to the proxy.
type socks5ProxyDialer struct {
	inner   proxy.Dialer
	timeout time.Duration
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
	return &socks5ProxyDialer{inner: inner, timeout: connectTimeout}, nil
}

// DialContext dials addr under min(ctx, timeout); addr's hostname is passed
// through to the proxy unchanged (remote DNS).
func (d *socks5ProxyDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()
	if cd, ok := d.inner.(proxy.ContextDialer); ok {
		return cd.DialContext(ctx, network, addr)
	}
	return d.inner.Dial(network, addr)
}
