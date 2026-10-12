// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

// Package httpx is the shared HTTP client plumbing for the raw-HTTP
// senders (logplex, elasticsearch/opensearch; GCL joins later). It owns
// three things that were previously duplicated per sender:
//
//   - client construction (transport, TLS config, optional dial hook);
//   - proxy selection, PARAMETERIZED per sender: ProxySelector is a
//     transport-level strategy so a sender can plug in the semantics of
//     ITS actual Python client. The requests-compatible port
//     (RequestsProxy) is one implementation and is what both logplex and
//     elasticsearch/opensearch use, because both Python senders talk to
//     the server through requests (logplex.py builds a requests.Session;
//     elasticsearch_opensearch_sender.py calls
//     journalpump.util.get_requests_session, also requests — NOT
//     elasticsearch-py/urllib3). GCL uses Httplib2Proxy: its Python client is
//     googleapiclient.discovery.build, whose default HTTP object wraps
//     httplib2.Http (google-auth-httplib2), so its proxy semantics are
//     httplib2's, not requests'.
//   - request timing: Do applies an optional per-request context
//     deadline/cancellation, and the requests-compatible senders use an
//     INACTIVITY timeout (the deadline resets on every read/write).
//     InactivityClient applies that timeout per REQUEST and per PHASE, so a
//     reused keep-alive connection carries the timeout of the request
//     currently in flight rather than the one that dialed it. Connection
//     establishment (dial plus TLS handshake) is bounded by its own phase,
//     the response-read deadline is armed only after the request is written
//     (a progressing upload that outlasts the timeout is not aborted), and
//     the phase is reset for EVERY request attempt, so a redirected upload
//     re-enters the upload phase rather than inheriting the previous
//     attempt's read deadline; RequestTimeout supplies a FALLBACK for requests
//     that carry no per-request value (GCL's oauth2 token fetches). Senders
//     whose Python client uses a socket timeout use one of these so a
//     slow-but-progressing response is not aborted the way a total deadline
//     would abort it; IdleTimeoutDialer is the fixed-dial variant for a sender
//     with a constant session timeout whose deadline may never be reused
//     across requests (logplex).
//
// It deliberately provides NO retry hook: retries are owned by the shared
// sender.Loop (a failed SendMessages retains the batch and retries after
// a classified backoff). Adding a retry layer here would duplicate and
// conflict with that contract.
package httpx

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

// Doer is the HTTP client seam (Python's requests.Session); tests inject
// a recording fake.
type Doer interface {
	Do(req *http.Request) (*http.Response, error)
}

// ProxySelector is the http.Transport.Proxy strategy type. A sender
// passes the selector that matches its Python client's proxy semantics.
type ProxySelector func(*http.Request) (*url.URL, error)

// ClientOptions configures NewClient. Zero values are safe: a nil Proxy
// selects no proxying (the transport default), a nil TLSClientConfig
// uses the system defaults, and a nil DialContext uses the standard
// net.Dialer.
type ClientOptions struct {
	// Proxy is the proxy-selection strategy; see ProxySelector.
	Proxy ProxySelector
	// TLSClientConfig is the TLS configuration (may be nil).
	TLSClientConfig *tls.Config
	// DialContext overrides connection establishment (e.g. the logplex
	// sender's inactivity-timeout conn wrapper); nil uses the default
	// dialer.
	DialContext func(ctx context.Context, network, addr string) (net.Conn, error)
	// RequestTimeout is the FALLBACK per-request inactivity timeout used by
	// NewInactivityClient when a request carries no WithRequestTimeout value
	// on its context (e.g. oauth2 token fetches, which build their own
	// request). Zero means no fallback.
	RequestTimeout time.Duration
}

// NewClient builds an *http.Client with the given transport options. The
// transport bounds idle pooled connections (IdleConnTimeout = 90 s,
// net/http's DefaultTransport value) so a long-lived client cannot
// accumulate sockets/goroutines indefinitely; callers still release them
// eagerly via CloseIdleConnections.
func NewClient(opts ClientOptions) *http.Client {
	transport := &http.Transport{
		Proxy:           opts.Proxy,
		TLSClientConfig: opts.TLSClientConfig,
		DialContext:     opts.DialContext,
		IdleConnTimeout: 90 * time.Second,
	}
	return &http.Client{Transport: transport}
}

// timeoutKey carries a per-request inactivity timeout through the request
// context to the dialer installed by IdleTimeoutDialer and to
// InactivityClient.Do (net/http passes the request context to
// Transport.DialContext, and httpx.Do builds the request with this context).
type timeoutKey struct{}

// WithRequestTimeout records the per-request inactivity timeout for the
// client built by InactivityClient (per request, including reused
// connections) or, for the fixed-dial IdleTimeoutDialer, the value present
// when a connection is dialed. A non-positive timeout leaves the context
// unchanged (InactivityClient keeps the previous value; the dialer's fallback
// applies).
func WithRequestTimeout(ctx context.Context, timeout time.Duration) context.Context {
	if timeout <= 0 {
		return ctx
	}
	return context.WithValue(ctx, timeoutKey{}, timeout)
}

// IdleTimeoutConn applies an INACTIVITY deadline: the read/write deadline is
// reset before every operation, so a stalled peer trips the timeout while a
// slow-but-progressing transfer does not — requests' socket-timeout
// semantics, not a single total request deadline.
type IdleTimeoutConn struct {
	net.Conn
	idle time.Duration
}

// Read resets the read deadline, then reads.
func (c *IdleTimeoutConn) Read(b []byte) (int, error) {
	_ = c.SetReadDeadline(time.Now().Add(c.idle))
	return c.Conn.Read(b)
}

// Write resets the write deadline, then writes.
func (c *IdleTimeoutConn) Write(b []byte) (int, error) {
	_ = c.SetWriteDeadline(time.Now().Add(c.idle))
	return c.Conn.Write(b)
}

// IdleTimeoutDialer returns a Transport.DialContext that dials with
// `fallback` as the connect timeout and wraps each connection with an
// inactivity deadline. A per-request timeout recorded by WithRequestTimeout
// overrides the fallback for connections dialed by that request.
func IdleTimeoutDialer(fallback time.Duration) func(ctx context.Context, network, addr string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		idle := fallback
		if v, ok := ctx.Value(timeoutKey{}).(time.Duration); ok && v > 0 {
			idle = v
		}
		dialer := &net.Dialer{Timeout: idle}
		conn, err := dialer.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		return &IdleTimeoutConn{Conn: conn, idle: idle}, nil
	}
}

// InactivityClient wraps an *http.Client so requests' per-request INACTIVITY
// timeout is applied at each REQUEST boundary and to the correct PHASE, not
// once at connection-dial time. net/http resolves a transport's DialContext
// once per connection and keeps a background readLoop blocked on pooled
// connections, so a dial-baked deadline leaks across requests: a bulk POST
// reusing the version/alias connection would inherit the ordinary timeout
// (and a later ordinary request the bulk timeout), and a pooled read would
// measure inactivity from before the next request.
//
// The timeout for the request in flight is carried on its context
// (WithRequestTimeout). The client tracks, under one mutex:
//
//   - the current idle timeout, updated at the request boundary;
//   - the current PHASE (connect, upload, or response-read). The read
//     deadline is armed while establishing the connection (dial plus TLS
//     handshake) so a peer that accepts TCP but never answers the handshake
//     is bounded, kept armed during response reading (matching urllib3 arming
//     its read timeout only AFTER sending the request), and NOT armed during
//     the upload, so a progressing upload that outlasts the timeout cannot
//     fail before response reading should begin;
//   - the live wrapped connections, so a request boundary re-arms a pooled
//     connection's PENDING background read (a just-armed deadline must not be
//     overwritten by a read that already loaded the previous timeout).
//
// The phase is reset for EVERY request attempt through the httptrace hooks
// (GotConn enters the upload phase, WroteRequest enters the read phase), not
// once per Client.Do: a redirect issues additional attempts that would
// otherwise carry the first attempt's read phase into a redirected (possibly
// long) upload.
//
// Every load/install of a deadline happens under the mutex together, so a
// background read cannot interleave between "read the timeout" and "install
// the deadline". The actual blocking read/write happens outside the mutex.
// Requests must be issued sequentially (the sender.Loop contract): concurrent
// requests would share one deadline.
type InactivityClient struct {
	client *http.Client

	// fallback is the inactivity timeout applied when a request carries no
	// WithRequestTimeout value (e.g. oauth2 token fetches).
	fallback time.Duration

	mu    sync.Mutex
	idle  time.Duration
	phase connPhase
	conns map[*dynamicIdleConn]struct{}
}

// inactivityTransport is the phase-aware http.RoundTripper used by
// InactivityClient (see its comment).
type inactivityTransport struct {
	base  http.RoundTripper
	owner *InactivityClient
}

var _ http.RoundTripper = (*inactivityTransport)(nil)

// connPhase is the phase of the request attempt currently in flight. The read
// deadline is armed in phaseConnect (connection establishment) and phaseRead
// (response reading) but NOT in phaseUpload, where a progressing request body
// may legitimately outlast the inactivity timeout.
type connPhase int

const (
	phaseConnect connPhase = iota
	phaseUpload
	phaseRead
)

// NewInactivityClient builds an InactivityClient with the given transport
// options, mirroring NewClient (idle pooled connections are bounded at 90 s).
// opts.RequestTimeout, when non-zero, is the fallback inactivity timeout for
// requests that carry no WithRequestTimeout value (see inactivityTransport).
func NewInactivityClient(opts ClientOptions) *InactivityClient {
	c := &InactivityClient{conns: map[*dynamicIdleConn]struct{}{}, fallback: opts.RequestTimeout}
	base := &http.Transport{
		Proxy:           opts.Proxy,
		TLSClientConfig: opts.TLSClientConfig,
		IdleConnTimeout: 90 * time.Second,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			c.mu.Lock()
			idle := c.idle
			c.mu.Unlock()
			dialer := &net.Dialer{Timeout: idle}
			conn, err := dialer.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			wrapped := &dynamicIdleConn{Conn: conn, owner: c}
			c.mu.Lock()
			c.conns[wrapped] = struct{}{}
			// Bound connection establishment: a freshly dialed connection may
			// still be mid TLS handshake, whose reads happen before GotConn
			// (and thus before any WroteRequest). Arm the read deadline now so
			// a peer that accepts TCP but never answers the handshake cannot
			// stall the request; GotConn clears it when the connection is
			// ready and the upload phase begins.
			if idle > 0 {
				_ = conn.SetReadDeadline(time.Now().Add(idle))
			}
			c.mu.Unlock()
			return wrapped, nil
		},
	}
	c.client = &http.Client{Transport: &inactivityTransport{base: base, owner: c}}
	return c
}

// inactivityTransport installs the per-request inactivity timeout at the
// RoundTrip boundary — ONE request attempt — rather than at connection-dial
// time. A fixed-dial deadline (IdleTimeoutConn) is armed when net/http's
// background read on an IDLE pooled connection begins, so a request that
// reuses that connection shortly before the deadline would inherit the
// remaining time instead of a fresh socket-operation timeout and could fail a
// healthy send; clearing the deadline here (beginRequest) and arming it only
// in the read phase (WroteRequest) gives every attempt a fresh budget.
func (t *inactivityTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	idle := t.owner.fallback
	if v, ok := req.Context().Value(timeoutKey{}).(time.Duration); ok && v > 0 {
		idle = v
	}
	t.owner.beginRequest(idle)
	if idle <= 0 {
		return t.base.RoundTrip(req)
	}
	// The httptrace hooks re-enter the upload phase for EVERY actual request
	// attempt (GotConn) and arm the read deadline when an attempt has been
	// fully written (WroteRequest), so a redirect re-enters the upload phase
	// instead of inheriting the first attempt's read phase.
	trace := &httptrace.ClientTrace{
		GotConn:      func(httptrace.GotConnInfo) { t.owner.beginUpload() },
		WroteRequest: func(httptrace.WroteRequestInfo) { t.owner.beginRead() },
	}
	return t.base.RoundTrip(req.WithContext(httptrace.WithClientTrace(req.Context(), trace)))
}

// Client returns the wrapped *http.Client (its Transport is the phase-aware
// inactivityTransport, so callers may also use it directly as an
// oauth2.HTTPClient or option.WithHTTPClient target).
func (c *InactivityClient) Client() *http.Client { return c.client }

// Transport exposes the underlying *http.Transport for proxy/TLS inspection.
func (c *InactivityClient) Transport() *http.Transport {
	it, ok := c.client.Transport.(*inactivityTransport)
	if !ok {
		return nil
	}
	tr, _ := it.base.(*http.Transport)
	return tr
}

// CloseIdleConnections forwards to the underlying transport so
// http.Client.CloseIdleConnections releases pooled connections.
func (t *inactivityTransport) CloseIdleConnections() {
	if ci, ok := t.base.(interface{ CloseIdleConnections() }); ok {
		ci.CloseIdleConnections()
	}
}

// Do delegates to the wrapped client; the phase-aware transport applies the
// per-request timeout (WithRequestTimeout) or the fallback.
func (c *InactivityClient) Do(req *http.Request) (*http.Response, error) {
	return c.client.Do(req)
}

// beginRequest enters the CONNECT phase for a new request: it installs the
// request's timeout and CLEARS the read deadline on every connection, so a
// pending pooled read does not retain a stale deadline. The clear+install
// happen under the mutex so a concurrent wrapped Read/Write cannot overwrite
// the new state.
func (c *InactivityClient) beginRequest(idle time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.idle = idle
	c.phase = phaseConnect
	c.clearReadDeadlines()
}

// beginUpload enters the UPLOAD phase once a connection has been acquired for
// a request attempt (GotConn, which fires for every attempt including a
// redirect). It clears any read deadline armed for connection establishment
// and any left over from a previous attempt, so a progressing upload is never
// aborted by a read timeout.
func (c *InactivityClient) beginUpload() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.phase = phaseUpload
	c.clearReadDeadlines()
}

// beginRead enters the RESPONSE-READ phase: it arms the read deadline
// (inactivity) on every connection, matching urllib3 setting the socket
// timeout after the request is sent.
func (c *InactivityClient) beginRead() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.phase = phaseRead
	if c.idle <= 0 {
		return
	}
	deadline := time.Now().Add(c.idle)
	for conn := range c.conns {
		_ = conn.SetReadDeadline(deadline)
	}
}

// clearReadDeadlines zeroes the read deadline on every live connection. The
// caller must hold c.mu.
func (c *InactivityClient) clearReadDeadlines() {
	for conn := range c.conns {
		_ = conn.SetReadDeadline(time.Time{})
	}
}

// CloseIdleConnections releases pooled connections (see Sender.Close).
func (c *InactivityClient) CloseIdleConnections() { c.client.CloseIdleConnections() }

// dynamicIdleConn applies the client's current inactivity deadline before
// every operation. The read deadline is armed ONLY in the read phase, so a
// progressing upload is never aborted by a read timeout, and every deadline
// install happens under the owner's mutex so a request-boundary update cannot
// be overwritten by a read that loaded the previous timeout.
type dynamicIdleConn struct {
	net.Conn
	owner *InactivityClient
}

func (c *dynamicIdleConn) Read(b []byte) (int, error) {
	c.owner.mu.Lock()
	if c.owner.phase == phaseRead && c.owner.idle > 0 {
		_ = c.SetReadDeadline(time.Now().Add(c.owner.idle))
	}
	c.owner.mu.Unlock()
	return c.Conn.Read(b)
}

func (c *dynamicIdleConn) Write(b []byte) (int, error) {
	c.owner.mu.Lock()
	if c.owner.idle > 0 {
		_ = c.SetWriteDeadline(time.Now().Add(c.owner.idle))
	}
	c.owner.mu.Unlock()
	return c.Conn.Write(b)
}

func (c *dynamicIdleConn) Close() error {
	c.owner.mu.Lock()
	delete(c.owner.conns, c)
	c.owner.mu.Unlock()
	return c.Conn.Close()
}

// Do runs one HTTP request under a per-request deadline (when timeout >
// 0) and returns the status and the fully-read body. It is the shared
// implementation of the elasticsearch sender's request path; senders
// whose Python client relies on socket inactivity timeouts (logplex) do
// not use it, so their timing semantics are untouched.
func Do(ctx context.Context, client Doer, method, target string, body []byte, headers map[string]string, timeout time.Duration) (int, []byte, error) {
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, target, reader)
	if err != nil {
		return 0, nil, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, data, nil
}

// RequestsProxy reproduces requests.Session's proxy resolution for one
// request (requests.utils.resolve_proxies + select_proxy over
// urllib.request.getproxies). It deliberately does NOT use
// net/http.ProxyFromEnvironment (httpproxy): that helper gives uppercase
// variables precedence, ignores the ALL_PROXY fallback, and
// unconditionally bypasses loopback addresses — three differences from
// requests.
func RequestsProxy(req *http.Request) (*url.URL, error) {
	u := req.URL
	if u == nil || u.Hostname() == "" {
		return nil, nil
	}
	if ShouldBypassProxies(u) {
		return nil, nil
	}
	proxies := ProxyEnvironment()
	proxy := proxies[u.Scheme]
	if proxy == "" {
		proxy = proxies["all"]
	}
	if proxy == "" {
		return nil, nil
	}
	return ParseProxyURL(proxy)
}

// ParseProxyURL normalizes a proxy address the way requests does before
// building the connection: a scheme-less address (e.g. HTTP_PROXY=
// "proxy.example:8080") gets an "http://" scheme prepended
// (utils.prepend_scheme_if_needed), and the result must have a host.
// requests accepts these addresses; net/url would otherwise parse
// "proxy.example:8080" as an opaque URL with no usable host.
func ParseProxyURL(proxy string) (*url.URL, error) {
	if !strings.Contains(proxy, "://") {
		if strings.HasPrefix(proxy, "//") {
			proxy = "http:" + proxy
		} else {
			proxy = "http://" + proxy
		}
	}
	u, err := url.Parse(proxy)
	if err != nil {
		return nil, fmt.Errorf("proxy %q: %w", proxy, err)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("proxy %q has no host", proxy)
	}
	return u, nil
}

// ShouldBypassProxies mirrors requests.utils.should_bypass_proxies in
// full. It is TWO OR-ed layers with DIFFERENT sources and matching rules,
// and conflating them is wrong:
//
//  1. requests' own no_proxy loop (only when `get_proxy("no_proxy")`, i.e.
//     a lowercase value or the uppercase fallback, is non-empty). It is
//     CASE-SENSITIVE and a PLAIN suffix: for a hostname, the RAW entry must
//     suffix-match the lowercased host or host:port with no label boundary;
//     for an IP host, CIDR and plain-IP matching apply. Space characters are
//     stripped and empty entries skipped.
//  2. urllib's proxy_bypass_environment fallback (requests' `set_environ(
//     "no_proxy", None)` leaves the environment untouched, so it is NOT a
//     no-op). Its list comes from getproxies_environment()["no"] — a DIFFERENT
//     source: an empty lowercase no_proxy REMOVES the entry, so `no_proxy=""`
//     with `NO_PROXY="*"` does NOT bypass everything. Matching here IS
//     case-insensitive and label-boundary aware (hostonly == name or a
//     ".name" suffix), with exact `== "*"` bypassing all.
//
// Loopback targets are NOT special-cased: they are proxied unless no_proxy
// matches.
func ShouldBypassProxies(u *url.URL) bool {
	host := u.Hostname()
	if host == "" {
		return true
	}
	hostLower := strings.ToLower(host)
	hostWithPort := hostLower
	if port := u.Port(); port != "" {
		hostWithPort = hostLower + ":" + port
	}

	// Layer 1: requests' own case-sensitive, plain-suffix loop.
	firstNoProxy := os.Getenv("no_proxy")
	if firstNoProxy == "" {
		firstNoProxy = os.Getenv("NO_PROXY")
	}
	if firstNoProxy != "" {
		entries := SplitNoProxyEntries(firstNoProxy)
		if ip := net.ParseIP(hostLower); ip != nil {
			for _, entry := range entries {
				if _, network, err := net.ParseCIDR(entry); err == nil {
					if network.Contains(ip) {
						return true
					}
					continue
				}
				if other := net.ParseIP(entry); other != nil && other.Equal(ip) {
					return true
				}
			}
		} else {
			for _, entry := range entries {
				if strings.HasSuffix(hostLower, entry) || strings.HasSuffix(hostWithPort, entry) {
					return true
				}
			}
		}
	}

	// Layer 2: urllib's case-insensitive, label-boundary fallback, fed by
	// ProxyEnvironment()["no"] (empty lowercase removes the entry).
	fallback, present := ProxyEnvironment()["no"]
	if !present || fallback == "" {
		return false
	}
	if fallback == "*" {
		return true
	}
	for _, entry := range SplitFallbackEntries(fallback) {
		name := strings.ToLower(strings.TrimLeft(entry, "."))
		if name == "" {
			continue
		}
		if hostLower == name || strings.HasSuffix(hostLower, "."+name) {
			return true
		}
	}
	return false
}

// Httplib2Proxy reproduces httplib2's proxy selection
// (proxy_info_from_environment / proxy_info_from_url / ProxyInfo.bypass_host),
// which is what the Google Cloud Logging sender's Python client uses: the
// GCL sender is built with googleapiclient.discovery.build, whose default HTTP
// object is google_auth_httplib2.AuthorizedHttp wrapping httplib2.Http, and
// httplib2.Http defaults proxy_info to proxy_info_from_environment.
//
// Differences from the requests port (RequestsProxy) are deliberate and
// quirk-locked:
//
//   - the proxy variable is <scheme>_proxy for the REQUEST scheme (https_proxy
//     for an https target, http_proxy otherwise); a lowercase variable that is
//     present but EMPTY disables proxying for that scheme (it does NOT fall
//     back to the uppercase twin);
//   - no_proxy is read lowercase-then-uppercase; a value of exactly "*" bypasses
//     ALL hosts; otherwise it is split on commas with EMPTY entries dropped but
//     NOT whitespace-stripped;
//   - bypass matching is CASE-SENSITIVE and operates on the bare hostname
//     (no port). A skip entry ending in "."-prefix form matches the host as a
//     dotted suffix; otherwise it must equal the host exactly. Loopback is not
//     special-cased;
//   - the proxy is only consulted for an http/https DESTINATION: httplib2's
//     proxy_info_from_environment returns None for any other method;
//   - the proxy PORT is defaulted from the DESTINATION scheme, not the proxy
//     URL's own scheme (httplib2: url.port or dict(https=443, http=80)[method]),
//     so an http:// proxy with no port used for an https target defaults to 443;
//   - the proxy URL's scheme selects the transport: socks4 → SOCKS4,
//     socks5/socks → SOCKS5, and EVERYTHING ELSE (http, https, and any unknown
//     scheme) → a plain HTTP proxy (CONNECT for an https target). An https://
//     proxy URL is therefore NOT a TLS-to-proxy connection; it is an ordinary
//     HTTP CONNECT proxy.
//
// The returned URL is NORMALIZED for Go's http.Transport (see
// normalizeHttplib2Proxy): scheme mapped to http/socks5 and the port resolved.
// One residual deviation: Go's transport has no SOCKS4 dialer, so a socks4://
// proxy is REJECTED with an error (rather than returned as-is, which Go would
// treat as a plain HTTP proxy and could drive a CONNECT handshake) where
// httplib2 (via PySocks) would use SOCKS4 — documented in charter §2.
//
// Note the reference environment: httplib2 only uses a proxy when PySocks is
// importable (ProxyInfo.isgood() is `socks and ...`), so a deployment without
// PySocks silently bypasses every configured proxy. That is an environment
// property, not a selection rule, and is not reproduced here.
func Httplib2Proxy(req *http.Request) (*url.URL, error) {
	u := req.URL
	if u == nil || u.Hostname() == "" {
		return nil, nil
	}
	// proxy_info_from_environment only handles http/https methods.
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, nil
	}
	if httplib2Bypass(u.Hostname()) {
		return nil, nil
	}
	proxy := envLowerThenUpper(u.Scheme + "_proxy")
	if proxy == "" {
		return nil, nil
	}
	parsed, err := url.Parse(proxy)
	if err != nil {
		return nil, fmt.Errorf("proxy %q: %w", proxy, err)
	}
	return normalizeHttplib2Proxy(parsed, u.Scheme)
}

// normalizeHttplib2Proxy maps a parsed proxy URL to httplib2's effective
// proxy (proxy_info_from_url): the destination scheme supplies the default
// port and the proxy URL scheme selects the proxy type.
func normalizeHttplib2Proxy(parsed *url.URL, destScheme string) (*url.URL, error) {
	if parsed.Hostname() == "" {
		return nil, fmt.Errorf("proxy %q has no host", parsed.String())
	}
	port := parsed.Port()
	if port == "" {
		if destScheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}
	out := *parsed
	switch parsed.Scheme {
	case "socks4":
		// Go's http.Transport implements SOCKS5 only: handed a "socks4" URL it
		// would fall back to a plain HTTP-proxy path and could emit a CONNECT
		// handshake rather than the SOCKS4 negotiation httplib2/PySocks would
		// perform. Reject it explicitly so no connection is attempted, instead
		// of silently producing the wrong protocol.
		return nil, fmt.Errorf("proxy scheme %q is not supported (Go has no SOCKS4 dialer)", parsed.Scheme)
	case "socks5", "socks":
		out.Scheme = "socks5"
	default:
		// http, https, and any unknown scheme are all a plain HTTP proxy.
		out.Scheme = "http"
	}
	out.Host = net.JoinHostPort(parsed.Hostname(), port)
	return &out, nil
}

// envLowerThenUpper mirrors httplib2's os.environ.get(lower, os.environ.get(UPPER)):
// the lowercase variable wins when PRESENT, even when empty (an empty value is
// falsy in the caller and disables that proxy).
func envLowerThenUpper(name string) string {
	if v, ok := os.LookupEnv(name); ok {
		return v
	}
	return os.Getenv(strings.ToUpper(name))
}

// httplib2Bypass ports ProxyInfo.bypass_host fed by the no_proxy handling in
// proxy_info_from_url.
func httplib2Bypass(host string) bool {
	noProxy := envLowerThenUpper("no_proxy")
	if noProxy == "*" {
		return true
	}
	if strings.TrimSpace(noProxy) == "" {
		return false
	}
	hostname := "." + strings.TrimLeft(host, ".")
	for _, entry := range strings.Split(noProxy, ",") {
		if entry == "" {
			continue
		}
		if strings.HasPrefix(entry, ".") {
			if strings.HasSuffix(hostname, entry) {
				return true
			}
			continue
		}
		if hostname == "."+entry {
			return true
		}
	}
	return false
}

// SplitNoProxyEntries mirrors requests' no_proxy preprocessing for LAYER 1:
// strip ALL ASCII spaces, split on commas, drop empty entries (tabs and other
// whitespace are NOT removed here).
func SplitNoProxyEntries(noProxy string) []string {
	var entries []string
	for _, entry := range strings.Split(strings.ReplaceAll(noProxy, " ", ""), ",") {
		if entry != "" {
			entries = append(entries, entry)
		}
	}
	return entries
}

// SplitFallbackEntries mirrors urllib's proxy_bypass_environment preprocessing
// for LAYER 2: split on commas, then strip each entry's leading/trailing
// whitespace (Python str.strip(): space, \t, \n, \r, \f, \v — TrimSpace also
// covers these), dropping empty entries.
func SplitFallbackEntries(noProxy string) []string {
	var entries []string
	for _, entry := range strings.Split(noProxy, ",") {
		entry = strings.TrimSpace(entry)
		if entry != "" {
			entries = append(entries, entry)
		}
	}
	return entries
}

// ProxyEnvironment builds the scheme -> proxy map like urllib's
// getproxies_environment: every *_proxy variable contributes under its
// lowercase scheme name, and a LOWERCASE variable wins over its uppercase
// twin (an empty lowercase value removes the entry). The second pass inspects
// the ORIGINAL name, so an uppercase *_PROXY never overrides a lowercase one.
func ProxyEnvironment() map[string]string {
	proxies := make(map[string]string)
	for _, kv := range os.Environ() {
		name, value, ok := strings.Cut(kv, "=")
		if !ok {
			continue
		}
		lower := strings.ToLower(name)
		if strings.HasSuffix(lower, "_proxy") && value != "" {
			proxies[strings.TrimSuffix(lower, "_proxy")] = value
		}
	}
	for _, kv := range os.Environ() {
		name, value, ok := strings.Cut(kv, "=")
		if !ok || !strings.HasSuffix(name, "_proxy") {
			continue
		}
		key := strings.TrimSuffix(strings.ToLower(name), "_proxy")
		if value != "" {
			proxies[key] = value
		} else {
			delete(proxies, key)
		}
	}
	return proxies
}
