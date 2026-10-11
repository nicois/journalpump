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
//     elasticsearch-py/urllib3). GCL, whose Python client is
//     google-cloud-logging, will be parameterized separately.
//   - request timing: Do applies an optional per-request context
//     deadline/cancellation, and the requests-compatible senders use an
//     INACTIVITY timeout (the deadline resets on every read/write).
//     InactivityClient applies that timeout per REQUEST, so a reused
//     keep-alive connection carries the timeout of the request currently in
//     flight rather than the one that dialed it; IdleTimeoutDialer is the
//     fixed-dial variant for a sender with a constant session timeout
//     (logplex). Senders whose Python client uses requests' socket timeout
//     use one of these so a slow-but-progressing response is not aborted
//     the way a total deadline would abort it.
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
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
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
// timeout is applied at each REQUEST boundary, not once at connection-dial
// time. net/http resolves a transport's DialContext once per connection and
// keeps a background readLoop blocked on pooled connections, so a
// dial-baked deadline leaks across requests: a bulk POST reusing the
// version/alias connection would inherit the ordinary timeout (and a later
// ordinary request the bulk timeout), and a pooled read would measure
// inactivity from before the next request.
//
// The timeout for the request in flight is carried on its context
// (WithRequestTimeout). Do copies it to a shared value every wrapped
// connection consults before each read/write, and re-arms any pooled
// connection's pending background read, so a reused connection uses the
// CURRENT request's timeout. Requests must be issued sequentially (the
// sender.Loop contract): concurrent requests would share one deadline.
type InactivityClient struct {
	client *http.Client
	idle   atomic.Int64
	// conns tracks live wrapped connections so Do can re-arm a pooled
	// connection's pending background read to the current request's timeout.
	conns sync.Map // *dynamicIdleConn -> struct{}
}

// NewInactivityClient builds an InactivityClient with the given transport
// options, mirroring NewClient (idle pooled connections are bounded at 90 s).
func NewInactivityClient(opts ClientOptions) *InactivityClient {
	c := &InactivityClient{}
	c.client = &http.Client{Transport: &http.Transport{
		Proxy:           opts.Proxy,
		TLSClientConfig: opts.TLSClientConfig,
		IdleConnTimeout: 90 * time.Second,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			var idle time.Duration
			if v, ok := ctx.Value(timeoutKey{}).(time.Duration); ok && v > 0 {
				idle = v
			}
			dialer := &net.Dialer{Timeout: idle}
			conn, err := dialer.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			wrapped := &dynamicIdleConn{Conn: conn, idle: &c.idle, owner: c}
			c.conns.Store(wrapped, struct{}{})
			return wrapped, nil
		},
	}}
	return c
}

// Client returns the wrapped *http.Client (for transport inspection in tests).
func (c *InactivityClient) Client() *http.Client { return c.client }

// Do applies the request's inactivity timeout, then delegates to the wrapped
// client.
func (c *InactivityClient) Do(req *http.Request) (*http.Response, error) {
	if v, ok := req.Context().Value(timeoutKey{}).(time.Duration); ok && v > 0 {
		c.idle.Store(int64(v))
		deadline := time.Now().Add(v)
		c.conns.Range(func(k, _ any) bool {
			_ = k.(*dynamicIdleConn).SetReadDeadline(deadline)
			return true
		})
	}
	return c.client.Do(req)
}

// CloseIdleConnections releases pooled connections (see Sender.Close).
func (c *InactivityClient) CloseIdleConnections() { c.client.CloseIdleConnections() }

// dynamicIdleConn applies an inactivity deadline that is re-read from the
// client's shared value before every operation, so a reused connection uses
// the current request's timeout.
type dynamicIdleConn struct {
	net.Conn
	idle  *atomic.Int64
	owner *InactivityClient
}

func (c *dynamicIdleConn) Read(b []byte) (int, error) {
	if d := time.Duration(c.idle.Load()); d > 0 {
		_ = c.SetReadDeadline(time.Now().Add(d))
	}
	return c.Conn.Read(b)
}

func (c *dynamicIdleConn) Write(b []byte) (int, error) {
	if d := time.Duration(c.idle.Load()); d > 0 {
		_ = c.SetWriteDeadline(time.Now().Add(d))
	}
	return c.Conn.Write(b)
}

func (c *dynamicIdleConn) Close() error {
	c.owner.conns.Delete(c)
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
