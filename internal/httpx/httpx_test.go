// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package httpx

import (
	"bufio"
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

// recordingConn is a net.Conn test double that records deadline installs and
// can block a Read until released, so request-boundary interleavings are
// driven deterministically (no sleeps).
type recordingConn struct {
	mu          sync.Mutex
	readDL      []time.Time
	writeDL     []time.Time
	readEntered chan struct{}
	readBlock   chan struct{}
}

func (c *recordingConn) Read(_ []byte) (int, error) {
	if c.readEntered != nil {
		select {
		case <-c.readEntered:
		default:
			close(c.readEntered)
		}
	}
	if c.readBlock != nil {
		<-c.readBlock
	}
	return 0, io.EOF
}

func (c *recordingConn) Write(b []byte) (int, error) { return len(b), nil }
func (c *recordingConn) Close() error                { return nil }

func (c *recordingConn) LocalAddr() net.Addr  { return connAddr{} }
func (c *recordingConn) RemoteAddr() net.Addr { return connAddr{} }
func (c *recordingConn) SetDeadline(t time.Time) error {
	_ = c.SetReadDeadline(t)
	return c.SetWriteDeadline(t)
}

func (c *recordingConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	c.readDL = append(c.readDL, t)
	c.mu.Unlock()
	return nil
}

func (c *recordingConn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	c.writeDL = append(c.writeDL, t)
	c.mu.Unlock()
	return nil
}

type connAddr struct{}

func (connAddr) Network() string { return "fake" }
func (connAddr) String() string  { return "fake" }

func (c *recordingConn) lastRead() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.readDL) == 0 {
		return time.Time{}
	}
	return c.readDL[len(c.readDL)-1]
}

func (c *recordingConn) lastWrite() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.writeDL) == 0 {
		return time.Time{}
	}
	return c.writeDL[len(c.writeDL)-1]
}

func newDeadlineTestClient() (*InactivityClient, *dynamicIdleConn, *recordingConn) {
	fc := &recordingConn{}
	c := &InactivityClient{conns: map[*dynamicIdleConn]struct{}{}}
	conn := &dynamicIdleConn{Conn: fc, owner: c}
	c.conns[conn] = struct{}{}
	return c, conn, fc
}

// TestInactivityClientTimeoutPerPhaseAndBoundary pins review round 3 item 2:
// the read deadline is armed only in the RESPONSE-READ phase, so a
// progressing upload that outlasts the timeout is not aborted before
// response reading begins (urllib3 arms its read timeout after sending), and
// review round 3 item 1: a request boundary replaces the timeout and clears a
// pending read deadline rather than letting a stale one survive.
func TestInactivityClientTimeoutPerPhaseAndBoundary(t *testing.T) {
	c, conn, fc := newDeadlineTestClient()

	// Upload phase (the bulk POST): NO read deadline; the write deadline is
	// armed per write.
	c.beginRequest(300 * time.Millisecond)
	c.beginUpload()
	if got := fc.lastRead(); !got.IsZero() {
		t.Fatalf("read deadline armed during the upload phase: %v", got)
	}
	_, _ = conn.Write([]byte("x"))
	if got := fc.lastWrite(); got.IsZero() {
		t.Fatal("write deadline not armed")
	}

	// Read phase: the read deadline is armed with the current timeout.
	c.beginRead()
	_, _ = conn.Read(make([]byte, 1))
	if d := time.Until(fc.lastRead()); d <= 0 || d > 300*time.Millisecond {
		t.Fatalf("read deadline = %v, want ~300ms", d)
	}

	// Next request boundary (an ordinary request): the shorter timeout
	// replaces the bulk one and the read deadline is cleared for the upload
	// phase. A plain Read during that phase must not re-arm it.
	c.beginRequest(150 * time.Millisecond)
	c.beginUpload()
	if got := fc.lastRead(); !got.IsZero() {
		t.Fatalf("read deadline survived the request boundary: %v", got)
	}
	_, _ = conn.Read(make([]byte, 1))
	if got := fc.lastRead(); !got.IsZero() {
		t.Fatalf("read deadline armed during the next upload phase: %v", got)
	}
	c.beginRead()
	_, _ = conn.Read(make([]byte, 1))
	if d := time.Until(fc.lastRead()); d <= 0 || d > 150*time.Millisecond {
		t.Fatalf("read deadline = %v, want ~150ms (the new timeout, not the old 300ms)", d)
	}
}

// TestInactivityClientBoundaryUpdateWithPendingRead pins review round 3 item 1
// deterministically: a background Read that is already in flight (armed the
// old timeout and blocked inside the syscall) must NOT overwrite the deadline
// installed at a new request boundary. The pending read is driven by channels,
// so the interleaving is fixed rather than timing-dependent.
func TestInactivityClientBoundaryUpdateWithPendingRead(t *testing.T) {
	c, conn, fc := newDeadlineTestClient()
	entered := make(chan struct{})
	release := make(chan struct{})
	fc.readEntered = entered
	fc.readBlock = release

	// Bulk request A: read phase, a background read is pending with 300ms.
	c.beginRequest(300 * time.Millisecond)
	c.beginRead()
	done := make(chan struct{})
	go func() {
		_, _ = conn.Read(make([]byte, 1))
		close(done)
	}()
	<-entered // the pending read has armed 300ms and is blocked in Conn.Read

	// Request boundary B (ordinary): the upload phase clears the read
	// deadline. The pending read must not later re-install its stale 300ms.
	c.beginRequest(150 * time.Millisecond)
	if got := fc.lastRead(); !got.IsZero() {
		t.Fatalf("request-boundary deadline overwritten: got %v, want zeroed read deadline", got)
	}
	close(release)
	<-done
	if got := fc.lastRead(); !got.IsZero() {
		t.Fatalf("pending read re-armed a stale deadline after the boundary update: %v", got)
	}
}

func TestDoReturnsStatusAndBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("x-test"); got != "1" {
			t.Errorf("header x-test = %q", got)
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("hello"))
	}))
	defer srv.Close()

	status, body, err := Do(context.Background(), srv.Client(), http.MethodPost, srv.URL, []byte("payload"), map[string]string{"x-test": "1"}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if status != http.StatusCreated {
		t.Errorf("status = %d", status)
	}
	if string(body) != "hello" {
		t.Errorf("body = %q", body)
	}
}

func TestDoAppliesPerRequestDeadline(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		<-release
	}))
	defer func() { close(release); srv.Close() }()

	start := time.Now()
	_, _, err := Do(context.Background(), srv.Client(), http.MethodGet, srv.URL, nil, nil, 100*time.Millisecond)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected a deadline error")
	}
	if elapsed > 2*time.Second {
		t.Errorf("deadline not applied, took %s", elapsed)
	}
}

func TestRequestsProxyUsesEnvironment(t *testing.T) {
	t.Setenv("http_proxy", "http://proxy.example:8080")
	t.Setenv("no_proxy", "")
	t.Setenv("NO_PROXY", "")

	req, _ := http.NewRequest(http.MethodGet, "http://target.example/", nil)
	u, err := RequestsProxy(req)
	if err != nil {
		t.Fatal(err)
	}
	if u == nil || u.String() != "http://proxy.example:8080" {
		t.Fatalf("proxy = %v, want http://proxy.example:8080", u)
	}

	// A matching no_proxy bypasses the proxy (requests layer 1, plain suffix).
	t.Setenv("no_proxy", "target.example")
	if u, err := RequestsProxy(req); err != nil || u != nil {
		t.Fatalf("no_proxy target not bypassed: %v, %v", u, err)
	}
}

func TestRequestsProxyFallsBackToAllProxy(t *testing.T) {
	t.Setenv("http_proxy", "")
	t.Setenv("no_proxy", "")
	t.Setenv("NO_PROXY", "")
	t.Setenv("ALL_PROXY", "http://all.example:3128")

	req, _ := http.NewRequest(http.MethodGet, "http://target.example/", nil)
	u, err := RequestsProxy(req)
	if err != nil {
		t.Fatal(err)
	}
	if u == nil || u.String() != "http://all.example:3128" {
		t.Fatalf("ALL_PROXY fallback = %v", u)
	}
}

// TestHttplib2Proxy pins httplib2's proxy selection, which is what the GCL
// sender's Python client (googleapiclient + httplib2) uses: scheme-specific
// variables, an empty lowercase variable disabling that scheme (no uppercase
// fallback), no_proxy="*" bypassing everything, case-sensitive bare-hostname
// matching, and no ALL_PROXY fallback.
func TestHttplib2Proxy(t *testing.T) {
	for _, k := range []string{"http_proxy", "HTTP_PROXY", "https_proxy", "HTTPS_PROXY", "no_proxy", "NO_PROXY", "ALL_PROXY", "all_proxy"} {
		t.Setenv(k, "")
	}
	t.Setenv("http_proxy", "http://proxy.example:8080")
	t.Setenv("https_proxy", "http://sproxy.example:8443")
	t.Setenv("ALL_PROXY", "http://all.example:3128")

	httpReq, _ := http.NewRequest(http.MethodGet, "http://target.example/", nil)
	if u, err := Httplib2Proxy(httpReq); err != nil || u == nil || u.String() != "http://proxy.example:8080" {
		t.Fatalf("http proxy = %v, %v (want http://proxy.example:8080, no ALL_PROXY fallback)", u, err)
	}
	httpsReq, _ := http.NewRequest(http.MethodGet, "https://target.example/", nil)
	if u, err := Httplib2Proxy(httpsReq); err != nil || u == nil || u.String() != "http://sproxy.example:8443" {
		t.Fatalf("https proxy = %v, %v", u, err)
	}

	// An empty lowercase variable disables that scheme; the uppercase twin is
	// NOT consulted.
	t.Setenv("HTTP_PROXY", "http://upper.example:9999")
	t.Setenv("http_proxy", "")
	if u, err := Httplib2Proxy(httpReq); err != nil || u != nil {
		t.Fatalf("empty http_proxy = %v, %v (want nil; uppercase ignored)", u, err)
	}

	// no_proxy="*" bypasses every host.
	t.Setenv("no_proxy", "*")
	if u, err := Httplib2Proxy(httpsReq); err != nil || u != nil {
		t.Fatalf("no_proxy=* not bypassed: %v, %v", u, err)
	}

	// Case-sensitive, dotted-suffix matching on the bare hostname.
	t.Setenv("no_proxy", ".example")
	if u, err := Httplib2Proxy(httpsReq); err != nil || u != nil {
		t.Fatalf("no_proxy .example did not bypass: %v, %v", u, err)
	}
	t.Setenv("no_proxy", "TARGET.EXAMPLE")
	if u, err := Httplib2Proxy(httpsReq); err != nil || u == nil {
		t.Fatalf("no_proxy matching is case-sensitive (uppercase entry should NOT bypass): %v, %v", u, err)
	}
}

// TestNormalizeHttplib2Proxy pins httplib2's proxy_info_from_url translation
// (review item 4): the PORT is defaulted from the DESTINATION scheme (not the
// proxy URL's own scheme), and the proxy URL SCHEME selects the transport —
// socks5/socks map to SOCKS5, while http, https and any unknown scheme become
// a plain HTTP proxy; socks4 is REJECTED (Go has no SOCKS4 dialer and would
// otherwise treat it as a plain HTTP proxy).
func TestNormalizeHttplib2Proxy(t *testing.T) {
	cases := []struct {
		name       string
		proxy      string
		destScheme string
		want       string
		wantErr    bool
	}{
		{"omitted_port_https_dest_defaults_443", "http://proxy.example", "https", "http://proxy.example:443", false},
		{"omitted_port_http_dest_defaults_80", "http://proxy.example", "http", "http://proxy.example:80", false},
		{"explicit_port_preserved", "http://proxy.example:3128", "https", "http://proxy.example:3128", false},
		{"https_proxy_is_plain_http", "https://proxy.example:8443", "https", "http://proxy.example:8443", false},
		{"unknown_scheme_is_plain_http", "foo://proxy.example", "https", "http://proxy.example:443", false},
		{"socks5_scheme", "socks5://proxy.example:1080", "https", "socks5://proxy.example:1080", false},
		{"socks_alias_is_socks5", "socks://proxy.example:1080", "https", "socks5://proxy.example:1080", false},
		{"socks4_rejected", "socks4://proxy.example:1080", "https", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := url.Parse(tc.proxy)
			if err != nil {
				t.Fatal(err)
			}
			got, err := normalizeHttplib2Proxy(parsed, tc.destScheme)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("normalize(%q) = %v, want an error", tc.proxy, got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got.String() != tc.want {
				t.Errorf("normalize(%q, %q) = %q, want %q", tc.proxy, tc.destScheme, got.String(), tc.want)
			}
		})
	}
}

// TestHttplib2ProxySocks4Rejected proves a socks4:// proxy is refused at
// SELECTION time, so the transport never attempts a connection (returning the
// URL unchanged would let Go treat it as a plain HTTP proxy and emit a CONNECT
// handshake).
func TestHttplib2ProxySocks4Rejected(t *testing.T) {
	for _, k := range []string{"http_proxy", "HTTP_PROXY", "https_proxy", "HTTPS_PROXY", "no_proxy", "NO_PROXY", "ALL_PROXY", "all_proxy"} {
		t.Setenv(k, "")
	}
	t.Setenv("https_proxy", "socks4://proxy.example:1080")
	req, _ := http.NewRequest(http.MethodGet, "https://target.example/", nil)
	u, err := Httplib2Proxy(req)
	if err == nil {
		t.Fatalf("socks4 proxy = %v, want an error (no connection attempted)", u)
	}
	if u != nil {
		t.Errorf("socks4 proxy URL = %v, want nil", u)
	}
	if !strings.Contains(err.Error(), "SOCKS4") {
		t.Errorf("error = %v, want it to name the unsupported SOCKS4 protocol", err)
	}
}

// proxyRequestLine is a minimal HTTP CONNECT proxy that records the request
// line it received and then blind-tunnels bytes to the requested host. It
// proves the CLIENT performs a real CONNECT handshake, not merely that a URL
// was selected.
type proxyRequestLine struct {
	ln     net.Listener
	mu     sync.Mutex
	lines  []string
	closed bool
}

func newProxyRequestLine(t *testing.T) *proxyRequestLine {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &proxyRequestLine{ln: ln}
	go p.serve()
	t.Cleanup(func() { p.close() })
	return p
}

func (p *proxyRequestLine) addr() string { return p.ln.Addr().String() }

func (p *proxyRequestLine) close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	p.mu.Unlock()
	_ = p.ln.Close()
}

func (p *proxyRequestLine) requests() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.lines...)
}

func (p *proxyRequestLine) serve() {
	for {
		conn, err := p.ln.Accept()
		if err != nil {
			return
		}
		go p.handle(conn)
	}
}

func (p *proxyRequestLine) handle(conn net.Conn) {
	defer func() { _ = conn.Close() }()
	br := bufio.NewReader(conn)
	line, err := br.ReadString('\n')
	if err != nil {
		return
	}
	// Drain the request headers up to the blank line.
	for {
		h, err := br.ReadString('\n')
		if err != nil {
			return
		}
		if h == "\r\n" || h == "\n" {
			break
		}
	}
	p.mu.Lock()
	p.lines = append(p.lines, strings.TrimSpace(line))
	p.mu.Unlock()

	fields := strings.Fields(line)
	if len(fields) < 2 || fields[0] != http.MethodConnect {
		_, _ = conn.Write([]byte("HTTP/1.1 405 Method Not Allowed\r\n\r\n"))
		return
	}
	remote, err := net.Dial("tcp", fields[1])
	if err != nil {
		_, _ = conn.Write([]byte("HTTP/1.1 502 Bad Gateway\r\n\r\n"))
		return
	}
	defer func() { _ = remote.Close() }()
	_, _ = conn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n"))
	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(remote, br); done <- struct{}{} }()
	go func() { _, _ = io.Copy(conn, remote); done <- struct{}{} }()
	<-done
}

// TestHttplib2ProxyHandshake drives a real HTTPS request through
// httpx.Httplib2Proxy with https_proxy set to an https:// URL. httplib2 treats
// an https:// proxy URL as an ordinary HTTP CONNECT proxy (not TLS-to-proxy),
// so the proxy must receive a plain "CONNECT host:port" line and the body must
// round-trip through the tunnel.
func TestHttplib2ProxyHandshake(t *testing.T) {
	for _, k := range []string{"http_proxy", "HTTP_PROXY", "https_proxy", "HTTPS_PROXY", "no_proxy", "NO_PROXY", "ALL_PROXY", "all_proxy"} {
		t.Setenv(k, "")
	}
	target := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("tunneled"))
	}))
	defer target.Close()

	proxy := newProxyRequestLine(t)
	t.Setenv("https_proxy", "https://"+proxy.addr())

	client := NewClient(ClientOptions{
		Proxy:           Httplib2Proxy,
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	})
	defer client.CloseIdleConnections()

	resp, err := client.Get(target.URL)
	if err != nil {
		t.Fatalf("GET through CONNECT proxy: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "tunneled" {
		t.Errorf("body = %q, want tunneled", body)
	}
	reqs := proxy.requests()
	if len(reqs) == 0 {
		t.Fatal("the proxy saw no request; the transport did not CONNECT")
	}
	if !strings.HasPrefix(reqs[0], http.MethodConnect+" ") {
		t.Errorf("proxy request line = %q, want a CONNECT", reqs[0])
	}
}

func TestNewClientNilProxyUsesNoProxy(t *testing.T) {
	client := NewClient(ClientOptions{})
	tr, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport = %T", client.Transport)
	}
	if tr.Proxy != nil {
		t.Errorf("nil ProxySelector should leave transport.Proxy nil")
	}
}

func TestNewClientInstallsProxySelector(t *testing.T) {
	client := NewClient(ClientOptions{Proxy: RequestsProxy})
	tr := client.Transport.(*http.Transport)
	if tr.Proxy == nil {
		t.Fatal("transport.Proxy not set")
	}
}

// TestIdleTimeoutDialerAppliesInactivityTimeout proves the timeout is an
// inactivity timeout, not a total deadline: the response body streams over a
// total longer than the timeout, but no single gap between reads exceeds it,
// so the request succeeds.
func TestIdleTimeoutDialerAppliesInactivityTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		flusher := w.(http.Flusher)
		for _, chunk := range []string{"a", "b", "c"} {
			_, _ = w.Write([]byte(chunk))
			flusher.Flush()
			time.Sleep(120 * time.Millisecond)
		}
	}))
	defer srv.Close()

	idle := 200 * time.Millisecond
	client := NewClient(ClientOptions{DialContext: IdleTimeoutDialer(idle)})
	ctx := WithRequestTimeout(context.Background(), idle)
	status, body, err := Do(ctx, client, http.MethodGet, srv.URL, nil, nil, 0)
	if err != nil {
		t.Fatalf("inactivity timeout aborted a progressing response: %v", err)
	}
	if status != http.StatusOK || string(body) != "abc" {
		t.Fatalf("status = %d body = %q", status, body)
	}
}

// pacedReader yields `chunks` reads of chunkSize bytes, sleeping gap between
// reads so a request body uploads slowly (total >> the timeout) while every
// single socket write still completes quickly.
type pacedReader struct {
	chunkSize int
	chunks    int
	gap       time.Duration
	read      int
}

func (p *pacedReader) Read(b []byte) (int, error) {
	if p.read > 0 {
		time.Sleep(p.gap)
	}
	if p.read >= p.chunks {
		return 0, io.EOF
	}
	p.read++
	n := p.chunkSize
	if n > len(b) {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		b[i] = 'a'
	}
	return n, nil
}

func (p *pacedReader) total() int { return p.chunkSize * p.chunks }

// TestInactivityClientProgressingUploadExceedsTimeout pins review round 3
// item 2: the response-read deadline must not run while the request body is
// still uploading. The upload here lasts 20×20ms = 400ms (>> the 200ms
// timeout) yet every single write progresses, and the server answers
// immediately once the body is received, so the request must succeed.
func TestInactivityClientProgressingUploadExceedsTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok")
	}))
	defer srv.Close()

	idle := 200 * time.Millisecond
	client := NewInactivityClient(ClientOptions{})
	body := &pacedReader{chunkSize: 1024, chunks: 20, gap: 20 * time.Millisecond}

	req, err := http.NewRequest(http.MethodPost, srv.URL, body)
	if err != nil {
		t.Fatal(err)
	}
	req.ContentLength = int64(body.total())
	req = req.WithContext(WithRequestTimeout(context.Background(), idle))

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("progressing upload longer than the timeout was aborted: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}

// TestInactivityClientStalledTLSHandshake pins review round 4 item 1:
// connection establishment (dial plus TLS handshake) is its own timeout
// phase. A peer that accepts TCP but never completes the TLS handshake must
// be bounded by the inactivity timeout, even though WroteRequest cannot fire
// before the handshake finishes.
func TestInactivityClientStalledTLSHandshake(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()

	var (
		mu   sync.Mutex
		held []net.Conn
	)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			held = append(held, conn)
			mu.Unlock()
			// Never read or write: the TLS handshake never completes.
		}
	}()
	defer func() {
		mu.Lock()
		for _, c := range held {
			_ = c.Close()
		}
		mu.Unlock()
	}()

	client := NewInactivityClient(ClientOptions{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // the handshake never completes
	})
	idle := 200 * time.Millisecond
	req, err := http.NewRequest(http.MethodGet, "https://"+ln.Addr().String()+"/", nil)
	if err != nil {
		t.Fatal(err)
	}
	req = req.WithContext(WithRequestTimeout(context.Background(), idle))

	done := make(chan error, 1)
	go func() {
		resp, err := client.Do(req)
		if resp != nil {
			_ = resp.Body.Close()
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("stalled TLS handshake returned no error")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("stalled TLS handshake was not bounded by the inactivity timeout")
	}
}

// TestInactivityClientRedirectedUploadExceedsTimeout pins review round 4 item
// 2: Do begins the request once, but a 307 redirect issues a second request
// attempt internally. The per-attempt GotConn/WroteRequest hooks must reset
// the read phase for that attempt, so the redirected upload is not aborted by
// the previous attempt's read deadline. Both the initial and the redirected
// upload last 20x20ms = 400ms (>> the 200ms timeout).
func TestInactivityClientRedirectedUploadExceedsTimeout(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok")
	}))
	defer target.Close()

	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer redirector.Close()

	idle := 200 * time.Millisecond
	client := NewInactivityClient(ClientOptions{})

	const chunkSize, chunks = 1024, 20
	newBody := func() io.ReadCloser {
		return io.NopCloser(&pacedReader{chunkSize: chunkSize, chunks: chunks, gap: 20 * time.Millisecond})
	}
	req, err := http.NewRequest(http.MethodPost, redirector.URL, newBody())
	if err != nil {
		t.Fatal(err)
	}
	req.ContentLength = chunkSize * chunks
	req.GetBody = func() (io.ReadCloser, error) { return newBody(), nil }
	req = req.WithContext(WithRequestTimeout(context.Background(), idle))

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("redirected upload longer than the timeout was aborted: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}
