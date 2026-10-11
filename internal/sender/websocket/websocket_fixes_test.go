// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package websocket

import (
	"bufio"
	"context"
	"crypto/sha1"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/Aiven-Open/journalpump/internal/sender"
	"github.com/Aiven-Open/journalpump/internal/statsd"
	"github.com/Aiven-Open/journalpump/internal/tagged"
)

// withVar temporarily overrides a package timing var.
func withVar(t *testing.T, p *time.Duration, v time.Duration) {
	t.Helper()
	old := *p
	*p = v
	t.Cleanup(func() { *p = old })
}

// --- item #2: ssl with a ws:// URI is rejected (would be plaintext) ---

func TestParseConfigSSLWithWSURIRejected(t *testing.T) {
	cfg := map[string]any{
		"websocket_uri": "ws://h/pump",
		"ssl":           true,
		"ca":            "ca.pem", "keyfile": "k.pem", "certfile": "c.pem",
	}
	_, err := parseConfig(cfg)
	if err == nil || !strings.Contains(err.Error(), "plaintext") {
		t.Fatalf("ssl + ws:// error = %v, want plaintext rejection", err)
	}
	// wss:// is fine.
	cfg["websocket_uri"] = "wss://h/pump"
	if _, err := parseConfig(cfg); err != nil {
		t.Fatalf("ssl + wss:// rejected: %v", err)
	}
}

func TestParseConfigRejectsBadScheme(t *testing.T) {
	if _, err := parseConfig(map[string]any{"websocket_uri": "http://h/"}); err == nil {
		t.Fatal("http:// scheme accepted")
	}
}

// --- item #11: a present null enum is an error, an absent key defaults ---

func TestParseConfigNullEnumsRejected(t *testing.T) {
	if _, err := parseConfig(map[string]any{"websocket_uri": "ws://h/", "compression": nil}); err == nil {
		t.Error("compression: null accepted, want error")
	}
	if _, err := parseConfig(map[string]any{"websocket_uri": "ws://h/", "websocket_compression": nil}); err == nil {
		t.Error("websocket_compression: null accepted, want error")
	}
	// A null string/bool key is still treated as absent (Python .get() falsy).
	if _, err := parseConfig(map[string]any{"websocket_uri": "ws://h/", "ca": nil, "ssl": nil}); err != nil {
		t.Errorf("null ca/ssl rejected: %v", err)
	}
}

// --- item #5: userinfo becomes an Authorization header, stripped from URL ---

func TestStripUserinfoAndBasicAuth(t *testing.T) {
	if got := stripUserinfo("ws://u:p@host:8080/path"); got != "ws://host:8080/path" {
		t.Errorf("stripUserinfo = %q", got)
	}
	h := http.Header{}
	applyBasicAuth("ws://user:s3cr3t@host/", h)
	want := "Basic " + base64.StdEncoding.EncodeToString([]byte("user:s3cr3t"))
	if got := h.Get("Authorization"); got != want {
		t.Errorf("Authorization = %q, want %q", got, want)
	}
	// No userinfo -> no header.
	h2 := http.Header{}
	applyBasicAuth("ws://host/", h2)
	if h2.Get("Authorization") != "" {
		t.Error("Authorization set without userinfo")
	}
}

func TestSendMessagesBasicAuth(t *testing.T) {
	srv := newWebsocketTestServer(t)
	// Inject the credentials into the dial URL.
	uri := strings.Replace(srv.wsURL(), "ws://", "ws://alice:wonderland@", 1)
	s := newTestSender(t, map[string]any{"websocket_uri": uri, "compression": "none"})
	if err := s.SendMessages(context.Background(), [][]byte{[]byte("x")}, ""); err != nil {
		t.Fatalf("SendMessages: %v", err)
	}
	select {
	case <-srv.gotOne:
	case <-time.After(5 * time.Second):
		t.Fatal("no frame received")
	}
	want := "Basic " + base64.StdEncoding.EncodeToString([]byte("alice:wonderland"))
	if got := srv.firstHeaders().Get("Authorization"); got != want {
		t.Errorf("Authorization = %q, want %q", got, want)
	}
}

// --- item #4: environment proxy selection matches websockets get_proxy ---

func TestSelectEnvProxy(t *testing.T) {
	clearProxyEnv(t)
	// ws:// picks http_proxy when no ws/socks/https proxy is set.
	t.Setenv("http_proxy", "http://proxy:3128")
	t.Setenv("ws_proxy", "")
	if got := selectEnvProxy("ws://host/pump"); got != "http://proxy:3128" {
		t.Errorf("ws:// http_proxy = %q", got)
	}
	// ws_proxy takes priority over http_proxy.
	t.Setenv("ws_proxy", "http://wsproxy:1")
	if got := selectEnvProxy("ws://host/pump"); got != "http://wsproxy:1" {
		t.Errorf("ws:// ws_proxy = %q", got)
	}
	// A socks entry declared as http:// is rewritten to socks5h://.
	t.Setenv("ws_proxy", "")
	t.Setenv("socks_proxy", "http://sockshost:1080")
	if got := selectEnvProxy("ws://host/pump"); got != "socks5h://sockshost:1080" {
		t.Errorf("ws:// socks rewrite = %q", got)
	}
	// no_proxy bypasses everything.
	t.Setenv("no_proxy", "example.com")
	if got := selectEnvProxy("ws://sub.example.com/pump"); got != "" {
		t.Errorf("no_proxy bypass = %q, want empty", got)
	}
	// wss:// does NOT consult http_proxy.
	t.Setenv("no_proxy", "")
	t.Setenv("socks_proxy", "")
	t.Setenv("http_proxy", "http://proxy:3128")
	if got := selectEnvProxy("wss://host/pump"); got != "" {
		t.Errorf("wss:// http_proxy = %q, want empty", got)
	}
	t.Setenv("https_proxy", "http://sproxy:443")
	if got := selectEnvProxy("wss://host/pump"); got != "http://sproxy:443" {
		t.Errorf("wss:// https_proxy = %q", got)
	}
}

// --- item #3: SOCKS5 resolves remotely (hostname reaches the proxy) ---

// fakeSOCKS5 is a minimal SOCKS5 server that records the CONNECT target.
type fakeSOCKS5 struct {
	ln    net.Listener
	mu    sync.Mutex
	atyp  byte
	host  string
	port  uint16
	ready chan struct{}
}

func newFakeSOCKS5(t *testing.T) *fakeSOCKS5 {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeSOCKS5{ln: ln, ready: make(chan struct{})}
	go f.serve()
	t.Cleanup(func() { _ = ln.Close() })
	return f
}

func (f *fakeSOCKS5) addr() string { return f.ln.Addr().String() }

func (f *fakeSOCKS5) serve() {
	conn, err := f.ln.Accept()
	if err != nil {
		return
	}
	defer func() { _ = conn.Close() }()
	buf := make([]byte, 512)
	// Greeting: VER, NMETHODS, METHODS.
	if _, err := io.ReadFull(conn, buf[:2]); err != nil {
		return
	}
	n := int(buf[1])
	if _, err := io.ReadFull(conn, buf[:n]); err != nil {
		return
	}
	if _, err := conn.Write([]byte{0x05, 0x00}); err != nil { // no auth
		return
	}
	// Request: VER, CMD, RSV, ATYP.
	if _, err := io.ReadFull(conn, buf[:4]); err != nil {
		return
	}
	atyp := buf[3]
	var host string
	switch atyp {
	case 0x01: // IPv4
		if _, err := io.ReadFull(conn, buf[:4]); err != nil {
			return
		}
		host = net.IP(buf[:4]).String()
	case 0x03: // domain
		if _, err := io.ReadFull(conn, buf[:1]); err != nil {
			return
		}
		l := int(buf[0])
		if _, err := io.ReadFull(conn, buf[:l]); err != nil {
			return
		}
		host = string(buf[:l])
	case 0x04: // IPv6
		if _, err := io.ReadFull(conn, buf[:16]); err != nil {
			return
		}
		host = net.IP(buf[:16]).String()
	}
	if _, err := io.ReadFull(conn, buf[:2]); err != nil {
		return
	}
	port := binary.BigEndian.Uint16(buf[:2])
	f.mu.Lock()
	f.atyp, f.host, f.port = atyp, host, port
	f.mu.Unlock()
	close(f.ready)
	// Success reply with a dummy bound address.
	_, _ = conn.Write([]byte{0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
	// Hold the connection open until the client closes it.
	_, _ = io.Copy(io.Discard, conn)
}

func TestSocks5RemoteDNS(t *testing.T) {
	f := newFakeSOCKS5(t)
	d, err := newSocks5Dialer("socks5://" + f.addr())
	if err != nil {
		t.Fatal(err)
	}
	conn, err := d.DialContext(context.Background(), "tcp", "example.invalid:443")
	if err != nil {
		t.Fatalf("socks dial: %v", err)
	}
	_ = conn.Close()
	select {
	case <-f.ready:
	case <-time.After(5 * time.Second):
		t.Fatal("no SOCKS request recorded")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.atyp != 0x03 {
		t.Errorf("ATYP = %#x, want 0x03 (domain, remote DNS)", f.atyp)
	}
	if f.host != "example.invalid" || f.port != 443 {
		t.Errorf("target = %s:%d, want example.invalid:443", f.host, f.port)
	}
}

// --- item #6: handshake redirects are followed ---

func TestSendMessagesFollowsRedirect(t *testing.T) {
	target := newWebsocketTestServer(t)
	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.wsURL(), http.StatusTemporaryRedirect)
	}))
	defer redirector.Close()
	uri := "ws" + strings.TrimPrefix(redirector.URL, "http") + "/pump"

	s := newTestSender(t, map[string]any{"websocket_uri": uri, "compression": "none"})
	if err := s.SendMessages(context.Background(), [][]byte{[]byte("hello")}, ""); err != nil {
		t.Fatalf("SendMessages: %v", err)
	}
	select {
	case <-target.gotOne:
	case <-time.After(5 * time.Second):
		t.Fatal("redirected frame not received")
	}
	if got := target.capturedFrames(); len(got) != 1 || string(got[0]) != "hello" {
		t.Errorf("frames = %q", got)
	}
}

func TestRedirectTargetRejectsTLSDowngrade(t *testing.T) {
	resp := &http.Response{StatusCode: 302, Header: http.Header{"Location": []string{"ws://insecure/"}}}
	if _, err := redirectTarget("wss://secure/", resp, false); err == nil {
		t.Fatal("TLS downgrade redirect accepted")
	}
}

func TestRedirectTargetRejectsPreexistingSocket(t *testing.T) {
	resp := &http.Response{StatusCode: 302, Header: http.Header{"Location": []string{"wss://other/"}}}
	if _, err := redirectTarget("wss://secure/", resp, true); err == nil {
		t.Fatal("redirect with a preexisting socket accepted")
	}
}

// --- item #8: decompressed output is bounded, not just the wire frame ---

func TestDrainDecompressionBomb(t *testing.T) {
	payload := make([]byte, inboundLimit+1024) // 2 MiB+ of zeros, compresses tiny
	up := websocket.Upgrader{EnableCompression: true}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = c.Close() }()
		c.EnableWriteCompression(true)
		if err := c.WriteMessage(websocket.BinaryMessage, payload); err != nil {
			return
		}
		// Hold the connection so the client can drain the bomb.
		_, _, _ = c.ReadMessage()
	}))
	defer srv.Close()
	uri := "ws" + strings.TrimPrefix(srv.URL, "http") + "/pump"

	d := websocket.Dialer{EnableCompression: true}
	conn, _, err := d.Dial(uri, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	conn.SetReadLimit(inboundLimit)
	if err := drain(conn); !errors.Is(err, errInboundTooLarge) {
		t.Fatalf("drain error = %v, want errInboundTooLarge", err)
	}
}

// --- item #12: ordinary transport failures do not emit the metric ---

func TestShouldReport(t *testing.T) {
	if shouldReport(context.Background(), &transportError{errors.New("write: broken pipe")}) {
		t.Error("transportError reported as unexpected")
	}
	if shouldReport(context.Background(), errNotConnected) {
		t.Error("errNotConnected reported")
	}
	if shouldReport(context.Background(), errStopped) {
		t.Error("errStopped reported")
	}
	if !shouldReport(context.Background(), errors.New("weird")) {
		t.Error("generic error not reported")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if shouldReport(ctx, errors.New("weird")) {
		t.Error("cancelled ctx error reported")
	}
}

// udpStats returns a statsd client writing to a local UDP socket plus the
// socket, so a test can assert which metrics were emitted.
func udpStats(t *testing.T) (*statsd.Client, *net.UDPConn) {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	host := "127.0.0.1"
	port := int64(conn.LocalAddr().(*net.UDPAddr).Port)
	return statsd.New(&host, &port, nil, nil, "", nil), conn
}

// drainStats reads and returns all datagrams currently queued.
func drainStats(conn *net.UDPConn) []string {
	var out []string
	buf := make([]byte, 2048)
	for {
		_ = conn.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			return out
		}
		out = append(out, string(buf[:n]))
	}
}

func TestSendMessagesNoMetricOnConnectionFailure(t *testing.T) {
	withVar(t, &sendWaitConnected, 50*time.Millisecond)
	stats, udp := udpStats(t)
	inst, err := New(sender.ConstructorArgs{
		Name:   "out",
		Config: map[string]any{"websocket_uri": "ws://127.0.0.1:1/none", "compression": "none"},
		Stats:  stats,
		Tags:   tagged.New(nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	s := inst.Transport.(*Sender)
	t.Cleanup(func() { _ = s.Close() })
	if err := s.SendMessages(context.Background(), [][]byte{[]byte("x")}, ""); err == nil {
		t.Fatal("send to unreachable endpoint succeeded")
	}
	for _, d := range drainStats(udp) {
		if strings.Contains(d, "exception") {
			t.Errorf("unexpected_exception emitted on a connection failure: %q", d)
		}
	}
}

// --- item #1: a peer that stops reading cannot stall shutdown ---

func TestCloseUnblocksBlockedWrite(t *testing.T) {
	withVar(t, &closeTimeout, 200*time.Millisecond)
	srv := newStalledReadServer(t)
	s := newTestSender(t, map[string]any{"websocket_uri": srv.url(), "compression": "none"})

	if err := s.SendMessages(context.Background(), [][]byte{[]byte("warm")}, ""); err != nil {
		t.Fatalf("warmup send: %v", err)
	}
	// A frame too large for the socket buffer, with a peer that never reads.
	big := make([]byte, 8*1024*1024)
	done := make(chan struct{})
	go func() {
		_ = s.SendMessages(context.Background(), [][]byte{big}, "")
		close(done)
	}()
	time.Sleep(200 * time.Millisecond) // let the write block

	start := time.Now()
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("Close took %v after a blocked write, want bounded", elapsed)
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("blocked send did not return after Close")
	}
	// Reload path: a fresh send after Close starts a new runner and succeeds
	// against the (reconnected) peer.
	if err := s.SendMessages(context.Background(), [][]byte{[]byte("after-reload")}, ""); err != nil {
		t.Fatalf("send after reload: %v", err)
	}
}

// --- item #6: the closing handshake awaits the peer's close (production) ---

// TestCloseAwaitsPeerCloseOnStop drives the real Stop path: the server
// observes the client's close frame, withholds its close response, and Stop
// must not return until that response is released; then it returns promptly.
func TestCloseAwaitsPeerCloseOnStop(t *testing.T) {
	withVar(t, &closeTimeout, 2*time.Second)

	release := make(chan struct{})
	sawClose := make(chan struct{})
	up := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = c.Close() }()
		c.SetCloseHandler(func(code int, text string) error {
			close(sawClose)
			<-release // withhold the close response
			return c.WriteControl(websocket.CloseMessage,
				websocket.FormatCloseMessage(code, text), time.Now().Add(time.Second))
		})
		for {
			if _, _, err := c.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer srv.Close()
	uri := "ws" + strings.TrimPrefix(srv.URL, "http") + "/pump"

	s := newTestSender(t, map[string]any{"websocket_uri": uri, "compression": "none"})
	if err := s.SendMessages(context.Background(), [][]byte{[]byte("warm")}, ""); err != nil {
		t.Fatalf("warmup send: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- s.Close() }()
	select {
	case <-sawClose:
	case <-time.After(3 * time.Second):
		t.Fatal("server never saw the client close frame")
	}
	select {
	case <-done:
		t.Fatal("Close returned before the peer released its close response")
	case <-time.After(300 * time.Millisecond):
	}
	close(release)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not return after the peer close response")
	}
}

// --- item #10/#8: a stalled handshake fails on the inner open_timeout ---

// TestDialStalledHandshake asserts the ACTUAL dial timing: the TCP preconnect
// succeeds immediately (outer budget), then the HTTP opening handshake must
// fail at ~openTimeout, well before the outer connectTimeout.
func TestDialStalledHandshake(t *testing.T) {
	withVar(t, &openTimeout, 300*time.Millisecond)
	withVar(t, &connectTimeout, 5*time.Second)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		// Hold the socket, never answer the HTTP upgrade.
		_, _ = io.Copy(io.Discard, c)
	}()

	s := newTestSender(t, map[string]any{
		"websocket_uri": "ws://" + ln.Addr().String() + "/x",
		"compression":   "none",
	})
	r := newRunner(s)
	start := time.Now()
	_, err = r.dial()
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("dial to a stalled handshake succeeded")
	}
	if elapsed < 150*time.Millisecond {
		t.Fatalf("dial failed after %v, want ~openTimeout (300ms)", elapsed)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("dial took %v, want ~openTimeout, not the outer connectTimeout", elapsed)
	}
}

// --- item #7: a peer that withholds pongs is detected ---

func TestHeartbeatDetectsPongWithholding(t *testing.T) {
	withVar(t, &pingInterval, 100*time.Millisecond)
	withVar(t, &pingTimeout, 100*time.Millisecond)

	var mu sync.Mutex
	conns, pings := 0, 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		up := websocket.Upgrader{}
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		mu.Lock()
		conns++
		mu.Unlock()
		c.SetPingHandler(func(string) error { // record but never pong
			mu.Lock()
			pings++
			mu.Unlock()
			return nil
		})
		defer func() { _ = c.Close() }()
		for {
			if _, _, err := c.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer srv.Close()
	uri := "ws" + strings.TrimPrefix(srv.URL, "http") + "/pump"

	s := newTestSender(t, map[string]any{"websocket_uri": uri, "compression": "none"})
	// Reconnect immediately (zero jitter) so the test does not wait out the
	// 20 s base backoff.
	s.jitter = func(float64) float64 { return 0 }
	_ = s.SendMessages(context.Background(), [][]byte{[]byte("x")}, "")

	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		c, p := conns, pings
		mu.Unlock()
		if c >= 2 {
			if p == 0 {
				t.Fatal("reconnected without sending any ping")
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no reconnect (conns=%d pings=%d)", c, p)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// --- item #5: a peer that keeps sending data but never pongs is detected ---

func TestHeartbeatDetectsPongWithholdingWhileDataFlows(t *testing.T) {
	withVar(t, &pingInterval, 100*time.Millisecond)
	withVar(t, &pingTimeout, 100*time.Millisecond)

	var mu sync.Mutex
	conns := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		up := websocket.Upgrader{}
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		mu.Lock()
		conns++
		mu.Unlock()
		c.SetPingHandler(func(string) error { return nil }) // never pong
		done := make(chan struct{})
		go func() { // keep sending data so the READ deadline is renewed
			tk := time.NewTicker(30 * time.Millisecond)
			defer tk.Stop()
			for {
				select {
				case <-done:
					return
				case <-tk.C:
					if c.WriteMessage(websocket.BinaryMessage, []byte("data")) != nil {
						return
					}
				}
			}
		}()
		defer close(done)
		defer func() { _ = c.Close() }()
		for {
			if _, _, err := c.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer srv.Close()
	uri := "ws" + strings.TrimPrefix(srv.URL, "http") + "/pump"

	s := newTestSender(t, map[string]any{"websocket_uri": uri, "compression": "none"})
	s.jitter = func(float64) float64 { return 0 }
	_ = s.SendMessages(context.Background(), [][]byte{[]byte("x")}, "")

	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		c := conns
		mu.Unlock()
		if c >= 2 {
			return // a data-only peer did not satisfy the pong requirement
		}
		if time.Now().After(deadline) {
			t.Fatalf("no reconnect: data traffic masked the withheld pong (conns=%d)", c)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// --- item #3: urllib env precedence and no_proxy bypass rules ---

func clearProxyEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{"ws_proxy", "wss_proxy", "socks_proxy", "https_proxy", "http_proxy", "no_proxy"} {
		t.Setenv(k, "")
	}
}

func TestEnvValueUppercaseFallbackAndEmptyRemoval(t *testing.T) {
	// A synthetic key with no lowercase counterpart: the uppercase applies.
	t.Setenv("JOURNALPUMP_TEST_PROXY", "http://upper:1")
	if got := envValue("journalpump_test_proxy"); got != "http://upper:1" {
		t.Errorf("uppercase fallback = %q", got)
	}
	// A PRESENT empty lowercase key REMOVES the setting.
	t.Setenv("journalpump_test_proxy", "")
	if got := envValue("journalpump_test_proxy"); got != "" {
		t.Errorf("empty lowercase did not remove uppercase: %q", got)
	}
}

func TestNoProxyStarOnlyWholeValue(t *testing.T) {
	clearProxyEnv(t)
	t.Setenv("http_proxy", "http://proxy:3128")
	t.Setenv("no_proxy", "*,example.com")
	if got := selectEnvProxy("ws://other/"); got != "http://proxy:3128" {
		t.Errorf("no_proxy with * among entries wrongly bypassed: %q", got)
	}
	t.Setenv("no_proxy", "*")
	if got := selectEnvProxy("ws://other/"); got != "" {
		t.Errorf("no_proxy = * did not bypass: %q", got)
	}
}

func TestNoProxyEmptyLowercaseRemovesUppercase(t *testing.T) {
	clearProxyEnv(t)
	t.Setenv("http_proxy", "http://proxy:3128")
	t.Setenv("NO_PROXY", "*")
	t.Setenv("no_proxy", "")
	if got := selectEnvProxy("ws://other/"); got != "http://proxy:3128" {
		t.Errorf("empty lowercase no_proxy did not remove NO_PROXY: %q", got)
	}
}

func TestSelectEnvProxyUnaffectedByUppercaseLeak(t *testing.T) {
	clearProxyEnv(t)
	// A stray uppercase env var must not override an explicitly empty
	// lowercase one (the failure mode the review reported).
	t.Setenv("WS_PROXY", "http://leak:1")
	t.Setenv("ws_proxy", "")
	t.Setenv("http_proxy", "http://proxy:3128")
	if got := selectEnvProxy("ws://host/"); got != "http://proxy:3128" {
		t.Errorf("WS_PROXY leaked into selection: %q", got)
	}
}

// --- item #4: the proxy is re-resolved for every redirect target ---

// fakeConnectProxy is a minimal HTTP CONNECT proxy recording tunnel targets.
type fakeConnectProxy struct {
	ln      net.Listener
	mu      sync.Mutex
	targets []string
}

func newFakeConnectProxy(t *testing.T) *fakeConnectProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &fakeConnectProxy{ln: ln}
	go p.serve()
	t.Cleanup(func() { _ = ln.Close() })
	return p
}

func (p *fakeConnectProxy) url() string { return "http://" + p.ln.Addr().String() }

func (p *fakeConnectProxy) seen() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, len(p.targets))
	copy(out, p.targets)
	return out
}

func (p *fakeConnectProxy) serve() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		go p.handle(c)
	}
}

func (p *fakeConnectProxy) handle(c net.Conn) {
	defer func() { _ = c.Close() }()
	br := bufio.NewReader(c)
	req, err := http.ReadRequest(br)
	if err != nil || req.Method != http.MethodConnect {
		return
	}
	target := req.Host
	p.mu.Lock()
	p.targets = append(p.targets, target)
	p.mu.Unlock()
	up, err := net.DialTimeout("tcp", target, 2*time.Second)
	if err != nil {
		_, _ = c.Write([]byte("HTTP/1.1 502 Bad Gateway\r\n\r\n"))
		return
	}
	defer func() { _ = up.Close() }()
	if _, err := c.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n")); err != nil {
		return
	}
	go func() { _, _ = io.Copy(up, br) }()
	_, _ = io.Copy(c, up)
}

// hostPortOf parses a URL and returns its host:port.
func hostPortOf(t *testing.T, raw string) string {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return u.Host
}

func TestRedirectReselectsProxy(t *testing.T) {
	t.Run("proxied-then-bypassed", func(t *testing.T) {
		clearProxyEnv(t)
		proxy := newFakeConnectProxy(t)
		target := newWebsocketTestServer(t)
		redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, target.wsURL(), http.StatusTemporaryRedirect)
		}))
		defer redirector.Close()

		t.Setenv("http_proxy", proxy.url())
		t.Setenv("no_proxy", hostPortOf(t, target.srv.URL)) // bypass the redirect target
		uri := "ws" + strings.TrimPrefix(redirector.URL, "http") + "/pump"

		s := newTestSender(t, map[string]any{"websocket_uri": uri, "compression": "none"})
		if err := s.SendMessages(context.Background(), [][]byte{[]byte("hello")}, ""); err != nil {
			t.Fatalf("SendMessages: %v", err)
		}
		select {
		case <-target.gotOne:
		case <-time.After(5 * time.Second):
			t.Fatal("redirected frame not received")
		}
		seen := proxy.seen()
		if len(seen) != 1 || seen[0] != hostPortOf(t, redirector.URL) {
			t.Fatalf("proxy tunnels = %v, want only the original %s", seen, hostPortOf(t, redirector.URL))
		}
	})

	t.Run("bypassed-then-proxied", func(t *testing.T) {
		clearProxyEnv(t)
		proxy := newFakeConnectProxy(t)
		target := newWebsocketTestServer(t)
		redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, target.wsURL(), http.StatusTemporaryRedirect)
		}))
		defer redirector.Close()

		t.Setenv("http_proxy", proxy.url())
		t.Setenv("no_proxy", hostPortOf(t, redirector.URL)) // bypass the ORIGINAL
		uri := "ws" + strings.TrimPrefix(redirector.URL, "http") + "/pump"

		s := newTestSender(t, map[string]any{"websocket_uri": uri, "compression": "none"})
		if err := s.SendMessages(context.Background(), [][]byte{[]byte("hello")}, ""); err != nil {
			t.Fatalf("SendMessages: %v", err)
		}
		select {
		case <-target.gotOne:
		case <-time.After(5 * time.Second):
			t.Fatal("redirected frame not received")
		}
		seen := proxy.seen()
		if len(seen) != 1 || seen[0] != hostPortOf(t, target.srv.URL) {
			t.Fatalf("proxy tunnels = %v, want only the redirect target %s", seen, hostPortOf(t, target.srv.URL))
		}
	})
}

// --- item #2: an HTTPS proxy is tunneled with a verified TLS hop ---

func TestSendMessagesThroughHTTPSProxy(t *testing.T) {
	clearProxyEnv(t)
	target := newWebsocketTestServer(t)

	var mu sync.Mutex
	var connects []string
	proxySrv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect {
			http.Error(w, "CONNECT required", http.StatusMethodNotAllowed)
			return
		}
		mu.Lock()
		connects = append(connects, r.Host)
		mu.Unlock()
		upstream, err := net.DialTimeout("tcp", r.Host, 2*time.Second)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer func() { _ = upstream.Close() }()
		hj, ok := w.(http.Hijacker)
		if !ok {
			http.Error(w, "no hijack", http.StatusInternalServerError)
			return
		}
		client, _, err := hj.Hijack()
		if err != nil {
			return
		}
		defer func() { _ = client.Close() }()
		if _, err := client.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n")); err != nil {
			return
		}
		go func() { _, _ = io.Copy(upstream, client) }()
		_, _ = io.Copy(client, upstream)
	}))
	defer proxySrv.Close()

	pool := x509.NewCertPool()
	pool.AddCert(proxySrv.Certificate())
	old := httpsProxyTLSConfig
	httpsProxyTLSConfig = func() *tls.Config {
		return &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	}
	t.Cleanup(func() { httpsProxyTLSConfig = old })

	t.Setenv("https_proxy", proxySrv.URL)
	s := newTestSender(t, map[string]any{"websocket_uri": target.wsURL(), "compression": "none"})
	if err := s.SendMessages(context.Background(), [][]byte{[]byte("hello")}, ""); err != nil {
		t.Fatalf("SendMessages through an HTTPS proxy: %v", err)
	}
	select {
	case <-target.gotOne:
	case <-time.After(5 * time.Second):
		t.Fatal("frame not received through the HTTPS proxy")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(connects) != 1 || connects[0] != hostPortOf(t, target.srv.URL) {
		t.Fatalf("proxy CONNECTs = %v, want [%s]", connects, hostPortOf(t, target.srv.URL))
	}
}

// --- item #7: a redirect Location with credentials is authenticated ---

func TestSendMessagesAuthenticatedRedirect(t *testing.T) {
	target := newWebsocketTestServer(t)
	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		loc := strings.Replace(target.wsURL(), "ws://", "ws://redir:secret@", 1)
		http.Redirect(w, r, loc, http.StatusTemporaryRedirect)
	}))
	defer redirector.Close()
	uri := "ws" + strings.TrimPrefix(redirector.URL, "http") + "/pump"

	s := newTestSender(t, map[string]any{"websocket_uri": uri, "compression": "none"})
	if err := s.SendMessages(context.Background(), [][]byte{[]byte("hello")}, ""); err != nil {
		t.Fatalf("SendMessages: %v", err)
	}
	select {
	case <-target.gotOne:
	case <-time.After(5 * time.Second):
		t.Fatal("redirected frame not received")
	}
	want := "Basic " + base64.StdEncoding.EncodeToString([]byte("redir:secret"))
	if got := target.firstHeaders().Get("Authorization"); got != want {
		t.Errorf("redirect Authorization = %q, want %q", got, want)
	}
}

// --- item #1: abort is irreversible; a later write cannot re-arm it ---

func TestAbortPreventsWriteRearm(t *testing.T) {
	srv := newWebsocketTestServer(t)
	s := newTestSender(t, map[string]any{"websocket_uri": srv.wsURL(), "compression": "none"})
	r := newRunner(s)
	dc, err := r.dial()
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	lc := &liveConn{ws: dc.ws, raw: dc.raw, sendCh: make(chan sendRequest), closed: make(chan struct{}), pongCh: dc.pongCh}
	lc.abort()

	done := make(chan error, 1)
	go func() { done <- lc.ws.WriteMessage(websocket.BinaryMessage, []byte("x")) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("write after abort succeeded")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("write after abort blocked: the deadline was re-armed")
	}
	_ = lc.ws.Close()
}

// --- helpers ---

// newStalledReadServer is a minimal raw-TCP websocket endpoint that completes
// the handshake then NEVER reads, so a large client write blocks. It avoids
// net/http's server bookkeeping entirely and has an explicit stop signal so
// every accepted connection and handshake goroutine is closed and joined at
// cleanup (item #9).
type stalledReadServer struct {
	ln   net.Listener
	stop chan struct{}
	wg   sync.WaitGroup
	mu   sync.Mutex
	cons []net.Conn
}

func newStalledReadServer(t *testing.T) *stalledReadServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &stalledReadServer{ln: ln, stop: make(chan struct{})}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.accept()
	}()
	t.Cleanup(func() {
		_ = s.ln.Close()
		close(s.stop)
		s.mu.Lock()
		for _, c := range s.cons {
			_ = c.Close()
		}
		s.mu.Unlock()
		s.wg.Wait()
	})
	return s
}

func (s *stalledReadServer) accept() {
	for {
		c, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		s.cons = append(s.cons, c)
		s.mu.Unlock()
		s.wg.Add(1)
		go s.handshake(c)
	}
}

// handshake performs the RFC 6455 server handshake by hand, then blocks until
// cleanup without ever reading a frame.
func (s *stalledReadServer) handshake(c net.Conn) {
	defer s.wg.Done()
	br := bufio.NewReader(c)
	req, err := http.ReadRequest(br)
	if err != nil {
		return
	}
	key := req.Header.Get("Sec-WebSocket-Key")
	h := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	accept := base64.StdEncoding.EncodeToString(h[:])
	if _, err := fmt.Fprintf(c,
		"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n",
		accept); err != nil {
		return
	}
	<-s.stop
}

func (s *stalledReadServer) url() string {
	return "ws://" + s.ln.Addr().String() + "/pump"
}
