// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package websocket

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
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
}

// --- item #9: the closing handshake awaits the peer's close ---

func TestCloseConnAwaitsPeerClose(t *testing.T) {
	withVar(t, &closeTimeout, time.Second)
	clientConn, _, serverClose := websocketPair(t)
	defer serverClose()
	start := time.Now()
	(&runner{}).closeConn(clientConn)
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("closeConn took %v", elapsed)
	}
}

// --- item #10: a stalled handshake fails on the inner open_timeout ---

func TestDialStalledHandshake(t *testing.T) {
	withVar(t, &openTimeout, 200*time.Millisecond)
	withVar(t, &connectTimeout, 5*time.Second)
	withVar(t, &sendWaitConnected, 50*time.Millisecond)

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
	start := time.Now()
	err = s.SendMessages(context.Background(), [][]byte{[]byte("x")}, "")
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("send to a stalled handshake succeeded")
	}
	if elapsed > 3*time.Second {
		t.Fatalf("stalled handshake took %v, want ~openTimeout", elapsed)
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

// --- helpers ---

// newStalledReadServer completes the websocket handshake then never reads.
type stalledReadServer struct {
	srv *httptest.Server
}

func newStalledReadServer(t *testing.T) *stalledReadServer {
	t.Helper()
	s := &stalledReadServer{}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		up := websocket.Upgrader{}
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		// Never read; hold until the test ends.
		<-r.Context().Done()
		_ = c.Close()
	}))
	t.Cleanup(s.srv.Close)
	return s
}

func (s *stalledReadServer) url() string {
	return "ws" + strings.TrimPrefix(s.srv.URL, "http") + "/pump"
}

// websocketPair returns a connected client/server pair plus a cleanup.
func websocketPair(t *testing.T) (*websocket.Conn, *websocket.Conn, func()) {
	t.Helper()
	accepted := make(chan *websocket.Conn, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		up := websocket.Upgrader{}
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		accepted <- c
		for {
			if _, _, err := c.ReadMessage(); err != nil {
				return
			}
		}
	}))
	uri := "ws" + strings.TrimPrefix(srv.URL, "http") + "/pump"
	client, _, err := websocket.DefaultDialer.Dial(uri, nil)
	if err != nil {
		srv.Close()
		t.Fatal(err)
	}
	server := <-accepted
	return client, server, func() { srv.Close() }
}
