// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package syslogframing

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// TestFramingNegativeAndZeroMaxMsg pins Python's slice semantics for
// max_msg edge values (rsyslog.py:118,193-202): ONLY 0 defaults to 2048;
// negatives are kept and applied as negative slice indices.
func TestFramingNegativeAndZeroMaxMsg(t *testing.T) {
	body := []byte("abcdef") // len 6
	cases := []struct {
		name   string
		maxMsg int
		octet  bool
		want   string
	}{
		{"octet_negative_one", -1, true, "5 abcde"},
		{"octet_negative_three", -3, true, "3 abc"},
		{"octet_zero_defaults", 0, true, "6 abcdef"},
		{"nontransparent_negative_one", -1, false, "abcd\n"},
		{"nontransparent_negative_three", -3, false, "ab\n"},
		{"nontransparent_zero_defaults", 0, false, "abcdef"},
		{"nontransparent_one", 1, false, "\n"},
		{"nontransparent_fits_exactly", 7, false, "abcdef"},
		{"nontransparent_body_at_max", 6, false, "abcde\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := NewFraming(tc.maxMsg, tc.octet).Frame(body)
			if string(got) != tc.want {
				t.Fatalf("Frame = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestEncodeUTF8Replace pins Python's s.encode("utf-8", "replace"):
// journal bytes that are not valid UTF-8 are lone surrogates in Python and
// re-encode to one '?' EACH; a real U+FFFD passes through unchanged.
func TestEncodeUTF8Replace(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"ascii", "plain", "plain"},
		{"single_invalid_byte", "a\xffb", "a?b"},
		{"two_invalid_bytes", "\xff\xfe", "??"},
		{"invalid_then_valid_utf8", "\xff\xc3\xa9", "?é"},
		{"valid_replacement_char_preserved", "a�b", "a�b"},
		{"valid_multibyte", "héllo", "héllo"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := string(encodeUTF8Replace(tc.in)); got != tc.want {
				t.Fatalf("encodeUTF8Replace(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestLogInvalidUTF8AffectsByteCount proves the replacement happens BEFORE
// framing: an invalid byte becomes a single '?' body byte and counts once
// toward truncation.
func TestLogInvalidUTF8AffectsByteCount(t *testing.T) {
	c, sock := makeClientCFG(t, Config{
		RFC:                 "RFC5424",
		MaxMsg:              4096,
		OctetCountedFraming: true,
	})
	host, msg := "host", "x\xffy"
	if err := c.Log(context.Background(), LogInput{
		Facility: 1, Severity: 6,
		Timestamp: "2024-01-01T00:00:00.000000Z",
		Hostname:  &host, Msg: &msg,
	}); err != nil {
		t.Fatal(err)
	}
	want := "<14>1 2024-01-01T00:00:00.000000Z host - - - x?y\n"
	sp := strings.IndexByte(string(sock.sent), ' ')
	body := sock.sent[sp+1:]
	if string(body) != want {
		t.Fatalf("body = %q, want %q", body, want)
	}
	n := 0
	for _, ch := range string(sock.sent[:sp]) {
		n = n*10 + int(ch-'0')
	}
	if n != len(want) {
		t.Fatalf("advertised length = %d, want %d (invalid byte counted once)", n, len(want))
	}
}

// TestCustomStructuredDataValues pins the CUSTOM %structured-data%
// rendering: Python str.format turns None into the four characters
// "None", while a present value (including "") renders as itself.
func TestCustomStructuredDataValues(t *testing.T) {
	cases := []struct {
		name string
		sd   *string
		want string
	}{
		{"nil_is_None", nil, "None\n"},
		{"empty_is_empty", strPtr(""), "\n"},
		{"populated", strPtr(`x="y"`), `x="y"` + "\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// "{sd}" is what GenerateFormat emits for %structured-data%.
			got, err := FormatCustom("{sd}\n", Fields{SD: tc.sd})
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != tc.want {
				t.Fatalf("sd=%v -> %q, want %q", tc.sd, got, tc.want)
			}
		})
	}
}

func strPtr(s string) *string { return &s }

func TestShouldRetryMatchesPython(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"EPIPE", syscall.EPIPE, true},
		{"ECONNRESET", syscall.ECONNRESET, true},
		{"ETIMEDOUT", syscall.ETIMEDOUT, true},
		{"unexpected-eof-not-retried", io.ErrUnexpectedEOF, false},
		{"eof-not-retried", io.EOF, false},
		{"generic", errors.New("boom"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldRetry(tc.err); got != tc.want {
				t.Fatalf("shouldRetry(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// flakyConn fails the first write with fail and succeeds afterwards.
type flakyConn struct {
	mu    sync.Mutex
	wrote int
	fail  error
}

func (f *flakyConn) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.wrote++
	if f.wrote == 1 {
		return 0, f.fail
	}
	return len(p), nil
}
func (f *flakyConn) Close() error { return nil }

func TestSendRetriesOnceOnRetriable(t *testing.T) {
	fc := &flakyConn{fail: syscall.EPIPE}
	c, err := NewClient(context.Background(), Config{RFC: "RFC5424", Dial: func(context.Context) (Conn, error) { return fc, nil }})
	if err != nil {
		t.Fatal(err)
	}
	// The initial connect consumed no write; Send must retry the write.
	// (The Dial-seam construction does not write, so force a fresh conn.)
	fc.mu.Lock()
	fc.wrote = 0
	fc.mu.Unlock()
	if err := c.Send(context.Background(), []byte("hello\n")); err != nil {
		t.Fatalf("Send: %v (want one retry to succeed)", err)
	}
	if fc.wrote != 2 {
		t.Fatalf("writes = %d, want 2 (fail then retry)", fc.wrote)
	}
}

func TestSendDoesNotRetryNonRetriable(t *testing.T) {
	fc := &flakyConn{fail: io.ErrUnexpectedEOF}
	c, err := NewClient(context.Background(), Config{RFC: "RFC5424", Dial: func(context.Context) (Conn, error) { return fc, nil }})
	if err != nil {
		t.Fatal(err)
	}
	fc.mu.Lock()
	fc.wrote = 0
	fc.mu.Unlock()
	if err := c.Send(context.Background(), []byte("hello\n")); err == nil {
		t.Fatal("Send succeeded, want the non-retriable error")
	}
	if fc.wrote != 1 {
		t.Fatalf("writes = %d, want 1 (no retry for an unexpected EOF)", fc.wrote)
	}
}

// blockingConn blocks in Write until its write deadline is forced.
type blockingConn struct {
	unblocked chan struct{}
	once      sync.Once
}

func newBlockingConn() *blockingConn { return &blockingConn{unblocked: make(chan struct{})} }
func (b *blockingConn) Write(_ []byte) (int, error) {
	<-b.unblocked
	return 0, errors.New("write interrupted")
}
func (b *blockingConn) Close() error { return nil }
func (b *blockingConn) SetWriteDeadline(time.Time) error {
	b.once.Do(func() { close(b.unblocked) })
	return nil
}

// TestSendCancellationUnblocksStalledWrite proves a canceled context
// unblocks a stalled write and returns the ctx error, so a sender worker
// terminates promptly on shutdown/reload.
func TestSendCancellationUnblocksStalledWrite(t *testing.T) {
	bc := newBlockingConn()
	c, err := NewClient(context.Background(), Config{RFC: "RFC5424", Dial: func(context.Context) (Conn, error) { return bc, nil }})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Send(ctx, []byte("hello\n")) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Send returned %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Send did not return after context cancellation (worker would hang)")
	}
}

// tlsServer starts a self-signed TLS listener and reports the SNI it saw
// for each accepted connection.
func tlsServer(t *testing.T) (addr string, sni <-chan string, caPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test.local"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		DNSNames:     []string{"test.local"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	snis := make(chan string, 8)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				if tc, ok := c.(*tls.Conn); ok {
					_ = tc.Handshake()
					snis <- tc.ConnectionState().ServerName
				}
				_ = c.Close()
			}(conn)
		}
	}()
	return ln.Addr().String(), snis, certPEM
}

func dialTo(addr string) func(context.Context) (Conn, error) {
	return func(ctx context.Context) (Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", addr)
	}
}

func poolFromPEM(t *testing.T, caPEM []byte) *x509.CertPool {
	t.Helper()
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		t.Fatal("failed to add CA")
	}
	return pool
}

// TestNewClientTLSTrustedAndSNI proves the handshake is completed during
// construction (not lazily at first write) and that SNI carries the
// configured server name.
func TestNewClientTLSTrustedAndSNI(t *testing.T) {
	addr, sni, caPEM := tlsServer(t)
	c, err := NewClient(context.Background(), Config{
		RFC:       "RFC5424",
		Server:    "test.local",
		Dial:      dialTo(addr),
		TLSConfig: &tls.Config{RootCAs: poolFromPEM(t, caPEM)},
	})
	if err != nil {
		t.Fatalf("NewClient: %v (handshake should have succeeded)", err)
	}
	t.Cleanup(c.Close)
	select {
	case got := <-sni:
		if got != "test.local" {
			t.Fatalf("server SNI = %q, want test.local", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server never observed a completed handshake")
	}
}

// TestNewClientTLSRejectsUntrustedCert proves NewClient fails on an
// untrusted certificate instead of reporting success before verification.
func TestNewClientTLSRejectsUntrustedCert(t *testing.T) {
	addr, _, _ := tlsServer(t)
	_, err := NewClient(context.Background(), Config{
		RFC:       "RFC5424",
		Server:    "test.local",
		Dial:      dialTo(addr),
		TLSConfig: &tls.Config{ /* empty pool: system roots only */ },
	})
	if err == nil {
		t.Fatal("NewClient accepted an untrusted certificate")
	}
}

// TestNewClientTLSRejectsHostnameMismatch proves hostname verification.
func TestNewClientTLSRejectsHostnameMismatch(t *testing.T) {
	addr, _, caPEM := tlsServer(t)
	_, err := NewClient(context.Background(), Config{
		RFC:       "RFC5424",
		Server:    "wrong.example",
		Dial:      dialTo(addr),
		TLSConfig: &tls.Config{RootCAs: poolFromPEM(t, caPEM), ServerName: "wrong.example"},
	})
	if err == nil {
		t.Fatal("NewClient accepted a certificate for the wrong hostname")
	}
}
