// Copyright 2026, Aiven, https://aiven.io
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package kafka

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"golang.org/x/net/proxy"
)

// socksRequest records the destination the client asked the proxy to
// reach: the ATYP byte and the address (an IP for IPv4/IPv6, a name for
// ATYP=3).
type socksRequest struct {
	atyp byte
	addr string
}

// startSocksServer runs a minimal SOCKS5 server that records one CONNECT
// request and then answers success.
func startSocksServer(t *testing.T) (string, <-chan socksRequest) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	requests := make(chan socksRequest, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		// Read the client greeting, then select "no authentication".
		greeting := make([]byte, 2)
		if _, err := io.ReadFull(conn, greeting); err != nil {
			return
		}
		methods := make([]byte, int(greeting[1]))
		if _, err := io.ReadFull(conn, methods); err != nil {
			return
		}
		if _, err := conn.Write([]byte{5, 0}); err != nil {
			return
		}
		header := make([]byte, 4)
		if _, err := io.ReadFull(conn, header); err != nil {
			return
		}
		var addr string
		switch header[3] {
		case 1:
			b := make([]byte, 4)
			_, _ = io.ReadFull(conn, b)
			addr = net.IP(b).String()
		case 4:
			b := make([]byte, 16)
			_, _ = io.ReadFull(conn, b)
			addr = net.IP(b).String()
		case 3:
			l := make([]byte, 1)
			_, _ = io.ReadFull(conn, l)
			b := make([]byte, int(l[0]))
			_, _ = io.ReadFull(conn, b)
			addr = string(b)
		}
		portb := make([]byte, 2)
		_, _ = io.ReadFull(conn, portb)
		requests <- socksRequest{atyp: header[3], addr: addr}
		// Success, bound address 0.0.0.0:0.
		_, _ = conn.Write([]byte{5, 0, 0, 1, 0, 0, 0, 0, 0, 0})
		_, _ = io.Copy(io.Discard, conn)
	}()
	return ln.Addr().String(), requests
}

// TestSocks5ResolvesLocally pins the scheme-specific resolution: socks5
// resolves the broker name locally and sends the proxy an IP, whereas
// socks5h sends the name for the proxy to resolve (review item C8).
func TestSocks5ResolvesLocally(t *testing.T) {
	for _, tc := range []struct {
		scheme       string
		target       string
		wantName     string
		wantResolved bool
	}{
		{"socks5", "localhost:9092", "", true},
		{"socks5h", "broker.example.invalid:9092", "broker.example.invalid", false},
	} {
		t.Run(tc.scheme, func(t *testing.T) {
			proxyAddr, requests := startSocksServer(t)
			dialer, err := socks5Dialer(tc.scheme + "://" + proxyAddr)
			if err != nil {
				t.Fatalf("socks5Dialer: %v", err)
			}
			conn, err := dialer.Dial("tcp", tc.target)
			if err != nil {
				t.Fatalf("Dial: %v", err)
			}
			defer conn.Close()
			select {
			case req := <-requests:
				if tc.wantResolved {
					if req.atyp == 3 {
						t.Fatalf("socks5 sent a hostname %q; want a locally-resolved IP", req.addr)
					}
					if ip := net.ParseIP(req.addr); ip == nil || !ip.IsLoopback() {
						t.Fatalf("socks5 destination = %q, want a loopback IP", req.addr)
					}
				} else {
					if req.atyp != 3 || req.addr != tc.wantName {
						t.Fatalf("socks5h destination = atyp %d %q, want a hostname %q", req.atyp, req.addr, tc.wantName)
					}
				}
			case <-time.After(2 * time.Second):
				t.Fatal("proxy did not receive a request")
			}
		})
	}
}

func TestSocks5DialerRejectsBadScheme(t *testing.T) {
	if _, err := socks5Dialer("http://proxy:8080"); err == nil {
		t.Error("http scheme: want error, got nil")
	}
}

// blockingDialer is a proxy.Dialer without DialContext that never returns.
type blockingDialer struct{ release chan struct{} }

func (d *blockingDialer) Dial(string, string) (net.Conn, error) {
	<-d.release
	return nil, context.DeadlineExceeded
}

// TestTimeoutDialerBoundsHandshake: a dialer that never completes must be
// abandoned once the timeout elapses (review item C8).
func TestTimeoutDialerBoundsHandshake(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	d := &timeoutDialer{inner: &blockingDialer{release: release}, timeout: 100 * time.Millisecond}
	start := time.Now()
	if _, err := d.Dial("tcp", "broker:9092"); err == nil {
		t.Fatal("Dial: want timeout error, got nil")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Dial took %v, want bounded near 100ms", elapsed)
	}
}

// TestSocks5DialerHasTimeout asserts the production proxy dialer is
// wrapped with the handshake bound.
func TestSocks5DialerHasTimeout(t *testing.T) {
	proxyAddr, _ := startSocksServer(t)
	dialer, err := socks5Dialer("socks5h://" + proxyAddr)
	if err != nil {
		t.Fatalf("socks5Dialer: %v", err)
	}
	if _, ok := dialer.(*timeoutDialer); !ok {
		t.Fatalf("dialer = %T, want *timeoutDialer", dialer)
	}
	var _ proxy.Dialer = dialer
}
