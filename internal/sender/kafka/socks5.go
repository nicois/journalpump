// Copyright 2026, Aiven, https://aiven.io
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package kafka

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"time"

	"golang.org/x/net/proxy"
)

// proxyDialTimeout bounds the WHOLE proxy connect: the local broker-name
// lookup (socks5), the TCP connect to the proxy and the SOCKS negotiation.
// The dialer is wired into kgo.Dialer, whose signature carries the request
// context, so the caller's cancellation is honored alongside this deadline;
// without the deadline a black-holed proxy, a stalled resolver or a stalled
// SOCKS handshake hangs the connect (review items C8, 8).
const proxyDialTimeout = 10 * time.Second

// socks5Dialer builds the SOCKS5 dialer for kafka_sender.py's socks5_proxy
// (the Aiven kafka-python fork's option; franz-go surfaces it as
// kgo.Dialer). socks5-kafka is a hard requirement.
//
// Scheme semantics match kafka-python: socks5 resolves the broker hostname
// locally and hands the proxy an IP, while socks5h defers the name to the
// proxy. x/net resolves names remotely in both cases, so a local resolver is
// layered in for socks5 (review item C8).
func socks5Dialer(address string) (*socks5ProxyDialer, error) {
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
	d := &socks5ProxyDialer{inner: inner, timeout: proxyDialTimeout}
	if u.Scheme == "socks5" {
		d.resolve = lookupHost
	}
	return d, nil
}

// socks5ProxyDialer dials through a SOCKS5 proxy under ONE cancellable
// deadline that covers local resolution, the TCP connect and the
// negotiation (review item 8).
type socks5ProxyDialer struct {
	inner   proxy.Dialer
	timeout time.Duration
	// resolve, when non-nil, resolves the target host locally before dialing
	// the proxy (the socks5, not socks5h, behavior).
	resolve func(ctx context.Context, host string) (string, error)
}

// lookupHost is the production local resolver.
func lookupHost(ctx context.Context, host string) (string, error) {
	ips, err := net.DefaultResolver.LookupHost(ctx, host)
	if err != nil {
		return "", err
	}
	if len(ips) == 0 {
		return "", fmt.Errorf("socks5_proxy: no addresses for %s", host)
	}
	return ips[0], nil
}

func (d *socks5ProxyDialer) Dial(network, addr string) (net.Conn, error) {
	return d.DialContext(context.Background(), network, addr)
}

// DialContext bounds the whole exchange by min(ctx, timeout): the caller's
// context (construction/shutdown cancellation) is honored alongside the
// proxy deadline (review round-3 item 2).
func (d *socks5ProxyDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()
	if d.resolve != nil {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}
		if net.ParseIP(host) == nil {
			ip, err := d.resolve(ctx, host)
			if err != nil {
				return nil, err
			}
			addr = net.JoinHostPort(ip, port)
		}
	}
	return dialWithContext(ctx, d.inner, network, addr)
}

// dialWithContext dials under ctx, using the inner dialer's DialContext when
// available (x/net's SOCKS dialer honors the deadline for its handshake) and
// otherwise racing the dial against the deadline, closing a late connection.
func dialWithContext(ctx context.Context, inner proxy.Dialer, network, addr string) (net.Conn, error) {
	if cd, ok := inner.(proxy.ContextDialer); ok {
		return cd.DialContext(ctx, network, addr)
	}
	type result struct {
		conn net.Conn
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		conn, err := inner.Dial(network, addr)
		ch <- result{conn, err}
	}()
	select {
	case <-ctx.Done():
		go func() {
			r := <-ch
			if r.conn != nil {
				_ = r.conn.Close()
			}
		}()
		return nil, ctx.Err()
	case r := <-ch:
		return r.conn, r.err
	}
}
