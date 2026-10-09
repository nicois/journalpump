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

// proxyDialTimeout bounds the proxy TCP connect plus the SOCKS handshake.
// sarama dials through the proxy with a context-less Dial, so a deadline
// must live in the dialer; without it a black-holed proxy or a stalled
// SOCKS negotiation hangs the connect forever (review item C8).
const proxyDialTimeout = 10 * time.Second

// socks5Dialer builds the SOCKS5 dialer for kafka_sender.py's
// socks5_proxy (the Aiven kafka-python fork's option; sarama surfaces it
// as Net.Proxy.Dialer). socks5-kafka is a hard requirement.
//
// Scheme semantics match kafka-python: socks5 resolves the broker
// hostname locally and hands the proxy an IP, while socks5h defers the
// name to the proxy. x/net resolves names remotely in both cases, so a
// local resolver is layered in for socks5 (review item C8).
func socks5Dialer(address string) (proxy.Dialer, error) {
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
	var dialer proxy.Dialer = &timeoutDialer{inner: inner, timeout: proxyDialTimeout}
	if u.Scheme == "socks5" {
		dialer = &localResolverDialer{inner: dialer}
	}
	return dialer, nil
}

// timeoutDialer bounds a context-less Dial by dialing with a deadline: the
// returned context is honoured by x/net's SOCKS dialer, which sets the
// connection deadline for the handshake.
type timeoutDialer struct {
	inner   proxy.Dialer
	timeout time.Duration
}

func (d *timeoutDialer) Dial(network, addr string) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), d.timeout)
	defer cancel()
	if cd, ok := d.inner.(proxy.ContextDialer); ok {
		return cd.DialContext(ctx, network, addr)
	}
	// Fallback for a dialer without DialContext: race the dial against the
	// deadline, closing a late connection.
	type result struct {
		conn net.Conn
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		conn, err := d.inner.Dial(network, addr)
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

// localResolverDialer resolves the target hostname locally before dialing
// the proxy, reproducing kafka-python's socks5 (not socks5h) behavior.
type localResolverDialer struct {
	inner proxy.Dialer
}

func (d *localResolverDialer) Dial(network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	if net.ParseIP(host) == nil {
		ips, err := net.DefaultResolver.LookupHost(context.Background(), host)
		if err != nil {
			return nil, err
		}
		if len(ips) == 0 {
			return nil, fmt.Errorf("socks5_proxy: no addresses for %s", host)
		}
		addr = net.JoinHostPort(ips[0], port)
	}
	return d.inner.Dial(network, addr)
}
