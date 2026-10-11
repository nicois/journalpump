// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package kafka

import (
	"context"
	"crypto/tls"
	"net"
	"time"
)

// dialTimeout bounds a single TCP connect. franz-go's default dialer uses 10s
// (config.go dialTimeout); because we install our own dialer for the per-broker
// reconnect backoff, that default is replicated here.
const dialTimeout = 10 * time.Second

// kafkaDialer is the kgo.Dialer implementation: it enforces the per-broker
// reconnect backoff (item 2), dials through the SOCKS5 proxy when configured,
// and performs the TLS handshake INSIDE the dialer when the security protocol
// is SSL/SASL_SSL.
//
// Folding TLS into the dialer (item 1, SECURITY) is mandatory, not cosmetic:
// franz-go REJECTS a client configured with BOTH kgo.Dialer and
// kgo.DialTLSConfig ("cannot set both Dialer and DialTLSConfig"), so a custom
// dialer would otherwise silently bypass TLS and send SASL credentials in
// plaintext. Wrapping the proxy/direct connection here keeps the broker
// HOSTNAME for certificate verification and SNI, even when the SOCKS5 proxy
// (socks5, not socks5h) resolved the name to an IP locally.
type kafkaDialer struct {
	backoff *reconnectBackoff
	dial    func(ctx context.Context, network, addr string) (net.Conn, error)
	tlsCfg  *tls.Config
}

// DialContext implements kgo's dialer signature.
func (d *kafkaDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	if err := d.backoff.begin(ctx, addr); err != nil {
		return nil, err
	}
	conn, err := d.dial(ctx, network, addr)
	if err != nil {
		d.backoff.failed(addr)
		return nil, err
	}
	if d.tlsCfg != nil {
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			_ = conn.Close()
			d.backoff.failed(addr)
			return nil, err
		}
		cfg := d.tlsCfg.Clone()
		if cfg.ServerName == "" {
			// The BROKER hostname (from the dial address), never the proxy or
			// the resolved IP, so certificate verification and SNI target the
			// broker the operator configured.
			cfg.ServerName = host
		}
		tlsConn := tls.Client(conn, cfg)
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			_ = conn.Close()
			d.backoff.failed(addr)
			return nil, err
		}
		conn = tlsConn
	}
	d.backoff.connected(addr)
	return conn, nil
}

// tlsEnabled reports whether the security protocol uses TLS.
func (pc producerConfig) tlsEnabled() bool {
	return pc.securityProtocol == "SSL" || pc.securityProtocol == "SASL_SSL"
}

// newDialer builds the kgo dialer for this config: per-broker reconnect
// backoff, SOCKS5 proxy when configured, and TLS folded in when the security
// protocol requires it.
func (pc producerConfig) newDialer() (*kafkaDialer, error) {
	d := &kafkaDialer{backoff: newReconnectBackoff(reconnectBackoffBase, reconnectBackoffMax)}
	if pc.socks5Proxy != "" {
		sd, err := socks5Dialer(pc.socks5Proxy)
		if err != nil {
			return nil, err
		}
		d.dial = sd.DialContext
	} else {
		nd := &net.Dialer{Timeout: dialTimeout}
		d.dial = nd.DialContext
	}
	if pc.tlsEnabled() {
		cfg, err := buildTLSConfig(pc)
		if err != nil {
			return nil, err
		}
		d.tlsCfg = cfg
	}
	return d, nil
}

// sleepCtx sleeps d or returns ctx.Err() — the ctx-aware backoff wait.
func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
