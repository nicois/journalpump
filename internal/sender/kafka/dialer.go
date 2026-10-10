// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package kafka

import (
	"math/rand"
	"net"
	"sync"
	"time"

	"golang.org/x/net/proxy"
)

// brokerBackoffConfig carries kafka-python's reconnect_backoff_ms /
// reconnect_backoff_max_ms (kafka_sender.py:59-60).
type brokerBackoffConfig struct {
	initial time.Duration
	max     time.Duration
}

// defaultBrokerBackoff is the deployed reconnect contract: 1 s doubling to a
// 10 s cap.
func defaultBrokerBackoff() brokerBackoffConfig {
	return brokerBackoffConfig{
		initial: time.Duration(reconnectBackoffMS) * time.Millisecond,
		max:     time.Duration(reconnectBackoffMaxMS) * time.Millisecond,
	}
}

// backoffDialer implements kafka-python's PER-BROKER reconnect backoff at the
// connection layer: after a failed dial to an address it waits an
// exponentially growing, jittered interval before dialing that same address
// again, and resets the moment a dial succeeds. Addresses are tracked
// independently. This is the reconnect contract itself — sarama's
// Metadata.Retry.Backoff only paces metadata retries and leaves other
// connection paths unaffected (review item 6).
//
// The sleep/monotonic/random functions are seams for tests.
type backoffDialer struct {
	inner     proxy.Dialer
	cfg       brokerBackoffConfig
	sleep     func(time.Duration)
	randFloat func() float64

	mu   sync.Mutex
	next map[string]time.Duration
}

func newBackoffDialer(inner proxy.Dialer, cfg brokerBackoffConfig) *backoffDialer {
	return &backoffDialer{
		inner:     inner,
		cfg:       cfg,
		sleep:     time.Sleep,
		randFloat: rand.Float64,
		next:      make(map[string]time.Duration),
	}
}

func (d *backoffDialer) Dial(network, addr string) (net.Conn, error) {
	d.mu.Lock()
	wait := d.next[addr]
	d.mu.Unlock()
	if wait > 0 {
		d.sleep(jitter(wait, d.randFloat()))
	}
	conn, err := d.inner.Dial(network, addr)
	d.mu.Lock()
	if err != nil {
		d.next[addr] = d.grow(wait)
	} else {
		delete(d.next, addr)
	}
	d.mu.Unlock()
	return conn, err
}

// grow returns the next backoff for an address: the initial value after the
// first failure, doubling up to the cap thereafter.
func (d *backoffDialer) grow(current time.Duration) time.Duration {
	if current <= 0 {
		return d.cfg.initial
	}
	next := current * 2
	if next > d.cfg.max {
		return d.cfg.max
	}
	return next
}

// jitter applies kafka-python's uniform ±20% (random.uniform(0.8, 1.2)).
func jitter(base time.Duration, r float64) time.Duration {
	return time.Duration(float64(base) * (0.8 + 0.4*r))
}

// dialerFor builds the connection dialer chain for the resolved config: the
// SOCKS5 dialer when socks5_proxy is set, else a direct dialer carrying
// sarama's dial timeout/keepalive, wrapped by the per-broker backoff dialer.
func dialerFor(pc producerConfig) (proxy.Dialer, error) {
	var base proxy.Dialer
	if pc.socks5Proxy != "" {
		d, err := socks5Dialer(pc.socks5Proxy)
		if err != nil {
			return nil, err
		}
		base = d
	} else {
		base = &net.Dialer{Timeout: requestTimeout, KeepAlive: 30 * time.Second}
	}
	return newBackoffDialer(base, defaultBrokerBackoff()), nil
}
