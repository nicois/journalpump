// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package kafka

import (
	"context"
	"errors"
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

// errDialerAborted is returned by the tracking dialer once the connection
// registry has been aborted (review round-3 items 1-3).
var errDialerAborted = errors.New("kafka: connection aborted")

// sleepCtx sleeps for d, returning ctx.Err() if the context ends first.
func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// brokerBackoffState is the per-address reconnect state. next is the instant
// a dial becomes allowed again, so a caller only ever waits the REMAINING
// time, exactly like kafka-python's _can_connect (review round-3 item 4).
type brokerBackoffState struct {
	attempts int
	next     time.Time
}

// backoffDialer implements kafka-python's PER-BROKER reconnect backoff at the
// connection layer (kafka-python conn.py _reconnect_backoff/_can_connect):
// after a failed connection attempt the address waits an exponentially
// growing, jittered interval, and a later dial only sleeps the REMAINING
// part of that interval (elapsed time counts). Addresses are independent.
//
// The history resets ONLY when the complete connection succeeds — TCP dial,
// TLS and SASL included — which the sender signals explicitly with
// markConnected after the client is fully constructed; a TCP connect on its
// own does not reset it, and a post-connect failure is recorded with
// markFailed (review round-3 item 4).
//
// The now/sleep/random functions are seams for tests.
type backoffDialer struct {
	inner     proxy.Dialer
	cfg       brokerBackoffConfig
	now       func() time.Time
	sleep     func(context.Context, time.Duration) error
	randFloat func() float64

	mu    sync.Mutex
	state map[string]*brokerBackoffState
}

func newBackoffDialer(inner proxy.Dialer, cfg brokerBackoffConfig) *backoffDialer {
	return &backoffDialer{
		inner:     inner,
		cfg:       cfg,
		now:       time.Now,
		sleep:     sleepCtx,
		randFloat: rand.Float64,
		state:     make(map[string]*brokerBackoffState),
	}
}

func (d *backoffDialer) Dial(network, addr string) (net.Conn, error) {
	return d.DialContext(context.Background(), network, addr)
}

func (d *backoffDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	if wait := d.remaining(addr); wait > 0 {
		if err := d.sleep(ctx, wait); err != nil {
			return nil, err
		}
	}
	conn, err := dialWithContext(ctx, d.inner, network, addr)
	if err != nil {
		d.markFailed(addr)
	}
	// A successful dial does NOT reset the history: TLS/SASL may still fail.
	return conn, err
}

// remaining is the time left before a dial to addr is allowed again (zero
// when none, or once the interval has elapsed).
func (d *backoffDialer) remaining(addr string) time.Duration {
	d.mu.Lock()
	defer d.mu.Unlock()
	st := d.state[addr]
	if st == nil {
		return 0
	}
	if wait := st.next.Sub(d.now()); wait > 0 {
		return wait
	}
	return 0
}

// markFailed records a failed connection attempt (a transport error or a
// post-connect handshake failure) and schedules the next allowed dial.
func (d *backoffDialer) markFailed(addrs ...string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, addr := range addrs {
		st := d.state[addr]
		if st == nil {
			st = &brokerBackoffState{}
			d.state[addr] = st
		}
		st.attempts++
		st.next = d.now().Add(jitter(d.backoff(st.attempts), d.randFloat()))
	}
}

// markConnected resets the history for addresses whose connection fully
// succeeded.
func (d *backoffDialer) markConnected(addrs ...string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, addr := range addrs {
		delete(d.state, addr)
	}
}

// backoff is the un-jittered delay for the given attempt count: the initial
// value doubling up to the cap.
func (d *backoffDialer) backoff(attempts int) time.Duration {
	delay := d.cfg.initial
	for i := 1; i < attempts; i++ {
		delay *= 2
		if delay >= d.cfg.max {
			return d.cfg.max
		}
	}
	return delay
}

// jitter applies kafka-python's uniform ±20% (random.uniform(0.8, 1.2)).
func jitter(base time.Duration, r float64) time.Duration {
	return time.Duration(float64(base) * (0.8 + 0.4*r))
}

// trackingDialer records every connection it hands out so the sender can
// TERMINATE them (aborting in-flight produce/metadata/admin/SOCKS requests)
// before joining the worker that owns them (review round-3 items 1-3). Its
// dials also honor the construction context, so an in-flight dial is
// abandoned when that context ends rather than running to its own timeout
// (review round-3 item 2).
type trackingDialer struct {
	inner proxy.Dialer
	ctx   context.Context

	mu     sync.Mutex
	conns  map[net.Conn]struct{}
	closed bool
}

func newTrackingDialer(ctx context.Context, inner proxy.Dialer) *trackingDialer {
	if ctx == nil {
		ctx = context.Background()
	}
	return &trackingDialer{inner: inner, ctx: ctx, conns: make(map[net.Conn]struct{})}
}

func (d *trackingDialer) Dial(network, addr string) (net.Conn, error) {
	return d.DialContext(d.ctx, network, addr)
}

func (d *trackingDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil, errDialerAborted
	}
	d.mu.Unlock()
	conn, err := dialWithContext(ctx, d.inner, network, addr)
	if err != nil {
		return nil, err
	}
	tracked := &trackedConn{Conn: conn, owner: d}
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		_ = conn.Close()
		return nil, errDialerAborted
	}
	d.conns[tracked] = struct{}{}
	d.mu.Unlock()
	return tracked, nil
}

// closeAll terminates every live connection and refuses new ones, so any
// request in flight on them fails at once.
func (d *trackingDialer) closeAll() {
	d.mu.Lock()
	d.closed = true
	conns := make([]net.Conn, 0, len(d.conns))
	for c := range d.conns {
		conns = append(conns, c)
	}
	d.conns = make(map[net.Conn]struct{})
	d.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
}

func (d *trackingDialer) remove(c net.Conn) {
	d.mu.Lock()
	delete(d.conns, c)
	d.mu.Unlock()
}

// trackedConn unregisters itself on close.
type trackedConn struct {
	net.Conn
	owner *trackingDialer
}

func (c *trackedConn) Close() error {
	c.owner.remove(c)
	return c.Conn.Close()
}

// dialerFor builds the connection dialer chain for the resolved config: the
// SOCKS5 dialer when socks5_proxy is set, else a direct dialer carrying
// sarama's dial timeout/keepalive, wrapped by the per-broker backoff dialer.
// The caller wraps the result in a trackingDialer for abort/cancellation.
func dialerFor(pc producerConfig) (*backoffDialer, error) {
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
