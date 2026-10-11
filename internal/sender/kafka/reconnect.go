// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package kafka

import (
	"context"
	"math/rand"
	"sync"
	"time"
)

// Per-broker reconnect backoff defaults mirroring kafka-python
// (reconnect_backoff_ms / reconnect_backoff_max_ms; journalpump leaves the
// kafka-python defaults in place).
const (
	reconnectBackoffBase = 1 * time.Second
	reconnectBackoffMax  = 10 * time.Second
)

// reconnectBackoff reproduces kafka-python BrokerConnection's reconnect
// scheduling per broker ADDRESS: an exponential interval from base to max
// with ±20% jitter, measured from the START of each connection attempt
// (kafka-python sets last_attempt at the top of connect()), reset to base
// after a COMPLETE connection succeeds (mark_connected), and grown after a
// failed attempt. The interval is scheduled BEFORE it is increased, so the
// waits run 1s, 2s, 4s, 8s, 10s (not 2s, 4s, ... for a first retry).
//
// franz-go has no per-broker reconnect hook of its own, so the scheduling is
// re-expressed on the dialer it does expose (kgo.Dialer): the dialer blocks
// for the remaining interval before dialing and records the attempt start.
// Success/failure accounting does NOT live on the dialer (a TCP/TLS success
// is not yet a Kafka connection): the paired brokerConnectHook (dial.go)
// grows/resets the address's state from franz-go's COMPLETE-connection
// outcome (HookBrokerConnect, after ApiVersions + any SASL flow). The clock,
// sleep and jitter are injectable so the scheduling is unit-testable without
// real sleeps.
type reconnectBackoff struct {
	base   time.Duration
	max    time.Duration
	now    func() time.Time
	sleep  func(context.Context, time.Duration) error
	jitter func() float64

	mu    sync.Mutex
	state map[string]*reconnectState
}

// reconnectState is one broker address's scheduling state. Its own mutex is
// held across the enforced wait so concurrent dials to the SAME address are
// serialized, while dials to DIFFERENT addresses stay independent.
//
// failures counts CONSECUTIVE failed connection attempts since the last
// complete connection; the interval for the NEXT attempt is derived from it
// (see intervalFor), so the first retry after the initial 1s interval is 1s
// again — not 2s from doubling before the interval was ever used.
type reconnectState struct {
	mu          sync.Mutex
	lastAttempt time.Time
	failures    int
}

func newReconnectBackoff(base, maxBackoff time.Duration) *reconnectBackoff {
	return &reconnectBackoff{
		base:   base,
		max:    maxBackoff,
		now:    time.Now,
		sleep:  sleepCtx,
		jitter: func() float64 { return 0.8 + rand.Float64()*0.4 },
		state:  make(map[string]*reconnectState),
	}
}

// stateFor returns (creating if needed) the state for addr.
func (b *reconnectBackoff) stateFor(addr string) *reconnectState {
	b.mu.Lock()
	defer b.mu.Unlock()
	st := b.state[addr]
	if st == nil {
		st = &reconnectState{}
		b.state[addr] = st
	}
	return st
}

// intervalFor is the reconnect interval used BEFORE the next attempt, given
// the number of consecutive failures so far: base for 0 or 1 failures
// (the initial interval is not doubled before its first use), then 2x per
// further failure, capped at max. Sequence for 1s base / 10s max:
// 1s, 2s, 4s, 8s, 10s, 10s.
func (b *reconnectBackoff) intervalFor(failures int) time.Duration {
	if failures < 1 {
		return b.base
	}
	d := b.base
	for i := 1; i < failures; i++ {
		if d >= b.max {
			return b.max
		}
		d *= 2
	}
	if d > b.max {
		d = b.max
	}
	return d
}

// effective applies the jitter to the interval for this attempt.
func (b *reconnectBackoff) effective(interval time.Duration) time.Duration {
	if interval <= 0 {
		interval = b.base
	}
	return time.Duration(float64(interval) * b.jitter())
}

// begin enforces the per-address reconnect interval and records the attempt
// start. It blocks (ctx-aware) for the remaining interval, so a slow previous
// attempt that already consumed the interval leaves no wait and a prompt one
// waits only the remainder. The recorded attempt time is the point dialing
// actually starts, so a slow dial counts toward the NEXT interval.
func (b *reconnectBackoff) begin(ctx context.Context, addr string) error {
	st := b.stateFor(addr)
	st.mu.Lock()
	defer st.mu.Unlock()
	now := b.now()
	if !st.lastAttempt.IsZero() {
		next := st.lastAttempt.Add(b.effective(b.intervalFor(st.failures)))
		if now.Before(next) {
			if err := b.sleep(ctx, next.Sub(now)); err != nil {
				return err
			}
		}
	}
	st.lastAttempt = b.now()
	return nil
}

// connected resets the address's failure count to zero — kafka-python's
// mark_connected after a COMPLETE connection (franz-go's HookBrokerConnect
// with a nil error, i.e. after ApiVersions + any SASL flow).
func (b *reconnectBackoff) connected(addr string) {
	st := b.stateFor(addr)
	st.mu.Lock()
	defer st.mu.Unlock()
	st.failures = 0
}

// failed records a complete-connection failure, growing the next interval —
// kafka-python's mark_failed.
func (b *reconnectBackoff) failed(addr string) {
	st := b.stateFor(addr)
	st.mu.Lock()
	defer st.mu.Unlock()
	st.failures++
}

// backoffFor exposes the interval that would be used before the next attempt.
func (b *reconnectBackoff) backoffFor(addr string) time.Duration {
	st := b.stateFor(addr)
	st.mu.Lock()
	defer st.mu.Unlock()
	return b.intervalFor(st.failures)
}

// failuresFor exposes the consecutive-failure count for tests.
func (b *reconnectBackoff) failuresFor(addr string) int {
	st := b.stateFor(addr)
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.failures
}
