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
// failed attempt.
//
// franz-go has no per-broker reconnect hook of its own, so the scheduling is
// re-expressed on the dialer it does expose (kgo.Dialer): the dialer blocks
// for the remaining interval before dialing and records the attempt, and then
// grows/resets the address's state based on the dial outcome. The clock,
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
type reconnectState struct {
	mu          sync.Mutex
	lastAttempt time.Time
	backoff     time.Duration
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
		st = &reconnectState{backoff: b.base}
		b.state[addr] = st
	}
	return st
}

// effective applies the jitter to the current backoff for this attempt.
func (b *reconnectBackoff) effective(backoff time.Duration) time.Duration {
	if backoff <= 0 {
		backoff = b.base
	}
	return time.Duration(float64(backoff) * b.jitter())
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
		next := st.lastAttempt.Add(b.effective(st.backoff))
		if now.Before(next) {
			if err := b.sleep(ctx, next.Sub(now)); err != nil {
				return err
			}
		}
	}
	st.lastAttempt = b.now()
	return nil
}

// connected resets the address's backoff to base — kafka-python's
// mark_connected after a complete connection.
func (b *reconnectBackoff) connected(addr string) {
	st := b.stateFor(addr)
	st.mu.Lock()
	defer st.mu.Unlock()
	st.backoff = b.base
}

// failed grows the address's backoff (doubling, capped at max) —
// kafka-python's mark_failed.
func (b *reconnectBackoff) failed(addr string) {
	st := b.stateFor(addr)
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.backoff <= 0 {
		st.backoff = b.base
	} else {
		st.backoff *= 2
	}
	if st.backoff > b.max {
		st.backoff = b.max
	}
}

// backoffFor exposes the current interval for tests.
func (b *reconnectBackoff) backoffFor(addr string) time.Duration {
	st := b.stateFor(addr)
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.backoff
}
