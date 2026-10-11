// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package kafka

import (
	"context"
	"testing"
	"time"
)

// fakeClock drives the reconnect backoff deterministically: now reads the
// virtual time, sleep advances it by d and records the wait.
type fakeClock struct {
	t      time.Time
	slept  time.Duration
	sleeps int
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Unix(1_700_000_000, 0)}
}

func (c *fakeClock) now() time.Time { return c.t }

func (c *fakeClock) sleepFn(_ context.Context, d time.Duration) error {
	c.sleeps++
	c.slept += d
	c.t = c.t.Add(d)
	return nil
}

// newTestBackoff builds a backoff with a virtual clock and no jitter, so the
// intervals are exact.
func newTestBackoff(c *fakeClock) *reconnectBackoff {
	b := newReconnectBackoff(reconnectBackoffBase, reconnectBackoffMax)
	b.now = c.now
	b.sleep = c.sleepFn
	b.jitter = func() float64 { return 1.0 }
	return b
}

// TestReconnectBackoffWaitsOnlyRemaining locks the elapsed-time accounting:
// the next attempt waits only the interval REMAINING from the previous
// attempt's start (kafka-python measures from the top of connect()).
func TestReconnectBackoffWaitsOnlyRemaining(t *testing.T) {
	c := newFakeClock()
	b := newTestBackoff(c)

	if err := b.begin(context.Background(), "broker:9092"); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if c.sleeps != 0 {
		t.Fatalf("first attempt slept %v, want none", c.slept)
	}
	// The first attempt consumed 300ms of its 1s interval.
	c.t = c.t.Add(300 * time.Millisecond)

	if err := b.begin(context.Background(), "broker:9092"); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if c.sleeps != 1 || c.slept != 700*time.Millisecond {
		t.Fatalf("second attempt slept %v (%d sleeps), want exactly 700ms", c.slept, c.sleeps)
	}
}

// TestReconnectBackoffMeasuresFromAttemptStart: a slow previous attempt that
// already exceeded the interval leaves NO wait for the next one.
func TestReconnectBackoffMeasuresFromAttemptStart(t *testing.T) {
	c := newFakeClock()
	b := newTestBackoff(c)

	if err := b.begin(context.Background(), "broker:9092"); err != nil {
		t.Fatalf("begin: %v", err)
	}
	// The dial itself was slow (2s > the 1s interval).
	c.t = c.t.Add(2 * time.Second)

	if err := b.begin(context.Background(), "broker:9092"); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if c.sleeps != 0 {
		t.Fatalf("next attempt slept %v, want none (interval already elapsed)", c.slept)
	}
}

// TestReconnectBackoffGrowsCapsAndResets locks the exponential growth from
// base to max and the reset on a complete connection (mark_connected).
func TestReconnectBackoffGrowsCapsAndResets(t *testing.T) {
	c := newFakeClock()
	b := newTestBackoff(c)
	const addr = "broker:9092"

	if got := b.backoffFor(addr); got != reconnectBackoffBase {
		t.Fatalf("initial backoff = %v, want %v", got, reconnectBackoffBase)
	}
	want := []time.Duration{2 * time.Second, 4 * time.Second, 8 * time.Second, 10 * time.Second, 10 * time.Second}
	for i, w := range want {
		b.failed(addr)
		if got := b.backoffFor(addr); got != w {
			t.Fatalf("after failure %d: backoff = %v, want %v", i+1, got, w)
		}
	}
	b.connected(addr)
	if got := b.backoffFor(addr); got != reconnectBackoffBase {
		t.Fatalf("after connected: backoff = %v, want reset to %v", got, reconnectBackoffBase)
	}
}

// TestReconnectBackoffIsPerAddress: one broker's backoff must not affect
// another's schedule.
func TestReconnectBackoffIsPerAddress(t *testing.T) {
	c := newFakeClock()
	b := newTestBackoff(c)

	b.failed("a:9092")
	b.failed("a:9092")
	if got := b.backoffFor("a:9092"); got != 4*time.Second {
		t.Fatalf("a backoff = %v, want 4s", got)
	}
	if got := b.backoffFor("b:9092"); got != reconnectBackoffBase {
		t.Fatalf("b backoff = %v, want the untouched base %v", got, reconnectBackoffBase)
	}
}

// TestReconnectBackoffHonorsContext: a cancelled context aborts the enforced
// wait promptly (so cancellation is not delayed by the backoff).
func TestReconnectBackoffHonorsContext(t *testing.T) {
	b := newReconnectBackoff(reconnectBackoffBase, reconnectBackoffMax)
	ctx, cancel := context.WithCancel(context.Background())
	if err := b.begin(ctx, "broker:9092"); err != nil {
		t.Fatalf("first begin: %v", err)
	}
	cancel()
	start := time.Now()
	if err := b.begin(ctx, "broker:9092"); err == nil {
		t.Fatal("begin with a cancelled context: want error")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("cancelled backoff wait took %v, want prompt", elapsed)
	}
}
