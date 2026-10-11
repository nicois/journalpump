// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package kafka

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

// Round-3 regression tests: cancellation and the delivery deadline must be
// genuinely ABORTIVE and end to end. franz-go is context-aware, so the
// produce path needs no socket-force-close workaround: on ctx.Done the
// batch's client is closed (a documented library API that fails the in-flight
// records) and the worker is joined. These tests exercise that mechanism
// through the producer seam (a stalled produce) and through the REAL franz-go
// client (a stalled metadata handshake against a silent broker).

// --- stalled producer: Produce blocks until the client is closed ------------

// stalledProducer models a produce whose response never arrives: Produce
// blocks until Close (mirroring kgo.Client.Close, which fails buffered and
// in-flight records) or until the context ends.
type stalledProducer struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	mu      sync.Mutex
	closed  bool
}

func newStalledProducer() *stalledProducer {
	return &stalledProducer{entered: make(chan struct{}), release: make(chan struct{})}
}

func (p *stalledProducer) Produce(ctx context.Context, _ [][]byte) error {
	p.once.Do(func() { close(p.entered) })
	select {
	case <-p.release:
	case <-ctx.Done():
	}
	return ctx.Err()
}

func (p *stalledProducer) Close() error {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	// Fails the in-flight produce, like kgo.Client.Close.
	select {
	case <-p.release:
	default:
		close(p.release)
	}
	return nil
}

// --- Item 1: abortive delivery deadline ------------------------------------

func TestSendDeliveryDeadlineAbortsStalledProduce(t *testing.T) {
	s := newSender(t, baseConfig())
	sp := newStalledProducer()
	s.newProducer = func(context.Context, producerConfig) (producer, error) { return sp, nil }
	s.deliveryDeadline = 100 * time.Millisecond

	start := time.Now()
	err := s.SendMessages(context.Background(), [][]byte{[]byte("payload")}, "c0")
	elapsed := time.Since(start)

	select {
	case <-sp.entered:
	default:
		t.Fatalf("Produce was never entered; err=%v", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("SendMessages error = %v, want context.DeadlineExceeded", err)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("SendMessages took %v; the abortive delivery deadline did not bound it", elapsed)
	}
	s.mu.Lock()
	installed := s.prod
	s.mu.Unlock()
	if installed != nil {
		t.Error("producer left installed after a deadline abort; the next send would reuse a stalled client")
	}
}

// --- Item 1/3: abortive in-flight cancellation -----------------------------

func TestSendCancellationAbortsStalledProduce(t *testing.T) {
	s := newSender(t, baseConfig())
	sp := newStalledProducer()
	s.newProducer = func(context.Context, producerConfig) (producer, error) { return sp, nil }
	// No deadline: the ONLY thing that can end this send is the cancellation.
	s.deliveryDeadline = time.Hour

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.SendMessages(ctx, [][]byte{[]byte("payload")}, "c0") }()

	select {
	case <-sp.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("send never reached the producer")
	}
	start := time.Now()
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("SendMessages error = %v, want context.Canceled", err)
		}
		if elapsed := time.Since(start); elapsed > 5*time.Second {
			t.Fatalf("SendMessages took %v to return after cancellation", elapsed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SendMessages did not return after cancellation; the worker was not joined")
	}
	s.mu.Lock()
	installed := s.prod
	s.mu.Unlock()
	if installed != nil {
		t.Error("cancelled send left a producer installed")
	}
}

// --- Item 3: Close aborts and JOINS without a timer ------------------------

func TestCloseAbortsAndJoinsWithoutTimer(t *testing.T) {
	s := newSender(t, baseConfig())
	sp := newStalledProducer()
	s.newProducer = func(context.Context, producerConfig) (producer, error) { return sp, nil }
	s.deliveryDeadline = time.Hour

	if _, err := s.ensureProducer(context.Background()); err != nil {
		t.Fatalf("ensureProducer: %v", err)
	}
	sendDone := make(chan error, 1)
	go func() { sendDone <- s.SendMessages(context.Background(), [][]byte{[]byte("payload")}, "c0") }()

	select {
	case <-sp.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("send never reached the producer")
	}
	start := time.Now()
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("Close took %v; it must abort and join, not wait on a timer", elapsed)
	}
	// The blocked send must be released by the abort and the worker joined.
	select {
	case <-sendDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Close returned but the blocked send worker never finished")
	}
}

// --- Item 2: construction honors the caller's context (REAL franz-go) ------

// startSilentBroker accepts connections and never answers anything, modelling
// a stalled metadata handshake.
func startSilentBroker(t *testing.T) (addr string, requestSeen <-chan struct{}) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	first := make(chan struct{})
	var once sync.Once
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer func() { _ = c.Close() }()
				buf := make([]byte, 4096)
				for {
					n, err := c.Read(buf)
					if n > 0 {
						once.Do(func() { close(first) })
					}
					if err != nil {
						return
					}
				}
			}(conn)
		}
	}()
	return ln.Addr().String(), first
}

// brokerSender builds a Sender whose config points at addr with an explicit
// api_version (so no version negotiation is inferred) and a short-ish path to
// constructing a real franz-go client.
func brokerSender(t *testing.T, addr string) *Sender {
	t.Helper()
	raw := baseConfig()
	raw["kafka_address"] = addr
	raw["kafka_api_version"] = "0.10.0.0"
	raw["kafka_topic"] = "test-topic"
	return newSender(t, raw)
}

// TestConnectProducerCancellationTerminatesMetadata: with the REAL franz-go
// client, a metadata handshake blocked against a silent broker is terminated
// by the construction context WITHOUT any socket-force-close workaround —
// franz-go's dials and metadata requests take the context, so connectProducer
// returns promptly.
func TestConnectProducerCancellationTerminatesMetadata(t *testing.T) {
	addr, requestSeen := startSilentBroker(t)
	s := brokerSender(t, addr)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := s.ensureProducer(ctx); done <- err }()

	select {
	case <-requestSeen:
	case <-time.After(5 * time.Second):
		t.Fatal("connection never reached the silent broker")
	}
	start := time.Now()
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ensureProducer error = %v, want context.Canceled", err)
		}
		if elapsed := time.Since(start); elapsed > 5*time.Second {
			t.Fatalf("ensureProducer took %v to honor cancellation", elapsed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ensureProducer did not return after cancellation; the metadata handshake was not terminated")
	}
}

// --- Item 5: the client delivery deadline is distinct from request timeout -

func TestDeliveryDeadlineIsClientSideAndDistinct(t *testing.T) {
	s := newSender(t, baseConfig())
	if s.deliveryDeadline != deliveryTimeout {
		t.Errorf("deliveryDeadline = %v, want %v", s.deliveryDeadline, deliveryTimeout)
	}
	if s.deliveryDeadline == requestTimeout {
		t.Fatal("the client-side delivery deadline must differ from the broker-side request timeout")
	}
}
