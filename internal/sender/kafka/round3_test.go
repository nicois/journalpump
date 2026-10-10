// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package kafka

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"
)

// Round-3 regression tests: the cancellation/deadline machinery must be
// genuinely ABORTIVE (terminate network operations and retries BEFORE
// joining), the construction context must be propagated, Close must join
// without a timer, Producer.Timeout must carry request_timeout_ms, and the
// reconnect backoff must track remaining time and complete connection
// outcomes. The delivery-deadline/cancellation tests run against a REAL
// sarama producer pointed at a broker that answers metadata but stalls
// Produce — a fake whose Close releases the send would not exercise the abort.

// --- fake broker: metadata works, Produce stalls --------------------------

const (
	apiKeyProduce  = 0
	apiKeyMetadata = 3
)

func kint16(v int16) []byte { return binary.BigEndian.AppendUint16(nil, uint16(v)) }
func kint32(v int32) []byte { return binary.BigEndian.AppendUint32(nil, uint32(v)) }

func kstring(s string) []byte {
	b := binary.BigEndian.AppendUint16(nil, uint16(len(s)))
	return append(b, s...)
}

func knullableString() []byte { return kint16(-1) }

func kbool(v bool) []byte {
	if v {
		return []byte{1}
	}
	return []byte{0}
}

func kint32array(vs ...int32) []byte {
	b := kint32(int32(len(vs)))
	for _, v := range vs {
		b = append(b, kint32(v)...)
	}
	return b
}

// metadataV1Body encodes a Metadata v1 response advertising a single broker
// (host:port) leading a single-partition topic.
func metadataV1Body(host string, port int32, topic string) []byte {
	var b []byte
	b = append(b, kint32(1)...) // brokers
	b = append(b, kint32(0)...) // node id
	b = append(b, kstring(host)...)
	b = append(b, kint32(port)...)
	b = append(b, knullableString()...) // rack
	b = append(b, kint32(0)...)         // controller id
	b = append(b, kint32(1)...)         // topics
	b = append(b, kint16(0)...)         // topic error
	b = append(b, kstring(topic)...)
	b = append(b, kbool(false)...)   // is_internal
	b = append(b, kint32(1)...)      // partitions
	b = append(b, kint16(0)...)      // partition error
	b = append(b, kint32(0)...)      // partition id
	b = append(b, kint32(0)...)      // leader
	b = append(b, kint32array(0)...) // replicas
	b = append(b, kint32array(0)...) // isr
	return b
}

// startStalledProduceBroker accepts connections, answers Metadata (so a real
// client connects and picks partition 0) and NEVER answers Produce. It returns
// the advertised address, a channel closed when a Produce request arrives, and
// a channel closed on the first request of any kind.
func startStalledProduceBroker(t *testing.T) (addr string, produceSeen, requestSeen <-chan struct{}) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	host, portStr, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		t.Fatalf("split %s: %v", ln.Addr(), err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("port %s: %v", portStr, err)
	}
	produce := make(chan struct{})
	first := make(chan struct{})
	var produceOnce, firstOnce sync.Once
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer func() { _ = c.Close() }()
				for {
					header := make([]byte, 4)
					if _, err := io.ReadFull(c, header); err != nil {
						return
					}
					size := int(binary.BigEndian.Uint32(header))
					if size < 8 {
						return
					}
					payload := make([]byte, size)
					if _, err := io.ReadFull(c, payload); err != nil {
						return
					}
					firstOnce.Do(func() { close(first) })
					apiKey := int16(binary.BigEndian.Uint16(payload[0:2]))
					switch apiKey {
					case apiKeyProduce:
						produceOnce.Do(func() { close(produce) })
						// Never answer: the produce stays in flight until the
						// sender aborts the connection.
					case apiKeyMetadata:
						body := metadataV1Body(host, int32(port), "test-topic")
						out := make([]byte, 8+len(body))
						binary.BigEndian.PutUint32(out[0:4], uint32(4+len(body)))
						copy(out[4:8], payload[4:8])
						copy(out[8:], body)
						if _, err := c.Write(out); err != nil {
							return
						}
					}
				}
			}(conn)
		}
	}()
	return ln.Addr().String(), produce, first
}

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

// brokerSender builds a Sender whose resolved config points at addr as an
// explicit 0.10 broker (so no version probe runs and the negotiated Metadata
// request is v1, which the fake broker answers).
func brokerSender(t *testing.T, addr string) *Sender {
	t.Helper()
	raw := baseConfig()
	raw["kafka_address"] = addr
	raw["kafka_api_version"] = "0.10.0.0"
	raw["kafka_topic"] = "test-topic"
	return newSender(t, raw)
}

// --- Item 1: abortive delivery deadline against a real stalled producer ----

func TestSendDeliveryDeadlineAbortsRealStalledProduce(t *testing.T) {
	addr, produceSeen, _ := startStalledProduceBroker(t)
	s := brokerSender(t, addr)
	s.deliveryDeadline = 1 * time.Second

	start := time.Now()
	err := s.SendMessages(context.Background(), [][]byte{[]byte("payload")}, "c0")
	elapsed := time.Since(start)

	select {
	case <-produceSeen:
	default:
		t.Fatalf("no Produce request reached the broker; err=%v", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("SendMessages error = %v, want context.DeadlineExceeded", err)
	}
	// The abort terminates the in-flight produce and its retries; without it a
	// graceful close would wait on the stalled broker (30s read timeout times
	// the bounded retries), far beyond the deadline.
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

// --- Item 1/3: abortive in-flight cancellation against a real stalled producer

func TestSendCancellationAbortsRealStalledProduce(t *testing.T) {
	addr, produceSeen, _ := startStalledProduceBroker(t)
	s := brokerSender(t, addr)
	// No deadline: the ONLY thing that can end this send is the cancellation.
	s.deliveryDeadline = time.Hour

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.SendMessages(ctx, [][]byte{[]byte("payload")}, "c0") }()

	select {
	case <-produceSeen:
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
	addr, produceSeen, _ := startStalledProduceBroker(t)
	s := brokerSender(t, addr)
	s.deliveryDeadline = time.Hour

	if _, err := s.ensureProducer(context.Background()); err != nil {
		t.Fatalf("ensureProducer: %v", err)
	}
	sendDone := make(chan error, 1)
	go func() { sendDone <- s.SendMessages(context.Background(), [][]byte{[]byte("payload")}, "c0") }()

	select {
	case <-produceSeen:
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

// --- Item 2: construction honors the caller's context ----------------------

// TestConnectProducerCancellationTerminatesMetadata: a metadata handshake
// blocked against a silent broker is terminated by the construction context
// WITHOUT releasing the broker by hand — the tracking dialer closes the
// connection the moment ctx ends, so ensureProducer returns promptly.
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

// --- Item 5: the client delivery deadline is distinct from Producer.Timeout -

func TestDeliveryDeadlineIsClientSideAndDistinct(t *testing.T) {
	s := newSender(t, baseConfig())
	if s.deliveryDeadline != deliveryTimeout {
		t.Errorf("deliveryDeadline = %v, want %v", s.deliveryDeadline, deliveryTimeout)
	}
	if s.deliveryDeadline == requestTimeout {
		t.Fatal("the client-side delivery deadline must differ from the broker-side request timeout")
	}
}
