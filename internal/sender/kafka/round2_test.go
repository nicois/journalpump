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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"golang.org/x/net/proxy"
)

// --- Item 1: the producer retry state must be BOUNDED ---------------------
//
// sarama's async producer allocates Producer.Retry.Max+1 partitionRetryState
// entries per partition on the first message to it. The previous value
// (2147483647, to mirror kafka-python's effectively-unlimited retries) made
// that ~2^31 entries — tens of GiB per partition — before delivery, which
// the mocked tests could not see because they bypass the real producer.

func TestProducerRetryMaxIsBounded(t *testing.T) {
	pc, err := resolveProducerConfig(baseConfig())
	if err != nil {
		t.Fatalf("resolveProducerConfig: %v", err)
	}
	cfg, err := pc.saramaConfig()
	if err != nil {
		t.Fatalf("saramaConfig: %v", err)
	}
	if cfg.Producer.Retry.Max != producerRetryMax {
		t.Errorf("Producer.Retry.Max = %d, want %d", cfg.Producer.Retry.Max, producerRetryMax)
	}
	// Guard the memory contract: Retry.Max+1 retry states per partition must
	// stay small enough that a real producer cannot exhaust memory.
	if cfg.Producer.Retry.Max > 1000 {
		t.Errorf("Producer.Retry.Max = %d; sarama allocates Retry.Max+1 retry states per partition (~%d entries), too many to be safe",
			cfg.Producer.Retry.Max, cfg.Producer.Retry.Max+1)
	}
}

// --- Item 2/3: the delivery deadline and cancellation, against a REAL
// sarama producer with a stalled broker, live in round3_test.go. A fake whose
// Close releases the send would not exercise the abortive path.

// --- Item 4: SASL handshake selection for the detected/configured version --

// TestProbeConfigAvoidsLocalSASLRejection reproduces the bootstrap bug: sarama
// authenticates BEFORE the explicit ApiVersions call, and at Version 0.10 a
// SASLHandshake v1 request is rejected LOCALLY as ErrUnsupportedVersion, so
// PLAIN and both SCRAM mechanisms fail whenever kafka_api_version is unset
// (review item 4). The probe config disables SASL so the pre-auth ApiVersions
// probe can complete.
func TestProbeConfigAvoidsLocalSASLRejection(t *testing.T) {
	addr := startFakeBroker(t, func(int16, int16) []byte { return nil })

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V0_10_0_0
	cfg.Net.SASL.Enable = true
	cfg.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	cfg.Net.SASL.User = "user"
	cfg.Net.SASL.Password = "password"
	cfg.Net.ReadTimeout = 200 * time.Millisecond

	b := sarama.NewBroker(addr)
	// Broker.Open returns nil and performs the connection (and any SASL
	// handshake) asynchronously; the outcome is surfaced by Connected().
	if err := b.Open(cfg); err != nil {
		t.Fatalf("Open: %v", err)
	}
	connected, cerr := b.Connected()
	_ = b.Close()
	if connected {
		t.Fatal("SASL handshake v1 at Version 0.10 connected; want a local ErrUnsupportedVersion rejection")
	}
	if !errors.Is(cerr, sarama.ErrUnsupportedVersion) {
		t.Fatalf("connect error = %v, want ErrUnsupportedVersion (the bug under test)", cerr)
	}

	probe := probeConfig(cfg)
	if probe.Version != sarama.V0_10_0_0 {
		t.Errorf("probe Version = %v, want V0_10_0_0", probe.Version)
	}
	if probe.ApiVersionsRequest {
		t.Error("probe must not fire sarama's own fire-and-forget ApiVersionsRequest")
	}
	if probe.Net.SASL.Enable || probe.Net.SASL.Handshake || probe.Net.SASL.SCRAMClientGeneratorFunc != nil {
		t.Errorf("probe did not disable SASL: enable=%v handshake=%v scram=%v",
			probe.Net.SASL.Enable, probe.Net.SASL.Handshake, probe.Net.SASL.SCRAMClientGeneratorFunc != nil)
	}
	// TLS must survive (it is transport-level, not authentication), and the
	// probe must be able to connect where the SASL-enabled config could not.
	pb := sarama.NewBroker(addr)
	if err := pb.Open(probe); err != nil {
		t.Fatalf("probe Open against a reachable broker: %v", err)
	}
	ok, perr := pb.Connected()
	_ = pb.Close()
	if !ok || perr != nil {
		t.Fatalf("probe broker connected=%v err=%v; want a clean connection", ok, perr)
	}
}

// TestSASLHandshakeVersionFollowsBrokerVersion: across a detected pre-1.0
// broker the handshake falls back to v0 (sarama would reject v1 locally),
// while a modern broker keeps v1 — for every mechanism, and the resulting
// config validates (review item 4).
func TestSASLHandshakeVersionFollowsBrokerVersion(t *testing.T) {
	cases := []struct {
		detected sarama.KafkaVersion
		want     int16
	}{
		{sarama.V0_10_0_0, sarama.SASLHandshakeV0},
		{sarama.V2_5_0_0, sarama.SASLHandshakeV1},
	}
	for _, mech := range []string{"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"} {
		for _, tc := range cases {
			t.Run(mech+"/"+tc.detected.String(), func(t *testing.T) {
				raw := baseConfig()
				raw["security_protocol"] = "SASL_PLAINTEXT"
				raw["sasl_mechanism"] = mech
				raw["sasl_plain_username"] = "user"
				raw["sasl_plain_password"] = "password"
				s := newSender(t, raw)
				pc, err := resolveProducerConfig(raw)
				if err != nil {
					t.Fatalf("resolveProducerConfig: %v", err)
				}
				s.detectVersion = func(context.Context, []string, *sarama.Config) (sarama.KafkaVersion, error) {
					return tc.detected, nil
				}
				cfg, _, err := s.buildSaramaConfig(context.Background(), pc)
				if err != nil {
					t.Fatalf("buildSaramaConfig: %v", err)
				}
				if !cfg.Net.SASL.Enable {
					t.Error("SASL not enabled for a SASL security protocol")
				}
				wantMech, _ := saslMechanism(mech)
				if cfg.Net.SASL.Mechanism != wantMech {
					t.Errorf("mechanism = %q, want %q", cfg.Net.SASL.Mechanism, wantMech)
				}
				if cfg.Version != tc.detected {
					t.Errorf("Version = %v, want %v", cfg.Version, tc.detected)
				}
				if cfg.Net.SASL.Version != tc.want {
					t.Errorf("SASL handshake version = %d, want %d", cfg.Net.SASL.Version, tc.want)
				}
				if err := cfg.Validate(); err != nil {
					t.Errorf("sarama rejected the config: %v", err)
				}
			})
		}
	}
}

// --- Item 5: the legacy pre-0.10 detection path ---------------------------

// TestProbeViaApiVersionsRejectsErrorResponse: an ApiVersions response that
// carries an error (pre-0.10 brokers answer with UnsupportedVersion) must NOT
// be inferred from (review item 5).
func TestProbeViaApiVersionsRejectsErrorResponse(t *testing.T) {
	addr := startFakeBroker(t, func(apiKey, _ int16) []byte {
		if apiKey == apiKeyAPIVersions {
			return []byte{0x00, 0x23, 0, 0, 0, 0} // error_code=35 (UnsupportedVersion), no keys
		}
		return nil
	})
	_, err := probeViaAPIVersions(addr, probeConfig(sarama.NewConfig()))
	if err == nil {
		t.Fatal("an error-carrying ApiVersions response was inferred from; want an error")
	}
	var ke sarama.KError
	if !errors.As(err, &ke) || ke != sarama.ErrUnsupportedVersion {
		t.Fatalf("probeViaAPIVersions error = %v, want KError UnsupportedVersion", err)
	}
}

// TestProbeViaApiVersionsInfersVersion is the happy path of the same probe.
func TestProbeViaApiVersionsInfersVersion(t *testing.T) {
	addr := startFakeBroker(t, func(apiKey, _ int16) []byte {
		if apiKey == apiKeyAPIVersions {
			return apiVersionsBody(map[int16]int16{1: 8}) // Fetch v8 => 2.0
		}
		return nil
	})
	ver, err := probeViaAPIVersions(addr, probeConfig(sarama.NewConfig()))
	if err != nil {
		t.Fatalf("probeViaAPIVersions: %v", err)
	}
	if ver != sarama.V2_0_0_0 {
		t.Errorf("detected %v, want V2_0_0_0", ver)
	}
}

// TestDetectVersionFallsBackToLegacyProbe: when ApiVersions answers with an
// error (a 0.8/0.9 broker), detection must fall through to the legacy probes
// (kafka-python's VERSION_CHECKS) and still connect — here via ListGroups v0
// identifying 0.9 (review item 5).
func TestDetectVersionFallsBackToLegacyProbe(t *testing.T) {
	addr := startFakeBroker(t, func(apiKey, _ int16) []byte {
		switch apiKey {
		case apiKeyAPIVersions:
			return []byte{0x00, 0x23, 0, 0, 0, 0} // UnsupportedVersion
		case apiKeyListGroups: // v0: success, empty group list
			return []byte{0x00, 0x00, 0, 0, 0, 0}
		default:
			return nil
		}
	})
	cfg := sarama.NewConfig()
	cfg.Net.ReadTimeout = time.Second
	ver, err := detectBrokerVersion(context.Background(), []string{addr}, cfg)
	if err != nil {
		t.Fatalf("detectBrokerVersion: %v", err)
	}
	if ver != sarama.V0_9_0_0 {
		t.Errorf("detected %v, want V0_9_0_0 via the legacy ListGroups probe", ver)
	}
}

// --- Item 6: per-broker reconnect backoff ---------------------------------

// flakyDialer fails or succeeds per a scripted sequence; a nil error means
// success.
type flakyDialer struct {
	mu      sync.Mutex
	results []error
	calls   int
}

func (d *flakyDialer) Dial(string, string) (net.Conn, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls++
	if len(d.results) == 0 {
		return nil, errors.New("dial refused")
	}
	r := d.results[0]
	d.results = d.results[1:]
	if r == nil {
		return nopConn{}, nil
	}
	return nil, r
}

type nopConn struct{}

func (nopConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (nopConn) Write(b []byte) (int, error)      { return len(b), nil }
func (nopConn) Close() error                     { return nil }
func (nopConn) LocalAddr() net.Addr              { return dummyAddr{} }
func (nopConn) RemoteAddr() net.Addr             { return dummyAddr{} }
func (nopConn) SetDeadline(time.Time) error      { return nil }
func (nopConn) SetReadDeadline(time.Time) error  { return nil }
func (nopConn) SetWriteDeadline(time.Time) error { return nil }

type dummyAddr struct{}

func (dummyAddr) Network() string { return "tcp" }
func (dummyAddr) String() string  { return "dummy" }

// fakeClock drives the backoff dialer's now/sleep seams deterministically so
// the elapsed-time behavior (review round-3 item 4) can be asserted exactly.
type fakeClock struct {
	now   time.Time
	slept []time.Duration
}

func (c *fakeClock) sleep(_ context.Context, d time.Duration) error {
	c.slept = append(c.slept, d)
	c.now = c.now.Add(d)
	return nil
}

func newTestBackoffDialer(inner proxy.Dialer, initial, maxDelay time.Duration) (*backoffDialer, *fakeClock) {
	clock := &fakeClock{now: time.Unix(0, 0)}
	d := newBackoffDialer(inner, brokerBackoffConfig{initial: initial, max: maxDelay})
	d.now = func() time.Time { return clock.now }
	d.sleep = clock.sleep
	d.randFloat = func() float64 { return 0.5 } // factor exactly 1.0
	return d, clock
}

// TestBackoffDialerGrowsCapsAndJitters: each failed attempt waits the growing,
// capped, jittered interval (raw randFloat=0.5 makes the jitter factor 1.0).
func TestBackoffDialerGrowsCapsAndJitters(t *testing.T) {
	fail := errors.New("dial refused")
	inner := &flakyDialer{results: []error{fail, fail, fail, fail, fail}}
	d, clock := newTestBackoffDialer(inner, time.Second, 4*time.Second)

	for i := 0; i < 5; i++ {
		if _, err := d.Dial("tcp", "broker:9092"); err == nil {
			t.Fatalf("dial %d: want error", i)
		}
	}
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 4 * time.Second}
	if len(clock.slept) != len(want) {
		t.Fatalf("slept = %v, want %v", clock.slept, want)
	}
	for i := range want {
		if clock.slept[i] != want[i] {
			t.Errorf("sleep %d = %v, want %v", i, clock.slept[i], want[i])
		}
	}
}

// TestBackoffDialerWaitsOnlyRemaining: the elapsed time since the failed
// attempt counts against the interval, so only the remaining part is waited
// (kafka-python _can_connect; review round-3 item 4).
func TestBackoffDialerWaitsOnlyRemaining(t *testing.T) {
	fail := errors.New("dial refused")
	inner := &flakyDialer{results: []error{fail, fail}}
	d, clock := newTestBackoffDialer(inner, time.Second, 10*time.Second)

	_, _ = d.Dial("tcp", "broker:9092") // fail: next = now+1s
	clock.now = clock.now.Add(300 * time.Millisecond)
	_, _ = d.Dial("tcp", "broker:9092") // only 700ms remain

	if len(clock.slept) != 1 || clock.slept[0] != 700*time.Millisecond {
		t.Fatalf("slept = %v, want [700ms]", clock.slept)
	}
}

// TestBackoffDialerTCPSuccessDoesNotReset: a successful dial keeps the history
// (TLS/SASL may still fail); only markConnected resets it (review round-3 item
// 4).
func TestBackoffDialerTCPSuccessDoesNotReset(t *testing.T) {
	fail := errors.New("dial refused")
	inner := &flakyDialer{results: []error{fail, nil, fail}}
	d, clock := newTestBackoffDialer(inner, time.Second, 10*time.Second)

	_, _ = d.Dial("tcp", "broker:9092") // fail -> attempts 1, next = now+1s
	_, _ = d.Dial("tcp", "broker:9092") // sleep 1s, TCP succeeds, history kept
	_, _ = d.Dial("tcp", "broker:9092") // fail -> attempts 2 (NOT reset to 1)

	// A reset would have scheduled 1s; the retained history schedules 2s.
	clock.now = clock.now.Add(time.Second)
	clock.slept = nil
	_, _ = d.Dial("tcp", "broker:9092") // dial refused (scripted out), remaining after 1s elapsed
	if len(clock.slept) != 1 || clock.slept[0] != time.Second {
		t.Fatalf("slept = %v, want [1s] (2s interval minus 1s elapsed); a TCP success must not reset", clock.slept)
	}
}

// TestBackoffDialerTracksAddressesIndependently: one address's backoff does
// not delay another (review round-3 item 4).
func TestBackoffDialerTracksAddressesIndependently(t *testing.T) {
	fail := errors.New("dial refused")
	inner := &flakyDialer{results: []error{fail, fail}}
	d, clock := newTestBackoffDialer(inner, time.Second, 10*time.Second)

	_, _ = d.Dial("tcp", "a:9092") // fail: a backs off
	_, _ = d.Dial("tcp", "b:9092") // b's first dial: no wait
	if len(clock.slept) != 0 {
		t.Fatalf("slept = %v; a different address must not inherit a's backoff", clock.slept)
	}
}

// TestBackoffDialerMarkConnectedResetsAndMarkFailedGrows: a post-connect
// failure (handshake/admin) grows the delay, and a full success resets it
// (review round-3 item 4).
func TestBackoffDialerMarkConnectedResetsAndMarkFailedGrows(t *testing.T) {
	d, clock := newTestBackoffDialer(&flakyDialer{}, time.Second, 10*time.Second)

	d.markFailed("broker:9092") // post-connect failure recorded
	d.markFailed("broker:9092") // second failure doubles it
	if got := d.remaining("broker:9092"); got != 2*time.Second {
		t.Fatalf("remaining after two failures = %v, want 2s", got)
	}
	d.markConnected("broker:9092")
	if got := d.remaining("broker:9092"); got != 0 {
		t.Fatalf("remaining after markConnected = %v, want 0", got)
	}
	_ = clock
}

func TestJitterBounds(t *testing.T) {
	if got := jitter(10*time.Second, 0); got != 8*time.Second {
		t.Errorf("jitter(r=0) = %v, want 8s", got)
	}
	if got := jitter(10*time.Second, 0.5); got != 10*time.Second {
		t.Errorf("jitter(r=0.5) = %v, want 10s", got)
	}
	if got := jitter(10*time.Second, 1); got != 12*time.Second {
		t.Errorf("jitter(r=1) = %v, want 12s", got)
	}
}

// --- Item 7: the aggregate send error exposes its causes -------------------

func TestWrapProducerErrorsExposesCauses(t *testing.T) {
	auth := errors.New("SASL authentication failed")
	pes := sarama.ProducerErrors{
		&sarama.ProducerError{Err: sarama.ErrNotLeaderForPartition},
		&sarama.ProducerError{Err: auth},
	}
	err := wrapProducerErrors(pes)
	if !errors.Is(err, auth) {
		t.Error("individual cause not exposed to errors.Is/As")
	}
	if !errors.Is(err, sarama.ErrNotLeaderForPartition) {
		t.Error("KError cause not exposed to errors.Is/As")
	}
	if !strings.Contains(err.Error(), "SASL authentication failed") {
		t.Errorf("Error() = %q; the cause text must be rendered, not just 'Failed to deliver N messages'", err.Error())
	}
	var se *sendErrors
	if !errors.As(err, &se) {
		t.Fatal("wrapped error is not a *sendErrors")
	}
	if se.retriable() {
		t.Error("a batch with a permanent cause must not be all-retriable")
	}
	if Retriable(err) {
		t.Error("Retriable(mixed causes) = true, want false")
	}
}

func TestSendErrorsAllRetriable(t *testing.T) {
	err := wrapProducerErrors(producerErrors(sarama.ErrNotLeaderForPartition))
	if !Retriable(err) {
		t.Error("Retriable(all-retriable) = false, want true")
	}
	var se *sendErrors
	if !errors.As(err, &se) {
		t.Fatal("wrapped error is not a *sendErrors")
	}
	if !se.retriable() {
		t.Error("sendErrors.retriable() = false, want true")
	}
}

func TestWrapProducerErrorsPassesOtherErrorsThrough(t *testing.T) {
	sentinel := errors.New("plain failure")
	if got := wrapProducerErrors(sentinel); !errors.Is(got, sentinel) {
		t.Errorf("non-batch error = %v, want it passed through", got)
	}
	if wrapProducerErrors(nil) != nil {
		t.Error("nil error must stay nil")
	}
}

// --- fake Kafka broker -----------------------------------------------------

const (
	apiKeyAPIVersions = 18
	apiKeyListGroups  = 16
)

// startFakeBroker runs a TCP server that answers each length-prefixed Kafka
// request with the body returned by respond (framed with the request's
// correlation id); a nil body leaves the request unanswered. It returns the
// listener address.
func startFakeBroker(t *testing.T, respond func(apiKey, apiVersion int16) []byte) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go serveFakeBroker(conn, respond)
		}
	}()
	return ln.Addr().String()
}

func serveFakeBroker(conn net.Conn, respond func(apiKey, apiVersion int16) []byte) {
	defer func() { _ = conn.Close() }()
	for {
		header := make([]byte, 4)
		if _, err := io.ReadFull(conn, header); err != nil {
			return
		}
		size := int(binary.BigEndian.Uint32(header))
		if size < 8 {
			return
		}
		payload := make([]byte, size)
		if _, err := io.ReadFull(conn, payload); err != nil {
			return
		}
		apiKey := int16(binary.BigEndian.Uint16(payload[0:2]))
		apiVersion := int16(binary.BigEndian.Uint16(payload[2:4]))
		correlation := payload[4:8]
		body := respond(apiKey, apiVersion)
		if body == nil {
			continue
		}
		out := make([]byte, 8+len(body))
		binary.BigEndian.PutUint32(out[0:4], uint32(4+len(body)))
		copy(out[4:8], correlation)
		copy(out[8:], body)
		if _, err := conn.Write(out); err != nil {
			return
		}
	}
}

// apiVersionsBody encodes an ApiVersions v0 response with no error and the
// given (api key, max version) entries.
func apiVersionsBody(pairs map[int16]int16) []byte {
	buf := []byte{0, 0} // error_code = 0
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(pairs)))
	for key, max := range pairs {
		buf = binary.BigEndian.AppendUint16(buf, uint16(key))
		buf = binary.BigEndian.AppendUint16(buf, 0) // min version
		buf = binary.BigEndian.AppendUint16(buf, uint16(max))
	}
	return buf
}
