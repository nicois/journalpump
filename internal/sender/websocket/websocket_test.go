// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package websocket

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/golang/snappy"
	"github.com/gorilla/websocket"

	"github.com/Aiven-Open/journalpump/internal/sender"
	"github.com/Aiven-Open/journalpump/internal/tagged"
)

func TestParseConfigDefaults(t *testing.T) {
	cfg, err := parseConfig(map[string]any{"websocket_uri": "ws://host/pump"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.uri != "ws://host/pump" {
		t.Errorf("uri = %q", cfg.uri)
	}
	if cfg.compression != compressionSnappy {
		t.Errorf("compression = %q, want snappy default", cfg.compression)
	}
	if cfg.websocketCompression != wsCompressionNone {
		t.Errorf("websocket_compression = %q, want none default", cfg.websocketCompression)
	}
	if cfg.maxBatchSize != defaultMaxBatchSize {
		t.Errorf("max_batch_size = %d, want %d", cfg.maxBatchSize, defaultMaxBatchSize)
	}
	if cfg.maxSendInterval != defaultMaxSendInterval {
		t.Errorf("max_send_interval = %v, want %v", cfg.maxSendInterval, defaultMaxSendInterval)
	}
}

func TestParseConfigRequiresURI(t *testing.T) {
	if _, err := parseConfig(map[string]any{}); err == nil {
		t.Fatal("missing websocket_uri accepted")
	}
}

func TestParseConfigSSLRequiresTriple(t *testing.T) {
	_, err := parseConfig(map[string]any{"websocket_uri": "wss://h/", "ssl": true})
	if err == nil || !strings.Contains(err.Error(), "required when ssl") {
		t.Fatalf("ssl without ca/keyfile/certfile: err = %v", err)
	}
	_, err = parseConfig(map[string]any{
		"websocket_uri": "wss://h/", "ssl": true,
		"ca": "ca.pem", "keyfile": "k.pem", "certfile": "c.pem",
	})
	if err != nil {
		t.Fatalf("complete ssl triple rejected: %v", err)
	}
}

func TestParseConfigInvalidEnums(t *testing.T) {
	for _, tc := range []map[string]any{
		{"websocket_uri": "ws://h/", "websocket_compression": "brotli"},
		{"websocket_uri": "ws://h/", "compression": "gzip"},
		{"websocket_uri": "ws://h/", "max_batch_size": "big"},
		{"websocket_uri": "ws://h/", "max_send_interval": "soon"},
		{"websocket_uri": 5},
	} {
		if _, err := parseConfig(tc); err == nil {
			t.Errorf("config %v accepted, want error", tc)
		}
	}
}

func TestParseConfigMaxBatchSizeZeroDisablesBatching(t *testing.T) {
	cfg, err := parseConfig(map[string]any{"websocket_uri": "ws://h/", "max_batch_size": float64(0)})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.maxBatchSize != 0 {
		t.Errorf("max_batch_size = %d, want 0", cfg.maxBatchSize)
	}
	s := &Sender{cfg: cfg}
	if s.batching() {
		t.Error("batching should be disabled for max_batch_size 0")
	}
}

func TestFramesNulJoiAndSnappy(t *testing.T) {
	s := &Sender{cfg: config{
		websocketCompression: wsCompressionNone,
		compression:          compressionSnappy,
		maxBatchSize:         1024,
	}}
	batch := [][]byte{[]byte("aaa"), []byte("bbb")}
	got := s.frames(batch)
	if len(got) != 1 {
		t.Fatalf("frames = %d, want 1 (batching)", len(got))
	}
	want := snappy.Encode(nil, []byte("aaa\x00bbb"))
	if !bytes.Equal(got[0], want) {
		t.Errorf("frame = %x, want snappy(NUL-join) %x", got[0], want)
	}
	// Round-trips to the NUL-joined messages.
	dec, err := snappy.Decode(nil, got[0])
	if err != nil {
		t.Fatal(err)
	}
	if string(dec) != "aaa\x00bbb" {
		t.Errorf("decoded = %q", dec)
	}
}

func TestFramesNoBatchingSeparateFrames(t *testing.T) {
	s := &Sender{cfg: config{
		websocketCompression: wsCompressionNone,
		compression:          compressionSnappy,
		maxBatchSize:         0,
	}}
	got := s.frames([][]byte{[]byte("aaa"), []byte("bbb")})
	if len(got) != 2 {
		t.Fatalf("frames = %d, want 2 (no batching)", len(got))
	}
	if !bytes.Equal(got[0], snappy.Encode(nil, []byte("aaa"))) || !bytes.Equal(got[1], snappy.Encode(nil, []byte("bbb"))) {
		t.Error("per-message snappy frames mismatch")
	}
}

func TestFramesCompressionNone(t *testing.T) {
	s := &Sender{cfg: config{
		websocketCompression: wsCompressionNone,
		compression:          compressionNone,
		maxBatchSize:         1024,
	}}
	got := s.frames([][]byte{[]byte("aaa"), []byte("bbb")})
	if len(got) != 1 || string(got[0]) != "aaa\x00bbb" {
		t.Fatalf("frames = %q, want raw NUL-join", got)
	}
}

func TestHandshakeHeaders(t *testing.T) {
	s := &Sender{cfg: config{
		compression:          compressionSnappy,
		websocketCompression: wsCompressionNone,
		maxBatchSize:         1048576,
	}}
	h := s.handshakeHeaders()
	if ua := h.Get("User-Agent"); !strings.HasPrefix(ua, "journalpump/") {
		t.Errorf("User-Agent = %q", ua)
	}
	if got := h.Get("Journalpump-Batch-Size"); got != "1048576" {
		t.Errorf("Journalpump-Batch-Size = %q, want 1048576", got)
	}
	if got := h.Get("Journalpump-Compression"); got != "snappy" {
		t.Errorf("Journalpump-Compression = %q, want snappy", got)
	}
}

func TestHandshakeHeadersNoBatchingNoCompression(t *testing.T) {
	s := &Sender{cfg: config{
		compression:          compressionNone,
		websocketCompression: wsCompressionNone,
		maxBatchSize:         0,
	}}
	h := s.handshakeHeaders()
	if h.Get("Journalpump-Batch-Size") != "" {
		t.Error("batch-size header present with batching disabled")
	}
	if h.Get("Journalpump-Compression") != "" {
		t.Error("compression header present with compression none")
	}
}

// TestExponentialBackoffSequence pins journalpump.util.ExponentialBackoff
// (base 20, factor 1.8, cap 90, full jitter): the pre-jitter value doubles
// until it exceeds the cap, then stays at the cap; jitter is injected as
// identity-on-max so the sequence is deterministic.
func TestExponentialBackoffSequence(t *testing.T) {
	b := newExponentialBackoff(20, 1.8, 90, func(m float64) float64 { return m })
	want := []float64{20, 36, 64.8, 90, 90, 90}
	for i, w := range want {
		got := b.next().Seconds()
		if diff := got - w; diff > 1e-6 || diff < -1e-6 {
			t.Fatalf("next() #%d = %v, want %v", i, got, w)
		}
	}
	// reset returns to the base.
	b.reset()
	if got := b.next().Seconds(); got != 20 {
		t.Fatalf("after reset next() = %v, want 20", got)
	}
}

// TestRunnerReconnectBackoffTiming pins the reconnect cadence under
// testing/synctest: a failing dial retries at base 20 s, factor 1.8, cap
// 90 s (jitter injected as identity-on-max for determinism). Virtual time
// makes the waits exact.
func TestRunnerReconnectBackoffTiming(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := &Sender{
			cfg:    config{uri: "ws://x/", maxBatchSize: defaultMaxBatchSize},
			jitter: func(m float64) float64 { return m },
		}
		r := newRunner(s)
		var at []time.Time
		r.dialFn = func() (*websocket.Conn, error) {
			at = append(at, time.Now())
			return nil, errors.New("dial failed")
		}
		go r.run()
		// Let four dial attempts happen (0, 20, 56, 120.8 s) then stop.
		done := make(chan struct{})
		go func() {
			time.Sleep(121 * time.Second)
			r.stop()
			close(done)
		}()
		<-done

		var deltas []float64
		for i := 1; i < len(at); i++ {
			deltas = append(deltas, at[i].Sub(at[i-1]).Seconds())
		}
		want := []float64{20, 36, 64.8}
		if len(deltas) < len(want) {
			t.Fatalf("dial attempts = %d (%v), want at least %d", len(at), deltas, len(want)+1)
		}
		for i, w := range want {
			if d := deltas[i] - w; d > 1e-6 || d < -1e-6 {
				t.Fatalf("reconnect #%d = %v, want %v", i, deltas[i], w)
			}
		}
	})
}

// websocketTestServer is an in-process gorilla websocket server capturing
// the handshake headers and the binary frames.
type websocketTestServer struct {
	srv      *httptest.Server
	upgrader websocket.Upgrader

	mu      sync.Mutex
	headers http.Header
	frames  [][]byte
	gotOne  chan struct{}
	once    sync.Once
}

func newWebsocketTestServer(t *testing.T) *websocketTestServer {
	t.Helper()
	s := &websocketTestServer{gotOne: make(chan struct{})}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.headers = r.Header.Clone()
		s.mu.Unlock()
		conn, err := s.upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			s.mu.Lock()
			s.frames = append(s.frames, append([]byte(nil), data...))
			s.mu.Unlock()
			s.once.Do(func() { close(s.gotOne) })
		}
	}))
	t.Cleanup(s.srv.Close)
	return s
}

func (s *websocketTestServer) wsURL() string {
	return "ws" + strings.TrimPrefix(s.srv.URL, "http") + "/pump-pump"
}

func (s *websocketTestServer) firstHeaders() http.Header {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.headers.Clone()
}

func (s *websocketTestServer) capturedFrames() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([][]byte, len(s.frames))
	copy(out, s.frames)
	return out
}

func newTestSender(t *testing.T, cfg map[string]any) *Sender {
	t.Helper()
	args := sender.ConstructorArgs{
		Name:   "out",
		Config: cfg,
		Tags:   tagged.New(map[string]string{"host": "h"}),
	}
	inst, err := New(args)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	s, ok := inst.Transport.(*Sender)
	if !ok {
		t.Fatalf("transport = %T", inst.Transport)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// TestSendMessagesBatchedSnappy ports test_producer_batch
// (test/test_websocket.py): one NUL-joined, snappy-compressed frame.
func TestSendMessagesBatchedSnappy(t *testing.T) {
	srv := newWebsocketTestServer(t)
	s := newTestSender(t, map[string]any{
		"websocket_uri":  srv.wsURL(),
		"compression":    "snappy",
		"max_batch_size": 1024,
	})
	messages := [][]byte{
		[]byte(`{"timestamp": "2019-10-07 14:00:00"}`),
		[]byte(`{"timestamp": "2019-10-07 15:00:00"}`),
	}
	if err := s.SendMessages(context.Background(), messages, "c"); err != nil {
		t.Fatalf("SendMessages: %v", err)
	}
	select {
	case <-srv.gotOne:
	case <-time.After(5 * time.Second):
		t.Fatal("no frame received")
	}
	frames := srv.capturedFrames()
	if len(frames) != 1 {
		t.Fatalf("frames = %d, want 1", len(frames))
	}
	want := snappy.Encode(nil, bytes.Join(messages, []byte{0}))
	if !bytes.Equal(frames[0], want) {
		t.Errorf("frame = %x, want %x", frames[0], want)
	}
	// The connect headers carry the configured batch size and compression.
	h := srv.firstHeaders()
	if h.Get("Journalpump-Batch-Size") != "1024" {
		t.Errorf("Journalpump-Batch-Size = %q, want 1024", h.Get("Journalpump-Batch-Size"))
	}
	if h.Get("Journalpump-Compression") != "snappy" {
		t.Errorf("Journalpump-Compression = %q", h.Get("Journalpump-Compression"))
	}
	if ua := h.Get("User-Agent"); !strings.HasPrefix(ua, "journalpump/") {
		t.Errorf("User-Agent = %q", ua)
	}
}

// TestSendMessagesNobatch ports test_producer_nobatch
// (test/test_websocket.py): batching disabled, compression none, each
// message its own frame.
func TestSendMessagesNobatch(t *testing.T) {
	srv := newWebsocketTestServer(t)
	s := newTestSender(t, map[string]any{
		"websocket_uri":  srv.wsURL(),
		"compression":    "none",
		"max_batch_size": 0,
	})
	messages := [][]byte{
		[]byte(`{"timestamp": "2019-10-07 14:00:00"}`),
		[]byte(`{"timestamp": "2019-10-07 15:00:00"}`),
	}
	if err := s.SendMessages(context.Background(), messages, "c"); err != nil {
		t.Fatalf("SendMessages: %v", err)
	}
	select {
	case <-srv.gotOne:
	case <-time.After(5 * time.Second):
		t.Fatal("no frame received")
	}
	// Wait for both frames.
	deadline := time.Now().Add(5 * time.Second)
	for len(srv.capturedFrames()) < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	frames := srv.capturedFrames()
	if len(frames) != 2 {
		t.Fatalf("frames = %d, want 2", len(frames))
	}
	for i, m := range messages {
		if !bytes.Equal(frames[i], m) {
			t.Errorf("frame %d = %q, want %q", i, frames[i], m)
		}
	}
	// No batch-size header when batching is disabled.
	if got := srv.firstHeaders().Get("Journalpump-Batch-Size"); got != "" {
		t.Errorf("Journalpump-Batch-Size = %q, want empty", got)
	}
}

// TestSendMessagesNotConnected pins the send-wait: with no server the
// send fails with errNotConnected after the (shortened) wait.
func TestSendMessagesNotConnected(t *testing.T) {
	old := sendWaitConnected
	sendWaitConnected = 100 * time.Millisecond
	t.Cleanup(func() { sendWaitConnected = old })

	s := newTestSender(t, map[string]any{
		"websocket_uri": "ws://127.0.0.1:1/none",
		"compression":   "none",
	})
	err := s.SendMessages(context.Background(), [][]byte{[]byte("x")}, "")
	if err == nil {
		t.Fatal("send to unreachable endpoint succeeded")
	}
}

// TestNewRegistersInstance asserts the framework wiring values: the
// websocket sender overrides the shared batch overhead to 1, the default
// type name, and the default max_send_interval.
func TestNewRegistersInstance(t *testing.T) {
	inst, err := New(sender.ConstructorArgs{
		Name:   "out",
		Config: map[string]any{"websocket_uri": "ws://h/pump"},
		Tags:   tagged.New(nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	if inst.Type != "WebsocketSender" {
		t.Errorf("Type = %q", inst.Type)
	}
	if inst.MessageOverhead != 1 {
		t.Errorf("MessageOverhead = %d, want 1", inst.MessageOverhead)
	}
	if inst.MaxBatchSize != defaultMaxBatchSize {
		t.Errorf("MaxBatchSize = %d, want %d", inst.MaxBatchSize, defaultMaxBatchSize)
	}
	if inst.MaxSendInterval != time.Second {
		t.Errorf("MaxSendInterval = %v, want 1s", inst.MaxSendInterval)
	}
}

// TestConfigNumbersAsJSONNumber ensures json.Number values (UseNumber
// decoding) are accepted.
func TestConfigNumbersAsJSONNumber(t *testing.T) {
	cfg, err := parseConfig(map[string]any{
		"websocket_uri":     "ws://h/",
		"max_batch_size":    json.Number("2048"),
		"max_send_interval": json.Number("2.5"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.maxBatchSize != 2048 || cfg.maxSendInterval != 2.5 {
		t.Fatalf("cfg = %+v", cfg)
	}
}
