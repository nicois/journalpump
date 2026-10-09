// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package statsd

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// udpCapture binds a loopback UDP socket every subtest sends into, so
// parallel subtests never share datagrams.
type udpCapture struct {
	t    *testing.T
	conn *net.UDPConn
	mu   sync.Mutex
	seen []string
}

func newUDPCapture(t *testing.T) *udpCapture {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("ListenUDP: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	c := &udpCapture{t: t, conn: conn}
	go c.collect()
	return c
}

func (c *udpCapture) collect() {
	buf := make([]byte, 65535)
	for {
		n, _, err := c.conn.ReadFromUDP(buf)
		if err != nil {
			return // socket closed by the test cleanup
		}
		c.mu.Lock()
		c.seen = append(c.seen, string(buf[:n]))
		c.mu.Unlock()
	}
}

// addr returns the loopback address/port the client must send to.
func (c *udpCapture) addr() (string, *int64) {
	port := int64(c.conn.LocalAddr().(*net.UDPAddr).Port)
	return "127.0.0.1", &port
}

// datagram returns the first datagram received within a deadline.
func (c *udpCapture) datagram() string {
	c.t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		if len(c.seen) > 0 {
			defer c.mu.Unlock()
			return c.seen[0]
		}
		c.mu.Unlock()
		time.Sleep(time.Millisecond)
	}
	c.t.Fatal("no datagram arrived within 2s")
	return ""
}

// assertNoDatagram verifies nothing was sent.
func (c *udpCapture) assertNoDatagram() {
	c.t.Helper()
	time.Sleep(50 * time.Millisecond)
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.seen) != 0 {
		c.t.Fatalf("expected no datagrams, got %q", c.seen)
	}
}

// capturingLogger records log lines (send failures).
type capturingLogger struct {
	mu    sync.Mutex
	lines []string
}

func (l *capturingLogger) Printf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

// recordingSink records GaugeInt emissions in order.
type recordingSink struct {
	mu    sync.Mutex
	order []emittedGauge
}

type emittedGauge struct {
	metric string
	value  int64
	tags   map[string]string
}

func (r *recordingSink) GaugeInt(metric string, value int64, tags map[string]string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.order = append(r.order, emittedGauge{metric: metric, value: value, tags: tags})
}

func (r *recordingSink) emitted() []emittedGauge {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]emittedGauge(nil), r.order...)
}

func (r *recordingSink) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.order = nil
}

// goldenError is a named error type so the exception tag (Go's dynamic
// type name standing in for Python's ex.__class__.__name__) is stable
// in the golden datagrams.
type goldenError struct{}

func (*goldenError) Error() string { return "boom" }

// TestWireFormatGolden asserts the exact datagram bytes for every
// metric type, against the format documented in statsd.py:50
// ("user.logins,service=payroll,region=us-west:1|c").
func TestWireFormatGolden(t *testing.T) {
	tests := []struct {
		name     string
		prefix   string
		global   map[string]string
		tagOrder []string
		ops      func(c *Client)
		want     string
	}{
		{
			name: "increase default count",
			ops:  func(c *Client) { c.Increase("user.logins", 1, nil) },
			want: "user.logins:1|c",
		},
		{
			name: "increase custom count",
			ops:  func(c *Client) { c.Increase("journal.secret_filter_match", 42, nil) },
			want: "journal.secret_filter_match:42|c",
		},
		{
			name: "gauge float integral value keeps .0",
			ops:  func(c *Client) { c.Gauge("journal.perform_search_regex_duration", 60.0, nil) },
			want: "journal.perform_search_regex_duration:60.0|g",
		},
		{
			name: "gauge float small value uses python exponent form",
			ops:  func(c *Client) { c.Gauge("journal.perform_search_regex_duration", 1e-05, nil) },
			want: "journal.perform_search_regex_duration:1e-05|g",
		},
		{
			name: "gauge int renders as decimal",
			ops:  func(c *Client) { c.GaugeInt("journal.read_lines", 123, nil) },
			want: "journal.read_lines:123|g",
		},
		{
			name: "timing",
			ops:  func(c *Client) { c.Timing("journal.read_duration", 12.5, nil) },
			want: "journal.read_duration:12.5|ms",
		},
		{
			name:   "prefix is prepended",
			prefix: "jp.",
			ops:    func(c *Client) { c.Increase("user.logins", 1, nil) },
			want:   "jp.user.logins:1|c",
		},
		{
			name:   "global tags only, document order captured reversed on the wire",
			global: map[string]string{"a": "1", "b": "2", "c": "3"},
			// Config document order c,a,b (captured by the config layer);
			// Python inserts every tag at index 1 (statsd.py:55), so the
			// merged order [c, a, b] lands reversed: b, a, c.
			tagOrder: []string{"c", "a", "b"},
			ops:      func(c *Client) { c.Increase("m", 1, nil) },
			want:     "m,b=2,a=1,c=3:1|c",
		},
		{
			name:   "global tags without captured order fall back to sorted",
			global: map[string]string{"a": "1", "b": "2", "c": "3"},
			// A synthetic client (no config capture) sorts the keys;
			// merged [a, b, c] lands reversed: c, b, a.
			ops:  func(c *Client) { c.Increase("m", 1, nil) },
			want: "m,c=3,b=2,a=1:1|c",
		},
		{
			name:   "call tags appended after global, still reversed",
			global: map[string]string{"a": "1", "b": "2"},
			ops:    func(c *Client) { c.Increase("m", 1, map[string]string{"c": "3"}) },
			want:   "m,c=3,b=2,a=1:1|c",
		},
		{
			name:   "call tag overrides global value but keeps position",
			global: map[string]string{"a": "1"},
			ops:    func(c *Client) { c.Increase("m", 1, map[string]string{"a": "9", "b": "2"}) },
			want:   "m,b=2,a=9:1|c",
		},
		{
			name:   "call tags only",
			global: map[string]string{},
			ops:    func(c *Client) { c.Increase("m", 1, map[string]string{"service": "payroll"}) },
			want:   "m,service=payroll:1|c",
		},
		{
			name: "unexpected_exception adds exception and where tags",
			ops: func(c *Client) {
				c.UnexpectedException(&goldenError{}, "mainloop", map[string]string{"app": "journalpump"})
			},
			// Python: all_tags = {exception, where} then update(tags)
			// (statsd.py:38-42); merged order [exception, where, app]
			// reverses on the wire. Verified against the real
			// statsd.py: "exception,app=...,where=...,exception=...:1|c".
			// The exception tag carries the Go dynamic type name where
			// Python has ex.__class__.__name__.
			want: "exception,app=journalpump,where=mainloop,exception=*statsd.goldenError:1|c",
		},
		{
			name:     "unexpected_exception keeps the global document order",
			global:   map[string]string{"a": "1", "b": "2"},
			tagOrder: []string{"a", "b"},
			ops: func(c *Client) {
				c.UnexpectedException(&goldenError{}, "mainloop", nil)
			},
			// Python (probe-verified with global tags): send_tags order
			// [a, b, exception, where] reverses to where, exception, b, a.
			want: "exception,where=mainloop,exception=*statsd.goldenError,b=2,a=1:1|c",
		},
		{
			name: "unexpected_exception with a nil error still counts",
			ops: func(c *Client) {
				c.UnexpectedException(nil, "maintenance", nil)
			},
			// Python would crash on ex.__class__ of None; the Go guard
			// renders the type name "error" so the counter still fires.
			want: "exception,where=maintenance,exception=error:1|c",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			capture := newUDPCapture(t)
			host, port := capture.addr()
			client := New(stringPtr(host), port, tc.global, tc.tagOrder, tc.prefix, nil)
			t.Cleanup(func() { _ = client.Close() })
			tc.ops(client)
			if got := capture.datagram(); got != tc.want {
				t.Fatalf("datagram mismatch\n got: %q\nwant: %q", got, tc.want)
			}
		})
	}
}

// TestDisabled mirrors statsd.py:46-48: a missing (None) host or port
// disables every send. An explicitly EMPTY host is NOT covered here —
// Python resolves it to 0.0.0.0 and sends (TestEmptyHostSendsToLoopback).
func TestDisabled(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		host *string
		port *int64
	}{
		{name: "missing host", host: nil, port: int64Ptr(8125)},
		{name: "missing port", host: stringPtr("127.0.0.1"), port: nil},
		{name: "both missing", host: nil, port: nil},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			capture := newUDPCapture(t)
			client := New(tc.host, tc.port, nil, nil, "", nil)
			client.Gauge("m", 1.0, nil)
			client.GaugeInt("m", 1, nil)
			client.Increase("m", 1, nil)
			client.Timing("m", 1.0, nil)
			client.UnexpectedException(&goldenError{}, "where", nil)
			capture.assertNoDatagram()
		})
	}
}

// TestSendFailureLogged mirrors statsd.py:59-60: resolution and send
// errors log "Stats UDP send failed: %s" instead of crashing (an
// unresolvable host fails at the lazy resolution step). The literal
// "256.256.256.256" fails address validation outright, without a DNS
// round-trip.
func TestSendFailureLogged(t *testing.T) {
	t.Parallel()
	logger := &capturingLogger{}
	client := New(stringPtr("256.256.256.256"), int64Ptr(8125), nil, nil, "", logger)
	client.Increase("m", 1, nil)
	client.Increase("m", 1, nil)
	logger.mu.Lock()
	defer logger.mu.Unlock()
	if len(logger.lines) != 2 {
		t.Fatalf("expected one failure log line per send, got %q", logger.lines)
	}
	for _, line := range logger.lines {
		if !strings.Contains(line, "Stats UDP send failed: ") {
			t.Fatalf("unexpected log line: %q", line)
		}
	}
}

// TestIPv6HostFailsLikePythonAF_INET pins the udp4 family: an
// IPv6-literal host fails resolution exactly like Python's AF_INET
// getaddrinfo (an OSError caught at statsd.py:59-60) instead of sending
// successfully.
func TestIPv6HostFailsLikePythonAF_INET(t *testing.T) {
	t.Parallel()
	capture := newUDPCapture(t)
	_, port := capture.addr()
	logger := &capturingLogger{}
	client := New(stringPtr("::1"), port, nil, nil, "", logger)
	client.Increase("m", 1, nil)
	logger.mu.Lock()
	defer logger.mu.Unlock()
	if len(logger.lines) != 1 {
		t.Fatalf("expected one failure log line, got %q", logger.lines)
	}
	if !strings.Contains(logger.lines[0], "Stats UDP send failed: ") {
		t.Fatalf("unexpected log line: %q", logger.lines[0])
	}
	capture.assertNoDatagram()
}

// TestEmptyHostSendsToLoopback pins the host-presence semantics: an
// explicitly EMPTY host string is not None — Python's
// socket.sendto(("", port)) resolves via getaddrinfo("") to 0.0.0.0 and
// the kernel delivers to the local listener, and Go must do the same
// instead of silently dropping.
func TestEmptyHostSendsToLoopback(t *testing.T) {
	t.Parallel()
	capture := newUDPCapture(t)
	_, port := capture.addr()
	client := New(stringPtr(""), port, nil, nil, "", nil)
	t.Cleanup(func() { _ = client.Close() })
	client.Increase("m", 1, nil)
	if got := capture.datagram(); got != "m:1|c" {
		t.Fatalf("datagram mismatch: got %q", got)
	}
}

// TestPersistentGauge ports test/unit/reader/test_stats.py
// test_persistent_gauge: zeros are only emitted on the non-zero -> zero
// transition, non-zeros are only recorded (the pump batches them). The
// steps are sequential by nature — each builds on the previous state —
// so this test deliberately does not parallelize its subtests.
func TestPersistentGauge(t *testing.T) {
	tests := []struct {
		name     string
		value    int64
		wantEmit bool
	}{
		{name: "initial zero is not emitted", value: 0, wantEmit: false},
		{name: "duplicate zero is not emitted", value: 0, wantEmit: false},
		{name: "non-zero is only recorded, not emitted", value: 1, wantEmit: false},
		{name: "zero after non-zero is emitted once", value: 0, wantEmit: true},
		{name: "further zero is not emitted", value: 0, wantEmit: false},
	}
	sink := &recordingSink{}
	gauges := NewPersistentGauges(sink)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gauges.Set("test", tc.value, map[string]string{})
			got := sink.emitted()
			if tc.wantEmit {
				if len(got) != 1 {
					t.Fatalf("expected exactly one emission, got %+v", got)
				}
				if got[0].metric != "test" || got[0].value != 0 {
					t.Fatalf("unexpected emission: %+v", got[0])
				}
				sink.reset()
				return
			}
			if len(got) != 0 {
				t.Fatalf("expected no emission, got %+v", got)
			}
		})
	}
}

// TestPersistentGaugeRefreshNonZero ports refresh_gauges
// (journalpump.py:1104-1109): only non-zero values are re-sent.
func TestPersistentGaugeRefreshNonZero(t *testing.T) {
	sink := &recordingSink{}
	gauges := NewPersistentGauges(sink)
	gauges.Set("sender.failed_to_start", 3, map[string]string{"env": "prod"})
	gauges.Set("reader.nonzero", 5, map[string]string{})
	gauges.Set("reader.nonzero", 0, map[string]string{})
	sink.reset()
	gauges.RefreshNonZero()
	got := sink.emitted()
	if len(got) != 1 {
		t.Fatalf("expected only the non-zero gauge to be resent, got %+v", got)
	}
	if got[0].metric != "sender.failed_to_start" || got[0].value != 3 {
		t.Fatalf("unexpected resend: %+v", got[0])
	}
}

// TestPersistentGaugeRefreshNonZeroTo locks the pump's live-client
// refresh: the gauge VALUES/TAGS are the recorded ones, but emission goes
// through the GIVEN client — so a reload that replaces the statsd client
// moves the resend loop to the new one (journalpump.py:1102-1108 uses
// self.stats, the live pump client).
func TestPersistentGaugeRefreshNonZeroTo(t *testing.T) {
	oldSink := &recordingSink{}
	gauges := NewPersistentGauges(oldSink)
	gauges.Set("reader.nonzero", 5, map[string]string{"reader": "r"})
	oldSink.reset()

	newSink := &recordingSink{}
	gauges.RefreshNonZeroTo(newSink)

	if got := oldSink.emitted(); len(got) != 0 {
		t.Fatalf("old client received %+v, want nothing", got)
	}
	got := newSink.emitted()
	if len(got) != 1 || got[0].metric != "reader.nonzero" || got[0].value != 5 {
		t.Fatalf("new client emitted %+v, want reader.nonzero=5", got)
	}

	// A nil live client is a no-op (Python self.stats is None).
	gauges.RefreshNonZeroTo(nil)
}

// TestPersistentGaugeWithoutStats ports the self.stats is None case:
// values are still tracked so the transition logic and the pump refresh
// see them, but nothing is ever emitted.
func TestPersistentGaugeWithoutStats(t *testing.T) {
	gauges := NewPersistentGauges(nil)
	gauges.Set("m", 5, nil)
	gauges.Set("m", 0, nil)
	gauges.RefreshNonZero()
	// No sink: nothing to assert beyond "does not panic"; the transition
	// state is checked via Snapshot.
	state := gauges.Snapshot()
	if len(state) != 1 || state["m"].Value != 0 {
		t.Fatalf("unexpected tracked state: %+v", state)
	}
}

func int64Ptr(v int64) *int64 { return &v }

func stringPtr(v string) *string { return &v }
