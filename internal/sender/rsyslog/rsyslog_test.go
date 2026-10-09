// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package rsyslog

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Aiven-Open/journalpump/internal/sender"
	"github.com/Aiven-Open/journalpump/internal/syslogframing"
)

// recordClient is a fake logClient recording every Log call (the Python
// unit tests' Mock rsyslog_client).
type recordClient struct {
	calls    []syslogframing.LogInput
	failWith error
	closed   int
}

func (c *recordClient) Log(_ context.Context, in syslogframing.LogInput) error {
	if c.failWith != nil {
		return c.failWith
	}
	c.calls = append(c.calls, in)
	return nil
}

func (c *recordClient) Close() { c.closed++ }

// recordLogger captures warning/info lines.
type recordLogger struct {
	warnings []string
	infos    []string
}

func (l *recordLogger) Printf(format string, args ...any) {
	l.infos = append(l.infos, fmt.Sprintf(format, args...))
}

func (l *recordLogger) Warningf(format string, args ...any) {
	l.warnings = append(l.warnings, fmt.Sprintf(format, args...))
}

// newTestSender builds a Sender via the real constructor, then injects a
// recording client and a fixed clock (the Python tests' mock injection).
func newTestSender(t *testing.T, cfg map[string]any) (*Sender, *recordClient, *recordLogger) {
	t.Helper()
	logger := &recordLogger{}
	inst, err := New(sender.ConstructorArgs{Name: "test-rsyslog", Config: cfg, Log: logger})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	s := inst.Transport.(*Sender)
	client := &recordClient{}
	s.newClient = func(context.Context) (logClient, error) { return client, nil }
	return s, client, logger
}

func baseConfig() map[string]any {
	return map[string]any{"rsyslog_server": "127.0.0.1", "rsyslog_port": 514}
}

// TestSendSucceedsWithTimestamp ports
// test_send_succeeds_with_timestamp: a message with a timestamp is
// forwarded without warnings, and the timestamp is raw[:26]+"Z".
func TestSendSucceedsWithTimestamp(t *testing.T) {
	s, client, logger := newTestSender(t, baseConfig())
	msg := []byte(`{"MESSAGE": "hello", "timestamp": "2025-06-26T14:52:33.581000", "PRIORITY": "6"}`)

	if err := s.SendMessages(context.Background(), [][]byte{msg}, "c1"); err != nil {
		t.Fatalf("SendMessages: %v", err)
	}
	if len(client.calls) != 1 {
		t.Fatalf("log calls = %d, want 1", len(client.calls))
	}
	if got := client.calls[0].Timestamp; got != "2025-06-26T14:52:33.581000Z" {
		t.Fatalf("timestamp = %q, want 2025-06-26T14:52:33.581000Z", got)
	}
	if len(logger.warnings) != 0 {
		t.Fatalf("warnings = %v, want none", logger.warnings)
	}
}

// TestSendSucceedsWithoutTimestamp ports
// test_send_succeeds_without_timestamp: a missing timestamp does not
// crash and the message is still sent.
func TestSendSucceedsWithoutTimestamp(t *testing.T) {
	s, client, _ := newTestSender(t, baseConfig())
	msg := []byte(`{"MESSAGE": "hello", "PRIORITY": "6"}`)
	if err := s.SendMessages(context.Background(), [][]byte{msg}, "c1"); err != nil {
		t.Fatalf("SendMessages: %v", err)
	}
	if len(client.calls) != 1 {
		t.Fatalf("log calls = %d, want 1", len(client.calls))
	}
}

// TestFallbackTimestampIsValidISO ports
// test_fallback_timestamp_is_valid_iso: the fallback is strftime("%Y-%m-%dT%H:%M:%S.%f")+"Z".
func TestFallbackTimestampIsValidISO(t *testing.T) {
	s, client, _ := newTestSender(t, baseConfig())
	fixed := time.Date(2025, 6, 26, 14, 52, 33, 581000000, time.UTC)
	s.now = func() time.Time { return fixed }
	msg := []byte(`{"MESSAGE": "no-ts"}`)
	if err := s.SendMessages(context.Background(), [][]byte{msg}, "c1"); err != nil {
		t.Fatalf("SendMessages: %v", err)
	}
	got := client.calls[0].Timestamp
	if got != "2025-06-26T14:52:33.581000Z" {
		t.Fatalf("fallback timestamp = %q", got)
	}
	if _, err := time.Parse("2006-01-02T15:04:05.000000Z", got); err != nil {
		t.Fatalf("fallback timestamp is not valid ISO: %v", err)
	}
}

// TestWarningLoggedWhenTimestampMissing ports
// test_warning_logged_when_timestamp_missing: a warning mentioning
// "timestamp" is emitted for each message lacking one.
func TestWarningLoggedWhenTimestampMissing(t *testing.T) {
	s, _, logger := newTestSender(t, baseConfig())
	msg := []byte(`{"MESSAGE": "no-ts"}`)
	if err := s.SendMessages(context.Background(), [][]byte{msg}, "c1"); err != nil {
		t.Fatalf("SendMessages: %v", err)
	}
	if len(logger.warnings) != 1 {
		t.Fatalf("warnings = %d, want 1", len(logger.warnings))
	}
	if !strings.Contains(strings.ToLower(logger.warnings[0]), "timestamp") {
		t.Fatalf("warning %q does not mention timestamp", logger.warnings[0])
	}
}

// TestTruncatedMessageWithoutTimestampDoesNotCrash ports
// test_truncated_message_without_timestamp_does_not_crash.
func TestTruncatedMessageWithoutTimestampDoesNotCrash(t *testing.T) {
	s, client, _ := newTestSender(t, baseConfig())
	truncated := []byte(`{"error": "too large message 3101559 bytes vs maximum 1048576 bytes", ` +
		`"partial_data": "{\"MESSAGE\": \"AUDI..."}`)
	if err := s.SendMessages(context.Background(), [][]byte{truncated}, "c1"); err != nil {
		t.Fatalf("SendMessages: %v", err)
	}
	if len(client.calls) != 1 {
		t.Fatalf("log calls = %d, want 1", len(client.calls))
	}
}

// TestPriorityUsedAsSeverity ports test_priority_used_as_severity.
func TestPriorityUsedAsSeverity(t *testing.T) {
	for _, tc := range []struct {
		priority string
		want     int
	}{{"3", 3}, {"6", 6}} {
		t.Run(tc.priority, func(t *testing.T) {
			s, client, _ := newTestSender(t, baseConfig())
			msg := []byte(`{"MESSAGE": "test", "timestamp": "2025-01-01T00:00:00.000000", "PRIORITY": "` + tc.priority + `"}`)
			if err := s.SendMessages(context.Background(), [][]byte{msg}, "c1"); err != nil {
				t.Fatalf("SendMessages: %v", err)
			}
			if got := client.calls[0].Severity; got != tc.want {
				t.Fatalf("severity = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestFieldMapping pins the journald-field → client.log mapping
// (rsyslog.py:60-99): facility from SYSLOG_FACILITY (scalar or list
// first element, out-of-range/non-int → default), hostname, the
// appname key chain, pid, msg.
func TestFieldMapping(t *testing.T) {
	cases := []struct {
		name     string
		payload  string
		config   map[string]any
		facility int
		severity int
		hostname *string
		program  *string
		pid      any
	}{
		{
			name:     "facility_list_first_element",
			payload:  `{"SYSLOG_FACILITY": ["3", "4"], "PRIORITY": "6", "timestamp": "t"}`,
			config:   baseConfig(),
			facility: 3, severity: 6, pid: nil,
		},
		{
			name:     "facility_out_of_range_default",
			payload:  `{"SYSLOG_FACILITY": "bogus", "PRIORITY": "6", "timestamp": "t"}`,
			config:   baseConfig(),
			facility: 1, severity: 6, pid: nil,
		},
		{
			name:     "custom_defaults",
			payload:  `{"PRIORITY": "3", "timestamp": "t"}`,
			config:   map[string]any{"rsyslog_server": "127.0.0.1", "default_facility": 20, "default_severity": 3},
			facility: 20, severity: 3, pid: nil,
		},
		{
			name:     "appname_prefers_syslog_identifier",
			payload:  `{"SYSLOG_IDENTIFIER": "ident", "SYSTEMD_UNIT": "unit", "PROCESS_NAME": "proc", "PRIORITY": "6", "timestamp": "t"}`,
			config:   baseConfig(),
			facility: 1, severity: 6, program: strptr("ident"), pid: nil,
		},
		{
			name:     "appname_falls_back_to_process_name",
			payload:  `{"PROCESS_NAME": "proc", "PRIORITY": "6", "timestamp": "t"}`,
			config:   baseConfig(),
			facility: 1, severity: 6, program: strptr("proc"), pid: nil,
		},
		{
			name:     "hostname_and_pid",
			payload:  `{"HOSTNAME": "host.example", "PID": "4321", "PRIORITY": "6", "timestamp": "t"}`,
			config:   baseConfig(),
			facility: 1, severity: 6, hostname: strptr("host.example"), pid: "4321",
		},
		{
			name:     "pid_zero_is_nilvalue",
			payload:  `{"PID": 0, "PRIORITY": "6", "timestamp": "t"}`,
			config:   baseConfig(),
			facility: 1, severity: 6, pid: nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, client, _ := newTestSender(t, tc.config)
			if err := s.SendMessages(context.Background(), [][]byte{[]byte(tc.payload)}, "c1"); err != nil {
				t.Fatalf("SendMessages: %v", err)
			}
			got := client.calls[0]
			if got.Facility != tc.facility || got.Severity != tc.severity {
				t.Fatalf("facility/severity = %d/%d, want %d/%d", got.Facility, got.Severity, tc.facility, tc.severity)
			}
			if !eqStrPtr(got.Hostname, tc.hostname) {
				t.Fatalf("hostname = %v, want %v", got.Hostname, tc.hostname)
			}
			if !eqStrPtr(got.Program, tc.program) {
				t.Fatalf("program = %v, want %v", got.Program, tc.program)
			}
			if !eqAny(got.PID, tc.pid) {
				t.Fatalf("pid = %v, want %v", got.PID, tc.pid)
			}
		})
	}
}

// TestStructuredDataFromConfig proves the sender passes structured_data
// through to every message.
func TestStructuredDataFromConfig(t *testing.T) {
	cfg := baseConfig()
	cfg["structured_data"] = `x="y"`
	s, client, _ := newTestSender(t, cfg)
	msg := []byte(`{"MESSAGE": "hello", "PRIORITY": "6", "timestamp": "t"}`)
	if err := s.SendMessages(context.Background(), [][]byte{msg}, "c1"); err != nil {
		t.Fatalf("SendMessages: %v", err)
	}
	if client.calls[0].SD == nil || *client.calls[0].SD != `x="y"` {
		t.Fatalf("SD = %v, want x=\"y\"", client.calls[0].SD)
	}
}

// TestSendFailureDropsClient proves a send error drops the client so the
// next send reconnects (Python's re-init path), and that a non-retriable
// error is classified as unexpected (Retriable false).
func TestSendFailureDropsClient(t *testing.T) {
	s, client, _ := newTestSender(t, baseConfig())
	client.failWith = fmt.Errorf("boom")
	msg := []byte(`{"MESSAGE": "hello", "PRIORITY": "6", "timestamp": "t"}`)
	if err := s.SendMessages(context.Background(), [][]byte{msg}, "c1"); err == nil {
		t.Fatal("SendMessages succeeded, want the failure")
	}
	if client.closed == 0 {
		t.Fatal("failed client was not closed/dropped")
	}
	if Retriable(fmt.Errorf("boom")) {
		t.Fatal("a generic error must classify as non-retriable")
	}
}

// TestInitLoopRetriesRetriableConnectErrors ports the retry arm of
// rsyslog.py:_init_rsyslog_client: a retriable connection failure logs a
// warning, sleeps backoff+5s, and retries until the client comes up.
func TestInitLoopRetriesRetriableConnectErrors(t *testing.T) {
	s, client, logger := newTestSender(t, baseConfig())
	attempts := 0
	var slept []time.Duration
	s.sleep = func(_ context.Context, d time.Duration) bool {
		slept = append(slept, d)
		return true
	}
	s.newClient = func(context.Context) (logClient, error) {
		attempts++
		if attempts < 3 {
			return nil, syscall.ECONNREFUSED
		}
		return client, nil
	}
	msg := []byte(`{"MESSAGE": "hello", "PRIORITY": "6", "timestamp": "t"}`)
	if err := s.SendMessages(context.Background(), [][]byte{msg}, "c1"); err != nil {
		t.Fatalf("SendMessages: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("connect attempts = %d, want 3", attempts)
	}
	if len(slept) != 2 {
		t.Fatalf("init sleeps = %d, want 2", len(slept))
	}
	for _, d := range slept {
		// Python's _backoff() jittered sleep PLUS the flat 5 s sleep.
		if d < 5*time.Second {
			t.Fatalf("init sleep %s shorter than the 5 s extra sleep", d)
		}
	}
	if len(logger.warnings) != 2 {
		t.Fatalf("init warnings = %d, want 2", len(logger.warnings))
	}
	if len(client.calls) != 1 {
		t.Fatalf("log calls = %d, want 1", len(client.calls))
	}
}

// TestInitLoopCancelledDuringBackoff proves the init loop is interruptible:
// a cancellation during the backoff+5s wait stops the retry and surfaces
// the context error (shutdown must not hang).
func TestInitLoopCancelledDuringBackoff(t *testing.T) {
	s, _, _ := newTestSender(t, baseConfig())
	ctx, cancel := context.WithCancel(context.Background())
	s.sleep = func(_ context.Context, _ time.Duration) bool {
		cancel()
		return false
	}
	s.newClient = func(context.Context) (logClient, error) { return nil, syscall.ECONNREFUSED }
	msg := []byte(`{"MESSAGE": "hello", "PRIORITY": "6", "timestamp": "t"}`)
	if err := s.SendMessages(ctx, [][]byte{msg}, "c1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("SendMessages error = %v, want context.Canceled", err)
	}
}

// TestInitLoopNonRetriableReturnsImmediately proves a non-RSYSLOG_CONN_ERRORS
// connect failure (e.g. a missing rsyslog_server) is not retried in the
// init loop — it propagates for the shared loop's unexpected-exception path.
func TestInitLoopNonRetriableReturnsImmediately(t *testing.T) {
	s, _, _ := newTestSender(t, baseConfig())
	s.sleep = func(context.Context, time.Duration) bool {
		t.Fatal("non-retriable failure must not sleep in the init loop")
		return true
	}
	s.newClient = func(context.Context) (logClient, error) { return nil, errors.New("bad config") }
	msg := []byte(`{"MESSAGE": "hello", "PRIORITY": "6", "timestamp": "t"}`)
	if err := s.SendMessages(context.Background(), [][]byte{msg}, "c1"); err == nil {
		t.Fatal("SendMessages succeeded, want the non-retriable failure")
	}
}

// TestInstanceClassifierRoutesConnErrors proves New installs the rsyslog
// classifier: RSYSLOG_CONN_ERRORS are retriable, anything else is the
// Python generic-except path.
func TestInstanceClassifierRoutesConnErrors(t *testing.T) {
	inst, err := New(sender.ConstructorArgs{Name: "t", Config: baseConfig()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if inst.Classifier == nil {
		t.Fatal("Instance.Classifier is nil; the rsyslog classification would not run")
	}
	if !inst.Classifier.Retriable(syscall.ECONNREFUSED) {
		t.Fatal("ECONNREFUSED must be retriable (RSYSLOG_CONN_ERRORS)")
	}
	if inst.Classifier.Retriable(errors.New("boom")) {
		t.Fatal("a generic error must take the unexpected-exception path")
	}
}

func strptr(s string) *string { return &s }

func eqStrPtr(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func eqAny(a, b any) bool {
	as, aok := a.(string)
	bs, bok := b.(string)
	if aok || bok {
		return aok && bok && as == bs
	}
	return a == b
}
