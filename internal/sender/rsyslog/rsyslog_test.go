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
	"sync"
	"syscall"
	"testing"
	"testing/synctest"
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

// TestSendFailureRefusedReconnectThenSuccess covers the whole reconnect
// cycle the reviewer asked for: a send failure drops the client, a
// REFUSED reconnect returns a retriable error after the flat 5 s init
// sleep (no private retry counter — the shared loop owns the backoff),
// and a subsequent successful reconnect delivers the batch. The error is
// retriable so the loop routes it through mark_disconnected (health
// errors) + shared backoff.
func TestSendFailureRefusedReconnectThenSuccess(t *testing.T) {
	s, first, logger := newTestSender(t, baseConfig())
	first.failWith = errors.New("write failed")
	var slept []time.Duration
	s.sleep = func(_ context.Context, d time.Duration) bool {
		slept = append(slept, d)
		return true
	}
	second := &recordClient{}
	attempts := 0
	s.newClient = func(context.Context) (logClient, error) {
		attempts++
		switch attempts {
		case 1:
			return first, nil
		case 2:
			return nil, syscall.ECONNREFUSED // refused reconnect
		default:
			return second, nil // successful reconnect
		}
	}
	msg := []byte(`{"MESSAGE": "hello", "PRIORITY": "6", "timestamp": "t"}`)

	// 1) A send failure drops the client so the next attempt reconnects.
	if err := s.SendMessages(context.Background(), [][]byte{msg}, "c1"); err == nil {
		t.Fatal("SendMessages succeeded, want the send failure")
	}
	if first.closed == 0 {
		t.Fatal("failed client was not closed/dropped")
	}

	// 2) The refused reconnect returns a RETRIABLE error (loop path) after
	// exactly the flat 5 s init sleep — no private exponential backoff.
	err := s.SendMessages(context.Background(), [][]byte{msg}, "c2")
	if err == nil {
		t.Fatal("SendMessages succeeded, want the refused reconnect")
	}
	if !Retriable(err) {
		t.Fatalf("ECONNREFUSED reconnect error must classify retriable, got %v", err)
	}
	if len(slept) != 1 || slept[0] != initRetrySleep {
		t.Fatalf("init sleeps = %v, want [%s] (loop owns the exponential backoff)", slept, initRetrySleep)
	}
	if len(logger.warnings) != 1 {
		t.Fatalf("init warnings = %d, want 1", len(logger.warnings))
	}

	// 3) A successful reconnect delivers the batch through the new client.
	if err := s.SendMessages(context.Background(), [][]byte{msg}, "c3"); err != nil {
		t.Fatalf("SendMessages after reconnect: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("connect attempts = %d, want 3", attempts)
	}
	if len(second.calls) != 1 {
		t.Fatalf("reconnected client log calls = %d, want 1", len(second.calls))
	}
}

// TestInitRetriableFailureCancelledDuringSleep proves the flat 5 s init
// wait is interruptible WITHOUT changing the observable failure: Python
// records the connection error (mark_disconnected) BEFORE its sleep
// (rsyslog.py:62-70), so a cancellation during the wait must still surface
// the original error in the health ring and as the returned error, never
// context.Canceled.
func TestInitRetriableFailureCancelledDuringSleep(t *testing.T) {
	s, _, _ := newTestSender(t, baseConfig())
	var recorded []error
	s.SetConnectionObserver(nil, func(err error) { recorded = append(recorded, err) })
	ctx, cancel := context.WithCancel(context.Background())
	s.sleep = func(_ context.Context, _ time.Duration) bool {
		if len(recorded) != 1 || !errors.Is(recorded[0], syscall.ECONNREFUSED) {
			t.Fatalf("disconnect recorded before the sleep = %v, want the original ECONNREFUSED", recorded)
		}
		cancel()
		return false
	}
	s.newClient = func(context.Context) (logClient, error) { return nil, syscall.ECONNREFUSED }
	msg := []byte(`{"MESSAGE": "hello", "PRIORITY": "6", "timestamp": "t"}`)
	err := s.SendMessages(ctx, [][]byte{msg}, "c1")
	if !errors.Is(err, syscall.ECONNREFUSED) {
		t.Fatalf("SendMessages error = %v, want the original ECONNREFUSED", err)
	}
	if errors.Is(err, context.Canceled) {
		t.Fatalf("SendMessages error = %v, must not be replaced by context.Canceled", err)
	}
	if len(recorded) != 1 || !errors.Is(recorded[0], syscall.ECONNREFUSED) {
		t.Fatalf("recorded = %v, want the ECONNREFUSED recorded before the sleep", recorded)
	}
}

// TestNonStringCACertsRejected is the validation regression for the
// truthy non-string ca_certs bug: Python's
// ssl.create_default_context(cafile=<value>) raises on a non-string
// value, so a value like 1 must be rejected rather than silently mapped
// to an empty cafile (system roots) while still selecting TLS.
func TestNonStringCACertsRejected(t *testing.T) {
	cfg := baseConfig()
	cfg["ca_certs"] = 1
	inst, err := New(sender.ConstructorArgs{Name: "t", Config: cfg})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	s := inst.Transport.(*Sender)
	if _, err := s.resolvedConfig(); err == nil {
		t.Fatal("resolvedConfig accepted a non-string ca_certs; want an error")
	}
	msg := []byte(`{"MESSAGE": "hello", "PRIORITY": "6", "timestamp": "t"}`)
	if err := s.SendMessages(context.Background(), [][]byte{msg}, "c1"); err == nil {
		t.Fatal("SendMessages accepted a non-string ca_certs; want an error")
	}
}

// TestLoopReconnectResetsSharedBackoff is the REAL sender.Loop integration
// regression for the mark_connected reset: every attempt connects
// successfully and then fails its payload (list-valued PRIORITY), so the
// client is dropped and the next attempt reconnects. Python's
// mark_connected resets the backoff exponent on each successful connect,
// so the reconnect cadence must stay at the attempt-1 backoff. Without the
// reset the exponent escalates (0.5s, 1s, 2s, 4s, 8s ...), which caps the
// number of attempts that fit in a fixed window. The fake clock makes the
// separation deterministic: >=8 attempts in 10 s only holds with the reset.
func TestLoopReconnectResetsSharedBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		inst, err := New(sender.ConstructorArgs{Name: "rsyslog-loop", Config: baseConfig(), Log: &recordLogger{}})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		s := inst.Transport.(*Sender)
		var mu sync.Mutex
		connects := 0
		s.newClient = func(context.Context) (logClient, error) {
			mu.Lock()
			connects++
			mu.Unlock()
			return &recordClient{}, nil
		}
		in := make(chan sender.Item, 10)
		loop := sender.NewLoop(sender.LoopConfig{Name: "rsyslog-loop", Log: &recordLogger{}}, inst, in)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			_ = loop.Run(ctx)
			close(done)
		}()
		// PRIORITY [6] fails pythonInt after every successful connect, so
		// each retry reconnects and then fails its payload again.
		in <- sender.Item{Payload: []byte(`{"PRIORITY":[6],"MESSAGE":"m","timestamp":"t"}`), Cursor: "c0"}
		time.Sleep(10 * time.Second)
		synctest.Wait()
		mu.Lock()
		got := connects
		mu.Unlock()
		cancel()
		<-done
		if got < 8 {
			t.Fatalf("connects in 10s = %d, want >= 8: a successful reconnect must reset the shared backoff exponent", got)
		}
	})
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

// TestNumericFieldsKeepIntegerIdentity is the regression for the
// json.Unmarshal float64 collapse: an INTEGER JSON PID must render
// exactly Python's str(100) == "100", not "100.0"; a FLOAT PID keeps its
// fraction (Python str(100.0) == "100.0"). Numeric PRIORITY and
// SYSLOG_FACILITY are exercised too.
func TestNumericFieldsKeepIntegerIdentity(t *testing.T) {
	cases := []struct {
		name string
		msg  string
		pid  any
	}{
		{"integer pid", `{"MESSAGE":"m","PRIORITY":6,"timestamp":"t","PID":100}`, "100"},
		{"float pid", `{"MESSAGE":"m","PRIORITY":6,"timestamp":"t","PID":100.0}`, "100.0"},
		{"negative integer pid", `{"MESSAGE":"m","PRIORITY":6,"timestamp":"t","PID":-7}`, "-7"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, client, _ := newTestSender(t, baseConfig())
			if err := s.SendMessages(context.Background(), [][]byte{[]byte(tc.msg)}, "c1"); err != nil {
				t.Fatalf("SendMessages: %v", err)
			}
			if got := client.calls[0].PID; got != tc.pid {
				t.Fatalf("pid = %v, want %v", got, tc.pid)
			}
		})
	}

	t.Run("numeric priority and facility", func(t *testing.T) {
		s, client, _ := newTestSender(t, baseConfig())
		msg := []byte(`{"MESSAGE":"m","PRIORITY":3,"timestamp":"t","SYSLOG_FACILITY":20}`)
		if err := s.SendMessages(context.Background(), [][]byte{msg}, "c1"); err != nil {
			t.Fatalf("SendMessages: %v", err)
		}
		got := client.calls[0]
		if got.Severity != 3 || got.Facility != 20 {
			t.Fatalf("severity/facility = %d/%d, want 3/20", got.Severity, got.Facility)
		}
	})
}

// TestEmptyCACertsStillSelectsTLS is the sender-side half of the
// ca_certs presence rule: an explicitly empty ca_certs must map to
// CACertsPresent (Python's `cacerts is not None`), so it cannot silently
// select plaintext; an absent or null ca_certs does not.
func TestEmptyCACertsStillSelectsTLS(t *testing.T) {
	cases := []struct {
		name    string
		config  map[string]any
		present bool
		ssl     bool
	}{
		{"present empty string", map[string]any{"ca_certs": ""}, true, false},
		{"present path", map[string]any{"ca_certs": "/etc/ca.pem"}, true, false},
		{"absent", map[string]any{}, false, false},
		{"present null", map[string]any{"ca_certs": nil}, false, false},
		{"ssl with absent ca_certs", map[string]any{"ssl": true}, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := baseConfig()
			for k, v := range tc.config {
				cfg[k] = v
			}
			inst, err := New(sender.ConstructorArgs{Name: "t", Config: cfg})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			rc, err := inst.Transport.(*Sender).resolvedConfig()
			if err != nil {
				t.Fatalf("resolvedConfig: %v", err)
			}
			if rc.client.CACertsPresent != tc.present {
				t.Fatalf("CACertsPresent = %v, want %v", rc.client.CACertsPresent, tc.present)
			}
			wantProto := "PLAINTEXT"
			if tc.ssl {
				wantProto = "SSL"
			}
			if rc.client.Protocol != wantProto {
				t.Fatalf("Protocol = %q, want %q", rc.client.Protocol, wantProto)
			}
		})
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
