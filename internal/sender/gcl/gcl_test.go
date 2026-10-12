// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package gcl

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/api/logging/v2"
	"google.golang.org/api/option"

	"github.com/Aiven-Open/journalpump/internal/httpx"
	"github.com/Aiven-Open/journalpump/internal/sender"
)

// fakeWriter captures the WriteLogEntriesRequest the sender builds and can
// fail it, standing in for the Cloud Logging client.
type fakeWriter struct {
	mu   sync.Mutex
	reqs []*logging.WriteLogEntriesRequest
	err  error
}

func (f *fakeWriter) WriteLogEntries(_ context.Context, req *logging.WriteLogEntriesRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reqs = append(f.reqs, req)
	return f.err
}

// callCount is the number of write attempts (successes and failures).
func (f *fakeWriter) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.reqs)
}

// setErr swaps the write result so a loop test can fail repeatedly, then
// recover.
func (f *fakeWriter) setErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

// fakeStats records unexpected_exception emissions.
type fakeStats struct {
	mu     sync.Mutex
	errs   []error
	wheres []string
	tags   []map[string]string
}

func (f *fakeStats) UnexpectedException(ex error, where string, tags map[string]string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.errs = append(f.errs, ex)
	f.wheres = append(f.wheres, where)
	f.tags = append(f.tags, tags)
}

func (f *fakeStats) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.errs)
}

// testOptions is the Python test CONFIG's project/log/labels (minus the SA
// credentials, which the transformation tests do not need).
func testOptions() options {
	return options{
		projectID:      "project-id",
		logID:          "log-id",
		resourceLabels: map[string]string{"location": "us-east-1", "node_id": "my-test-node"},
	}
}

// newTestSender builds a Sender over a fake writer with the Python fixture
// config.
func newTestSender(w writer) *Sender {
	return newSender(nil, nil, testOptions(), w)
}

// entryMaps renders the request's entries as plain maps for comparison.
func entryMaps(t *testing.T, req *logging.WriteLogEntriesRequest) []map[string]any {
	t.Helper()
	out := make([]map[string]any, 0, len(req.Entries))
	for _, e := range req.Entries {
		out = append(out, decodeMap(t, e))
	}
	return out
}

func decodeMap(t *testing.T, v any) map[string]any {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func assertRequestEnvelope(t *testing.T, req *logging.WriteLogEntriesRequest) {
	t.Helper()
	if req.LogName != "projects/project-id/logs/log-id" {
		t.Errorf("logName = %q", req.LogName)
	}
	if req.Resource == nil || req.Resource.Type != "generic_node" {
		t.Errorf("resource = %+v", req.Resource)
	}
	if !reflect.DeepEqual(req.Resource.Labels, map[string]string{"location": "us-east-1", "node_id": "my-test-node"}) {
		t.Errorf("resource labels = %v", req.Resource.Labels)
	}
}

// TestMessageString ports test_message_string.
func TestMessageString(t *testing.T) {
	w := &fakeWriter{}
	s := newTestSender(w)
	if err := s.SendMessages(context.Background(), [][]byte{[]byte(`{"MESSAGE": "Hello"}`)}, ""); err != nil {
		t.Fatal(err)
	}
	req := w.reqs[0]
	assertRequestEnvelope(t, req)
	want := []map[string]any{{"jsonPayload": map[string]any{"MESSAGE": "Hello"}}}
	if got := entryMaps(t, req); !reflect.DeepEqual(got, want) {
		t.Errorf("entries = %#v, want %#v", got, want)
	}
}

// TestMissingMessage ports test_missing_message: an entry without MESSAGE
// is fine.
func TestMissingMessage(t *testing.T) {
	w := &fakeWriter{}
	s := newTestSender(w)
	if err := s.SendMessages(context.Background(), [][]byte{[]byte(`{"not_message": "Hello"}`)}, ""); err != nil {
		t.Fatal(err)
	}
	want := []map[string]any{{"jsonPayload": map[string]any{"not_message": "Hello"}}}
	if got := entryMaps(t, w.reqs[0]); !reflect.DeepEqual(got, want) {
		t.Errorf("entries = %#v, want %#v", got, want)
	}
}

// TestMessageJSON ports test_message_json: a MESSAGE that is itself a JSON
// object string is parsed into jsonPayload.
func TestMessageJSON(t *testing.T) {
	w := &fakeWriter{}
	s := newTestSender(w)
	message := `{"MESSAGE": "{\"test_key\": \"test_value\"}"}`
	if err := s.SendMessages(context.Background(), [][]byte{[]byte(message)}, ""); err != nil {
		t.Fatal(err)
	}
	want := []map[string]any{{
		"jsonPayload": map[string]any{"MESSAGE": map[string]any{"test_key": "test_value"}},
	}}
	if got := entryMaps(t, w.reqs[0]); !reflect.DeepEqual(got, want) {
		t.Errorf("entries = %#v, want %#v", got, want)
	}
}

// TestBadRequestNotMarkedSent ports test_bad_request_did_not_marked_sent: a
// Cloud Logging error fails the batch (the loop does not advance the cursor)
// and emits unexpected_exception.
func TestBadRequestNotMarkedSent(t *testing.T) {
	w := &fakeWriter{err: errors.New("400 bad request")}
	stats := &fakeStats{}
	s := newSender(nil, stats, testOptions(), w)
	err := s.SendMessages(context.Background(), [][]byte{[]byte(`{"MESSAGE": "Hello"}`)}, "")
	if err == nil {
		t.Fatal("expected the batch to fail")
	}
	var backoff *sender.BackoffError
	if !errors.As(err, &backoff) {
		t.Errorf("error = %v, want a sender.BackoffError (backoff already performed)", err)
	}
	if stats.count() != 1 {
		t.Fatalf("unexpected_exception emissions = %d, want 1", stats.count())
	}
	if stats.wheres[0] != "sender" || stats.tags[0]["app"] != "journalpump" {
		t.Errorf("stats where=%q tags=%v", stats.wheres[0], stats.tags[0])
	}
}

// TestCorrectTimestamp ports test_correct_timestamp: the 26-character
// timestamp gets a "Z", PRIORITY 0 maps to EMERGENCY.
func TestCorrectTimestamp(t *testing.T) {
	w := &fakeWriter{}
	s := newTestSender(w)
	msg := `{"MESSAGE": "Hello", "PRIORITY": 0, "timestamp": "2020-06-25T06:24:13.787255"}`
	if err := s.SendMessages(context.Background(), [][]byte{[]byte(msg)}, ""); err != nil {
		t.Fatal(err)
	}
	want := []map[string]any{{
		"timestamp":   "2020-06-25T06:24:13.787255Z",
		"severity":    "EMERGENCY",
		"jsonPayload": map[string]any{"MESSAGE": "Hello"},
	}}
	if got := entryMaps(t, w.reqs[0]); !reflect.DeepEqual(got, want) {
		t.Errorf("entries = %#v, want %#v", got, want)
	}
}

// TestBigLogEntryIsTruncated ports test_big_logentry_is_truncated. Note the
// Python fixture uses "A" * 257_00 = 25,700 characters, below the 250 KiB
// quota, so the MESSAGE passes through intact.
func TestBigLogEntryIsTruncated(t *testing.T) {
	w := &fakeWriter{}
	s := newTestSender(w)
	messageContent := strings.Repeat("A", 25700)
	message, err := json.Marshal(map[string]any{"MESSAGE": messageContent})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SendMessages(context.Background(), [][]byte{message}, ""); err != nil {
		t.Fatal(err)
	}
	want := []map[string]any{{
		"jsonPayload": map[string]any{"MESSAGE": strings.Repeat("A", 25700)},
	}}
	if got := entryMaps(t, w.reqs[0]); !reflect.DeepEqual(got, want) {
		t.Errorf("entries = %#v, want %#v", got, want)
	}
}

// TestBigLogEntrySendsDefault ports test_big_logentry_sends_default: an entry
// too large even after MESSAGE truncation (and one with no MESSAGE) is
// replaced by a MESSAGE-only DEFAULT entry, and both are still sent.
func TestBigLogEntrySendsDefault(t *testing.T) {
	w := &fakeWriter{}
	s := newTestSender(w)

	oversizedWithMessage, err := json.Marshal(map[string]any{
		"MESSAGE":     strings.Repeat("A", 200000),
		"OTHER_FIELD": strings.Repeat("B", 200000),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SendMessages(context.Background(), [][]byte{oversizedWithMessage}, ""); err != nil {
		t.Fatal(err)
	}
	oversizedNoMessage, err := json.Marshal(map[string]any{"OTHER_FIELD": strings.Repeat("B", 257000)})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SendMessages(context.Background(), [][]byte{oversizedNoMessage}, ""); err != nil {
		t.Fatal(err)
	}
	if len(w.reqs) != 2 {
		t.Fatalf("requests = %d, want 2", len(w.reqs))
	}
	defaultEntry := map[string]any{"jsonPayload": map[string]any{"MESSAGE": defaultTruncatedMessage}}
	for i, req := range w.reqs {
		if got := entryMaps(t, req); !reflect.DeepEqual(got, []map[string]any{defaultEntry}) {
			t.Errorf("request %d entries = %#v, want %#v", i, got, []map[string]any{defaultEntry})
		}
	}
}

// TestTruncatedMessageAppendsMarker pins the intermediate case: an oversized
// entry whose truncated MESSAGE still fits keeps the truncated MESSAGE with
// the marker rather than falling back to DEFAULT.
func TestTruncatedMessageAppendsMarker(t *testing.T) {
	w := &fakeWriter{}
	s := newTestSender(w)
	// 256 KiB of MESSAGE pushes the raw message over the 250 KiB quota; the
	// truncated (200 KiB) MESSAGE keeps the entry well under quota.
	message, err := json.Marshal(map[string]any{"MESSAGE": strings.Repeat("A", 256*1024)})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SendMessages(context.Background(), [][]byte{message}, ""); err != nil {
		t.Fatal(err)
	}
	got := entryMaps(t, w.reqs[0])
	wantMessage := strings.Repeat("A", maxMessageSize) + truncationSuffix
	want := []map[string]any{{"jsonPayload": map[string]any{"MESSAGE": wantMessage}}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("entries = %#v, want a truncated MESSAGE", got)
	}
}

// TestUnicodeLineSeparatorSizeMatchesPython is the review item 5 regression.
// Go's json.Marshal escapes U+2028 into six ASCII bytes, so a 50,000-rune
// U+2028 MESSAGE becomes a line over the 250 KiB quota and takes the
// truncation branch. Python's recompute measures
// json.dumps(msg, ensure_ascii=False), where those runes are 3-byte UTF-8
// (150,000 bytes, well under quota), so Python keeps the truncated MESSAGE.
// Measuring with encoding/json would over-count to ~300,000 and wrongly swap
// in the DEFAULT replacement, DISCARDING the message content.
func TestUnicodeLineSeparatorSizeMatchesPython(t *testing.T) {
	w := &fakeWriter{}
	s := newTestSender(w)
	messageContent := strings.Repeat("\u2028", 50000)
	message, err := json.Marshal(map[string]any{"MESSAGE": messageContent})
	if err != nil {
		t.Fatal(err)
	}
	if len(message) <= logEntryQuota {
		t.Fatalf("fixture line = %d bytes, want > quota %d so truncation runs", len(message), logEntryQuota)
	}
	if err := s.SendMessages(context.Background(), [][]byte{message}, ""); err != nil {
		t.Fatal(err)
	}
	want := []map[string]any{{
		"jsonPayload": map[string]any{"MESSAGE": messageContent + truncationSuffix},
	}}
	if got := entryMaps(t, w.reqs[0]); !reflect.DeepEqual(got, want) {
		t.Errorf("entries = %#v, want the truncated MESSAGE kept (not the DEFAULT replacement)", got)
	}
}

// TestPythonJSONSize pins the size probe's Python semantics directly:
// U+2028/U+2029 count as raw 3-byte UTF-8 and separators match
// json.dumps defaults.
func TestPythonJSONSize(t *testing.T) {
	cases := []struct {
		name string
		in   any
		want int
	}{
		{"line_separator_is_raw_utf8", strings.Repeat("\u2028", 3), 3*3 + 2},
		{"paragraph_separator_is_raw_utf8", "\u2029", 3 + 2},
		{"python_separators", map[string]any{"a": json.Number("1"), "b": json.Number("2")}, len(`{"a": 1, "b": 2}`)},
		{"control_escaped", "\n", 2 + 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := pythonJSONSize(tc.in)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Errorf("pythonJSONSize = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestFailurePathOrderAndBackoff pins the optimistic-reconnect failure path:
// mark_disconnected, then the shared backoff, then mark_connected, and a
// sender.BackoffError so the loop does not wait twice.
func TestFailurePathOrderAndBackoff(t *testing.T) {
	w := &fakeWriter{err: errors.New("api error")}
	stats := &fakeStats{}
	s := newSender(nil, stats, testOptions(), w)
	var order []string
	s.SetConnectionObserver(
		func() { order = append(order, "connected") },
		func(error) { order = append(order, "disconnected") },
	)
	s.SetBackoffObserver(func(context.Context) { order = append(order, "backoff") })

	err := s.SendMessages(context.Background(), [][]byte{[]byte(`{"MESSAGE": "x"}`)}, "")
	var backoff *sender.BackoffError
	if !errors.As(err, &backoff) {
		t.Fatalf("error = %v, want sender.BackoffError", err)
	}
	if want := []string{"disconnected", "backoff", "connected"}; !reflect.DeepEqual(order, want) {
		t.Errorf("observer order = %v, want %v", order, want)
	}
	if stats.count() != 1 {
		t.Errorf("unexpected_exception emissions = %d, want 1", stats.count())
	}
}

// TestLoopRepeatedFailuresThenRecovery drives the GCL sender through the REAL
// sender.Loop under synctest (review item 8). It pins: a NON-escalating
// backoff (the failure path re-marks connected, resetting the loop exponent,
// so ~10 attempts fit in 10 virtual seconds where an escalating schedule
// would manage ~4), the unchanged failed-batch cursor and a zero sent count
// while every write fails, and Go's TRUTHFUL disconnected health during the
// outage — a deliberate deviation from Python, whose failure path ends with a
// false-success mark_connected. On recovery the sent count, cursor and
// connected health all converge.
func TestLoopRepeatedFailuresThenRecovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := &fakeWriter{err: errors.New("logging api unavailable")}
		stats := &fakeStats{}
		s := newSender(nil, stats, testOptions(), w)
		inst := sender.Instance{
			Transport:       s,
			MaxSendInterval: defaultMaxSendInterval,
			Type:            "GoogleCloudLoggingSender",
			ConnectOnInit:   true,
		}
		in := make(chan sender.Item, 4)
		loop := sender.NewLoop(sender.LoopConfig{Name: "gcl-loop"}, inst, in)
		// The supervisor marks ConnectOnInit senders connected at startup
		// (python __init__ calls mark_connected).
		loop.MarkConnected()
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { _ = loop.Run(ctx); close(done) }()

		in <- sender.Item{Payload: []byte(`{"MESSAGE": "retry-me"}`), Cursor: "c0"}
		time.Sleep(10 * time.Second)
		synctest.Wait()

		if got := w.callCount(); got < 7 {
			t.Errorf("write attempts in 10s = %d, want >= 7 (non-escalating backoff); an escalating schedule would give ~4", got)
		}
		mid := loop.State()
		if mid.Sent.Count != 0 {
			t.Errorf("sent count while failing = %d, want 0", mid.Sent.Count)
		}
		if mid.Sent.Cursor != nil {
			t.Errorf("cursor advanced to %v while failing; want nil (the failed batch cursor is unchanged)", *mid.Sent.Cursor)
		}
		if mid.Health.Status != "disconnected" {
			t.Errorf("health while failing = %q, want disconnected (Go's truthful state, retained over Python's false-success reset)", mid.Health.Status)
		}
		if len(mid.Health.Errors) == 0 {
			t.Error("health errors empty; the failing sends were not recorded")
		}

		w.setErr(nil)
		time.Sleep(2 * time.Second)
		synctest.Wait()
		st := loop.State()
		cancel()
		<-done

		if st.Sent.Count != 1 {
			t.Errorf("sent count after recovery = %d, want 1", st.Sent.Count)
		}
		if st.Sent.Cursor == nil || *st.Sent.Cursor != "c0" {
			t.Errorf("sent cursor after recovery = %v, want c0", st.Sent.Cursor)
		}
		if st.Health.Status != "connected" {
			t.Errorf("health after recovery = %q, want connected", st.Health.Status)
		}
		if len(st.Health.Errors) != 0 {
			t.Errorf("health errors after recovery = %v, want cleared", st.Health.Errors)
		}
	})
}

// TestSeverityMapping pins the journald PRIORITY → severity map and its
// non-integer fallbacks.
func TestSeverityMapping(t *testing.T) {
	cases := []struct {
		name string
		in   any
		want string
	}{
		{"emergency", json.Number("0"), "EMERGENCY"},
		{"alert", json.Number("1"), "ALERT"},
		{"critical", json.Number("2"), "CRITICAL"},
		{"error", json.Number("3"), "ERROR"},
		{"warning", json.Number("4"), "WARNING"},
		{"notice", json.Number("5"), "NOTICE"},
		{"info", json.Number("6"), "INFO"},
		{"debug", json.Number("7"), "DEBUG"},
		{"out_of_range", json.Number("8"), "DEFAULT"},
		{"negative", json.Number("-1"), "DEFAULT"},
		{"float", json.Number("1.0"), "DEFAULT"},
		{"string", "3", "DEFAULT"},
		// Python bools are ints: True == 1 → ALERT, False == 0 → EMERGENCY.
		{"bool_true", true, "ALERT"},
		{"bool_false", false, "EMERGENCY"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := severityFor(tc.in); got != tc.want {
				t.Errorf("severityFor(%v) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestMissingMessageOnOversizedEntry pins the no-MESSAGE replacement path
// directly.
func TestMissingMessageOnOversizedEntry(t *testing.T) {
	msg, err := parseMessage([]byte(`{"OTHER": "x"}`))
	if err != nil {
		t.Fatal(err)
	}
	got, err := truncateMessage(msg)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, map[string]any{"MESSAGE": defaultTruncatedMessage}) {
		t.Errorf("truncateMessage = %#v", got)
	}
}

// TestParseConfig pins the config decoding: required keys, resource-label
// coercion, service-account capture and the max_send_interval default.
func TestParseConfig(t *testing.T) {
	if _, err := parseConfig(map[string]any{"google_cloud_logging_log_id": "l"}); err == nil {
		t.Error("missing project id did not error")
	}
	if _, err := parseConfig(map[string]any{"google_cloud_logging_project_id": "p"}); err == nil {
		t.Error("missing log id did not error")
	}

	opts, err := parseConfig(map[string]any{
		"google_cloud_logging_project_id":      "p",
		"google_cloud_logging_log_id":          "l",
		"google_cloud_logging_resource_labels": map[string]any{"node_id": "n", "count": json.Number("2"), "flag": true},
		"max_send_interval":                    1.5,
	})
	if err != nil {
		t.Fatal(err)
	}
	if opts.maxSendInterval != 1500*time.Millisecond {
		t.Errorf("max_send_interval = %v", opts.maxSendInterval)
	}
	wantLabels := map[string]string{"node_id": "n", "count": "2", "flag": "true"}
	if !reflect.DeepEqual(opts.resourceLabels, wantLabels) {
		t.Errorf("labels = %v, want %v", opts.resourceLabels, wantLabels)
	}

	if _, err := parseConfig(map[string]any{
		"google_cloud_logging_project_id": "p",
		"google_cloud_logging_log_id":     "l",
		"max_send_interval":               0,
	}); err == nil {
		t.Error("non-positive max_send_interval did not error")
	}

	if _, err := parseConfig(map[string]any{
		"google_cloud_logging_project_id":      "p",
		"google_cloud_logging_log_id":          "l",
		"google_cloud_logging_resource_labels": map[string]any{"nested": map[string]any{"a": 1}},
	}); err == nil {
		t.Error("non-scalar resource label did not error")
	}
}

func gcpServiceAccountConfig(t *testing.T) map[string]any {
	t.Helper()
	keyPath := filepath.Join("testdata", "gcp_private_key.pem")
	key, err := os.ReadFile(keyPath)
	if err != nil {
		t.Fatal(err)
	}
	return map[string]any{
		"type":           "service_account",
		"project_id":     "sa-project",
		"private_key_id": "abcdefg",
		"private_key":    string(key),
		"client_email":   "test@sa-project.iam.gserviceaccount.com",
		"client_id":      "123456789",
		"token_uri":      "https://oauth2.googleapis.com/token",
	}
}

// TestNewServiceAccountOverridesProjectID pins google_cloud_logging.py:50-53:
// with google_service_account_credentials the credential's project_id
// replaces the configured one. Credential parsing is local, so this needs no
// network.
func TestNewServiceAccountOverridesProjectID(t *testing.T) {
	config := map[string]any{
		"google_cloud_logging_project_id":    "config-project",
		"google_cloud_logging_log_id":        "log-id",
		"google_service_account_credentials": gcpServiceAccountConfig(t),
	}

	inst, err := New(sender.ConstructorArgs{Name: "gcl", Config: config})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if inst.Type != "GoogleCloudLoggingSender" {
		t.Errorf("type = %q", inst.Type)
	}
	if !inst.ConnectOnInit {
		t.Error("ConnectOnInit = false, want true (Python marks connected in __init__)")
	}
	if inst.MaxSendInterval != defaultMaxSendInterval {
		t.Errorf("max_send_interval = %v, want %v", inst.MaxSendInterval, defaultMaxSendInterval)
	}
	s, ok := inst.Transport.(*Sender)
	if !ok {
		t.Fatalf("transport = %T", inst.Transport)
	}
	if s.projectID != "sa-project" {
		t.Errorf("projectID = %q, want the service account's sa-project", s.projectID)
	}
}

// TestNewMissingProjectID pins the required-key error.
func TestNewMissingProjectID(t *testing.T) {
	_, err := New(sender.ConstructorArgs{Name: "gcl", Config: map[string]any{}})
	if err == nil {
		t.Fatal("New without project id did not error")
	}
}

// TestWriteWireShape drives the REAL generated logging/v2 client at an
// httptest server through the injectable HTTP transport, asserting the
// request the generated client puts on the wire.
func TestWriteWireShape(t *testing.T) {
	var (
		mu   sync.Mutex
		body map[string]any
		path string
		meth string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		path, meth = r.URL.Path, r.Method
		_ = json.NewDecoder(r.Body).Decode(&body)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	client := httpx.NewClient(httpx.ClientOptions{Proxy: httpx.Httplib2Proxy})
	svc, err := logging.NewService(
		context.Background(),
		option.WithHTTPClient(client),
		option.WithEndpoint(srv.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}
	s := newSender(nil, nil, testOptions(), serviceWriter{svc: svc})
	if err := s.SendMessages(context.Background(), [][]byte{[]byte(`{"MESSAGE": "Hello"}`)}, ""); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if meth != http.MethodPost {
		t.Errorf("method = %q", meth)
	}
	if !strings.HasSuffix(path, "/v2/entries:write") {
		t.Errorf("path = %q", path)
	}
	if body["logName"] != "projects/project-id/logs/log-id" {
		t.Errorf("logName = %v", body["logName"])
	}
	resource, _ := body["resource"].(map[string]any)
	if resource["type"] != "generic_node" {
		t.Errorf("resource = %v", resource)
	}
	entries, _ := body["entries"].([]any)
	if len(entries) != 1 {
		t.Fatalf("entries = %v", entries)
	}
	entry, _ := entries[0].(map[string]any)
	payload, _ := entry["jsonPayload"].(map[string]any)
	if payload["MESSAGE"] != "Hello" {
		t.Errorf("jsonPayload = %v", payload)
	}
}

// TestWriteHTTPErrorFailsBatch proves an HTTP error response from the real
// client fails the batch (so the loop retries rather than advancing).
func TestWriteHTTPErrorFailsBatch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error": {"code": 400, "message": "bad"}}`))
	}))
	defer srv.Close()

	client := httpx.NewClient(httpx.ClientOptions{})
	svc, err := logging.NewService(
		context.Background(),
		option.WithHTTPClient(client),
		option.WithEndpoint(srv.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}
	s := newSender(nil, nil, testOptions(), serviceWriter{svc: svc})
	err = s.SendMessages(context.Background(), [][]byte{[]byte(`{"MESSAGE": "Hello"}`)}, "")
	if err == nil {
		t.Fatal("a 400 response did not fail the batch")
	}
	var backoff *sender.BackoffError
	if !errors.As(err, &backoff) {
		t.Errorf("error = %v, want sender.BackoffError", err)
	}
}

// TestMaintenanceOperationsIsNoOp pins the inherited no-op.
func TestMaintenanceOperationsIsNoOp(t *testing.T) {
	s := newTestSender(&fakeWriter{})
	if err := s.MaintenanceOperations(context.Background()); err != nil {
		t.Errorf("MaintenanceOperations = %v, want nil", err)
	}
}

// TestInvalidJSONMessageFailsBatch pins the generic-except path for a
// non-object payload: the batch fails and is retried.
func TestInvalidJSONMessageFailsBatch(t *testing.T) {
	w := &fakeWriter{}
	s := newTestSender(w)
	err := s.SendMessages(context.Background(), [][]byte{[]byte(`not json`)}, "")
	if err == nil {
		t.Fatal("invalid JSON did not fail the batch")
	}
	if len(w.reqs) != 0 {
		t.Errorf("writer called %d times, want 0", len(w.reqs))
	}
}

// TestTimestampTruncatedTo26Chars pins timestamp[:26] + "Z": a longer
// timestamp is cut to 26 characters before the Z is appended.
func TestTimestampTruncatedTo26Chars(t *testing.T) {
	w := &fakeWriter{}
	s := newTestSender(w)
	msg := `{"MESSAGE": "x", "timestamp": "2020-06-25T06:24:13.787255123"}`
	if err := s.SendMessages(context.Background(), [][]byte{[]byte(msg)}, ""); err != nil {
		t.Fatal(err)
	}
	got := entryMaps(t, w.reqs[0])
	if got[0]["timestamp"] != "2020-06-25T06:24:13.787255Z" {
		t.Errorf("timestamp = %v", got[0]["timestamp"])
	}
}

// clearProxyEnv unsets every proxy variable the shared transport consults, so
// a construction test starts from a known no-proxy state.
func clearProxyEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{"http_proxy", "HTTP_PROXY", "https_proxy", "HTTPS_PROXY", "no_proxy", "NO_PROXY", "ALL_PROXY", "all_proxy"} {
		t.Setenv(k, "")
	}
}

// unsetConfigEnv removes key for the test and restores the prior value after.
// GCL distinguishes an UNSET HTTPLIB2_CA_CERTS (use the default roots) from an
// EMPTY one (rejected), so tests must genuinely unset it.
func unsetConfigEnv(t *testing.T, key string) {
	t.Helper()
	old, ok := os.LookupEnv(key)
	if err := os.Unsetenv(key); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if ok {
			_ = os.Setenv(key, old)
			return
		}
		_ = os.Unsetenv(key)
	})
}

// writeADCFile writes an authorized_user application-default-credentials file
// whose token endpoint is tokenURI and points GOOGLE_APPLICATION_CREDENTIALS at
// it, so FindDefaultCredentials picks it up exactly as in production.
func writeADCFile(t *testing.T, tokenURI string) {
	t.Helper()
	data, err := json.Marshal(map[string]string{
		"type":          "authorized_user",
		"client_id":     "abc.apps.googleusercontent.com",
		"client_secret": "secret",
		"refresh_token": "refresh-token",
		"token_uri":     tokenURI,
	})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "adc.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", path)
}

// tokenHandler answers a refresh-token exchange with a fixed access token.
func tokenHandler(token string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"` + token + `","token_type":"Bearer","expires_in":3600}`))
	}
}

// TestTokenFetchAfterConstructionAndAuthSend is the review item 1 regression:
// credentials resolved during construction must still mint a token AFTER the
// construction function returns (a bounded discovery context would have been
// canceled and every later fetch would fail "context canceled"), and a send
// through the oauth2-wrapped client must carry that token.
func TestTokenFetchAfterConstructionAndAuthSend(t *testing.T) {
	clearProxyEnv(t)
	unsetConfigEnv(t, "HTTPLIB2_CA_CERTS")
	tokenSrv := httptest.NewServer(tokenHandler("test-token"))
	defer tokenSrv.Close()
	writeADCFile(t, tokenSrv.URL)

	var mu sync.Mutex
	var authHeader string
	apiSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		authHeader = r.Header.Get("Authorization")
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer apiSrv.Close()

	cfg, err := tlsConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	apiClient, creds, err := buildAuthorizedClient(cfg, testOptions())
	if err != nil {
		t.Fatalf("buildAuthorizedClient: %v", err)
	}
	if creds.TokenSource == nil {
		t.Fatal("no token source resolved")
	}

	// The token fetch and the authenticated request both happen after
	// construction returned.
	req, err := http.NewRequest(http.MethodGet, apiSrv.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := apiClient.Do(req)
	if err != nil {
		t.Fatalf("authenticated request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	mu.Lock()
	got := authHeader
	mu.Unlock()
	if got != "Bearer test-token" {
		t.Errorf("Authorization = %q, want Bearer test-token (token fetched after construction)", got)
	}
}

// TestTokenFetchUsesProxyTransport is the review item 2 proxy regression:
// oauth2.Transport.Base does not govern token acquisition, so the shared
// oauth2.HTTPClient transport must. With http_proxy set, the refresh-token
// exchange must be sent to the proxy (absolute-form request URI).
func TestTokenFetchUsesProxyTransport(t *testing.T) {
	clearProxyEnv(t)
	unsetConfigEnv(t, "HTTPLIB2_CA_CERTS")
	var mu sync.Mutex
	var seen []string
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.Method+" "+r.URL.String())
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"proxy-token","token_type":"Bearer","expires_in":3600}`))
	}))
	defer proxy.Close()
	t.Setenv("http_proxy", proxy.URL)
	writeADCFile(t, "http://token.example/token")

	cfg, err := tlsConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	_, creds, err := buildAuthorizedClient(cfg, testOptions())
	if err != nil {
		t.Fatalf("buildAuthorizedClient: %v", err)
	}
	tok, err := creds.TokenSource.Token()
	if err != nil {
		t.Fatalf("Token: %v", err)
	}
	if tok.AccessToken != "proxy-token" {
		t.Errorf("access token = %q, want proxy-token (the proxy answered)", tok.AccessToken)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) == 0 {
		t.Fatal("the proxy saw no token request; token acquisition bypassed Httplib2Proxy")
	}
	if seen[0] != "POST http://token.example/token" {
		t.Errorf("proxy request = %q, want POST http://token.example/token", seen[0])
	}
}

// TestStalledTokenFetchIsBounded is the review item 2 stalled-token
// regression: a token endpoint that accepts the connection but never answers
// must not hang the fetch forever — the shared transport's inactivity timeout
// bounds it.
func TestStalledTokenFetchIsBounded(t *testing.T) {
	clearProxyEnv(t)
	unsetConfigEnv(t, "HTTPLIB2_CA_CERTS")
	old := socketTimeout
	socketTimeout = 200 * time.Millisecond
	t.Cleanup(func() { socketTimeout = old })

	release := make(chan struct{})
	stall := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer stall.Close()
	// LIFO: release the handler before waiting for the server to close.
	defer close(release)
	writeADCFile(t, stall.URL)

	cfg, err := tlsConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	_, creds, err := buildAuthorizedClient(cfg, testOptions())
	if err != nil {
		t.Fatalf("buildAuthorizedClient: %v", err)
	}
	start := time.Now()
	if _, err := creds.TokenSource.Token(); err == nil {
		t.Fatal("a stalled token fetch returned no error")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("stalled token fetch took %v, want it bounded by the socket timeout", elapsed)
	}
}

// TestAPIInactivityTimeoutBounds is the review item 3 regression: the API
// transport must bound connect/read/write INACTIVITY (Python's 60 s httplib2
// socket timeout), across a stalled response, a stalled TLS handshake, and a
// slow-but-progressing transfer that legitimately outlasts the timeout.
func TestAPIInactivityTimeoutBounds(t *testing.T) {
	clearProxyEnv(t)
	unsetConfigEnv(t, "HTTPLIB2_CA_CERTS")
	old := socketTimeout
	socketTimeout = 200 * time.Millisecond
	t.Cleanup(func() { socketTimeout = old })
	cfg, err := tlsConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}

	t.Run("stalled_response", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			<-r.Context().Done()
		}))
		defer srv.Close()
		client := newBaseClient(cfg)
		start := time.Now()
		if _, err := client.Get(srv.URL); err == nil {
			t.Fatal("a stalled response returned no error")
		}
		if elapsed := time.Since(start); elapsed > 5*time.Second {
			t.Errorf("stalled response took %v, want it bounded by the socket timeout", elapsed)
		}
	})

	t.Run("stalled_tls_handshake", func(t *testing.T) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		release := make(chan struct{})
		t.Cleanup(func() { close(release); _ = ln.Close() })
		go func() {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			<-release // accept TCP but never speak TLS
			_ = conn.Close()
		}()
		client := newBaseClient(cfg)
		start := time.Now()
		if _, err := client.Get("https://" + ln.Addr().String() + "/"); err == nil {
			t.Fatal("a stalled TLS handshake returned no error")
		}
		if elapsed := time.Since(start); elapsed > 5*time.Second {
			t.Errorf("stalled TLS handshake took %v, want it bounded by the socket timeout", elapsed)
		}
	})

	t.Run("slow_but_progressing", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			flusher := w.(http.Flusher)
			for range 5 {
				_, _ = w.Write([]byte("x"))
				flusher.Flush()
				time.Sleep(120 * time.Millisecond) // each gap < 200ms, total > 200ms
			}
		}))
		defer srv.Close()
		client := newBaseClient(cfg)
		resp, err := client.Get(srv.URL)
		if err != nil {
			t.Fatalf("a slow-but-progressing transfer failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		if string(body) != "xxxxx" {
			t.Errorf("body = %q, want xxxxx", body)
		}
	})
}

// TestHTTPLIB2CACertsTrust is the review item 6 regression for a custom trust
// bundle: with HTTPLIB2_CA_CERTS pointing at the server's CA, the shared
// transport trusts it (and, separately, the token fetch does too).
func TestHTTPLIB2CACertsTrust(t *testing.T) {
	clearProxyEnv(t)
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("trusted"))
	}))
	defer srv.Close()
	caPath := filepath.Join(t.TempDir(), "ca.pem")
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	if err := os.WriteFile(caPath, pemBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HTTPLIB2_CA_CERTS", caPath)

	cfg, err := tlsConfigFromEnv()
	if err != nil {
		t.Fatalf("tlsConfigFromEnv: %v", err)
	}
	client := newBaseClient(cfg)
	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("GET with a custom CA failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "trusted" {
		t.Errorf("body = %q, want trusted", body)
	}
}

// TestTokenFetchHonorsCACerts proves the trust bundle applies to token traffic
// too (the shared oauth2.HTTPClient transport), not only API requests.
func TestTokenFetchHonorsCACerts(t *testing.T) {
	clearProxyEnv(t)
	srv := httptest.NewTLSServer(tokenHandler("ca-token"))
	defer srv.Close()
	caPath := filepath.Join(t.TempDir(), "ca.pem")
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	if err := os.WriteFile(caPath, pemBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HTTPLIB2_CA_CERTS", caPath)
	writeADCFile(t, srv.URL)

	cfg, err := tlsConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	_, creds, err := buildAuthorizedClient(cfg, testOptions())
	if err != nil {
		t.Fatalf("buildAuthorizedClient: %v", err)
	}
	tok, err := creds.TokenSource.Token()
	if err != nil {
		t.Fatalf("token fetch ignored HTTPLIB2_CA_CERTS: %v", err)
	}
	if tok.AccessToken != "ca-token" {
		t.Errorf("access token = %q, want ca-token", tok.AccessToken)
	}
}

// TestHTTPLIB2CACertsInvalid pins the validation: a missing file and a
// directory are both rejected (httplib2 raises at import when the path is not
// a file).
func TestHTTPLIB2CACertsInvalid(t *testing.T) {
	t.Setenv("HTTPLIB2_CA_CERTS", filepath.Join(t.TempDir(), "missing.pem"))
	if _, err := tlsConfigFromEnv(); err == nil {
		t.Error("a missing HTTPLIB2_CA_CERTS path was accepted")
	}
	t.Setenv("HTTPLIB2_CA_CERTS", t.TempDir())
	if _, err := tlsConfigFromEnv(); err == nil {
		t.Error("a directory HTTPLIB2_CA_CERTS path was accepted")
	}
}

// TestReusedConnectionGetsFreshDeadline is the review item 1 regression. The
// fixed-dial IdleTimeoutDialer arms a read deadline when net/http's background
// Peek on an IDLE pooled connection begins, so a request reusing that
// connection carries the remaining time rather than a fresh socket-operation
// timeout. Here the connection idles for 200ms of a 300ms timeout and the
// second response takes another 250ms (~450ms from the idle Peek), which would
// trip a dial-baked deadline; the phase-aware transport clears the deadline at
// the request boundary and arms it only for the response read, so the healthy
// send succeeds.
func TestReusedConnectionGetsFreshDeadline(t *testing.T) {
	clearProxyEnv(t)
	unsetConfigEnv(t, "HTTPLIB2_CA_CERTS")
	old := socketTimeout
	socketTimeout = 300 * time.Millisecond
	t.Cleanup(func() { socketTimeout = old })

	var (
		mu     sync.Mutex
		count  int
		remote []string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		count++
		n := count
		remote = append(remote, r.RemoteAddr)
		mu.Unlock()
		if n == 2 {
			time.Sleep(250 * time.Millisecond)
		}
		_, _ = fmt.Fprintf(w, "resp-%d", n)
	}))
	defer srv.Close()

	cfg, err := tlsConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	client := newBaseClient(cfg)

	get := func() string {
		t.Helper()
		resp, err := client.Get(srv.URL)
		if err != nil {
			t.Fatalf("GET: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		return string(body)
	}
	if got := get(); got != "resp-1" {
		t.Fatalf("first body = %q", got)
	}
	// Let the connection sit idle, then issue a second request whose response
	// takes long enough that idle+response exceeds the timeout.
	time.Sleep(200 * time.Millisecond)
	if got := get(); got != "resp-2" {
		t.Fatalf("second body = %q, want resp-2 (a reused connection must get a fresh deadline)", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(remote) != 2 {
		t.Fatalf("server saw %d requests, want 2", len(remote))
	}
	if remote[0] != remote[1] {
		t.Fatalf("connection was NOT reused (%q vs %q); the regression did not exercise the idle-connection path", remote[0], remote[1])
	}
}

// TestHTTPLIB2CACertsEmptyRejected is the review item 3 regression: httplib2
// distinguishes UNSET from EMPTY and rejects an empty path (it is not a file),
// so an explicitly empty HTTPLIB2_CA_CERTS must error rather than silently
// selecting the system roots.
func TestHTTPLIB2CACertsEmptyRejected(t *testing.T) {
	t.Setenv("HTTPLIB2_CA_CERTS", "")
	if _, err := tlsConfigFromEnv(); err == nil {
		t.Error("an empty HTTPLIB2_CA_CERTS was accepted; want an error")
	}
}

// TestNewEmptyServiceAccountSelectsADC is the review item 5 construction
// regression: an empty object ({}), a null and an omitted
// google_service_account_credentials all fall through to application default
// credentials, and the CONFIGURED project id is preserved.
func TestNewEmptyServiceAccountSelectsADC(t *testing.T) {
	clearProxyEnv(t)
	unsetConfigEnv(t, "HTTPLIB2_CA_CERTS")
	writeADCFile(t, "http://token.invalid/token")

	cases := []struct {
		name  string
		creds any
		omit  bool
	}{
		{"empty_object", map[string]any{}, false},
		{"null", nil, false},
		{"omitted", nil, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			config := map[string]any{
				"google_cloud_logging_project_id": "config-project",
				"google_cloud_logging_log_id":     "log-id",
			}
			if !tc.omit {
				config["google_service_account_credentials"] = tc.creds
			}
			inst, err := New(sender.ConstructorArgs{Name: "gcl", Config: config})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			s, ok := inst.Transport.(*Sender)
			if !ok {
				t.Fatalf("transport = %T", inst.Transport)
			}
			if s.projectID != "config-project" {
				t.Errorf("projectID = %q, want the configured config-project (ADC has no service-account project id)", s.projectID)
			}
		})
	}
}
