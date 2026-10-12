// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package gcl

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
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
