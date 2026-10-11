// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package elasticsearch

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Aiven-Open/journalpump/internal/httpx"
	"github.com/Aiven-Open/journalpump/internal/journaldlog"
	"github.com/Aiven-Open/journalpump/internal/sender"
)

// _JSON_MESSAGE from test/test_elasticsearch_opensearch_sender.py.
const jsonMessage = `{"field1": 42, "field2": true, "field3": 42.0, "field4": "aaa bbb ccc", "field5": null, "field6": {"a": 1, "b": 2.0, "c": true, "d": "dddd", "e": null}, "field7": [1, 2]}`

func parsedJSONMessage(t *testing.T) map[string]any {
	t.Helper()
	message, err := parseMessage([]byte(jsonMessage))
	if err != nil {
		t.Fatalf("parseMessage: %v", err)
	}
	return message
}

func wantProperties() map[string]any {
	return map[string]any{
		"SYSTEMD_SESSION": map[string]any{"type": "text"},
		"SESSION_ID":      map[string]any{"type": "text"},
		"field1":          map[string]any{"type": "integer"},
		"field2":          map[string]any{"type": "boolean"},
		"field3":          map[string]any{"type": "float"},
		"field4":          map[string]any{"type": "text"},
		"field6": map[string]any{
			"properties": map[string]any{
				"a": map[string]any{"type": "integer"},
				"b": map[string]any{"type": "float"},
				"c": map[string]any{"type": "boolean"},
				"d": map[string]any{"type": "text"},
			},
		},
	}
}

// --- capture server -------------------------------------------------------

type capturedRequest struct {
	Method string
	Path   string
	Header http.Header
	Body   []byte
}

type captureServer struct {
	*httptest.Server
	mu           sync.Mutex
	reqs         []capturedRequest
	version      string
	aliasJSON    string
	putStatus    int
	putBody      string
	bulkStatus   int
	bulkBody     string
	deleteStatus int
}

func newCaptureServer(t *testing.T) *captureServer {
	t.Helper()
	c := &captureServer{
		version:      "9.0.0",
		aliasJSON:    "{}",
		putStatus:    http.StatusOK,
		bulkStatus:   http.StatusOK,
		deleteStatus: http.StatusOK,
	}
	c.Server = httptest.NewServer(http.HandlerFunc(c.handle))
	t.Cleanup(c.Close)
	return c
}

func (c *captureServer) handle(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	_ = r.Body.Close()
	c.mu.Lock()
	c.reqs = append(c.reqs, capturedRequest{Method: r.Method, Path: r.URL.RequestURI(), Header: r.Header.Clone(), Body: body})
	c.mu.Unlock()

	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/":
		w.Header().Set("content-type", "application/json")
		_, _ = io.WriteString(w, `{"version": {"number": "`+c.version+`"}}`)
	case r.Method == http.MethodGet && r.URL.Path == "/_aliases":
		w.Header().Set("content-type", "application/json")
		_, _ = io.WriteString(w, c.aliasJSON)
	case r.Method == http.MethodPut:
		w.WriteHeader(c.putStatus)
		if c.putBody != "" {
			_, _ = io.WriteString(w, c.putBody)
		}
	case r.Method == http.MethodPost:
		w.WriteHeader(c.bulkStatus)
		if c.bulkBody != "" {
			_, _ = io.WriteString(w, c.bulkBody)
		}
	case r.Method == http.MethodDelete:
		w.WriteHeader(c.deleteStatus)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (c *captureServer) requests() []capturedRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]capturedRequest(nil), c.reqs...)
}

func (c *captureServer) requestsWith(method, path string) []capturedRequest {
	var out []capturedRequest
	for _, r := range c.requests() {
		if r.Method == method && r.Path == path {
			out = append(out, r)
		}
	}
	return out
}

// --- recording logger -----------------------------------------------------

type recordingLogger struct {
	mu    sync.Mutex
	lines []string
}

func (l *recordingLogger) Printf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func (l *recordingLogger) has(substr string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, line := range l.lines {
		if strings.Contains(line, substr) {
			return true
		}
	}
	return false
}

func (l *recordingLogger) count(substr string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, line := range l.lines {
		if strings.Contains(line, substr) {
			n++
		}
	}
	return n
}

// --- helpers --------------------------------------------------------------

func newTestSender(t *testing.T, st senderType, url string, log sender.Logger) *Sender {
	t.Helper()
	var (
		inst sender.Instance
		err  error
	)
	cfg := map[string]any{string(st) + "_url": url}
	switch st {
	case elasticsearch:
		inst, err = NewElasticsearch(sender.ConstructorArgs{Config: cfg, Log: log})
	case opensearch:
		inst, err = NewOpenSearch(sender.ConstructorArgs{Config: cfg, Log: log})
	}
	if err != nil {
		t.Fatalf("newSender: %v", err)
	}
	return inst.Transport.(*Sender)
}

// --- config ---------------------------------------------------------------

func TestConfigDefaults(t *testing.T) {
	for _, st := range []senderType{elasticsearch, opensearch} {
		cfg, err := configFrom(map[string]any{string(st) + "_url": "http://aaa"}, st)
		if err != nil {
			t.Fatalf("%s: %v", st, err)
		}
		if cfg.senderType != st {
			t.Errorf("%s: senderType = %q", st, cfg.senderType)
		}
		if cfg.sessionURL != "http://aaa" {
			t.Errorf("%s: sessionURL = %q", st, cfg.sessionURL)
		}
		if cfg.requestTimeout != defaultRequestTimeout {
			t.Errorf("%s: requestTimeout = %v", st, cfg.requestTimeout)
		}
		if cfg.indexLifetimeInDays != defaultIndexLifetimeInDays {
			t.Errorf("%s: indexLifetimeInDays = %d", st, cfg.indexLifetimeInDays)
		}
		if cfg.indexName != defaultIndexPrefix {
			t.Errorf("%s: indexName = %q", st, cfg.indexName)
		}
	}
}

func TestConfigCustom(t *testing.T) {
	for _, st := range []senderType{elasticsearch, opensearch} {
		cfg, err := configFrom(map[string]any{
			string(st) + "_url":            "http://bbb/",
			string(st) + "_index_prefix":   "some_idx_prefix",
			string(st) + "_timeout":        1000,
			string(st) + "_index_days_max": 42,
		}, st)
		if err != nil {
			t.Fatalf("%s: %v", st, err)
		}
		if cfg.sessionURL != "http://bbb" {
			t.Errorf("%s: sessionURL = %q", st, cfg.sessionURL)
		}
		if cfg.requestTimeout != 1000*time.Second {
			t.Errorf("%s: requestTimeout = %v", st, cfg.requestTimeout)
		}
		if cfg.indexName != "some_idx_prefix" {
			t.Errorf("%s: indexName = %q", st, cfg.indexName)
		}
		if cfg.indexLifetimeInDays != 42 {
			t.Errorf("%s: indexLifetimeInDays = %d", st, cfg.indexLifetimeInDays)
		}
	}
}

func TestRequestURL(t *testing.T) {
	cfg, err := configFrom(map[string]any{"elasticsearch_url": "http://aaa"}, elasticsearch)
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.requestURL("b/c/d"); got != "http://aaa/b/c/d" {
		t.Errorf("requestURL(b/c/d) = %q", got)
	}
	if got := cfg.requestURL("b/c?d"); got != "http://aaa/b/c?d" {
		t.Errorf("requestURL(b/c?d) = %q", got)
	}
}

func TestConfigRaisesForEmptyURL(t *testing.T) {
	for _, st := range []senderType{elasticsearch, opensearch} {
		_, err := configFrom(map[string]any{}, st)
		if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("%s_url hasn't been defined", st)) {
			t.Errorf("%s: err = %v", st, err)
		}
	}
}

func TestSenderMaxSendInterval(t *testing.T) {
	for _, st := range []senderType{elasticsearch, opensearch} {
		args := sender.ConstructorArgs{Config: map[string]any{string(st) + "_url": "http://aaa"}}
		var (
			inst sender.Instance
			err  error
		)
		if st == elasticsearch {
			inst, err = NewElasticsearch(args)
		} else {
			inst, err = NewOpenSearch(args)
		}
		if err != nil {
			t.Fatalf("%s: %v", st, err)
		}
		if inst.MaxSendInterval != defaultMaxSendInterval {
			t.Errorf("%s: default interval = %v", st, inst.MaxSendInterval)
		}
		wantType := "OpenSearchSender"
		if st == elasticsearch {
			wantType = "ElasticsearchSender"
		}
		if inst.Type != wantType {
			t.Errorf("%s: type = %q", st, inst.Type)
		}
	}
	inst, err := NewOpenSearch(sender.ConstructorArgs{Config: map[string]any{
		"opensearch_url":    "http://aaa",
		"max_send_interval": 42,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if inst.MaxSendInterval != 42*time.Second {
		t.Errorf("custom interval = %v", inst.MaxSendInterval)
	}
}

// --- header / mapping -----------------------------------------------------

func TestMessageHeaderWithoutType(t *testing.T) {
	cases := []struct {
		st      senderType
		version version
	}{
		{opensearch, version{major: 1, minor: 2, patch: 4}},
		{elasticsearch, version{major: 8, minor: 0, patch: 0}},
	}
	for _, tc := range cases {
		s := &Sender{cfg: config{senderType: tc.st}, log: discardLogger{}, version: &tc.version}
		header, err := s.messageHeader("some_index")
		if err != nil {
			t.Fatal(err)
		}
		want := `{"index": {"_index": "some_index"}}`
		if string(header) != want {
			t.Errorf("%s: header = %s, want %s", tc.st, header, want)
		}
	}
}

func TestMessageHeaderWithType(t *testing.T) {
	v := version{major: 7, minor: 10, patch: 2}
	s := &Sender{cfg: config{senderType: elasticsearch}, log: discardLogger{}, version: &v}
	header, err := s.messageHeader("some_index")
	if err != nil {
		t.Fatal(err)
	}
	want := `{"index": {"_index": "some_index", "_type": "journal_msg"}}`
	if string(header) != want {
		t.Errorf("header = %s, want %s", header, want)
	}
}

func TestCreateMappingWithoutType(t *testing.T) {
	cases := []struct {
		st      senderType
		version version
	}{
		{opensearch, version{major: 1, minor: 2, patch: 4}},
		{elasticsearch, version{major: 8, minor: 0, patch: 0}},
	}
	for _, tc := range cases {
		s := &Sender{cfg: config{senderType: tc.st}, log: discardLogger{}, version: &tc.version}
		got := s.createMapping(parsedJSONMessage(t))
		want := map[string]any{"mappings": map[string]any{"properties": wantProperties()}}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s: mapping = %#v, want %#v", tc.st, got, want)
		}
	}
}

func TestCreateMappingWithType(t *testing.T) {
	v := version{major: 7, minor: 10, patch: 2}
	s := &Sender{cfg: config{senderType: elasticsearch}, log: discardLogger{}, version: &v}
	got := s.createMapping(parsedJSONMessage(t))
	want := map[string]any{"mappings": map[string]any{"journal_msg": map[string]any{"properties": wantProperties()}}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("mapping = %#v, want %#v", got, want)
	}
}

func TestIndexURLForElasticsearch(t *testing.T) {
	s := &Sender{cfg: config{senderType: elasticsearch, sessionURL: "http://aaa"}, log: discardLogger{}}
	for _, tc := range []struct {
		v        version
		expected string
	}{
		{version{major: 7, minor: 10, patch: 2}, "http://aaa/some_index?include_type_name=true"},
		{version{major: 1, minor: 3, patch: 3}, "http://aaa/some_index?include_type_name=true"},
		{version{major: 8, minor: 0, patch: 0}, "http://aaa/some_index"},
	} {
		v := tc.v
		s.version = &v
		got, err := s.indexURL("some_index")
		if err != nil {
			t.Fatal(err)
		}
		if got != tc.expected {
			t.Errorf("v%d: indexURL = %q, want %q", tc.v.major, got, tc.expected)
		}
	}
}

func TestIndexURLForOpenSearch(t *testing.T) {
	s := &Sender{cfg: config{senderType: opensearch, sessionURL: "http://aaa"}, log: discardLogger{}, version: &version{major: 1, minor: 3, patch: 0}}
	got, err := s.indexURL("some_index")
	if err != nil {
		t.Fatal(err)
	}
	if got != "http://aaa/some_index" {
		t.Errorf("indexURL = %q", got)
	}
}

// --- send path ------------------------------------------------------------

func TestSendMessagesUsesCurrentDateWhenTimestampMissing(t *testing.T) {
	for _, st := range []senderType{opensearch, elasticsearch} {
		srv := newCaptureServer(t)
		log := &recordingLogger{}
		s := newTestSender(t, st, srv.URL, log)
		// The probed version is set here because a pre-populated index set
		// short-circuits loadIndices (Python's _load_indices early return).
		s.version = &version{major: 8, minor: 0, patch: 0}
		today := time.Now().UTC().Format("2006-01-02")
		s.indices[defaultIndexPrefix+"-"+today] = struct{}{}

		msg := []byte(`{"MESSAGE": "audit log line", "PRIORITY": "6"}`)
		if err := s.SendMessages(context.Background(), [][]byte{msg}, "c1"); err != nil {
			t.Fatalf("%s: %v", st, err)
		}
		posts := srv.requestsWith(http.MethodPost, "/"+defaultIndexPrefix+"-"+today+"/_bulk")
		if len(posts) != 1 {
			t.Fatalf("%s: posts = %d", st, len(posts))
		}
		if !log.has("timestamp") {
			t.Errorf("%s: expected a missing-timestamp warning, got %v", st, log.lines)
		}
	}
}

func TestSendMessagesUsesTimestampForIndexName(t *testing.T) {
	for _, st := range []senderType{opensearch, elasticsearch} {
		srv := newCaptureServer(t)
		log := &recordingLogger{}
		s := newTestSender(t, st, srv.URL, log)
		s.version = &version{major: 8, minor: 0, patch: 0}
		expected := defaultIndexPrefix + "-2025-06-26"
		s.indices[expected] = struct{}{}

		msg := []byte(`{"MESSAGE": "log line", "timestamp": "2025-06-26T14:52:33.581000", "PRIORITY": "6"}`)
		if err := s.SendMessages(context.Background(), [][]byte{msg}, "c1"); err != nil {
			t.Fatalf("%s: %v", st, err)
		}
		if len(srv.requestsWith(http.MethodPost, "/"+expected+"/_bulk")) != 1 {
			t.Fatalf("%s: bulk POST missing", st)
		}
		if log.has("timestamp") {
			t.Errorf("%s: unexpected warning: %v", st, log.lines)
		}
	}
}

func TestBulkBodyAndHeaders(t *testing.T) {
	srv := newCaptureServer(t)
	s := newTestSender(t, opensearch, srv.URL, discardLogger{})
	today := time.Now().UTC().Format("2006-01-02")
	idx := defaultIndexPrefix + "-" + today
	s.indices[idx] = struct{}{}

	msg := []byte(`{"MESSAGE": "hi", "timestamp": "` + today + `T00:00:00"}`)
	if err := s.SendMessages(context.Background(), [][]byte{msg}, "c1"); err != nil {
		t.Fatal(err)
	}
	posts := srv.requestsWith(http.MethodPost, "/"+idx+"/_bulk")
	if len(posts) != 1 {
		t.Fatalf("posts = %d", len(posts))
	}
	wantBody := `{"index": {"_index": "` + idx + `"}}` + "\n" + string(msg) + "\n"
	if string(posts[0].Body) != wantBody {
		t.Errorf("bulk body = %q, want %q", posts[0].Body, wantBody)
	}
	if got := posts[0].Header.Get("content-type"); got != "application/x-ndjson" {
		t.Errorf("content-type = %q", got)
	}
	if got := posts[0].Header.Get("content-length"); got != fmt.Sprint(len(wantBody)) {
		t.Errorf("content-length = %q, want %d", got, len(wantBody))
	}
}

// TestBulkStatusCodeIgnored quirk-locks Python: the bulk response status
// is never enforced.
func TestBulkStatusCodeIgnored(t *testing.T) {
	srv := newCaptureServer(t)
	srv.bulkStatus = http.StatusInternalServerError
	log := &recordingLogger{}
	s := newTestSender(t, opensearch, srv.URL, log)
	today := time.Now().UTC().Format("2006-01-02")
	s.indices[defaultIndexPrefix+"-"+today] = struct{}{}

	msg := []byte(`{"MESSAGE": "hi", "timestamp": "` + today + `T00:00:00"}`)
	if err := s.SendMessages(context.Background(), [][]byte{msg}, "c1"); err != nil {
		t.Fatalf("bulk 500 should still succeed, got %v", err)
	}
	// Python logs the informational result and its success flag from the
	// status, without enforcing it (item 9).
	if !log.has("Sent 1 log events successfully: false") {
		t.Errorf("missing informational bulk result: %v", log.lines)
	}
}

func TestCreateIndexAlreadyExistsTolerated(t *testing.T) {
	srv := newCaptureServer(t)
	srv.aliasJSON = "{}"
	srv.putStatus = http.StatusBadRequest
	srv.putBody = `{"error":{"type":"resource_already_exists_exception"},"status":400}`
	s := newTestSender(t, opensearch, srv.URL, discardLogger{})

	msg := []byte(`{"MESSAGE": "hi", "timestamp": "2019-10-07 14:00:00"}`)
	if err := s.SendMessages(context.Background(), [][]byte{msg}, "c1"); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.indices[defaultIndexPrefix+"-2019-10-07"]; !ok {
		t.Errorf("already-exists index not cached: %v", s.indices)
	}
	if len(srv.requestsWith(http.MethodPut, "/"+defaultIndexPrefix+"-2019-10-07")) != 1 {
		t.Errorf("PUT missing")
	}
}

func TestCreateIndexUsesMappingAndTypeQuirk(t *testing.T) {
	srv := newCaptureServer(t)
	srv.version = "7.10.2"
	srv.aliasJSON = "{}"
	s := newTestSender(t, elasticsearch, srv.URL, discardLogger{})

	msg := []byte(`{"MESSAGE": "hi", "FIELD": 1, "timestamp": "2019-10-07 14:00:00"}`)
	if err := s.SendMessages(context.Background(), [][]byte{msg}, "c1"); err != nil {
		t.Fatal(err)
	}
	puts := srv.requestsWith(http.MethodPut, "/"+defaultIndexPrefix+"-2019-10-07?include_type_name=true")
	if len(puts) != 1 {
		t.Fatalf("PUT with include_type_name missing: %+v", srv.requests())
	}
	var mapping map[string]any
	if err := json.Unmarshal(puts[0].Body, &mapping); err != nil {
		t.Fatal(err)
	}
	journalMsg, ok := mapping["mappings"].(map[string]any)["journal_msg"].(map[string]any)
	if !ok {
		t.Fatalf("legacy type wrapper missing: %v", mapping)
	}
	if _, ok := journalMsg["properties"].(map[string]any)["FIELD"]; !ok {
		t.Errorf("FIELD missing from mapping: %v", mapping)
	}
}

func TestCreateIndexFailureLoggedButContinues(t *testing.T) {
	srv := newCaptureServer(t)
	srv.aliasJSON = "{}"
	srv.putStatus = http.StatusBadRequest
	srv.putBody = `{"error":"boom"}`
	log := &recordingLogger{}
	s := newTestSender(t, opensearch, srv.URL, log)

	msg := []byte(`{"MESSAGE": "hi", "timestamp": "2019-10-07 14:00:00"}`)
	if err := s.SendMessages(context.Background(), [][]byte{msg}, "c1"); err != nil {
		t.Fatalf("create failure must not fail the batch: %v", err)
	}
	if _, ok := s.indices[defaultIndexPrefix+"-2019-10-07"]; ok {
		t.Errorf("failed index should not be cached")
	}
	if !log.has("Could not create index mappings") {
		t.Errorf("missing warning: %v", log.lines)
	}
	if len(srv.requestsWith(http.MethodPost, "/"+defaultIndexPrefix+"-2019-10-07/_bulk")) != 1 {
		t.Errorf("bulk POST should still happen")
	}
}

// --- load / dedup ---------------------------------------------------------

type timeoutErr struct{}

func (timeoutErr) Error() string { return "timeout" }
func (timeoutErr) Timeout() bool { return true }

func TestLoadErrorDedupLogging(t *testing.T) {
	log := &recordingLogger{}
	s := &Sender{cfg: config{senderType: opensearch}, log: log}

	_ = s.logLoadError(timeoutErr{})
	_ = s.logLoadError(timeoutErr{})
	if got := log.count("Connection error to opensearch"); got != 1 {
		t.Errorf("same-class errors logged %d times, want 1", got)
	}
	_ = s.logLoadError(errors.New("refused"))
	if got := log.count("Connection error to opensearch"); got != 2 {
		t.Errorf("different-class error not logged: %d", got)
	}
}

func TestLoadIndicesVersionProbeOnce(t *testing.T) {
	srv := newCaptureServer(t)
	srv.aliasJSON = `{"journalpump-2021-12-27": {}}`
	s := newTestSender(t, opensearch, srv.URL, discardLogger{})

	if err := s.loadIndices(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.loadIndices(context.Background()); err != nil {
		t.Fatal(err)
	}
	// The version probe (GET /) runs once; the alias census (GET /_aliases)
	// also only runs on the cold path.
	if got := len(srv.requestsWith(http.MethodGet, "/")); got != 1 {
		t.Errorf("version probes = %d, want 1", got)
	}
	if _, ok := s.indices["journalpump-2021-12-27"]; !ok {
		t.Errorf("aliases not cached: %v", s.indices)
	}
}

// --- retention ------------------------------------------------------------

func TestMaintenanceDeletesOldestIndices(t *testing.T) {
	srv := newCaptureServer(t)
	srv.aliasJSON = `{"journalpump-2021-01-01": {}, "journalpump-2021-01-02": {}, "journalpump-2021-01-03": {}, "journalpump-2021-01-04": {}, "journalpump-2021-01-05": {}, "other-2021-01-01": {}}`
	s := newTestSender(t, opensearch, srv.URL, discardLogger{})

	if err := s.MaintenanceOperations(context.Background()); err != nil {
		t.Fatal(err)
	}
	oldest := []string{"journalpump-2021-01-01", "journalpump-2021-01-02"}
	for _, idx := range oldest {
		if len(srv.requestsWith(http.MethodDelete, "/"+idx)) != 1 {
			t.Errorf("expected DELETE %s", idx)
		}
	}
	if len(srv.requestsWith(http.MethodDelete, "/journalpump-2021-01-03")) != 0 {
		t.Errorf("third index should be kept")
	}
	if len(srv.requestsWith(http.MethodDelete, "/other-2021-01-01")) != 0 {
		t.Errorf("non-prefixed index must not be deleted")
	}

	// A second call within the hour is a no-op.
	before := len(srv.requests())
	if err := s.MaintenanceOperations(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := len(srv.requests()); got != before {
		t.Errorf("second maintenance call issued %d extra requests", got-before)
	}
}

// --- HTTP client wiring ---------------------------------------------------

// TestHTTPClientUsesRequestsProxySemantics pins that the ES sender's client
// selects proxies with the requests-compatible strategy (its Python client
// is journalpump.util.get_requests_session → requests), not Go's
// http.ProxyFromEnvironment.
func TestHTTPClientUsesRequestsProxySemantics(t *testing.T) {
	client, err := newHTTPClient(nil)
	if err != nil {
		t.Fatal(err)
	}
	tr, ok := client.Client().Transport.(*http.Transport)
	if !ok || tr.Proxy == nil {
		t.Fatalf("transport.Proxy not installed: %T", client.Client().Transport)
	}
	t.Setenv("http_proxy", "http://proxy.example:8080")
	t.Setenv("no_proxy", "")
	t.Setenv("NO_PROXY", "")

	req, _ := http.NewRequest(http.MethodGet, "http://target.example/", nil)
	u, err := tr.Proxy(req)
	if err != nil {
		t.Fatal(err)
	}
	if u == nil || u.String() != "http://proxy.example:8080" {
		t.Fatalf("proxy = %v", u)
	}

	t.Setenv("no_proxy", "target.example")
	if u, err := tr.Proxy(req); err != nil || u != nil {
		t.Fatalf("no_proxy target not bypassed: %v, %v", u, err)
	}
}

// --- fixture parity -------------------------------------------------------

type esFixture struct {
	Capture struct {
		Runs []struct {
			Index string `json:"index"`
		} `json:"runs"`
	} `json:"capture"`
	Indices map[string]struct {
		Mapping  map[string]any `json:"mapping"`
		Payloads []string       `json:"payloads"`
	} `json:"indices"`
}

func TestFixtureStructuralParity(t *testing.T) {
	path := filepath.Join("..", "..", "..", "systest-go", "fixtures", "elasticsearch.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("fixture: %v", err)
	}
	var fixture esFixture
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	if len(fixture.Indices) == 0 {
		t.Fatal("fixture has no indices")
	}
	for indexName, index := range fixture.Indices {
		srv := newCaptureServer(t)
		srv.version = "9.0.0"
		srv.aliasJSON = "{}"
		s := newTestSender(t, opensearch, srv.URL, discardLogger{})

		batch := make([][]byte, len(index.Payloads))
		for i, p := range index.Payloads {
			batch[i] = []byte(p)
		}
		if err := s.SendMessages(context.Background(), batch, "c1"); err != nil {
			t.Fatalf("%s: %v", indexName, err)
		}

		puts := srv.requestsWith(http.MethodPut, "/"+indexName)
		if len(puts) != 1 {
			t.Fatalf("%s: PUTs = %d", indexName, len(puts))
		}
		var mapping map[string]any
		if err := json.Unmarshal(puts[0].Body, &mapping); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(mapping, index.Mapping) {
			t.Errorf("%s: mapping mismatch\n got: %s\nwant: %v", indexName, puts[0].Body, index.Mapping)
		}

		posts := srv.requestsWith(http.MethodPost, "/"+indexName+"/_bulk")
		if len(posts) != 1 {
			t.Fatalf("%s: bulk POSTs = %d", indexName, len(posts))
		}
		lines := strings.Split(string(posts[0].Body), "\n")
		// Trailing newline yields one empty element.
		if last := lines[len(lines)-1]; last != "" {
			t.Errorf("%s: body does not end in a newline", indexName)
		}
		lines = lines[:len(lines)-1]
		if len(lines) != 2*len(index.Payloads) {
			t.Fatalf("%s: %d NDJSON lines, want %d", indexName, len(lines), 2*len(index.Payloads))
		}
		for i, want := range index.Payloads {
			header := lines[2*i]
			wantHeader := `{"index": {"_index": "` + indexName + `"}}`
			if header != wantHeader {
				t.Errorf("%s[%d]: header = %s, want %s", indexName, i, header, wantHeader)
			}
			if got := lines[2*i+1]; got != want {
				t.Errorf("%s[%d]: payload = %s, want %s", indexName, i, got, want)
			}
		}
	}
}

// --- response timeout parity (review items 1) -------------------------------

// TestZeroAndNegativeTimeoutRejected pins that ES/OpenSearch REJECT a
// non-positive timeout at load. Unlike logplex (a real 60 s session default),
// get_requests_session(timeout=X) installs a TimeoutAdapter whose fallback IS
// X, and requests rejects a non-positive connect timeout — so zero must NOT
// silently become 60 s. Both the config loader and the constructors reject it.
func TestZeroAndNegativeTimeoutRejected(t *testing.T) {
	for _, st := range []senderType{elasticsearch, opensearch} {
		for _, bad := range []any{0, -1, -1.5} {
			_, err := configFrom(map[string]any{
				string(st) + "_url":     "http://aaa",
				string(st) + "_timeout": bad,
			}, st)
			if err == nil || !strings.Contains(err.Error(), "less than or equal to 0") {
				t.Errorf("%s: timeout %v err = %v, want a load-time rejection", st, bad, err)
			}

			args := sender.ConstructorArgs{Config: map[string]any{
				string(st) + "_url":     "http://aaa",
				string(st) + "_timeout": bad,
			}}
			var cerr error
			if st == elasticsearch {
				_, cerr = NewElasticsearch(args)
			} else {
				_, cerr = NewOpenSearch(args)
			}
			if cerr == nil {
				t.Errorf("%s: constructor accepted timeout %v", st, bad)
			}
		}

		// A positive value is still accepted and kept.
		ok, err := configFrom(map[string]any{
			string(st) + "_url":     "http://aaa",
			string(st) + "_timeout": 0.5,
		}, st)
		if err != nil || ok.requestTimeout != 500*time.Millisecond {
			t.Errorf("%s: positive timeout = %v, %v", st, ok.requestTimeout, err)
		}
	}
}

// --- maintenance exhaustion (review item 2) --------------------------------

func TestMaintenanceNegativeDaysMaxReturnsErrorNotPanic(t *testing.T) {
	srv := newCaptureServer(t)
	srv.aliasJSON = `{"journalpump-2021-01-01": {}, "journalpump-2021-01-02": {}}`
	s := newTestSender(t, opensearch, srv.URL, discardLogger{})
	s.cfg.indexLifetimeInDays = -1

	if err := s.MaintenanceOperations(context.Background()); err == nil {
		t.Fatal("exhausted index list with negative index_days_max must return an error, not panic")
	}
	// Python deletes every matching index before the IndexError.
	for _, idx := range []string{"journalpump-2021-01-01", "journalpump-2021-01-02"} {
		if len(srv.requestsWith(http.MethodDelete, "/"+idx)) != 1 {
			t.Errorf("expected DELETE %s", idx)
		}
	}
}

// TestLoopContinuesAfterNegativeDaysMaxMaintenance drives the sender through
// the REAL sender.Loop: the exhausted-list maintenance error is caught by the
// loop, so a subsequent batch still sends (Python's IndexError is swallowed by
// the maintenance caller).
func TestLoopContinuesAfterNegativeDaysMaxMaintenance(t *testing.T) {
	srv := newCaptureServer(t)
	srv.aliasJSON = `{"journalpump-2021-01-01": {}}`
	inst, err := NewOpenSearch(sender.ConstructorArgs{Config: map[string]any{
		"opensearch_url":            srv.URL,
		"opensearch_index_days_max": -1,
		"max_send_interval":         0.05,
	}})
	if err != nil {
		t.Fatal(err)
	}

	in := make(chan sender.Item, 4)
	loop := sender.NewLoop(sender.LoopConfig{Name: "es-loop"}, inst, in)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = loop.Run(ctx)
		close(done)
	}()
	today := time.Now().UTC().Format("2006-01-02")
	msg := []byte(`{"MESSAGE": "hi", "timestamp": "` + today + `T00:00:00"}`)
	in <- sender.Item{Payload: msg, Cursor: "c0"}

	deadline := time.Now().Add(3 * time.Second)
	for loop.State().Sent.Count < 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	st := loop.State()
	cancel()
	<-done
	if st.Sent.Count != 1 {
		t.Fatalf("sent count = %d, want 1 (the maintenance error must not stop sending)", st.Sent.Count)
	}
}

// --- CA resolution (review items 3 and 4) ----------------------------------

func writeCertPEM(t *testing.T, dir, name string, cert *x509.Certificate) string {
	t.Helper()
	path := filepath.Join(dir, name)
	pem := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw})
	if err := os.WriteFile(path, pem, 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	return path
}

func clientGets(t *testing.T, client *httpx.InactivityClient, url string) error {
	t.Helper()
	resp, err := client.Client().Get(url)
	if err != nil {
		return err
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return nil
}

// newTLSServerWithCert starts a TLS test server using a freshly generated
// self-signed certificate (httptest's default cert is a shared hardcoded one,
// so two httptest servers would present identical certs) and returns the
// server and its leaf certificate.
func newTLSServerWithCert(t *testing.T) (*httptest.Server, *x509.Certificate) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: "localhost"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
		DNSNames:              []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse certificate: %v", err)
	}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}}}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv, leaf
}

// TestHTTPClientCAResolution covers requests' merge_environment_settings +
// merge_setting precedence (the env bundle WINS over session.verify) and the
// fail-closed refusal of a CA DIRECTORY (user decision 2026-10-12).
func TestHTTPClientCAResolution(t *testing.T) {
	srv, cert := newTLSServerWithCert(t)
	dir := t.TempDir()
	certFile := writeCertPEM(t, dir, "ca.pem", cert)

	// With no environment override, these verify against the TLS server.
	for _, tc := range []struct {
		name string
		ca   any
		env  map[string]string
	}{
		{"ca-file", certFile, nil},
		{"requests-ca-bundle-env", true, map[string]string{"REQUESTS_CA_BUNDLE": certFile}},
		{"curl-ca-bundle-env", true, map[string]string{"CURL_CA_BUNDLE": certFile}},
		// The env bundle WINS over session.verify, even when ca is false, "" or
		// an explicit (wrong) path — requests consults the env because the
		// per-request verify is None, then merge_setting keeps the env string.
		{"env-overrides-false", false, map[string]string{"REQUESTS_CA_BUNDLE": certFile}},
		{"env-overrides-empty", "", map[string]string{"REQUESTS_CA_BUNDLE": certFile}},
		{"env-overrides-explicit-path", filepath.Join(dir, "nope.pem"), map[string]string{"REQUESTS_CA_BUNDLE": certFile}},
		{"requests-bundle-wins-over-curl", true, map[string]string{
			"REQUESTS_CA_BUNDLE": certFile,
			"CURL_CA_BUNDLE":     filepath.Join(dir, "nope.pem"),
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("REQUESTS_CA_BUNDLE", "")
			t.Setenv("CURL_CA_BUNDLE", "")
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			client, err := newHTTPClient(tc.ca)
			if err != nil {
				t.Fatalf("newHTTPClient(%v): %v", tc.ca, err)
			}
			if err := clientGets(t, client, srv.URL); err != nil {
				t.Fatalf("HTTPS GET with ca=%v: %v", tc.ca, err)
			}
		})
	}

	// ca=false/"" disable verification when no env bundle is set.
	for _, tc := range []struct {
		name string
		ca   any
	}{
		{"ca-false-disables", false},
		{"empty-string-disables", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("REQUESTS_CA_BUNDLE", "")
			t.Setenv("CURL_CA_BUNDLE", "")
			client, err := newHTTPClient(tc.ca)
			if err != nil {
				t.Fatalf("newHTTPClient(%v): %v", tc.ca, err)
			}
			// A server with an untrusted cert is accepted because verification
			// is off.
			if err := clientGets(t, client, srv.URL); err != nil {
				t.Fatalf("insecure client rejected %v: %v", srv.URL, err)
			}
		})
	}

	// A CA DIRECTORY is refused at load (fail closed): trusting every
	// hashed-looking file would broaden trust beyond OpenSSL's subject-hash
	// capath lookup, so the config is rejected instead.
	t.Run("ca-directory-rejected", func(t *testing.T) {
		t.Setenv("REQUESTS_CA_BUNDLE", "")
		t.Setenv("CURL_CA_BUNDLE", "")
		hashedDir := t.TempDir()
		writeCertPEM(t, hashedDir, "9d66eef0.0", cert) // even a hashed entry is refused
		_, err := newHTTPClient(hashedDir)
		if err == nil {
			t.Fatal("newHTTPClient accepted a CA directory")
		}
		if !strings.Contains(err.Error(), "directory") || !strings.Contains(err.Error(), "unsupported") {
			t.Fatalf("directory rejection message = %q, want a clear 'unsupported directory' error", err)
		}
	})

	// A CA directory reached through an environment bundle is refused too.
	t.Run("ca-directory-env-rejected", func(t *testing.T) {
		t.Setenv("CURL_CA_BUNDLE", "")
		t.Setenv("REQUESTS_CA_BUNDLE", t.TempDir())
		if _, err := newHTTPClient(true); err == nil || !strings.Contains(err.Error(), "directory") {
			t.Fatalf("env CA directory err = %v, want a directory rejection", err)
		}
	})

	t.Setenv("REQUESTS_CA_BUNDLE", "")
	t.Setenv("CURL_CA_BUNDLE", "")
	if _, err := newHTTPClient(filepath.Join(dir, "nope.pem")); err == nil {
		t.Error("newHTTPClient accepted a missing CA path")
	}
	if _, err := newHTTPClient(t.TempDir()); err == nil {
		t.Error("newHTTPClient accepted a CA directory with no certificates")
	}
}

// --- level-aware diagnostics (review item 4) -------------------------------

// TestDiagnosticsUseWarningAndErrorLevels routes the sender's diagnostics
// through a production level-aware logger (journaldlog TTY, WARNING threshold)
// and asserts that Python's warning/error lines survive while its info lines
// are dropped.
func TestDiagnosticsUseWarningAndErrorLevels(t *testing.T) {
	srv := newCaptureServer(t)
	srv.aliasJSON = "{}"
	srv.putStatus = http.StatusBadRequest
	srv.putBody = `{"error":"boom"}`
	var buf bytes.Buffer
	log := journaldlog.NewTTYLog("es-test", &buf, journaldlog.LevelWarning)
	s := newTestSender(t, opensearch, srv.URL, log)

	// Missing timestamp + unmapped field (list) + failing create: all WARNING.
	msg := []byte(`{"MESSAGE": "hi", "FIELDLIST": [1, 2]}`)
	if err := s.SendMessages(context.Background(), [][]byte{msg}, "c1"); err != nil {
		t.Fatalf("create failure must not fail the batch: %v", err)
	}
	out := buf.String()
	for _, want := range []string{"timestamp", "Unmapped fields", "Could not create index mappings"} {
		if !strings.Contains(out, want) {
			t.Errorf("WARNING-level output missing %q:\n%s", want, out)
		}
	}
	// The INFO lines (Creating index / Sent ...) are below the threshold.
	if strings.Contains(out, "Creating index") || strings.Contains(out, "Sent 1 log events") {
		t.Errorf("INFO diagnostics leaked past the WARNING threshold:\n%s", out)
	}

	// A deletion TRANSPORT failure is ERROR-level (a 500 status does not raise in
	// requests, so only a connection error reaches the except clause).
	var delBuf bytes.Buffer
	delLog := journaldlog.NewTTYLog("es-test", &delBuf, journaldlog.LevelWarning)
	delSender := &Sender{
		cfg: config{
			senderType:          opensearch,
			sessionURL:          "http://es",
			requestTimeout:      defaultRequestTimeout,
			indexName:           defaultIndexPrefix,
			indexLifetimeInDays: defaultIndexLifetimeInDays,
		},
		log:     delLog,
		now:     time.Now,
		indices: map[string]struct{}{},
		client: &methodDoer{
			get: map[string]string{
				"/_aliases": `{"journalpump-2021-01-01": {}, "journalpump-2021-01-02": {}, "journalpump-2021-01-03": {}, "journalpump-2021-01-04": {}}`,
			},
			err: map[string]error{http.MethodDelete: errors.New("connection refused")},
		},
	}
	if err := delSender.MaintenanceOperations(context.Background()); err != nil {
		t.Fatalf("maintenance: %v", err)
	}
	if !strings.Contains(delBuf.String(), "Unexpected exception deleting index") {
		t.Errorf("deletion-failure ERROR line missing:\n%s", delBuf.String())
	}
}

// --- create-index transport error (review item 5) --------------------------

type fakeStats struct {
	mu    sync.Mutex
	calls []string
}

func (f *fakeStats) UnexpectedException(_ error, where string, _ map[string]string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, where)
}

func (f *fakeStats) has(where string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.calls {
		if c == where {
			return true
		}
	}
	return false
}

// methodDoer answers canned responses by HTTP method (used to simulate a
// transport-level PUT failure that a capture server cannot express).
type methodDoer struct {
	get map[string]string
	err map[string]error
}

func (d *methodDoer) Do(req *http.Request) (*http.Response, error) {
	if err := d.err[req.Method]; err != nil {
		return nil, err
	}
	body := ""
	if req.Method == http.MethodGet {
		body = d.get[req.URL.Path]
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     make(http.Header),
	}, nil
}

func TestCreateIndexTransportErrorBacksOffAndContinues(t *testing.T) {
	stats := &fakeStats{}
	var backoffs int
	s := &Sender{
		cfg:     config{senderType: opensearch, sessionURL: "http://es", requestTimeout: defaultRequestTimeout, indexName: defaultIndexPrefix},
		log:     discardLogger{},
		stats:   stats,
		now:     time.Now,
		indices: map[string]struct{}{},
		backoff: func(context.Context) { backoffs++ },
		client: &methodDoer{
			get: map[string]string{
				"/":         `{"version": {"number": "9.0.0"}}`,
				"/_aliases": `{}`,
			},
			err: map[string]error{http.MethodPut: errors.New("connection refused")},
		},
	}

	msg := []byte(`{"MESSAGE": "hi", "timestamp": "2019-10-07 14:00:00"}`)
	if err := s.SendMessages(context.Background(), [][]byte{msg}, "c1"); err != nil {
		t.Fatalf("create transport error must not fail the batch: %v", err)
	}
	if backoffs != 1 {
		t.Errorf("backoffs = %d, want 1 (Python's self._backoff after a PUT ConnectionError)", backoffs)
	}
	if !stats.has("es_pump_create_index_and_mappings") {
		t.Errorf("stats where missing: %v", stats.calls)
	}
}

// TestLoopWiresBackoffObserver pins that the shared loop injects its
// cancellable backoff into the sender (the seam the create path borrows).
func TestLoopWiresBackoffObserver(t *testing.T) {
	s := newTestSender(t, opensearch, "http://aaa", discardLogger{})
	in := make(chan sender.Item)
	_ = sender.NewLoop(sender.LoopConfig{Name: "es-backoff"}, sender.Instance{Transport: s, MaxSendInterval: time.Second}, in)
	if s.backoff == nil {
		t.Fatal("NewLoop did not inject the shared backoff into the sender")
	}
}

// --- cleanup (review item 6) -----------------------------------------------

func TestCloseReleasesIdleConnections(t *testing.T) {
	var open int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		switch state {
		case http.StateNew:
			atomic.AddInt32(&open, 1)
		case http.StateClosed, http.StateHijacked:
			atomic.AddInt32(&open, -1)
		}
	}
	srv.Start()
	defer srv.Close()

	s := newTestSender(t, opensearch, srv.URL, discardLogger{})
	today := time.Now().UTC().Format("2006-01-02")
	s.indices[defaultIndexPrefix+"-"+today] = struct{}{}
	msg := []byte(`{"MESSAGE": "hi", "timestamp": "` + today + `T00:00:00"}`)
	if err := s.SendMessages(context.Background(), [][]byte{msg}, "c1"); err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(&open) == 0 {
		t.Fatal("no live connection after a send; the test cannot observe a leak")
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt32(&open) != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := atomic.LoadInt32(&open); got != 0 {
		t.Fatalf("open connections = %d after Close, want 0", got)
	}
}

var _ io.Closer = (*Sender)(nil)

// --- per-request inactivity timeout (review item 2) ------------------------

// writeSlowResponse writes body in two halves around a mid-response gap, so
// the time between two reads exceeds the low timeout while the total is not a
// single deadline.
func writeSlowResponse(w http.ResponseWriter, body string, gap time.Duration) {
	w.WriteHeader(http.StatusOK)
	flusher := w.(http.Flusher)
	half := len(body) / 2
	_, _ = io.WriteString(w, body[:half])
	flusher.Flush()
	time.Sleep(gap)
	_, _ = io.WriteString(w, body[half:])
	flusher.Flush()
}

// TestInactivityTimeoutPerRequestOnReusedConnection pins review item 2: the
// inactivity timeout is applied at each REQUEST boundary, not baked at dial
// time. With opensearch_timeout=0.15 the ordinary timeout is 0.15 s and the
// bulk timeout 0.3 s; the server keeps ONE keep-alive connection and stalls
// 0.2 s mid-response, which lies BETWEEN the two limits. So a bulk reusing
// the version/alias connection must keep its 0.3 s, and a later ordinary
// request on the same connection must fall back to 0.15 s.
func TestInactivityTimeoutPerRequestOnReusedConnection(t *testing.T) {
	var newConns int32
	var slowAlias atomic.Bool
	handler := func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/":
			w.Header().Set("content-type", "application/json")
			_, _ = io.WriteString(w, `{"version": {"number": "1.2.4"}}`)
		case r.Method == http.MethodGet && r.URL.Path == "/_aliases":
			if slowAlias.Load() {
				writeSlowResponse(w, `{"a": {}}`, 200*time.Millisecond)
				return
			}
			_, _ = io.WriteString(w, "{}")
		case r.Method == http.MethodPut:
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost:
			writeSlowResponse(w, `{"errors": false}`, 200*time.Millisecond)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(handler))
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			atomic.AddInt32(&newConns, 1)
		}
	}
	srv.Start()
	defer srv.Close()

	inst, err := NewOpenSearch(sender.ConstructorArgs{Config: map[string]any{
		"opensearch_url":     srv.URL,
		"opensearch_timeout": 0.15, // ordinary 0.15 s, bulk 0.3 s
	}})
	if err != nil {
		t.Fatal(err)
	}
	s := inst.Transport.(*Sender)

	// Send #1: loadIndices (version + aliases) dials ONE connection; the PUT
	// and the 0.2 s-stalled bulk REUSE it. The bulk must use its own 0.3 s,
	// not the 0.15 s of the request that dialed the connection.
	msg := []byte(`{"MESSAGE": "hi", "timestamp": "2019-10-07 14:00:00"}`)
	if err := s.SendMessages(context.Background(), [][]byte{msg}, "c1"); err != nil {
		t.Fatalf("bulk on a reused connection used the ordinary timeout: %v", err)
	}
	if got := atomic.LoadInt32(&newConns); got != 1 {
		t.Fatalf("connections = %d, want 1 (the client must reuse keep-alive)", got)
	}

	// Send #2: an ORDINARY request (maintenance alias census) on the SAME
	// reused connection must use the ordinary 0.15 s — the bulk's 0.3 s that
	// ran last must not stick — so the 0.2 s stall fails it.
	slowAlias.Store(true)
	if err := s.MaintenanceOperations(context.Background()); err == nil {
		t.Fatal("ordinary request reused the bulk timeout (0.3 s stuck on the connection)")
	}
	if got := atomic.LoadInt32(&newConns); got != 1 {
		t.Fatalf("connections = %d, want 1 (maintenance must reuse the keep-alive)", got)
	}
}

// TestBulkItemErrorsAdvanceCursorThroughLoop quirk-locks Python: a 200 bulk
// response whose body reports per-item failures is NOT inspected — the batch is
// acked, the cursor advances, and there is no retry.
func TestBulkItemErrorsAdvanceCursorThroughLoop(t *testing.T) {
	srv := newCaptureServer(t)
	srv.aliasJSON = "{}"
	srv.bulkStatus = http.StatusOK
	srv.bulkBody = `{"errors": true, "items": [{"index": {"_index": "journalpump-2019-10-07", "status": 429, "error": "rejected"}}]}`

	inst, err := NewOpenSearch(sender.ConstructorArgs{Config: map[string]any{
		"opensearch_url":    srv.URL,
		"max_send_interval": 0.05,
	}})
	if err != nil {
		t.Fatal(err)
	}

	in := make(chan sender.Item, 4)
	loop := sender.NewLoop(sender.LoopConfig{Name: "es-loop"}, inst, in)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = loop.Run(ctx)
		close(done)
	}()
	msg := []byte(`{"MESSAGE": "hi", "timestamp": "2019-10-07 14:00:00"}`)
	in <- sender.Item{Payload: msg, Cursor: "c0"}

	deadline := time.Now().Add(3 * time.Second)
	for loop.State().Sent.Count < 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	st := loop.State()
	cancel()
	<-done

	if st.Sent.Count != 1 {
		t.Fatalf("sent count = %d, want 1 (per-item errors must not retry)", st.Sent.Count)
	}
	if st.Sent.Cursor == nil || *st.Sent.Cursor != "c0" {
		t.Fatalf("sent cursor = %v, want c0 (the cursor must advance)", st.Sent.Cursor)
	}
	if posts := srv.requestsWith(http.MethodPost, "/journalpump-2019-10-07/_bulk"); len(posts) != 1 {
		t.Fatalf("bulk POSTs = %d, want exactly 1 (no retry)", len(posts))
	}
}
