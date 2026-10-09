// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package logplex

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Aiven-Open/journalpump/internal/sender"
)

// fakeDoer records requests and returns a canned response.
type fakeDoer struct {
	status int
	calls  []capturedRequest
}

type capturedRequest struct {
	method  string
	url     string
	headers http.Header
	body    []byte
}

func (d *fakeDoer) Do(req *http.Request) (*http.Response, error) {
	body, _ := io.ReadAll(req.Body)
	d.calls = append(d.calls, capturedRequest{method: req.Method, url: req.URL.String(), headers: req.Header, body: body})
	status := d.status
	if status == 0 {
		status = 200
	}
	return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader("")), Header: http.Header{}}, nil
}

func newTestSender(t *testing.T, cfg map[string]any) (*Sender, *fakeDoer) {
	t.Helper()
	inst, err := New(sender.ConstructorArgs{Name: "test-logplex", Config: cfg})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	s := inst.Transport.(*Sender)
	d := &fakeDoer{}
	s.client = d
	s.now = func() time.Time { return time.Date(2025, 6, 26, 14, 52, 33, 0, time.UTC) }
	return s, d
}

func baseConfig() map[string]any {
	return map[string]any{
		"logplex_log_input_url": "http://logplex.example/input/",
		"logplex_token":         "token",
	}
}

// TestFormatMessageCleanFrame pins the SANCTIONED clean RFC frame (charter
// §2/D6): Python's f-string bytes-repr bug is deliberately NOT reproduced,
// so the wire is the plain length-prefixed frame with no b'...' wrapper and
// the length prefix counts the frame's own bytes.
func TestFormatMessageCleanFrame(t *testing.T) {
	s, _ := newTestSender(t, baseConfig())
	frame, err := s.formatMsg([]byte(`{"MESSAGE": "hello", "HOSTNAME": "host", "PID": "42"}`))
	if err != nil {
		t.Fatalf("formatMsg: %v", err)
	}
	content := "<190>1 2025-06-26T14:52:33+00:00 localhost token localhost - -hello"
	want := strconv.Itoa(len(content)) + " " + content
	if frame != want {
		t.Fatalf("frame = %q, want %q", frame, want)
	}
	if strings.Contains(frame, "b'") {
		t.Fatalf("frame %q reproduces Python's bytes-repr bug; must be the clean frame", frame)
	}
}

// TestHostnamePIDAlwaysLocalhost is the named quirk-lock: the journal
// processor strips leading underscores before the sender sees a payload
// (journalpump.py:708-714), so the _HOSTNAME/_PID lookups never match and
// hostname/pid are the literal "localhost" even when the payload carries
// HOSTNAME/PID.
func TestHostnamePIDAlwaysLocalhost(t *testing.T) {
	s, _ := newTestSender(t, baseConfig())
	frame, err := s.formatMsg([]byte(`{"MESSAGE": "m", "HOSTNAME": "host.example", "PID": "4321"}`))
	if err != nil {
		t.Fatalf("formatMsg: %v", err)
	}
	parsed := parseFrame(t, frame)
	if parsed.hostname != "localhost" || parsed.pid != "localhost" {
		t.Fatalf("hostname/pid = %q/%q, want localhost/localhost", parsed.hostname, parsed.pid)
	}
}

// TestPRIAndFieldConstants locks the fixed frame fields: PRI is always 190,
// msgid and structured-data are "-", and the token is the configured value.
func TestPRIAndFieldConstants(t *testing.T) {
	s, _ := newTestSender(t, baseConfig())
	frame, err := s.formatMsg([]byte(`{"MESSAGE": "m"}`))
	if err != nil {
		t.Fatalf("formatMsg: %v", err)
	}
	parsed := parseFrame(t, frame)
	if parsed.pri != "190" || parsed.version != "1" {
		t.Fatalf("PRI/version = %q/%q, want 190/1", parsed.pri, parsed.version)
	}
	if parsed.token != "token" || parsed.msgID != "-" || parsed.sd != "-" {
		t.Fatalf("token/msgid/sd = %q/%q/%q, want token/-/-", parsed.token, parsed.msgID, parsed.sd)
	}
}

// TestFormatTimestampIsFormatTime asserts the timestamp is the format-time
// clock (not the journal timestamp) in Python's
// "%Y-%m-%dT%H:%M:%S+00:00 " shape.
func TestFormatTimestampIsFormatTime(t *testing.T) {
	s, _ := newTestSender(t, baseConfig())
	frame, err := s.formatMsg([]byte(`{"MESSAGE": "m", "timestamp": "1999-01-01T00:00:00.000000"}`))
	if err != nil {
		t.Fatalf("formatMsg: %v", err)
	}
	parsed := parseFrame(t, frame)
	if parsed.timestamp != "2025-06-26T14:52:33+00:00" {
		t.Fatalf("timestamp = %q, want the format-time 2025-06-26T14:52:33+00:00", parsed.timestamp)
	}
}

// TestMissingMessageIsAnError covers Python's unguarded entry["MESSAGE"]
// (KeyError) and the non-string append (TypeError): both fail the send.
func TestMissingMessageIsAnError(t *testing.T) {
	s, _ := newTestSender(t, baseConfig())
	if _, err := s.formatMsg([]byte(`{"HOSTNAME": "h"}`)); err == nil {
		t.Fatal("formatMsg succeeded without MESSAGE; want an error")
	}
	if _, err := s.formatMsg([]byte(`{"MESSAGE": 5}`)); err == nil {
		t.Fatal("formatMsg succeeded with a non-string MESSAGE; want an error")
	}
	if _, err := s.formatMsg([]byte(`not json`)); err == nil {
		t.Fatal("formatMsg succeeded on invalid JSON; want an error")
	}
}

// TestSendPostsHeadersAndBody asserts the POST shape: method, path, Basic
// auth ("token" + token), Content-Type application/logplex-1, the message
// count header, and a body that concatenates the per-message frames.
func TestSendPostsHeadersAndBody(t *testing.T) {
	s, d := newTestSender(t, baseConfig())
	batch := [][]byte{[]byte(`{"MESSAGE": "one"}`), []byte(`{"MESSAGE": "two"}`)}
	if err := s.SendMessages(context.Background(), batch, "c1"); err != nil {
		t.Fatalf("SendMessages: %v", err)
	}
	if len(d.calls) != 1 {
		t.Fatalf("requests = %d, want 1", len(d.calls))
	}
	req := d.calls[0]
	if req.method != http.MethodPost {
		t.Fatalf("method = %q, want POST", req.method)
	}
	if got := req.url; got != "http://logplex.example/input/" {
		t.Fatalf("url = %q", got)
	}
	if got := req.headers.Get("Content-Type"); got != "application/logplex-1" {
		t.Fatalf("Content-Type = %q, want application/logplex-1", got)
	}
	if got := req.headers.Get("Logplex-Msg-Count"); got != "2" {
		t.Fatalf("Logplex-Msg-Count = %q, want 2", got)
	}
	user, pass, ok := req.headers.Get("Authorization"), "", false
	if v, p, has := basicAuth(req.headers); has {
		user, pass, ok = v, p, true
	}
	if !ok || user != "token" || pass != "token" {
		t.Fatalf("basic auth = %q/%q ok=%v, want token/token", user, pass, ok)
	}
	one := "<190>1 2025-06-26T14:52:33+00:00 localhost token localhost - -one"
	two := "<190>1 2025-06-26T14:52:33+00:00 localhost token localhost - -two"
	wantBody := strconv.Itoa(len(one)) + " " + one + strconv.Itoa(len(two)) + " " + two
	if string(req.body) != wantBody {
		t.Fatalf("body = %q, want %q", req.body, wantBody)
	}
}

// Test500DoesNotRetryOrDisconnect is the fire-and-forget quirk lock run
// through the REAL sender.Loop: a 500 response is ignored (no error), so the
// batch is acked once and the sender stays connected — no retry, no
// mark_disconnected.
func Test500DoesNotRetryOrDisconnect(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		inst, err := New(sender.ConstructorArgs{Name: "logplex-loop", Config: baseConfig()})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		s := inst.Transport.(*Sender)
		d := &fakeDoer{status: 500}
		s.client = d
		s.now = func() time.Time { return time.Unix(0, 0) }

		in := make(chan sender.Item, 4)
		loop := sender.NewLoop(sender.LoopConfig{Name: "logplex-loop"}, inst, in)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			_ = loop.Run(ctx)
			close(done)
		}()
		in <- sender.Item{Payload: []byte(`{"MESSAGE": "m"}`), Cursor: "c0"}
		time.Sleep(6 * time.Second)
		synctest.Wait()
		st := loop.State()
		cancel()
		<-done
		if len(d.calls) != 1 {
			t.Fatalf("requests = %d, want exactly 1 (a 500 must not retry)", len(d.calls))
		}
		if st.Sent.Count != 1 {
			t.Fatalf("sent count = %d, want 1 (the batch is still marked sent)", st.Sent.Count)
		}
		if st.Health.Status != "connected" {
			t.Fatalf("health status = %q, want connected (a 500 must not disconnect)", st.Health.Status)
		}
	})
}

// TestNewValidatesRequiredConfig ports LogplexSender.__init__'s config[...]
// KeyErrors and the default max_send_interval / mark_connected.
func TestNewValidatesRequiredConfig(t *testing.T) {
	if _, err := New(sender.ConstructorArgs{Name: "x", Config: map[string]any{"logplex_token": "t"}}); err == nil {
		t.Fatal("New accepted a config without logplex_log_input_url")
	}
	if _, err := New(sender.ConstructorArgs{Name: "x", Config: map[string]any{"logplex_log_input_url": "http://x/"}}); err == nil {
		t.Fatal("New accepted a config without logplex_token")
	}
	inst, err := New(sender.ConstructorArgs{Name: "x", Config: baseConfig()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if inst.Type != "LogplexSender" {
		t.Fatalf("Type = %q, want LogplexSender", inst.Type)
	}
	if !inst.ConnectOnInit {
		t.Fatal("ConnectOnInit = false; logplex.py calls mark_connected in __init__")
	}
	if inst.MaxSendInterval != 5*time.Second {
		t.Fatalf("MaxSendInterval = %s, want 5s", inst.MaxSendInterval)
	}
}

// TestHTTPServerEndToEnd runs the sender against a real httptest server and
// checks the captured request (belt-and-braces over the fake doer).
func TestHTTPServerEndToEnd(t *testing.T) {
	var (
		gotAuth, gotType, gotCount string
		gotBody                    []byte
		requests                   int
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		gotAuth = r.Header.Get("Authorization")
		gotType = r.Header.Get("Content-Type")
		gotCount = r.Header.Get("Logplex-Msg-Count")
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	cfg := baseConfig()
	cfg["logplex_log_input_url"] = srv.URL
	inst, err := New(sender.ConstructorArgs{Name: "x", Config: cfg})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	s := inst.Transport.(*Sender)
	s.now = func() time.Time { return time.Unix(0, 0) }
	if err := s.SendMessages(context.Background(), [][]byte{[]byte(`{"MESSAGE": "hi"}`)}, "c"); err != nil {
		t.Fatalf("SendMessages: %v", err)
	}
	if requests != 1 {
		t.Fatalf("requests = %d, want 1", requests)
	}
	if gotAuth != "Basic dG9rZW46dG9rZW4=" {
		t.Fatalf("Authorization = %q", gotAuth)
	}
	if gotType != "application/logplex-1" || gotCount != "1" {
		t.Fatalf("content-type/count = %q/%q", gotType, gotCount)
	}
	if !strings.Contains(string(gotBody), "- -hi") || strings.Contains(string(gotBody), "b'") {
		t.Fatalf("body = %q, want the clean frame", gotBody)
	}
}

// frameFields is the parsed clean RFC frame.
type frameFields struct {
	pri, version, timestamp string
	hostname, token, pid    string
	msgID, sd, message      string
	lengthPrefix            int
}

func parseFrame(t *testing.T, frame string) frameFields {
	t.Helper()
	prefix, rest, ok := strings.Cut(frame, " ")
	if !ok {
		t.Fatalf("frame %q has no length prefix", frame)
	}
	n, err := strconv.Atoi(prefix)
	if err != nil {
		t.Fatalf("length prefix %q: %v", prefix, err)
	}
	if n != len(rest) {
		t.Fatalf("frame %q: length prefix %d, want %d", frame, n, len(rest))
	}
	f := parseContent(t, rest)
	f.lengthPrefix = n
	return f
}

// parseContent parses a length-prefix-free clean frame (the fixture's frames
// are stored repr-stripped, without the prefix).
func parseContent(t *testing.T, content string) frameFields {
	t.Helper()
	const head = "<190>1 "
	if !strings.HasPrefix(content, head) {
		t.Fatalf("content %q does not start with %q", content, head)
	}
	parts := strings.SplitN(content[len(head):], " ", 6)
	if len(parts) != 6 {
		t.Fatalf("content %q: %d space-separated fields, want 6", content, len(parts))
	}
	if !strings.HasPrefix(parts[5], "-") {
		t.Fatalf("content %q: msgid/sd tail %q does not start with -", content, parts[5])
	}
	return frameFields{
		pri:       "190",
		version:   "1",
		timestamp: parts[0],
		hostname:  parts[1],
		token:     parts[2],
		pid:       parts[3],
		msgID:     parts[4],
		sd:        parts[5][:1],
		message:   parts[5][1:],
	}
}

// TestFixtureStructuralParity replays the Task 11 Python ground-truth
// fixture's repr-STRIPPED frame content through the Go sender and compares
// the parsed fields (never the raw Python bytes, which carry the sanctioned
// b'...' framing bug). This pins the field VALUES Python intends: PRI 190,
// literal localhost hostname/pid, the configured token, msgid/sd "-", and
// the message text.
func TestFixtureStructuralParity(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "systest-go", "fixtures", "logplex.json"))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var fixture struct {
		Frames  []string `json:"frames"`
		Request struct {
			Headers map[string]string `json:"headers"`
		} `json:"request"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatalf("unmarshal fixture: %v", err)
	}
	if len(fixture.Frames) == 0 {
		t.Fatal("fixture has no frames")
	}
	if got := fixture.Request.Headers["content-type"]; got != "application/logplex-1" {
		t.Fatalf("fixture content-type = %q", got)
	}
	s, _ := newTestSender(t, baseConfig())
	s.now = func() time.Time { return time.Unix(0, 0) }

	for i, want := range fixture.Frames {
		wanted := parseContent(t, want)
		payload, err := json.Marshal(map[string]any{"MESSAGE": wanted.message, "HOSTNAME": "ignored", "PID": "9"})
		if err != nil {
			t.Fatalf("marshal payload: %v", err)
		}
		frame, err := s.formatMsg(payload)
		if err != nil {
			t.Fatalf("frame %d formatMsg: %v", i, err)
		}
		if strings.Contains(frame, "b'") {
			t.Fatalf("frame %d = %q reproduces the sanitized-away bytes-repr bug", i, frame)
		}
		got := parseFrame(t, frame)
		if got.pri != wanted.pri || got.version != wanted.version {
			t.Fatalf("frame %d PRI/version = %q/%q, want %q/%q", i, got.pri, got.version, wanted.pri, wanted.version)
		}
		if got.hostname != "localhost" || got.pid != "localhost" {
			t.Fatalf("frame %d hostname/pid = %q/%q, want localhost/localhost", i, got.hostname, got.pid)
		}
		if got.token != "token" {
			t.Fatalf("frame %d token = %q, want token", i, got.token)
		}
		if got.msgID != "-" || got.sd != "-" {
			t.Fatalf("frame %d msgid/sd = %q/%q, want -/-", i, got.msgID, got.sd)
		}
		if got.message != wanted.message {
			t.Fatalf("frame %d message = %q, want %q", i, got.message, wanted.message)
		}
		if len(got.timestamp) != len(wanted.timestamp) || !strings.HasSuffix(got.timestamp, "+00:00") {
			t.Fatalf("frame %d timestamp = %q, want a %d-char +00:00 shape", i, got.timestamp, len(wanted.timestamp))
		}
	}
}

// basicAuth decodes a Basic Authorization header.
func basicAuth(h http.Header) (user, pass string, ok bool) {
	req := &http.Request{Header: h}
	return req.BasicAuth()
}
