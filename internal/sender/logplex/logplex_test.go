// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package logplex

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Aiven-Open/journalpump/internal/sender"
)

// fakeDoer records requests and returns a canned response (or an error).
type fakeDoer struct {
	status int
	err    error
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
	if d.err != nil {
		return nil, d.err
	}
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
// are stored repr-stripped, without the prefix). PRI and version are parsed
// from the "<PRI>VERSION " head rather than assumed, so the fixture comparison
// genuinely checks them.
func parseContent(t *testing.T, content string) frameFields {
	t.Helper()
	if !strings.HasPrefix(content, "<") {
		t.Fatalf("content %q does not start with '<'", content)
	}
	gt := strings.IndexByte(content, '>')
	if gt < 0 {
		t.Fatalf("content %q has no '>'", content)
	}
	pri := content[1:gt]
	rest := content[gt+1:]
	sp := strings.IndexByte(rest, ' ')
	if sp < 0 {
		t.Fatalf("content %q has no version separator", content)
	}
	version := rest[:sp]
	parts := strings.SplitN(rest[sp+1:], " ", 6)
	if len(parts) != 6 {
		t.Fatalf("content %q: %d space-separated fields, want 6", content, len(parts))
	}
	if !strings.HasPrefix(parts[5], "-") {
		t.Fatalf("content %q: msgid/sd tail %q does not start with -", content, parts[5])
	}
	return frameFields{
		pri:       pri,
		version:   version,
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
// fixture at the HTTP CAPTURE boundary: the fixture-derived batch goes
// through the PRODUCTION HTTP client to a recording server, and every
// captured length-prefixed frame is parsed and compared field-by-field with
// the fixture's parsed values (never byte-for-byte against Python's raw
// bytes-repr wire, which is the sanctioned D6 deviation). It also rejects
// any b'...' repr framing.
func TestFixtureStructuralParity(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "systest-go", "fixtures", "logplex.json"))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var fixture struct {
		Frames  []string `json:"frames"`
		Request struct {
			Method  string            `json:"method"`
			Path    string            `json:"path"`
			Headers map[string]string `json:"headers"`
		} `json:"request"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatalf("unmarshal fixture: %v", err)
	}
	if len(fixture.Frames) == 0 {
		t.Fatal("fixture has no frames")
	}
	if len(fixture.Request.Headers) == 0 {
		t.Fatal("fixture request has no headers")
	}

	var body []byte
	var captured http.Header
	var method, path string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ = io.ReadAll(r.Body)
		captured = r.Header.Clone()
		method, path = r.Method, r.URL.Path
		w.WriteHeader(http.StatusInternalServerError) // status must still be ignored
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

	batch := make([][]byte, 0, len(fixture.Frames))
	want := make([]frameFields, 0, len(fixture.Frames))
	for _, raw := range fixture.Frames {
		parsed := parseContent(t, raw)
		payload, err := json.Marshal(map[string]any{"MESSAGE": parsed.message, "HOSTNAME": "ignored", "PID": "9"})
		if err != nil {
			t.Fatalf("marshal payload: %v", err)
		}
		batch = append(batch, payload)
		want = append(want, parsed)
	}
	if err := s.SendMessages(context.Background(), batch, "c"); err != nil {
		t.Fatalf("SendMessages: %v", err)
	}
	if strings.Contains(string(body), "b'") {
		t.Fatalf("captured body reproduces the sanitized-away bytes-repr bug: %q", body)
	}
	// The captured request must match the fixture's request (method, path and
	// every fixture header), not hard-coded constants.
	if method != fixture.Request.Method || path != fixture.Request.Path {
		t.Fatalf("captured %s %s, want %s %s", method, path, fixture.Request.Method, fixture.Request.Path)
	}
	for key, want := range fixture.Request.Headers {
		if got := captured.Get(key); got != want {
			t.Fatalf("captured header %s = %q, want %q", key, got, want)
		}
	}
	got := parseFrames(t, body)
	if len(got) != len(want) {
		t.Fatalf("captured %d frames, want %d", len(got), len(want))
	}
	for i := range want {
		g, w := got[i], want[i]
		// Compare every non-clock field directly against the fixture's parsed
		// values; the clock is the only field the capture cannot pin.
		if g.pri != w.pri || g.version != w.version {
			t.Fatalf("frame %d PRI/version = %q/%q, want %q/%q", i, g.pri, g.version, w.pri, w.version)
		}
		if g.hostname != w.hostname || g.pid != w.pid {
			t.Fatalf("frame %d hostname/pid = %q/%q, want %q/%q", i, g.hostname, g.pid, w.hostname, w.pid)
		}
		if g.token != w.token {
			t.Fatalf("frame %d token = %q, want %q", i, g.token, w.token)
		}
		if g.msgID != w.msgID || g.sd != w.sd {
			t.Fatalf("frame %d msgid/sd = %q/%q, want %q/%q", i, g.msgID, g.sd, w.msgID, w.sd)
		}
		if g.message != w.message {
			t.Fatalf("frame %d message = %q, want %q", i, g.message, w.message)
		}
		if len(g.timestamp) != len(w.timestamp) || !strings.HasSuffix(g.timestamp, "+00:00") {
			t.Fatalf("frame %d timestamp = %q, want a %d-char +00:00 shape", i, g.timestamp, len(w.timestamp))
		}
	}
}

// TestHostnamePIDFromExtras proves the _HOSTNAME/_PID lookups ARE reachable:
// extra_field_values are merged after the underscore stripping, so a sender
// extra can supply them and its JSON values must interpolate exactly as
// Python's f-string does.
func TestHostnamePIDFromExtras(t *testing.T) {
	cases := []struct {
		name              string
		payload           string
		wantHost, wantPid string
	}{
		{"integer pid", `{"MESSAGE":"m","_PID":5}`, "localhost", "5"},
		{"float pid", `{"MESSAGE":"m","_PID":5.0}`, "localhost", "5.0"},
		{"bool hostname", `{"MESSAGE":"m","_HOSTNAME":true}`, "True", "localhost"},
		{"null hostname", `{"MESSAGE":"m","_HOSTNAME":null}`, "None", "localhost"},
		{"string pid", `{"MESSAGE":"m","_PID":"abc"}`, "localhost", "abc"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := newTestSender(t, baseConfig())
			frame, err := s.formatMsg([]byte(tc.payload))
			if err != nil {
				t.Fatalf("formatMsg: %v", err)
			}
			parsed := parseContent(t, mustCutPrefix(t, frame))
			if parsed.hostname != tc.wantHost || parsed.pid != tc.wantPid {
				t.Fatalf("hostname/pid = %q/%q, want %q/%q", parsed.hostname, parsed.pid, tc.wantHost, tc.wantPid)
			}
		})
	}
}

// TestPyStrMatchesPython pins pyStr/pyRepr against Python str()/repr() for
// container-valued _HOSTNAME/_PID extras: strings use Python's quote selection
// (double quotes when the string contains ' and not ") and escape control
// characters (\xNN) and backslashes rather than emitting them literally.
func TestPyStrMatchesPython(t *testing.T) {
	cases := []struct {
		name string
		in   any
		want string
	}{
		{"apostrophe selects double quotes", []any{"a'b"}, `["a'b"]`},
		{"double quote keeps single", []any{`a"b`}, `['a"b']`},
		{"both quotes keeps single", []any{`a'b"c`}, `['a\'b"c']`},
		{"nul and apostrophe", []any{"a'b", "nul\x00"}, `["a'b", 'nul\x00']`},
		{"control chars", []any{"tab\tend", "nl\nend", "cr\rend", "bell\x07", "del\x7f"}, `['tab\tend', 'nl\nend', 'cr\rend', 'bell\x07', 'del\x7f']`},
		{"backslash", []any{`back\slash`}, `['back\\slash']`},
		{"scalars", []any{1, 2.5, true, nil}, `[1, 2.5, True, None]`},
		{"object", map[string]any{"k": "a'b"}, `{'k': "a'b"}`},
		{"latin-1 nonprintables", []any{"nel\u0085", "nbsp\u00a0", "softhy\u00ad"}, `['nel\x85', 'nbsp\xa0', 'softhy\xad']`},
		{"bmp nonprintables", []any{"zwsp\u200b", "ls\u2028", "ps\u2029", "nnbsp\u202f", "wj\u2060", "bom\ufeff", "ideo\u3000"}, `['zwsp\u200b', 'ls\u2028', 'ps\u2029', 'nnbsp\u202f', 'wj\u2060', 'bom\ufeff', 'ideo\u3000']`},
		{"private use is nonprintable", []any{"\ue000", "\uf8ff"}, `['\ue000', '\uf8ff']`},
		{"astral nonprintable", []any{"\U000e0001"}, `['\U000e0001']`},
		{"printable non-ascii is literal", []any{"\u00e9", "\u2603", "\U0001f600", "\ufffd"}, "['\u00e9', '\u2603', '\U0001f600', '\ufffd']"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := pyStr(tc.in); got != tc.want {
				t.Fatalf("pyStr(%#v) = %s, want %s", tc.in, got, tc.want)
			}
		})
	}
}

// TestTransportFailureRetainsAndRetries is the sanctioned-deviation
// regression (charter §2): on a transport error Go returns the error so the
// shared loop RETAINS the batch, RETRIES it, and marks the sender
// disconnected — Python instead drops the batch and stays green.
func TestTransportFailureRetainsAndRetries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		inst, err := New(sender.ConstructorArgs{Name: "logplex-loop", Config: baseConfig()})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		s := inst.Transport.(*Sender)
		d := &fakeDoer{err: errors.New("dial failed")}
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
		time.Sleep(8 * time.Second)
		synctest.Wait()
		st := loop.State()
		cancel()
		<-done
		if len(d.calls) < 2 {
			t.Fatalf("requests = %d, want >= 2 (the batch must be retried)", len(d.calls))
		}
		if st.Sent.Count != 0 {
			t.Fatalf("sent count = %d, want 0 (the batch is retained, not dropped)", st.Sent.Count)
		}
		if st.Health.Status != "disconnected" {
			t.Fatalf("health status = %q, want disconnected", st.Health.Status)
		}
		if len(st.Health.Errors) == 0 {
			t.Fatal("health errors is empty; the transport failure was not recorded")
		}
	})
}

// TestTruncatedBodyIsAnError: a response whose body ends before its declared
// length must fail the send (requests consumes the body and raises).
func TestTruncatedBodyIsAnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "100")
		_, _ = w.Write([]byte("short"))
	}))
	defer srv.Close()
	s := productionSender(t, srv.URL, 2)
	if err := s.SendMessages(context.Background(), [][]byte{[]byte(`{"MESSAGE":"m"}`)}, "c"); err == nil {
		t.Fatal("SendMessages succeeded on a truncated body; want an error")
	}
}

// TestStalledBodyTimesOut: a body that stops delivering for longer than the
// configured timeout must fail the send.
func TestStalledBodyTimesOut(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "100")
		_, _ = w.Write([]byte("par"))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-release
	}))
	defer func() {
		close(release)
		srv.Close()
	}()
	s := productionSender(t, srv.URL, 0.1)
	if err := s.SendMessages(context.Background(), [][]byte{[]byte(`{"MESSAGE":"m"}`)}, "c"); err == nil {
		t.Fatal("SendMessages succeeded on a stalled body; want a timeout error")
	}
}

// TestResolvedTimeoutUsesSessionDefault: logplex_request_timeout = 0 (falsy)
// must become requests' 60 s session default, not "no timeout". Only ZERO is
// falsy here: a negative value is truthy in Python and is rejected at load.
func TestResolvedTimeoutUsesSessionDefault(t *testing.T) {
	for _, tc := range []struct {
		in   float64
		want time.Duration
	}{{0, 60 * time.Second}, {2, 2 * time.Second}, {0.5, 500 * time.Millisecond}} {
		if got := resolvedTimeout(tc.in); got != tc.want {
			t.Fatalf("resolvedTimeout(%v) = %s, want %s", tc.in, got, tc.want)
		}
	}
}

// TestNegativeTimeoutRejected: a negative logplex_request_timeout is truthy in
// Python, so TimeoutAdapter passes it through and requests raises "the timeout
// cannot be set to a value less than or equal to 0"; Go rejects it at load.
func TestNegativeTimeoutRejected(t *testing.T) {
	cfg := baseConfig()
	cfg["logplex_request_timeout"] = -1
	if _, err := New(sender.ConstructorArgs{Name: "x", Config: cfg}); err == nil {
		t.Fatal("New accepted a negative logplex_request_timeout; want an error")
	}
}

// TestSlowResponseWithinTimeout: a server that delays its response less than
// the timeout still succeeds (inactivity, not a total deadline).
func TestSlowResponseWithinTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(50 * time.Millisecond)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()
	s := productionSender(t, srv.URL, 0.5)
	if err := s.SendMessages(context.Background(), [][]byte{[]byte(`{"MESSAGE":"m"}`)}, "c"); err != nil {
		t.Fatalf("SendMessages on a slow-but-within-timeout response: %v", err)
	}
}

// TestIncrementalDeliveryExceedsTotalTimeout: the response takes LONGER than
// the timeout in total but never stalls longer than it between chunks, so an
// inactivity timeout succeeds where a total deadline would fail.
func TestIncrementalDeliveryExceedsTotalTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		f, _ := w.(http.Flusher)
		for i := 0; i < 6; i++ {
			_, _ = w.Write([]byte("x"))
			if f != nil {
				f.Flush()
			}
			time.Sleep(40 * time.Millisecond)
		}
	}))
	defer srv.Close()
	s := productionSender(t, srv.URL, 0.1)
	if err := s.SendMessages(context.Background(), [][]byte{[]byte(`{"MESSAGE":"m"}`)}, "c"); err != nil {
		t.Fatalf("SendMessages on an incremental response: %v (total 240ms > 100ms timeout)", err)
	}
}

// proxyEnvVars is every proxy-related variable the selector reads.
var proxyEnvVars = []string{"HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy", "ALL_PROXY", "all_proxy", "NO_PROXY", "no_proxy"}

// syncProxyEnv UNSETS every proxy variable (restoring them at cleanup) and
// then sets exactly the requested ones, so a proxy test is independent of the
// ambient environment. Unsetting — rather than setting to "" — matters: an
// empty lowercase http_proxy removes the entry in requests, so it must only be
// present when a test deliberately supplies it.
func syncProxyEnv(t *testing.T, vars map[string]string) {
	t.Helper()
	for _, name := range proxyEnvVars {
		name := name
		old, had := os.LookupEnv(name)
		t.Cleanup(func() {
			if had {
				_ = os.Setenv(name, old)
			} else {
				_ = os.Unsetenv(name)
			}
		})
		if err := os.Unsetenv(name); err != nil {
			t.Fatalf("unset %s: %v", name, err)
		}
	}
	for name, value := range vars {
		if err := os.Setenv(name, value); err != nil {
			t.Fatalf("set %s: %v", name, err)
		}
	}
}

// TestProxySelectionMatchesRequests unit-tests the requests-faithful selector:
// a lowercase http_proxy wins over an uppercase HTTP_PROXY (and an empty
// lowercase value removes the entry), and ALL_PROXY is the scheme fallback.
func TestProxySelectionMatchesRequests(t *testing.T) {
	req, err := http.NewRequest(http.MethodPost, "http://logplex.example.invalid/input", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	cases := []struct {
		name string
		env  map[string]string
		want string // "" means no proxy
	}{
		{"uppercase only", map[string]string{"HTTP_PROXY": "http://up:1"}, "http://up:1"},
		{"lowercase wins", map[string]string{"http_proxy": "http://lo:1", "HTTP_PROXY": "http://up:1"}, "http://lo:1"},
		{"empty lowercase removes", map[string]string{"HTTP_PROXY": "http://up:1", "http_proxy": ""}, ""},
		{"ALL_PROXY fallback", map[string]string{"ALL_PROXY": "http://all:1"}, "http://all:1"},
		{"scheme beats ALL_PROXY", map[string]string{"HTTP_PROXY": "http://up:1", "ALL_PROXY": "http://all:1"}, "http://up:1"},
		{"none", map[string]string{}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			syncProxyEnv(t, tc.env)
			got, err := proxyForRequest(req)
			if err != nil {
				t.Fatalf("proxyForRequest: %v", err)
			}
			if tc.want == "" {
				if got != nil {
					t.Fatalf("proxy = %v, want none", got)
				}
				return
			}
			if got == nil || got.String() != tc.want {
				t.Fatalf("proxy = %v, want %s", got, tc.want)
			}
		})
	}
}

// TestNoProxySelection exercises the full requests/urllib no_proxy semantics
// (requests' plain suffix loop OR urllib's proxy_bypass_environment fallback):
// lowercase-first lookup, wildcard, case-insensitivity, leading-dot and
// label-boundary matching, port handling, CIDR/plain IP, and that loopback is
// not special-cased.
func TestNoProxySelection(t *testing.T) {
	cases := []struct {
		name string
		url  string
		env  map[string]string
		want bool // bypass?
	}{
		{"hostname match", "http://logplex.example.invalid/input", map[string]string{"no_proxy": "logplex.example.invalid"}, true},
		{"hostname no match", "http://logplex.example.invalid/input", map[string]string{"no_proxy": "other.invalid"}, false},
		{"loopback not special-cased", "http://logplex.example.invalid/input", map[string]string{}, false},
		{"uppercase NO_PROXY ip", "http://127.0.0.1/input", map[string]string{"NO_PROXY": "127.0.0.1"}, true},
		{"wildcard", "http://logplex.example.invalid/input", map[string]string{"no_proxy": "*"}, true},
		{"wildcard ip", "http://127.0.0.1/input", map[string]string{"no_proxy": "*"}, true},
		{"mixed-case entry", "http://example.com/input", map[string]string{"no_proxy": "Example.Com"}, true},
		{"mixed-case uppercase entry", "http://example.com/input", map[string]string{"NO_PROXY": "EXAMPLE.COM"}, true},
		{"leading dot matches apex", "http://example.com/input", map[string]string{"no_proxy": ".example.com"}, true},
		{"leading dot matches subdomain", "http://foo.example.com/input", map[string]string{"no_proxy": ".example.com"}, true},
		{"bare domain matches subdomain", "http://foo.example.com/input", map[string]string{"no_proxy": "example.com"}, true},
		// requests' explicit no_proxy loop is a PLAIN suffix match with no label
		// boundary, so "example.com" also matches "notexample.com" (probe-verified
		// against requests 2.33.1). Python parity is preserved here deliberately;
		// a stricter label-boundary rule would be an unsanctioned deviation.
		{"plain suffix has no label boundary", "http://notexample.com/input", map[string]string{"no_proxy": "example.com"}, true},
		{"port match", "http://example.com:8080/input", map[string]string{"no_proxy": "example.com:8080"}, true},
		{"port mismatch", "http://example.com:9090/input", map[string]string{"no_proxy": "example.com:8080"}, false},
		{"bare domain ignores port", "http://example.com:9090/input", map[string]string{"no_proxy": "example.com"}, true},
		{"cidr network", "http://10.1.2.3/input", map[string]string{"no_proxy": "10.0.0.0/8"}, true},
		{"cidr mismatch", "http://11.1.2.3/input", map[string]string{"no_proxy": "10.0.0.0/8"}, false},
		{"plain ip match", "http://10.1.2.3/input", map[string]string{"no_proxy": "10.1.2.3"}, true},
		{"plain ip mismatch", "http://10.1.2.4/input", map[string]string{"no_proxy": "10.1.2.3"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := map[string]string{"HTTP_PROXY": "http://up:1"}
			for k, v := range tc.env {
				env[k] = v
			}
			syncProxyEnv(t, env)
			req, err := http.NewRequest(http.MethodPost, tc.url, nil)
			if err != nil {
				t.Fatalf("NewRequest: %v", err)
			}
			got, err := proxyForRequest(req)
			if err != nil {
				t.Fatalf("proxyForRequest: %v", err)
			}
			if bypassed := got == nil; bypassed != tc.want {
				t.Fatalf("proxyForRequest = %v (bypassed=%v), want bypassed=%v", got, bypassed, tc.want)
			}
		})
	}
}

// TestParseProxyURL: requests accepts scheme-less proxy addresses and prepends
// "http://" (utils.prepend_scheme_if_needed); a proxy with no host is rejected.
func TestParseProxyURL(t *testing.T) {
	cases := []struct {
		in      string
		want    string
		wantErr bool
	}{
		{"proxy.example:8080", "http://proxy.example:8080", false},
		{"//proxy.example:8080", "http://proxy.example:8080", false},
		{"http://proxy.example:8080", "http://proxy.example:8080", false},
		{"socks5://proxy.example:1080", "socks5://proxy.example:1080", false},
		{"proxy.example", "http://proxy.example", false},
		{"http://", "", true},
	}
	for _, tc := range cases {
		u, err := parseProxyURL(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Fatalf("parseProxyURL(%q) = %v, want an error", tc.in, u)
			}
			continue
		}
		if err != nil {
			t.Fatalf("parseProxyURL(%q): %v", tc.in, err)
		}
		if u.String() != tc.want {
			t.Fatalf("parseProxyURL(%q) = %q, want %q", tc.in, u.String(), tc.want)
		}
	}
}

// TestProxyConfiguredWithoutScheme: a recording proxy configured WITHOUT a
// scheme (HTTP_PROXY=host:port) is still used for delivery.
func TestProxyConfiguredWithoutScheme(t *testing.T) {
	var proxied int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&proxied, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer proxy.Close()
	schemeLess := strings.TrimPrefix(proxy.URL, "http://")
	syncProxyEnv(t, map[string]string{"HTTP_PROXY": schemeLess})

	cfg := baseConfig()
	cfg["logplex_log_input_url"] = "http://logplex.example.invalid/input"
	inst, err := New(sender.ConstructorArgs{Name: "x", Config: cfg})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	s := inst.Transport.(*Sender)
	if err := s.SendMessages(context.Background(), [][]byte{[]byte(`{"MESSAGE":"m"}`)}, "c"); err != nil {
		t.Fatalf("SendMessages through a scheme-less proxy: %v", err)
	}
	if atomic.LoadInt32(&proxied) != 1 {
		t.Fatalf("proxy hits = %d, want 1 (scheme-less proxy address)", proxied)
	}
}

// TestProxyUsedForLoopbackTarget is the non-vacuous loopback control: unlike
// net/http.ProxyFromEnvironment, the requests-faithful selector does NOT bypass
// loopback, so a loopback target IS proxied when no no_proxy matches.
func TestProxyUsedForLoopbackTarget(t *testing.T) {
	var proxied, direct int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&proxied, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer proxy.Close()
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&direct, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()
	syncProxyEnv(t, map[string]string{"HTTP_PROXY": proxy.URL})

	s := productionSender(t, target.URL, 2)
	if err := s.SendMessages(context.Background(), [][]byte{[]byte(`{"MESSAGE":"m"}`)}, "c"); err != nil {
		t.Fatalf("SendMessages: %v", err)
	}
	if atomic.LoadInt32(&proxied) != 1 || atomic.LoadInt32(&direct) != 0 {
		t.Fatalf("proxied/direct = %d/%d, want 1/0 (loopback must be proxied)", proxied, direct)
	}
}

// TestNoProxyBypass: no_proxy matching a non-loopback target bypasses the
// proxy, so the request is attempted directly (and fails DNS); the proxy sees
// nothing. The negative control (proxy IS used without no_proxy) is covered by
// TestProxyUsedForLoopbackTarget.
func TestNoProxyBypass(t *testing.T) {
	var proxied int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&proxied, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer proxy.Close()
	syncProxyEnv(t, map[string]string{"HTTP_PROXY": proxy.URL, "NO_PROXY": "logplex.example.invalid"})

	cfg := baseConfig()
	cfg["logplex_log_input_url"] = "http://logplex.example.invalid/input"
	inst, err := New(sender.ConstructorArgs{Name: "x", Config: cfg})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	s := inst.Transport.(*Sender)
	// The direct attempt cannot resolve the non-loopback host, so an error is
	// expected; what matters is that the proxy was not used.
	if err := s.SendMessages(context.Background(), [][]byte{[]byte(`{"MESSAGE":"m"}`)}, "c"); err == nil {
		t.Fatal("SendMessages against an unresolvable non-loopback host succeeded; want a direct-connect error")
	}
	if atomic.LoadInt32(&proxied) != 0 {
		t.Fatalf("proxy hits = %d, want 0 (no_proxy bypass)", proxied)
	}
}

// productionSender builds a Sender with the production HTTP client pointing
// at url and the given request timeout (seconds).
func productionSender(t *testing.T, url string, timeoutSeconds float64) *Sender {
	t.Helper()
	cfg := baseConfig()
	cfg["logplex_log_input_url"] = url
	cfg["logplex_request_timeout"] = timeoutSeconds
	inst, err := New(sender.ConstructorArgs{Name: "x", Config: cfg})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	s := inst.Transport.(*Sender)
	s.now = func() time.Time { return time.Unix(0, 0) }
	return s
}

// parseFrames splits a clean logplex body into its length-prefixed frames.
func parseFrames(t *testing.T, body []byte) []frameFields {
	t.Helper()
	var frames []frameFields
	for len(body) > 0 {
		sp := strings.IndexByte(string(body), ' ')
		if sp < 0 {
			t.Fatalf("body tail %q has no length prefix", body)
		}
		n, err := strconv.Atoi(string(body[:sp]))
		if err != nil {
			t.Fatalf("length prefix %q: %v", body[:sp], err)
		}
		content := body[sp+1 : sp+1+n]
		frames = append(frames, parseContent(t, string(content)))
		body = body[sp+1+n:]
	}
	return frames
}

// mustCutPrefix strips a frame's length prefix, returning its content.
func mustCutPrefix(t *testing.T, frame string) string {
	t.Helper()
	_, rest, ok := strings.Cut(frame, " ")
	if !ok {
		t.Fatalf("frame %q has no length prefix", frame)
	}
	return rest
}

// basicAuth decodes a Basic Authorization header.
func basicAuth(h http.Header) (user, pass string, ok bool) {
	req := &http.Request{Header: h}
	return req.BasicAuth()
}
