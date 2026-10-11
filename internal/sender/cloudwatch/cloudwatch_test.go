// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package cloudwatch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Aiven-Open/journalpump/internal/sender"
)

// recordingLogger captures log lines for assertions about diagnostics.
type recordingLogger struct {
	mu     sync.Mutex
	lines  []string
	errors []string
}

func (l *recordingLogger) Printf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func (l *recordingLogger) Errorf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	line := fmt.Sprintf(format, args...)
	l.lines = append(l.lines, line)
	l.errors = append(l.errors, line)
}

func (l *recordingLogger) has(fragment string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, line := range l.lines {
		if strings.Contains(line, fragment) {
			return true
		}
	}
	return false
}

func (l *recordingLogger) count(fragment string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, line := range l.lines {
		if strings.Contains(line, fragment) {
			n++
		}
	}
	return n
}

// fakeStats records unexpected_exception emissions.
type fakeStats struct {
	mu   sync.Mutex
	errs []error
	tags []map[string]string
}

func (f *fakeStats) UnexpectedException(ex error, _ string, tags map[string]string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.errs = append(f.errs, ex)
	f.tags = append(f.tags, tags)
}

func (f *fakeStats) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.errs)
}

// fakeStep is one scripted client interaction. op is one of "create_group",
// "create_stream", "describe", "put".
type fakeStep struct {
	op     string
	err    error
	token  string // describe
	found  bool   // describe
	next   string // put
	status int    // put
}

type putCall struct {
	token  string
	events []logEvent
}

// fakeLogs is the hand-written client-interface fake (aws-sdk-go-v2 ships no
// generated mocks). It fails the test on an out-of-order or unscripted call.
type fakeLogs struct {
	t     *testing.T
	steps []fakeStep
	idx   int
	mu    sync.Mutex
	puts  []putCall
	order *[]string
}

func (f *fakeLogs) step(op string) fakeStep {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.order != nil {
		*f.order = append(*f.order, op)
	}
	if f.idx >= len(f.steps) {
		f.t.Fatalf("unexpected %s: no scripted step", op)
	}
	s := f.steps[f.idx]
	if s.op != op {
		f.t.Fatalf("step %d: called %s, want %s", f.idx, op, s.op)
	}
	f.idx++
	return s
}

func (f *fakeLogs) CreateLogGroup(context.Context, string) error { return f.step("create_group").err }

func (f *fakeLogs) CreateLogStream(context.Context, string, string) error {
	return f.step("create_stream").err
}

func (f *fakeLogs) UploadSequenceToken(context.Context, string, string) (string, bool, error) {
	s := f.step("describe")
	return s.token, s.found, s.err
}

func (f *fakeLogs) PutLogEvents(_ context.Context, _, _, token string, events []logEvent) (string, int, error) {
	s := f.step("put")
	f.mu.Lock()
	f.puts = append(f.puts, putCall{token: token, events: events})
	f.mu.Unlock()
	return s.next, s.status, s.err
}

// newTestSender builds a sender around a scripted fake and a recording stats
// sink.
func newTestSender(t *testing.T, steps []fakeStep) (*Sender, *fakeLogs, *fakeStats) {
	t.Helper()
	logs := &fakeLogs{t: t, steps: steps}
	stats := &fakeStats{}
	s := newSender(senderConfig{group: "group", stream: "stream", seconds: 0.3}, logs, sender.ConstructorArgs{})
	s.stats = stats
	return s, logs, stats
}

// msg builds a raw JSON line with an optional REALTIME_TIMESTAMP literal.
func msg(body string) []byte { return []byte(body) }

func putStep(token string, status int) fakeStep {
	return fakeStep{op: "put", next: token, status: status}
}

// TestSenderPutSequence ports test_awscloudwatch_sender: init recovers the
// sequence token, then puts carry nextSequenceToken and per-event ms
// timestamps, and a failing put re-inits (recovering the token) while
// emitting unexpected_exception.
func TestSenderPutSequence(t *testing.T) {
	const (
		tsLine = `{"REALTIME_TIMESTAMP": 1590581737.308352}`
		helo   = `{"MESSAGE": "Hello World!"}`
	)
	initOK := []fakeStep{
		{op: "create_group", err: &apiError{Code: "ResourceAlreadyExistsException"}},
		{op: "create_stream"},
		{op: "describe", token: "token", found: true},
	}
	steps := append([]fakeStep{}, initOK...)
	steps = append(steps, putStep("token1", 200))
	steps = append(steps, putStep("token2", 200))
	steps = append(steps, putStep("token2", 200))
	// Throttling put → re-init (recovering "token"), returns error.
	steps = append(steps, fakeStep{op: "put", err: &apiError{Code: "ThrottlingException", Message: "rate"}})
	steps = append(steps, initOK...)
	// InternalFailure put → re-init too.
	steps = append(steps, fakeStep{op: "put", err: &apiError{Code: "InternalFailure", Message: "boom"}})
	steps = append(steps, initOK...)

	s, logs, stats := newTestSender(t, steps)

	// Init + first put.
	if err := s.SendMessages(context.Background(), [][]byte{msg(tsLine)}, ""); err != nil {
		t.Fatalf("send 1: %v", err)
	}
	if s.nextSequenceToken == nil || *s.nextSequenceToken != "token1" {
		t.Fatalf("nextSequenceToken = %v, want token1", s.nextSequenceToken)
	}
	if !s.connected {
		t.Fatal("sender not connected after init")
	}

	// Second put uses the wall clock when REALTIME_TIMESTAMP is absent.
	s.now = func() time.Time { return time.Unix(123456789, 0) }
	if err := s.SendMessages(context.Background(), [][]byte{msg(helo)}, ""); err != nil {
		t.Fatalf("send 2: %v", err)
	}
	if *s.nextSequenceToken != "token2" {
		t.Fatalf("nextSequenceToken = %q, want token2", *s.nextSequenceToken)
	}

	// Third put carries the numeric REALTIME_TIMESTAMP.
	if err := s.SendMessages(context.Background(), [][]byte{msg(tsLine)}, ""); err != nil {
		t.Fatalf("send 3: %v", err)
	}

	logs.mu.Lock()
	puts := append([]putCall(nil), logs.puts...)
	logs.mu.Unlock()
	if len(puts) != 3 {
		t.Fatalf("put calls = %d, want 3", len(puts))
	}
	if puts[0].token != "token" {
		t.Errorf("first put token = %q, want the token recovered during init", puts[0].token)
	}
	if got := puts[1].events[0].Timestamp; got != 123456789000 {
		t.Errorf("wall-clock timestamp = %d, want 123456789000", got)
	}
	if got := puts[1].events[0].Message; got != helo {
		t.Errorf("message = %q, want %q", got, helo)
	}
	if puts[1].token != "token1" {
		t.Errorf("second put token = %q, want token1", puts[1].token)
	}
	if got := puts[2].events[0].Timestamp; got != 1590581737308 {
		t.Errorf("REALTIME timestamp = %d, want 1590581737308", got)
	}

	// A throttled put fails, re-inits (recovers the token) and leaves the
	// sender connected.
	if err := s.SendMessages(context.Background(), [][]byte{msg(tsLine)}, ""); err == nil {
		t.Fatal("throttled send returned nil, want error")
	}
	if !s.connected {
		t.Fatal("sender not connected after re-init following throttling")
	}
	if *s.nextSequenceToken != "token" {
		t.Fatalf("nextSequenceToken = %q, want the recovered token", *s.nextSequenceToken)
	}

	// An InternalFailure put also re-inits and stays connected.
	if err := s.SendMessages(context.Background(), [][]byte{msg(tsLine)}, ""); err == nil {
		t.Fatal("InternalFailure send returned nil, want error")
	}
	if !s.connected {
		t.Fatal("sender not connected after re-init following InternalFailure")
	}
	if stats.count() != 2 {
		t.Errorf("unexpected_exception emissions = %d, want 2", stats.count())
	}
}

// TestSenderInit ports test_awscloudwatch_sender_init: AccessDenied exhausts
// MAX_INIT_TRIES → InitializationError; AccessDenied and ServiceUnavailable
// each recover on retry.
func TestSenderInit(t *testing.T) {
	t.Run("access-denied-exhausts", func(t *testing.T) {
		steps := []fakeStep{
			{op: "create_group", err: &apiError{Code: "AccessDeniedException"}},
			{op: "create_group", err: &apiError{Code: "AccessDeniedException"}},
			{op: "create_group", err: &apiError{Code: "AccessDeniedException"}},
		}
		s, _, _ := newTestSender(t, steps)
		err := s.SendMessages(context.Background(), [][]byte{msg(`{"REALTIME_TIMESTAMP": 1.5}`)}, "")
		var initErr *sender.InitializationError
		if !errors.As(err, &initErr) {
			t.Fatalf("err = %v, want InitializationError", err)
		}
	})

	t.Run("access-denied-then-success", func(t *testing.T) {
		steps := []fakeStep{
			{op: "create_group", err: &apiError{Code: "AccessDeniedException"}},
			{op: "create_group"},
			{op: "create_stream"},
			{op: "describe", token: "token", found: true},
			putStep("token2", 200),
		}
		s, _, _ := newTestSender(t, steps)
		if err := s.SendMessages(context.Background(), [][]byte{msg(`{"REALTIME_TIMESTAMP": 1.5}`)}, ""); err != nil {
			t.Fatalf("send: %v", err)
		}
		if !s.connected {
			t.Fatal("sender not connected after retry")
		}
	})

	t.Run("service-unavailable-then-success", func(t *testing.T) {
		steps := []fakeStep{
			{op: "create_group", err: &apiError{Code: "ServiceUnavailable"}},
			{op: "create_group"},
			{op: "create_stream"},
			{op: "describe", token: "token", found: true},
			putStep("token2", 200),
		}
		s, _, _ := newTestSender(t, steps)
		if err := s.SendMessages(context.Background(), [][]byte{msg(`{"REALTIME_TIMESTAMP": 1.5}`)}, ""); err != nil {
			t.Fatalf("send: %v", err)
		}
		if !s.connected {
			t.Fatal("sender not connected after retry")
		}
	})
}

// TestSenderInitPaginateAccessDenied ports
// test_awscloudwatch_sender_init_paginate: an AccessDenied embedded in the
// describe paginator is logged, NOT raised, and leaves the sender
// disconnected.
func TestSenderInitPaginateAccessDenied(t *testing.T) {
	steps := []fakeStep{
		{op: "create_group"},
		{op: "create_stream"},
		{op: "describe", err: &apiError{Code: "AccessDeniedException"}},
	}
	s, _, stats := newTestSender(t, steps)
	if err := s.initLogGroup(context.Background()); err != nil {
		t.Fatalf("initLogGroup returned %v, want nil (Python logs and continues)", err)
	}
	if s.connected {
		t.Fatal("sender connected after a paginator AccessDenied")
	}
	if stats.count() != 1 {
		t.Errorf("unexpected_exception emissions = %d, want 1", stats.count())
	}
}

// TestConfigRequiresGroupAndStream ports the ValueError for missing names.
func TestConfigRequiresGroupAndStream(t *testing.T) {
	if _, err := parseConfig(map[string]any{}); err == nil {
		t.Fatal("missing group/stream accepted")
	}
	cfg, err := parseConfig(map[string]any{
		"aws_cloudwatch_log_group":  "g",
		"aws_cloudwatch_log_stream": "s",
	})
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.group != "g" || cfg.stream != "s" || cfg.seconds != defaultMaxSendInterval {
		t.Fatalf("cfg = %+v", cfg)
	}
}

// TestEventTimestampNonNumeric asserts a truthy non-numeric
// REALTIME_TIMESTAMP fails like Python's TypeError.
func TestEventTimestampNonNumeric(t *testing.T) {
	s, _, _ := newTestSender(t, nil)
	if _, err := s.event([]byte(`{"REALTIME_TIMESTAMP": "1590581737.3"}`)); err == nil {
		t.Fatal("string REALTIME_TIMESTAMP accepted, want error")
	}
	if _, err := s.event([]byte(`{"REALTIME_TIMESTAMP": 0}`)); err != nil {
		t.Fatalf("falsy timestamp should fall back to wall clock: %v", err)
	}
}

// newOrderedSender builds a sender whose backoff observer records ordering
// relative to client calls, plus a recording logger.
func newOrderedSender(t *testing.T, steps []fakeStep, order *[]string) (*Sender, *fakeLogs, *recordingLogger) {
	t.Helper()
	logs := &fakeLogs{t: t, steps: steps, order: order}
	s := newSender(senderConfig{group: "group", stream: "stream", seconds: 0.3}, logs, sender.ConstructorArgs{Log: &recordingLogger{}})
	s.stats = &fakeStats{}
	s.SetBackoffObserver(func(context.Context) { *order = append(*order, "backoff") })
	return s, logs, s.log.(*recordingLogger)
}

// TestFailedPutBacksOffBeforeReinit pins Python's ordering on a failed put:
// mark_disconnected → log → stats → _backoff → _init_log_group
// (aws_cloudwatch.py:125-129). The backoff must be recorded BEFORE the
// re-init's create call.
func TestFailedPutBacksOffBeforeReinit(t *testing.T) {
	var order []string
	steps := []fakeStep{
		{op: "create_group"},
		{op: "create_stream"},
		{op: "describe", token: "token", found: true},
		putStep("token", 200),
		{op: "put", err: &apiError{Code: "ThrottlingException", Message: "rate"}},
		{op: "create_group"},
		{op: "create_stream"},
		{op: "describe", token: "token", found: true},
	}
	s, _, _ := newOrderedSender(t, steps, &order)

	if err := s.SendMessages(context.Background(), [][]byte{msg(`{"REALTIME_TIMESTAMP": 1.5}`)}, ""); err != nil {
		t.Fatalf("first send: %v", err)
	}
	order = nil // ignore init ordering
	if err := s.SendMessages(context.Background(), [][]byte{msg(`{"REALTIME_TIMESTAMP": 1.5}`)}, ""); err == nil {
		t.Fatal("throttled send returned nil")
	}
	// Expected tail: put, backoff, create_group, create_stream, describe.
	if len(order) < 2 || order[0] != "put" || order[1] != "backoff" {
		t.Fatalf("ordering = %v, want put then backoff", order)
	}
	idx := -1
	for i, op := range order {
		if op == "create_group" {
			idx = i
			break
		}
	}
	if idx == -1 || idx < 2 {
		t.Fatalf("create_group not after backoff: %v", order)
	}
}

// TestFailedPutErrorPreservedWhenReinitAlsoFails: a re-init failure must NOT
// replace the put failure as the returned cause; it is reported separately.
func TestFailedPutErrorPreservedWhenReinitAlsoFails(t *testing.T) {
	steps := []fakeStep{
		{op: "create_group"},
		{op: "create_stream"},
		{op: "describe", token: "token", found: true},
		putStep("token", 200),
		{op: "put", err: &apiError{Code: "ThrottlingException", Message: "rate"}},
		{op: "create_group", err: &apiError{Code: "AccessDeniedException"}},
		{op: "create_group", err: &apiError{Code: "AccessDeniedException"}},
		{op: "create_group", err: &apiError{Code: "AccessDeniedException"}},
	}
	s, _, log := newOrderedSender(t, steps, new([]string))
	if err := s.SendMessages(context.Background(), [][]byte{msg(`{"REALTIME_TIMESTAMP": 1.5}`)}, ""); err != nil {
		t.Fatalf("first send: %v", err)
	}
	err := s.SendMessages(context.Background(), [][]byte{msg(`{"REALTIME_TIMESTAMP": 1.5}`)}, "")
	if err == nil {
		t.Fatal("send returned nil")
	}
	// The put failure is the returned cause, NOT the re-init's
	// InitializationError.
	var initErr *sender.InitializationError
	if errors.As(err, &initErr) {
		t.Fatalf("returned error is InitializationError; the put failure was replaced: %v", err)
	}
	var api *apiError
	if !errors.As(err, &api) || api.Code != "ThrottlingException" {
		t.Fatalf("returned cause = %v, want the ThrottlingException put failure", err)
	}
	var backoff *sender.BackoffError
	if !errors.As(err, &backoff) {
		t.Fatalf("returned error is not a BackoffError: %v", err)
	}
	if !log.has("re-initialization") {
		t.Errorf("re-init failure not logged separately; errors = %v", log.errors)
	}
}

// TestSuccessfulPutSyncsConnected: a describe that does not find the stream
// leaves the sender disconnected, but a successful put connects it so the
// NEXT batch does not re-init (Python mark_sent → mark_connected).
func TestSuccessfulPutSyncsConnected(t *testing.T) {
	steps := []fakeStep{
		{op: "create_group"},
		{op: "create_stream"},
		{op: "describe", found: false}, // stream not found → disconnected
		putStep("tokenA", 200),
		putStep("tokenB", 200), // second batch: NO re-init
	}
	s, logs, _ := newTestSender(t, steps)
	if err := s.SendMessages(context.Background(), [][]byte{msg(`{"REALTIME_TIMESTAMP": 1.5}`)}, ""); err != nil {
		t.Fatalf("send 1: %v", err)
	}
	if !s.connected {
		t.Fatal("sender not connected after a successful put")
	}
	if s.nextSequenceToken == nil || *s.nextSequenceToken != "tokenA" {
		t.Fatalf("nextSequenceToken = %v, want tokenA", s.nextSequenceToken)
	}
	// The second send must not re-init (the fake would Fatal on an
	// unscripted create/describe).
	if err := s.SendMessages(context.Background(), [][]byte{msg(`{"REALTIME_TIMESTAMP": 1.5}`)}, ""); err != nil {
		t.Fatalf("send 2: %v", err)
	}
	logs.mu.Lock()
	puts := len(logs.puts)
	logs.mu.Unlock()
	if puts != 2 {
		t.Fatalf("put calls = %d, want 2", puts)
	}
}

// TestErrorLogUsesPercentQ guards item 5: the send-error diagnostic must not
// emit a Go %!r(...) artifact.
func TestErrorLogUsesPercentQ(t *testing.T) {
	steps := []fakeStep{
		{op: "create_group"},
		{op: "create_stream"},
		{op: "describe", token: "token", found: true},
		{op: "put", err: &apiError{Code: "ThrottlingException", Message: "rate exceeded"}},
		{op: "create_group"},
		{op: "create_stream"},
		{op: "describe", token: "token", found: true},
	}
	s, _, log := newOrderedSender(t, steps, new([]string))
	if err := s.SendMessages(context.Background(), [][]byte{msg(`{"REALTIME_TIMESTAMP": 1.5}`)}, ""); err == nil {
		t.Fatal("send returned nil")
	}
	if !log.has(`"ThrottlingException"`) || !log.has(`"rate exceeded"`) {
		t.Errorf("send-error log missing the quoted code/message: %v", log.errors)
	}
	if log.has("%!") {
		t.Errorf("send-error log contains a Go formatting artifact: %v", log.errors)
	}
}

// TestTimestampTruthiness pins Python's `message.get(...) or time.time()`
// truthiness table.
func TestTimestampTruthiness(t *testing.T) {
	fixed := time.Unix(1600000000, 0)
	tests := []struct {
		name    string
		value   any
		want    int64
		wantErr bool
	}{
		{"nil falls back", nil, 1600000000000, false},
		{"false falls back", false, 1600000000000, false},
		{"true is 1", true, 1000, false},
		{"zero number falls back", json.Number("0"), 1600000000000, false},
		{"number", json.Number("1590581737.308352"), 1590581737308, false},
		{"float zero falls back", float64(0), 1600000000000, false},
		{"float", float64(2), 2000, false},
		{"empty string falls back", "", 1600000000000, false},
		{"non-empty string errors", "1590581737.3", 0, true},
		{"empty list falls back", []any{}, 1600000000000, false},
		{"non-empty list errors", []any{1}, 0, true},
		{"empty map falls back", map[string]any{}, 1600000000000, false},
		{"non-empty map errors", map[string]any{"a": 1}, 0, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, _, _ := newTestSender(t, nil)
			s.now = func() time.Time { return fixed }
			got, err := s.timestamp(tc.value)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("timestamp(%v) = %d, want error", tc.value, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("timestamp(%v): %v", tc.value, err)
			}
			if got != tc.want {
				t.Errorf("timestamp(%v) = %d, want %d", tc.value, got, tc.want)
			}
		})
	}
}

// TestChunkEventsBoundaries pins the correct-the-Python batch guard.
func TestChunkEventsBoundaries(t *testing.T) {
	mk := func(n int) []logEvent {
		events := make([]logEvent, n)
		for i := range events {
			events[i] = logEvent{Timestamp: 1, Message: "x"}
		}
		return events
	}

	if chunks := chunkEvents(mk(maxEventsPerRequest)); len(chunks) != 1 {
		t.Fatalf("10000 events → %d chunks, want 1", len(chunks))
	}
	chunks := chunkEvents(mk(maxEventsPerRequest + 1))
	if len(chunks) != 2 || len(chunks[0]) != maxEventsPerRequest || len(chunks[1]) != 1 {
		t.Fatalf("10001 events → sizes %v, want [10000 1]", chunkSizes(chunks))
	}
	// An oversized first event becomes its own chunk; the rest follow.
	big := []logEvent{{Timestamp: 1, Message: strings.Repeat("a", maxRequestBytes)}, {Timestamp: 2, Message: "small"}}
	chunks = chunkEvents(big)
	if len(chunks) != 2 || len(chunks[0]) != 1 || len(chunks[1]) != 1 {
		t.Fatalf("oversized first event → sizes %v, want [1 1]", chunkSizes(chunks))
	}
}

func chunkSizes(chunks [][]logEvent) []int {
	out := make([]int, len(chunks))
	for i, c := range chunks {
		out[i] = len(c)
	}
	return out
}

// TestSendMessagesChunksLargeBatch drives the guard through SendMessages with
// the sequence token chained across chunks.
func TestSendMessagesChunksLargeBatch(t *testing.T) {
	steps := []fakeStep{
		{op: "create_group"},
		{op: "create_stream"},
		{op: "describe", token: "token", found: true},
		putStep("next1", 200),
		putStep("next2", 200),
	}
	s, logs, _ := newTestSender(t, steps)
	batch := make([][]byte, maxEventsPerRequest+1)
	for i := range batch {
		batch[i] = msg(`{"REALTIME_TIMESTAMP": 1.5}`)
	}
	if err := s.SendMessages(context.Background(), batch, ""); err != nil {
		t.Fatalf("send: %v", err)
	}
	logs.mu.Lock()
	puts := append([]putCall(nil), logs.puts...)
	logs.mu.Unlock()
	if len(puts) != 2 {
		t.Fatalf("put calls = %d, want 2", len(puts))
	}
	if len(puts[0].events) != maxEventsPerRequest || len(puts[1].events) != 1 {
		t.Fatalf("chunk sizes = %d,%d, want 10000,1", len(puts[0].events), len(puts[1].events))
	}
	if puts[1].token != "next1" {
		t.Errorf("second chunk token = %q, want next1", puts[1].token)
	}
}

// TestLoopSingleBackoffOnFailedPut runs the sender through the REAL
// sender.Loop and asserts the failed-put backoff happens exactly ONCE (the
// sender's, before re-init) rather than the loop adding a second wait.
// Non-vacuous: without the BackoffError skip the loop logs a second
// "sleeping for" line per failure.
func TestLoopSingleBackoffOnFailedPut(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		steps := []fakeStep{
			{op: "create_group"},
			{op: "create_stream"},
			{op: "describe", token: "token", found: true},
			{op: "put", err: &apiError{Code: "ThrottlingException", Message: "rate"}},
			{op: "create_group"},
			{op: "create_stream"},
			{op: "describe", token: "token", found: true},
			putStep("token2", 200),
		}
		logs := &fakeLogs{t: t, steps: steps}
		s := newSender(senderConfig{group: "group", stream: "stream", seconds: 0.3}, logs, sender.ConstructorArgs{})
		s.stats = &fakeStats{}
		log := &recordingLogger{}
		inst := sender.Instance{Transport: s, MaxSendInterval: 300 * time.Millisecond, Type: "AWSCloudWatchSender"}
		in := make(chan sender.Item, 4)
		loop := sender.NewLoop(sender.LoopConfig{Name: "cloudwatch-loop", Log: log}, inst, in)

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { _ = loop.Run(ctx); close(done) }()

		in <- sender.Item{Payload: msg(`{"REALTIME_TIMESTAMP": 1.5}`), Cursor: "c0"}
		time.Sleep(3 * time.Second) // failure + backoff + retry
		synctest.Wait()
		st := loop.State()
		cancel()
		<-done

		if st.Sent.Count != 1 {
			t.Fatalf("sent count = %d, want 1 (the batch is acked on the retry)", st.Sent.Count)
		}
		if got := log.count("sleeping for"); got != 1 {
			t.Fatalf("backoff waits = %d, want exactly 1 (sender's, before re-init)", got)
		}
	})
}

// TestLoopChunksLargeBatch drives an oversized batch through the REAL
// sender.Loop: the framework slices by bytes only, so a stream of tiny entries
// reaches the sender as one >10,000 batch, which the sender MUST split into
// API-legal PutLogEvents calls.
func TestLoopChunksLargeBatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		steps := []fakeStep{
			{op: "create_group"}, {op: "create_stream"},
			{op: "describe", token: "token", found: true},
			putStep("n1", 200),
			putStep("n2", 200),
		}
		logs := &fakeLogs{t: t, steps: steps}
		s := newSender(senderConfig{group: "group", stream: "stream", seconds: 0.3}, logs, sender.ConstructorArgs{})
		s.stats = &fakeStats{}
		inst := sender.Instance{Transport: s, MaxSendInterval: 300 * time.Millisecond, Type: "AWSCloudWatchSender"}
		in := make(chan sender.Item, maxEventsPerRequest+1)
		loop := sender.NewLoop(sender.LoopConfig{Name: "cloudwatch-loop"}, inst, in)

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { _ = loop.Run(ctx); close(done) }()

		payload := msg(`{"REALTIME_TIMESTAMP": 1.5}`)
		for i := 0; i <= maxEventsPerRequest; i++ {
			in <- sender.Item{Payload: payload, Cursor: fmt.Sprintf("c%d", i)}
		}
		time.Sleep(2 * time.Second)
		synctest.Wait()
		st := loop.State()
		cancel()
		<-done

		logs.mu.Lock()
		puts := append([]putCall(nil), logs.puts...)
		logs.mu.Unlock()
		if len(puts) != 2 {
			t.Fatalf("put calls = %d, want 2 (10000 + 1)", len(puts))
		}
		if len(puts[0].events) != maxEventsPerRequest || len(puts[1].events) != 1 {
			t.Fatalf("chunk sizes = %d,%d, want 10000,1", len(puts[0].events), len(puts[1].events))
		}
		if st.Sent.Count != int64(maxEventsPerRequest+1) {
			t.Fatalf("sent count = %d, want %d", st.Sent.Count, maxEventsPerRequest+1)
		}
	})
}
