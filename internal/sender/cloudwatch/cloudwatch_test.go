// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package cloudwatch

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Aiven-Open/journalpump/internal/sender"
)

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
}

func (f *fakeLogs) step(op string) fakeStep {
	f.mu.Lock()
	defer f.mu.Unlock()
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
