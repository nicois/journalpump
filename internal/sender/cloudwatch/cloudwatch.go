// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

// Package cloudwatch implements the AWS CloudWatch Logs sender: the port of
// journalpump/senders/aws_cloudwatch.py (plan Task 25). It drives an
// aws-sdk-go-v2 cloudwatchlogs client behind a small client interface
// (logsClient) so the Stubber-based Python cases port to hand-written
// fake-client tests (aws-sdk-go-v2 ships no generated mocks; thread B's
// sanctioned custom-fake case).
//
// TRANSPORT NOTE: unlike the GCL sender (Task 26), this sender does NOT use
// internal/httpx — the AWS SDK owns request transport, retries, proxy
// selection (HTTP_PROXY/HTTPS_PROXY/NO_PROXY) and TLS. The httpx helper is
// not applicable here.
package cloudwatch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Aiven-Open/journalpump/internal/sender"
	"github.com/Aiven-Open/journalpump/internal/statsd"
	"github.com/Aiven-Open/journalpump/internal/tagged"
)

// defaultMaxSendInterval is aws_cloudwatch.py:15's
// config.get("max_send_interval", 0.3).
const defaultMaxSendInterval = 0.3

// maxInitTries is aws_cloudwatch.py:8's MAX_INIT_TRIES.
const maxInitTries = 3

// CloudWatch PutLogEvents API limits (correctness guard, NOT reproduced
// from Python). Python's size-based slicing
// (base.py:get_and_send_messages, `batch_size >= max_batch_size`) packs
// batches up to 1 MiB with NO event-count cap, so a stream of tiny entries
// can produce a request the API rejects (10,000 events per PutLogEvents,
// 1 MiB per request). The Go sender chunks the batch and never builds a
// request the API would reject — a sanctioned correct-the-Python bug fix
// (charter §2). maxEventsPerRequest is the documented API limit; the size
// bound mirrors the framework's 1 MiB slicing limit.
const (
	maxEventsPerRequest = 10000
	maxRequestBytes     = 1048576
)

// perEventOverhead approximates the serialized per-event byte cost
// (timestamp + structural JSON) used by the size guard, matching the
// framework's message_overhead accounting.
const perEventOverhead = 26

// Fixed error codes the init path special-cases (aws_cloudwatch.py:51,64,93).
const (
	codeAlreadyExists = "ResourceAlreadyExistsException"
	codeAccessDenied  = "AccessDeniedException"
)

// logEvent is one CloudWatch Logs input event (timestamp in ms + raw JSON
// line).
type logEvent struct {
	Timestamp int64
	Message   string
}

// logsClient is the seam over the botocore "logs" client. Its error values
// carry a botocore-style Error Code/Message (see apiError).
type logsClient interface {
	CreateLogGroup(ctx context.Context, group string) error
	CreateLogStream(ctx context.Context, group, stream string) error
	// UploadSequenceToken pages describe_log_streams looking for stream and
	// returns its uploadSequenceToken. found is false when the stream is not
	// present (Python's found_stream_metadata empty).
	UploadSequenceToken(ctx context.Context, group, stream string) (token string, found bool, err error)
	// PutLogEvents returns the next sequence token and the response HTTP
	// status code.
	PutLogEvents(ctx context.Context, group, stream, sequenceToken string, events []logEvent) (next string, status int, err error)
}

// apiError is a botocore ClientError-shaped error: Error.Code, Error.Message
// and the response HTTP status code. The SDK adapter maps smithy API errors
// onto it; tests script it directly.
type apiError struct {
	Code       string
	Message    string
	StatusCode int
}

// Error implements error.
func (e *apiError) Error() string {
	if e.Message == "" {
		return e.Code
	}
	return e.Code + ": " + e.Message
}

// exceptionStats is the statsd seam for the sender's own
// unexpected_exception emissions (Python's self.stats.unexpected_exception).
// The pump's *statsd.Client satisfies it in production; tests inject a fake.
type exceptionStats interface {
	UnexpectedException(ex error, where string, tags map[string]string)
}

// taggedTags is the base-tag seam (Python self.make_tags()).
type taggedTags interface {
	MakeTags(tags map[string]string) map[string]string
}

// errorLogger is the OPTIONAL level-aware logging capability. Python logs
// these diagnostics at ERROR; a Printf-only logger falls back to Printf.
type errorLogger interface {
	Errorf(format string, args ...any)
}

// Sender is the CloudWatch Logs transport.
type Sender struct {
	logs   logsClient
	group  string
	stream string

	// nextSequenceToken mirrors Python self._next_sequence_token (nil until
	// known).
	nextSequenceToken *string
	// connected mirrors Python self._connected: gates _init_log_group. It is
	// reported to the loop through the ConnectionReporter seam.
	connected bool

	stats   exceptionStats
	tags    taggedTags
	log     sender.Logger
	now     func() time.Time
	backoff func(context.Context)

	onConnected    func()
	onDisconnected func(error)
}

// config is the sender's decoded config block.
type senderConfig struct {
	group   string
	stream  string
	seconds float64
}

// New is the wiring-map constructor for output_type "aws_cloudwatch"
// (journalpump/senders/__init__.py). It requires the log group and stream
// names (Python's ValueError) and builds the production aws-sdk-go-v2 client
// from the optional region/credential keys.
func New(args sender.ConstructorArgs) (sender.Instance, error) {
	cfg, err := parseConfig(args.Config)
	if err != nil {
		return sender.Instance{}, err
	}
	client, err := newSDKClient(args.Config)
	if err != nil {
		return sender.Instance{}, err
	}
	s := newSender(cfg, client, args)
	return sender.Instance{
		Transport:       s,
		MaxSendInterval: time.Duration(cfg.seconds * float64(time.Second)),
		Type:            "AWSCloudWatchSender",
	}, nil
}

// parseConfig decodes the config block, applying Python's defaults and the
// required-name check.
func parseConfig(cfg map[string]any) (senderConfig, error) {
	out := senderConfig{seconds: defaultMaxSendInterval}
	if raw, present := cfg["max_send_interval"]; present {
		parsed, ok := numberValue(raw)
		if !ok {
			return senderConfig{}, fmt.Errorf("max_send_interval: %v is not a number", raw)
		}
		out.seconds = parsed
	}
	group, err := optionalString(cfg, "aws_cloudwatch_log_group")
	if err != nil {
		return senderConfig{}, err
	}
	stream, err := optionalString(cfg, "aws_cloudwatch_log_stream")
	if err != nil {
		return senderConfig{}, err
	}
	if group == "" || stream == "" {
		// aws_cloudwatch.py:25 raises ValueError.
		return senderConfig{}, errors.New("AWS CloudWatch log group and stream names need to be configured")
	}
	out.group, out.stream = group, stream
	return out, nil
}

// newSender assembles a Sender around an already-built client (tests pass a
// fake directly).
func newSender(cfg senderConfig, client logsClient, args sender.ConstructorArgs) *Sender {
	return &Sender{
		logs:   client,
		group:  cfg.group,
		stream: cfg.stream,
		stats:  statSink(args.Stats),
		tags:   tagSink(args.Tags),
		log:    args.Log,
		now:    time.Now,
	}
}

// statSink adapts the pump stats client to the emission seam (nil-safe).
func statSink(stats *statsd.Client) exceptionStats {
	if stats == nil {
		return nil
	}
	return stats
}

// tagSink adapts the pump base-tag set to the make_tags seam (nil-safe).
func tagSink(tags *tagged.Tagged) taggedTags {
	if tags == nil {
		return nil
	}
	return tags
}

// optionalString reads an optional string config key, rejecting a present
// non-string value.
func optionalString(cfg map[string]any, key string) (string, error) {
	raw, present := cfg[key]
	if !present || raw == nil {
		return "", nil
	}
	s, ok := raw.(string)
	if !ok {
		return "", fmt.Errorf("%s: %v is not a string", key, raw)
	}
	return s, nil
}

// numberValue renders a decoded JSON number as float64.
func numberValue(value any) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case json.Number:
		f, err := v.Float64()
		return f, err == nil
	default:
		return 0, false
	}
}

// SendMessages implements sender.Sender: ensure the log group/stream exist
// (recovering the sequence token), build one event per message, and
// PutLogEvents. Ports aws_cloudwatch.py:103-141.
func (s *Sender) SendMessages(ctx context.Context, batch [][]byte, _ string) error {
	if !s.connected {
		if err := s.initLogGroup(ctx); err != nil {
			return err
		}
	}
	events := make([]logEvent, 0, len(batch))
	for _, msg := range batch {
		ev, err := s.event(msg)
		if err != nil {
			return err
		}
		events = append(events, ev)
	}
	// Correctness guard (NOT Python): never build a PutLogEvents request the
	// API rejects. Chunk by the 10,000-event and 1 MiB limits, chaining the
	// sequence token across chunks. An event that alone exceeds the size
	// bound becomes its own chunk (it cannot be made smaller).
	chunks := chunkEvents(events)
	for _, chunk := range chunks {
		token := ""
		if s.nextSequenceToken != nil {
			token = *s.nextSequenceToken
		}
		next, status, err := s.logs.PutLogEvents(ctx, s.group, s.stream, token, chunk)
		if err != nil {
			// aws_cloudwatch.py:120-135: mark disconnected, log, emit
			// unexpected_exception, BACK OFF, then re-init — and only then
			// return False. The backoff happens HERE, before the re-init, so
			// the sender wraps the error in sender.BackoffError to tell the
			// shared loop not to wait a second time after the return. A
			// re-init failure is reported separately and does NOT replace the
			// put failure, which stays the returned cause.
			s.setConnected(false, err)
			s.logSendError(err)
			s.unexpected(err)
			s.backoffNow(ctx)
			if initErr := s.initLogGroup(ctx); initErr != nil {
				s.logErrorf("AWS CloudWatch re-initialization after a failed put also failed: %v", initErr)
			}
			return &sender.BackoffError{Err: err}
		}
		if status < 200 || status >= 300 {
			// A non-2xx status without an error is not expected from the SDK,
			// but Python would fall through and return False.
			return fmt.Errorf("cloudwatch: put_log_events returned status %d", status)
		}
		s.nextSequenceToken = &next
	}
	// Python's mark_sent (base.py:220-224) calls mark_connected, so a
	// successful delivery connects the sender even when the init describe
	// did not find the stream. Sync the local gate so the next batch does
	// not re-init (the shared loop's markSent already handles its own state).
	s.connected = true
	return nil
}

// chunkEvents splits events into PutLogEvents requests under the API's
// 10,000-event and 1 MiB limits (correctness guard; see the constants).
func chunkEvents(events []logEvent) [][]logEvent {
	var chunks [][]logEvent
	var current []logEvent
	var size int
	for _, ev := range events {
		evSize := len(ev.Message) + perEventOverhead
		if len(current) > 0 && (len(current) >= maxEventsPerRequest || size+evSize > maxRequestBytes) {
			chunks = append(chunks, current)
			current = nil
			size = 0
		}
		current = append(current, ev)
		size += evSize
	}
	if len(current) > 0 {
		chunks = append(chunks, current)
	}
	return chunks
}

// event builds one log event from a raw JSON message line
// (aws_cloudwatch.py:107-112).
func (s *Sender) event(msg []byte) (logEvent, error) {
	if !utf8.Valid(msg) {
		// Python: msg.decode("utf8") raises UnicodeDecodeError.
		return logEvent{}, errors.New("cloudwatch: message is not valid UTF-8")
	}
	raw := string(msg)
	var decoded map[string]any
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&decoded); err != nil {
		return logEvent{}, fmt.Errorf("cloudwatch: decode payload: %w", err)
	}
	ts, err := s.timestamp(decoded["REALTIME_TIMESTAMP"])
	if err != nil {
		return logEvent{}, err
	}
	return logEvent{Timestamp: ts, Message: raw}, nil
}

// timestamp ports `int((message.get("REALTIME_TIMESTAMP") or time.time()) *
// 1000.0)`. Python truthiness decides the fallback: None/false/0/0.0/""/
// []/{} fall back to the wall clock; True is the integer 1; a non-empty
// string/list/dict is truthy but not a number, so `* 1000.0` raises
// TypeError.
func (s *Sender) timestamp(value any) (int64, error) {
	var seconds float64
	switch v := value.(type) {
	case nil:
		seconds = s.wallClock()
	case bool:
		if v {
			seconds = 1
		} else {
			seconds = s.wallClock()
		}
	case json.Number:
		f, err := v.Float64()
		if err != nil {
			return 0, fmt.Errorf("cloudwatch: REALTIME_TIMESTAMP: %w", err)
		}
		if f == 0 {
			seconds = s.wallClock()
		} else {
			seconds = f
		}
	case float64:
		if v == 0 {
			seconds = s.wallClock()
		} else {
			seconds = v
		}
	case float32:
		if v == 0 {
			seconds = s.wallClock()
		} else {
			seconds = float64(v)
		}
	case string:
		if v == "" {
			seconds = s.wallClock()
		} else {
			return 0, fmt.Errorf("cloudwatch: REALTIME_TIMESTAMP %q is not a number", v)
		}
	case []any:
		if len(v) == 0 {
			seconds = s.wallClock()
		} else {
			return 0, errors.New("cloudwatch: REALTIME_TIMESTAMP is a non-empty list, not a number")
		}
	case map[string]any:
		if len(v) == 0 {
			seconds = s.wallClock()
		} else {
			return 0, errors.New("cloudwatch: REALTIME_TIMESTAMP is a non-empty object, not a number")
		}
	default:
		return 0, fmt.Errorf("cloudwatch: REALTIME_TIMESTAMP %v is not a number", value)
	}
	return int64(math.Trunc(seconds * 1000.0)), nil
}

// wallClock renders the current time as float seconds (Python time.time()).
func (s *Sender) wallClock() float64 {
	return float64(s.now().UnixNano()) / 1e9
}

// initLogGroup ports aws_cloudwatch.py:36-101: up to MAX_INIT_TRIES attempts
// to create the log group and stream (ignoring already-exists), then a
// describe_log_streams pager to recover the upload sequence token. Returns a
// wrapped InitializationError when AccessDenied persists (base.py
// SenderInitializationError), the raw error for other persisted ClientErrors,
// and nil for a paginator failure (Python logs it and leaves the sender
// disconnected without raising).
func (s *Sender) initLogGroup(ctx context.Context) error {
	s.logf("Initializing AWS CloudWatch")
	if err := s.createGroupAndStream(ctx); err != nil {
		return err
	}
	return s.recoverSequenceToken(ctx)
}

// createGroupAndStream runs the create-with-retry loop.
func (s *Sender) createGroupAndStream(ctx context.Context) error {
	attempts := maxInitTries
	for {
		attempts--
		err := s.createOnce(ctx)
		if err == nil {
			return nil
		}
		s.unexpected(err)
		code := errorCode(err)
		if code == codeAccessDenied {
			s.logErrorf("Access denied exception when trying to create log group and stream in AWS Cloudwatch.")
		} else {
			s.logErrorf("AWS ClientError: %v", err)
		}
		if attempts == 0 {
			if code == codeAccessDenied {
				return &sender.InitializationError{Err: err}
			}
			return err
		}
		s.backoffNow(ctx)
	}
}

// createOnce creates the log group then the stream, ignoring
// already-exists on each (aws_cloudwatch.py:47-58).
func (s *Sender) createOnce(ctx context.Context) error {
	if err := s.logs.CreateLogGroup(ctx, s.group); err != nil && errorCode(err) != codeAlreadyExists {
		return err
	}
	if err := s.logs.CreateLogStream(ctx, s.group, s.stream); err != nil && errorCode(err) != codeAlreadyExists {
		return err
	}
	return nil
}

// recoverSequenceToken pages describe_log_streams for the stream's upload
// sequence token (aws_cloudwatch.py:81-101).
func (s *Sender) recoverSequenceToken(ctx context.Context) error {
	token, found, err := s.logs.UploadSequenceToken(ctx, s.group, s.stream)
	if err == nil && found {
		s.nextSequenceToken = &token
		s.setConnected(true, nil)
		return nil
	}
	if err != nil {
		s.unexpected(err)
		if errorCode(err) == codeAccessDenied {
			s.logErrorf("Access denied exception when trying to create log group and stream in AWS Cloudwatch.")
		} else {
			s.logErrorf("AWS ClientError: %v", err)
		}
		s.backoffNow(ctx)
	}
	s.setConnected(false, err)
	s.logErrorf("Failed to init sender. AWS CloudWatch logs could not update sequence token.")
	return nil
}

// MaintenanceOperations implements sender.Sender; CloudWatch has none (no
// base.py override).
func (s *Sender) MaintenanceOperations(context.Context) error { return nil }

// SetConnectionObserver implements sender.ConnectionReporter: the loop owns
// the shared connected state and backoff exponent, so the sender reports its
// init transitions through it.
func (s *Sender) SetConnectionObserver(connected func(), disconnected func(error)) {
	s.onConnected = connected
	s.onDisconnected = disconnected
}

// SetBackoffObserver implements sender.Backoffer: the sender's init retry
// waits share the loop's cancellable jittered backoff (Python self._backoff).
func (s *Sender) SetBackoffObserver(backoff func(context.Context)) { s.backoff = backoff }

// setConnected reports a connection transition to the loop and updates the
// local gate.
func (s *Sender) setConnected(connected bool, err error) {
	s.connected = connected
	if connected {
		if s.onConnected != nil {
			s.onConnected()
		}
		return
	}
	if s.onDisconnected != nil {
		s.onDisconnected(err)
	}
}

// backoffNow waits out one loop backoff when the loop injected the seam
// (unit tests without a loop simply skip the wait).
func (s *Sender) backoffNow(ctx context.Context) {
	if s.backoff != nil {
		s.backoff(ctx)
	}
}

// logSendError ports the two send-error log lines (aws_cloudwatch.py:126/132).
func (s *Sender) logSendError(err error) {
	var api *apiError
	if errors.As(err, &api) {
		s.logErrorf("Error sending events %q: %q", api.Code, api.Message)
		return
	}
	s.logErrorf("Unexpected exception during send to AWS CloudWatch: %v", err)
}

// unexpected emits stats.unexpected_exception with the sender's base tags
// plus app=journalpump (aws_cloudwatch.py:60/92/127/133).
func (s *Sender) unexpected(err error) {
	if s.stats == nil {
		return
	}
	var tags map[string]string
	if s.tags != nil {
		tags = s.tags.MakeTags(map[string]string{"app": "journalpump"})
	} else {
		tags = map[string]string{"app": "journalpump"}
	}
	s.stats.UnexpectedException(err, "sender", tags)
}

func (s *Sender) logf(format string, args ...any) {
	if s.log != nil {
		s.log.Printf(format, args...)
	}
}

func (s *Sender) logErrorf(format string, args ...any) {
	if s.log == nil {
		return
	}
	if l, ok := s.log.(errorLogger); ok {
		l.Errorf(format, args...)
		return
	}
	s.log.Printf(format, args...)
}

// errorCode extracts the botocore Error.Code from an error.
func errorCode(err error) string {
	var api *apiError
	if errors.As(err, &api) {
		return api.Code
	}
	return ""
}
