// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

// Package gcl implements the google_cloud_logging sender: the port of
// journalpump/senders/google_cloud_logging.py (plan Task 26).
//
// It writes batches through the Cloud Logging v2 API
// (google.golang.org/api/logging/v2) with an INJECTABLE HTTP transport:
// the generated client is handed an *http.Client built on the shared
// internal/httpx helper, so tests drive it against an httptest server and
// production talks to logging.googleapis.com. The httpx transport uses
// logger's own proxy policy (GCL's Python client is
// googleapiclient.build → google-auth-httplib2 wrapping httplib2.Http,
// NOT requests), supplied as httpx.Httplib2Proxy.
//
// Credentials follow Python: a config-provided service-account JSON is
// parsed with google.CredentialsFromJSON and its project_id OVERRIDES the
// configured project id; otherwise application default credentials are
// found. The resulting oauth2 token source is wrapped over the httpx base
// transport so token fetches use the same proxy policy.
//
// Ported behavior: the 250 KiB / 200 KiB log-entry quota with the
// MESSAGE-truncation / DEFAULT-replacement logic, the journald-priority →
// severity map, the 26-character timestamp + "Z" truncation, MESSAGE
// JSON-parse into jsonPayload, the generic_node resource, the
// projects/<project>/logs/<log_id> logName and the optimistic-reconnect
// failure path (see the §2 charter note for the one residual difference).
package gcl

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/logging/v2"
	"google.golang.org/api/option"

	"github.com/Aiven-Open/journalpump/internal/httpx"
	"github.com/Aiven-Open/journalpump/internal/sender"
)

// Ported defaults and limits (google_cloud_logging.py).
const (
	// _LOG_ENTRY_QUOTA = 250 * 1024. "A bit on the safe side, not exactly
	// 256KB but this is an approximation anyway" (Cloud Logging quotas).
	logEntryQuota = 250 * 1024
	// _MAX_MESSAGE_SIZE = 200 * 1024. Leaves ~56K headroom for the other
	// LogEntry fields.
	maxMessageSize = 200 * 1024
	// The replacement MESSAGE when a log entry cannot be made to fit.
	// Byte-for-byte the Python string (note: "256K", not the actual quota).
	defaultTruncatedMessage = "Log entry can't be logged because its size is greater than GCP logging quota of 256K"
	// The generic_node monitored resource type every entry is written with.
	genericNode = "generic_node"
	// The default idle-flush interval (config.get("max_send_interval", 0.3)).
	defaultMaxSendInterval = 300 * time.Millisecond
	// initTimeout bounds credential resolution at construction.
	initTimeout = 30 * time.Second
	// truncationSuffix is appended to a truncated MESSAGE.
	truncationSuffix = "[MESSAGE TRUNCATED]"
)

// severityMapping ports _SEVERITY_MAPPING (journald PRIORITY → Cloud
// Logging severity). An absent/undefined priority maps to "DEFAULT".
var severityMapping = map[int64]string{
	7: "DEBUG",
	6: "INFO",
	5: "NOTICE",
	4: "WARNING",
	3: "ERROR",
	2: "CRITICAL",
	1: "ALERT",
	0: "EMERGENCY",
}

// writer is the injected Cloud Logging seam: the one API call the sender
// makes. The production implementation wraps *logging.Service; tests
// inject a fake (and, for the transport/wire tests, a real generated
// client pointed at an httptest server via option.WithEndpoint).
type writer interface {
	WriteLogEntries(ctx context.Context, req *logging.WriteLogEntriesRequest) error
}

// serviceWriter is the production writer over the generated logging/v2
// client. The generated Write call takes the request context, so a batch
// in flight is cancellable at shutdown (no sarama/gorilla-class
// cancellation gap).
type serviceWriter struct{ svc *logging.Service }

func (w serviceWriter) WriteLogEntries(ctx context.Context, req *logging.WriteLogEntriesRequest) error {
	_, err := w.svc.Entries.Write(req).Context(ctx).Do()
	return err
}

// exceptionStats is the statsd seam for the sender's own
// unexpected_exception emissions (Python self.stats.unexpected_exception).
// The pump's *statsd.Client satisfies it; tests inject a fake.
type exceptionStats interface {
	UnexpectedException(ex error, where string, tags map[string]string)
}

// Sender is the Google Cloud Logging transport.
type Sender struct {
	log            sender.Logger
	stats          exceptionStats
	projectID      string
	logID          string
	resourceLabels map[string]string
	writer         writer

	// onConnected / onDisconnected / backoff are the loop-owned seams
	// (ConnectionReporter / Backoffer); tests that build the Sender
	// directly leave them nil.
	onConnected    func()
	onDisconnected func(error)
	backoff        func(context.Context)
}

type discardLogger struct{}

func (discardLogger) Printf(string, ...any) {}

// errorLogger is the OPTIONAL level-aware logging capability (the
// production journald sink provides Errorf; plain loggers fall back to
// Printf). Python logs the send failure at ERROR (log.exception).
type errorLogger interface {
	Errorf(format string, args ...any)
}

func (s *Sender) logError(format string, args ...any) {
	if l, ok := s.log.(errorLogger); ok {
		l.Errorf(format, args...)
		return
	}
	s.log.Printf(format, args...)
}

// unexpected emits stats.unexpected_exception; Python passes the base tags
// plus app=journalpump. A nil stats client skips the emission.
func (s *Sender) unexpected(err error) {
	if s.stats != nil {
		s.stats.UnexpectedException(err, "sender", map[string]string{"app": "journalpump"})
	}
}

// SetConnectionObserver satisfies sender.ConnectionReporter: the loop owns
// the shared connected state and backoff exponent. The failure path
// re-marks connected (Python's optimistic reconnect), which also resets the
// exponent.
func (s *Sender) SetConnectionObserver(connected func(), disconnected func(error)) {
	s.onConnected = connected
	s.onDisconnected = disconnected
}

func (s *Sender) disconnected(err error) {
	if s.onDisconnected != nil {
		s.onDisconnected(err)
	}
}

func (s *Sender) connected() {
	if s.onConnected != nil {
		s.onConnected()
	}
}

// SetBackoffObserver satisfies sender.Backoffer: the failure path performs
// the shared jittered backoff before returning, so the loop must not wait a
// second time (the returned error is a sender.BackoffError).
func (s *Sender) SetBackoffObserver(backoff func(context.Context)) { s.backoff = backoff }

func (s *Sender) waitBackoff(ctx context.Context) {
	if s.backoff != nil {
		s.backoff(ctx)
	}
}

var (
	_ sender.Sender             = (*Sender)(nil)
	_ sender.ConnectionReporter = (*Sender)(nil)
	_ sender.Backoffer          = (*Sender)(nil)
)

// options is the decoded config block.
type options struct {
	projectID       string
	logID           string
	resourceLabels  map[string]string
	serviceAccount  []byte
	maxSendInterval time.Duration
}

// parseConfig decodes the sender's config keys and applies defaults.
// google_cloud_logging_project_id and google_cloud_logging_log_id are
// required (Python indexes the dict directly).
func parseConfig(raw map[string]any) (options, error) {
	opts := options{maxSendInterval: defaultMaxSendInterval}
	project, err := requiredString(raw, "google_cloud_logging_project_id")
	if err != nil {
		return options{}, err
	}
	opts.projectID = project
	logID, err := requiredString(raw, "google_cloud_logging_log_id")
	if err != nil {
		return options{}, err
	}
	opts.logID = logID

	if v, present := raw["google_cloud_logging_resource_labels"]; present && v != nil {
		labels, ok := v.(map[string]any)
		if !ok {
			return options{}, fmt.Errorf("google_cloud_logging_resource_labels: %v is not an object", v)
		}
		opts.resourceLabels = map[string]string{}
		for k, lv := range labels {
			str, err := labelString(lv)
			if err != nil {
				return options{}, fmt.Errorf("google_cloud_logging_resource_labels[%q]: %w", k, err)
			}
			opts.resourceLabels[k] = str
		}
	}

	if v, present := raw["google_service_account_credentials"]; present && v != nil {
		obj, ok := v.(map[string]any)
		if !ok {
			return options{}, fmt.Errorf("google_service_account_credentials: %v is not an object", v)
		}
		data, err := json.Marshal(obj)
		if err != nil {
			return options{}, fmt.Errorf("google_service_account_credentials: %w", err)
		}
		opts.serviceAccount = data
	}

	if v, present := raw["max_send_interval"]; present {
		seconds, ok := numberSeconds(v)
		if !ok {
			return options{}, fmt.Errorf("max_send_interval: %v is not a number", v)
		}
		if seconds <= 0 {
			return options{}, fmt.Errorf("max_send_interval: %v must be positive", v)
		}
		opts.maxSendInterval = time.Duration(seconds * float64(time.Second))
	}
	return opts, nil
}

func requiredString(raw map[string]any, key string) (string, error) {
	v, present := raw[key]
	if !present || v == nil {
		return "", fmt.Errorf("%s hasn't been defined", key)
	}
	str, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("%s: %v is not a string", key, v)
	}
	return str, nil
}

// labelString renders one resource-label value the way Python's json.dumps
// would render it, so a scalar label survives the string-typed
// MonitoredResource.Labels map. Non-scalars are refused: Cloud Logging
// resource labels are strings, and Python would send an unusable value.
func labelString(v any) (string, error) {
	switch t := v.(type) {
	case string:
		return t, nil
	case json.Number:
		return t.String(), nil
	case float64:
		return strconv64(t), nil
	case bool:
		if t {
			return "true", nil
		}
		return "false", nil
	default:
		return "", fmt.Errorf("%T is not a scalar", v)
	}
}

func strconv64(f float64) string {
	b, _ := json.Marshal(f)
	return string(b)
}

// New is the wiring-map constructor for output_type
// "google_cloud_logging".
func New(args sender.ConstructorArgs) (sender.Instance, error) {
	opts, err := parseConfig(args.Config)
	if err != nil {
		return sender.Instance{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), initTimeout)
	defer cancel()

	creds, err := resolveCredentials(ctx, opts)
	if err != nil {
		return sender.Instance{}, &sender.InitializationError{Err: err}
	}
	if len(opts.serviceAccount) > 0 {
		// google_cloud_logging.py:52-53 — the service account's project_id
		// overrides the configured one.
		opts.projectID = creds.ProjectID
	}

	// The base transport carries the httplib2-compatible proxy policy; the
	// oauth2 token source wraps it so token fetches share the policy.
	base := httpx.NewClient(httpx.ClientOptions{Proxy: httpx.Httplib2Proxy})
	client := &http.Client{Transport: &oauth2.Transport{Source: creds.TokenSource, Base: base.Transport}}

	svc, err := logging.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		return sender.Instance{}, &sender.InitializationError{Err: err}
	}
	s := newSender(args.Log, statsFromArgs(args), opts, serviceWriter{svc: svc})
	return sender.Instance{
		Transport:       s,
		MaxSendInterval: opts.maxSendInterval,
		Type:            "GoogleCloudLoggingSender",
		// google_cloud_logging.py:66 — the constructor calls mark_connected.
		ConnectOnInit: true,
	}, nil
}

func statsFromArgs(args sender.ConstructorArgs) exceptionStats {
	if args.Stats == nil {
		return nil
	}
	return args.Stats
}

// resolveCredentials parses the configured service-account JSON, else
// finds application default credentials, requesting the log-write scope.
func resolveCredentials(ctx context.Context, opts options) (*google.Credentials, error) {
	scope := logging.LoggingWriteScope
	if len(opts.serviceAccount) > 0 {
		// CredentialsFromJSON is deprecated (it does not validate the
		// credential type); the type-specific entry point is required to load
		// exactly a service account — matching Python's
		// Credentials.from_service_account_info.
		return google.CredentialsFromJSONWithType(ctx, opts.serviceAccount, google.ServiceAccount, scope)
	}
	return google.FindDefaultCredentials(ctx, scope)
}

// newSender builds the transport around an already-constructed writer
// (used by New and by tests).
func newSender(log sender.Logger, stats exceptionStats, opts options, w writer) *Sender {
	if log == nil {
		log = discardLogger{}
	}
	return &Sender{
		log:            log,
		stats:          stats,
		projectID:      opts.projectID,
		logID:          opts.logID,
		resourceLabels: opts.resourceLabels,
		writer:         w,
	}
}

// SendMessages ports send_messages. A successful write returns nil (the
// loop advances the cursor); any failure takes the shared failure path.
func (s *Sender) SendMessages(ctx context.Context, batch [][]byte, _ string) error {
	req, err := s.buildRequest(batch)
	if err != nil {
		return s.fail(ctx, err)
	}
	if err := s.writer.WriteLogEntries(ctx, req); err != nil {
		return s.fail(ctx, err)
	}
	return nil
}

// fail is the shared except path (google_cloud_logging.py:119-131): mark
// disconnected, log at ERROR, emit unexpected_exception, wait out the
// shared backoff, optimistically re-mark connected, and report the batch as
// failed. The error is wrapped in sender.BackoffError because the backoff
// already happened here.
func (s *Sender) fail(ctx context.Context, err error) error {
	s.disconnected(err)
	s.logError("Client error during send to Google Cloud Logging: %v", err)
	s.unexpected(err)
	s.waitBackoff(ctx)
	s.connected()
	return &sender.BackoffError{Err: err}
}

// MaintenanceOperations implements sender.Sender; the GCL sender has no
// maintenance work (Python inherits the no-op base).
func (s *Sender) MaintenanceOperations(context.Context) error { return nil }

// buildRequest assembles one WriteLogEntriesRequest.
func (s *Sender) buildRequest(batch [][]byte) (*logging.WriteLogEntriesRequest, error) {
	req := &logging.WriteLogEntriesRequest{
		LogName:  fmt.Sprintf("projects/%s/logs/%s", s.projectID, s.logID),
		Resource: &logging.MonitoredResource{Type: genericNode},
	}
	if s.resourceLabels != nil {
		req.Resource.Labels = s.resourceLabels
	}
	for _, message := range batch {
		entry, err := s.buildEntry(message)
		if err != nil {
			return nil, err
		}
		req.Entries = append(req.Entries, entry)
	}
	return req, nil
}

// buildEntry ports one iteration of the send_messages build loop.
func (s *Sender) buildEntry(message []byte) (*logging.LogEntry, error) {
	msg, err := parseMessage(message)
	if err != nil {
		return nil, err
	}
	// Quota handling runs BEFORE the timestamp/PRIORITY pops, exactly as
	// Python (the recomputed size still includes them).
	if len(message) > logEntryQuota {
		msg, err = truncateMessage(msg)
		if err != nil {
			return nil, err
		}
	}

	timestamp, hasTimestamp := msg["timestamp"]
	delete(msg, "timestamp")
	priority, hasPriority := msg["PRIORITY"]
	delete(msg, "PRIORITY")

	if m, present := msg["MESSAGE"]; present {
		// contextlib.suppress(json.JSONDecodeError): a MESSAGE that is not
		// valid JSON stays as the raw string; a non-string MESSAGE makes
		// Python's json.loads raise TypeError, which is NOT suppressed.
		str, ok := m.(string)
		if !ok {
			return nil, fmt.Errorf("MESSAGE is not a string: %T", m)
		}
		if parsed, ok := jsonLoads(str); ok {
			msg["MESSAGE"] = parsed
		}
	}

	payload, err := marshalJSON(msg)
	if err != nil {
		return nil, err
	}
	entry := &logging.LogEntry{JsonPayload: payload}
	if hasTimestamp && timestamp != nil {
		str, ok := timestamp.(string)
		if !ok {
			return nil, fmt.Errorf("timestamp is not a string: %T", timestamp)
		}
		runes := []rune(str)
		if len(runes) > 26 {
			runes = runes[:26]
		}
		entry.Timestamp = string(runes) + "Z"
	}
	if hasPriority && priority != nil {
		entry.Severity = severityFor(priority)
	}
	return entry, nil
}

// truncateMessage ports the oversized-entry branch: truncate a string
// MESSAGE to 200 KiB (codepoints, as Python slices str), append the marker,
// and fall back to a MESSAGE-only DEFAULT entry when even that does not fit
// (or when the entry has no MESSAGE at all).
func truncateMessage(msg map[string]any) (map[string]any, error) {
	if m, present := msg["MESSAGE"]; present {
		str, ok := m.(string)
		if !ok {
			return nil, fmt.Errorf("cannot truncate non-string MESSAGE: %T", m)
		}
		runes := []rune(str)
		if len(runes) > maxMessageSize {
			runes = runes[:maxMessageSize]
		}
		msg["MESSAGE"] = string(runes) + truncationSuffix
		// Python measures json.dumps(msg, ensure_ascii=False).encode("utf-8").
		// This compact marshal omits Python's ", "/": " separators, so the
		// measured size can differ by a couple of bytes per field — the same
		// approximation the Python comment concedes.
		size, err := marshalJSON(msg)
		if err != nil {
			return nil, err
		}
		if len(size) > logEntryQuota {
			return map[string]any{"MESSAGE": defaultTruncatedMessage}, nil
		}
		return msg, nil
	}
	return map[string]any{"MESSAGE": defaultTruncatedMessage}, nil
}

// severityFor ports _SEVERITY_MAPPING lookup: an integer priority (Python
// bool is an int subclass) in 0..7 maps, anything else is "DEFAULT".
func severityFor(v any) string {
	var key int64
	switch p := v.(type) {
	case bool:
		if p {
			key = 1
		}
	case json.Number:
		if !isIntegerLiteral(p.String()) {
			return "DEFAULT"
		}
		i, err := p.Int64()
		if err != nil {
			return "DEFAULT"
		}
		key = i
	default:
		return "DEFAULT"
	}
	if sev, ok := severityMapping[key]; ok {
		return sev
	}
	return "DEFAULT"
}

// parseMessage decodes one wire payload preserving int/float (UseNumber),
// like Python's json.loads.
func parseMessage(message []byte) (map[string]any, error) {
	dec := json.NewDecoder(bytes.NewReader(message))
	dec.UseNumber()
	var msg map[string]any
	if err := dec.Decode(&msg); err != nil {
		return nil, err
	}
	return msg, nil
}

// jsonLoads mirrors json.loads: it succeeds only when the whole string is
// one JSON value (numbers preserved as json.Number).
func jsonLoads(s string) (any, bool) {
	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, false
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, false
	}
	return v, true
}

// marshalJSON renders a value as JSON without Go's HTML escaping (Python
// does not escape <, >, &).
func marshalJSON(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// isIntegerLiteral distinguishes Python ints from floats by the literal
// (json.Number keeps the source spelling).
func isIntegerLiteral(s string) bool {
	return !strings.ContainsAny(s, ".eE")
}

func numberSeconds(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	default:
		return 0, false
	}
}
