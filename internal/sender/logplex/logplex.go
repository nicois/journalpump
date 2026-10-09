// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

// Package logplex implements the logplex sender: the port of
// journalpump/senders/logplex.py (plan Task 21). It is a fire-and-forget
// HTTP sender — the response status is ignored, the batch is always marked
// sent, and there is no retry or disconnect on an HTTP error response.
//
// SANCTIONED DEVIATION (charter §2/D6): Python's format_msg f-strings the
// ENCODED packet, so the wire carries the bytes REPR (b'...') of each frame
// and every length prefix is followed by length+3 characters. Go sends the
// CLEAN RFC frame instead; the length prefix counts the frame's own bytes.
// The Python ground-truth capture is therefore compared STRUCTURALLY (field
// values via the repr-stripped frame list), never byte-for-byte.
package logplex

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Aiven-Open/journalpump/internal/sender"
)

// defaultMaxSendInterval is logplex.py:11's
// config.get("max_send_interval", 5.0).
const defaultMaxSendInterval = 5.0

// defaultRequestTimeout is logplex.py:13's
// config.get("logplex_request_timeout", 2) (seconds).
const defaultRequestTimeout = 2.0

// Fixed frame fields (logplex.py:16-17, 26): PRI is always 190, and the
// msgid and structured-data slots are the literal "-".
const (
	pri            = "190"
	msgID          = "-"
	structuredData = "-"
)

// doer is the HTTP client seam (Python's requests.Session); tests inject a
// recording fake.
type doer interface {
	Do(req *http.Request) (*http.Response, error)
}

// Sender posts batches to the logplex input URL (logplex.py LogplexSender).
type Sender struct {
	url    string
	token  string
	client doer
	now    func() time.Time
	log    sender.Logger
}

// New is the wiring-map constructor for output_type "logplex"
// (journalpump/senders/__init__.py). Like Python's LogplexSender.__init__ it
// requires logplex_log_input_url and logplex_token (config[...] KeyErrors)
// and calls mark_connected() at construction (ConnectOnInit), so the sender
// starts connected.
func New(args sender.ConstructorArgs) (sender.Instance, error) {
	seconds := defaultMaxSendInterval
	if raw, present := args.Config["max_send_interval"]; present {
		parsed, ok := numberSeconds(raw)
		if !ok {
			return sender.Instance{}, fmt.Errorf("max_send_interval: %v is not a number", raw)
		}
		seconds = parsed
	}
	timeout := defaultRequestTimeout
	if raw, present := args.Config["logplex_request_timeout"]; present {
		parsed, ok := numberSeconds(raw)
		if !ok {
			return sender.Instance{}, fmt.Errorf("logplex_request_timeout: %v is not a number", raw)
		}
		timeout = parsed
	}
	rawURL, present := args.Config["logplex_log_input_url"]
	if !present {
		// Python: config["logplex_log_input_url"] raises KeyError inside
		// LogplexSender.__init__.
		return sender.Instance{}, errors.New("config key 'logplex_log_input_url' missing")
	}
	url, ok := rawURL.(string)
	if !ok {
		return sender.Instance{}, fmt.Errorf("logplex_log_input_url: %v is not a string", rawURL)
	}
	rawToken, present := args.Config["logplex_token"]
	if !present {
		return sender.Instance{}, errors.New("config key 'logplex_token' missing")
	}
	token, ok := rawToken.(string)
	if !ok {
		return sender.Instance{}, fmt.Errorf("logplex_token: %v is not a string", rawToken)
	}
	s := &Sender{
		url:    url,
		token:  token,
		client: newHTTPClient(timeout),
		now:    time.Now,
		log:    args.Log,
	}
	return sender.Instance{
		Transport: s,
		// logplex.py:18 calls mark_connected() at the end of __init__.
		ConnectOnInit:   true,
		MaxSendInterval: time.Duration(seconds * float64(time.Second)),
		Type:            "LogplexSender",
	}, nil
}

// newHTTPClient builds the production HTTP client: the per-request timeout
// (Python's timeout=self.request_timeout) and a TLS config with verification
// DISABLED (Python's verify=False — deliberate, spec §2/D6).
func newHTTPClient(timeoutSeconds float64) *http.Client {
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true, //nolint:gosec // verify=False is the ported Python behavior (spec §2)
		},
	}
	return &http.Client{
		Timeout:   time.Duration(timeoutSeconds * float64(time.Second)),
		Transport: transport,
	}
}

// formatMsg ports logplex.py:20-29 into the CLEAN frame (sanctioned
// deviation D6): "<190>1 <format-now> localhost <token> localhost - -<MESSAGE>"
// prefixed by its byte length. The hostname/pid lookups keep Python's
// `entry.get("_HOSTNAME", "localhost")` shape; because the journal processor
// strips leading underscores before the sender sees a payload
// (journalpump.py:708-714), those keys never match and the values are always
// the literal "localhost" (the named quirk).
func (s *Sender) formatMsg(msg []byte) (string, error) {
	var m map[string]any
	if err := json.Unmarshal(msg, &m); err != nil {
		// Python: json.loads(msg.decode("utf8")) raises on bad JSON.
		return "", fmt.Errorf("logplex sender: decode payload: %w", err)
	}
	hostname := "localhost"
	if v, present := m["_HOSTNAME"]; present {
		hostname = pyStr(v)
	}
	pid := "localhost"
	if v, present := m["_PID"]; present {
		pid = pyStr(v)
	}
	message, ok := m["MESSAGE"].(string)
	if !ok {
		// Python: entry["MESSAGE"] raises KeyError when absent, and str +=
		// raises TypeError on a non-string value.
		return "", errors.New("logplex sender: MESSAGE missing or not a string")
	}
	// Python: datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%S+00:00 ")
	// — the FORMAT time, not the journal timestamp.
	timestamp := s.now().UTC().Format("2006-01-02T15:04:05") + "+00:00 "
	pkt := "<190>1 " + timestamp + hostname + " " + s.token + " " + pid + " " + msgID + " " + structuredData + message
	// Python writes len(encoded_pkt) as the prefix (the content length); the
	// clean frame has exactly that many bytes, so the number is preserved.
	return strconv.Itoa(len(pkt)) + " " + pkt, nil
}

// SendMessages implements sender.Sender: format the whole batch, POST it
// once, and IGNORE the response status (logplex.py:31-48). A transport-level
// error is returned for the shared loop's failure path; an HTTP error status
// is not.
func (s *Sender) SendMessages(ctx context.Context, batch [][]byte, _ string) error {
	var body strings.Builder
	for _, msg := range batch {
		frame, err := s.formatMsg(msg)
		if err != nil {
			return err
		}
		body.WriteString(frame)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, strings.NewReader(body.String()))
	if err != nil {
		return err
	}
	req.SetBasicAuth("token", s.token)
	req.Header.Set("Content-Type", "application/logplex-1")
	req.Header.Set("Logplex-Msg-Count", strconv.Itoa(len(batch)))
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	// Fire-and-forget: the status code is not inspected (Python never calls
	// raise_for_status and always marks the batch sent).
	_ = resp.Body.Close()
	return nil
}

// MaintenanceOperations implements sender.Sender; logplex has none (no
// base.py override).
func (s *Sender) MaintenanceOperations(context.Context) error { return nil }

// pyStr mirrors Python's str(value) for the f-string interpolation of a
// possibly non-string _HOSTNAME/_PID (unreachable through the journal
// processor, kept for exact parity).
func pyStr(v any) string {
	if v == nil {
		return "None"
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

// numberSeconds renders a decoded JSON number as float seconds, rejecting
// anything non-numeric.
func numberSeconds(value any) (float64, bool) {
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
