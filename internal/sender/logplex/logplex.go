// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

// Package logplex implements the logplex sender: the port of
// journalpump/senders/logplex.py (plan Task 21). It is a fire-and-forget
// HTTP sender — the response STATUS is ignored, so a 4xx/5xx still marks the
// batch sent and does not retry or disconnect.
//
// SANCTIONED DEVIATION (charter §2/D6): Python's format_msg f-strings the
// ENCODED packet, so the wire carries the bytes REPR (b'...') of each frame
// and every length prefix is followed by length+3 characters. Go sends the
// CLEAN RFC frame instead; the length prefix counts the frame's own bytes.
// The Python ground-truth capture is therefore compared STRUCTURALLY (field
// values via the repr-stripped frame list), never byte-for-byte.
//
// SANCTIONED DEVIATION (charter §2, Task 21 review ruling): on a TRANSPORT
// error (dial/read/timeout) Python's uncaught requests exception aborts
// get_and_send_messages, silently drops the drained batch and leaves the
// sender marked connected — silent data loss. Go instead returns the error,
// so the shared loop retains the batch, retries it, and marks the sender
// disconnected. This follows the framework-wide retry semantics the approved
// spec mandates; it is the "Python is incorrect" clause, not an accidental
// divergence.
package logplex

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Aiven-Open/journalpump/internal/journal"
	"github.com/Aiven-Open/journalpump/internal/sender"
)

// defaultMaxSendInterval is logplex.py:11's
// config.get("max_send_interval", 5.0).
const defaultMaxSendInterval = 5.0

// defaultRequestTimeout is logplex.py:13's
// config.get("logplex_request_timeout", 2) (seconds).
const defaultRequestTimeout = 2.0

// defaultSessionTimeout mirrors util.py:get_requests_session's TimeoutAdapter
// default (60 s): requests substitutes the session timeout whenever the
// per-request timeout is FALSY, so logplex_request_timeout = 0 means 60 s,
// not "no timeout".
const defaultSessionTimeout = 60.0

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
		// Python's TimeoutAdapter substitutes the session default only when the
		// per-request timeout is FALSY; a negative number is TRUTHY and is
		// passed through to requests, which raises "the timeout cannot be set
		// to a value less than or equal to 0". Strict-at-load deviation:
		// reject it here instead of at send time.
		if parsed < 0 {
			return sender.Instance{}, fmt.Errorf("logplex_request_timeout: %v cannot be set to a value less than 0", parsed)
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

// resolvedTimeout maps the configured logplex_request_timeout to the
// effective per-connection timeout: requests substitutes the session's 60 s
// default only when the value is falsy — that is, exactly ZERO here, because
// a negative number is truthy in Python and is rejected (see New) rather than
// defaulted. Python's timeout is an INACTIVITY timeout (connect and per-read),
// not a total deadline.
func resolvedTimeout(timeoutSeconds float64) time.Duration {
	if timeoutSeconds == 0 {
		return time.Duration(defaultSessionTimeout * float64(time.Second))
	}
	return time.Duration(timeoutSeconds * float64(time.Second))
}

// newHTTPClient builds the production HTTP client with Python's requests
// semantics:
//
//   - timeout: an INACTIVITY timeout, applied to the dial and to every
//     socket read (see idleTimeoutConn), with 0 → the 60 s session default.
//     There is deliberately NO http.Client.Timeout — that would be a TOTAL
//     request deadline and would abort a slow-but-progressing response.
//   - proxy: the standard environment (HTTP_PROXY/HTTPS_PROXY/NO_PROXY) is
//     honored, like requests.Session.
//   - TLS verification is DISABLED (Python's verify=False — deliberate,
//     spec §2).
func newHTTPClient(timeoutSeconds float64) *http.Client {
	idle := resolvedTimeout(timeoutSeconds)
	dialer := &net.Dialer{Timeout: idle}
	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, err := dialer.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			return &idleTimeoutConn{Conn: conn, idle: idle}, nil
		},
		// Match requests.Session's proxy selection (resolve_proxies +
		// select_proxy over urllib.getproxies()), reading the environment per
		// request — not net/http.ProxyFromEnvironment, whose precedence,
		// ALL_PROXY handling and loopback bypass all differ.
		Proxy: proxyForRequest,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true, //nolint:gosec // verify=False is the ported Python behavior (spec §2)
		},
	}
	return &http.Client{Transport: transport}
}

// idleTimeoutConn resets the read/write deadline before every operation, so
// a stalled peer trips the timeout while a slow-but-progressing transfer does
// not — Python's socket inactivity timeout semantics.
type idleTimeoutConn struct {
	net.Conn
	idle time.Duration
}

func (c *idleTimeoutConn) Read(b []byte) (int, error) {
	_ = c.SetReadDeadline(time.Now().Add(c.idle))
	return c.Conn.Read(b)
}

func (c *idleTimeoutConn) Write(b []byte) (int, error) {
	_ = c.SetWriteDeadline(time.Now().Add(c.idle))
	return c.Conn.Write(b)
}

// proxyForRequest reproduces requests.Session's proxy resolution for one
// request (requests.utils.resolve_proxies + select_proxy over
// urllib.request.getproxies). It deliberately does NOT use
// net/http.ProxyFromEnvironment (httpproxy): that helper gives uppercase
// variables precedence, ignores the ALL_PROXY fallback, and unconditionally
// bypasses loopback addresses — three differences from requests.
func proxyForRequest(req *http.Request) (*url.URL, error) {
	u := req.URL
	if u == nil || u.Hostname() == "" {
		return nil, nil
	}
	if shouldBypassProxies(u) {
		return nil, nil
	}
	proxies := proxyEnvironment()
	proxy := proxies[u.Scheme]
	if proxy == "" {
		proxy = proxies["all"]
	}
	if proxy == "" {
		return nil, nil
	}
	return url.Parse(proxy)
}

// shouldBypassProxies mirrors requests.utils.should_bypass_proxies (minus the
// platform-specific proxy_bypass call, which is a no-op on Linux: requests
// clears no_proxy around it when no explicit no_proxy was supplied). The
// no_proxy lookup is lowercase-first with an uppercase fallback
// (`os.environ.get("no_proxy") or os.environ.get("NO_PROXY")`). Loopback
// targets are NOT special-cased: they are proxied unless no_proxy matches.
func shouldBypassProxies(u *url.URL) bool {
	noProxy := os.Getenv("no_proxy")
	if noProxy == "" {
		noProxy = os.Getenv("NO_PROXY")
	}
	host := u.Hostname()
	if host == "" {
		return true
	}
	if noProxy == "" {
		return false
	}
	entries := strings.Split(strings.ReplaceAll(noProxy, " ", ""), ",")
	if ip := net.ParseIP(host); ip != nil {
		for _, entry := range entries {
			if entry == "" {
				continue
			}
			if _, network, err := net.ParseCIDR(entry); err == nil {
				if network.Contains(ip) {
					return true
				}
				continue
			}
			if other := net.ParseIP(entry); other != nil && other.Equal(ip) {
				return true
			}
		}
		return false
	}
	hostWithPort := host
	if port := u.Port(); port != "" {
		hostWithPort = host + ":" + port
	}
	for _, entry := range entries {
		if entry == "" {
			continue
		}
		if strings.HasSuffix(host, entry) || strings.HasSuffix(hostWithPort, entry) {
			return true
		}
	}
	return false
}

// proxyEnvironment builds the scheme -> proxy map like urllib's
// getproxies_environment: every *_proxy variable contributes under its
// lowercase scheme name, and a LOWERCASE variable wins over its uppercase
// twin (an empty lowercase value removes the entry). The second pass inspects
// the ORIGINAL name, so an uppercase *_PROXY never overrides a lowercase one.
func proxyEnvironment() map[string]string {
	proxies := make(map[string]string)
	for _, kv := range os.Environ() {
		name, value, ok := strings.Cut(kv, "=")
		if !ok {
			continue
		}
		lower := strings.ToLower(name)
		if strings.HasSuffix(lower, "_proxy") && value != "" {
			proxies[strings.TrimSuffix(lower, "_proxy")] = value
		}
	}
	for _, kv := range os.Environ() {
		name, value, ok := strings.Cut(kv, "=")
		if !ok || !strings.HasSuffix(name, "_proxy") {
			continue
		}
		key := strings.TrimSuffix(strings.ToLower(name), "_proxy")
		if value != "" {
			proxies[key] = value
		} else {
			delete(proxies, key)
		}
	}
	return proxies
}

// formatMsg ports logplex.py:20-29 into the CLEAN frame (sanctioned
// deviation D6): "<190>1 <format-now> <hostname> <token> <pid> - -<MESSAGE>"
// prefixed by its byte length. The hostname/pid lookups keep Python's
// `entry.get("_HOSTNAME", "localhost")` shape. For the JOURNAL fields the
// processor strips leading underscores before the sender sees a payload
// (journalpump.py:708-714), so those keys never match and the values are the
// literal "localhost"; that qualifies the quirk rather than claiming it
// unconditionally, because extra_field_values are merged AFTER the stripping
// and CAN carry _HOSTNAME/_PID.
func (s *Sender) formatMsg(msg []byte) (string, error) {
	// UseNumber keeps integer fields as integer literals so _PID/_HOSTNAME
	// supplied through extra_field_values (merged AFTER the underscore
	// stripping, so these keys ARE reachable) render like Python's ints
	// rather than collapsing to float64.
	var m map[string]any
	dec := json.NewDecoder(bytes.NewReader(msg))
	dec.UseNumber()
	if err := dec.Decode(&m); err != nil {
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
	// requests.post CONSUMES the response body before returning (and raises
	// on a truncated/stalled body), so read it fully and propagate read
	// failures. The HTTP STATUS is still ignored: Python never calls
	// raise_for_status, so a 500 still marks the batch sent.
	_, readErr := io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return readErr
}

// MaintenanceOperations implements sender.Sender; logplex has none (no
// base.py override).
func (s *Sender) MaintenanceOperations(context.Context) error { return nil }

// pyStr mirrors Python's f-string interpolation (format(value, ""), i.e.
// str(value)) for the _HOSTNAME/_PID values. Those keys are unreachable for
// the journal fields (the processor strips the leading underscores), but
// extra_field_values are merged AFTER the stripping, so a sender extra can
// supply them, and its values are ordinary JSON (strings, ints, floats,
// bools, lists, objects) — all must render as Python would.
func pyStr(v any) string {
	switch t := v.(type) {
	case nil:
		return "None"
	case string:
		return t
	case bool:
		if t {
			return "True"
		}
		return "False"
	case int64:
		return strconv.FormatInt(t, 10)
	case float64:
		return journal.PythonFloatRepr(t)
	case json.Number:
		// An integer literal (no . e E) is a Python int and str()s to the
		// literal; a float literal uses Python's float repr.
		s := t.String()
		if strings.ContainsAny(s, ".eE") {
			if f, err := t.Float64(); err == nil {
				return journal.PythonFloatRepr(f)
			}
		}
		return s
	case []any:
		parts := make([]string, len(t))
		for i, item := range t {
			parts[i] = pyRepr(item)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys) // JSON decoding lost insertion order
		parts := make([]string, len(keys))
		for i, k := range keys {
			parts[i] = pyRepr(k) + ": " + pyRepr(t[k])
		}
		return "{" + strings.Join(parts, ", ") + "}"
	default:
		return fmt.Sprintf("%v", v)
	}
}

// pyRepr renders repr(value), used inside list/object interpolation. Strings
// follow Python's repr exactly: single quotes unless the string contains a
// single quote and no double quote (then double quotes), with backslash
// escaping of the chosen quote, backslash, the short escapes \t \n \r, and
// \xNN for every other control character (including NUL and DEL).
func pyRepr(v any) string {
	s, ok := v.(string)
	if !ok {
		return pyStr(v)
	}
	quote := byte('\'')
	if strings.Contains(s, "'") && !strings.Contains(s, `"`) {
		quote = '"'
	}
	var b strings.Builder
	b.WriteByte(quote)
	for _, r := range s {
		switch {
		case r == rune(quote) || r == '\\':
			b.WriteByte('\\')
			b.WriteRune(r)
		case r == '\t':
			b.WriteString(`\t`)
		case r == '\n':
			b.WriteString(`\n`)
		case r == '\r':
			b.WriteString(`\r`)
		case r < 0x20 || r == 0x7f:
			_, _ = fmt.Fprintf(&b, `\x%02x`, r)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte(quote)
	return b.String()
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
