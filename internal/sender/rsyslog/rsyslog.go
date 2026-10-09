// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

// Package rsyslog implements the rsyslog sender: the port of
// journalpump/senders/rsyslog.py (plan Task 20, spec §5). It reads the
// journald JSON payloads the shared buffer hands it, maps each to a
// SyslogTcpClient.log() call (the Task 19 syslogframing package), and
// lets the shared loop own reconnection/backoff classification.
package rsyslog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Aiven-Open/journalpump/internal/journal"
	"github.com/Aiven-Open/journalpump/internal/sender"
	"github.com/Aiven-Open/journalpump/internal/syslogframing"
)

// defaultMaxSendInterval is rsyslog.py:17's config.get("max_send_interval", 0.3).
const defaultMaxSendInterval = 0.3

// defaultFacility and defaultSeverity are rsyslog.py:22-23.
const (
	defaultFacility = 1
	defaultSeverity = 6
)

// initRetrySleep is rsyslog.py:70's flat time.sleep(5.0) after every
// retriable connection failure inside _init_rsyslog_client.
const initRetrySleep = 5 * time.Second

// logClient is the consumer-side seam onto the syslog transport (thread B
// custom-fake rule): *syslogframing.Client implements it in production,
// tests inject a recording fake (the Python unit tests' Mock
// rsyslog_client).
type logClient interface {
	Log(ctx context.Context, in syslogframing.LogInput) error
	Close()
}

// clientFactory builds the transport client. It is a seam so tests can
// inject a recording client without a live socket.
type clientFactory func(ctx context.Context) (logClient, error)

// warningLogger is the OPTIONAL logger capability for WARNING-level lines
// (Python's self.log.warning for a missing timestamp); plain test loggers
// fall back to Printf.
type warningLogger interface {
	Warningf(format string, args ...any)
}

// Sender is the rsyslog transport (rsyslog.py RsyslogSender). The client
// is established lazily on the first send; on any send failure it is
// dropped so the next attempt reconnects, exactly like Python's
// _init_rsyslog_client re-init after close().
type Sender struct {
	rawConfig  map[string]any
	senderName string
	log        sender.Logger
	now        func() time.Time
	newClient  clientFactory
	// sleep is the cancelable wait seam for the init loop (tests replace
	// it to avoid real backoff/5 s sleeps). It returns false when ctx is
	// done.
	sleep func(ctx context.Context, d time.Duration) bool

	mu     sync.Mutex
	client logClient
	// cfg is the parsed client config, lazily built on the first connect.
	cfg     *clientConfig
	parseEr error
	parsed  bool
}

// clientConfig is the resolved SyslogTcpClient configuration
// (rsyslog.py:36-49).
type clientConfig struct {
	client          syslogframing.Config
	defaultFacility int
	defaultSeverity int
	structuredData  *string
}

// New is the wiring-map constructor for output_type "rsyslog"
// (journalpump/senders/__init__.py). Like Python's RsyslogSender.__init__,
// it reads only max_send_interval eagerly (base.py) — the transport
// config is resolved lazily at first send, so a bad rsyslog_server or
// format surfaces on the send path (Python's deferred _init_rsyslog_client)
// rather than failing construction.
func New(args sender.ConstructorArgs) (sender.Instance, error) {
	seconds := defaultMaxSendInterval
	if raw, present := args.Config["max_send_interval"]; present {
		parsed, ok := intervalSeconds(raw)
		if !ok {
			// Python stores the non-numeric value and later crashes the
			// run loop's float comparison with TypeError; contained here as
			// a construction failure, like the file sender.
			return sender.Instance{}, fmt.Errorf("max_send_interval: %v is not a number", raw)
		}
		seconds = parsed
	}
	s := &Sender{
		rawConfig:  args.Config,
		senderName: args.Name,
		log:        args.Log,
		now:        time.Now,
		sleep:      sleepCtx,
	}
	s.newClient = s.connect
	return sender.Instance{
		Transport:       s,
		MaxSendInterval: time.Duration(seconds * float64(time.Second)),
		Type:            "RsyslogSender",
		// The rsyslog classifier routes RSYSLOG_CONN_ERRORS to the
		// silent retriable path and everything else to Python's generic
		// "unexpected exception" path (rsyslog.py:114-128).
		Classifier: sender.ClassifierFunc(Retriable),
	}, nil
}

// parseConfig resolves the raw config block into clientConfig, mirroring
// SyslogTcpClient's keyword arguments in _init_rsyslog_client
// (rsyslog.py:30-49). config["rsyslog_server"] is required (Python KeyError).
func parseConfig(raw map[string]any) (*clientConfig, error) {
	server, present := raw["rsyslog_server"]
	if !present {
		return nil, errors.New("config key 'rsyslog_server' missing")
	}
	serverStr, ok := server.(string)
	if !ok {
		return nil, fmt.Errorf("rsyslog_server: %v is not a string", server)
	}
	port := 514
	if v, present := raw["rsyslog_port"]; present {
		n, err := pythonInt(v)
		if err != nil {
			return nil, fmt.Errorf("rsyslog_port: %w", err)
		}
		port = n
	}
	rfc := "rfc5424"
	if v, present := raw["format"]; present {
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("format: %v is not a string", v)
		}
		rfc = s
	}
	maxMsg := 0
	if v, present := raw["max_message_size"]; present && v != nil {
		n, err := pythonInt(v)
		if err != nil {
			return nil, fmt.Errorf("max_message_size: %w", err)
		}
		maxMsg = n
	}
	protocol := "PLAINTEXT"
	if isTrue(raw["ssl"]) {
		protocol = "SSL"
	}
	// Python's `cacerts is not None` (rsyslog.py:135): the TLS context is
	// built whenever ca_certs is supplied with a non-null value — an
	// explicitly empty string included. config.get returns None for both
	// an absent key and an explicit null, so only present+non-nil counts.
	caCertsPresent := false
	if v, present := raw["ca_certs"]; present && v != nil {
		caCertsPresent = true
	}
	cfg := syslogframing.Config{
		Server:              serverStr,
		Port:                port,
		RFC:                 strings.ToUpper(rfc),
		MaxMsg:              maxMsg,
		Protocol:            protocol,
		CACerts:             optionalConfigString(raw, "ca_certs"),
		CACertsPresent:      caCertsPresent,
		KeyFile:             optionalConfigString(raw, "client_key"),
		CertFile:            optionalConfigString(raw, "client_cert"),
		LogFormat:           optionalConfigString(raw, "logline"),
		OctetCountedFraming: isTrue(raw["octet_counted_framing"]),
		EscapeNewlines:      isTrue(raw["escape_newlines"]),
	}
	res := &clientConfig{client: cfg, defaultFacility: defaultFacility, defaultSeverity: defaultSeverity}
	if v, present := raw["default_facility"]; present {
		n, err := pythonInt(v)
		if err != nil {
			return nil, fmt.Errorf("default_facility: %w", err)
		}
		res.defaultFacility = n
	}
	if v, present := raw["default_severity"]; present {
		n, err := pythonInt(v)
		if err != nil {
			return nil, fmt.Errorf("default_severity: %w", err)
		}
		res.defaultSeverity = n
	}
	if v, present := raw["structured_data"]; present && v != nil {
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("structured_data: %v is not a string", v)
		}
		res.structuredData = &s
	}
	return res, nil
}

// resolvedConfig returns the parsed client config, parsing once.
func (s *Sender) resolvedConfig() (*clientConfig, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.parsed {
		s.cfg, s.parseEr = parseConfig(s.rawConfig)
		s.parsed = true
	}
	return s.cfg, s.parseEr
}

// connect builds a real syslogframing client, completing the TCP/TLS
// connection so errors surface here (Python's _init_rsyslog_client).
func (s *Sender) connect(ctx context.Context) (logClient, error) {
	cfg, err := s.resolvedConfig()
	if err != nil {
		return nil, err
	}
	c, err := syslogframing.NewClient(ctx, cfg.client)
	if err != nil {
		return nil, err
	}
	return c, nil
}

// ensureClient returns the live client, connecting on first use. The
// connect call runs WITHOUT holding s.mu (the production connect reads
// the parsed config under the same lock; holding it here would deadlock).
//
// Reconnection is integrated with the shared Task 13 loop: on a retriable
// connection failure the sender logs rsyslog.py:62-66's warning, waits
// out the flat 5 s sleep (rsyslog.py:70), and RETURNS the error so the
// loop owns the mark_disconnected transition (populating the health error
// ring), the jittered exponential backoff — one shared counter, so a
// send-failure backoff and a subsequent reconnect-failure backoff
// continue the same exponent instead of restarting — and the reset on
// the next successful batch. The sender keeps no private backoff state.
// A non-retriable failure (missing rsyslog_server, bad format) returns
// immediately for the loop's "unexpected exception" path.
func (s *Sender) ensureClient(ctx context.Context) (logClient, error) {
	s.mu.Lock()
	if s.client != nil {
		c := s.client
		s.mu.Unlock()
		return c, nil
	}
	s.mu.Unlock()

	s.logf("Initializing Rsyslog Client %s", s.name())
	c, err := s.newClient(ctx)
	if err != nil {
		if !Retriable(err) {
			return nil, err
		}
		s.warn("Retryable error during Rsyslog Client initialization: %s: %s, sleeping", fmt.Sprintf("%T", err), err)
		if !s.sleep(ctx, initRetrySleep) {
			return nil, ctx.Err()
		}
		return nil, err
	}
	s.mu.Lock()
	if s.client == nil {
		s.client = c
	} else {
		// Another path connected first; keep theirs.
		c.Close()
	}
	res := s.client
	s.mu.Unlock()
	if cfg, cerr := s.resolvedConfig(); cerr == nil {
		s.logf("Initialized Rsyslog Client %s, server: %s, port: %d", s.name(), cfg.client.Server, cfg.client.Port)
	}
	return res, nil
}

// sleepCtx waits d, returning false if ctx is cancelled first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// name is the sender's configured name (rsyslog.py's self.name from
// base.py), stored at construction for log parity.
func (s *Sender) name() string { return s.senderName }

// logf emits an INFO-level line when the logger supports Printf.
func (s *Sender) logf(format string, args ...any) {
	if s.log == nil {
		return
	}
	s.log.Printf(format, args...)
}

// dropClient closes and clears the client so the next send reconnects
// (Python's self.rsyslog_client.close(); self.rsyslog_client = None).
func (s *Sender) dropClient() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.client != nil {
		s.client.Close()
		s.client = nil
	}
}

// SendMessages implements sender.Sender: for each journald JSON payload,
// build the SyslogTcpClient.log() arguments and send (rsyslog.py:59-100).
// A failure drops the client and returns the error; the shared loop
// classifies it, backs off and retries the whole batch from its head —
// Python's False return with the messages re-sent next round.
func (s *Sender) SendMessages(ctx context.Context, batch [][]byte, _ string) error {
	client, err := s.ensureClient(ctx)
	if err != nil {
		return err
	}
	for _, msg := range batch {
		in, err := s.logInput(msg)
		if err != nil {
			s.dropClient()
			return err
		}
		if err := client.Log(ctx, in); err != nil {
			s.dropClient()
			return err
		}
	}
	return nil
}

// MaintenanceOperations implements sender.Sender; the rsyslog sender has
// none (no base.py override).
func (s *Sender) MaintenanceOperations(context.Context) error { return nil }

// Close releases the transport client (io.Closer, called by the
// supervisor after Finalize).
func (s *Sender) Close() error {
	s.dropClient()
	return nil
}

// logInput maps one journald JSON payload to SyslogTcpClient.log kwargs
// (rsyslog.py:60-99).
func (s *Sender) logInput(msg []byte) (syslogframing.LogInput, error) {
	// Decode with UseNumber so an integer field (JSON `100`) keeps its
	// integer identity instead of collapsing to float64(100) and rendering
	// "100.0" — Python's json.loads yields int(100), str(100) == "100".
	// The json.Number literal preserves the int/float distinction through
	// pythonStr/pythonInt.
	var m map[string]any
	dec := json.NewDecoder(bytes.NewReader(msg))
	dec.UseNumber()
	if err := dec.Decode(&m); err != nil {
		return syslogframing.LogInput{}, fmt.Errorf("rsyslog sender: decode payload: %w", err)
	}
	cfg, err := s.resolvedConfig()
	if err != nil {
		return syslogframing.LogInput{}, err
	}

	facility := cfg.defaultFacility
	if raw, present := m["SYSLOG_FACILITY"]; present {
		// facility = int(_facility[0] if isinstance(_facility, list) else _facility),
		// any IndexError/TypeError/ValueError falling back to the default.
		elem := raw
		if list, ok := raw.([]any); ok {
			if len(list) == 0 {
				elem = nil
			} else {
				elem = list[0]
			}
		}
		if n, err := pythonInt(elem); err == nil {
			facility = n
		}
	}

	// severity = int(message.get("PRIORITY", self.default_severity)) — NOT
	// guarded by a try, so a non-int PRIORITY is an error (Python's
	// unexpected-exception path).
	sevRaw, present := m["PRIORITY"]
	if !present {
		sevRaw = cfg.defaultSeverity
	}
	severity, err := pythonInt(sevRaw)
	if err != nil {
		return syslogframing.LogInput{}, fmt.Errorf("rsyslog sender: PRIORITY: %w", err)
	}

	timestamp, err := s.timestamp(m)
	if err != nil {
		return syslogframing.LogInput{}, err
	}

	return syslogframing.LogInput{
		Facility:  facility,
		Severity:  severity,
		Timestamp: timestamp,
		Hostname:  optionalStr(m, "HOSTNAME"),
		Program:   appName(m),
		PID:       pidValue(m),
		Msg:       optionalStr(m, "MESSAGE"),
		SD:        cfg.structuredData,
	}, nil
}

// timestamp ports rsyslog.py:71-75: a truthy "timestamp" yields
// raw_timestamp[:26] + "Z" (assume UTC); otherwise warn and fall back to
// the current UTC time rendered like Python's strftime("%Y-%m-%dT%H:%M:%S.%f").
func (s *Sender) timestamp(m map[string]any) (string, error) {
	raw, present := m["timestamp"]
	if present && truthy(raw) {
		str, ok := raw.(string)
		if !ok {
			return "", fmt.Errorf("rsyslog sender: timestamp: %v is not a string", raw)
		}
		return truncateRunes(str, 26) + "Z", nil
	}
	s.warn("Message missing 'timestamp' field, using current time as fallback")
	return s.now().UTC().Format("2006-01-02T15:04:05.000000") + "Z", nil
}

// warn emits a WARNING-level line when the logger supports it, else Printf.
func (s *Sender) warn(format string, args ...any) {
	if s.log == nil {
		return
	}
	if l, ok := s.log.(warningLogger); ok {
		l.Warningf(format, args...)
		return
	}
	s.log.Printf(format, args...)
}

// appName ports the nested default lookup
// message.get("SYSLOG_IDENTIFIER", message.get("SYSTEMD_UNIT", message.get("PROCESS_NAME"))):
// the first present key wins (a present null still wins and is falsy).
func appName(m map[string]any) *string {
	for _, key := range []string{"SYSLOG_IDENTIFIER", "SYSTEMD_UNIT", "PROCESS_NAME"} {
		if v, present := m[key]; present {
			return valueStr(v)
		}
	}
	return nil
}

// pidValue ports progid = message.get("PID") with the client's
// `str(pid) if pid else NILVALUE`: falsy → NILVALUE.
func pidValue(m map[string]any) any {
	v, present := m["PID"]
	if !present || !truthy(v) {
		return nil
	}
	return pythonStr(v)
}

// optionalStr returns valueStr(m[key]) for a present key (null/"" → nil).
func optionalStr(m map[string]any, key string) *string {
	v, present := m[key]
	if !present {
		return nil
	}
	return valueStr(v)
}

// valueStr renders a journal field value as an Optional[str] argument:
// a falsy value is None (the client turns None into NILVALUE); otherwise
// Python's str(value).
func valueStr(v any) *string {
	if !truthy(v) {
		return nil
	}
	s := pythonStr(v)
	return &s
}

// truthy mirrors Python truthiness for JSON-decoded values.
func truthy(v any) bool {
	switch t := v.(type) {
	case nil:
		return false
	case bool:
		return t
	case string:
		return t != ""
	case float64:
		return t != 0
	case int64:
		return t != 0
	case json.Number:
		f, err := t.Float64()
		return err == nil && f != 0
	case []any:
		return len(t) > 0
	case map[string]any:
		return len(t) > 0
	default:
		return true
	}
}

// pythonStr renders str(value) for JSON-decoded values.
func pythonStr(v any) string {
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
	case float64:
		return journal.PythonFloatRepr(t)
	case int64:
		return strconv.FormatInt(t, 10)
	case json.Number:
		return jsonNumberStr(t)
	case []any:
		parts := make([]string, len(t))
		for i, item := range t {
			parts[i] = pythonRepr(item)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	default:
		return fmt.Sprintf("%v", v)
	}
}

// jsonNumberStr renders str(value) for a JSON number kept as json.Number:
// a literal with no fraction/exponent is an int in Python (str is the
// literal, so PID 100 → "100"); otherwise Python parsed a float and str
// uses Python's float repr (so 100.0 → "100.0").
func jsonNumberStr(n json.Number) string {
	s := n.String()
	if strings.ContainsAny(s, ".eE") {
		if f, err := n.Float64(); err == nil {
			return journal.PythonFloatRepr(f)
		}
	}
	return s
}

// pythonRepr renders repr(value), used inside list rendering. Strings are
// single-quoted with the common escapes (Python's repr).
func pythonRepr(v any) string {
	s, ok := v.(string)
	if !ok {
		return pythonStr(v)
	}
	var b strings.Builder
	b.WriteByte('\'')
	for _, r := range s {
		switch r {
		case '\\':
			b.WriteString(`\\`)
		case '\'':
			b.WriteString(`\'`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('\'')
	return b.String()
}

// pythonInt ports Python's int(value) for JSON-decoded values: floats
// truncate toward zero, numeric strings parse, bools are 0/1; anything
// else (None, list, dict, non-numeric string) is an error.
func pythonInt(v any) (int, error) {
	switch t := v.(type) {
	case int:
		return t, nil
	case int64:
		return int(t), nil
	case float64:
		return int(t), nil
	case json.Number:
		if n, err := t.Int64(); err == nil {
			return int(n), nil
		}
		// Python's int() accepts a float and truncates toward zero.
		f, err := t.Float64()
		if err != nil {
			return 0, fmt.Errorf("invalid literal for int(): %q", t)
		}
		return int(f), nil
	case bool:
		if t {
			return 1, nil
		}
		return 0, nil
	case string:
		s := strings.TrimSpace(t)
		n, err := strconv.Atoi(s)
		if err != nil {
			return 0, fmt.Errorf("invalid literal for int(): %q", t)
		}
		return n, nil
	default:
		return 0, fmt.Errorf("int() argument must be a string or a number, not %T", v)
	}
}

// truncateRunes returns the first n runes of s (Python's str[:n], which
// indexes codepoints, not bytes).
func truncateRunes(s string, n int) string {
	runes := 0
	for i := range s {
		if runes == n {
			return s[:i]
		}
		runes++
	}
	return s
}

// isTrue reports Python's `value is True` (only a JSON true, not truthy).
func isTrue(v any) bool {
	b, ok := v.(bool)
	return ok && b
}

// optionalConfigString returns a config string value or "" when absent.
func optionalConfigString(raw map[string]any, key string) string {
	if v, present := raw[key]; present {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// Retriable classifies a send error like Python's RSYSLOG_CONN_ERRORS
// (socket.timeout, ConnectionRefusedError, TimeoutError): connect/read
// timeouts and connection refusals are retriable; everything else is the
// unexpected-exception path.
func Retriable(err error) bool {
	if err == nil {
		return false
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}
	return errors.Is(err, syscall.ECONNREFUSED)
}

// intervalSeconds renders a decoded JSON number as float seconds.
func intervalSeconds(value any) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case int64:
		return float64(v), true
	case json.Number:
		f, err := v.Float64()
		return f, err == nil
	default:
		return 0, false
	}
}
