// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

// Package elasticsearch implements the elasticsearch and opensearch
// senders: the port of
// journalpump/senders/elasticsearch_opensearch_sender.py (plan Task 23).
//
// Both output types share one transport (Python's _EsOsLogSenderBase);
// they differ only in the ES≤7 legacy mapping-type quirks
// (?include_type_name=true + _type: journal_msg), the config key
// prefix (<type>_url, <type>_timeout, <type>_index_prefix,
// <type>_index_days_max) and the state-file type name.
//
// It is a raw-HTTP sender (requests in Python, net/http here): the
// version probe (GET /), the alias census (GET /_aliases), index
// creation (PUT /<index>) and the bulk write (POST /<index>/_bulk).
// Success is 200/201 only for index creation; the bulk response status
// is logged but NOT enforced (quirk-locked), and per-item bulk errors
// are never inspected. Maintenance deletes the oldest
// <prefix>-YYYY-MM-DD indices while more than index_days_max exist.
//
// SANCTIONED DEVIATION (charter §2, Task 23): Python's request-session
// timeout is a per-request inactivity timeout; Go applies a per-request
// context deadline instead (the bulk request gets 2× the configured
// timeout, exactly as Python passes timeout=request_timeout*2). A
// stalled peer therefore fails the batch and the shared loop retries it
// rather than hanging the sender goroutine.
//
// SANCTIONED DEVIATION (charter §2, Task 23): Python's
// stats.unexpected_exception calls from the connect/create paths are not
// reproduced — no reference sender in this tree owns a statsd client
// (the shared loop emits the unexpected-exception metric for permanent
// send errors). The errors themselves are logged and routed through the
// loop's disconnect/backoff path.
package elasticsearch

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Aiven-Open/journalpump/internal/journal"
	"github.com/Aiven-Open/journalpump/internal/sender"
)

// senderType is Python's SenderType enum value.
type senderType string

const (
	elasticsearch senderType = "elasticsearch"
	opensearch    senderType = "opensearch"
)

// Ported defaults (elasticsearch_opensearch_sender.py).
const (
	// _DEFAULT_REQUEST_TIMEOUT = 60.0 (seconds).
	defaultRequestTimeout = 60 * time.Second
	// _DEFAULT_INDEX_LIFETIME_IN_DAYS = 3.
	defaultIndexLifetimeInDays = 3
	// _DEFAULT_INDEX_PREFIX = "journalpump".
	defaultIndexPrefix = "journalpump"
	// _DEFAULT_MAX_SENDER_INTERVAL = 10.0 (seconds).
	defaultMaxSendInterval = 10 * time.Second
	// _ONE_HOUR_LAST_INDEX_CHECK = 3600.
	oneHourLastIndexCheck = time.Hour
	// _VERSION_WITH_MAPPING_TYPE_SUPPORT = 7.
	versionWithMappingTypeSupport = 7
	// _LEGACY_TYPE = "journal_msg".
	legacyMappingType = "journal_msg"
)

// version mirrors the frozen Version dataclass.
type version struct {
	major int
	minor int
	patch int
}

// versionFromJSON is Version.from_json: it reads response["version"]["number"]
// and requires exactly three dot-separated integers.
func versionFromJSON(data []byte) (version, error) {
	var body struct {
		Version struct {
			Number string `json:"number"`
		} `json:"version"`
	}
	if err := json.Unmarshal(data, &body); err != nil {
		return version{}, err
	}
	if body.Version.Number == "" {
		return version{}, errors.New(`version response missing "version.number"`)
	}
	parts := strings.Split(body.Version.Number, ".")
	if len(parts) != 3 {
		return version{}, fmt.Errorf("version %q is not major.minor.patch", body.Version.Number)
	}
	nums := make([]int, 3)
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil {
			return version{}, fmt.Errorf("version %q is not major.minor.patch", body.Version.Number)
		}
		nums[i] = n
	}
	return version{major: nums[0], minor: nums[1], patch: nums[2]}, nil
}

// config mirrors the frozen Config dataclass (Config.create).
type config struct {
	senderType          senderType
	sessionURL          string
	requestTimeout      time.Duration
	indexName           string
	indexLifetimeInDays int
	ca                  any
}

// requestURL is Config.request_url: "<session_url>/<path>".
func (c config) requestURL(path string) string {
	return c.sessionURL + "/" + path
}

// configFrom is Config.create: it requires "<type>_url" (rstripped),
// defaulting the timeout, index prefix and lifetime.
func configFrom(raw map[string]any, st senderType) (config, error) {
	rawURL, present := raw[string(st)+"_url"]
	if !present {
		return config{}, fmt.Errorf("%s_url hasn't been defined", st)
	}
	url, ok := rawURL.(string)
	if !ok {
		return config{}, fmt.Errorf("%s_url: %v is not a string", st, rawURL)
	}
	cfg := config{
		senderType:          st,
		sessionURL:          strings.TrimRight(url, "/"),
		requestTimeout:      defaultRequestTimeout,
		indexName:           defaultIndexPrefix,
		indexLifetimeInDays: defaultIndexLifetimeInDays,
	}
	if v, present := raw[string(st)+"_timeout"]; present {
		seconds, ok := numberSeconds(v)
		if !ok {
			return config{}, fmt.Errorf("%s_timeout: %v is not a number", st, v)
		}
		cfg.requestTimeout = time.Duration(seconds * float64(time.Second))
	}
	if v, present := raw[string(st)+"_index_prefix"]; present {
		s, ok := v.(string)
		if !ok {
			return config{}, fmt.Errorf("%s_index_prefix: %v is not a string", st, v)
		}
		cfg.indexName = s
	}
	if v, present := raw[string(st)+"_index_days_max"]; present {
		n, ok := numberInt(v)
		if !ok {
			return config{}, fmt.Errorf("%s_index_days_max: %v is not an integer", st, v)
		}
		cfg.indexLifetimeInDays = n
	}
	if v, present := raw["ca"]; present {
		cfg.ca = v
	}
	return cfg, nil
}

// doer is the HTTP client seam (Python's requests.Session); tests inject
// a recording/httptest-backed client.
type doer interface {
	Do(req *http.Request) (*http.Response, error)
}

// Sender is the shared elasticsearch/opensearch transport.
type Sender struct {
	cfg    config
	log    sender.Logger
	client doer
	now    func() time.Time

	version        *version
	indices        map[string]struct{}
	lastIndexCheck time.Time
	// lastErrorClass dedups the connection-error warning by error class
	// (Python's _last_es_error.__class__ comparison).
	lastErrorClass string

	onConnected    func()
	onDisconnected func(error)
}

// NewElasticsearch is the wiring-map constructor for output_type
// "elasticsearch".
func NewElasticsearch(args sender.ConstructorArgs) (sender.Instance, error) {
	return newSender(args, elasticsearch)
}

// NewOpenSearch is the wiring-map constructor for output_type
// "opensearch".
func NewOpenSearch(args sender.ConstructorArgs) (sender.Instance, error) {
	return newSender(args, opensearch)
}

func newSender(args sender.ConstructorArgs, st senderType) (sender.Instance, error) {
	cfg, err := configFrom(args.Config, st)
	if err != nil {
		return sender.Instance{}, err
	}
	interval := defaultMaxSendInterval
	if v, present := args.Config["max_send_interval"]; present {
		seconds, ok := numberSeconds(v)
		if !ok {
			return sender.Instance{}, fmt.Errorf("max_send_interval: %v is not a number", v)
		}
		if seconds <= 0 {
			return sender.Instance{}, fmt.Errorf("max_send_interval: %v must be positive", v)
		}
		interval = time.Duration(seconds * float64(time.Second))
	}
	client, err := newHTTPClient(cfg.ca)
	if err != nil {
		return sender.Instance{}, err
	}
	s := &Sender{
		cfg:     cfg,
		log:     args.Log,
		client:  client,
		now:     time.Now,
		indices: map[string]struct{}{},
	}
	if s.log == nil {
		s.log = discardLogger{}
	}
	typeName := "OpenSearchSender"
	if st == elasticsearch {
		typeName = "ElasticsearchSender"
	}
	return sender.Instance{
		Transport:       s,
		MaxSendInterval: interval,
		Type:            typeName,
	}, nil
}

type discardLogger struct{}

func (discardLogger) Printf(string, ...any) {}

// newHTTPClient builds the net/http client for the configured `ca`
// value (Python session.verify = config.get("ca", True)): absent/true
// uses the system CA set, false disables verification, a string is a
// PEM bundle path.
func newHTTPClient(ca any) (*http.Client, error) {
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	switch v := ca.(type) {
	case nil:
		// default: verify with the system roots
	case bool:
		if !v {
			tlsCfg.InsecureSkipVerify = true //nolint:gosec // explicit operator opt-out, Python verify=False parity
		}
	case string:
		pem, err := os.ReadFile(v)
		if err != nil {
			return nil, fmt.Errorf("ca: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("ca: %q contains no PEM certificates", v)
		}
		tlsCfg.RootCAs = pool
	default:
		return nil, fmt.Errorf("ca: %v is not a bool or path", ca)
	}
	transport := &http.Transport{
		Proxy:           http.ProxyFromEnvironment,
		TLSClientConfig: tlsCfg,
	}
	return &http.Client{Transport: transport}, nil
}

// SetConnectionObserver satisfies sender.ConnectionReporter: the loop
// owns connected state and the shared backoff exponent.
func (s *Sender) SetConnectionObserver(connected func(), disconnected func(error)) {
	s.onConnected = connected
	s.onDisconnected = disconnected
}

func (s *Sender) connected() {
	if s.onConnected != nil {
		s.onConnected()
	}
}

func (s *Sender) disconnected(err error) {
	if s.onDisconnected != nil {
		s.onDisconnected(err)
	}
}

// SendMessages ports _EsOsLogSenderBase.send_messages.
func (s *Sender) SendMessages(ctx context.Context, batch [][]byte, _ string) error {
	if err := s.loadIndices(ctx); err != nil {
		s.log.Printf("Waiting for connection for %s", s.cfg.senderType)
		return err
	}
	var buf bytes.Buffer
	var lastName string
	for _, msg := range batch {
		message, err := parseMessage(msg)
		if err != nil {
			return err
		}
		idxDate, err := s.indexDate(message)
		if err != nil {
			return err
		}
		idxName := s.cfg.indexName + "-" + idxDate
		if _, ok := s.indices[idxName]; !ok {
			s.createIndexAndMapping(ctx, idxName, message)
		}
		header, err := s.messageHeader(idxName)
		if err != nil {
			return err
		}
		buf.Write(header)
		buf.WriteByte('\n')
		buf.Write(msg)
		buf.WriteByte('\n')
		lastName = idxName
	}
	if buf.Len() == 0 {
		return nil
	}
	// Python logs success purely from the status; the bulk response
	// status is NOT enforced and per-item errors are ignored.
	if err := s.postBulk(ctx, lastName, buf.Bytes()); err != nil {
		return err
	}
	return nil
}

// postBulk sends the NDJSON body to <index>/_bulk with the 2× timeout.
func (s *Sender) postBulk(ctx context.Context, indexName string, body []byte) error {
	headers := map[string]string{
		"content-length": strconv.Itoa(len(body)),
		"content-type":   "application/x-ndjson",
	}
	_, _, err := s.do(ctx, http.MethodPost, s.cfg.requestURL(indexName+"/_bulk"), body, headers, s.cfg.requestTimeout*2)
	return err
}

// indexDate returns timestamp[:10] when the message carries a non-empty
// "timestamp", else the current UTC date plus warning (send_messages).
func (s *Sender) indexDate(message map[string]any) (string, error) {
	raw, present := message["timestamp"]
	if present && raw != nil {
		str, ok := raw.(string)
		if !ok {
			return "", fmt.Errorf("timestamp: %v is not a string", raw)
		}
		if str != "" {
			if len(str) < 10 {
				return str, nil
			}
			return str[:10], nil
		}
	}
	s.log.Printf("Message missing 'timestamp' field, using current date for index name")
	return s.now().UTC().Format("2006-01-02"), nil
}

// messageHeader is _message_header: the NDJSON action line. For
// Elasticsearch it carries the legacy _type for major ≤ 7, and requires
// the probed version.
func (s *Sender) messageHeader(indexName string) ([]byte, error) {
	if s.cfg.senderType == elasticsearch {
		if s.version == nil {
			return nil, errors.New("Version has not been set") //nolint:staticcheck // error-text parity with the Python ValueError
		}
		if s.version.major <= versionWithMappingTypeSupport {
			return []byte(`{"index": {"_index": ` + journal.PythonJSONString(indexName) +
				`, "_type": "` + legacyMappingType + `"}}`), nil
		}
	}
	return []byte(`{"index": {"_index": ` + journal.PythonJSONString(indexName) + `}}`), nil
}

// indexURL is _index_url: Elasticsearch ≤ 7 appends
// ?include_type_name=true.
func (s *Sender) indexURL(indexName string) (string, error) {
	url := s.cfg.requestURL(indexName)
	if s.cfg.senderType != elasticsearch {
		return url, nil
	}
	if s.version == nil {
		return "", errors.New("Version has not been set") //nolint:staticcheck // error-text parity with the Python ValueError
	}
	if s.version.major <= versionWithMappingTypeSupport {
		url += "?include_type_name=true"
	}
	return url, nil
}

// createIndexAndMapping is _create_index_and_mapping: PUT the mapping,
// treating 200/201 (or an already-exists response body) as success. All
// failures are logged and swallowed, exactly as Python continues to the
// bulk write.
func (s *Sender) createIndexAndMapping(ctx context.Context, indexName string, message map[string]any) {
	s.log.Printf("Creating index: %q for %s", indexName, s.cfg.senderType)
	url, err := s.indexURL(indexName)
	if err != nil {
		s.log.Printf("Problem creating index %q: %v", indexName, err)
		return
	}
	mapping, err := json.Marshal(s.createMapping(message))
	if err != nil {
		s.log.Printf("Problem creating index %q: %v", indexName, err)
		return
	}
	status, body, err := s.do(ctx, http.MethodPut, url, mapping, map[string]string{"content-type": "application/json"}, s.cfg.requestTimeout)
	if err != nil {
		s.disconnected(err)
		s.log.Printf("Problem creating index %q: %s: %v", indexName, errorClass(err), err)
		return
	}
	if status == http.StatusOK || status == http.StatusCreated || strings.Contains(string(body), "already_exists_exception") {
		s.indices[indexName] = struct{}{}
		return
	}
	s.disconnected(fmt.Errorf("Cannot create index %q (%d %s)", indexName, status, string(body))) //nolint:staticcheck // error-text parity with the Python mark_disconnected message
	s.log.Printf("Could not create index mappings for: %q, %q %d", indexName, string(body), status)
}

// createMapping is _create_mapping (plus the Elasticsearch ≤ 7 legacy
// wrapper).
func (s *Sender) createMapping(message map[string]any) map[string]any {
	properties := map[string]any{
		"SYSTEMD_SESSION": map[string]any{"type": "text"},
		"SESSION_ID":      map[string]any{"type": "text"},
	}
	for k, v := range s.messageFields(message) {
		properties[k] = v
	}
	if s.cfg.senderType == elasticsearch && s.version != nil && s.version.major <= versionWithMappingTypeSupport {
		return map[string]any{
			"mappings": map[string]any{
				legacyMappingType: map[string]any{"properties": properties},
			},
		}
	}
	return map[string]any{
		"mappings": map[string]any{"properties": properties},
	}
}

// messageFields is _message_fields: bool→boolean, float→float,
// int→integer, str→text, dict→recursive properties; anything else is an
// unmapped field (dropped, with a warning). "timestamp" is skipped.
func (s *Sender) messageFields(message map[string]any) map[string]any {
	fields := map[string]any{}
	unmapped := map[string]string{}
	for k, v := range message {
		if k == "timestamp" {
			continue
		}
		switch t := v.(type) {
		case bool:
			fields[k] = map[string]any{"type": "boolean"}
		case json.Number:
			if isIntegerLiteral(t.String()) {
				fields[k] = map[string]any{"type": "integer"}
			} else {
				fields[k] = map[string]any{"type": "float"}
			}
		case float64:
			fields[k] = map[string]any{"type": "float"}
		case string:
			fields[k] = map[string]any{"type": "text"}
		case map[string]any:
			fields[k] = map[string]any{"properties": s.messageFields(t)}
		default:
			unmapped[k] = fmt.Sprintf("%T", v)
		}
	}
	if len(unmapped) > 0 {
		s.log.Printf("Unmapped fields: %v", unmapped)
	}
	return fields
}

// loadIndices is _load_indices: probe the version and the alias census
// once, caching the result. Connection errors are dedup-logged by class;
// the returned error fails the batch so the shared loop backs off.
func (s *Sender) loadIndices(ctx context.Context) error {
	if len(s.indices) > 0 {
		return nil
	}
	if s.version == nil {
		_, body, err := s.do(ctx, http.MethodGet, s.cfg.requestURL(""), nil, nil, s.cfg.requestTimeout)
		if err != nil {
			return s.logLoadError(err)
		}
		v, err := versionFromJSON(body)
		if err != nil {
			return s.logUnexpected(err)
		}
		s.version = &v
	}
	_, body, err := s.do(ctx, http.MethodGet, s.cfg.requestURL("_aliases"), nil, nil, s.cfg.requestTimeout)
	if err != nil {
		return s.logLoadError(err)
	}
	var aliases map[string]any
	if err := json.Unmarshal(body, &aliases); err != nil {
		return s.logUnexpected(err)
	}
	for k := range aliases {
		s.indices[k] = struct{}{}
	}
	s.lastErrorClass = ""
	s.log.Printf("Initialized %s HTTP connection for %s", s.cfg.senderType, s.cfg.indexName)
	s.connected()
	return nil
}

// logLoadError is the connection/timeout except clause: dedup the
// warning by error class and report disconnected.
func (s *Sender) logLoadError(err error) error {
	class := errorClass(err)
	s.disconnected(err)
	if class != s.lastErrorClass {
		s.log.Printf("Connection error to %s: %s: %v", s.cfg.senderType, class, err)
	}
	s.lastErrorClass = class
	return err
}

// logUnexpected is the broad except clause.
func (s *Sender) logUnexpected(err error) error {
	s.disconnected(err)
	s.log.Printf("Unexpected exception connecting to %s: %v", s.cfg.senderType, err)
	return err
}

// MaintenanceOperations is maintenance_operations / _check_indices: once
// an hour (and on the first call) delete the oldest rolled indices while
// more than index_days_max exist.
func (s *Sender) MaintenanceOperations(ctx context.Context) error {
	if s.now().Sub(s.lastIndexCheck) <= oneHourLastIndexCheck {
		return nil
	}
	s.lastIndexCheck = s.now()
	return s.checkIndices(ctx)
}

func (s *Sender) checkIndices(ctx context.Context) error {
	_, body, err := s.do(ctx, http.MethodGet, s.cfg.requestURL("_aliases"), nil, nil, s.cfg.requestTimeout)
	if err != nil {
		return err
	}
	var aliases map[string]any
	if err := json.Unmarshal(body, &aliases); err != nil {
		return err
	}
	prefix := s.cfg.indexName + "-"
	var indices []string
	for k := range aliases {
		if strings.HasPrefix(k, prefix) {
			indices = append(indices, k)
		}
	}
	sort.Strings(indices)
	s.log.Printf("Checking indices, currently: %v are available, max_indices: %d", indices, s.cfg.indexLifetimeInDays)
	for len(indices) > s.cfg.indexLifetimeInDays {
		indexToDelete := indices[0]
		indices = indices[1:]
		s.log.Printf("Deleting index: %q since we only keep %d days worth of indices", indexToDelete, s.cfg.indexLifetimeInDays)
		if err := s.deleteIndex(ctx, indexToDelete); err != nil {
			s.log.Printf("Unexpected exception deleting index %q: %v", indexToDelete, err)
		}
	}
	return nil
}

func (s *Sender) deleteIndex(ctx context.Context, indexName string) error {
	_, _, err := s.do(ctx, http.MethodDelete, s.cfg.requestURL(indexName), nil, nil, s.cfg.requestTimeout)
	if err != nil {
		return err
	}
	delete(s.indices, indexName)
	return nil
}

// do runs one HTTP request under a per-request deadline and returns the
// status and the fully-read body.
func (s *Sender) do(ctx context.Context, method, url string, body []byte, headers map[string]string, timeout time.Duration) (int, []byte, error) {
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, reader)
	if err != nil {
		return 0, nil, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, data, nil
}

// parseMessage decodes one wire payload preserving int/float (UseNumber),
// like Python's json.loads.
func parseMessage(msg []byte) (map[string]any, error) {
	dec := json.NewDecoder(bytes.NewReader(msg))
	dec.UseNumber()
	var message map[string]any
	if err := dec.Decode(&message); err != nil {
		return nil, err
	}
	return message, nil
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

func numberInt(v any) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case int64:
		return int(n), true
	case float64:
		if n == float64(int(n)) {
			return int(n), true
		}
		return 0, false
	case json.Number:
		i, err := n.Int64()
		return int(i), err == nil
	default:
		return 0, false
	}
}

// errorClass names a transport error the way Python's except-clause
// logging names the requests exception class.
func errorClass(err error) string {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return "Timeout"
	}
	var netErr interface{ Timeout() bool }
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "Timeout"
	}
	return "ConnectionError"
}
