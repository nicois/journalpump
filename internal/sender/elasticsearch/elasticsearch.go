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
// The HTTP client is requests-faithful: the timeout is an INACTIVITY
// timeout applied PER REQUEST, including on reused keep-alive connections
// (httpx.InactivityClient), so the bulk POST's 2× timeout never leaks onto a
// reused ordinary connection and vice versa. The CA resolution mirrors
// requests' merge_environment_settings + merge_setting precedence
// (REQUESTS_CA_BUNDLE/CURL_CA_BUNDLE WIN over session.verify, even when ca is
// false, empty, or an explicit path; an empty/false ca disables verification;
// a CA DIRECTORY is refused at load — fail closed, user decision 2026-10-12),
// and the stats.unexpected_exception emissions of the connect/create/delete
// paths are reproduced. The sender implements io.Closer so the daemon's
// retire path releases its idle keep-alive connections.
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

	"github.com/Aiven-Open/journalpump/internal/httpx"
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
		// get_requests_session(timeout=X) installs a TimeoutAdapter whose
		// fallback IS X; the ES sender never passes a per-request timeout for
		// the version/alias/PUT requests, so the adapter substitutes X and
		// requests REJECTS a non-positive value ("the timeout cannot be set to
		// a value less than or equal to 0"). This differs from logplex, whose
		// session default is a real 60 s. Reject zero and negative at load
		// (strict-at-load) instead of silently substituting 60 s or leaving
		// the deadline unlimited.
		if seconds <= 0 {
			return config{}, fmt.Errorf("%s_timeout: %v cannot be set to a value less than or equal to 0", st, seconds)
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

// exceptionStats is the statsd seam for the sender's own
// unexpected_exception emissions (Python's self.stats.unexpected_exception).
// The pump's *statsd.Client satisfies it in production; tests inject a fake.
type exceptionStats interface {
	UnexpectedException(ex error, where string, tags map[string]string)
}

// Sender is the shared elasticsearch/opensearch transport.
type Sender struct {
	cfg    config
	log    sender.Logger
	client doer
	stats  exceptionStats
	now    func() time.Time

	version        *version
	indices        map[string]struct{}
	lastIndexCheck time.Time
	// lastErrorClass dedups the connection-error warning by error class
	// (Python's _last_es_error.__class__ comparison).
	lastErrorClass string

	onConnected    func()
	onDisconnected func(error)
	// backoff is the loop-owned cancellable backoff (Python's self._backoff
	// called inside _create_index_and_mapping), injected by the loop.
	backoff func(context.Context)
}

// warningLogger / errorLogger are the OPTIONAL level-aware logging
// capabilities (the production journald sink provides Warningf/Errorf; plain
// loggers fall back to Printf at INFO). Python logs these diagnostics at
// WARNING/ERROR, so a Printf-only port would drop them at a WARNING/ERROR
// threshold.
type warningLogger interface {
	Warningf(format string, args ...any)
}

type errorLogger interface {
	Errorf(format string, args ...any)
}

func (s *Sender) logWarn(format string, args ...any) {
	if l, ok := s.log.(warningLogger); ok {
		l.Warningf(format, args...)
		return
	}
	s.log.Printf(format, args...)
}

func (s *Sender) logError(format string, args ...any) {
	if l, ok := s.log.(errorLogger); ok {
		l.Errorf(format, args...)
		return
	}
	s.log.Printf(format, args...)
}

// unexpected emits stats.unexpected_exception (Python passes no extra tags;
// the client's global tags apply). A nil stats client skips the emission.
func (s *Sender) unexpected(err error, where string) {
	if s.stats != nil {
		s.stats.UnexpectedException(err, where, nil)
	}
}

// SetBackoffObserver satisfies sender.Backoffer: the loop injects its
// cancellable jittered backoff so the sender can pace a failing index-create
// WITHOUT failing the batch (Python's self._backoff() inside the create path
// then continues to the bulk write).
func (s *Sender) SetBackoffObserver(backoff func(context.Context)) { s.backoff = backoff }

// waitBackoff runs the loop's backoff when wired (tests may leave it nil).
func (s *Sender) waitBackoff(ctx context.Context) {
	if s.backoff != nil {
		s.backoff(ctx)
	}
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
	var stats exceptionStats
	if args.Stats != nil {
		stats = args.Stats
	}
	s := &Sender{
		cfg:     cfg,
		log:     args.Log,
		client:  client,
		stats:   stats,
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

// newHTTPClient builds the shared httpx client for the configured `ca`
// value (Python session.verify = config.get("ca", True), with requests'
// environment-bundle precedence). Proxy selection is the requests-compatible
// strategy, because the Python ES/OpenSearch sender talks to the server
// through requests (journalpump.util.get_requests_session), not
// elasticsearch-py/urllib3. The client is an InactivityClient, which applies
// the per-request inactivity timeout (WithRequestTimeout in do) at each
// request boundary, so a slow-but-progressing response is not aborted and a
// reused connection uses the current request's timeout.
func newHTTPClient(ca any) (*httpx.InactivityClient, error) {
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	bundle, insecure, err := resolveVerify(ca)
	if err != nil {
		return nil, err
	}
	if insecure {
		tlsCfg.InsecureSkipVerify = true //nolint:gosec // explicit operator opt-out, Python verify=False parity
	} else if bundle != "" {
		pool, err := loadCertPool(bundle)
		if err != nil {
			return nil, err
		}
		tlsCfg.RootCAs = pool
	}
	return httpx.NewInactivityClient(httpx.ClientOptions{
		Proxy:           httpx.RequestsProxy,
		TLSClientConfig: tlsCfg,
	}), nil
}

// resolveVerify maps Python's session.verify to a CA bundle path and an
// insecure flag, reproducing requests' merge_environment_settings +
// merge_setting precedence. The ES sender sets Session.verify and passes NO
// per-request verify, so the per-request verify reaching
// merge_environment_settings is None — which makes requests consult
// REQUESTS_CA_BUNDLE / CURL_CA_BUNDLE, and the resulting non-empty string
// then WINS over Session.verify in merge_setting. Therefore the environment
// bundle takes precedence even when `ca` is false, empty, or an explicit
// path:
//
//   - REQUESTS_CA_BUNDLE, else CURL_CA_BUNDLE, when non-empty → that bundle
//     (FILE or DIRECTORY), regardless of `ca`;
//   - otherwise `ca` is used: absent / true → the system roots (empty path);
//     false → verification DISABLED; "" → DISABLED (requests treats a falsy
//     verify as no verification); another string → a CA bundle FILE (a
//     DIRECTORY is refused at load — fail closed, user decision 2026-10-12).
func resolveVerify(ca any) (bundle string, insecure bool, err error) {
	if env := caBundleFromEnv(); env != "" {
		return env, false, nil
	}
	switch v := ca.(type) {
	case nil:
		return "", false, nil
	case bool:
		if v {
			return "", false, nil
		}
		return "", true, nil
	case string:
		if v == "" {
			return "", true, nil
		}
		return v, false, nil
	default:
		return "", false, fmt.Errorf("ca: %v is not a bool or path", ca)
	}
}

// caBundleFromEnv returns the CA bundle path from REQUESTS_CA_BUNDLE or
// CURL_CA_BUNDLE (requests consults REQUESTS_CA_BUNDLE first), or "" when
// neither is set.
func caBundleFromEnv() string {
	if v := os.Getenv("REQUESTS_CA_BUNDLE"); v != "" {
		return v
	}
	return os.Getenv("CURL_CA_BUNDLE")
}

// loadCertPool builds a cert pool from a CA bundle FILE. A DIRECTORY is
// REFUSED (fail closed, user decision 2026-10-12): requests passes a
// directory verify to OpenSSL as a capath, whose entries are selected by the
// SUBJECT HASH of the chain being verified (plus collision-sequence
// numbering), and reproducing that faithfully is not cheap; silently trusting
// every hashed-looking file would instead broaden trust beyond OpenSSL.
// Refusing the configuration never trusts more than Python would — it just
// refuses the config.
func loadCertPool(path string) (*x509.CertPool, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("ca: %w", err)
	}
	if info.IsDir() {
		return nil, fmt.Errorf(
			"ca: a CA directory path is unsupported (OpenSSL capath subject-hash selection cannot be reproduced faithfully); use a CA bundle file or the system store: %q", path,
		)
	}
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("ca: %w", err)
	}
	pool := x509.NewCertPool()
	if added := pool.AppendCertsFromPEM(pem); !added {
		return nil, fmt.Errorf("ca: %q contains no PEM certificates", path)
	}
	return pool, nil
}

// Close implements io.Closer: release the transport's idle keep-alive
// connections so a retired sender (SIGHUP reload) does not leak sockets or
// transport goroutines. The daemon's retire path closes io.Closers.
func (s *Sender) Close() error {
	if c, ok := s.client.(interface{ CloseIdleConnections() }); ok {
		c.CloseIdleConnections()
	}
	return nil
}

var _ io.Closer = (*Sender)(nil)

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

// SendMessages ports _EsOsLogSenderBase.send_messages for bare payloads
// (no index-date sidecar): it reconstructs Items with zero sidecars, so
// every message takes the parsing path.
func (s *Sender) SendMessages(ctx context.Context, batch [][]byte, _ string) error {
	items := make([]sender.Item, len(batch))
	for i, msg := range batch {
		items[i] = sender.Item{Payload: msg}
	}
	return s.send(ctx, items)
}

// SendMessagesWithIndexDate is the Item-taking entry point (design §2.4):
// a message whose producer supplied the sidecar skips parseMessage on the
// common path (index already known); a zero sidecar falls back to the
// parsing path, which preserves the missing/invalid timestamp behaviour.
func (s *Sender) SendMessagesWithIndexDate(ctx context.Context, batch []sender.Item, _ string) error {
	return s.send(ctx, batch)
}

// send ports _EsOsLogSenderBase.send_messages.
func (s *Sender) send(ctx context.Context, batch []sender.Item) error {
	start := time.Now()
	if err := s.loadIndices(ctx); err != nil {
		s.logWarn("Waiting for connection for %s", s.cfg.senderType)
		return err
	}
	var buf bytes.Buffer
	var lastName string
	for _, item := range batch {
		msg := item.Payload
		var message map[string]any
		var idxDate string
		if item.IndexDate != ([10]byte{}) {
			idxDate = string(item.IndexDate[:])
		} else {
			parsed, err := parseMessage(msg)
			if err != nil {
				return err
			}
			message = parsed
			date, err := s.indexDate(message)
			if err != nil {
				return err
			}
			idxDate = date
		}
		idxName := s.cfg.indexName + "-" + idxDate
		if _, ok := s.indices[idxName]; !ok {
			if message == nil {
				parsed, err := parseMessage(msg)
				if err != nil {
					return err
				}
				message = parsed
			}
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
	// Python logs success purely from the status; the bulk response status is
	// NOT enforced and per-item bulk errors are never inspected (the cursor
	// advances regardless — TestBulkItemErrorsAdvanceCursorThroughLoop).
	status, err := s.postBulk(ctx, lastName, buf.Bytes())
	if err != nil {
		return err
	}
	s.log.Printf(
		"Sent %d log events successfully: %t, took: %.2fs",
		len(batch),
		status == http.StatusOK || status == http.StatusCreated,
		time.Since(start).Seconds(),
	)
	return nil
}

// postBulk sends the NDJSON body to <index>/_bulk with the 2× timeout and
// returns the response status.
func (s *Sender) postBulk(ctx context.Context, indexName string, body []byte) (int, error) {
	headers := map[string]string{
		"content-length": strconv.Itoa(len(body)),
		"content-type":   "application/x-ndjson",
	}
	status, _, err := s.do(ctx, http.MethodPost, s.cfg.requestURL(indexName+"/_bulk"), body, headers, s.cfg.requestTimeout*2)
	return status, err
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
	s.logWarn("Message missing 'timestamp' field, using current date for index name")
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
		// Python: mark_disconnected, log.error, stats.unexpected_exception and —
		// importantly — self._backoff() BEFORE continuing to the bulk write, so
		// an uncached index's failing PUTs are paced even when the bulk succeeds.
		s.disconnected(err)
		s.logError("Problem creating index %q: %s: %v", indexName, errorClass(err), err)
		s.unexpected(err, "es_pump_create_index_and_mappings")
		s.waitBackoff(ctx)
		return
	}
	if status == http.StatusOK || status == http.StatusCreated || strings.Contains(string(body), "already_exists_exception") {
		s.indices[indexName] = struct{}{}
		return
	}
	s.disconnected(fmt.Errorf("Cannot create index %q (%d %s)", indexName, status, string(body))) //nolint:staticcheck // error-text parity with the Python mark_disconnected message
	s.logWarn("Could not create index mappings for: %q, %q %d", indexName, string(body), status)
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
		s.logWarn("Unmapped fields: %v", unmapped)
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
		s.logWarn("Connection error to %s: %s: %v", s.cfg.senderType, class, err)
	}
	s.lastErrorClass = class
	return err
}

// logUnexpected is the broad except clause (Python's log.exception +
// stats.unexpected_exception).
func (s *Sender) logUnexpected(err error) error {
	s.disconnected(err)
	s.logError("Unexpected exception connecting to %s: %v", s.cfg.senderType, err)
	s.unexpected(err, "es_pump_init_es_client")
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
		if len(indices) == 0 {
			// A negative index_days_max makes the loop condition true on an
			// empty list. Python then raises IndexError from indices.pop(0),
			// which the base maintenance caller catches (where=
			// maintenance_operation) so sending continues. Return an error
			// instead of panicking.
			return fmt.Errorf("no indices left to delete for %s (index_days_max=%d)", s.cfg.indexName, s.cfg.indexLifetimeInDays)
		}
		indexToDelete := indices[0]
		indices = indices[1:]
		s.log.Printf("Deleting index: %q since we only keep %d days worth of indices", indexToDelete, s.cfg.indexLifetimeInDays)
		if err := s.deleteIndex(ctx, indexToDelete); err != nil {
			s.logError("Unexpected exception deleting index %q: %v", indexToDelete, err)
			s.unexpected(err, "es_pump_check_indices")
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

// do runs one HTTP request and returns the status and the fully-read body.
// The timeout is requests' INACTIVITY timeout, applied to the request in
// flight by the InactivityClient (carried via WithRequestTimeout), not a
// total deadline, so a slow-but-progressing response is not aborted.
// httpx.Do is called with a zero deadline: the per-request inactivity
// timeout governs the request.
func (s *Sender) do(ctx context.Context, method, url string, body []byte, headers map[string]string, timeout time.Duration) (int, []byte, error) {
	return httpx.Do(httpx.WithRequestTimeout(ctx, timeout), s.client, method, url, body, headers, 0)
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
