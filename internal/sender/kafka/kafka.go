// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

// Package kafka implements the Kafka sender: the port of
// journalpump/senders/kafka_sender.py (plan Task 22, threads A7/A8). It
// maps the Python sender's kafka-python keyword configuration onto
// github.com/twmb/franz-go and produces one batch per shared-loop flush
// through kgo.Client.ProduceSync, so a broker ack advances the shared
// cursor (ack-then-advance) — exactly Python's mark_sent-after-flush
// contract.
//
// franz-go is context-aware end to end (produce, dial, admin, flush,
// close), so cancellation needs no socket-force-close workaround: on
// deadline/cancellation the batch's client is closed, which fails the
// buffered and in-flight records promptly via a documented library API
// (kgo.Client.Close + failBufferedRecords).
//
// Shape difference from Python (thread A7): kafka-python sends each
// message, flushes, then waits on each FutureRecordMetadata with a 1 s
// timeout; franz-go's ProduceSync is synchronous and already blocks until
// every record is acked, so the per-future timeout dance collapses into
// one call. The broker-side outcome (delivered payload bytes, key,
// partition) is identical.
//
// Compression (thread A8, plan Task 22): the Python sender prefers zstd
// when the `zstandard` module is importable, else snappy, else gzip. The
// deployment ships python-snappy and no zstandard (requirements.txt;
// pinned in the Task 12 fixture), so the deployed behavior is snappy —
// franz-go's SnappyCompression, which emits the same raw snappy block
// framing the sarama sender used (verified; the kafka-python consumer
// interop test reads it back). The zstd-preference fact is recorded rather
// than reproduced.
//
// The sender is NOT a DurableReporter: kafka's cursor advances on the
// broker ack (ProduceSync's synchronous ack), so there is no write-behind
// buffer whose flush boundary would lag the cursor. It is the file sender
// alone that keeps the durable (disk-buffered) deliverable.
package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kversion"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/Aiven-Open/journalpump/internal/sender"
)

// defaultMaxSendInterval is kafka_sender.py:49's
// config.get("max_send_interval", 0.3).
const defaultMaxSendInterval = 0.3

// Producer settings pinned by kafka_sender.py.
const (
	// lingerMS is kafka_sender.py:93's linger_ms=500 (the producer waits
	// up to 500 ms to batch records).
	lingerMS = 500
)

// Producer retry/timeout defaults mirroring kafka-python (review item D12):
// RecordRetries bounded by RecordDeliveryTimeout=120000 and
// ProduceRequestTimeout=30000, with a 100 ms retry backoff to keep the
// documented shutdown bound. franz-go retries produce requests internally,
// so the shared sender loop owns only the outer batch retry.
const (
	deliveryTimeout  = 120 * time.Second
	requestTimeout   = 30 * time.Second
	retryBackoff     = 100 * time.Millisecond
	producerRetryMax = 20
)

// compressionName is the codec the deployment pins (snappy).
const compressionName = "snappy"

// producer is the produce-side seam onto franz-go. The production
// implementation owns a *kgo.Client and the resolved topic/key; Produce
// sends one batch of payloads and returns after the broker acks (or the
// ctx ends). Keeping the interface free of kgo types lets tests fake it
// (loop_test.go's recordingProducer).
type producer interface {
	Produce(ctx context.Context, batch [][]byte) error
	Close() error
}

// admin is the cluster-admin seam (topic auto-create).
type admin interface {
	CreateTopic(ctx context.Context, partitions int32, replicationFactor int16, topic string) error
	Close() error
}

// producerFactory and adminFactory build the live clients from a resolved
// producerConfig and the caller's context. Both are ctx-aware (franz-go
// dials, metadata and admin requests take a context), so cancellation is
// propagated without a tracking-dialer workaround. They are seams so tests
// inject recording fakes.
type (
	producerFactory func(ctx context.Context, pc producerConfig) (producer, error)
	adminFactory    func(ctx context.Context, pc producerConfig) (admin, error)
)

// Sender is the Kafka transport (kafka_sender.py KafkaSender). The
// producer is established lazily on the first send and dropped on any
// failure so the next attempt re-initializes — Python's
// send_messages/_init_kafka re-init cycle, with the shared loop owning
// the backoff/classification (sender.ConnectionReporter).
type Sender struct {
	rawConfig  map[string]any
	senderName string
	log        sender.Logger
	topic      string
	// keyBytes is the fixed message key (Python kafka_msg_key.encode("utf8"))
	// or nil when unset.
	keyBytes    []byte
	newProducer producerFactory
	newAdmin    adminFactory
	// deliveryDeadline is the client-side delivery deadline enforced across a
	// Produce call (its internal retries included). It defaults to
	// deliveryTimeout and is a field for tests (review item 2).
	deliveryDeadline time.Duration

	mu sync.Mutex
	// prod is the live producer; pc is the resolved producer config
	// (parsed once on first connect).
	prod     producer
	pc       *producerConfig
	pcErr    error
	pcParsed bool
	// onConnected/onDisconnected are the shared-loop lifecycle callbacks
	// (sender.ConnectionReporter), installed by NewLoop before Run. A
	// successful connect reports onConnected, resetting the shared
	// backoff exponent exactly like Python's mark_connected in
	// _init_kafka.
	onConnected    func()
	onDisconnected func(error)
}

// clientConfig is the resolved kafka_sender.py:_generate_client_config
// output, in the sender's own terms (the parity unit tests assert these
// fields; clientOpts projects them onto franz-go options).
type clientConfig struct {
	brokers          []string
	apiVersion       string // normalized "major.minor.patch", "" = auto/broker-negotiated
	securityProtocol string
	caFile           string
	certFile         string
	keyFile          string
	saslMechanism    string
	saslUsername     string
	saslPassword     string
}

// producerConfig is clientConfig plus kafka_sender.py:_generate_producer_config.
type producerConfig struct {
	clientConfig
	lingerMS    int
	compression string
	socks5Proxy string
	// keyed reports whether a fixed kafka_msg_key is configured; it picks
	// the partitioner's consistency requirement (see murmur2Partitioner).
	keyed bool
}

// New is the wiring-map constructor for output_type "kafka"
// (journalpump/senders/__init__.py). It reads max_send_interval and
// kafka_topic eagerly like Python's KafkaSender.__init__
// (config["kafka_topic"] raises KeyError when absent); the transport
// config is resolved on the first connect (Python's deferred
// _generate_producer_config).
func New(args sender.ConstructorArgs) (sender.Instance, error) {
	// kafka_sender.py:44 silences the client's internal logging
	// (logging.getLogger("kafka").setLevel(CRITICAL)). franz-go's default
	// logger discards all output; New does not install a global logger, so
	// concurrent construction cannot race on shared state.
	seconds := defaultMaxSendInterval
	if raw, present := args.Config["max_send_interval"]; present {
		parsed, ok := intervalSeconds(raw)
		if !ok {
			return sender.Instance{}, fmt.Errorf("max_send_interval: %v is not a number", raw)
		}
		seconds = parsed
	}
	rawTopic, present := args.Config["kafka_topic"]
	if !present {
		// Python: config["kafka_topic"] raises KeyError inside
		// KafkaSender.__init__.
		return sender.Instance{}, errors.New("config key 'kafka_topic' missing")
	}
	topic, ok := rawTopic.(string)
	if !ok {
		return sender.Instance{}, fmt.Errorf("kafka_topic: %v is not a string", rawTopic)
	}
	// Strict-at-load version validation (spec §6.1(c), review item 3): a
	// pre-0.10 kafka_api_version is rejected here, at construction, so a bad
	// config fails startup and reload instead of surfacing on the first send.
	// The rest of the transport config stays deferred (Python parses it in
	// _generate_producer_config on first use), so only the version is eager.
	if v, present := args.Config["kafka_api_version"]; present && v != nil {
		s, ok := v.(string)
		if !ok {
			return sender.Instance{}, fmt.Errorf("kafka_api_version: %v is not a string", v)
		}
		if _, err := normalizeKafkaVersion(s); err != nil {
			return sender.Instance{}, err
		}
	}
	s := &Sender{
		rawConfig:  args.Config,
		senderName: args.Name,
		log:        args.Log,
		topic:      topic,
	}
	if raw, present := args.Config["kafka_msg_key"]; present && truthy(raw) {
		keyStr, ok := raw.(string)
		if !ok {
			return sender.Instance{}, fmt.Errorf("kafka_msg_key: %v is not a string", raw)
		}
		// Python: kafka_msg_key.encode("utf8"); the bytes are the key.
		s.keyBytes = []byte(keyStr)
	}
	s.newProducer = s.connectProducer
	s.newAdmin = s.connectAdmin
	s.deliveryDeadline = deliveryTimeout
	return sender.Instance{
		Transport:       s,
		MaxSendInterval: time.Duration(seconds * float64(time.Second)),
		Type:            "KafkaSender",
		// The send path routes retriable Kafka errors to the silent
		// retriable path and everything else to the generic
		// "unexpected exception" path (kafka_sender.py:174-188).
		Classifier: sender.ClassifierFunc(Retriable),
	}, nil
}

// resolveClientConfig ports kafka_sender.py:_generate_client_config: the
// security protocol derivation, its validation, and the SSL/SASL
// credential requirements.
func resolveClientConfig(raw map[string]any) (clientConfig, error) {
	cfg := clientConfig{}
	var err error
	if v, present := raw["kafka_api_version"]; present && v != nil {
		s, ok := v.(string)
		if !ok {
			return cfg, fmt.Errorf("kafka_api_version: %v is not a string", v)
		}
		ver, err := normalizeKafkaVersion(s)
		if err != nil {
			return cfg, err
		}
		cfg.apiVersion = ver
	}
	if v, present := raw["kafka_address"]; present && v != nil {
		brokers, err := parseBrokers(v)
		if err != nil {
			return cfg, err
		}
		cfg.brokers = brokers
	}
	// security_protocol: explicit value (validated) or derived from ssl.
	if v, present := raw["security_protocol"]; present && truthy(v) {
		s, ok := v.(string)
		if !ok || !validSecurityProtocol(s) {
			return cfg, fmt.Errorf("Invalid security_protocol value: %v", v) //nolint:staticcheck // ValueError text parity with kafka_sender.py:67
		}
		cfg.securityProtocol = s
	} else if truthy(raw["ssl"]) {
		// Python uses `elif self.config.get("ssl"):` — truthiness, not an
		// exact-boolean check. Treating only `true` as SSL would silently
		// downgrade 1/"true"/"yes" to PLAINTEXT (review item B4).
		cfg.securityProtocol = "SSL"
	} else {
		cfg.securityProtocol = "PLAINTEXT"
	}
	// SSL settings for SSL and SASL_SSL. A malformed value is rejected
	// rather than silently discarded, so transport security is never
	// downgraded without an operator-visible error (item B4).
	if cfg.securityProtocol == "SSL" || cfg.securityProtocol == "SASL_SSL" {
		if cfg.caFile, err = configString(raw, "ca"); err != nil {
			return cfg, err
		}
	}
	if cfg.securityProtocol == "SSL" {
		if cfg.certFile, err = configString(raw, "certfile"); err != nil {
			return cfg, err
		}
		if cfg.keyFile, err = configString(raw, "keyfile"); err != nil {
			return cfg, err
		}
	}
	// SASL settings for SASL_PLAINTEXT and SASL_SSL.
	if cfg.securityProtocol == "SASL_PLAINTEXT" || cfg.securityProtocol == "SASL_SSL" {
		cfg.saslMechanism = "PLAIN"
		if v, present := raw["sasl_mechanism"]; present && truthy(v) {
			s, ok := v.(string)
			if !ok {
				return cfg, fmt.Errorf("sasl_mechanism: %v is not a string", v)
			}
			cfg.saslMechanism = s
		}
		if cfg.saslUsername, err = configString(raw, "sasl_plain_username"); err != nil {
			return cfg, err
		}
		if cfg.saslPassword, err = configString(raw, "sasl_plain_password"); err != nil {
			return cfg, err
		}
		if cfg.saslUsername == "" || cfg.saslPassword == "" {
			return cfg, errors.New("SASL username and password must be provided for SASL authentication")
		}
	}
	return cfg, nil
}

// resolveProducerConfig ports kafka_sender.py:_generate_producer_config:
// the client config plus linger_ms, the compression choice and the
// optional socks5 proxy.
func resolveProducerConfig(raw map[string]any) (producerConfig, error) {
	cc, err := resolveClientConfig(raw)
	if err != nil {
		return producerConfig{}, err
	}
	// Validate kafka_topic_config now so a malformed value fails the connect
	// instead of surfacing only after the producer is up (review item E13).
	if _, _, _, err := topicConfig(raw); err != nil {
		return producerConfig{}, err
	}
	p := producerConfig{
		clientConfig: cc,
		lingerMS:     lingerMS,
		// Deployed codec: snappy (kafka_sender.py chooses snappy when
		// python-snappy is importable and zstandard is absent).
		compression: compressionName,
	}
	if v, present := raw["socks5_proxy"]; present && truthy(v) {
		s, ok := v.(string)
		if !ok {
			return p, fmt.Errorf("socks5_proxy: %v is not a string", v)
		}
		p.socks5Proxy = s
	}
	if v, present := raw["kafka_msg_key"]; present && truthy(v) {
		p.keyed = true
	}
	return p, nil
}

// clientOpts projects the resolved config onto franz-go options common to
// the producer and the admin client: seed brokers, the version pin, the
// dialer (SOCKS5 + per-broker reconnect backoff + TLS when configured) and
// SASL. TLS is folded into the dialer rather than set via kgo.DialTLSConfig,
// which franz-go forbids alongside a custom Dialer (item 1) — otherwise a
// SOCKS5+TLS config would silently drop TLS.
func (pc producerConfig) clientOpts() ([]kgo.Opt, error) {
	opts := []kgo.Opt{kgo.SeedBrokers(pc.brokers...)}
	if pc.apiVersion != "" {
		versions, err := maxVersionsFor(pc.apiVersion)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.MaxVersions(versions))
	}
	d, err := pc.newDialer()
	if err != nil {
		return nil, err
	}
	opts = append(opts, kgo.Dialer(d.DialContext))
	if pc.securityProtocol == "SASL_PLAINTEXT" || pc.securityProtocol == "SASL_SSL" {
		mech, err := saslOption(pc)
		if err != nil {
			return nil, err
		}
		opts = append(opts, mech)
	}
	return opts, nil
}

// producerOpts adds the produce-semantics options to the common client
// options, mirroring kafka-python's _generate_producer_config and the
// review-item pins:
//
//   - acks=1 (LeaderAck); franz-go only permits acks != all with
//     DisableIdempotentWrite, matching Python's non-idempotent producer.
//   - linger_ms=500.
//   - Snappy compression (the deployed codec; raw-block framing).
//   - the kafka-python murmur2 partitioner (nil-key: random over all
//     partitions) instead of franz-go's Java-compatible default.
//   - request_timeout_ms=30000 as the produce request timeout,
//     delivery_timeout_ms=120000 as the record delivery timeout, retries
//     bounded at producerRetryMax, and a fixed 100 ms retry backoff to keep
//     the documented abort bound.
func (pc producerConfig) producerOpts() []kgo.Opt {
	return []kgo.Opt{
		// kafka-python produces to a not-yet-existing topic and lets the
		// broker's auto-create setting decide; franz-go defaults
		// AllowAutoTopicCreation to false, so opt in to preserve that
		// behavior (the Task 12 fixture's auto-create run).
		kgo.AllowAutoTopicCreation(),
		kgo.RequiredAcks(kgo.LeaderAck()),
		kgo.DisableIdempotentWrite(),
		kgo.ProducerLinger(time.Duration(pc.lingerMS) * time.Millisecond),
		kgo.ProducerBatchCompression(compressionCodec()),
		kgo.RecordPartitioner(murmur2Partitioner{keyed: pc.keyed}),
		kgo.ProduceRequestTimeout(requestTimeout),
		kgo.RecordDeliveryTimeout(deliveryTimeout),
		kgo.RecordRetries(producerRetryMax),
		kgo.RetryBackoffFn(func(int) time.Duration { return retryBackoff }),
	}
}

// compressionCodec is the kgo codec for the pinned compression name.
func compressionCodec() kgo.CompressionCodec {
	return kgo.SnappyCompression()
}

// buildTLSConfig loads the CA and client certificate material
// (kafka_sender.py ssl_cafile/ssl_certfile/ssl_keyfile).
func buildTLSConfig(pc producerConfig) (*tls.Config, error) {
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if pc.caFile != "" {
		pemBytes, err := os.ReadFile(pc.caFile)
		if err != nil {
			return nil, fmt.Errorf("ca: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pemBytes) {
			return nil, fmt.Errorf("ca: %q contains no valid certificates", pc.caFile)
		}
		tlsCfg.RootCAs = pool
	}
	if pc.certFile != "" {
		cert, err := tls.LoadX509KeyPair(pc.certFile, pc.keyFile)
		if err != nil {
			return nil, fmt.Errorf("certfile/keyfile: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}
	return tlsCfg, nil
}

// saslOption maps kafka-python's sasl_mechanism names onto franz-go's
// built-in SASL mechanisms (PLAIN, SCRAM-SHA-256/512).
func saslOption(pc producerConfig) (kgo.Opt, error) {
	switch pc.saslMechanism {
	case "PLAIN":
		return kgo.SASL(plain.Auth{User: pc.saslUsername, Pass: pc.saslPassword}.AsMechanism()), nil
	case "SCRAM-SHA-256":
		return kgo.SASL(scram.Auth{User: pc.saslUsername, Pass: pc.saslPassword}.AsSha256Mechanism()), nil
	case "SCRAM-SHA-512":
		return kgo.SASL(scram.Auth{User: pc.saslUsername, Pass: pc.saslPassword}.AsSha512Mechanism()), nil
	default:
		return nil, fmt.Errorf("Invalid sasl_mechanism value: %s", pc.saslMechanism) //nolint:staticcheck // ValueError text parity with kafka-python's mechanism validation
	}
}

// connectProducer builds a live franz-go producer. kgo.NewClient is lazy,
// so Ping is used to force the initial metadata/connection handshake under
// the caller's context: a successful Ping means the client is genuinely
// connected (Python's KafkaProducer.__init__ connects eagerly and raises on
// failure), and a cancelled context aborts promptly (franz-go's dials and
// metadata requests take the context).
func (s *Sender) connectProducer(ctx context.Context, pc producerConfig) (producer, error) {
	opts, err := pc.clientOpts()
	if err != nil {
		return nil, err
	}
	opts = append(opts, pc.producerOpts()...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	if err := pingWithClose(ctx, cl); err != nil {
		cl.Close()
		return nil, err
	}
	return &liveProducer{cl: cl, topic: s.topic, key: s.keyBytes}, nil
}

// connectAdmin builds a live ClusterAdmin over its own franz-go client.
func (s *Sender) connectAdmin(ctx context.Context, pc producerConfig) (admin, error) {
	opts, err := pc.clientOpts()
	if err != nil {
		return nil, err
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	if err := pingWithClose(ctx, cl); err != nil {
		cl.Close()
		return nil, err
	}
	return &liveAdmin{cl: cl, adm: kadm.NewClient(cl)}, nil
}

// doWithClose runs fn under ctx and, if ctx ends first, closes the client —
// the documented franz-go lever for an in-flight request (a request already
// written to a stalled broker is failed promptly by Close, which closes the
// connections). The worker is joined before returning, so no goroutine or
// retired client outlives the caller.
func doWithClose(ctx context.Context, cl *kgo.Client, fn func(context.Context) error) error {
	done := make(chan error, 1)
	go func() { done <- fn(ctx) }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		cl.Close()
		<-done
		return ctx.Err()
	}
}

// pingWithClose forces the initial metadata/connection handshake under ctx. A
// successful ping means the client is genuinely connected (Python's
// KafkaProducer.__init__ connects eagerly and raises on failure); a cancelled
// context aborts even a stalled in-flight metadata request via Close.
func pingWithClose(ctx context.Context, cl *kgo.Client) error {
	if err := doWithClose(ctx, cl, cl.Ping); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return err
	}
	return nil
}

// liveProducer owns a franz-go client and the resolved topic/key.
type liveProducer struct {
	cl    *kgo.Client
	topic string
	key   []byte
}

// Produce sends one batch with ProduceSync, which returns after every
// record is acked or failed. The per-record causes are aggregated so the
// loop's classification and health ring see them (review item 7).
func (p *liveProducer) Produce(ctx context.Context, batch [][]byte) error {
	recs := make([]*kgo.Record, len(batch))
	for i, body := range batch {
		recs[i] = &kgo.Record{Topic: p.topic, Key: p.key, Value: body}
	}
	results := p.cl.ProduceSync(ctx, recs...)
	causes := make([]error, 0, len(results))
	for _, r := range results {
		if r.Err != nil {
			causes = append(causes, r.Err)
		}
	}
	if len(causes) == 0 {
		return nil
	}
	return &sendErrors{causes: causes}
}

// Close closes the client, which closes every connection and fails any
// buffered records (kgo.Client.Close + failBufferedRecords). It returns
// promptly even against a stalled broker.
func (p *liveProducer) Close() error {
	p.cl.Close()
	return nil
}

// liveAdmin owns a franz-go client and its kadm client.
type liveAdmin struct {
	cl  *kgo.Client
	adm *kadm.Client
}

// CreateTopic creates the topic, returning the broker's error (a *kerr.Error)
// so the caller can tolerate TopicAlreadyExists.
func (a *liveAdmin) CreateTopic(ctx context.Context, partitions int32, replicationFactor int16, topic string) error {
	return doWithClose(ctx, a.cl, func(ctx context.Context) error {
		_, err := a.adm.CreateTopic(ctx, partitions, replicationFactor, nil, topic)
		return err
	})
}

func (a *liveAdmin) Close() error {
	a.cl.Close()
	return nil
}

// resolvedProducerConfig parses the raw config once.
func (s *Sender) resolvedProducerConfig() (*producerConfig, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.pcParsed {
		pc, err := resolveProducerConfig(s.rawConfig)
		if err != nil {
			s.pcErr = err
			s.pcParsed = true
			return nil, err
		}
		s.pc = &pc
		s.pcParsed = true
	}
	return s.pc, s.pcErr
}

// ensureProducer returns the live producer, connecting (and running the
// topic auto-create) on first use. A successful connect reports
// connected through the sender.ConnectionReporter seam, resetting the
// shared backoff exponent exactly like Python's mark_connected at the
// end of _init_kafka; a failure is returned so the shared loop marks the
// sender disconnected, backs off and retries the batch.
func (s *Sender) ensureProducer(ctx context.Context) (producer, error) {
	s.mu.Lock()
	if s.prod != nil {
		p := s.prod
		s.mu.Unlock()
		return p, nil
	}
	s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	pc, err := s.resolvedProducerConfig()
	if err != nil {
		return nil, err
	}
	s.logf("Initializing Kafka client, address: %r for %s", strings.Join(pc.brokers, ","), s.name())
	// Construction is cancellation-aware end to end: franz-go's dials take the
	// caller's context and the connect closes the client on cancellation, so a
	// stalled broker cannot hold shutdown/reload past the grace window (review
	// round-3 item 2, C7).
	p, err := s.newProducer(ctx, *pc)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	if s.prod == nil {
		s.prod = p
	} else {
		_ = p.Close()
	}
	res := s.prod
	s.mu.Unlock()
	// Python creates the topic after a successful connect, tolerating an
	// already-existing topic (kafka_sender.py:143-153).
	if err := s.maybeCreateTopic(ctx, *pc); err != nil {
		s.dropProducer()
		return nil, err
	}
	if cc := s.connectedObserver(); cc != nil {
		cc()
	}
	s.logf("Initialized Kafka client, address: %r for %s", strings.Join(pc.brokers, ","), s.name())
	return res, nil
}

// maybeCreateTopic creates the configured topic when BOTH
// kafka_topic_config values are present (kafka_sender.py:143-153).
// TopicAlreadyExists is tolerated; any other error is returned. The admin
// dial and the create call are context-aware (item C7).
func (s *Sender) maybeCreateTopic(ctx context.Context, pc producerConfig) error {
	numPartitions, repFactor, ok, err := topicConfig(s.rawConfig)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	a, err := s.newAdmin(ctx, pc)
	if err != nil {
		return err
	}
	defer func() { _ = a.Close() }()
	err = a.CreateTopic(ctx, numPartitions, repFactor, s.topic)
	if err != nil {
		if errors.Is(err, kerr.TopicAlreadyExists) {
			s.logf("Kafka topic %r already exists for %s", s.topic, s.name())
			return nil
		}
		return err
	}
	s.logf("Create Kafka topic, address: %r for %s", s.topic, s.name())
	return nil
}

// SendMessages implements sender.Sender: one ProduceSync batch per flush
// (thread A7). A nil return lets the shared loop advance the cursor to the
// batch's last cursor (ack-then-advance); a failure drops the producer and
// returns the error for the loop's classification/backoff/retry.
func (s *Sender) SendMessages(ctx context.Context, batch [][]byte, _ string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p, err := s.ensureProducer(ctx)
	if err != nil {
		return err
	}
	if err := s.sendBatch(ctx, p, batch); err != nil {
		s.dropProducer()
		return err
	}
	return nil
}

// sendBatch sends one batch under a client-side delivery deadline and is
// cancellation-aware. franz-go's RecordDeliveryTimeout bounds a record's
// total time, but a ProduceSync blocked on a stalled broker's response is
// only unblocked promptly (before that timeout) by closing the client, so
// once ctx is done the producer is closed — a documented library API that
// closes the connections and fails the buffered/in-flight records — and the
// worker is joined before returning. The producer is closed only once.
func (s *Sender) sendBatch(ctx context.Context, p producer, batch [][]byte) error {
	deadline := s.deliveryDeadline
	if deadline <= 0 {
		deadline = deliveryTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- p.Produce(ctx, batch) }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		// Detach first so the outer dropProducer does not close the same
		// producer twice, then close (aborting the in-flight produce) and
		// join the worker: no goroutine and no retired client outlives the
		// caller.
		s.detachProducer(p)
		_ = p.Close()
		<-done
		return ctx.Err()
	}
}

// sendErrors aggregates the individual per-record causes of a failed batch
// (franz-go's ProduceResults carries one error per record). It unwraps to
// every cause and renders them in the text so the loop's health ring sees
// the real cause, not just "Failed to deliver N messages" (review item 7).
type sendErrors struct {
	causes []error
}

func (e *sendErrors) Error() string {
	parts := make([]string, 0, len(e.causes))
	for _, c := range e.causes {
		if c != nil {
			parts = append(parts, c.Error())
		}
	}
	return fmt.Sprintf("failed to deliver %d messages: %s", len(e.causes), strings.Join(parts, "; "))
}

// Unwrap exposes the individual causes to errors.Is/As.
func (e *sendErrors) Unwrap() []error { return e.causes }

// retriable reports whether every cause is retriable (the batch-level
// classification kafka_sender applies to a failed flush).
func (e *sendErrors) retriable() bool {
	if len(e.causes) == 0 {
		return false
	}
	for _, c := range e.causes {
		if !Retriable(c) {
			return false
		}
	}
	return true
}

// MaintenanceOperations implements sender.Sender; kafka has none.
func (s *Sender) MaintenanceOperations(context.Context) error { return nil }

// Close releases the producer (io.Closer, called by the supervisor).
// franz-go's Close closes every connection and fails buffered records, so
// it returns promptly even against a stalled broker.
func (s *Sender) Close() error {
	s.mu.Lock()
	p := s.prod
	s.prod = nil
	s.mu.Unlock()
	if p == nil {
		return nil
	}
	return p.Close()
}

// SetConnectionObserver implements sender.ConnectionReporter.
func (s *Sender) SetConnectionObserver(connected func(), disconnected func(error)) {
	s.mu.Lock()
	s.onConnected = connected
	s.onDisconnected = disconnected
	s.mu.Unlock()
}

func (s *Sender) connectedObserver() func() {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.onConnected
}

// dropProducer closes and clears the producer so the next send
// re-initializes (Python's close(); self.kafka_producer = None).
func (s *Sender) dropProducer() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.prod != nil {
		_ = s.prod.Close()
		s.prod = nil
	}
}

// detachProducer clears s.prod when it is still p, without closing it, so
// the caller can close it exactly once (the send-abort path).
func (s *Sender) detachProducer(p producer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.prod == p {
		s.prod = nil
	}
}

func (s *Sender) name() string { return s.senderName }

func (s *Sender) logf(format string, args ...any) {
	if s.log == nil {
		return
	}
	s.log.Printf(format, args...)
}

// Retriable classifies a send error like Python's KAFKA_CONN_ERRORS:
// the retriable Kafka protocol errors plus socket timeout/TimeoutError
// are silent retriable; everything else is the unexpected-exception
// path (kafka_sender.py:183-188).
func Retriable(err error) bool {
	if err == nil {
		return false
	}
	// The sendErrors aggregate (item 7) wraps the same per-cause
	// classification.
	var agg *sendErrors
	if errors.As(err, &agg) {
		return agg.retriable()
	}
	// A cancelled or deadline-exceeded send (shutdown/reload or the client
	// delivery deadline) is transient: retriable, so the loop does not file
	// it as an unexpected exception.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	// franz-go's client-side produce errors: a closed client (shutdown/reload)
	// and records that timed out or exhausted their retries are transient.
	if errors.Is(err, kgo.ErrClientClosed) || errors.Is(err, kgo.ErrRecordTimeout) || errors.Is(err, kgo.ErrRecordRetries) {
		return true
	}
	// Network-level failures (Python socket.timeout, TimeoutError, and
	// the retriable kafka-python errors).
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	if errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) {
		return true
	}
	var ke *kerr.Error
	if errors.As(err, &ke) {
		return retriableCode(ke.Code)
	}
	return false
}

// retriableCode is exactly the set of Kafka protocol error codes that
// kafka-python 2.3.2 marks retriable=True (plus UnknownError, which
// journalpump adds explicitly to KAFKA_CONN_ERRORS). Deriving the set from
// the Python source is what keeps the health/backoff classification in
// parity: BrokerNotAvailable (8) and RebalanceInProgress (27) are NOT
// retriable in kafka-python (franz-go's kerr.Retriable says otherwise for
// BrokerNotAvailable), while ReplicaNotAvailable (9), NotController (41),
// ListenerNotFound (72) and ThrottlingQuotaExceeded (89) among others ARE
// (review item A3).
func retriableCode(code int16) bool {
	switch code {
	case -1, // UnknownError (journalpump's explicit addition)
		3,  // UnknownTopicOrPartition
		5,  // LeaderNotAvailable
		6,  // NotLeaderForPartition
		7,  // RequestTimedOut
		9,  // ReplicaNotAvailable
		13, // NetworkException
		14, // OffsetsLoadInProgress
		15, // ConsumerCoordinatorNotAvailable
		16, // NotCoordinatorForConsumer
		19, // NotEnoughReplicas
		20, // NotEnoughReplicasAfterAppend
		41, // NotController
		51, // ConcurrentTransactions
		56, // KafkaStorageError
		70, // FetchSessionIDNotFound
		71, // InvalidFetchSessionEpoch
		72, // ListenerNotFound
		74, // FencedLeaderEpoch
		75, // UnknownLeaderEpoch
		78, // OffsetNotAvailable
		80, // PreferredLeaderNotAvailable
		83, // EligibleLeadersNotAvailable
		84, // ElectionNotNeeded
		88, // UnstableOffsetCommit
		89, // ThrottlingQuotaExceeded
		// Codes kafka-python marks retriable that older clients had no
		// constant for:
		// UnknownTopicId, InconsistentTopicId, FetchSessionTopicId,
		// ShareSessionNotFound, InvalidShareSessionEpoch.
		100, 103, 106, 122, 123:
		return true
	default:
		return false
	}
}

// topicConfig extracts kafka_topic_config's num_partitions and
// replication_factor. ok is true only when BOTH are present and non-null
// (kafka_sender.py:143-146 — the admin create runs only then).
//
// A present but invalid value is an error, never a silent skip: kafka-python
// hands the values straight to the CreateTopics request encoding, where the
// Int32/Int16 fields reject a float, a string or an out-of-range integer.
// Silently skipping the create (or truncating a float, accepting a numeric
// string prefix, or narrowing an overflowing int) would leave the operator
// with a broken topic and no error (review item E13).
func topicConfig(raw map[string]any) (numPartitions int32, replicationFactor int16, ok bool, err error) {
	tc, present := raw["kafka_topic_config"]
	if !present || tc == nil {
		return 0, 0, false, nil
	}
	m, isMap := tc.(map[string]any)
	if !isMap {
		return 0, 0, false, fmt.Errorf("kafka_topic_config: %v is not a mapping", tc)
	}
	npv, npPresent := m["num_partitions"]
	rfv, rfPresent := m["replication_factor"]
	if !npPresent || npv == nil || !rfPresent || rfv == nil {
		// Python only creates the topic when both are non-None.
		return 0, 0, false, nil
	}
	np, err := configInt("kafka_topic_config.num_partitions", npv, math.MaxInt32, math.MinInt32)
	if err != nil {
		return 0, 0, false, err
	}
	rf, err := configInt("kafka_topic_config.replication_factor", rfv, math.MaxInt16, math.MinInt16)
	if err != nil {
		return 0, 0, false, err
	}
	return int32(np), int16(rf), true, nil
}

// validSecurityProtocol reports whether name is one of
// kafka_sender.py:66's accepted values.
func validSecurityProtocol(name string) bool {
	switch name {
	case "PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL":
		return true
	default:
		return false
	}
}

// versionCtors maps a normalized "major.minor.patch" version to the
// franz-go kversion constructor for that release. These are the broker
// versions our kafka_api_version pin can name; "auto" (an empty normalized
// value) instead lets franz-go negotiate via ApiVersions on every
// connection. The map starts at the 0.10.0 FLOOR: pre-0.10 brokers are not
// supported (the version-inference probes were dropped; charter §2
// deviation), and normalizeKafkaVersion rejects anything below it at
// configuration load.
var versionCtors = map[string]func() *kversion.Versions{
	"0.10.0": kversion.V0_10_0,
	"0.10.1": kversion.V0_10_1,
	"0.10.2": kversion.V0_10_2,
	"0.11.0": kversion.V0_11_0,
	"1.0.0":  kversion.V1_0_0,
	"1.1.0":  kversion.V1_1_0,
	"2.0.0":  kversion.V2_0_0,
	"2.1.0":  kversion.V2_1_0,
	"2.2.0":  kversion.V2_2_0,
	"2.3.0":  kversion.V2_3_0,
	"2.4.0":  kversion.V2_4_0,
	"2.5.0":  kversion.V2_5_0,
	"2.6.0":  kversion.V2_6_0,
	"2.7.0":  kversion.V2_7_0,
	"2.8.0":  kversion.V2_8_0,
	"3.0.0":  kversion.V3_0_0,
	"3.1.0":  kversion.V3_1_0,
	"3.2.0":  kversion.V3_2_0,
	"3.3.0":  kversion.V3_3_0,
	"3.4.0":  kversion.V3_4_0,
	"3.5.0":  kversion.V3_5_0,
	"3.6.0":  kversion.V3_6_0,
	"3.7.0":  kversion.V3_7_0,
	"3.8.0":  kversion.V3_8_0,
	"3.9.0":  kversion.V3_9_0,
	"4.0.0":  kversion.V4_0_0,
	"4.1.0":  kversion.V4_1_0,
	"4.2.0":  kversion.V4_2_0,
	"4.3.0":  kversion.V4_3_0,
	"4.4.0":  kversion.V4_4_0,
}

// maxVersionsFor returns the kversion pin for a normalized apiVersion.
func maxVersionsFor(apiVersion string) (*kversion.Versions, error) {
	ctor, ok := versionCtors[apiVersion]
	if !ok {
		return nil, fmt.Errorf("kafka_api_version: %q is not a valid version", apiVersion)
	}
	return ctor(), nil
}

// normalizeKafkaVersion maps the version strings kafka-python accepts onto
// a normalized "major.minor.patch" string (review item D10). kafka-python
// treats "auto" as None (broker negotiation) and otherwise splits the
// string on "." into an integer tuple, so "2.5" and "0.10.2" are valid. A
// version outside the supported set is rejected here rather than at dial
// time. An empty result means auto (see clientOpts).
func normalizeKafkaVersion(s string) (string, error) {
	trimmed := strings.TrimSpace(s)
	if trimmed == "" || strings.EqualFold(trimmed, "auto") {
		return "", nil
	}
	parts := strings.Split(trimmed, ".")
	nums := make([]int, 0, 3)
	for _, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 {
			return "", fmt.Errorf("kafka_api_version: %q is not a valid version", s)
		}
		nums = append(nums, n)
	}
	if len(nums) > 3 {
		// Kafka versions are major.minor.patch; tolerate trailing zero
		// components (0.10.2.0) but reject a meaningful fourth component.
		for _, extra := range nums[3:] {
			if extra != 0 {
				return "", fmt.Errorf("kafka_api_version: %q is not a valid version", s)
			}
		}
		nums = nums[:3]
	}
	for len(nums) < 3 {
		nums = append(nums, 0)
	}
	normalized := fmt.Sprintf("%d.%d.%d", nums[0], nums[1], nums[2])
	// Strict-at-load floor (review item 3): pre-0.10 brokers are unsupported.
	if nums[0] == 0 && nums[1] < 10 {
		return "", fmt.Errorf("kafka_api_version: %q is not a supported version (minimum 0.10.0)", s)
	}
	if _, ok := versionCtors[normalized]; !ok {
		return "", fmt.Errorf("kafka_api_version: %q is not a valid version", s)
	}
	return normalized, nil
}

// defaultKafkaPort is kafka-python's DEFAULT_KAFKA_PORT
// (kafka/conn.py); an address without a port uses it.
const defaultKafkaPort = "9092"

// parseBrokers ports kafka-python's collect_hosts + get_ip_port_afi onto
// Go: kafka_address may be a comma-separated string or a list, each entry
// may carry a leading scheme://, hostnames without a port default to
// 9092, and IPv6 literals may be bracketed ("[::1]:9092") or bare
// ("::1"). franz-go requires host:port, so entries are normalized (review
// item B6).
func parseBrokers(value any) ([]string, error) {
	var entries []string
	switch v := value.(type) {
	case string:
		entries = strings.Split(strings.TrimSpace(v), ",")
	case []any:
		for _, e := range v {
			s, ok := e.(string)
			if !ok {
				return nil, fmt.Errorf("kafka_address: %v is not a string", e)
			}
			entries = append(entries, s)
		}
	case []string:
		entries = v
	default:
		return nil, fmt.Errorf("kafka_address: %v is not a string or list", value)
	}
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		host, port, err := normalizeBroker(e)
		if err != nil {
			return nil, err
		}
		out = append(out, net.JoinHostPort(host, port))
	}
	return out, nil
}

// normalizeBroker resolves one address entry to a (host, port) pair with
// the default port applied.
func normalizeBroker(entry string) (host, port string, err error) {
	s := strings.TrimSpace(entry)
	// Strip a leading scheme, mimicking kafka-python's `re.sub('^.*://')`.
	if i := strings.Index(s, "://"); i >= 0 {
		s = s[i+3:]
	}
	if s == "" {
		return "", "", errors.New("kafka_address: empty broker entry")
	}
	if strings.HasPrefix(s, "[") {
		end := strings.Index(s, "]")
		if end < 0 {
			return "", "", fmt.Errorf("kafka_address: %q: missing closing bracket", entry)
		}
		host = s[1:end]
		rest := s[end+1:]
		if rest == "" {
			return host, defaultKafkaPort, nil
		}
		if !strings.HasPrefix(rest, ":") {
			return "", "", fmt.Errorf("kafka_address: %q: malformed bracketed address", entry)
		}
		return host, rest[1:], nil
	}
	if !strings.Contains(s, ":") {
		return s, defaultKafkaPort, nil
	}
	// A colon without brackets: an IPv6 literal (with or without a port
	// that reads as part of the literal) or a host:port pair. kafka-python
	// tries the whole string as IPv6 first.
	if net.ParseIP(s) != nil {
		return s, defaultKafkaPort, nil
	}
	host, port, err = net.SplitHostPort(s)
	if err != nil {
		return "", "", fmt.Errorf("kafka_address: %q: %w", entry, err)
	}
	return host, port, nil
}

// configString returns a config string value or "" when absent/null. A
// present non-string value is rejected instead of being silently dropped
// — for the SSL/SASL material the old lenient behavior could downgrade
// transport security with no operator-visible error (review item B4).
func configString(raw map[string]any, key string) (string, error) {
	v, present := raw[key]
	if !present || v == nil {
		return "", nil
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("%s: %v is not a string", key, v)
	}
	return s, nil
}

// configInt renders a config value as an int within [min,max], matching the
// integer semantics of kafka-python's protocol encoding: a JSON integer, a
// Go int, or a Python bool (an int subclass) are accepted; a float, a
// fractional JSON number, a numeric string or any out-of-range value is
// rejected rather than truncated or narrowed (review item E13).
func configInt(key string, v any, maxVal, minVal int64) (int64, error) {
	var n int64
	switch t := v.(type) {
	case int:
		n = int64(t)
	case int32:
		n = int64(t)
	case int64:
		n = t
	case json.Number:
		// Int64 rejects a fractional or exponent form ("2.5", "2e3"),
		// which Python's json would parse as a float and the encoder would
		// then reject.
		parsed, err := t.Int64()
		if err != nil {
			return 0, fmt.Errorf("%s: %v is not an integer", key, v)
		}
		n = parsed
	case bool:
		// Python bool is an int subclass: replication_factor=True is 1.
		if t {
			n = 1
		}
	default:
		return 0, fmt.Errorf("%s: %v is not an integer", key, v)
	}
	if n > maxVal || n < minVal {
		return 0, fmt.Errorf("%s: %d is out of range [%d, %d]", key, n, minVal, maxVal)
	}
	return n, nil
}

// truthy mirrors Python truthiness for decoded config values.
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
	case int:
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

// intervalSeconds renders a decoded config number as float seconds.
func intervalSeconds(value any) (float64, bool) {
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
