// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

// Package kafka implements the Kafka sender: the port of
// journalpump/senders/kafka_sender.py (plan Task 22, threads A7/A8). It
// maps the Python sender's kafka-python keyword configuration onto
// sarama and produces one batch per shared-loop flush through
// sarama.SyncProducer.SendMessages, so a broker ack advances the shared
// cursor (ack-then-advance) — exactly Python's mark_sent-after-flush
// contract.
//
// Shape difference from Python (thread A7): kafka-python sends each
// message, flushes, then waits on each FutureRecordMetadata with a 1 s
// timeout; sarama's SyncProducer.SendMessages is synchronous and
// already blocks until every record is acked, so the per-future
// timeout dance collapses into one call. The broker-side outcome
// (delivered payload bytes, key, partition) is identical.
//
// Compression (thread A8, plan Task 22): the Python sender prefers
// zstd when the `zstandard` module is importable, else snappy, else
// gzip. The deployment ships python-snappy and no zstandard
// (requirements.txt; pinned in the Task 12 fixture), so the deployed
// behavior is snappy — sarama.CompressionSnappy. sarama would support
// zstd too, but pinning to the deployed codec keeps the on-wire framing
// story simple; the zstd-preference fact is recorded rather than
// reproduced.
//
// The sender is NOT a DurableReporter: kafka's cursor advances on the
// broker ack (sarama's synchronous send), so there is no write-behind
// buffer whose flush boundary would lag the cursor. It is the file
// sender alone that keeps the durable (disk-buffered) deliverable.
package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/xdg-go/scram"

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
	// reconnectBackoffMS and reconnectBackoffMaxMS are
	// kafka_sender.py:59-60 (1 s / 10 s).
	reconnectBackoffMS    = 1000
	reconnectBackoffMaxMS = 10000
)

// Producer retry/timeout defaults mirroring kafka-python (review item D12):
// retries bounded by delivery_timeout_ms=120000 and request_timeout_ms=30000,
// with retry_backoff_ms=100. metadataRetryMax is how many metadata retries
// sarama performs within one connect (kafka-python retries until its
// backoff cap; the shared sender loop owns the unbounded outer retry).
const (
	deliveryTimeout  = 120 * time.Second
	requestTimeout   = 30 * time.Second
	retryBackoff     = 100 * time.Millisecond
	metadataRetryMax = 10
	producerRetryMax = 2147483647
)

// compressionName is the codec the deployment pins (snappy).
const compressionName = "snappy"

// producer is the producer-side seam onto sarama (thread B: the
// maintained sarama mock tooling — github.com/IBM/sarama/mocks — is the
// primary fake). *sarama.SyncProducer implements it in production.
type producer interface {
	SendMessages(msgs []*sarama.ProducerMessage) error
	Close() error
}

// admin is the cluster-admin seam (topic auto-create). *sarama.ClusterAdmin
// implements it in production.
type admin interface {
	CreateTopic(topic string, detail *sarama.TopicDetail, validateOnly bool) error
	Close() error
}

// producerFactory and adminFactory build the live clients from a
// resolved producerConfig. They are seams so tests inject sarama mocks
// or recording fakes.
type (
	producerFactory func(pc producerConfig) (producer, error)
	adminFactory    func(pc producerConfig) (admin, error)
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
	// key is the fixed message key (Python kafka_msg_key.encode("utf8"))
	// or nil when unset.
	key         sarama.Encoder
	newProducer producerFactory
	newAdmin    adminFactory
	// detectVersion probes the broker for its version when api_version is
	// unset, reproducing kafka-python's check_version (review item D11).
	detectVersion func(brokers []string, cfg *sarama.Config) (sarama.KafkaVersion, error)

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
// fields; saramaConfig projects them onto sarama).
type clientConfig struct {
	brokers          []string
	apiVersion       *sarama.KafkaVersion
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
	// (logging.getLogger("kafka").setLevel(CRITICAL)). sarama's
	// package-level Logger already defaults to a discarding logger
	// (log.New(io.Discard, ...)), which is that silence; we deliberately do
	// NOT assign it here because every constructor writing the shared
	// global while other clients read it is a data race (review item C9).
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
		s.key = sarama.ByteEncoder(keyStr)
	}
	s.newProducer = s.connectProducer
	s.newAdmin = s.connectAdmin
	s.detectVersion = detectBrokerVersion
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

// saramaConfig projects the resolved config onto a *sarama.Config.
func (pc producerConfig) saramaConfig() (*sarama.Config, error) {
	cfg := sarama.NewConfig()
	// api_version: an explicit value is used as-is; an unset one is
	// auto-detected from the broker before connecting (buildSaramaConfig,
	// review item D11).
	if pc.apiVersion != nil {
		cfg.Version = *pc.apiVersion
	}
	// reconnect_backoff_ms=1000 / reconnect_backoff_max_ms=10000
	// (kafka_sender.py:59-60). sarama's metadata retry backoff is the
	// equivalent reconnect knob; BackoffFunc supplies the exponential
	// growth with the 10 s cap that the fixed Backoff alone cannot express
	// (review item D12).
	cfg.Metadata.Retry.Max = metadataRetryMax
	cfg.Metadata.Retry.Backoff = reconnectBackoffMS * time.Millisecond
	maxBackoff := time.Duration(reconnectBackoffMaxMS) * time.Millisecond
	cfg.Metadata.Retry.BackoffFunc = func(retries, _ int) time.Duration {
		d := time.Duration(reconnectBackoffMS) * time.Millisecond
		for i := 0; i < retries; i++ {
			d *= 2
			if d >= maxBackoff {
				return maxBackoff
			}
		}
		return d
	}
	// Producer defaults: kafka-python retries (effectively unlimited) are
	// bounded by delivery_timeout_ms=120000 and request_timeout_ms=30000,
	// with retry_backoff_ms=100 (review item D12).
	cfg.Producer.Timeout = deliveryTimeout
	cfg.Producer.Retry.Max = producerRetryMax
	cfg.Producer.Retry.Backoff = retryBackoff
	cfg.Net.ReadTimeout = requestTimeout
	cfg.Net.WriteTimeout = requestTimeout

	switch pc.securityProtocol {
	case "SSL", "SASL_SSL":
		tlsCfg, err := buildTLSConfig(pc)
		if err != nil {
			return nil, err
		}
		cfg.Net.TLS.Enable = true
		cfg.Net.TLS.Config = tlsCfg
	}
	if pc.securityProtocol == "SASL_PLAINTEXT" || pc.securityProtocol == "SASL_SSL" {
		mechanism, err := saslMechanism(pc.saslMechanism)
		if err != nil {
			return nil, err
		}
		cfg.Net.SASL.Enable = true
		cfg.Net.SASL.Mechanism = mechanism
		cfg.Net.SASL.User = pc.saslUsername
		cfg.Net.SASL.Password = pc.saslPassword
		// sarama rejects a SCRAM config without a client generator before
		// it ever dials; supply working xdg-go/scram clients (item B5).
		switch mechanism {
		case sarama.SASLTypeSCRAMSHA256:
			cfg.Net.SASL.SCRAMClientGeneratorFunc = scramClientGenerator(scram.SHA256)
		case sarama.SASLTypeSCRAMSHA512:
			cfg.Net.SASL.SCRAMClientGeneratorFunc = scramClientGenerator(scram.SHA512)
		}
	}
	if pc.socks5Proxy != "" {
		dialer, err := socks5Dialer(pc.socks5Proxy)
		if err != nil {
			return nil, err
		}
		cfg.Net.Proxy.Enable = true
		cfg.Net.Proxy.Dialer = dialer
	}
	// Synthesis of the producer config.
	cfg.Producer.Return.Successes = true
	// Python kafka-python's default acks is 1 (leader ack) — sarama's
	// WaitForLocal.
	cfg.Producer.RequiredAcks = sarama.WaitForLocal
	cfg.Producer.Flush.Frequency = time.Duration(pc.lingerMS) * time.Millisecond
	cfg.Producer.Compression = sarama.CompressionSnappy
	// kafka-python's default partitioner is murmur2(key) % num_partitions.
	// sarama's own hash partitioners use FNV-1a, NOT murmur2, so the sender
	// installs the kafka-python-compatible partitioner (review item A1) to
	// keep keyed records on the same partitions as the Python sender.
	keyed := pc.keyed
	cfg.Producer.Partitioner = func(topic string) sarama.Partitioner {
		return &murmur2Partitioner{topic: topic, consistency: keyed}
	}
	return cfg, nil
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

// saslMechanism maps kafka-python's sasl_mechanism names onto sarama's.
func saslMechanism(name string) (sarama.SASLMechanism, error) {
	switch name {
	case "PLAIN":
		return sarama.SASLTypePlaintext, nil
	case "SCRAM-SHA-256":
		return sarama.SASLTypeSCRAMSHA256, nil
	case "SCRAM-SHA-512":
		return sarama.SASLTypeSCRAMSHA512, nil
	default:
		return "", fmt.Errorf("Invalid sasl_mechanism value: %s", name) //nolint:staticcheck // ValueError text parity with kafka-python's mechanism validation
	}
}

// connectProducer builds a live sarama SyncProducer.
func (s *Sender) connectProducer(pc producerConfig) (producer, error) {
	cfg, err := s.buildSaramaConfig(pc)
	if err != nil {
		return nil, err
	}
	return sarama.NewSyncProducer(pc.brokers, cfg)
}

// connectAdmin builds a live sarama ClusterAdmin.
func (s *Sender) connectAdmin(pc producerConfig) (admin, error) {
	cfg, err := s.buildSaramaConfig(pc)
	if err != nil {
		return nil, err
	}
	return sarama.NewClusterAdmin(pc.brokers, cfg)
}

// buildSaramaConfig projects the resolved config and, when api_version is
// unset, detects the broker version first — sarama's ApiVersionsRequest is
// fire-and-forget and does not adjust the configured version, so an
// explicit probe is required to match kafka-python's auto-detection
// (review item D11).
func (s *Sender) buildSaramaConfig(pc producerConfig) (*sarama.Config, error) {
	cfg, err := pc.saramaConfig()
	if err != nil {
		return nil, err
	}
	if pc.apiVersion == nil {
		ver, err := s.detectVersion(pc.brokers, cfg)
		if err != nil {
			return nil, err
		}
		cfg.Version = ver
	}
	return cfg, nil
}

// detectBrokerVersion opens each bootstrap broker with the oldest protocol
// that supports ApiVersions (0.10) and infers the broker release from the
// advertised API versions. It is the default detectVersion seam.
func detectBrokerVersion(brokers []string, cfg *sarama.Config) (sarama.KafkaVersion, error) {
	probeCfg := *cfg
	probeCfg.Version = sarama.V0_10_0_0
	var lastErr error
	for _, addr := range brokers {
		b := sarama.NewBroker(addr)
		if err := b.Open(&probeCfg); err != nil {
			lastErr = err
			_ = b.Close()
			continue
		}
		resp, err := b.ApiVersions(&sarama.ApiVersionsRequest{Version: 0})
		_ = b.Close()
		if err != nil {
			lastErr = err
			continue
		}
		return inferKafkaVersion(resp), nil
	}
	if lastErr == nil {
		lastErr = errors.New("no bootstrap brokers to probe")
	}
	return sarama.V0_10_0_0, fmt.Errorf("kafka_api_version auto-detection failed: %w", lastErr)
}

// inferKafkaVersion ports kafka-python's
// _infer_broker_version_from_api_versions: check the decision list from the
// newest release down and return the first whose probe API supports the
// required version. Newer releases than the list are reported as the
// highest known entry (2.6), exactly as kafka-python does.
func inferKafkaVersion(resp *sarama.ApiVersionsResponse) sarama.KafkaVersion {
	type apiRange struct{ min, max int16 }
	apis := make(map[int16]apiRange, len(resp.ApiKeys))
	for _, k := range resp.ApiKeys {
		apis[k.ApiKey] = apiRange{k.MinVersion, k.MaxVersion}
	}
	supports := func(key, version int16) bool {
		r, ok := apis[key]
		return ok && r.min <= version && version <= r.max
	}
	// (sarama version, api key, required max version), newest first.
	// API keys: Produce=0, Fetch=1, ListOffsets=2, Metadata=3,
	// OffsetFetch=9, DescribeAcls=29, DescribeClientQuotas=48.
	cases := []struct {
		version sarama.KafkaVersion
		key     int16
		need    int16
	}{
		{sarama.V2_6_0_0, 48, 0},
		{sarama.V2_5_0_0, 29, 2},
		{sarama.V2_4_0_0, 0, 8},
		{sarama.V2_3_0_0, 1, 11},
		{sarama.V2_2_0_0, 2, 5},
		{sarama.V2_1_0_0, 1, 10},
		{sarama.V2_0_0_0, 1, 8},
		{sarama.V1_1_0_0, 1, 7},
		{sarama.V1_0_0_0, 3, 5},
		{sarama.V0_11_0_0, 3, 4},
		{sarama.V0_10_2_0, 9, 2},
		{sarama.V0_10_1_0, 3, 2},
	}
	for _, c := range cases {
		if supports(c.key, c.need) {
			return c.version
		}
	}
	// ApiVersionsResponse only exists on 0.10+, so fall back to it.
	return sarama.V0_10_0_0
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
	// The dial is cancellation-aware: a stalled broker or SOCKS
	// negotiation must not hold shutdown/reload past the grace window
	// (review item C7). If the context wins the race the late producer is
	// closed by a drain goroutine so nothing leaks.
	p, err := runCtx(ctx, func() (producer, error) { return s.newProducer(*pc) }, func(p producer) {
		if p != nil {
			_ = p.Close()
		}
	})
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
// TopicAlreadyExists is tolerated; any other error is returned. Both the
// admin dial and the create call are cancellation-aware (item C7).
func (s *Sender) maybeCreateTopic(ctx context.Context, pc producerConfig) error {
	numPartitions, repFactor, ok := topicConfig(s.rawConfig)
	if !ok {
		return nil
	}
	a, err := runCtx(ctx, func() (admin, error) { return s.newAdmin(pc) }, func(a admin) {
		if a != nil {
			_ = a.Close()
		}
	})
	if err != nil {
		return err
	}
	defer func() { _ = a.Close() }()
	_, err = runCtx(ctx, func() (struct{}, error) {
		return struct{}{}, a.CreateTopic(s.topic, &sarama.TopicDetail{
			NumPartitions:     numPartitions,
			ReplicationFactor: repFactor,
		}, false)
	}, nil)
	if err != nil {
		if errors.Is(err, sarama.ErrTopicAlreadyExists) {
			s.logf("Kafka topic %r already exists for %s", s.topic, s.name())
			return nil
		}
		return err
	}
	s.logf("Create Kafka topic, address: %r for %s", s.topic, s.name())
	return nil
}

// runCtx runs fn and returns its result, but gives up as soon as ctx is
// done. If ctx wins the race, a drain goroutine waits for fn to finish so
// the abandoned value can be cleaned up (a client that was still dialing)
// without leaking the goroutine or the client (review item C7).
func runCtx[T any](ctx context.Context, fn func() (T, error), cleanup func(T)) (T, error) {
	type result struct {
		v   T
		err error
	}
	ch := make(chan result, 1)
	go func() {
		v, err := fn()
		ch <- result{v, err}
	}()
	select {
	case <-ctx.Done():
		go func() {
			r := <-ch
			if cleanup != nil {
				cleanup(r.v)
			}
		}()
		var zero T
		return zero, ctx.Err()
	case r := <-ch:
		return r.v, r.err
	}
}

// SendMessages implements sender.Sender: one sarama SyncProducer
// SendMessages per batch (thread A7). A nil return lets the shared loop
// advance the cursor to the batch's last cursor (ack-then-advance); a
// failure drops the producer and returns the error for the loop's
// classification/backoff/retry.
func (s *Sender) SendMessages(ctx context.Context, batch [][]byte, _ string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p, err := s.ensureProducer(ctx)
	if err != nil {
		return err
	}
	msgs := make([]*sarama.ProducerMessage, len(batch))
	for i, body := range batch {
		msgs[i] = &sarama.ProducerMessage{
			Topic: s.topic,
			Key:   s.key,
			Value: sarama.ByteEncoder(body),
		}
	}
	if err := p.SendMessages(msgs); err != nil {
		s.dropProducer()
		return err
	}
	return nil
}

// MaintenanceOperations implements sender.Sender; kafka has none.
func (s *Sender) MaintenanceOperations(context.Context) error { return nil }

// Close releases the producer (io.Closer, called by the supervisor).
func (s *Sender) Close() error {
	s.dropProducer()
	return nil
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
	// sarama's SyncProducer.SendMessages returns a ProducerErrors slice,
	// which has an Error method but no Unwrap, so errors.As cannot see
	// the per-message causes through it. Inspect each cause explicitly:
	// the batch is retriable only when every failed message failed for a
	// retriable reason (a permanent cause makes the whole batch
	// unexpected, matching Python treating the unclassified error as an
	// unexpected exception). Review item A2.
	var batch sarama.ProducerErrors
	if errors.As(err, &batch) {
		if len(batch) == 0 {
			return false
		}
		for _, pe := range batch {
			if !Retriable(pe.Err) {
				return false
			}
		}
		return true
	}
	// Network-level failures (Python socket.timeout, TimeoutError, and
	// the retriable kafka-python errors).
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	if errors.Is(err, os.ErrDeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	if errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) {
		return true
	}
	if errors.Is(err, sarama.ErrOutOfBrokers) {
		return true
	}
	// sarama client-side errors that stand in for kafka-python's retriable
	// client errors (NoBrokersAvailable/MetadataEmptyBrokerList/StaleMetadata
	// → ErrOutOfBrokers above; NodeNotReadyError → ErrNotConnected;
	// KafkaProtocolError/InvalidReceiveError/CorrelationIdError → truncated
	// or incomplete responses; UnknownError handled by retriableKError).
	if errors.Is(err, sarama.ErrNotConnected) ||
		errors.Is(err, sarama.ErrControllerNotAvailable) ||
		errors.Is(err, sarama.ErrIncompleteResponse) ||
		errors.Is(err, sarama.ErrInsufficientData) {
		return true
	}
	var kerr sarama.KError
	if errors.As(err, &kerr) {
		return retriableKError(kerr)
	}
	return false
}

// retriableKError is exactly the set of Kafka protocol error codes that
// kafka-python 2.3.2 marks retriable=True (plus UnknownError, which
// journalpump adds explicitly to KAFKA_CONN_ERRORS), mapped onto sarama's
// KError constants. Deriving the set from the Python source is what keeps
// the health/backoff classification in parity: BrokerNotAvailable (8) and
// RebalanceInProgress (27) are NOT retriable in kafka-python, while
// ReplicaNotAvailable (9), NotController (41), ListenerNotFound (72) and
// ThrottlingQuotaExceeded (89) among others ARE (review item A3).
func retriableKError(err sarama.KError) bool {
	switch err {
	case sarama.ErrUnknown, // -1, UnknownError (journalpump's explicit addition)
		sarama.ErrUnknownTopicOrPartition,         // 3
		sarama.ErrLeaderNotAvailable,              // 5
		sarama.ErrNotLeaderForPartition,           // 6
		sarama.ErrRequestTimedOut,                 // 7
		sarama.ErrReplicaNotAvailable,             // 9
		sarama.ErrNetworkException,                // 13
		sarama.ErrOffsetsLoadInProgress,           // 14
		sarama.ErrConsumerCoordinatorNotAvailable, // 15
		sarama.ErrNotCoordinatorForConsumer,       // 16
		sarama.ErrNotEnoughReplicas,               // 19
		sarama.ErrNotEnoughReplicasAfterAppend,    // 20
		sarama.ErrNotController,                   // 41
		sarama.ErrConcurrentTransactions,          // 51
		sarama.ErrKafkaStorageError,               // 56
		sarama.ErrFetchSessionIDNotFound,          // 70
		sarama.ErrInvalidFetchSessionEpoch,        // 71
		sarama.ErrListenerNotFound,                // 72
		sarama.ErrFencedLeaderEpoch,               // 74
		sarama.ErrUnknownLeaderEpoch,              // 75
		sarama.ErrOffsetNotAvailable,              // 78
		sarama.ErrPreferredLeaderNotAvailable,     // 80
		sarama.ErrEligibleLeadersNotAvailable,     // 83
		sarama.ErrElectionNotNeeded,               // 84
		sarama.ErrUnstableOffsetCommit,            // 88
		sarama.ErrThrottlingQuotaExceeded,         // 89
		// Codes kafka-python marks retriable but sarama has no named
		// constant for (UnknownTopicId, InconsistentTopicId,
		// FetchSessionTopicId, ShareSessionNotFound,
		// InvalidShareSessionEpoch).
		sarama.KError(100),
		sarama.KError(103),
		sarama.KError(106),
		sarama.KError(122),
		sarama.KError(123):
		return true
	default:
		return false
	}
}

// topicConfig extracts kafka_topic_config's num_partitions and
// replication_factor. ok is true only when BOTH are present and non-null
// (kafka_sender.py:143-146 — the admin create runs only then).
func topicConfig(raw map[string]any) (numPartitions int32, replicationFactor int16, ok bool) {
	tc, present := raw["kafka_topic_config"]
	if !present || tc == nil {
		return 0, 0, false
	}
	m, isMap := tc.(map[string]any)
	if !isMap {
		return 0, 0, false
	}
	np, npOK := configInt(m["num_partitions"])
	rf, rfOK := configInt(m["replication_factor"])
	if !npOK || !rfOK {
		return 0, 0, false
	}
	return np, int16(rf), true
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

// normalizeKafkaVersion maps the version strings kafka-python accepts onto
// a sarama.KafkaVersion (review item D10). kafka-python treats "auto" as
// None (broker detection) and otherwise splits the string on "." into an
// integer tuple, so "2.5" and "0.10.2" are valid. sarama's parser insists
// on exactly three components for 1.x+ and four for 0.x, so the tuple is
// zero-padded to that width; a nil result means auto-detection (see
// saramaConfig).
func normalizeKafkaVersion(s string) (*sarama.KafkaVersion, error) {
	trimmed := strings.TrimSpace(s)
	if trimmed == "" || strings.EqualFold(trimmed, "auto") {
		return nil, nil
	}
	parts := strings.Split(trimmed, ".")
	nums := make([]int, len(parts))
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil {
			return nil, fmt.Errorf("kafka_api_version: %q is not a valid version", s)
		}
		nums[i] = n
	}
	var normalized string
	if nums[0] == 0 {
		for len(nums) < 4 {
			nums = append(nums, 0)
		}
		if len(nums) != 4 {
			return nil, fmt.Errorf("kafka_api_version: %q is not a valid version", s)
		}
		normalized = fmt.Sprintf("%d.%d.%d.%d", nums[0], nums[1], nums[2], nums[3])
	} else {
		for len(nums) < 3 {
			nums = append(nums, 0)
		}
		if len(nums) != 3 {
			return nil, fmt.Errorf("kafka_api_version: %q is not a valid version", s)
		}
		normalized = fmt.Sprintf("%d.%d.%d", nums[0], nums[1], nums[2])
	}
	ver, err := sarama.ParseKafkaVersion(normalized)
	if err != nil {
		return nil, fmt.Errorf("kafka_api_version: %w", err)
	}
	return &ver, nil
}

// defaultKafkaPort is kafka-python's DEFAULT_KAFKA_PORT
// (kafka/conn.py); an address without a port uses it.
const defaultKafkaPort = "9092"

// parseBrokers ports kafka-python's collect_hosts + get_ip_port_afi onto
// Go: kafka_address may be a comma-separated string or a list, each entry
// may carry a leading scheme://, hostnames without a port default to
// 9092, and IPv6 literals may be bracketed ("[::1]:9092") or bare
// ("::1"). sarama requires host:port, so entries are normalized (review
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

// configInt renders a config value as an int32 with Python int()
// semantics (int, float truncation, numeric string, bool).
func configInt(v any) (int32, bool) {
	switch t := v.(type) {
	case int:
		return int32(t), true
	case int32:
		return t, true
	case int64:
		return int32(t), true
	case float64:
		return int32(t), true
	case bool:
		// Python bool is an int subclass: replication_factor=True is 1.
		if t {
			return 1, true
		}
		return 0, true
	case json.Number:
		if n, err := t.Int64(); err == nil {
			return int32(n), true
		}
		f, err := t.Float64()
		if err != nil {
			return 0, false
		}
		return int32(f), true
	case string:
		var n int
		if _, err := fmt.Sscanf(strings.TrimSpace(t), "%d", &n); err != nil {
			return 0, false
		}
		return int32(n), true
	default:
		return 0, false
	}
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
