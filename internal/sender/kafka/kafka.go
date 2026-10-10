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
	"math"
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
//
// producerRetryMax is BOUNDED. sarama allocates a per-partition
// []partitionRetryState of Producer.Retry.Max+1 entries on the first
// message to a partition (async_producer.go:612), so kafka-python's
// effectively-unlimited default (2^31-1) would reserve ~64 GiB per
// partition on amd64 and exhaust memory before any delivery (review item
// BLOCKER 1). The unbounded-retry behavior is instead provided by the
// shared sender loop (outer retry) plus the client-side delivery deadline
// below (review item 2), which sarama's broker-side Producer.Timeout does
// NOT provide.
const (
	deliveryTimeout  = 120 * time.Second
	requestTimeout   = 30 * time.Second
	retryBackoff     = 100 * time.Millisecond
	metadataRetryMax = 10
	producerRetryMax = 20
)

// compressionName is the codec the deployment pins (snappy).
const compressionName = "snappy"

// aborter is implemented by the live clients: it TERMINATES their network
// operations (closes every tracked connection) so a stalled broker cannot
// keep an in-flight call running past its deadline, before the worker is
// joined (review round-3 items 1 and 3). Test fakes may implement it too.
type aborter interface {
	abort()
}

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
// resolved producerConfig and the caller's context. The context aborts
// construction (dials, version probes, metadata, TLS/SASL) when it ends, so
// cancellation does not have to wait for each inner timeout to accumulate
// (review round-3 item 2). They are seams so tests inject sarama mocks or
// recording fakes.
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
	// key is the fixed message key (Python kafka_msg_key.encode("utf8"))
	// or nil when unset.
	key         sarama.Encoder
	newProducer producerFactory
	newAdmin    adminFactory
	// detectVersion probes the broker for its version when api_version is
	// unset, reproducing kafka-python's check_version (review item D11). The
	// context cancels the probe's dials and requests (review round-3 item 2).
	detectVersion func(ctx context.Context, brokers []string, cfg *sarama.Config) (sarama.KafkaVersion, error)
	// deliveryDeadline is the client-side delivery deadline enforced across
	// a SendMessages call (its internal retries included). It defaults to
	// deliveryTimeout and is a field for tests (review item 2).
	deliveryDeadline time.Duration

	// dialMu guards backoff; the per-address reconnect state must survive
	// across connect attempts so a later attempt only waits the REMAINING
	// interval (review round-3 item 4).
	dialMu  sync.Mutex
	backoff *backoffDialer

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

// saramaConfig projects the resolved config onto a *sarama.Config.
func (pc producerConfig) saramaConfig() (*sarama.Config, error) {
	cfg := sarama.NewConfig()
	// api_version: an explicit value is used as-is; an unset one is
	// auto-detected from the broker before connecting (buildSaramaConfig,
	// review item D11).
	if pc.apiVersion != nil {
		cfg.Version = *pc.apiVersion
	}
	// Metadata retries use the 100 ms retry_backoff_ms, NOT the reconnect
	// backoff: kafka-python keeps the two separate (retry_backoff_ms vs
	// reconnect_backoff_ms), so collapsing them here would double the delay
	// on top of the per-broker reconnect backoff the dialer already applies
	// (review round-3 item 4). The reconnect backoff lives in the dialer.
	cfg.Metadata.Retry.Max = metadataRetryMax
	cfg.Metadata.Retry.Backoff = retryBackoff
	// Producer.Timeout is the BROKER-side ack timeout sarama transmits in
	// Produce requests, so it maps to request_timeout_ms (30000), not
	// delivery_timeout_ms. The 120 s delivery_timeout_ms is enforced
	// client-side by the sender (sendBatch); Net.ReadTimeout does not
	// correct the timeout transmitted on the wire (review round-3 item 5).
	cfg.Producer.Timeout = requestTimeout
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
	// The dialer chain (per-broker reconnect backoff + SOCKS5 routing) is
	// installed by buildSaramaConfig, which also wraps it in the
	// cancellation-tracking dialer; it is not part of the pure projection
	// (review round-3 items 2 and 4).
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

// backoffDialerFor returns the sender's persistent per-broker backoff
// dialer, creating it on first use from the resolved config. Persisting it
// is what makes the reconnect backoff track the REMAINING interval across
// attempts rather than restarting at the initial value (review round-3
// item 4).
func (s *Sender) backoffDialerFor(pc producerConfig) (*backoffDialer, error) {
	s.dialMu.Lock()
	defer s.dialMu.Unlock()
	if s.backoff == nil {
		d, err := dialerFor(pc)
		if err != nil {
			return nil, err
		}
		s.backoff = d
	}
	return s.backoff, nil
}

// markConnectSuccess resets the reconnect history for the configured
// brokers after a COMPLETE connection (TCP + TLS + SASL + metadata) has
// succeeded.
func (s *Sender) markConnectSuccess(pc producerConfig) {
	if s.backoff != nil {
		s.backoff.markConnected(pc.brokers...)
	}
}

// markConnectFailure records a failed connection attempt — including a
// post-connect failure where the TCP dial itself succeeded — so the next
// attempt is delayed by the growing per-broker backoff (review round-3
// item 4).
func (s *Sender) markConnectFailure(pc producerConfig) {
	if s.backoff != nil {
		s.backoff.markFailed(pc.brokers...)
	}
}

// watchCancel aborts the tracking dialer (closing every tracked
// connection, so any in-flight dial, TLS/SASL handshake or metadata
// request fails at once) when ctx ends. The returned stop function ends
// the watch so a later context cancellation cannot kill a live client.
func watchCancel(ctx context.Context, td *trackingDialer) func() {
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			td.closeAll()
		case <-done:
		}
	}()
	return func() { close(done) }
}

// connectProducer builds a live sarama SyncProducer from a client it owns.
// The context aborts construction: the tracking dialer's connections are
// terminated the moment ctx ends, so version probes, metadata retries, DNS
// and SOCKS cannot accumulate past the caller's deadline (review round-3
// item 2).
func (s *Sender) connectProducer(ctx context.Context, pc producerConfig) (producer, error) {
	cfg, td, err := s.buildSaramaConfig(ctx, pc)
	if err != nil {
		return nil, err
	}
	stop := watchCancel(ctx, td)
	client, err := sarama.NewClient(pc.brokers, cfg)
	if err != nil {
		stop()
		td.closeAll()
		s.markConnectFailure(pc)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, err
	}
	s.markConnectSuccess(pc)
	p, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		stop()
		td.closeAll()
		_ = client.Close()
		return nil, err
	}
	return &liveProducer{sp: p, client: client, td: td, stop: stop}, nil
}

// connectAdmin builds a live sarama ClusterAdmin from a client it owns.
func (s *Sender) connectAdmin(ctx context.Context, pc producerConfig) (admin, error) {
	cfg, td, err := s.buildSaramaConfig(ctx, pc)
	if err != nil {
		return nil, err
	}
	stop := watchCancel(ctx, td)
	client, err := sarama.NewClient(pc.brokers, cfg)
	if err != nil {
		stop()
		td.closeAll()
		s.markConnectFailure(pc)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, err
	}
	s.markConnectSuccess(pc)
	a, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		stop()
		td.closeAll()
		_ = client.Close()
		return nil, err
	}
	return &liveAdmin{ca: a, client: client, td: td, stop: stop}, nil
}

// liveProducer wraps a sarama SyncProducer together with the client it
// owns and the tracking dialer of its connections. abort TERMINATES the
// in-flight network operations; Close aborts, then JOINS the producer and
// client goroutines (no timer, no abandoned closer) (review round-3 items
// 1 and 3).
type liveProducer struct {
	sp     sarama.SyncProducer
	client sarama.Client
	td     *trackingDialer
	stop   func()
	once   sync.Once
}

func (p *liveProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	return p.sp.SendMessages(msgs)
}

// abort terminates every connection so a stalled broker's in-flight
// produce/retry fails immediately (review round-3 item 1).
func (p *liveProducer) abort() { p.td.closeAll() }

func (p *liveProducer) Close() error {
	p.once.Do(func() {
		p.stop()
		p.td.closeAll()
		_ = p.sp.Close()
		_ = p.client.Close()
	})
	return nil
}

// liveAdmin wraps a sarama ClusterAdmin with the client it owns and its
// tracking dialer, with the same abort-then-join Close contract.
type liveAdmin struct {
	ca     sarama.ClusterAdmin
	client sarama.Client
	td     *trackingDialer
	stop   func()
	once   sync.Once
}

func (a *liveAdmin) CreateTopic(topic string, detail *sarama.TopicDetail, validateOnly bool) error {
	return a.ca.CreateTopic(topic, detail, validateOnly)
}

func (a *liveAdmin) abort() { a.td.closeAll() }

func (a *liveAdmin) Close() error {
	a.once.Do(func() {
		a.stop()
		a.td.closeAll()
		_ = a.ca.Close()
		_ = a.client.Close()
	})
	return nil
}

// abortFn returns v's abort method when v implements aborter, else nil.
func abortFn(v any) func() {
	if a, ok := v.(aborter); ok {
		return a.abort
	}
	return nil
}

// buildSaramaConfig projects the resolved config, installs the
// cancellation-tracking dialer chain and, when api_version is unset,
// detects the broker version first — sarama's ApiVersionsRequest is
// fire-and-forget and does not adjust the configured version, so an
// explicit probe is required to match kafka-python's auto-detection
// (review item D11). It returns the tracking dialer so the caller can
// terminate the client's connections on shutdown.
func (s *Sender) buildSaramaConfig(ctx context.Context, pc producerConfig) (*sarama.Config, *trackingDialer, error) {
	cfg, err := pc.saramaConfig()
	if err != nil {
		return nil, nil, err
	}
	backoff, err := s.backoffDialerFor(pc)
	if err != nil {
		return nil, nil, err
	}
	td := newTrackingDialer(ctx, backoff)
	// sarama only consults Net.Proxy.Dialer when Net.Proxy.Enable is set, so
	// the chain is installed unconditionally.
	cfg.Net.Proxy.Enable = true
	cfg.Net.Proxy.Dialer = td
	if pc.apiVersion == nil {
		// The probe dials and requests run under the construction context;
		// watchCancel closes the tracked connections the moment ctx ends so a
		// stalled probe cannot hold construction past the caller's deadline
		// (review round-3 item 2).
		stop := watchCancel(ctx, td)
		ver, err := s.detectVersion(ctx, pc.brokers, cfg)
		stop()
		if err != nil {
			return nil, nil, err
		}
		cfg.Version = ver
	}
	// A detected (or configured) pre-1.0 version cannot use SASLHandshake v1:
	// sarama would reject the handshake locally as ErrUnsupportedVersion.
	// Fall back to v0 for those brokers (review item 4).
	if cfg.Net.SASL.Enable && !cfg.Version.IsAtLeast(sarama.V1_0_0_0) {
		cfg.Net.SASL.Version = sarama.SASLHandshakeV0
	}
	return cfg, td, nil
}

// probeConfig builds the version-probe config: a shallow copy at the oldest
// ApiVersions-capable protocol, with SASL DISABLED. ApiVersions (and the
// legacy pre-0.10 probes) are pre-authentication requests, and opening a
// probe broker with SASL enabled at Version 0.10 makes sarama reject its own
// SASLHandshake v1 locally (ErrUnsupportedVersion, v1 needs Version >= 1.0),
// so PLAIN and both SCRAM would fail whenever kafka_api_version is unset
// (review item 4). TLS is transport-level and stays enabled.
func probeConfig(cfg *sarama.Config) *sarama.Config {
	probe := *cfg
	probe.Version = sarama.V0_10_0_0
	probe.ApiVersionsRequest = false
	probe.Net.SASL.Enable = false
	probe.Net.SASL.Handshake = false
	probe.Net.SASL.SCRAMClientGeneratorFunc = nil
	return &probe
}

// detectBrokerVersion opens each bootstrap broker and determines its release,
// porting kafka-python's check_version: an ApiVersions probe first, then the
// legacy pre-0.10 probes (ListGroups/FindCoordinator/OffsetFetch/Metadata) so
// a 0.8/0.9 deployment still connects (review item D11, item 5). It is the
// default detectVersion seam.
func detectBrokerVersion(ctx context.Context, brokers []string, cfg *sarama.Config) (sarama.KafkaVersion, error) {
	probe := probeConfig(cfg)
	var lastErr error
	for _, addr := range brokers {
		if err := ctx.Err(); err != nil {
			return sarama.V0_10_0_0, err
		}
		ver, err := probeOneBroker(ctx, addr, probe)
		if err == nil {
			return ver, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = errors.New("no bootstrap brokers to probe")
	}
	return sarama.V0_10_0_0, fmt.Errorf("kafka_api_version auto-detection failed: %w", lastErr)
}

// probeOneBroker determines one broker's version. The ApiVersions attempt and
// the legacy fallback each use their own connection, because a failed probe on
// an old broker may have left the socket unusable. The probe's dialer is the
// construction tracking dialer, so ctx cancellation terminates the probe's
// connections (review round-3 item 2).
func probeOneBroker(ctx context.Context, addr string, probe *sarama.Config) (sarama.KafkaVersion, error) {
	if err := ctx.Err(); err != nil {
		return sarama.KafkaVersion{}, err
	}
	if ver, err := probeViaAPIVersions(addr, probe); err == nil {
		return ver, nil
	}
	return probeLegacy(addr, probe)
}

// probeViaAPIVersions sends ApiVersions v0 and infers the release from the
// advertised API ranges. An ApiVersions response carrying an error (e.g.
// UnsupportedVersion from a pre-0.10 broker) is NOT inferred from (review
// item 5).
func probeViaAPIVersions(addr string, probe *sarama.Config) (sarama.KafkaVersion, error) {
	b := sarama.NewBroker(addr)
	if err := b.Open(probe); err != nil {
		_ = b.Close()
		return sarama.KafkaVersion{}, err
	}
	defer func() { _ = b.Close() }()
	resp, err := b.ApiVersions(&sarama.ApiVersionsRequest{Version: 0})
	if err != nil {
		return sarama.KafkaVersion{}, err
	}
	// An error carrying response (e.g. UnsupportedVersion from a pre-0.10
	// broker) must NOT be inferred from; fall through to the legacy probes.
	if resp.ErrorCode != int16(sarama.ErrNoError) {
		return sarama.KafkaVersion{}, sarama.KError(resp.ErrorCode)
	}
	return inferKafkaVersion(resp), nil
}

// probeLegacy ports kafka-python's pre-0.10 VERSION_CHECKS: the first request
// the broker answers (in newest-first order) identifies the release. sarama
// has no constants below 0.8.2, so the 0.8.1/0.8.0 probes map onto the
// closest available version.
func probeLegacy(addr string, probe *sarama.Config) (sarama.KafkaVersion, error) {
	type legacyCase struct {
		version sarama.KafkaVersion
		probe   func(*sarama.Broker) error
	}
	// (broker version, request) newest first, mirroring VERSION_CHECKS.
	cases := []legacyCase{
		{sarama.V0_9_0_0, func(b *sarama.Broker) error {
			_, err := b.ListGroups(&sarama.ListGroupsRequest{Version: 0})
			return err
		}},
		{sarama.V0_8_2_0, func(b *sarama.Broker) error {
			_, err := b.FindCoordinator(&sarama.FindCoordinatorRequest{
				Version:         0,
				CoordinatorKey:  "kafka-python-default-group",
				CoordinatorType: sarama.CoordinatorGroup,
			})
			return err
		}},
		{sarama.V0_8_2_0, func(b *sarama.Broker) error {
			_, err := b.FetchOffset(&sarama.OffsetFetchRequest{
				Version:       0,
				ConsumerGroup: "kafka-python-default-group",
			})
			return err
		}},
		{sarama.V0_8_2_0, func(b *sarama.Broker) error {
			_, err := b.GetMetadata(&sarama.MetadataRequest{Version: 0})
			return err
		}},
	}
	var lastErr error
	for _, c := range cases {
		b := sarama.NewBroker(addr)
		if err := b.Open(probe); err != nil {
			_ = b.Close()
			lastErr = err
			break // the broker is unreachable; no legacy probe will help
		}
		err := c.probe(b)
		_ = b.Close()
		if err == nil {
			return c.version, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = errors.New("no legacy probe succeeded")
	}
	return sarama.KafkaVersion{}, lastErr
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
	// Construction is cancellation-aware end to end: the caller's context is
	// threaded through the factory (version probes, metadata retries, DNS,
	// SOCKS), whose tracking dialer terminates the connections the moment ctx
	// ends, so a stalled broker cannot hold shutdown/reload past the grace
	// window (review round-3 items 2 and C7). If the context wins the race the
	// late producer is closed by the cleanup so nothing leaks.
	p, err := runCancellable(ctx, func() (producer, error) { return s.newProducer(ctx, *pc) }, nil, closeProducerValue)
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
		s.markConnectFailure(*pc)
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
	numPartitions, repFactor, ok, err := topicConfig(s.rawConfig)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	a, err := runCancellable(ctx, func() (admin, error) { return s.newAdmin(ctx, pc) }, nil, closeAdminValue)
	if err != nil {
		return err
	}
	defer func() { _ = a.Close() }()
	// abortFn terminates the in-flight CreateTopic's network operations,
	// then runCancellable JOINS the worker so a cancelled create cannot leave
	// a goroutine behind (review round-3 items 1 and 3).
	_, err = runCancellable(ctx, func() (struct{}, error) {
		return struct{}{}, a.CreateTopic(s.topic, &sarama.TopicDetail{
			NumPartitions:     numPartitions,
			ReplicationFactor: repFactor,
		}, false)
	}, abortFn(a), nil)
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

// runCancellable runs fn in a worker goroutine. If ctx wins the race it
// first calls abort — to cancel the underlying network operation (closing a
// half-open client) — then JOINS the worker before returning, so no
// goroutine and no retired client outlives the caller (shutdown/reload must
// not leak workers) and a late value is still cleaned up (review items C7,
// 3). A nil abort is allowed for operations already bounded by a dial
// timeout.
func runCancellable[T any](ctx context.Context, fn func() (T, error), abort func(), cleanup func(T)) (T, error) {
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
	case r := <-ch:
		return r.v, r.err
	case <-ctx.Done():
		if abort != nil {
			abort()
		}
		r := <-ch // join the worker: no leak, no retired client left behind
		if cleanup != nil {
			cleanup(r.v)
		}
		var zero T
		return zero, ctx.Err()
	}
}

// closeProducerValue is the runCancellable cleanup for a late producer.
func closeProducerValue(p producer) {
	if p != nil {
		_ = p.Close()
	}
}

// closeAdminValue is the runCancellable cleanup for a late admin client.
func closeAdminValue(a admin) {
	if a != nil {
		_ = a.Close()
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
	if err := s.sendBatch(ctx, p, msgs); err != nil {
		s.dropProducer()
		return err
	}
	return nil
}

// sendBatch sends one batch under a client-side delivery deadline and is
// cancellation-aware. sarama's Producer.Timeout is the BROKER-side ack
// timeout and bounds neither the client's retry loop nor SendMessages, so
// the delivery_timeout_ms parity is enforced here: once ctx is done or the
// deadline elapses the in-flight produce is TERMINATED (the producer's
// connections are closed, so the stalled ack and any pending retry fail at
// once) BEFORE the worker is joined; the producer is closed only after the
// join (review round-3 item 1).
func (s *Sender) sendBatch(ctx context.Context, p producer, msgs []*sarama.ProducerMessage) error {
	deadline := s.deliveryDeadline
	if deadline <= 0 {
		deadline = deliveryTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()
	_, err := runCancellable(ctx, func() (struct{}, error) {
		return struct{}{}, p.SendMessages(msgs)
	}, abortFn(p), func(struct{}) {
		// Detach first so the outer dropProducer does not close the same
		// producer twice, then join its goroutines.
		s.detachProducer(p)
		_ = p.Close()
	})
	return wrapProducerErrors(err)
}

// sendErrors aggregates the individual per-message causes of a sarama
// ProducerErrors batch. sarama's ProducerErrors only implements Error()
// ("Failed to deliver N messages"), so the causes were invisible to
// errors.As and to the loop's health ring; this aggregate unwraps to every
// cause and renders them in the text (review item 7).
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

// wrapProducerErrors turns sarama's batch error into a sendErrors so the
// individual causes survive to the loop's classification and health ring.
func wrapProducerErrors(err error) error {
	var pes sarama.ProducerErrors
	if !errors.As(err, &pes) {
		return err
	}
	causes := make([]error, 0, len(pes))
	for _, pe := range pes {
		if pe != nil {
			causes = append(causes, pe.Err)
		}
	}
	return &sendErrors{causes: causes}
}

// MaintenanceOperations implements sender.Sender; kafka has none.
func (s *Sender) MaintenanceOperations(context.Context) error { return nil }

// Close releases the producer (io.Closer, called by the supervisor). It
// TERMINATES the producer's outstanding network operations first, then
// JOINS every resource it owns (the producer and its client goroutines)
// before returning — no timer, so a stalled broker cannot leave a retired
// worker and client alive behind a released Close (review round-3 item 3).
func (s *Sender) Close() error {
	s.mu.Lock()
	p := s.prod
	s.prod = nil
	s.mu.Unlock()
	if p == nil {
		return nil
	}
	if a, ok := p.(aborter); ok {
		a.abort()
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
	if errors.Is(err, context.Canceled) {
		return true
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
