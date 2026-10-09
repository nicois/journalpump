// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"syscall"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"

	"github.com/Aiven-Open/journalpump/internal/sender"
)

// newSender builds a KafkaSender from a raw config map.
func newSender(t *testing.T, raw map[string]any) *Sender {
	t.Helper()
	inst, err := New(sender.ConstructorArgs{Name: "test-sender", Config: raw})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	s, ok := inst.Transport.(*Sender)
	if !ok {
		t.Fatalf("transport type = %T, want *kafka.Sender", inst.Transport)
	}
	return s
}

// baseConfig is the minimal valid kafka_sender.py config.
func baseConfig() map[string]any {
	return map[string]any{
		"kafka_address": "localhost:9092",
		"kafka_topic":   "test-topic",
	}
}

// --- KafkaSender.__init__ parity (test/unit/senders/test_kafka_sender.py) ---

func TestBasicInitialization(t *testing.T) {
	inst, err := New(sender.ConstructorArgs{Name: "test-sender", Config: baseConfig()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	s := inst.Transport.(*Sender)
	if s.key != nil {
		t.Errorf("key = %v, want nil", s.key)
	}
	if s.topic != "test-topic" {
		t.Errorf("topic = %q, want test-topic", s.topic)
	}
	if inst.Type != "KafkaSender" {
		t.Errorf("Type = %q, want KafkaSender", inst.Type)
	}
}

func TestInitializationWithMessageKey(t *testing.T) {
	raw := baseConfig()
	raw["kafka_msg_key"] = "my-key"
	s := newSender(t, raw)
	if s.key == nil {
		t.Fatal("key = nil, want encoded key")
	}
	got, err := s.key.Encode()
	if err != nil {
		t.Fatalf("key.Encode: %v", err)
	}
	if string(got) != "my-key" {
		t.Errorf("key = %q, want my-key", got)
	}
	if s.topic != "test-topic" {
		t.Errorf("topic = %q, want test-topic", s.topic)
	}
}

func TestInitializationWithEmptyMessageKeyIsNil(t *testing.T) {
	// Python: `kafka_msg_key.encode("utf8") if kafka_msg_key else None` — an
	// empty string is falsy and yields None.
	raw := baseConfig()
	raw["kafka_msg_key"] = ""
	s := newSender(t, raw)
	if s.key != nil {
		t.Errorf("key = %v, want nil for empty kafka_msg_key", s.key)
	}
}

func TestInitializationWithMaxSendInterval(t *testing.T) {
	raw := baseConfig()
	raw["max_send_interval"] = 1.5
	inst, err := New(sender.ConstructorArgs{Name: "test-sender", Config: raw})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got, want := inst.MaxSendInterval, 1500*time.Millisecond; got != want {
		t.Errorf("MaxSendInterval = %v, want %v", got, want)
	}
}

func TestInitializationDefaultMaxSendInterval(t *testing.T) {
	inst, err := New(sender.ConstructorArgs{Name: "test-sender", Config: baseConfig()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got, want := inst.MaxSendInterval, 300*time.Millisecond; got != want {
		t.Errorf("MaxSendInterval = %v, want %v", got, want)
	}
}

func TestInitializationMissingTopic(t *testing.T) {
	_, err := New(sender.ConstructorArgs{Name: "test-sender", Config: map[string]any{"kafka_address": "localhost:9092"}})
	if err == nil {
		t.Fatal("New with no kafka_topic: want error")
	}
	if got := err.Error(); got != "config key 'kafka_topic' missing" {
		t.Errorf("error = %q, want the Python KeyError message", got)
	}
}

// --- _generate_client_config parity ---

func TestBasicClientConfig(t *testing.T) {
	cfg, err := resolveClientConfig(baseConfig())
	if err != nil {
		t.Fatalf("resolveClientConfig: %v", err)
	}
	if len(cfg.brokers) != 1 || cfg.brokers[0] != "localhost:9092" {
		t.Errorf("brokers = %v, want [localhost:9092]", cfg.brokers)
	}
	if cfg.securityProtocol != "PLAINTEXT" {
		t.Errorf("securityProtocol = %q, want PLAINTEXT", cfg.securityProtocol)
	}
}

func TestClientConfigMultiBrokerAddress(t *testing.T) {
	raw := baseConfig()
	raw["kafka_address"] = "a:9092, b:9092"
	cfg, err := resolveClientConfig(raw)
	if err != nil {
		t.Fatalf("resolveClientConfig: %v", err)
	}
	if len(cfg.brokers) != 2 || cfg.brokers[0] != "a:9092" || cfg.brokers[1] != "b:9092" {
		t.Errorf("brokers = %v, want [a:9092 b:9092]", cfg.brokers)
	}
}

func TestClientConfigExplicitSSL(t *testing.T) {
	raw := baseConfig()
	raw["security_protocol"] = "SSL"
	raw["ca"] = "/path/to/ca.pem"
	raw["certfile"] = "/path/to/cert.pem"
	raw["keyfile"] = "/path/to/key.pem"
	cfg, err := resolveClientConfig(raw)
	if err != nil {
		t.Fatalf("resolveClientConfig: %v", err)
	}
	if cfg.securityProtocol != "SSL" {
		t.Errorf("securityProtocol = %q, want SSL", cfg.securityProtocol)
	}
	if cfg.caFile != "/path/to/ca.pem" || cfg.certFile != "/path/to/cert.pem" || cfg.keyFile != "/path/to/key.pem" {
		t.Errorf("ssl material = %q/%q/%q", cfg.caFile, cfg.certFile, cfg.keyFile)
	}
}

func TestClientConfigSSLDerivedFromSSLFlag(t *testing.T) {
	raw := baseConfig()
	raw["ssl"] = true
	cfg, err := resolveClientConfig(raw)
	if err != nil {
		t.Fatalf("resolveClientConfig: %v", err)
	}
	if cfg.securityProtocol != "SSL" {
		t.Errorf("securityProtocol = %q, want SSL", cfg.securityProtocol)
	}
}

func TestClientConfigExplicitProtocolBeatsSSLFlag(t *testing.T) {
	raw := baseConfig()
	raw["ssl"] = true
	raw["security_protocol"] = "PLAINTEXT"
	cfg, err := resolveClientConfig(raw)
	if err != nil {
		t.Fatalf("resolveClientConfig: %v", err)
	}
	if cfg.securityProtocol != "PLAINTEXT" {
		t.Errorf("securityProtocol = %q, want explicit PLAINTEXT", cfg.securityProtocol)
	}
}

func TestClientConfigInvalidSecurityProtocol(t *testing.T) {
	raw := baseConfig()
	raw["security_protocol"] = "SASL_SSL_PLAIN"
	_, err := resolveClientConfig(raw)
	if err == nil {
		t.Fatal("invalid security_protocol: want error")
	}
	if got := err.Error(); got != "Invalid security_protocol value: SASL_SSL_PLAIN" {
		t.Errorf("error = %q", got)
	}
}

func TestClientConfigSASLPlaintext(t *testing.T) {
	raw := baseConfig()
	raw["security_protocol"] = "SASL_PLAINTEXT"
	raw["sasl_mechanism"] = "PLAIN"
	raw["sasl_plain_username"] = "user"
	raw["sasl_plain_password"] = "password"
	cfg, err := resolveClientConfig(raw)
	if err != nil {
		t.Fatalf("resolveClientConfig: %v", err)
	}
	if cfg.securityProtocol != "SASL_PLAINTEXT" || cfg.saslMechanism != "PLAIN" {
		t.Errorf("protocol/mechanism = %q/%q", cfg.securityProtocol, cfg.saslMechanism)
	}
	if cfg.saslUsername != "user" || cfg.saslPassword != "password" {
		t.Errorf("credentials = %q/%q", cfg.saslUsername, cfg.saslPassword)
	}
}

func TestClientConfigSASLSSL(t *testing.T) {
	raw := baseConfig()
	raw["security_protocol"] = "SASL_SSL"
	raw["sasl_mechanism"] = "SCRAM-SHA-256"
	raw["sasl_plain_username"] = "user"
	raw["sasl_plain_password"] = "password"
	raw["ca"] = "/path/to/ca.pem"
	cfg, err := resolveClientConfig(raw)
	if err != nil {
		t.Fatalf("resolveClientConfig: %v", err)
	}
	if cfg.securityProtocol != "SASL_SSL" || cfg.saslMechanism != "SCRAM-SHA-256" {
		t.Errorf("protocol/mechanism = %q/%q", cfg.securityProtocol, cfg.saslMechanism)
	}
	if cfg.caFile != "/path/to/ca.pem" {
		t.Errorf("caFile = %q, want /path/to/ca.pem", cfg.caFile)
	}
}

func TestClientConfigSASLDefaultMechanism(t *testing.T) {
	raw := baseConfig()
	raw["security_protocol"] = "SASL_PLAINTEXT"
	raw["sasl_plain_username"] = "user"
	raw["sasl_plain_password"] = "password"
	cfg, err := resolveClientConfig(raw)
	if err != nil {
		t.Fatalf("resolveClientConfig: %v", err)
	}
	if cfg.saslMechanism != "PLAIN" {
		t.Errorf("saslMechanism = %q, want PLAIN", cfg.saslMechanism)
	}
}

func TestClientConfigSASLMissingUsername(t *testing.T) {
	raw := baseConfig()
	raw["security_protocol"] = "SASL_PLAINTEXT"
	raw["sasl_plain_password"] = "password"
	_, err := resolveClientConfig(raw)
	if err == nil {
		t.Fatal("missing SASL username: want error")
	}
	if got := err.Error(); got != "SASL username and password must be provided for SASL authentication" {
		t.Errorf("error = %q", got)
	}
}

func TestClientConfigSASLMissingPassword(t *testing.T) {
	raw := baseConfig()
	raw["security_protocol"] = "SASL_PLAINTEXT"
	raw["sasl_plain_username"] = "user"
	_, err := resolveClientConfig(raw)
	if err == nil {
		t.Fatal("missing SASL password: want error")
	}
	if got := err.Error(); got != "SASL username and password must be provided for SASL authentication" {
		t.Errorf("error = %q", got)
	}
}

func TestValidSecurityProtocols(t *testing.T) {
	for _, protocol := range []string{"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"} {
		t.Run(protocol, func(t *testing.T) {
			raw := baseConfig()
			raw["security_protocol"] = protocol
			if protocol == "SASL_PLAINTEXT" || protocol == "SASL_SSL" {
				raw["sasl_plain_username"] = "user"
				raw["sasl_plain_password"] = "password"
			}
			if protocol == "SSL" || protocol == "SASL_SSL" {
				raw["ca"] = "/path/to/ca.pem"
			}
			if protocol == "SSL" {
				raw["certfile"] = "/path/to/cert.pem"
				raw["keyfile"] = "/path/to/key.pem"
			}
			cfg, err := resolveClientConfig(raw)
			if err != nil {
				t.Fatalf("resolveClientConfig(%s): %v", protocol, err)
			}
			if cfg.securityProtocol != protocol {
				t.Errorf("securityProtocol = %q, want %q", cfg.securityProtocol, protocol)
			}
		})
	}
}

// --- _generate_producer_config parity ---

func TestProducerConfigInheritsClientConfig(t *testing.T) {
	raw := baseConfig()
	raw["kafka_api_version"] = "2.5.0"
	pc, err := resolveProducerConfig(raw)
	if err != nil {
		t.Fatalf("resolveProducerConfig: %v", err)
	}
	if len(pc.brokers) != 1 || pc.brokers[0] != "localhost:9092" {
		t.Errorf("brokers = %v", pc.brokers)
	}
	if pc.apiVersion == nil || *pc.apiVersion != sarama.V2_5_0_0 {
		t.Errorf("apiVersion = %v, want V2_5_0_0", pc.apiVersion)
	}
	if pc.securityProtocol != "PLAINTEXT" {
		t.Errorf("securityProtocol = %q, want PLAINTEXT", pc.securityProtocol)
	}
}

func TestProducerConfigInvalidAPIVersion(t *testing.T) {
	raw := baseConfig()
	raw["kafka_api_version"] = "not-a-version"
	if _, err := resolveProducerConfig(raw); err == nil {
		t.Fatal("invalid kafka_api_version: want error")
	}
}

func TestProducerConfigSetsLingerMS(t *testing.T) {
	pc, err := resolveProducerConfig(baseConfig())
	if err != nil {
		t.Fatalf("resolveProducerConfig: %v", err)
	}
	if pc.lingerMS != 500 {
		t.Errorf("lingerMS = %d, want 500", pc.lingerMS)
	}
}

func TestProducerConfigCompressionSnappy(t *testing.T) {
	pc, err := resolveProducerConfig(baseConfig())
	if err != nil {
		t.Fatalf("resolveProducerConfig: %v", err)
	}
	if pc.compression != "snappy" {
		t.Errorf("compression = %q, want snappy", pc.compression)
	}
	sc, err := pc.saramaConfig()
	if err != nil {
		t.Fatalf("saramaConfig: %v", err)
	}
	if sc.Producer.Compression != sarama.CompressionSnappy {
		t.Errorf("sarama compression = %v, want CompressionSnappy", sc.Producer.Compression)
	}
	if got, want := sc.Producer.Flush.Frequency, 500*time.Millisecond; got != want {
		t.Errorf("linger = %v, want %v", got, want)
	}
}

func TestProducerConfigWithSocks5Proxy(t *testing.T) {
	raw := baseConfig()
	raw["socks5_proxy"] = "socks5://proxy:1080"
	pc, err := resolveProducerConfig(raw)
	if err != nil {
		t.Fatalf("resolveProducerConfig: %v", err)
	}
	if pc.socks5Proxy != "socks5://proxy:1080" {
		t.Errorf("socks5Proxy = %q", pc.socks5Proxy)
	}
	sc, err := pc.saramaConfig()
	if err != nil {
		t.Fatalf("saramaConfig: %v", err)
	}
	if !sc.Net.Proxy.Enable || sc.Net.Proxy.Dialer == nil {
		t.Errorf("sarama proxy enable=%v dialer=%v, want enabled with dialer", sc.Net.Proxy.Enable, sc.Net.Proxy.Dialer)
	}
}

func TestProducerConfigWithoutSocks5Proxy(t *testing.T) {
	pc, err := resolveProducerConfig(baseConfig())
	if err != nil {
		t.Fatalf("resolveProducerConfig: %v", err)
	}
	if pc.socks5Proxy != "" {
		t.Errorf("socks5Proxy = %q, want empty", pc.socks5Proxy)
	}
	sc, err := pc.saramaConfig()
	if err != nil {
		t.Fatalf("saramaConfig: %v", err)
	}
	if sc.Net.Proxy.Enable {
		t.Error("sarama proxy enabled without socks5_proxy")
	}
}

func TestProducerConfigInvalidSocks5Proxy(t *testing.T) {
	raw := baseConfig()
	raw["socks5_proxy"] = "ftp://proxy:1080"
	pc, err := resolveProducerConfig(raw)
	if err != nil {
		t.Fatalf("resolveProducerConfig: %v", err)
	}
	if _, err := pc.saramaConfig(); err == nil {
		t.Fatal("invalid socks5 scheme: saramaConfig want error")
	}
}

func TestSocks5DialerFromURL(t *testing.T) {
	u, err := url.Parse("socks5://proxy:1080")
	if err != nil {
		t.Fatalf("url.Parse: %v", err)
	}
	if _, err := socks5Dialer(u.String()); err != nil {
		t.Fatalf("socks5Dialer: %v", err)
	}
}

func TestSaslMechanisms(t *testing.T) {
	cases := map[string]sarama.SASLMechanism{
		"PLAIN":         sarama.SASLTypePlaintext,
		"SCRAM-SHA-256": sarama.SASLTypeSCRAMSHA256,
		"SCRAM-SHA-512": sarama.SASLTypeSCRAMSHA512,
	}
	for name, want := range cases {
		got, err := saslMechanism(name)
		if err != nil {
			t.Fatalf("saslMechanism(%s): %v", name, err)
		}
		if got != want {
			t.Errorf("saslMechanism(%s) = %v, want %v", name, got, want)
		}
	}
	if _, err := saslMechanism("GSSAPI"); err == nil {
		t.Error("saslMechanism(GSSAPI): want error")
	}
}

// --- Retriable classification (kafka_sender.py:KAFKA_CONN_ERRORS) ---

func TestRetriableClassification(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"out of brokers", sarama.ErrOutOfBrokers, true},
		{"leader not available", sarama.ErrLeaderNotAvailable, true},
		{"unknown server error", sarama.ErrUnknown, true},
		{"network exception", sarama.ErrNetworkException, true},
		{"request timed out", sarama.ErrRequestTimedOut, true},
		{"producer error wraps retriable", &sarama.ProducerError{Err: sarama.ErrNotLeaderForPartition}, true},
		{"producer error wraps permanent", &sarama.ProducerError{Err: sarama.ErrInvalidMessage}, false},
		{"connection refused", syscall.ECONNREFUSED, true},
		{"invalid message is permanent", sarama.ErrInvalidMessage, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Retriable(tc.err); got != tc.want {
				t.Errorf("Retriable(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestRetriableTimeout(t *testing.T) {
	if !Retriable(context.DeadlineExceeded) {
		t.Error("Retriable(context.DeadlineExceeded) = false, want true")
	}
}

// --- send path ---

// mockProducer builds a sarama mocks SyncProducer valid for the resolved
// producer config (mocks validate the config).
func mockProducer(t *testing.T, pc producerConfig) *mocks.SyncProducer {
	t.Helper()
	sc, err := pc.saramaConfig()
	if err != nil {
		t.Fatalf("saramaConfig: %v", err)
	}
	return mocks.NewSyncProducer(t, sc)
}

func TestSendMessagesSuccess(t *testing.T) {
	raw := baseConfig()
	raw["kafka_msg_key"] = "journalpump"
	s := newSender(t, raw)
	pc, err := resolveProducerConfig(raw)
	if err != nil {
		t.Fatalf("resolveProducerConfig: %v", err)
	}
	mp := mockProducer(t, pc)
	bodies := [][]byte{[]byte("one"), []byte("two")}
	for range bodies {
		mp.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(func(msg *sarama.ProducerMessage) error {
			if msg.Topic != "test-topic" {
				return errors.New("wrong topic " + msg.Topic)
			}
			val, _ := msg.Value.Encode()
			if len(val) == 0 {
				return errors.New("empty value")
			}
			key, _ := msg.Key.Encode()
			if string(key) != "journalpump" {
				return errors.New("wrong key " + string(key))
			}
			return nil
		})
	}
	s.newProducer = func(producerConfig) (producer, error) { return mp, nil }

	if err := s.SendMessages(context.Background(), bodies, "cursor"); err != nil {
		t.Fatalf("SendMessages: %v", err)
	}
	if err := mp.Close(); err != nil {
		t.Fatalf("mock Close: %v", err)
	}
}

func TestSendMessagesWithoutKey(t *testing.T) {
	s := newSender(t, baseConfig())
	pc, err := resolveProducerConfig(baseConfig())
	if err != nil {
		t.Fatalf("resolveProducerConfig: %v", err)
	}
	mp := mockProducer(t, pc)
	mp.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(func(msg *sarama.ProducerMessage) error {
		if msg.Key != nil {
			return errors.New("key set without kafka_msg_key")
		}
		return nil
	})
	s.newProducer = func(producerConfig) (producer, error) { return mp, nil }

	if err := s.SendMessages(context.Background(), [][]byte{[]byte("one")}, "cursor"); err != nil {
		t.Fatalf("SendMessages: %v", err)
	}
	if err := mp.Close(); err != nil {
		t.Fatalf("mock Close: %v", err)
	}
}

func TestSendMessagesFailureDropsProducer(t *testing.T) {
	s := newSender(t, baseConfig())
	pc, err := resolveProducerConfig(baseConfig())
	if err != nil {
		t.Fatalf("resolveProducerConfig: %v", err)
	}
	mp := mockProducer(t, pc)
	mp.ExpectSendMessageAndFail(sarama.ErrOutOfBrokers)
	s.newProducer = func(producerConfig) (producer, error) { return mp, nil }

	err = s.SendMessages(context.Background(), [][]byte{[]byte("one")}, "cursor")
	if err == nil {
		t.Fatal("SendMessages: want error")
	}
	if s.prod != nil {
		t.Error("producer not dropped after send failure")
	}
	if err := mp.Close(); err != nil {
		t.Fatalf("mock Close: %v", err)
	}
}

func TestSendMessagesConnectFailure(t *testing.T) {
	s := newSender(t, baseConfig())
	connectErr := errors.New("no brokers")
	s.newProducer = func(producerConfig) (producer, error) { return nil, connectErr }
	if err := s.SendMessages(context.Background(), [][]byte{[]byte("one")}, "cursor"); !errors.Is(err, connectErr) {
		t.Fatalf("SendMessages error = %v, want %v", err, connectErr)
	}
}

// --- topic auto-create ---

type fakeAdmin struct {
	created []string
	detail  *sarama.TopicDetail
	err     error
	closed  bool
}

func (f *fakeAdmin) CreateTopic(topic string, detail *sarama.TopicDetail, _ bool) error {
	f.created = append(f.created, topic)
	f.detail = detail
	return f.err
}

func (f *fakeAdmin) Close() error {
	f.closed = true
	return nil
}

type fakeProducer struct{ closed bool }

func (f *fakeProducer) SendMessages([]*sarama.ProducerMessage) error { return nil }
func (f *fakeProducer) Close() error                                 { f.closed = true; return nil }

func TestMaybeCreateTopicWithBothValues(t *testing.T) {
	raw := baseConfig()
	raw["kafka_topic_config"] = map[string]any{"num_partitions": 2, "replication_factor": 1}
	s := newSender(t, raw)
	pc, _ := resolveProducerConfig(raw)
	fa := &fakeAdmin{}
	s.newAdmin = func(producerConfig) (admin, error) { return fa, nil }
	if err := s.maybeCreateTopic(pc); err != nil {
		t.Fatalf("maybeCreateTopic: %v", err)
	}
	if len(fa.created) != 1 || fa.created[0] != "test-topic" {
		t.Errorf("created = %v, want [test-topic]", fa.created)
	}
	if fa.detail == nil || fa.detail.NumPartitions != 2 || fa.detail.ReplicationFactor != 1 {
		t.Errorf("detail = %+v, want 2/1", fa.detail)
	}
	if !fa.closed {
		t.Error("admin not closed")
	}
}

func TestMaybeCreateTopicWithJSONNumberConfig(t *testing.T) {
	// The config loader decodes with UseNumber, so kafka_topic_config
	// values arrive as json.Number rather than float64.
	raw := baseConfig()
	raw["kafka_topic_config"] = map[string]any{"num_partitions": json.Number("2"), "replication_factor": json.Number("1")}
	s := newSender(t, raw)
	pc, _ := resolveProducerConfig(raw)
	fa := &fakeAdmin{}
	s.newAdmin = func(producerConfig) (admin, error) { return fa, nil }
	if err := s.maybeCreateTopic(pc); err != nil {
		t.Fatalf("maybeCreateTopic: %v", err)
	}
	if fa.detail == nil || fa.detail.NumPartitions != 2 || fa.detail.ReplicationFactor != 1 {
		t.Errorf("detail = %+v, want 2/1", fa.detail)
	}
}

func TestMaybeCreateTopicAlreadyExistsTolerated(t *testing.T) {
	raw := baseConfig()
	raw["kafka_topic_config"] = map[string]any{"num_partitions": 2, "replication_factor": 1}
	s := newSender(t, raw)
	pc, _ := resolveProducerConfig(raw)
	fa := &fakeAdmin{err: sarama.ErrTopicAlreadyExists}
	s.newAdmin = func(producerConfig) (admin, error) { return fa, nil }
	if err := s.maybeCreateTopic(pc); err != nil {
		t.Fatalf("TopicAlreadyExists should be tolerated, got %v", err)
	}
}

func TestMaybeCreateTopicPartialConfigSkips(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  map[string]any
	}{
		{"num_partitions only", map[string]any{"num_partitions": 2}},
		{"replication_factor only", map[string]any{"replication_factor": 1}},
		{"empty", map[string]any{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := baseConfig()
			raw["kafka_topic_config"] = tc.cfg
			s := newSender(t, raw)
			pc, _ := resolveProducerConfig(raw)
			fa := &fakeAdmin{}
			s.newAdmin = func(producerConfig) (admin, error) { return fa, nil }
			if err := s.maybeCreateTopic(pc); err != nil {
				t.Fatalf("maybeCreateTopic: %v", err)
			}
			if len(fa.created) != 0 {
				t.Errorf("created = %v, want none", fa.created)
			}
		})
	}
}

// --- connection reporting ---

func TestEnsureProducerReportsConnected(t *testing.T) {
	s := newSender(t, baseConfig())
	fp := &fakeProducer{}
	s.newProducer = func(producerConfig) (producer, error) { return fp, nil }
	connected := 0
	s.SetConnectionObserver(func() { connected++ }, func(error) {})
	if _, err := s.ensureProducer(context.Background()); err != nil {
		t.Fatalf("ensureProducer: %v", err)
	}
	if connected != 1 {
		t.Errorf("connected observer calls = %d, want 1", connected)
	}
}

func TestSourceNotDurableReporter(t *testing.T) {
	inst, err := New(sender.ConstructorArgs{Name: "test-sender", Config: baseConfig()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, ok := inst.Transport.(sender.DurableReporter); ok {
		t.Error("kafka sender must not implement DurableReporter (cursor advances on broker ack)")
	}
}
