// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"reflect"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kversion"

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

// TestConcurrentConstruction exercises the constructor from several
// goroutines; run with -race, it catches a global-logger write (C9): franz-go
// (like sarama) defaults to a discarding logger and New never installs one,
// so concurrent construction must not race on shared state.
func TestConcurrentConstruction(t *testing.T) {
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := New(sender.ConstructorArgs{Name: "test-sender", Config: baseConfig()}); err != nil {
				t.Errorf("New: %v", err)
			}
		}()
	}
	wg.Wait()
}

// --- KafkaSender.__init__ parity (test/unit/senders/test_kafka_sender.py) ---

func TestBasicInitialization(t *testing.T) {
	inst, err := New(sender.ConstructorArgs{Name: "test-sender", Config: baseConfig()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	s := inst.Transport.(*Sender)
	if s.keyBytes != nil {
		t.Errorf("keyBytes = %v, want nil", s.keyBytes)
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
	if s.keyBytes == nil {
		t.Fatal("keyBytes = nil, want encoded key")
	}
	if string(s.keyBytes) != "my-key" {
		t.Errorf("key = %q, want my-key", s.keyBytes)
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
	if s.keyBytes != nil {
		t.Errorf("keyBytes = %v, want nil for empty kafka_msg_key", s.keyBytes)
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

// TestClientConfigBootstrapAddressForms covers the accepted kafka_address
// forms: hostnames without a port default to 9092, IPv6 literals work
// bracketed or bare, entries may carry a scheme prefix, and the value may
// be a list (review item B6).
func TestClientConfigBootstrapAddressForms(t *testing.T) {
	cases := []struct {
		name  string
		value any
		want  []string
	}{
		{"hostname no port", "kafka.example.org", []string{"kafka.example.org:9092"}},
		{"host port", "kafka.example.org:19092", []string{"kafka.example.org:19092"}},
		{"bracketed ipv6", "[::1]:9092", []string{"[::1]:9092"}},
		{"bracketed ipv6 no port", "[2001:db8::1]", []string{"[2001:db8::1]:9092"}},
		{"bare ipv6", "::1", []string{"[::1]:9092"}},
		{"scheme prefix", "PLAINTEXT://broker:9092", []string{"broker:9092"}},
		{"mixed csv", "a:9092,b.example.org,[::1]:9092", []string{"a:9092", "b.example.org:9092", "[::1]:9092"}},
		{"list string values", []any{"a", "b:9092", "[::1]"}, []string{"a:9092", "b:9092", "[::1]:9092"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw := baseConfig()
			raw["kafka_address"] = tc.value
			cfg, err := resolveClientConfig(raw)
			if err != nil {
				t.Fatalf("resolveClientConfig: %v", err)
			}
			if len(cfg.brokers) != len(tc.want) {
				t.Fatalf("brokers = %v, want %v", cfg.brokers, tc.want)
			}
			for i := range tc.want {
				if cfg.brokers[i] != tc.want[i] {
					t.Errorf("brokers[%d] = %q, want %q", i, cfg.brokers[i], tc.want[i])
				}
			}
		})
	}
}

func TestClientConfigRejectsBadBootstrapAddress(t *testing.T) {
	for _, value := range []any{123, []any{"ok", 5}, "[::1"} {
		raw := baseConfig()
		raw["kafka_address"] = value
		if _, err := resolveClientConfig(raw); err == nil {
			t.Errorf("kafka_address=%v: want error, got nil", value)
		}
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

// TestClientConfigSSLTruthiness: Python derives SSL from the truthiness
// of the ssl setting, so values like 1, "true" and "yes" must enable SSL
// rather than silently falling back to PLAINTEXT (review item B4).
func TestClientConfigSSLTruthiness(t *testing.T) {
	for _, ssl := range []any{true, 1, int64(1), "true", "yes", "1", "0", json.Number("1")} {
		raw := baseConfig()
		raw["ssl"] = ssl
		cfg, err := resolveClientConfig(raw)
		if err != nil {
			t.Fatalf("ssl=%v: resolveClientConfig: %v", ssl, err)
		}
		if cfg.securityProtocol != "SSL" {
			t.Errorf("ssl=%v: securityProtocol = %q, want SSL", ssl, cfg.securityProtocol)
		}
	}
	for _, ssl := range []any{false, 0, "", nil} {
		raw := baseConfig()
		raw["ssl"] = ssl
		cfg, err := resolveClientConfig(raw)
		if err != nil {
			t.Fatalf("ssl=%v: resolveClientConfig: %v", ssl, err)
		}
		if cfg.securityProtocol != "PLAINTEXT" {
			t.Errorf("ssl=%v: securityProtocol = %q, want PLAINTEXT", ssl, cfg.securityProtocol)
		}
	}
}

// TestClientConfigRejectsMalformedSSLValues: a non-string ca/certfile/
// keyfile or SASL credential must be an error, never a silent downgrade
// (review item B4).
func TestClientConfigRejectsMalformedSSLValues(t *testing.T) {
	for _, tc := range []struct {
		key string
		val any
	}{
		{"ca", 123},
		{"ca", map[string]any{"path": "x"}},
		{"certfile", 5},
		{"keyfile", []any{"x"}},
	} {
		raw := baseConfig()
		raw["security_protocol"] = "SSL"
		raw[tc.key] = tc.val
		if _, err := resolveClientConfig(raw); err == nil {
			t.Errorf("%s=%v: want error, got nil", tc.key, tc.val)
		}
	}
	// A malformed CA under SASL_SSL is also rejected.
	raw := baseConfig()
	raw["security_protocol"] = "SASL_SSL"
	raw["sasl_plain_username"] = "user"
	raw["sasl_plain_password"] = "password"
	raw["ca"] = 123
	if _, err := resolveClientConfig(raw); err == nil {
		t.Error("ca=123 under SASL_SSL: want error, got nil")
	}
}

func TestClientConfigRejectsMalformedSASLCredentials(t *testing.T) {
	raw := baseConfig()
	raw["security_protocol"] = "SASL_PLAINTEXT"
	raw["sasl_plain_username"] = 42
	raw["sasl_plain_password"] = "password"
	if _, err := resolveClientConfig(raw); err == nil {
		t.Error("numeric sasl_plain_username: want error, got nil")
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
	if pc.apiVersion != "2.5.0" {
		t.Errorf("apiVersion = %q, want 2.5.0", pc.apiVersion)
	}
	if pc.securityProtocol != "PLAINTEXT" {
		t.Errorf("securityProtocol = %q, want PLAINTEXT", pc.securityProtocol)
	}
}

// TestNormalizeKafkaVersion covers the kafka-python accepted syntax
// (review item D10): "auto" (and empty) mean broker negotiation, "X.Y" and
// "X.Y.Z" are zero-padded, and a trailing zero component is tolerated.
func TestNormalizeKafkaVersion(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"2.5.0", "2.5.0"},
		{"2.5", "2.5.0"},
		{"0.10.2", "0.10.2"},
		{"0.10.2.0", "0.10.2"},
		{"0.10", "0.10.0"},
		{"0.11.0", "0.11.0"},
		{"1.0.0", "1.0.0"},
		{"4.0.0", "4.0.0"},
		{"auto", ""},
		{"AUTO", ""},
		{" Auto ", ""},
		{"", ""},
	}
	for _, tc := range cases {
		got, err := normalizeKafkaVersion(tc.in)
		if err != nil {
			t.Errorf("normalizeKafkaVersion(%q): %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("normalizeKafkaVersion(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
	for _, bad := range []string{"not-a-version", "2.x", "a.b.c", "9.9.9", "2.5.0.1"} {
		if _, err := normalizeKafkaVersion(bad); err == nil {
			t.Errorf("normalizeKafkaVersion(%q): want error", bad)
		}
	}
}

// TestMaxVersionsFor: a normalized version maps onto the franz-go kversion
// pin; an unknown one is an error. This replaces sarama's Config.Version
// projection (review item D10).
func TestMaxVersionsFor(t *testing.T) {
	got, err := maxVersionsFor("2.5.0")
	if err != nil {
		t.Fatalf("maxVersionsFor(2.5.0): %v", err)
	}
	if !reflect.DeepEqual(got, kversion.V2_5_0()) {
		t.Error("maxVersionsFor(2.5.0) did not yield the V2_5_0 pin")
	}
	if _, err := maxVersionsFor("9.9.9"); err == nil {
		t.Error("maxVersionsFor(9.9.9): want error")
	}
}

// TestProducerOptsConstructClient builds a real franz-go client from the
// resolved options. franz-go validates the acks/idempotency pairing at
// construction, so this pins that acks=1 is accompanied by
// DisableIdempotentWrite (review item D12 / the assessment's mandatory
// pairing).
func TestProducerOptsConstructClient(t *testing.T) {
	pc, err := resolveProducerConfig(baseConfig())
	if err != nil {
		t.Fatalf("resolveProducerConfig: %v", err)
	}
	base, err := pc.clientOpts()
	if err != nil {
		t.Fatalf("clientOpts: %v", err)
	}
	cl, err := kgo.NewClient(append(base, pc.producerOpts()...)...)
	if err != nil {
		t.Fatalf("kgo.NewClient rejected the producer options (acks=1 needs DisableIdempotentWrite): %v", err)
	}
	cl.Close()
}

// TestProducerSemanticsConstants pins the retry/timeout/linger values that
// map onto franz-go's RecordRetries, RecordDeliveryTimeout,
// ProduceRequestTimeout and ProducerLinger (review round-3 items 4/5).
func TestProducerSemanticsConstants(t *testing.T) {
	if lingerMS != 500 {
		t.Errorf("lingerMS = %d, want 500", lingerMS)
	}
	if requestTimeout != 30*time.Second {
		t.Errorf("requestTimeout = %v, want 30s", requestTimeout)
	}
	if deliveryTimeout != 120*time.Second {
		t.Errorf("deliveryTimeout = %v, want 120s", deliveryTimeout)
	}
	if deliveryTimeout == requestTimeout {
		t.Fatal("deliveryTimeout and requestTimeout must be distinct so the two are independently observable")
	}
	if retryBackoff != 100*time.Millisecond {
		t.Errorf("retryBackoff = %v, want 100ms", retryBackoff)
	}
	if producerRetryMax != 20 {
		t.Errorf("producerRetryMax = %d, want 20", producerRetryMax)
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
	if !reflect.DeepEqual(compressionCodec(), kgo.SnappyCompression()) {
		t.Error("compressionCodec() is not franz-go's SnappyCompression")
	}
	if reflect.DeepEqual(compressionCodec(), kgo.GzipCompression()) {
		t.Error("compressionCodec() must not be gzip")
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
	if _, err := pc.clientOpts(); err != nil {
		t.Fatalf("clientOpts with a socks5 proxy: %v", err)
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
	if _, err := pc.clientOpts(); err != nil {
		t.Fatalf("clientOpts: %v", err)
	}
}

func TestProducerConfigInvalidSocks5Proxy(t *testing.T) {
	raw := baseConfig()
	raw["socks5_proxy"] = "ftp://proxy:1080"
	pc, err := resolveProducerConfig(raw)
	if err != nil {
		t.Fatalf("resolveProducerConfig: %v", err)
	}
	if _, err := pc.clientOpts(); err == nil {
		t.Fatal("invalid socks5 scheme: clientOpts want error")
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
	for _, name := range []string{"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"} {
		if _, err := saslOption(producerConfig{saslMechanism: name, saslUsername: "user", saslPassword: "password"}); err != nil {
			t.Errorf("saslOption(%s): %v", name, err)
		}
	}
	if _, err := saslOption(producerConfig{saslMechanism: "GSSAPI"}); err == nil {
		t.Error("saslOption(GSSAPI): want error")
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
		{"leader not available", kerr.ErrorForCode(5), true},
		{"unknown server error", &kerr.Error{Code: -1}, true},
		{"network exception", kerr.ErrorForCode(13), true},
		{"request timed out", kerr.ErrorForCode(7), true},
		{"broker not available is not retriable", kerr.ErrorForCode(8), false},
		{"corrupt message permanent", &kerr.Error{Code: 2}, false},
		{"batch all retriable", sendErr(kerr.ErrorForCode(6), kerr.ErrorForCode(7)), true},
		{"batch mixed is permanent", sendErr(kerr.ErrorForCode(6), &kerr.Error{Code: 2}), false},
		{"empty batch", sendErr(), false},
		{"batch wrapped", fmt.Errorf("send failed: %w", sendErr(kerr.ErrorForCode(5))), true},
		{"connection refused", syscall.ECONNREFUSED, true},
		{"deadline exceeded", context.DeadlineExceeded, true},
		{"cancelled", context.Canceled, true},
		{"client closed", kgo.ErrClientClosed, true},
		{"record timeout", kgo.ErrRecordTimeout, true},
		{"record retries", kgo.ErrRecordRetries, true},
		{"eof", io.EOF, true},
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

// TestRetriableKErrorMapping pins the protocol-error table against
// kafka-python 2.3.2's retriable=True classes (review item A3). Certain
// codes that look transient (BrokerNotAvailable, RebalanceInProgress) are
// deliberately NOT retriable because kafka-python does not mark them so,
// even though franz-go's kerr.Retriable says BrokerNotAvailable is.
func TestRetriableKErrorMapping(t *testing.T) {
	retriable := []int16{
		-1, 3, 5, 6, 7, 9, 13, 14, 15, 16, 19, 20, 41, 51, 56, 70, 71, 72,
		74, 75, 78, 80, 83, 84, 88, 89, 100, 103, 106, 122, 123,
	}
	for _, code := range retriable {
		if !Retriable(&kerr.Error{Code: code}) {
			t.Errorf("Retriable(code %d) = false, want true", code)
		}
	}
	permanent := []int16{
		8,  // BrokerNotAvailable (kafka-python: not retriable)
		27, // RebalanceInProgress (kafka-python: not retriable)
		2,  // CorruptMessage
		36, // TopicAlreadyExists
		29, // TopicAuthorizationFailed
		10, // MessageSizeTooLarge
	}
	for _, code := range permanent {
		if Retriable(&kerr.Error{Code: code}) {
			t.Errorf("Retriable(code %d) = true, want false", code)
		}
	}
}

// TestRetriableNetworkErrors covers the socket-level errors mapped onto
// kafka-python's retriable client exceptions.
func TestRetriableNetworkErrors(t *testing.T) {
	for _, err := range []error{
		syscall.ECONNREFUSED,
		syscall.ECONNRESET,
		syscall.EPIPE,
		io.ErrUnexpectedEOF,
		&netTimeoutError{},
	} {
		if !Retriable(err) {
			t.Errorf("Retriable(%v) = false, want true", err)
		}
	}
}

// netTimeoutError is a net.Error whose Timeout() is true, modelling a socket
// read/write deadline.
type netTimeoutError struct{}

func (*netTimeoutError) Error() string   { return "i/o timeout" }
func (*netTimeoutError) Timeout() bool   { return true }
func (*netTimeoutError) Temporary() bool { return true }

// --- send path ---

type fakeProducer struct{ closed bool }

func (f *fakeProducer) Produce(context.Context, [][]byte) error { return nil }
func (f *fakeProducer) Close() error                            { f.closed = true; return nil }

func TestSendMessagesSuccess(t *testing.T) {
	raw := baseConfig()
	raw["kafka_msg_key"] = "journalpump"
	s := newSender(t, raw)
	rp := &recordingProducer{}
	s.newProducer = func(context.Context, producerConfig) (producer, error) { return rp, nil }

	bodies := [][]byte{[]byte("one"), []byte("two")}
	if err := s.SendMessages(context.Background(), bodies, "cursor"); err != nil {
		t.Fatalf("SendMessages: %v", err)
	}
	values := rp.recorded()
	if len(values) != 2 || string(values[0]) != "one" || string(values[1]) != "two" {
		t.Fatalf("delivered = %q, want [one two]", values)
	}
	// The topic and the fixed key are stamped by liveProducer from the Sender.
	if s.topic != "test-topic" {
		t.Errorf("topic = %q, want test-topic", s.topic)
	}
	if string(s.keyBytes) != "journalpump" {
		t.Errorf("keyBytes = %q, want journalpump", s.keyBytes)
	}
}

func TestSendMessagesWithoutKey(t *testing.T) {
	s := newSender(t, baseConfig())
	rp := &recordingProducer{}
	s.newProducer = func(context.Context, producerConfig) (producer, error) { return rp, nil }
	if err := s.SendMessages(context.Background(), [][]byte{[]byte("one")}, "cursor"); err != nil {
		t.Fatalf("SendMessages: %v", err)
	}
	if s.keyBytes != nil {
		t.Errorf("keyBytes = %v, want nil without kafka_msg_key", s.keyBytes)
	}
}

func TestSendMessagesFailureDropsProducer(t *testing.T) {
	s := newSender(t, baseConfig())
	rp := &recordingProducer{errs: []error{errors.New("boom")}}
	s.newProducer = func(context.Context, producerConfig) (producer, error) { return rp, nil }

	err := s.SendMessages(context.Background(), [][]byte{[]byte("one")}, "cursor")
	if err == nil {
		t.Fatal("SendMessages: want error")
	}
	if s.prod != nil {
		t.Error("producer not dropped after send failure")
	}
}

func TestSendMessagesConnectFailure(t *testing.T) {
	s := newSender(t, baseConfig())
	connectErr := errors.New("no brokers")
	s.newProducer = func(context.Context, producerConfig) (producer, error) { return nil, connectErr }
	if err := s.SendMessages(context.Background(), [][]byte{[]byte("one")}, "cursor"); !errors.Is(err, connectErr) {
		t.Fatalf("SendMessages error = %v, want %v", err, connectErr)
	}
}

// --- topic auto-create ---

type fakeAdmin struct {
	created    []string
	partitions int32
	rf         int16
	err        error
	closed     bool
}

func (f *fakeAdmin) CreateTopic(_ context.Context, partitions int32, rf int16, topic string) error {
	f.created = append(f.created, topic)
	f.partitions = partitions
	f.rf = rf
	return f.err
}

func (f *fakeAdmin) Close() error {
	f.closed = true
	return nil
}

func TestMaybeCreateTopicWithBothValues(t *testing.T) {
	raw := baseConfig()
	raw["kafka_topic_config"] = map[string]any{"num_partitions": 2, "replication_factor": 1}
	s := newSender(t, raw)
	pc, _ := resolveProducerConfig(raw)
	fa := &fakeAdmin{}
	s.newAdmin = func(context.Context, producerConfig) (admin, error) { return fa, nil }
	if err := s.maybeCreateTopic(context.Background(), pc); err != nil {
		t.Fatalf("maybeCreateTopic: %v", err)
	}
	if len(fa.created) != 1 || fa.created[0] != "test-topic" {
		t.Errorf("created = %v, want [test-topic]", fa.created)
	}
	if fa.partitions != 2 || fa.rf != 1 {
		t.Errorf("partitions/rf = %d/%d, want 2/1", fa.partitions, fa.rf)
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
	s.newAdmin = func(context.Context, producerConfig) (admin, error) { return fa, nil }
	if err := s.maybeCreateTopic(context.Background(), pc); err != nil {
		t.Fatalf("maybeCreateTopic: %v", err)
	}
	if fa.partitions != 2 || fa.rf != 1 {
		t.Errorf("partitions/rf = %d/%d, want 2/1", fa.partitions, fa.rf)
	}
}

func TestMaybeCreateTopicAlreadyExistsTolerated(t *testing.T) {
	raw := baseConfig()
	raw["kafka_topic_config"] = map[string]any{"num_partitions": 2, "replication_factor": 1}
	s := newSender(t, raw)
	pc, _ := resolveProducerConfig(raw)
	fa := &fakeAdmin{err: kerr.TopicAlreadyExists}
	s.newAdmin = func(context.Context, producerConfig) (admin, error) { return fa, nil }
	if err := s.maybeCreateTopic(context.Background(), pc); err != nil {
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
			s.newAdmin = func(context.Context, producerConfig) (admin, error) { return fa, nil }
			if err := s.maybeCreateTopic(context.Background(), pc); err != nil {
				t.Fatalf("maybeCreateTopic: %v", err)
			}
			if len(fa.created) != 0 {
				t.Errorf("created = %v, want none", fa.created)
			}
		})
	}
}

// TestTopicConfigRejectsInvalidValues: kafka-python encodes
// num_partitions as Int32 and replication_factor as Int16, so a float, a
// numeric string, a fractional JSON number or an out-of-range value fails
// the request encoding. Silently skipping the create (or truncating) would
// hide a broken config; the values must be rejected instead (review item
// E13).
func TestTopicConfigRejectsInvalidValues(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  map[string]any
	}{
		{"float num_partitions", map[string]any{"num_partitions": 2.0, "replication_factor": 1}},
		{"fractional num_partitions", map[string]any{"num_partitions": 2.5, "replication_factor": 1}},
		{"numeric string num_partitions", map[string]any{"num_partitions": "2", "replication_factor": 1}},
		{"trailing-garbage string", map[string]any{"num_partitions": "2abc", "replication_factor": 1}},
		{"fractional json number", map[string]any{"num_partitions": json.Number("2.5"), "replication_factor": json.Number("1")}},
		{"float replication_factor", map[string]any{"num_partitions": 2, "replication_factor": 1.5}},
		{"string replication_factor", map[string]any{"num_partitions": 2, "replication_factor": "1"}},
		{"num_partitions overflow", map[string]any{"num_partitions": int64(1) << 40, "replication_factor": 1}},
		{"replication_factor overflow", map[string]any{"num_partitions": 2, "replication_factor": int64(70000)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := baseConfig()
			raw["kafka_topic_config"] = tc.cfg
			if _, err := resolveProducerConfig(raw); err == nil {
				t.Fatal("resolveProducerConfig: want error for invalid kafka_topic_config, got nil")
			}
			s := newSender(t, raw)
			pc, _ := resolveProducerConfig(baseConfig())
			fa := &fakeAdmin{}
			s.newAdmin = func(context.Context, producerConfig) (admin, error) { return fa, nil }
			if err := s.maybeCreateTopic(context.Background(), pc); err == nil {
				t.Fatal("maybeCreateTopic: want error, got nil")
			}
			if len(fa.created) != 0 {
				t.Errorf("created = %v, want none", fa.created)
			}
		})
	}
}

// TestTopicConfigRejectsNonMapValue: a present kafka_topic_config that is
// not a mapping is a config error, not a silent skip.
func TestTopicConfigRejectsNonMapValue(t *testing.T) {
	raw := baseConfig()
	raw["kafka_topic_config"] = "2"
	if _, err := resolveProducerConfig(raw); err == nil {
		t.Fatal("want error for a non-map kafka_topic_config, got nil")
	}
}

// TestTopicConfigAcceptsIntegers pins the accepted forms: Go ints, integer
// json.Number, and Python's bool-as-int.
func TestTopicConfigAcceptsIntegers(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  map[string]any
		want struct {
			np int32
			rf int16
		}
	}{
		{"ints", map[string]any{"num_partitions": 3, "replication_factor": 2}, struct {
			np int32
			rf int16
		}{3, 2}},
		{"json numbers", map[string]any{"num_partitions": json.Number("5"), "replication_factor": json.Number("1")}, struct {
			np int32
			rf int16
		}{5, 1}},
		{"bools", map[string]any{"num_partitions": true, "replication_factor": true}, struct {
			np int32
			rf int16
		}{1, 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			np, rf, ok, err := topicConfig(map[string]any{"kafka_topic_config": tc.cfg})
			if err != nil {
				t.Fatalf("topicConfig: %v", err)
			}
			if !ok || np != tc.want.np || rf != tc.want.rf {
				t.Fatalf("topicConfig = (%d, %d, %v), want (%d, %d, true)", np, rf, ok, tc.want.np, tc.want.rf)
			}
		})
	}
}

// --- connection reporting ---

func TestEnsureProducerReportsConnected(t *testing.T) {
	s := newSender(t, baseConfig())
	fp := &fakeProducer{}
	s.newProducer = func(context.Context, producerConfig) (producer, error) { return fp, nil }
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

// --- cancellation (review item C7) ---

// signalledProducer signals Close through a channel so tests can wait for
// an abandoned connection to be cleaned up.
type signalledProducer struct {
	closed chan struct{}
	once   sync.Once
}

func (p *signalledProducer) Produce(context.Context, [][]byte) error { return nil }
func (p *signalledProducer) Close() error {
	p.once.Do(func() { close(p.closed) })
	return nil
}

// TestEnsureProducerRespectsCancellation: the factory seam must honor the
// caller's context — a connect that has not completed when ctx ends must
// surface context.Canceled and leave no producer installed, so the next
// attempt re-initializes. (The production connectProducer does this via
// franz-go's ctx-aware Ping, which aborts the metadata handshake.)
func TestEnsureProducerRespectsCancellation(t *testing.T) {
	s := newSender(t, baseConfig())
	release := make(chan struct{})
	entered := make(chan struct{})
	s.newProducer = func(ctx context.Context, _ producerConfig) (producer, error) {
		close(entered)
		<-release
		return nil, ctx.Err()
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := s.ensureProducer(ctx)
		done <- err
	}()
	<-entered
	cancel()
	close(release)
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ensureProducer error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ensureProducer did not return after cancellation")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.prod != nil {
		t.Error("cancelled connect left a producer installed; reload would reuse a dead client")
	}
}

// TestMaybeCreateTopicRespectsCancellation: the admin factory and the create
// call are cancellation-aware, and a cancelled connect surfaces
// context.Canceled.
func TestMaybeCreateTopicRespectsCancellation(t *testing.T) {
	raw := baseConfig()
	raw["kafka_topic_config"] = map[string]any{"num_partitions": 3, "replication_factor": 1}
	s := newSender(t, raw)
	pc, err := resolveProducerConfig(raw)
	if err != nil {
		t.Fatalf("resolveProducerConfig: %v", err)
	}
	release := make(chan struct{})
	entered := make(chan struct{})
	s.newAdmin = func(ctx context.Context, _ producerConfig) (admin, error) {
		close(entered)
		<-release
		return nil, ctx.Err()
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.maybeCreateTopic(ctx, pc) }()
	<-entered
	cancel()
	close(release)
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("maybeCreateTopic error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("maybeCreateTopic did not return after cancellation")
	}
}

// TestSendMessagesCancelledContext: a cancelled context short-circuits the
// send before any dial.
func TestSendMessagesCancelledContext(t *testing.T) {
	s := newSender(t, baseConfig())
	called := false
	s.newProducer = func(context.Context, producerConfig) (producer, error) {
		called = true
		return &fakeProducer{}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.SendMessages(ctx, [][]byte{[]byte("x")}, "c"); !errors.Is(err, context.Canceled) {
		t.Fatalf("SendMessages error = %v, want context.Canceled", err)
	}
	if called {
		t.Error("SendMessages connected despite a cancelled context")
	}
}

func TestSignalledProducerClose(t *testing.T) {
	// Guard the fake: Close fires its channel exactly once.
	sp := &signalledProducer{closed: make(chan struct{})}
	_ = sp.Close()
	_ = sp.Close()
	select {
	case <-sp.closed:
	default:
		t.Fatal("signalledProducer.Close did not signal")
	}
}
