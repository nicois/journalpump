// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package config

import (
	"reflect"
	"strings"
	"testing"
)

// goldenConfig contains every key accepted by the Python implementation for
// the top level, one reader, and all nine senders (spec §5, complete
// per-sender key sets; the Python code is the source of truth).
const goldenConfig = `{
    "log_level": "DEBUG",
    "statsd": {
        "host": "127.0.0.1",
        "port": 8125,
        "tags": {"env": "test", "site": 5},
        "prefix": "pump."
    },
    "tags": {"cluster": "c-1", "zone": "eu-1"},
    "geoip_database": "/var/lib/journalpump/GeoLite2-City.mmdb",
    "msg_buffer_max_length": 1234,
    "msg_buffer_max_bytes": 8388608,
    "json_state_file_path": "/var/lib/journalpump/state.json",
    "json_running_config_path": "/var/lib/journalpump/running.json",
    "write_running_config": true,
    "match_key": "SYSTEMD_UNIT",
    "match_value": "foo.service",
    "unknown_top_level": {"kept": [1, 2.5, null]},
    "field_filters": {
        "keep_important": {
            "type": "whitelist",
            "fields": ["MESSAGE", "_PID", "PRIORITY"]
        },
        "drop_noise": {
            "type": "blacklist",
            "fields": ["_TRANSPORT"]
        },
        "default_type": {
            "fields": ["MESSAGE"]
        },
        "null_type": {
            "type": null,
            "fields": ["MESSAGE"]
        }
    },
    "unit_log_levels": {
        "restrict_debug": [
            {"service_glob": "systemd-*", "log_level": "WARNING"},
            {"service_glob": "sshd.service", "log_level": "EMERGENCY"}
        ]
    },
    "readers": {
        "system": {
            "journal_flags": ["SYSTEM", "LOCAL"],
            "journal_path": "/var/log/journal",
            "journal_files": ["/var/log/journal/a.journal"],
            "journal_namespace": "namespace-1",
            "units_to_match": ["sshd.service"],
            "journald_filters": [{"_TRANSPORT": "stdout"}, {"PRIORITY": "3"}],
            "initial_position": "tail",
            "searches": [
                {"name": "stopped", "fields": {"MESSAGE": "Stopped (?P<unit>.+)"}},
                {"name": "ips", "tags": {"geo": "ip_to_geohash(ip_address,5)"}}
            ],
            "secret_filters": [
                {"pattern": "AVNS_[A-Za-z0-9-_]{15,123}", "replacement": "[REDACTED]"}
            ],
            "secret_filter_metrics": true,
            "threshold_for_metric_emit": 42,
            "match_key": "_TRANSPORT",
            "match_value": "stdout",
            "senders": {
                "to_kafka": {
                    "output_type": "kafka",
                    "field_filter": "keep_important",
                    "unit_log_level": "restrict_debug",
                    "extra_field_values": {"cluster": "c-1"},
                    "kafka_topic": "logs",
                    "kafka_address": "kafka:9092",
                    "kafka_msg_key": "pump-key",
                    "kafka_api_version": "2.4.0",
                    "security_protocol": "SASL_SSL",
                    "ssl": true,
                    "ca": "/etc/ca.pem",
                    "certfile": "/etc/cert.pem",
                    "keyfile": "/etc/key.pem",
                    "sasl_mechanism": "SCRAM-SHA-256",
                    "sasl_plain_username": "u",
                    "sasl_plain_password": "p",
                    "socks5_proxy": "socks5h://proxy:1080",
                    "kafka_topic_config": {"num_partitions": 1, "replication_factor": 3},
                    "max_send_interval": 0.3
                },
                "to_es": {
                    "output_type": "elasticsearch",
                    "elasticsearch_url": "http://es:9200",
                    "elasticsearch_timeout": 30.0,
                    "elasticsearch_index_prefix": "logs",
                    "elasticsearch_index_days_max": 7,
                    "ca": true,
                    "max_send_interval": 10.0
                },
                "to_os": {
                    "output_type": "opensearch",
                    "opensearch_url": "http://os:9200",
                    "opensearch_timeout": 60.0,
                    "opensearch_index_prefix": "journalpump",
                    "opensearch_index_days_max": 3,
                    "max_send_interval": 10.0
                },
                "to_cw": {
                    "output_type": "aws_cloudwatch",
                    "aws_cloudwatch_log_group": "group",
                    "aws_cloudwatch_log_stream": "stream",
                    "aws_region": "eu-central-1",
                    "aws_access_key_id": "AKIA...",
                    "aws_secret_access_key": "secret",
                    "max_send_interval": 0.3
                },
                "to_gcl": {
                    "output_type": "google_cloud_logging",
                    "google_cloud_logging_project_id": "proj",
                    "google_cloud_logging_log_id": "app",
                    "google_cloud_logging_resource_labels": {"env": "test"},
                    "google_service_account_credentials": "/etc/sa.json",
                    "max_send_interval": 0.3
                },
                "to_logplex": {
                    "output_type": "logplex",
                    "logplex_log_input_url": "https://logs.example.com",
                    "logplex_token": "t-abc",
                    "logplex_request_timeout": 2,
                    "max_send_interval": 5.0
                },
                "to_rsyslog": {
                    "output_type": "rsyslog",
                    "rsyslog_server": "rsyslog.example.com",
                    "rsyslog_port": 514,
                    "format": "RFC5424",
                    "logline": "%msg%",
                    "max_message_size": 4096,
                    "ssl": false,
                    "octet_counted_framing": true,
                    "escape_newlines": true,
                    "default_facility": 1,
                    "default_severity": 6,
                    "structured_data": "-",
                    "max_send_interval": 0.3
                },
                "to_ws": {
                    "output_type": "websocket",
                    "websocket_uri": "wss://ws.example.com",
                    "socks5_proxy": "socks5h://proxy:1080",
                    "ssl": true,
                    "ca": "/etc/ca.pem",
                    "keyfile": "/etc/key.pem",
                    "certfile": "/etc/cert.pem",
                    "websocket_compression": "deflate",
                    "compression": "snappy",
                    "max_batch_size": 1048576,
                    "max_send_interval": 1.0
                },
                "to_file": {
                    "output_type": "file",
                    "file_output": "/var/log/journalpump.log",
                    "max_send_interval": 0.3
                }
            }
        },
        "integer_flags": {
            "journal_flags": 8,
            "initial_position": 60,
            "senders": {}
        }
    }
}`

func mustDecode(t *testing.T, data string) *Config {
	t.Helper()
	cfg, err := Decode([]byte(data))
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	return cfg
}

// TestDecodeGoldenFullConfig locks the typed decoding of every implemented
// key per spec §5.
func TestDecodeGoldenFullConfig(t *testing.T) {
	t.Parallel()
	cfg := mustDecode(t, goldenConfig)
	assertGoldenTopLevel(t, cfg)
	assertGoldenFilters(t, cfg)
	assertGoldenReader(t, cfg)
	assertGoldenRawPreserved(t, cfg)
}

func assertGoldenTopLevel(t *testing.T, cfg *Config) {
	t.Helper()
	if cfg.LogLevel != 10 {
		t.Errorf("LogLevel = %d, want 10 (DEBUG)", cfg.LogLevel)
	}
	if cfg.Statsd.Host == nil || *cfg.Statsd.Host != "127.0.0.1" || cfg.Statsd.Port == nil || *cfg.Statsd.Port != 8125 {
		t.Errorf("Statsd = %+v", cfg.Statsd)
	}
	if cfg.Statsd.Disabled() {
		t.Error("statsd should be enabled with host and port set")
	}
	if !reflect.DeepEqual(cfg.Statsd.Tags, map[string]string{"env": "test", "site": "5"}) {
		t.Errorf("statsd tags = %v", cfg.Statsd.Tags)
	}
	if want := []string{"env", "site"}; !reflect.DeepEqual(cfg.Statsd.TagOrder, want) {
		// statsd.tags document order, captured from the raw JSON — this
		// is the order Python's dict reaches the statsd wire with.
		t.Errorf("statsd.TagOrder = %v, want %v", cfg.Statsd.TagOrder, want)
	}
	if cfg.Statsd.Prefix != "pump." {
		t.Errorf("statsd prefix = %q", cfg.Statsd.Prefix)
	}
	if !reflect.DeepEqual(cfg.Tags, map[string]string{"cluster": "c-1", "zone": "eu-1"}) {
		t.Errorf("tags = %v", cfg.Tags)
	}
	if cfg.MsgBufferMaxLength != 1234 || cfg.MsgBufferMaxBytes != 8388608 {
		t.Errorf("msg buffer = %d/%d", cfg.MsgBufferMaxLength, cfg.MsgBufferMaxBytes)
	}
	if cfg.JSONStateFilePath != "/var/lib/journalpump/state.json" {
		t.Errorf("state path = %q", cfg.JSONStateFilePath)
	}
	if !cfg.WriteRunningConfig {
		t.Error("write_running_config should be true")
	}
	if cfg.MatchKey != "SYSTEMD_UNIT" || cfg.MatchValue != "foo.service" {
		t.Errorf("match = %q/%v", cfg.MatchKey, cfg.MatchValue)
	}
}

func assertGoldenFilters(t *testing.T, cfg *Config) {
	t.Helper()
	// Field filters: type default (absent => whitelist) and both kinds.
	if got := cfg.FieldFilters["keep_important"]; !got.Whitelist ||
		!reflect.DeepEqual(got.Fields, []string{"MESSAGE", "_PID", "PRIORITY"}) {
		t.Errorf("keep_important = %+v", got)
	}
	if cfg.FieldFilters["drop_noise"].Whitelist {
		t.Error("blacklist filter decoded as whitelist")
	}
	if !cfg.FieldFilters["default_type"].Whitelist {
		t.Error("absent type must default to whitelist")
	}
	// Python: config.get("type", "whitelist") == "whitelist" evaluates
	// None == "whitelist" -> False, so a present-but-null type is
	// blacklist mode, not the default.
	if cfg.FieldFilters["null_type"].Whitelist {
		t.Error("present-but-null type must decode as blacklist")
	}

	// Unit log levels.
	if got := cfg.UnitLogLevels["restrict_debug"]; len(got) != 2 ||
		got[0].ServiceGlob != "systemd-*" || got[0].LogLevel != "WARNING" ||
		got[1].LogLevel != "EMERGENCY" {
		t.Errorf("restrict_debug = %+v", got)
	}
}

func assertGoldenReader(t *testing.T, cfg *Config) {
	t.Helper()
	// Reader.
	reader, ok := cfg.Readers["system"]
	if !ok {
		t.Fatal("reader 'system' missing")
	}
	if !reflect.DeepEqual(reader.JournalFlagNames, []string{"SYSTEM", "LOCAL"}) || reader.JournalFlagsInt != nil {
		t.Errorf("journal_flags = %v/%v", reader.JournalFlagNames, reader.JournalFlagsInt)
	}
	if reader.JournalPath != "/var/log/journal" ||
		!reflect.DeepEqual(reader.JournalFiles, []string{"/var/log/journal/a.journal"}) ||
		reader.JournalNamespace != "namespace-1" {
		t.Errorf("journal source config = %+v", reader)
	}
	if !reflect.DeepEqual(reader.UnitsToMatch, []string{"sshd.service"}) {
		t.Errorf("units_to_match = %v", reader.UnitsToMatch)
	}
	if len(reader.JournaldFilters) != 2 || reader.JournaldFilters[1]["PRIORITY"] != "3" {
		t.Errorf("journald_filters = %v", reader.JournaldFilters)
	}
	if reader.InitialPosition != "tail" {
		t.Errorf("initial_position = %v", reader.InitialPosition)
	}
	if len(reader.Searches) != 2 || reader.Searches[1].(map[string]any)["name"] != "ips" {
		t.Errorf("searches = %v", reader.Searches)
	}
	// Tag key order is captured from the raw JSON: search 0 has no
	// "tags" object (nil row), search 1 declares {"geo": ...}.
	wantTagOrder := [][]string{nil, {"geo"}}
	if !reflect.DeepEqual(reader.SearchTagOrder, wantTagOrder) {
		t.Errorf("SearchTagOrder = %v want %v", reader.SearchTagOrder, wantTagOrder)
	}
	if len(reader.SecretFilters) != 1 || reader.SecretFilters[0].Pattern != "AVNS_[A-Za-z0-9-_]{15,123}" ||
		reader.SecretFilters[0].Replacement != "[REDACTED]" {
		t.Errorf("secret_filters = %+v", reader.SecretFilters)
	}
	if !reader.SecretFilterMetrics {
		t.Error("secret_filter_metrics should be true")
	}
	if reader.ThresholdForMetricEmit != 42 {
		t.Errorf("threshold_for_metric_emit = %d", reader.ThresholdForMetricEmit)
	}
	if reader.MatchKey != "_TRANSPORT" || reader.MatchValue != "stdout" {
		t.Errorf("reader match = %q/%v", reader.MatchKey, reader.MatchValue)
	}
	if len(reader.Senders) != 9 {
		t.Errorf("senders = %d entries, want 9", len(reader.Senders))
	}

	// Integer-valued journal_flags and initial_position.
	intReader := cfg.Readers["integer_flags"]
	if intReader.JournalFlagsInt == nil || *intReader.JournalFlagsInt != 8 || intReader.JournalFlagNames != nil {
		t.Errorf("integer journal_flags = %v/%v", intReader.JournalFlagsInt, intReader.JournalFlagNames)
	}
	if intReader.InitialPosition != int64(60) {
		t.Errorf("integer initial_position = %v", intReader.InitialPosition)
	}
}

func assertGoldenRawPreserved(t *testing.T, cfg *Config) {
	t.Helper()
	// The full parsed document is preserved for the running-config writer,
	// including keys unknown to the typed schema.
	if _, ok := cfg.Raw()["unknown_top_level"]; !ok {
		t.Error("unknown top-level key lost from the raw document")
	}
	// The readers blob equals the parsed readers value.
	if !reflect.DeepEqual(cfg.ReadersBlob(), cfg.Raw()["readers"]) {
		t.Error("readers blob must be the parsed readers value")
	}
}

// TestDecodeDefaults locks the load-time defaults (Python applies per-sender
// defaults in the sender constructors, not at load).
func TestDecodeDefaults(t *testing.T) {
	t.Parallel()
	cfg := mustDecode(t, `{"readers": {"r": {"senders": {}}}}`)

	if cfg.LogLevel != DefaultLogLevel {
		t.Errorf("LogLevel = %d, want %d (logging.INFO)", cfg.LogLevel, DefaultLogLevel)
	}
	if !cfg.Statsd.Disabled() {
		t.Error("statsd must be disabled when unset")
	}
	if cfg.MsgBufferMaxLength != DefaultMsgBufferMaxLength {
		t.Errorf("MsgBufferMaxLength = %d, want 50000", cfg.MsgBufferMaxLength)
	}
	if cfg.MsgBufferMaxBytes != DefaultMsgBufferBytes {
		t.Errorf("MsgBufferMaxBytes = %d, want 5242880", cfg.MsgBufferMaxBytes)
	}
	if len(cfg.FieldFilters) != 0 || len(cfg.UnitLogLevels) != 0 || len(cfg.Readers) != 1 {
		t.Errorf("sections = %v/%v/%v", cfg.FieldFilters, cfg.UnitLogLevels, cfg.Readers)
	}
	reader := cfg.Readers["r"]
	if reader.ThresholdForMetricEmit != DefaultThresholdForMetricEmit {
		t.Errorf("ThresholdForMetricEmit = %d, want 10", reader.ThresholdForMetricEmit)
	}
	if len(reader.Senders) != 0 || len(reader.UnitsToMatch) != 0 {
		t.Errorf("reader defaults = %+v", reader)
	}
	if reader.InitialPosition != nil || reader.JournalFlagNames != nil || reader.JournalFlagsInt != nil {
		t.Errorf("reader source defaults = %+v", reader)
	}
	if len(cfg.ReadersBlob()) != 1 {
		t.Error("absent readers must yield an empty blob, not nil")
	}
}

// TestDecodeIgnoresUnknownKeys: Python's config is plain dicts, so unknown
// keys never break loading.
func TestDecodeIgnoresUnknownKeys(t *testing.T) {
	t.Parallel()
	cfg := mustDecode(t, `{
        "future_key": 1,
        "readers": {"r": {"future_reader_key": "x", "senders": {"s": {"output_type": "file", "future_sender_key": 2}}}}
    }`)
	if _, ok := cfg.Raw()["future_key"]; !ok {
		t.Error("unknown top-level key dropped")
	}
	if _, ok := cfg.Raw()["readers"].(map[string]any)["r"].(map[string]any)["future_reader_key"]; !ok {
		t.Error("unknown reader key dropped")
	}
	sender := cfg.Readers["r"].Senders["s"].(map[string]any)
	if sender["future_sender_key"] != int64(2) {
		t.Error("unknown sender key dropped")
	}
}

// TestCaptureSearchTagOrder locks the raw-JSON tag-order capture that
// keeps perform_searches' config-order callable evaluation working
// (encoding/json's map decode discards key order; Python's json.load
// preserves it, journalpump.py:629-634).
func TestCaptureSearchTagOrder(t *testing.T) {
	t.Parallel()
	cfg := mustDecode(t, `{
        "readers": {
            "r": {
                "searches": [
                    {"name": "no-tags"},
                    {"name": "ordered", "tags": {"zeta": "1", "alpha": "ip_to_geohash(zeta,5)", "mid": "2"}},
                    {"name": "tags-not-object", "tags": ["x"]}
                ],
                "senders": {}
            },
            "other": {"senders": {}}
        }
    }`)
	// Rows exist only for searches that DECLARE a tags object (the
	// capture pads up to the declaring index); shorter rows mean the
	// order is unknown and NewSearches falls back to sorted keys.
	want := [][]string{nil, {"zeta", "alpha", "mid"}}
	if got := cfg.Readers["r"].SearchTagOrder; !reflect.DeepEqual(got, want) {
		t.Errorf("r.SearchTagOrder = %v want %v", got, want)
	}
	if cfg.Readers["other"].SearchTagOrder != nil {
		t.Errorf("other.SearchTagOrder = %v want nil", cfg.Readers["other"].SearchTagOrder)
	}
}

// TestCaptureSearchTagOrderDuplicateSearches locks the duplicate-key
// alignment: a duplicate "searches" key within one reader visits the
// token walker's array branch twice while the map decode keeps only the
// LAST array, so the capture must restart its index per occurrence —
// otherwise the rows misalign with the decoded searches and silently
// fall back to sorted keys.
func TestCaptureSearchTagOrderDuplicateSearches(t *testing.T) {
	t.Parallel()
	cfg := mustDecode(t, `{
        "readers": {
            "r": {
                "searches": [
                    {"name": "a", "tags": {"x": "1"}},
                    {"name": "b", "tags": {"y": "1"}},
                    {"name": "c", "tags": {"z": "1"}}
                ],
                "searches": [
                    {"name": "only", "tags": {"q": "1", "p": "2"}}
                ],
                "senders": {}
            }
        }
    }`)
	want := [][]string{{"q", "p"}} // aligned with the LAST array (map decode wins)
	if got := cfg.Readers["r"].SearchTagOrder; !reflect.DeepEqual(got, want) {
		t.Errorf("r.SearchTagOrder = %v want %v", got, want)
	}
}

// TestCaptureSearchTagOrderDuplicateReader locks the reader-level
// duplicate-key alignment: a duplicate reader NAME visits walkReader
// twice while the map decode keeps only the LAST occurrence, so the
// rows captured for the first occurrence must not leak into the kept
// one.
func TestCaptureSearchTagOrderDuplicateReader(t *testing.T) {
	t.Parallel()
	cfg := mustDecode(t, `{
        "readers": {
            "r": {
                "searches": [
                    {"name": "a", "tags": {"x": "1"}},
                    {"name": "b", "tags": {"y": "1"}}
                ],
                "senders": {}
            },
            "r": {
                "searches": [
                    {"name": "only", "tags": {"q": "1"}}
                ],
                "senders": {}
            }
        }
    }`)
	want := [][]string{{"q"}} // aligned with the LAST occurrence (map decode wins)
	if got := cfg.Readers["r"].SearchTagOrder; !reflect.DeepEqual(got, want) {
		t.Errorf("r.SearchTagOrder = %v want %v", got, want)
	}
}

// TestCaptureStatsdTagOrder pins the statsd.tags document-order capture:
// Python's dict order reaches the statsd wire, so the client must emit
// the global tags in config order, not sorted order.
func TestCaptureStatsdTagOrder(t *testing.T) {
	t.Parallel()
	cfg := mustDecode(t, `{
        "statsd": {"host": "127.0.0.1", "port": 8125, "tags": {"zulu": "1", "alpha": "2", "mike": "3"}},
        "readers": {"r": {"senders": {}}}
    }`)
	want := []string{"zulu", "alpha", "mike"}
	if got := cfg.Statsd.TagOrder; !reflect.DeepEqual(got, want) {
		t.Errorf("Statsd.TagOrder = %v want %v", got, want)
	}
	if got := cfg.Readers["r"].SearchTagOrder; got != nil {
		t.Errorf("r.SearchTagOrder = %v want nil", got)
	}
}

// TestCaptureStatsdTagOrderAbsent: no statsd.tags object means no order
// (the client falls back to sorted keys, like a synthetic document).
func TestCaptureStatsdTagOrderAbsent(t *testing.T) {
	t.Parallel()
	cfg := mustDecode(t, `{"statsd": {"host": "h", "port": 1}, "readers": {"r": {"senders": {}}}}`)
	if cfg.Statsd.TagOrder != nil {
		t.Errorf("Statsd.TagOrder = %v want nil", cfg.Statsd.TagOrder)
	}
}

// TestCaptureSenderOrder: the reader's "senders" keys are captured in
// config document order, matching Python's configured_senders.items()
// iteration in initialize_senders.
func TestCaptureSenderOrder(t *testing.T) {
	t.Parallel()
	cfg := mustDecode(t, `{
        "readers": {"r": {"senders": {
            "zulu": {"output_type": "file", "file_output": "/tmp/z"},
            "alpha": {"output_type": "file", "file_output": "/tmp/a"},
            "mike": {"output_type": "file", "file_output": "/tmp/m"}
        }}}
    }`)
	want := []string{"zulu", "alpha", "mike"}
	if got := cfg.Readers["r"].SenderOrder; !reflect.DeepEqual(got, want) {
		t.Errorf("SenderOrder = %v want %v", got, want)
	}
	// A duplicate key keeps its first position (Python dict semantics).
	dup := mustDecode(t, `{
        "readers": {"r": {"senders": {
            "b": {"output_type": "file", "file_output": "/tmp/b"},
            "a": {"output_type": "file", "file_output": "/tmp/a"},
            "b": {"output_type": "file", "file_output": "/tmp/b2"}
        }}}
    }`)
	if got := dup.Readers["r"].SenderOrder; !reflect.DeepEqual(got, []string{"b", "a"}) {
		t.Errorf("duplicate SenderOrder = %v want [b a]", got)
	}
}

// TestDecodeToleratedValues: values Python accepts without error keep
// loading with the same semantics.
func TestDecodeToleratedValues(t *testing.T) {
	t.Parallel()
	cfg := mustDecode(t, `{
        "statsd": false,
        "write_running_config": false,
        "readers": {
            "r": {
                "searches": {},
                "secret_filter_metrics": "yes",
                "senders": {}
            }
        }
    }`)
	if !cfg.Statsd.Disabled() {
		t.Error("falsy statsd value must mean disabled (Python: `or {}`)")
	}
	if cfg.Readers["r"].Searches != nil {
		t.Error("falsy searches value must mean no searches (Python: `searches or []`)")
	}
	if cfg.Readers["r"].SecretFilterMetrics {
		t.Error("secret_filter_metrics activates only for JSON true (Python: `is True`)")
	}

	// Empty readers is the same as absent readers for diffing purposes.
	absent := mustDecode(t, `{}`)
	explicitEmpty := mustDecode(t, `{"readers": {}}`)
	if ReadersChanged(absent.ReadersBlob(), explicitEmpty.ReadersBlob()) {
		t.Error("absent vs empty readers must not count as a change")
	}
}

// TestDecodeRejectsWrongTypedValues locks the typed-decoding deviation:
// wrong-typed values fail at load with a key-qualified error.
func TestDecodeRejectsWrongTypedValues(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		config  string
		wantErr string
	}{
		{"log_level unknown name", `{"log_level": "NOISY"}`, "log_level: unknown log level name"},
		{"log_level wrong type", `{"log_level": 1.5}`, "log_level: must be a log level name or an integer"},
		{"statsd wrong type", `{"statsd": "127.0.0.1"}`, "statsd: must be an object"},
		{"statsd port wrong type", `{"statsd": {"host": "h", "port": "8125"}}`, "statsd.port: must be an integer"},
		{"statsd host wrong type", `{"statsd": {"host": 5}}`, "statsd.host: must be a string"},
		{"statsd tag wrong type", `{"statsd": {"host": "h", "port": 1, "tags": {"a": true}}}`, "statsd.tags.a: must be a string"},
		{"tags wrong type", `{"tags": ["a"]}`, "tags: must be an object"},
		{"msg_buffer_max_length wrong type", `{"msg_buffer_max_length": "50000"}`, "msg_buffer_max_length: must be an integer"},
		{"msg_buffer_max_length float", `{"msg_buffer_max_length": 50000.5}`, "msg_buffer_max_length: must be an integer"},
		{"geoip_database wrong type", `{"geoip_database": 5}`, "geoip_database: must be a string"},
		{"write_running_config wrong type", `{"write_running_config": "yes"}`, "write_running_config: must be a boolean"},
		{"field_filters wrong type", `{"field_filters": []}`, "field_filters: must be an object"},
		{"field filter missing fields", `{"field_filters": {"f": {"type": "whitelist"}}}`, "field_filters.f: 'fields' is required"},
		{"field filter fields wrong type", `{"field_filters": {"f": {"fields": "MESSAGE"}}}`, "field_filters.f.fields: must be a list"},
		{"field filter field wrong type", `{"field_filters": {"f": {"fields": [1]}}}`, "field_filters.f.fields[0]: must be a string"},
		{"unit_log_levels wrong type", `{"unit_log_levels": []}`, "unit_log_levels: must be an object"},
		{"unit log level missing glob", `{"unit_log_levels": {"u": [{"log_level": "INFO"}]}}`, "unit_log_levels.u[0]: 'service_glob' is required"},
		{"unit log level unknown name", `{"unit_log_levels": {"u": [{"service_glob": "*", "log_level": "LOUD"}]}}`, "unknown log level"},
		{"readers wrong type", `{"readers": []}`, "readers: must be an object"},
		{"readers null", `{"readers": null}`, "readers: must be an object"},
		{"reader wrong type", `{"readers": {"r": []}}`, "readers.r: must be an object"},
		{"journal_flags wrong type", `{"readers": {"r": {"journal_flags": "SYSTEM", "senders": {}}}}`, "readers.r.journal_flags: must be a list of flag names or an integer"},
		{"units_to_match wrong type", `{"readers": {"r": {"units_to_match": "sshd", "senders": {}}}}`, "readers.r.units_to_match: must be a list of strings"},
		{"initial_position wrong type", `{"readers": {"r": {"initial_position": 1.5, "senders": {}}}}`, "readers.r.initial_position:"},
		{"searches wrong type", `{"readers": {"r": {"searches": {"s": 1}, "senders": {}}}}`, "readers.r.searches: must be a list"},
		{"threshold wrong type", `{"readers": {"r": {"threshold_for_metric_emit": "10", "senders": {}}}}`, "readers.r.threshold_for_metric_emit: must be an integer"},
		{"senders wrong type", `{"readers": {"r": {"senders": []}}}`, "readers.r.senders: must be an object"},
		{"sender wrong type", `{"readers": {"r": {"senders": {"s": "kafka"}}}}`, "readers.r.senders.s: must be an object"},
		{"max_send_interval non-numeric", `{"readers": {"r": {"senders": {"s": {"output_type": "file", "file_output": "/tmp/x", "max_send_interval": "fast"}}}}}`, "readers.r.senders.s.max_send_interval: must be a number"},
		{"max_send_interval bool", `{"readers": {"r": {"senders": {"s": {"output_type": "file", "file_output": "/tmp/x", "max_send_interval": true}}}}}`, "readers.r.senders.s.max_send_interval: must be a number"},
		{"secret_filters not a list", `{"readers": {"r": {"secret_filters": {"pattern": "x"}}}}`, "invalid secret_filters configuration - must be a list"},
		{"secret_filters not a list (entry)", `{"readers": {"r": {"secret_filters": ["x"]}}}`, "invalid secret_filters configuration - must be a list"},
		{"secret_filter missing replacement", `{"readers": {"r": {"secret_filters": [{"pattern": "x"}]}}}`, "invalid secret_filters configuration - missing field 'replacement'"},
		{"secret_filter null replacement", `{"readers": {"r": {"secret_filters": [{"pattern": "x", "replacement": null}]}}}`, "invalid secret_filters configuration - missing field 'replacement'"},
		{"secret_filter missing pattern", `{"readers": {"r": {"secret_filters": [{"replacement": "r"}]}}}`, "invalid secret_filters configuration - missing field 'pattern'"},
		{"secret_filter null pattern", `{"readers": {"r": {"secret_filters": [{"replacement": "r", "pattern": null}]}}}`, "invalid secret_filters configuration - missing field 'pattern'"},
		{"match_key wrong type", `{"readers": {"r": {"match_key": 5, "senders": {}}}}`, "readers.r.match_key: must be a string"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := Decode([]byte(tc.config))
			if err == nil {
				t.Fatalf("Decode succeeded, want error containing %q", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error = %q, want containing %q", err.Error(), tc.wantErr)
			}
		})
	}
}

// TestReadersChanged locks the reload-diff semantics: Python compares parsed
// dicts, so key order and whitespace never trigger a reload; a value change
// does.
func TestReadersChanged(t *testing.T) {
	t.Parallel()
	base := `{"readers": {"a": {"senders": {}}, "b": {"senders": {}}}}`
	reordered := `{"readers": {"b": { "senders":{} },  "a" : {"senders": {}}}}`
	noReaders := `{}`

	changed := mustDecode(t, base)
	same := mustDecode(t, reordered)
	none := mustDecode(t, noReaders)

	if !ReadersChanged(nil, changed.ReadersBlob()) {
		t.Error("first load (nil active) must count as changed")
	}
	if ReadersChanged(changed.ReadersBlob(), same.ReadersBlob()) {
		t.Error("key-order/whitespace difference must NOT trigger a reload")
	}
	if !ReadersChanged(changed.ReadersBlob(), none.ReadersBlob()) {
		t.Error("readers removal must trigger a reload")
	}
	valueChanged := mustDecode(t, `{"readers": {"a": {"senders": {}}, "b": {"senders": {}, "extra": 1}}}`)
	if !ReadersChanged(changed.ReadersBlob(), valueChanged.ReadersBlob()) {
		t.Error("value change must trigger a reload")
	}
	// Numeric equal-values across representations behave like Python's
	// int/float equality (0.3 vs 0.30 compare equal).
	f1 := mustDecode(t, `{"readers": {"a": {"senders": {}, "ratio": 0.3}}}`)
	f2 := mustDecode(t, `{"readers": {"a": {"senders": {}, "ratio": 0.30}}}`)
	if ReadersChanged(f1.ReadersBlob(), f2.ReadersBlob()) {
		t.Error("numerically equal floats must NOT trigger a reload")
	}
}

// TestReadersChangedNumericEquality is the quirk-lock for Python dict
// equality across numeric types: 10 == 10.0 and 1 == True compare equal
// (Python's bool is an int subclass), so neither triggers a reload.
func TestReadersChangedNumericEquality(t *testing.T) {
	t.Parallel()
	intVersion := mustDecode(t, `{"readers": {"a": {"senders": {}, "x": 10}}}`)
	floatVersion := mustDecode(t, `{"readers": {"a": {"senders": {}, "x": 10.0}}}`)
	if ReadersChanged(intVersion.ReadersBlob(), floatVersion.ReadersBlob()) {
		t.Error("10 vs 10.0 must NOT trigger a reload (Python: {\"x\":10} == {\"x\":10.0})")
	}
	boolVersion := mustDecode(t, `{"readers": {"a": {"senders": {}, "x": true}}}`)
	oneVersion := mustDecode(t, `{"readers": {"a": {"senders": {}, "x": 1}}}`)
	if ReadersChanged(oneVersion.ReadersBlob(), boolVersion.ReadersBlob()) {
		t.Error("1 vs true must NOT trigger a reload (Python: 1 == True)")
	}
	zero := mustDecode(t, `{"readers": {"a": {"senders": {}, "x": 0}}}`)
	falseVersion := mustDecode(t, `{"readers": {"a": {"senders": {}, "x": false}}}`)
	if ReadersChanged(zero.ReadersBlob(), falseVersion.ReadersBlob()) {
		t.Error("0 vs false must NOT trigger a reload (Python: 0 == False)")
	}
	// ... but a real value change does.
	changed := mustDecode(t, `{"readers": {"a": {"senders": {}, "x": 11}}}`)
	if !ReadersChanged(intVersion.ReadersBlob(), changed.ReadersBlob()) {
		t.Error("10 vs 11 must trigger a reload")
	}
	// A number never equals a string or null (Python: 1 == "1" is False).
	stringVersion := mustDecode(t, `{"readers": {"a": {"senders": {}, "x": "10"}}}`)
	if !ReadersChanged(intVersion.ReadersBlob(), stringVersion.ReadersBlob()) {
		t.Error("10 vs \"10\" must trigger a reload")
	}
	nullVersion := mustDecode(t, `{"readers": {"a": {"senders": {}, "x": null}}}`)
	if !ReadersChanged(intVersion.ReadersBlob(), nullVersion.ReadersBlob()) {
		t.Error("10 vs null must trigger a reload")
	}
}

// TestDecodeStrictParsing locks the strict-load behaviors Python shares:
// trailing data after the first JSON document ("Extra data") and invalid
// UTF-8 bytes (UnicodeDecodeError) are load errors.
func TestDecodeStrictParsing(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		config  string
		wantErr string
	}{
		{"trailing object", `{"readers": {}} {"readers": {}}`, "extra data"},
		{"trailing garbage", `{} trailing`, "extra data"},
		{"invalid utf-8", "{\"readers\": {}}\xff\xfe", "not valid UTF-8"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := Decode([]byte(tc.config))
			if err == nil {
				t.Fatalf("Decode succeeded, want error containing %q", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error = %q, want containing %q", err.Error(), tc.wantErr)
			}
		})
	}
}

// TestDecodeLogLevelAliases locks Python's logging._nameToLevel aliases:
// FATAL == CRITICAL and WARN == WARNING both load.
func TestDecodeLogLevelAliases(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		level string
		want  int64
	}{
		{"WARN alias", "WARN", 30},
		{"FATAL alias", "FATAL", 50},
		{"canonical names", "WARNING", 30},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := mustDecode(t, `{"log_level": "`+tc.level+`"}`)
			if cfg.LogLevel != tc.want {
				t.Errorf("LogLevel(%q) = %d, want %d", tc.level, cfg.LogLevel, tc.want)
			}
		})
	}
}

// TestCaptureExtraFieldValuesRaw locks the extra_field_values subtree
// capture: the raw JSON keeps the document key order (map decoding
// discards it, but the wire key order and the sender-side extras merge
// depend on it), senders without the key have no entry, and a non-object
// value is captured verbatim so the wiring can warn on it.
func TestCaptureExtraFieldValuesRaw(t *testing.T) {
	t.Parallel()
	cfg := mustDecode(t, `{
        "readers": {
            "r": {
                "senders": {
                    "ordered": {"output_type": "file", "extra_field_values": {"zeta": 1, "alpha": "a", "mid": true}},
                    "none": {"output_type": "file"},
                    "wrong": {"output_type": "file", "extra_field_values": [1, 2]}
                }
            }
        }
    }`)
	raw := cfg.Readers["r"].ExtraFieldValuesRaw
	if got := string(raw["ordered"]); got != `{"zeta": 1, "alpha": "a", "mid": true}` {
		t.Errorf("ordered extras = %s, want the document-order JSON", got)
	}
	if _, present := raw["none"]; present {
		t.Errorf("none has an extras entry: %s", raw["none"])
	}
	if got := string(raw["wrong"]); got != `[1, 2]` {
		t.Errorf("wrong extras = %s, want [1, 2]", got)
	}
}
