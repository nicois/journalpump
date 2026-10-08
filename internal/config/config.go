// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

// Package config loads journalpump.json into typed structures with the same
// defaults and validation outcomes as the Python implementation
// (journalpump/journalpump.py, which is the source of truth for accepted
// keys and defaults — not README.rst).
//
// Parity notes:
//   - Unknown keys are ignored, as in Python (the config is plain dicts
//     there); they are preserved in the raw document so the running-config
//     writer reproduces them.
//   - Wrong-typed values are rejected at load with a key-qualified error.
//     Sanctioned deviation (spec §6.1a, NEWS in Task 28): Python crashed
//     mid-run when such a value was used; a few keys Python silently
//     coerced or truthiness-tested are rejected here too. Values Python
//     tolerates without error keep loading: e.g. secret_filter_metrics
//     activates only for JSON true (Python `is True`), and searches
//     accepts any falsy value as "no searches" (Python `searches or []`).
package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"unicode/utf8"
)

const (
	// DefaultMsgBufferMaxLength mirrors journalpump.py's
	// config.get("msg_buffer_max_length", 50000).
	DefaultMsgBufferMaxLength = int64(50000)
	// DefaultMsgBufferBytes mirrors journalpump.py's _5_MB default for
	// msg_buffer_max_bytes.
	DefaultMsgBufferBytes = int64(5 * 1024 * 1024)
	// DefaultLogLevel mirrors daemon.py's config.get("log_level",
	// logging.INFO) (logging.INFO == 20).
	DefaultLogLevel = int64(20)
	// DefaultThresholdForMetricEmit mirrors journalpump.py's
	// int(config.get("threshold_for_metric_emit", 10)).
	DefaultThresholdForMetricEmit = int64(10)
)

// SeverityLevels mirrors journalpump/types.py:LOG_SEVERITY_MAPPING: the
// syslog-style names accepted for unit_log_levels entries.
var SeverityLevels = map[string]int64{
	"EMERGENCY": 0,
	"ALERT":     1,
	"CRITICAL":  2,
	"ERROR":     3,
	"WARNING":   4,
	"NOTICE":    5,
	"INFO":      6,
	"DEBUG":     7,
}

// pythonLogLevels mirrors logging._nameToLevel: the level-name strings
// Python's logging.setLevel accepts at configure_logging time.
var pythonLogLevels = map[string]int64{
	"CRITICAL": 50,
	"FATAL":    50, // alias, per logging._nameToLevel
	"ERROR":    40,
	"WARN":     30, // alias, per logging._nameToLevel
	"WARNING":  30,
	"INFO":     20,
	"DEBUG":    10,
	"NOTSET":   0,
}

// StatsdConfig is the "statsd" section. An unset host or port disables
// stats sending entirely (Python: StatsClient._send checks
// `None in self._dest_addr`); there is no implicit 127.0.0.1 default at
// the config level — journalpump.py passes stats.get("host") straight
// through, so a missing host means "no stats".
type StatsdConfig struct {
	Host   string // empty = disabled
	Port   *int64 // nil = disabled
	Tags   map[string]string
	Prefix string // default ""
}

// Disabled reports whether the statsd client must not send anything.
func (s StatsdConfig) Disabled() bool {
	return s.Host == "" || s.Port == nil
}

// FieldFilterConfig is one entry of the top-level "field_filters" map.
type FieldFilterConfig struct {
	Name string
	// Whitelist is true unless type == "blacklist" (Python:
	// config.get("type", "whitelist") == "whitelist").
	Whitelist bool
	// Fields holds the field names unmodified; the leading-underscore trim
	// and lowercasing happen in the filter itself (Task 5).
	Fields []string
}

// UnitLogLevelEntry is one (service_glob, log_level) rule of a
// "unit_log_levels" entry.
type UnitLogLevelEntry struct {
	ServiceGlob string
	LogLevel    string // one of SeverityLevels' keys
}

// SecretFilterConfig is one entry of a reader's "secret_filters" list.
// Regex compilation happens in internal/filter (Task 5).
type SecretFilterConfig struct {
	Raw         map[string]any
	Pattern     string
	Replacement string
}

// ReaderConfig is the typed view of one "readers" entry.
type ReaderConfig struct {
	Name string
	// journal_flags: either a list of flag names (resolved against the
	// journal library constants at reader construction, Task 15) or an
	// integer bit mask; both nil = Python's flags=None default.
	JournalFlagNames []string
	JournalFlagsInt  *int64
	JournalPath      string
	JournalFiles     []string
	JournalNamespace string
	UnitsToMatch     []string
	// JournaldFilters holds the raw field->value dicts passed to
	// sd_journal add_match at reader construction (Task 15).
	JournaldFilters []map[string]any
	// InitialPosition is nil, "head"/"tail", or integer seconds from boot;
	// Python passes any other value through branches that ignore it.
	InitialPosition any
	// Searches is the raw parsed search list; compiled by internal/filter
	// (Task 5) and the tag callables (Task 6). Empty means no searches.
	Searches []any
	// SecretFilters is the validated structure only; regexes compile in
	// internal/filter (Task 5).
	SecretFilters          []SecretFilterConfig
	SecretFilterMetrics    bool // true only for JSON true, mirroring Python's `is True`
	ThresholdForMetricEmit int64
	MatchKey               string
	MatchValue             any
	// Senders holds the raw parsed per-sender configs; each sender package
	// decodes its own key set (per-sender defaults are applied by the
	// sender constructors, as in Python).
	Senders map[string]any
}

// Config is the typed view of a parsed journalpump.json document.
type Config struct {
	LogLevel int64
	Statsd   StatsdConfig
	Tags     map[string]string
	// GeoipDatabase enables GeoIP enrichment when non-empty (Task 6).
	GeoipDatabase string
	// MsgBufferMaxLength / MsgBufferMaxBytes are the backpressure budget
	// defaults shared by all readers.
	MsgBufferMaxLength int64
	MsgBufferMaxBytes  int64

	JSONStateFilePath     string
	JSONRunningConfigPath string
	WriteRunningConfig    bool

	MatchKey   string
	MatchValue any

	FieldFilters  map[string]FieldFilterConfig
	UnitLogLevels map[string][]UnitLogLevelEntry
	Readers       map[string]ReaderConfig

	readersBlob map[string]any // parsed "readers" blob for whole-blob reload diffing
	raw         map[string]any // full parsed document (running-config writer input)
}

// Raw returns the full parsed document, including keys unknown to the typed
// schema. The running-config writer dumps this, like Python dumps
// self.config.
func (c *Config) Raw() map[string]any {
	return c.raw
}

// ReadersBlob returns the parsed "readers" value for whole-blob reload
// diffing (Python: readers_active_config). Absent readers yield an empty
// map, mirroring config.get("readers", {}).
func (c *Config) ReadersBlob() map[string]any {
	return c.readersBlob
}

// ReadersChanged reports whether the readers blob differs from the active
// one (Python: `if self.readers_active_config == new_config`). Comparison
// is over parsed structures with Python's == semantics (blobEqual), so key
// order and whitespace never trigger a reload, and numeric types compare
// by value like Python dicts do (10 == 10.0 == True); a nil active blob
// (first load) always counts as changed, like Python's None == {}.
func ReadersChanged(active, blob map[string]any) bool {
	if active == nil {
		return true
	}
	return !blobEqual(active, blob)
}

// blobEqual compares two parsed JSON values with Python's == semantics:
// dict/list structure, scalars by value across numeric types (Python's
// bool is an int subclass, so 1 == True and 0 == False), None == None.
func blobEqual(a, b any) bool {
	switch av := a.(type) {
	case map[string]any:
		bv, ok := b.(map[string]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for key, value := range av {
			other, present := bv[key]
			if !present || !blobEqual(value, other) {
				return false
			}
		}
		return true
	case []any:
		bv, ok := b.([]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for i := range av {
			if !blobEqual(av[i], bv[i]) {
				return false
			}
		}
		return true
	default:
		return scalarEqual(a, b)
	}
}

// scalarNumber renders a scalar as a float for Python-style numeric
// comparison. Note: Python compares int vs float exactly, so the float64
// rendering here diverges for integers beyond float64 precision — those
// cannot appear as user-facing config semantics.
func scalarNumber(v any) (float64, bool) {
	switch typed := v.(type) {
	case bool:
		if typed {
			return 1, true
		}
		return 0, true
	case int64:
		return float64(typed), true
	case float64:
		return typed, true
	case json.Number: // out-of-int64 integer literal
		value, err := typed.Float64()
		return value, err == nil
	}
	return 0, false
}

func scalarEqual(a, b any) bool {
	valueA, numericA := scalarNumber(a)
	valueB, numericB := scalarNumber(b)
	if numericA && numericB {
		return valueA == valueB
	}
	if numericA != numericB {
		// Python: a number never equals a string/None (e.g. 1 == "1" is
		// False).
		return false
	}
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if strA, ok := a.(string); ok {
		strB, ok := b.(string)
		return ok && strA == strB
	}
	return reflect.DeepEqual(a, b)
}

// Load reads and decodes the config file at path.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Decode(data)
}

// Decode parses journalpump.json content into a typed Config.
func Decode(data []byte) (*Config, error) {
	// Python reads the file as UTF-8 text and raises UnicodeDecodeError on
	// invalid bytes; Go's decoder would silently substitute U+FFFD.
	if !utf8.Valid(data) {
		return nil, fmt.Errorf("parsing journalpump.json: config is not valid UTF-8")
	}
	var raw map[string]any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&raw); err != nil {
		return nil, fmt.Errorf("parsing journalpump.json: %w", err)
	}
	if decoder.More() {
		// Python: json.load raises ValueError("Extra data") on trailing
		// content after the first document.
		return nil, fmt.Errorf("parsing journalpump.json: extra data after the JSON document")
	}
	normalizeNumbers(raw)
	return newConfig(raw)
}

// isIntegerLiteral reports whether the JSON number literal has no fraction
// or exponent part (JSON grammar guarantees only digits and a minus sign
// remain).
func isIntegerLiteral(s string) bool {
	return !strings.ContainsAny(s, ".eE")
}

// normalizeNumbers converts json.Number values the way Python's json.load
// does: integer literals become int64 (Python ints are arbitrary precision,
// so out-of-int64 integer literals stay as json.Number and marshal
// verbatim), everything else becomes float64. Value-equality comparisons
// over the parsed readers blob (reload diffing) then match Python's
// int/float semantics via blobEqual. Recorded deviation (NEWS, Task 28):
// the running-config dump is parsed-JSON-equal to Python's
// json.dump(..., sort_keys=True) but not byte-identical — Go renders
// float64 like 60.0 as "60", 1e-05 as "1e-05", 1e+20 as "1e+20", and emits
// non-ASCII as raw UTF-8 where Python's ensure_ascii=True writes \uXXXX.
func normalizeNumbers(raw map[string]any) {
	for key, val := range raw {
		switch typed := val.(type) {
		case map[string]any:
			normalizeNumbers(typed)
		case []any:
			for i, item := range typed {
				if num, ok := item.(json.Number); ok {
					typed[i] = normalizeNumber(num)
					continue
				}
				normalizeNumbersValue(item)
			}
		case json.Number:
			raw[key] = normalizeNumber(typed)
		}
	}
}

func normalizeNumbersValue(v any) {
	switch typed := v.(type) {
	case map[string]any:
		normalizeNumbers(typed)
	case []any:
		for i, item := range typed {
			if num, ok := item.(json.Number); ok {
				typed[i] = normalizeNumber(num)
				continue
			}
			normalizeNumbersValue(item)
		}
	}
}

func normalizeNumber(n json.Number) any {
	s := n.String()
	if isIntegerLiteral(s) {
		if i, err := strconv.ParseInt(s, 10, 64); err == nil {
			return i
		}
		return n // out-of-int64 integer literal: keep it verbatim
	}
	if f, err := n.Float64(); err == nil {
		return f
	}
	return n
}

// asInt64 reports whether v is an integer (Python isinstance(v, int)).
func asInt64(v any) (int64, bool) {
	if i, ok := v.(int64); ok {
		return i, true
	}
	return 0, false
}

// isFalsy mirrors Python truthiness for the values the Python code coerces
// with `or {}` / `or []`: nil, false, 0, 0.0, "", empty map, empty list.
func isFalsy(v any) bool {
	switch typed := v.(type) {
	case nil:
		return true
	case bool:
		return !typed
	case int64:
		return typed == 0
	case float64:
		return typed == 0
	case string:
		return typed == ""
	case map[string]any:
		return len(typed) == 0
	case []any:
		return len(typed) == 0
	}
	return false
}
