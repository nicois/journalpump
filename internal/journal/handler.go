// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package journal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"reflect"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Aiven-Open/journalpump/internal/filter"
	"github.com/Aiven-Open/journalpump/internal/tagged"
)

// StatsSink is the statsd subset the handler needs. statsd imports this
// package for the Python float wire rendering, so the dependency is
// declared as an interface here; *statsd.Client satisfies it directly.
type StatsSink interface {
	Increase(metric string, incValue int64, tags map[string]string)
}

// Constants ported from journalpump/senders/base.py:25-26.
const (
	// MaxKafkaMessageSize is base.py:MAX_KAFKA_MESSAGE_SIZE — the
	// serialized-size threshold above which the entry is replaced by a
	// truncation error record.
	MaxKafkaMessageSize = 1024 * 1024
	// TruncatedMessagePreviewSize is base.py:TRUNCATED_MESSAGE_PREVIEW_SIZE.
	TruncatedMessagePreviewSize = 1024
)

// HandlerLogger is the logging subset the handler needs (Python's
// logging.getLogger("JournalObjectHandler")); the standard library
// *log.Logger satisfies it (same seam as internal/sender).
type HandlerLogger interface {
	Printf(format string, args ...any)
}

// warningLogger is the OPTIONAL HandlerLogger capability for WARNING-level
// lines (the level-aware journald sink provides Warningf).
type warningLogger interface {
	Warningf(format string, args ...any)
}

// HandlerConfig carries the pump- and reader-side state the per-message
// handler consults (Python's JournalReader and JournalPump attributes
// the handler reaches): the search set, the match_key/match_value pair
// of both levels, the secret filters, the too_long stats emission and
// the reader callbacks. All fields are read-only for the handler.
type HandlerConfig struct {
	// ReaderName is the reader's config name (the too_long tag value).
	ReaderName string
	// Searcher performs the reader's searches against the RAW entry
	// (Python reader.perform_searches(self.jobject)); nil or an empty
	// search set means the searches gate is skipped (Python
	// `self.reader.searches and ...`).
	Searcher *filter.Searcher
	// ReaderMatchKey / ReaderMatchValue are the reader-level
	// match_key/match_value (journalpump.py:642-653). An unset
	// match_value (nil) means never-match when a match_key is set — the
	// Task 15 pinned guard (parity doc): Python would compare against
	// None, which converted entry values (never None) never equal.
	ReaderMatchKey   string
	ReaderMatchValue any
	// SecretFilters are the reader's compiled secret filters.
	SecretFilters []*filter.SecretFilter
	// SecretFilterMetrics mirrors reader.secret_filter_metrics.
	SecretFilterMetrics bool
	// PumpMatchKey / PumpMatchValue are the pump-level match pair
	// (journalpump.py:998-1003).
	PumpMatchKey   string
	PumpMatchValue any
	// Stats is the pump-level statsd client (nil = Python self.stats
	// None: the too_long counter is skipped, everything else proceeds).
	Stats StatsSink
	// PumpTags is the pump-level base tag set (Python self.make_tags()).
	PumpTags *tagged.Tagged
	// IncLineStats ports reader.inc_line_stats (called once per
	// processed entry with the max serialized size, journalpump.py:746).
	IncLineStats func(journalLines, journalBytes int)
	// NoteSecretFilterMatch increments the reader's
	// secret_filter_matches counter (Python
	// self.reader.secret_filter_matches += 1, journalpump.py:828).
	NoteSecretFilterMatch func()
	// Log receives the too_long warning (Python log.warning).
	Log HandlerLogger
	// Now is the test seam for the fallback timestamp (Python
	// datetime.now(UTC).replace(tzinfo=None)); nil = time.Now.
	Now func() time.Time
}

// SenderTarget is one live sender the handler enqueues into — the Go
// counterpart of one entry in Python's reader.senders dict: the
// sender-resolved filter inputs plus the buffer admission callback
// (Python sender.msg_buffer.add_item).
type SenderTarget struct {
	// Name is the sender name (diagnostics only; the buffer is reached
	// through AddItem).
	Name string
	// FieldFilter is the sender's field_filter (nil = none; the cache
	// key becomes the empty string, journalpump.py:757).
	FieldFilter *filter.FieldFilter
	// UnitLogLevels is the sender's unit_log_level (nil = none).
	UnitLogLevels *filter.UnitLogLevel
	// ExtraFieldValues are the sender's extra_field_values in config
	// document order (empty when absent, empty, or warned as a
	// non-object at wiring time).
	ExtraFieldValues []Field
	// AddItem admits one serialized payload with the entry's cursor
	// (Python msg_buffer.add_item(item=..., cursor=...)).
	AddItem func(item []byte, cursor string)
}

// Handler is the per-message pipeline of journalpump.py
// (JournalObjectHandler, 699-830): one instance per journal entry, like
// Python constructs one per read_single_message call. The per-message
// state is the serialized-JSON cache (self.json_objects, keyed by field
// filter name) and the once-per-handler error_reported flag.
type Handler struct {
	cfg           HandlerConfig
	errorReported bool
	jsonObjects   map[string][]byte
}

// NewHandler builds the per-message handler.
func NewHandler(cfg HandlerConfig) *Handler {
	return &Handler{cfg: cfg, jsonObjects: map[string][]byte{}}
}

// Process ports JournalObjectHandler.process (journalpump.py:708-748)
// for one entry. It returns the Python SingleMessageReadResult as
// (hasMore, bytesRead, hasBytes): hasBytes false mirrors
// bytes_read=None (skipped/dropped entries — the caller then does NOT
// decrement its chunk budgets, journalpump.py:1036-1038), bytesRead is
// the max serialized size across the per-message JSON cache.
//
// The entry's Fields are the RAW converted fields (keys WITH
// underscores, []byte for undecodable values); the searches gate runs
// against them. Everything else works on the processed view (keys
// stripped of underscores, bytes repr()-ed) — and the processed slice
// is shared and MUTATED across senders (the timestamp and
// extra_field_values assignments of _get_or_generate_json land on
// Python's shared new_entry dict), deliberately reproducing the
// cross-sender contamination quirk of journalpump.py:766-769: sender
// B's serialization contains sender A's extras whenever their field
// filters differ.
//
// A non-nil error corresponds to a Python exception escaping process()
// (only the truncation-path UnicodeDecodeError can escape): the caller
// routes it through the broad read_single_message handler.
func (h *Handler) Process(entry Entry, targets []SenderTarget) (hasMore bool, bytesRead int, hasBytes bool, err error) {
	processed := ProcessEntry(entry.Fields)

	if entry.Cursor == "" {
		// Python: log.debug("No more journal entries to read") — a debug
		// line, not reproduced (Go runs stdout logging at info).
		return false, 0, false, nil
	}

	if h.cfg.Searcher != nil && len(h.cfg.Searcher.Searches()) > 0 {
		// perform_searches runs against the RAW entry — keys with
		// underscores, []byte values (journalpump.py:720).
		if !h.cfg.Searcher.PerformSearches(fieldMap(entry.Fields)) {
			return true, 0, false, nil
		}
	}

	if !checkMatch(processed, h.cfg.PumpMatchKey, h.cfg.PumpMatchValue) {
		return true, 0, false, nil
	}
	if !checkMatch(processed, h.cfg.ReaderMatchKey, h.cfg.ReaderMatchValue) {
		return true, 0, false, nil
	}

	// Redact secrets before other filtering for effective leak detection
	// metrics (journalpump.py:729-731).
	if len(h.cfg.SecretFilters) > 0 {
		view := fieldMap(processed)
		if filter.ApplySecretFilters(view, h.cfg.SecretFilters) {
			if h.cfg.SecretFilterMetrics && h.cfg.NoteSecretFilterMatch != nil {
				h.cfg.NoteSecretFilterMatch()
			}
			// Python data["MESSAGE"] = msg keeps the key's position.
			processed = SetField(processed, "MESSAGE", view["MESSAGE"])
		}
	}

	for _, target := range targets {
		jsonEntry, err := h.getOrGenerateJSON(
			target.FieldFilter, target.UnitLogLevels, target.ExtraFieldValues, &processed,
		)
		if err != nil {
			return false, 0, false, err
		}
		// Python: isinstance(json_entry, bytes) and json_entry — a None
		// (unit-level drop) or {} (cache-hit unit-level drop) result is
		// skipped.
		if len(jsonEntry) > 0 {
			if target.AddItem != nil {
				target.AddItem(jsonEntry, entry.Cursor)
			}
		}
	}

	maxBytes := 0
	if len(h.jsonObjects) > 0 {
		for _, cached := range h.jsonObjects {
			if len(cached) > maxBytes {
				maxBytes = len(cached)
			}
		}
		if h.cfg.IncLineStats != nil {
			h.cfg.IncLineStats(1, maxBytes)
		}
	}
	return true, maxBytes, true, nil
}

// getOrGenerateJSON ports _get_or_generate_json (journalpump.py:750-789).
// data is the shared processed entry; the timestamp and extras
// assignments mutate it (the cross-sender contamination quirk — see
// Process).
func (h *Handler) getOrGenerateJSON(
	fieldFilter *filter.FieldFilter, unitLogLevels *filter.UnitLogLevel,
	extraFieldValues []Field, data *[]Field,
) ([]byte, error) {
	ffName := ""
	if fieldFilter != nil {
		ffName = fieldFilter.Name()
	}
	if cached, ok := h.jsonObjects[ffName]; ok {
		// Cache hit: the cached bytes are the FIELD-FILTERED entry —
		// re-check the unit log level against the parsed cache (Python
		// json.loads the bytes, journalpump.py:759), so fields the field
		// filter dropped are absent and the unit check sees them absent.
		return h.filterByLogLevel(cached, unitLogLevels), nil
	}

	// Always set a timestamp field that gets turned into an ISO
	// timestamp based on REALTIME_TIMESTAMP if available
	// (journalpump.py:762-766).
	var timestamp time.Time
	var err error
	for _, field := range *data {
		if field.Key != "REALTIME_TIMESTAMP" {
			continue
		}
		seconds, ok := field.Value.(float64)
		if ok {
			timestamp, err = TimestampFromRealtime(seconds)
			if err != nil {
				// Python: datetime.fromtimestamp raises (OverflowError /
				// OSError / ValueError) → escapes process() → the broad
				// handler.
				return nil, err
			}
		} else {
			// A non-numeric REALTIME_TIMESTAMP cannot happen through the
			// converter table (convert_realtime always yields a float);
			// fall back to now like the absent branch does.
			timestamp = h.now()
		}
		break
	}
	if timestamp.IsZero() {
		timestamp = h.now()
	}
	*data = SetField(*data, "timestamp", timestamp)

	if len(extraFieldValues) > 0 {
		*data = MergeExtraFields(*data, extraFieldValues)
	}

	// Python: `if unit_log_levels:` — a dropped entry freezes the data
	// to {} (journalpump.py:771-775), which the emptiness check below
	// turns into None; it is NOT cached.
	if unitLogLevels != nil && !unitLogLevels.Filter(fieldMap(*data)) {
		return nil, nil
	}
	if len(*data) == 0 {
		// Python's `if not data: return None` (journalpump.py:777-779)
		// runs BEFORE the field filter.
		return nil, nil
	}

	filtered := *data
	if fieldFilter != nil {
		// Python's filter_fields dict comprehension keeps the dict's
		// (construction) order; the processed slice is exactly that
		// order, so a Keep sweep reproduces the wire key order. A
		// whitelist that removes every field yields {} — serialized and
		// enqueued as "{}", advancing that sender's cursor
		// (journalpump.py:781-784 has no emptiness re-check).
		filtered = make([]Field, 0, len(*data))
		for _, field := range *data {
			if fieldFilter.Keep(field.Key) {
				filtered = append(filtered, field)
			}
		}
	}

	jsonEntry, err := Serialize(filtered)
	if err != nil {
		// Serialize only fails on byte values that are not valid UTF-8 —
		// unreachable after ProcessEntry except inside repeated-field
		// lists, where Python would raise UnicodeDecodeError identically.
		return nil, err
	}
	if len(jsonEntry) > MaxKafkaMessageSize {
		var timestampValue any
		for _, field := range filtered {
			if field.Key == "timestamp" {
				timestampValue = field.Value
				break
			}
		}
		jsonEntry, err = h.truncateLongMessage(jsonEntry, timestampValue)
		if err != nil {
			return nil, err
		}
	}

	// The truncated error record IS cached (journalpump.py:788 runs
	// after truncation).
	h.jsonObjects[ffName] = jsonEntry
	return jsonEntry, nil
}

// filterByLogLevel is the cache-hit half of _filter_by_log_level
// (journalpump.py:791-794): the unit check runs over the parsed cached
// bytes; a dropped entry returns {} — not bytes — so the caller skips
// the enqueue (here: nil).
func (h *Handler) filterByLogLevel(cached []byte, unitLogLevels *filter.UnitLogLevel) []byte {
	if unitLogLevels == nil {
		return cached
	}
	var parsed map[string]any
	decoder := json.NewDecoder(bytes.NewReader(cached))
	decoder.UseNumber()
	if err := decoder.Decode(&parsed); err != nil {
		// Python json.loads would raise here too (the cache only holds
		// our own output, so this is unreachable); fail closed as a
		// dropped entry rather than enqueue unfiltered bytes.
		return nil
	}
	if !unitLogLevels.Filter(parsed) {
		return nil
	}
	return cached
}

// truncateLongMessage ports _truncate_long_message (journalpump.py:796-818).
// The truncated prefix of the wire bytes must be valid UTF-8 for Python's
// json.dumps to embed it (default_json_serialization decodes bytes); an
// invalid prefix raises UnicodeDecodeError inside json.dumps — which
// escapes process() — so it is returned as an error AFTER the warning
// side effects, exactly where Python raises (journalpump.py:818).
func (h *Handler) truncateLongMessage(jsonEntry []byte, timestamp any) ([]byte, error) {
	errorText := fmt.Sprintf("too large message %d bytes vs maximum %d bytes", len(jsonEntry), MaxKafkaMessageSize)
	truncated := jsonEntry[:TruncatedMessagePreviewSize]
	if !h.errorReported {
		if h.cfg.Stats != nil {
			// Python pump.make_tags({"error": "too_long", "reader": name}):
			// per-call extras sanitized over the pump base tags.
			h.cfg.Stats.Increase("journal.read_error", 1, h.cfg.PumpTags.MakeTags(map[string]string{
				"error":  "too_long",
				"reader": h.cfg.ReaderName,
			}))
		}
		// Python logs the raw bytes object (rendered repr-style by the
		// logging module); Go renders a quoted Go string — a rendering
		// deviation recorded in the parity doc.
		if h.cfg.Log != nil {
			// Python log.warning (journalpump.py:810): WARNING level when
			// the logger supports it, else the Printf seam.
			if w, ok := h.cfg.Log.(warningLogger); ok {
				w.Warningf("%s: %q ...", errorText, string(truncated))
			} else {
				h.cfg.Log.Printf("%s: %q ...", errorText, string(truncated))
			}
		}
		h.errorReported = true
	}
	if !utf8.Valid(truncated) {
		return nil, fmt.Errorf("truncating oversize entry: partial preview is not valid UTF-8")
	}
	entry := []Field{
		{Key: "error", Value: errorText},
		{Key: "partial_data", Value: string(truncated)},
	}
	if timestamp != nil {
		entry = append(entry, Field{Key: "timestamp", Value: timestamp})
	}
	return Serialize(entry)
}

// checkMatch ports check_match (journalpump.py:647-652) for the pump-
// and reader-level match pairs against the processed entry:
// `entry.get(match_key) == match_value`. An unset match_key passes
// everything. A nil match_value MATCHES when the key is absent (Python's
// dict.get yields None) or explicitly null — and only then, since a
// present, non-null value is never == None. (Task 15 pinned the
// opposite; the Python probe in the Task 16 review disproved it — see
// the parity doc.)
func checkMatch(fields []Field, matchKey string, matchValue any) bool {
	if matchKey == "" {
		return true
	}
	for _, field := range fields {
		if field.Key == matchKey {
			return PythonEqual(field.Value, matchValue)
		}
	}
	// Key absent: Python's entry.get(match_key) is None.
	return matchValue == nil
}

// PythonEqual compares a converted entry value against a config-decoded
// match_value with Python's == semantics: numbers compare by value
// across int/float/bool (True == 1) with EXACT integer comparison —
// two distinct int64 values above 2^53 never collapse, and an int
// equals a float only when the float's exact value is that integer
// (Python's int.__eq__ never rounds). Strings compare by content, None
// only to None, lists elementwise, dicts key-wise. Bytes never equal
// strings (Python b"x" == "x" is False), and a structurally different
// type is unequal like Python's __eq__ default.
func PythonEqual(a, b any) bool {
	numA, okA := asPythonNumber(a)
	numB, okB := asPythonNumber(b)
	if okA && okB {
		return numA.equal(numB)
	}
	if okA != okB {
		return false // Python: a number never equals a string/None/bytes
	}
	switch typed := a.(type) {
	case nil:
		return b == nil
	case string:
		s, ok := b.(string)
		return ok && typed == s
	case []any:
		list, ok := b.([]any)
		if !ok || len(typed) != len(list) {
			return false
		}
		for i := range typed {
			if !PythonEqual(typed[i], list[i]) {
				return false
			}
		}
		return true
	case []Field:
		obj, ok := b.([]Field)
		if !ok || len(typed) != len(obj) {
			return false
		}
		for i := range typed {
			if typed[i].Key != obj[i].Key || !PythonEqual(typed[i].Value, obj[i].Value) {
				return false
			}
		}
		return true
	}
	// Entry values are never maps; a map match_value can therefore
	// never match anything but a map (equally absent), and the fallback
	// below reports that faithfully. reflect.DeepEqual is the Go
	// fallback for the remaining shapes ([]byte vs []byte — Python's
	// bytes == bytes compares by content).
	return reflect.DeepEqual(a, b)
}

// pyNumber is the numeric view of a Python == operand: ints keep their
// exact int64 value, floats their float64 (Python's bool is an int
// subclass, so booleans fold into the integer side).
type pyNumber struct {
	isInt bool
	i     int64
	f     float64
}

// equal implements Python's numeric ==: ints compare exactly; an int and
// a float are equal only when the float's exact value is that integer
// (never rounding through float64, so distinct ints above 2^53 stay
// distinct).
func (n pyNumber) equal(m pyNumber) bool {
	switch {
	case n.isInt && m.isInt:
		return n.i == m.i
	case n.isInt:
		return intEqualsFloat(n.i, m.f)
	case m.isInt:
		return intEqualsFloat(m.i, n.f)
	default:
		return n.f == m.f
	}
}

// intEqualsFloat reports i == f without precision loss: f must be finite,
// integral, in int64 range, and int64(f) must be exactly i.
func intEqualsFloat(i int64, f float64) bool {
	if math.IsNaN(f) || math.IsInf(f, 0) || f != math.Trunc(f) {
		return false
	}
	if f < float64(math.MinInt64) || f >= float64(math.MaxInt64) {
		return false
	}
	return int64(f) == i
}

// asPythonNumber renders the numeric view of a Python == operand:
// bool/int64/int/float64/json.Number.
func asPythonNumber(v any) (pyNumber, bool) {
	switch typed := v.(type) {
	case bool:
		if typed {
			return pyNumber{isInt: true, i: 1}, true
		}
		return pyNumber{isInt: true, i: 0}, true
	case int64:
		return pyNumber{isInt: true, i: typed}, true
	case int:
		return pyNumber{isInt: true, i: int64(typed)}, true
	case float64:
		return pyNumber{f: typed}, true
	case json.Number:
		if i, err := typed.Int64(); err == nil {
			return pyNumber{isInt: true, i: i}, true
		}
		if f, err := typed.Float64(); err == nil {
			return pyNumber{f: f}, true
		}
	}
	return pyNumber{}, false
}

// fieldMap renders the fields as a plain map — the view Python's dict
// operations (search field lookup, unit-log-level checks, secret filter
// access) take. Duplicate keys follow dict semantics (last wins).
func fieldMap(fields []Field) map[string]any {
	m := make(map[string]any, len(fields))
	for _, field := range fields {
		m[field.Key] = field.Value
	}
	return m
}

// DecodeOrderedFields parses one JSON value into ordered fields,
// preserving the document key order — the decoder for
// extra_field_values subtrees (the config's map decode discards order,
// but the order determines the serialized key order on the wire, and
// Python's data.update(extra_field_values) appends new keys in dict
// order). Numbers follow config-decode semantics: integer literals
// become int64 (out-of-int64 magnitudes stay json.Number), everything
// else float64. Nested objects decode recursively as []Field, arrays as
// []any.
func DecodeOrderedFields(raw []byte) ([]Field, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	value, err := decodeOrderedValue(decoder)
	if err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, fmt.Errorf("extra data after JSON value")
	}
	fields, ok := value.([]Field)
	if !ok {
		return nil, fmt.Errorf("extra_field_values: must be a JSON object")
	}
	return fields, nil
}

func decodeOrderedValue(decoder *json.Decoder) (any, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	return decodeOrderedRest(decoder, token)
}

// decodeOrderedRest continues decoding a value whose first token was
// consumed (scalars are complete; containers run to their delimiter).
func decodeOrderedRest(decoder *json.Decoder, token json.Token) (any, error) {
	switch typed := token.(type) {
	case json.Delim:
		switch typed {
		case '{':
			var fields []Field
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return nil, err
				}
				key, ok := keyToken.(string)
				if !ok {
					return nil, fmt.Errorf("object key must be a string")
				}
				value, err := decodeOrderedValue(decoder)
				if err != nil {
					return nil, err
				}
				fields = append(fields, Field{Key: key, Value: value})
			}
			if _, err := decoder.Token(); err != nil { // closing }
				return nil, err
			}
			return fields, nil
		case '[':
			var items []any
			for decoder.More() {
				value, err := decodeOrderedValue(decoder)
				if err != nil {
					return nil, err
				}
				items = append(items, value)
			}
			if _, err := decoder.Token(); err != nil { // closing ]
				return nil, err
			}
			return items, nil
		default:
			return nil, fmt.Errorf("unexpected delimiter %v", typed)
		}
	case string:
		return typed, nil
	case bool:
		return typed, nil
	case nil:
		return nil, nil
	case json.Number:
		return normalizeOrderedNumber(typed), nil
	default:
		return nil, fmt.Errorf("unexpected token %v", typed)
	}
}

// normalizeOrderedNumber mirrors config-decode number semantics
// (integer literal → int64, else float64).
func normalizeOrderedNumber(n json.Number) any {
	s := n.String()
	if !strings.ContainsAny(s, ".eE") {
		if i, err := n.Int64(); err == nil {
			return i
		}
		return n // out-of-int64 integer literal: kept verbatim on the wire
	}
	if f, err := n.Float64(); err == nil {
		return f
	}
	return n
}

// now returns the handler's fallback timestamp: Python
// datetime.now(UTC).replace(tzinfo=None), microsecond resolution.
func (h *Handler) now() time.Time {
	if h.cfg.Now != nil {
		return h.cfg.Now().Truncate(time.Microsecond)
	}
	return time.Now().Truncate(time.Microsecond)
}
