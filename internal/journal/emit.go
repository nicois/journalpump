// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package journal

import (
	"bytes"
	"encoding/json"
	"errors"
	"strconv"
	"unicode/utf8"

	"github.com/Aiven-Open/journalpump/internal/filter"
)

// ExtraFragment is one sender extra_field_values entry with its value
// already rendered as JSON (the emission path appends it verbatim). It is
// built once per sender at construction, not per entry.
type ExtraFragment struct {
	// Key is the extra's key exactly as it appears in the processed entry
	// (no leading-underscore stripping is applied to extras).
	Key string
	// JSON is the value's wire bytes, e.g. `123`, `"s"`, `{"a": 1}`.
	JSON []byte
}

// EmitOptions carries the per-sender emission inputs.
type EmitOptions struct {
	// Filter, when non-nil, drops keys the field filter rejects (the same
	// predicate the old Keep applied, on the stripped key).
	Filter *filter.FieldFilter
	// Extras are the sender's extra_field_values in document order,
	// pre-rendered as JSON fragments. An extra whose key matches a raw or
	// synthesised key overrides it in place; the rest are appended in order.
	Extras []ExtraFragment
	// MessageOverride replaces the MESSAGE field's value with the given
	// raw bytes (a secret-filter redaction, rendered as a JSON string).
	// Extras still win over it, matching Python's data.update running
	// after the redaction. When nil the MESSAGE field is emitted as-is.
	MessageOverride []byte
	// MessageOverrideSet reports that MessageOverride is present even when
	// it is an EMPTY redaction (Python `data["MESSAGE"] = ""`), which
	// len(MessageOverride)==0 cannot distinguish from "no override".
	MessageOverrideSet bool
	// TimestampISO, when non-empty, is the synthesised "timestamp" value
	// (Python's data["timestamp"] after the per-miss reassignment and
	// extra overrides). It is used only when no extra overrides the
	// timestamp key; an empty value falls back to computing it from the
	// raw realtime (the standalone EmitJSON callers' behaviour).
	TimestampISO string
	// IndexDate, when non-nil, receives the Elasticsearch index date
	// (timestamp[:10]) of the emitted payload. It is filled only when the
	// payload carries the SYNTHESISED ISO timestamp (the timestamp key is
	// kept by the filter and no extra overrides it); it is left zero when
	// the timestamp is filtered out, overridden by an extra, or shorter
	// than 10 bytes — the Elasticsearch consumer then falls back to
	// parsing the payload (ElasticsearchSender.index_date).
	IndexDate *[10]byte
}

// NewExtraFragments renders a sender's extra_field_values (in document
// order) as pre-escaped JSON fragments for the raw emission path. It is
// called once at sender construction, not per entry.
func NewExtraFragments(fields []Field) []ExtraFragment {
	if len(fields) == 0 {
		return nil
	}
	out := make([]ExtraFragment, 0, len(fields))
	for _, field := range fields {
		var buf bytes.Buffer
		if err := appendPythonJSON(&buf, field.Key, field.Value); err != nil {
			continue
		}
		out = append(out, ExtraFragment{Key: field.Key, JSON: buf.Bytes()})
	}
	return out
}

// EmitJSON appends one JSON object for the raw entry to dst, using the
// byte views of b directly — no map, no []Field, no per-value string. The
// output is SEMANTICALLY equivalent to Serialize over the equivalent
// converted entry (same keys and values; key order and, where free, bytes
// too).
func EmitJSON(dst []byte, b *Batch, e *RawEntry, opts *EmitOptions) ([]byte, error) {
	usedExtras := make([]bool, len(opts.Extras))

	dst = append(dst, '{')
	first := true
	emitKey := func(key []byte) {
		if !first {
			dst = append(dst, ", "...)
		}
		first = false
		dst = appendJSONString(dst, key)
		dst = append(dst, ": "...)
	}
	emitExtra := func(idx int) {
		emitKey([]byte(opts.Extras[idx].Key))
		dst = append(dst, opts.Extras[idx].JSON...)
	}

	// Pass 1: raw fields, first occurrences only.
	for i := range e.Fields {
		field := &e.Fields[i]
		if field.Duplicate {
			continue
		}
		key := stripUnderscore(b.Key(field))
		if isSynthesisedKey(key) {
			// A literal REALTIME_TIMESTAMP / SEQNUM / timestamp field is
			// replaced by the synthesised value (SetField overwrites it);
			// the synthesised pass below emits it.
			continue
		}
		if !keeps(opts.Filter, key) {
			markExtras(opts.Extras, usedExtras, key)
			continue
		}
		if idx, ok := lastExtraIndex(opts.Extras, key); ok {
			markExtras(opts.Extras, usedExtras, key)
			emitExtra(idx)
			continue
		}
		emitKey(key)
		if opts.MessageOverrideSet && bytes.Equal(key, messageKey) {
			dst = appendJSONString(dst, opts.MessageOverride)
			continue
		}
		var err error
		dst, err = appendFieldValue(dst, b, e, i)
		if err != nil {
			return nil, err
		}
	}

	// Pass 2: synthesised keys, in the old append order.
	emitSynth := func(key string, render func() ([]byte, error)) error {
		k := []byte(key)
		if idx, ok := lastExtraIndex(opts.Extras, k); ok {
			markExtras(opts.Extras, usedExtras, k)
			if keeps(opts.Filter, k) {
				emitExtra(idx)
			}
			return nil
		}
		if !keeps(opts.Filter, k) {
			return nil
		}
		value, err := render()
		if err != nil {
			return err
		}
		emitKey(k)
		dst = append(dst, value...)
		return nil
	}
	if err := emitSynth("REALTIME_TIMESTAMP", func() ([]byte, error) {
		return []byte(PythonFloatRepr(float64(e.Realtime) / 1e6)), nil
	}); err != nil {
		return nil, err
	}
	if seqnum, ok := ParseSeqnumFromCursor(string(b.Cursor(e))); ok {
		rendered, err := renderNumber(seqnum)
		if err != nil {
			return nil, err
		}
		if err := emitSynth("SEQNUM", func() ([]byte, error) { return rendered, nil }); err != nil {
			return nil, err
		}
	}
	if err := emitSynth("timestamp", func() ([]byte, error) {
		iso := opts.TimestampISO
		if iso == "" {
			timestamp, err := TimestampFromRealtime(float64(e.Realtime) / 1e6)
			if err != nil {
				return nil, err
			}
			iso = pythonISOFormat(timestamp)
		}
		// render runs only when the synthesised value is actually emitted,
		// so filling the sidecar here is exactly the "payload carries the
		// synthesised timestamp" condition.
		if opts.IndexDate != nil && len(iso) >= 10 {
			copy(opts.IndexDate[:], iso[:10])
		}
		return appendJSONString(nil, []byte(iso)), nil
	}); err != nil {
		return nil, err
	}

	// Pass 3: extras not yet placed, in document order. A key's position
	// is its FIRST insertion (Python dict update keeps it), but its value
	// is the LAST assignment, so the first occurrence emits the last
	// occurrence's JSON and later duplicates are skipped.
	for i := range opts.Extras {
		if usedExtras[i] {
			continue
		}
		k := []byte(opts.Extras[i].Key)
		if firstIdx, ok := firstExtraIndex(opts.Extras, k); ok && firstIdx != i {
			continue
		}
		if !keeps(opts.Filter, k) {
			continue
		}
		if idx, ok := lastExtraIndex(opts.Extras, k); ok {
			emitExtra(idx)
		}
	}

	dst = append(dst, '}')
	return dst, nil
}

// keeps reports whether key survives the (optional) field filter.
func keeps(f *filter.FieldFilter, key []byte) bool {
	return f == nil || f.KeepBytes(key)
}

// firstExtraIndex finds the FIRST extras entry with the given key (its
// insertion position under Python dict.update).
func firstExtraIndex(extras []ExtraFragment, key []byte) (int, bool) {
	for i := range extras {
		if len(extras[i].Key) == len(key) && string(key) == extras[i].Key {
			return i, true
		}
	}
	return 0, false
}

// lastExtraIndex finds the LAST extras entry with the given key (the
// winning value under Python's successive data.update).
func lastExtraIndex(extras []ExtraFragment, key []byte) (int, bool) {
	for i := len(extras) - 1; i >= 0; i-- {
		if len(extras[i].Key) == len(key) && string(key) == extras[i].Key {
			return i, true
		}
	}
	return 0, false
}

// markExtras marks any extras sharing a filtered-out key as used (the key
// is filtered out entirely, so the extra must not reappear in pass 3).
func markExtras(extras []ExtraFragment, used []bool, key []byte) {
	for i := range extras {
		if len(extras[i].Key) == len(key) && string(key) == extras[i].Key {
			used[i] = true
		}
	}
}

// renderNumber renders a seqnum result (int64 or json.Number) as JSON.
func renderNumber(value any) ([]byte, error) {
	switch v := value.(type) {
	case int64:
		return strconv.AppendInt(nil, v, 10), nil
	case json.Number:
		return []byte(v.String()), nil
	default:
		return nil, errors.New("unsupported seqnum value")
	}
}

// appendFieldValue renders one raw field's value, honouring the converter
// class of its raw key, and appends it to dst. A first occurrence followed
// by same-key duplicates becomes a JSON array.
func appendFieldValue(dst []byte, b *Batch, e *RawEntry, index int) ([]byte, error) {
	field := &e.Fields[index]
	rawKey := b.Key(field)
	dups := duplicateIndexes(b, e, index)
	if len(dups) == 0 {
		return appendSingleValue(dst, rawKey, b.ValueBytes(field))
	}
	dst = append(dst, '[')
	var err error
	dst, err = appendSingleValue(dst, rawKey, b.ValueBytes(field))
	if err != nil {
		return nil, err
	}
	for _, dup := range dups {
		dst = append(dst, ", "...)
		dst, err = appendSingleValue(dst, rawKey, b.ValueBytes(&e.Fields[dup]))
		if err != nil {
			return nil, err
		}
	}
	return append(dst, ']'), nil
}

// duplicateIndexes returns the indexes of the fields after index whose
// marked-duplicate key equals field index's key.
func duplicateIndexes(b *Batch, e *RawEntry, index int) []int {
	key := b.Key(&e.Fields[index])
	var out []int
	for j := index + 1; j < len(e.Fields); j++ {
		if !e.Fields[j].Duplicate {
			continue
		}
		if bytes.Equal(b.Key(&e.Fields[j]), key) {
			out = append(out, j)
		}
	}
	return out
}

// appendSingleValue renders one raw value for the converter class of
// rawKey, appending its JSON to dst.
func appendSingleValue(dst []byte, rawKey, value []byte) ([]byte, error) {
	switch classOf(rawKey) {
	case classInt:
		if isCanonicalDecimal(value) {
			return append(dst, value...), nil
		}
		if n, ok := pythonIntString(string(value), 10); ok {
			return appendNumberValue(dst, n)
		}
		return appendStringValue(dst, value), nil
	case classRealtime:
		if n, ok := pythonIntString(string(value), 10); ok {
			return append(dst, PythonFloatRepr(realtimeFloat(n))...), nil
		}
		return appendStringValue(dst, value), nil
	case classMon:
		return append(dst, "null"...), nil
	case classUUID:
		if decoded, valid := decodeUTF8(value); valid {
			if canonical, ok := canonicalUUID(decoded); ok {
				return appendJSONString(dst, []byte(canonical)), nil
			}
		}
		return appendStringValue(dst, value), nil
	default:
		return appendStringValue(dst, value), nil
	}
}

// appendNumberValue renders a parsed int (int64 or json.Number) as JSON.
func appendNumberValue(dst []byte, value any) ([]byte, error) {
	switch v := value.(type) {
	case int64:
		return strconv.AppendInt(dst, v, 10), nil
	case json.Number:
		return append(dst, v.String()...), nil
	default:
		return nil, errors.New("unsupported numeric value")
	}
}

// appendStringValue renders a default-class value: a JSON string for valid
// UTF-8, the Python repr() string for undecodable bytes (D8 — raw invalid
// bytes never reach the wire).
func appendStringValue(dst []byte, value []byte) []byte {
	if utf8.Valid(value) {
		return appendJSONString(dst, value)
	}
	return appendJSONString(dst, []byte(pythonReprBytes(value)))
}

// converterClass is the converter class of a raw journal key.
type converterClass int

const (
	classDefault converterClass = iota
	classInt
	classRealtime
	classMon
	classUUID
)

// converterTable maps the RAW journal key (with underscores — convert_entry
// looks up the raw key before the underscore strip) to its class. A linear
// scan avoids a map lookup and the string allocation it would force.
var converterTable = []struct {
	key   []byte
	class converterClass
}{
	{[]byte("CODE_LINE"), classInt},
	{[]byte("COREDUMP_TIMESTAMP"), classRealtime},
	{[]byte("MESSAGE_ID"), classUUID},
	{[]byte("PRIORITY"), classInt},
	{[]byte("_AUDIT_ID"), classInt},
	{[]byte("_AUDIT_LOGINUID"), classInt},
	{[]byte("_AUDIT_SESSION"), classInt},
	{[]byte("_AUDIT_TYPE"), classInt},
	{[]byte("_BOOT_ID"), classUUID},
	{[]byte("_GID"), classInt},
	{[]byte("_MACHINE_ID"), classUUID},
	{[]byte("_PID"), classInt},
	{[]byte("_SOURCE_MONOTONIC_TIMESTAMP"), classMon},
	{[]byte("_SOURCE_REALTIME_TIMESTAMP"), classRealtime},
	{[]byte("__MONOTONIC_TIMESTAMP"), classMon},
	{[]byte("__REALTIME_TIMESTAMP"), classRealtime},
}

func classOf(rawKey []byte) converterClass {
	for i := range converterTable {
		if bytes.Equal(converterTable[i].key, rawKey) {
			return converterTable[i].class
		}
	}
	return classDefault
}

// synthesisedKey reports whether a stripped key is one the emission pass
// produces itself.
func isSynthesisedKey(key []byte) bool {
	return bytes.Equal(key, realtimeTimestampKey) ||
		bytes.Equal(key, seqnumKey) ||
		bytes.Equal(key, timestampKey)
}

var (
	realtimeTimestampKey = []byte("REALTIME_TIMESTAMP")
	seqnumKey            = []byte("SEQNUM")
	timestampKey         = []byte("timestamp")
	messageKey           = []byte("MESSAGE")
)

// stripUnderscore returns key without its leading underscores (Python
// key.lstrip("_")).
func stripUnderscore(key []byte) []byte {
	i := 0
	for i < len(key) && key[i] == '_' {
		i++
	}
	return key[i:]
}

// isCanonicalDecimal reports whether v is "0" or an optional '-' followed
// by a nonzero leading digit and digits — the subset of pythonIntString's
// accepted forms whose bytes are already valid JSON integers.
func isCanonicalDecimal(v []byte) bool {
	if len(v) == 0 {
		return false
	}
	i := 0
	if v[0] == '-' {
		i = 1
		if i == len(v) {
			return false
		}
	}
	if v[i] == '0' {
		return i == len(v)-1
	}
	if v[i] < '1' || v[i] > '9' {
		return false
	}
	for ; i < len(v); i++ {
		if v[i] < '0' || v[i] > '9' {
			return false
		}
	}
	return true
}

// appendJSONString appends a JSON string literal for v, escaping exactly
// like Python's json.dumps(ensure_ascii=True): the two JSON specials, the
// short control escapes, \u00xx for other controls/DEL, \uxxxx for
// non-ASCII BMP characters and surrogate pairs for astral ones. There is
// deliberately no HTML escaping.
func appendJSONString(dst, v []byte) []byte {
	dst = append(dst, '"')
	start := 0
	for i := 0; i < len(v); {
		c := v[i]
		if c >= 0x20 && c < 0x7f && c != '"' && c != '\\' {
			i++
			continue
		}
		dst = append(dst, v[start:i]...)
		switch {
		case c == '"' || c == '\\':
			dst = append(dst, '\\', c)
			i++
		case c == '\b':
			dst = append(dst, `\b`...)
			i++
		case c == '\f':
			dst = append(dst, `\f`...)
			i++
		case c == '\n':
			dst = append(dst, `\n`...)
			i++
		case c == '\r':
			dst = append(dst, `\r`...)
			i++
		case c == '\t':
			dst = append(dst, `\t`...)
			i++
		case c < 0x20 || c == 0x7f:
			dst = append(dst, `\u00`...)
			dst = appendHexByte(dst, c)
			i++
		default:
			r, size := utf8.DecodeRune(v[i:])
			if r == utf8.RuneError && size <= 1 {
				// Invalid byte (only reachable for corrupt keys — whole
				// values go through the repr fallback): emit \u00XX so the
				// output stays valid JSON.
				dst = append(dst, `\u00`...)
				dst = appendHexByte(dst, c)
				i++
			} else {
				dst = appendRuneEscape(dst, r)
				i += size
			}
		}
		start = i
	}
	dst = append(dst, v[start:]...)
	return append(dst, '"')
}

func appendHexByte(dst []byte, c byte) []byte {
	const digits = "0123456789abcdef"
	return append(dst, digits[c>>4], digits[c&0xf])
}

// appendRuneEscape appends the \uXXXX (or surrogate pair) escape for r.
func appendRuneEscape(dst []byte, r rune) []byte {
	if r > 0xFFFF {
		r -= 0x10000
		dst = append(dst, `\u`...)
		dst = appendHex16(dst, uint16(0xd800+((r>>10)&0x3ff)))
		dst = append(dst, `\u`...)
		return appendHex16(dst, uint16(0xdc00+(r&0x3ff)))
	}
	dst = append(dst, `\u`...)
	return appendHex16(dst, uint16(r))
}

func appendHex16(dst []byte, v uint16) []byte {
	const digits = "0123456789abcdef"
	return append(dst, digits[(v>>12)&0xf], digits[(v>>8)&0xf], digits[(v>>4)&0xf], digits[v&0xf])
}
