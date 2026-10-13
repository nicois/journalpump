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
	var usedExtras []bool
	if n := len(opts.Extras); n > 0 {
		usedExtras = make([]bool, n)
	}

	dst = append(dst, '{')
	first := true

	// Pass 1: raw fields. Distinct exact keys that strip to the SAME name
	// are a dict collision (ProcessEntry keeps the last value), so only
	// the LAST field of each stripped key is emitted; a repeated IDENTICAL
	// key (Duplicate) is folded into its first occurrence by appendFieldValue.
	for i := range e.Fields {
		field := &e.Fields[i]
		if field.Duplicate {
			continue
		}
		if lastStrippedIndex(b, e, i) != i {
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
			dst, first = appendExtra(dst, first, opts, idx)
			continue
		}
		dst, first = appendKey(dst, first, key)
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

	// Pass 2: synthesised keys, in the old append order. Each key is handled
	// explicitly so the emit path allocates no per-entry func value (the old
	// emitSynth/render closures captured dst and e and escaped to the heap).
	// REALTIME_TIMESTAMP: a same-key extra wins in place; a filtered key is
	// dropped; otherwise the value is rendered and appended.
	if idx, ok := lastExtraIndex(opts.Extras, realtimeTimestampKey); ok {
		markExtras(opts.Extras, usedExtras, realtimeTimestampKey)
		if keeps(opts.Filter, realtimeTimestampKey) {
			dst, first = appendExtra(dst, first, opts, idx)
		}
	} else if keeps(opts.Filter, realtimeTimestampKey) {
		dst, first = appendKey(dst, first, realtimeTimestampKey)
		dst = append(dst, PythonFloatRepr(float64(e.Realtime)/1e6)...)
	}
	// SEQNUM.
	if seqnum, ok := ParseSeqnumFromCursor(string(b.Cursor(e))); ok {
		if idx, ok := lastExtraIndex(opts.Extras, seqnumKey); ok {
			markExtras(opts.Extras, usedExtras, seqnumKey)
			if keeps(opts.Filter, seqnumKey) {
				dst, first = appendExtra(dst, first, opts, idx)
			}
		} else if keeps(opts.Filter, seqnumKey) {
			dst, first = appendKey(dst, first, seqnumKey)
			var err error
			dst, err = appendNumberValue(dst, seqnum)
			if err != nil {
				return nil, err
			}
		}
	}
	// timestamp.
	if idx, ok := lastExtraIndex(opts.Extras, timestampKey); ok {
		markExtras(opts.Extras, usedExtras, timestampKey)
		if keeps(opts.Filter, timestampKey) {
			dst, first = appendExtra(dst, first, opts, idx)
		}
	} else if keeps(opts.Filter, timestampKey) {
		iso := opts.TimestampISO
		if iso == "" {
			timestamp, err := TimestampFromRealtime(float64(e.Realtime) / 1e6)
			if err != nil {
				return nil, err
			}
			iso = pythonISOFormat(timestamp)
		}
		// The sidecar is filled only when the synthesised value is actually
		// emitted — the "payload carries the synthesised timestamp" condition.
		if opts.IndexDate != nil && len(iso) >= 10 {
			copy(opts.IndexDate[:], iso[:10])
		}
		dst, first = appendKey(dst, first, timestampKey)
		dst = appendJSONString(dst, []byte(iso))
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
			dst, first = appendExtra(dst, first, opts, idx)
		}
	}

	dst = append(dst, '}')
	return dst, nil
}

// appendKey writes the ", " separator (unless this is the first key), the
// JSON-quoted key and the ": " separator, returning the updated dst and
// first flag. It replaces the old emitKey closure, whose capture of dst
// forced a per-entry heap allocation.
func appendKey(dst []byte, first bool, key []byte) ([]byte, bool) {
	if !first {
		dst = append(dst, ", "...)
	}
	dst = appendJSONString(dst, key)
	dst = append(dst, ": "...)
	return dst, false
}

// appendExtra writes one extras fragment (key + pre-rendered JSON value).
func appendExtra(dst []byte, first bool, opts *EmitOptions, idx int) ([]byte, bool) {
	dst, first = appendKey(dst, first, []byte(opts.Extras[idx].Key))
	return append(dst, opts.Extras[idx].JSON...), first
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

// appendCanonicalUUID appends the canonical, double-quoted UUID form of the
// raw value v (Python uuid.UUID: markers/braces/hyphens removed, exactly 32
// hex digits, re-emitted lowercase in 8-4-4-4-12 groups), or reports false
// with dst unchanged when v is not a UUID. It is the allocation-free
// rewrite of decodeUTF8+canonicalUUID (which built several intermediate
// strings per UUID); the marker/brace/hyphen and hex rules are identical,
// including the "urn:"/"uuid:"-anywhere preprocessing, which falls back to
// the shared canonicalUUID for the rare marker-bearing value.
func appendCanonicalUUID(dst, v []byte) ([]byte, bool) {
	if bytes.Contains(v, urnMarker) || bytes.Contains(v, uuidMarker) {
		canonical, ok := canonicalUUID(string(v))
		if !ok {
			return dst, false
		}
		dst = append(dst, '"')
		dst = append(dst, canonical...)
		return append(dst, '"'), true
	}
	lo, hi := 0, len(v)
	for lo < hi && (v[lo] == '{' || v[lo] == '}') {
		lo++
	}
	for hi > lo && (v[hi-1] == '{' || v[hi-1] == '}') {
		hi--
	}
	var buf [32]byte
	n := 0
	for i := lo; i < hi; i++ {
		c := v[i]
		if c == '-' {
			continue
		}
		if n == 32 || !isHexDigit(c) {
			return dst, false
		}
		buf[n] = c
		n++
	}
	if n != 32 {
		return dst, false
	}
	dst = append(dst, '"')
	for i := 0; i < 32; i++ {
		switch i {
		case 8, 12, 16, 20:
			dst = append(dst, '-')
		}
		c := buf[i]
		if c >= 'A' && c <= 'F' {
			c += 'a' - 'A'
		}
		dst = append(dst, c)
	}
	return append(dst, '"'), true
}

func isHexDigit(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

var (
	urnMarker  = []byte("urn:")
	uuidMarker = []byte("uuid:")
)

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
	// Python's reader merged repeated identical keys into a LIST before
	// convert_entry, which applies the converter to the LIST as a whole:
	// only convert_mon accepts one (-> None). Every other class leaves the
	// list unchanged, so the serializer decodes each element as a string
	// (an int-class repeat is a list of STRINGS, not numbers) and errors on
	// invalid UTF-8 (ProcessEntry never repr()s list elements).
	if classOf(rawKey) == classMon {
		return append(dst, "null"...), nil
	}
	dst = append(dst, '[')
	var err error
	dst, err = appendListElement(dst, b.ValueBytes(field))
	if err != nil {
		return nil, err
	}
	for _, dup := range dups {
		dst = append(dst, ", "...)
		dst, err = appendListElement(dst, b.ValueBytes(&e.Fields[dup]))
		if err != nil {
			return nil, err
		}
	}
	return append(dst, ']'), nil
}

// appendListElement renders one element of a repeated field like
// Serialize renders a []byte list item: JSON-escaped string, or an error
// for invalid UTF-8.
func appendListElement(dst, value []byte) ([]byte, error) {
	if !utf8.Valid(value) {
		return nil, errors.New("undecodable byte value in repeated field")
	}
	return appendJSONString(dst, value), nil
}

// lastStrippedIndex returns the index of the LAST non-duplicate field whose
// stripped key equals field index's stripped key (a dict collision;
// ProcessEntry keeps the last value, so only this index is emitted).
func lastStrippedIndex(b *Batch, e *RawEntry, index int) int {
	key := stripUnderscore(b.Key(&e.Fields[index]))
	last := index
	for j := index + 1; j < len(e.Fields); j++ {
		if e.Fields[j].Duplicate {
			continue
		}
		if bytes.Equal(stripUnderscore(b.Key(&e.Fields[j])), key) {
			last = j
		}
	}
	return last
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
		if out, ok := appendCanonicalUUID(dst, value); ok {
			return out, nil
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
