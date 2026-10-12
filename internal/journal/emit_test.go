// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package journal

import (
	"bytes"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/Aiven-Open/journalpump/internal/config"
	"github.com/Aiven-Open/journalpump/internal/filter"
)

// buildBatch encodes fields exactly like jp_read_batch and parses them, so
// the emission tests exercise the real raw representation.
func buildBatch(t *testing.T, realtime uint64, cursor string, fields ...string) *Batch {
	t.Helper()
	buf := make([]byte, 0, 128)
	put := func(v uint64, n int) {
		for i := 0; i < n; i++ {
			buf = append(buf, byte(v>>(8*i)))
		}
	}
	put(realtime, 8)
	put(uint64(len(cursor)), 2)
	buf = append(buf, cursor...)
	put(uint64(len(fields)), 4)
	for _, field := range fields {
		put(uint64(len(field)), 4)
		buf = append(buf, field...)
	}
	b := &Batch{Buf: buf}
	if _, err := ParseBatch(b, len(buf), 1); err != nil {
		t.Fatalf("ParseBatch: %v", err)
	}
	return b
}

// rawToEntry rebuilds the converted entry the old chain would process.
func rawToEntry(t *testing.T, b *Batch, e *RawEntry) Entry {
	t.Helper()
	var fields []Field
	for i := range e.Fields {
		field := &e.Fields[i]
		key := string(b.Key(field))
		value := append([]byte(nil), b.ValueBytes(field)...)
		if field.Duplicate {
			for j := range fields {
				if fields[j].Key != key {
					continue
				}
				if existing, ok := fields[j].Value.([]any); ok {
					fields[j].Value = append(append([]any{}, existing...), value)
				} else {
					fields[j].Value = []any{fields[j].Value, value}
				}
				break
			}
			continue
		}
		fields = append(fields, Field{Key: key, Value: value})
	}
	fields = SetField(fields, "__REALTIME_TIMESTAMP", int64(e.Realtime))
	cursor := string(b.Cursor(e))
	if seqnum, ok := ParseSeqnumFromCursor(cursor); ok {
		fields = SetField(fields, "__SEQNUM", seqnum)
	}
	return Entry{Cursor: cursor, Fields: ConvertEntry(fields)}
}

// oraclePayload runs the OLD pipeline (_get_or_generate_json minus the
// cache) over the equivalent converted entry.
func oraclePayload(t *testing.T, b *Batch, e *RawEntry, ff *filter.FieldFilter, extras []Field) []byte {
	t.Helper()
	processed := ProcessEntry(rawToEntry(t, b, e).Fields)
	var timestamp time.Time
	for _, field := range processed {
		if field.Key == "REALTIME_TIMESTAMP" {
			if seconds, ok := field.Value.(float64); ok {
				var err error
				timestamp, err = TimestampFromRealtime(seconds)
				if err != nil {
					t.Fatalf("TimestampFromRealtime: %v", err)
				}
			}
			break
		}
	}
	if timestamp.IsZero() {
		timestamp = time.Unix(0, 0).UTC()
	}
	processed = SetField(processed, "timestamp", timestamp)
	processed = MergeExtraFields(processed, extras)
	if ff != nil {
		filtered := make([]Field, 0, len(processed))
		for _, field := range processed {
			if ff.Keep(field.Key) {
				filtered = append(filtered, field)
			}
		}
		processed = filtered
	}
	payload, err := Serialize(processed)
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}
	return payload
}

// extraFragments renders []Field extras into the pre-escaped fragments.
func extraFragments(t *testing.T, extras []Field) []ExtraFragment {
	t.Helper()
	out := make([]ExtraFragment, 0, len(extras))
	for _, extra := range extras {
		var buf bytes.Buffer
		if err := appendPythonJSON(&buf, extra.Key, extra.Value); err != nil {
			t.Fatalf("appendPythonJSON: %v", err)
		}
		out = append(out, ExtraFragment{Key: extra.Key, JSON: buf.Bytes()})
	}
	return out
}

// semanticEqual reports whether two JSON payloads carry the same keys and
// values (numbers compared as float64, or as decimal strings beyond 2^53;
// object key order is irrelevant).
func semanticEqual(t *testing.T, a, b []byte) bool {
	t.Helper()
	var va, vb any
	if err := json.Unmarshal(a, &va); err != nil {
		t.Fatalf("payload a is not JSON: %v (%s)", err, a)
	}
	if err := json.Unmarshal(b, &vb); err != nil {
		t.Fatalf("payload b is not JSON: %v (%s)", err, b)
	}
	return deepEqualJSON(va, vb)
}

func deepEqualJSON(a, b any) bool {
	switch av := a.(type) {
	case map[string]any:
		bv, ok := b.(map[string]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for key, value := range av {
			other, present := bv[key]
			if !present || !deepEqualJSON(value, other) {
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
			if !deepEqualJSON(av[i], bv[i]) {
				return false
			}
		}
		return true
	case float64:
		bv, ok := b.(float64)
		if !ok {
			return false
		}
		if av > 1<<53 || bv > 1<<53 {
			return strconv.FormatFloat(av, 'g', -1, 64) == strconv.FormatFloat(bv, 'g', -1, 64)
		}
		return av == bv
	default:
		return a == b
	}
}

func whitelistFilter(t *testing.T, fields ...string) *filter.FieldFilter {
	t.Helper()
	return filter.NewFieldFilter("wl", config.FieldFilterConfig{Whitelist: true, Fields: fields})
}

func blacklistFilter(t *testing.T, fields ...string) *filter.FieldFilter {
	t.Helper()
	return filter.NewFieldFilter("bl", config.FieldFilterConfig{Whitelist: false, Fields: fields})
}

// TestEmitJSONMatchesSerializeSemantically is the equivalence oracle: for
// each case the raw emitter and the old pipeline must agree on keys and
// values.
func TestEmitJSONMatchesSerializeSemantically(t *testing.T) {
	cursor := "s=x;i=1a2b;b=1;m=2;t=3;x=4"
	cases := []struct {
		name     string
		fields   []string
		filter   *filter.FieldFilter
		extras   []Field
		realtime uint64
	}{
		{name: "basic", fields: []string{"MESSAGE=hello", "PRIORITY=6", "_PID=42"}, realtime: 1700000000500000},
		{name: "int whitespace", fields: []string{"PRIORITY= 3 ", "CODE_LINE=0010"}, realtime: 1},
		{name: "int invalid", fields: []string{"PRIORITY=abc", "CODE_LINE="}, realtime: 1},
		{name: "int big", fields: []string{"_PID=99999999999999999999"}, realtime: 1},
		{name: "realtime field", fields: []string{"_SOURCE_REALTIME_TIMESTAMP=1700000000500000"}, realtime: 1700000000500000},
		{name: "monotonic null", fields: []string{"__MONOTONIC_TIMESTAMP=123"}, realtime: 1},
		{name: "uuid", fields: []string{"MESSAGE_ID=0123456789abcdef0123456789abcdef"}, realtime: 1},
		{name: "uuid invalid", fields: []string{"MESSAGE_ID=not-a-uuid"}, realtime: 1},
		{name: "non ascii", fields: []string{"MESSAGE=héllo ✓ wörld"}, realtime: 1},
		{name: "invalid utf8 repr", fields: []string{"MESSAGE=bad\xffbyte"}, realtime: 1},
		{name: "control chars", fields: []string{"MESSAGE=a\tb\nc\\d\"e"}, realtime: 1},
		{name: "duplicates", fields: []string{"MESSAGE=one", "MESSAGE=two", "MESSAGE=three"}, realtime: 1},
		{name: "whitelist", fields: []string{"MESSAGE=x", "_PID=7", "PRIORITY=5"}, filter: whitelistFilter(t, "MESSAGE", "PID"), realtime: 1},
		{name: "blacklist", fields: []string{"MESSAGE=x", "_PID=7", "PRIORITY=5"}, filter: blacklistFilter(t, "PRIORITY"), realtime: 1},
		{name: "extras new", fields: []string{"MESSAGE=x"}, extras: []Field{{Key: "env", Value: "prod"}, {Key: "n", Value: int64(3)}}, realtime: 1},
		{name: "extras override", fields: []string{"MESSAGE=x", "PRIORITY=6"}, extras: []Field{{Key: "PRIORITY", Value: int64(1)}}, realtime: 1},
		{name: "extras override timestamp", fields: []string{"MESSAGE=x"}, extras: []Field{{Key: "timestamp", Value: "2000-01-01T00:00:00"}}, realtime: 1},
		{name: "extras float bool object", fields: []string{"MESSAGE=x"}, extras: []Field{
			{Key: "f", Value: 1.5}, {Key: "b", Value: true}, {Key: "o", Value: []Field{{Key: "k", Value: "v"}}},
		}, realtime: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := buildBatch(t, tc.realtime, cursor, tc.fields...)
			entry := &b.Entries[0]
			opts := &EmitOptions{Filter: tc.filter, Extras: extraFragments(t, tc.extras)}
			got, err := EmitJSON(nil, b, entry, opts)
			if err != nil {
				t.Fatalf("EmitJSON: %v", err)
			}
			want := oraclePayload(t, b, entry, tc.filter, tc.extras)
			if !semanticEqual(t, got, want) {
				t.Fatalf("semantic mismatch:\n got  %s\n want %s", got, want)
			}
		})
	}
}

// TestEmitJSONStaysByteIdenticalForTheCommonCases is the byte canary: the
// cases where byte-identity is free must still be byte-for-byte the old
// output (no fixture churn while the contract is relaxed).
func TestEmitJSONStaysByteIdenticalForTheCommonCases(t *testing.T) {
	cursor := "s=x;i=1a2b;b=1;m=2;t=3;x=4"
	for _, fields := range [][]string{
		{"MESSAGE=hello", "PRIORITY=6", "_PID=42"},
		{"MESSAGE=héllo ✓"},
		{"MESSAGE=bad\xffbyte"},
		{"MESSAGE=one", "MESSAGE=two"},
	} {
		b := buildBatch(t, 1700000000500000, cursor, fields...)
		entry := &b.Entries[0]
		got, err := EmitJSON(nil, b, entry, &EmitOptions{})
		if err != nil {
			t.Fatalf("EmitJSON: %v", err)
		}
		want := oraclePayload(t, b, entry, nil, nil)
		if !bytes.Equal(got, want) {
			t.Fatalf("byte mismatch:\n got  %s\n want %s", got, want)
		}
	}
}

// TestEmitJSONOutputIsValidJSON fuzzes random values through the emitter
// and requires valid JSON with the same key set as the oracle.
func TestEmitJSONOutputIsValidJSON(t *testing.T) {
	values := []string{
		"", "0", "-0", "007", "42", "-17", " 9 ", "0x10", "1_000",
		"abc", "héllo", "\xff", "a\"b", "a\\b", "line\nbreak",
		"0123456789abcdef0123456789abcdef", "not-a-uuid",
		"9999999999999999999999", "1700000000500000",
	}
	keys := []string{"MESSAGE", "PRIORITY", "CODE_LINE", "_PID", "_GID", "MESSAGE_ID", "_BOOT_ID", "__MONOTONIC_TIMESTAMP", "_SOURCE_REALTIME_TIMESTAMP", "SYSLOG_IDENTIFIER"}
	for i, value := range values {
		key := keys[i%len(keys)]
		b := buildBatch(t, 1700000000500000, "s=x;i=7;b=1;m=2;t=3;x=4", key+"="+value)
		entry := &b.Entries[0]
		got, err := EmitJSON(nil, b, entry, &EmitOptions{})
		if err != nil {
			t.Fatalf("EmitJSON(%s=%q): %v", key, value, err)
		}
		var parsed map[string]any
		if err := json.Unmarshal(got, &parsed); err != nil {
			t.Fatalf("EmitJSON(%s=%q) not JSON: %v (%s)", key, value, err, got)
		}
		want := oraclePayload(t, b, entry, nil, nil)
		if !semanticEqual(t, got, want) {
			t.Fatalf("EmitJSON(%s=%q) != oracle:\n got  %s\n want %s", key, value, got, want)
		}
	}
}
