// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package journal

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math/big"
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
// values (object key order is irrelevant). Numbers are decoded with
// UseNumber and compared as exact rationals, so an integer beyond 2^53 is
// NOT rounded to a float64 and silently matched against a neighbour.
func semanticEqual(t *testing.T, a, b []byte) bool {
	t.Helper()
	va, err := decodeJSONNumber(a)
	if err != nil {
		t.Fatalf("payload a is not JSON: %v (%s)", err, a)
	}
	vb, err := decodeJSONNumber(b)
	if err != nil {
		t.Fatalf("payload b is not JSON: %v (%s)", err, b)
	}
	return deepEqualJSON(va, vb)
}

// decodeJSONNumber decodes one JSON value with json.Number, rejecting any
// trailing content after it.
func decodeJSONNumber(data []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, errors.New("trailing data after JSON value")
	}
	return v, nil
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
	case json.Number:
		bv, ok := b.(json.Number)
		if !ok {
			return false
		}
		return jsonNumbersEqual(av, bv)
	default:
		return a == b
	}
}

// jsonNumbersEqual compares two JSON number literals by exact value: the
// literal spellings first (the common case), then as arbitrary-precision
// rationals so "6" and "6.000" compare equal but 9007199254740993 and
// 9007199254740992 do not (a float64 round-trip would conflate them).
func jsonNumbersEqual(a, b json.Number) bool {
	if a.String() == b.String() {
		return true
	}
	ra, ok := new(big.Rat).SetString(a.String())
	if !ok {
		return false
	}
	rb, ok := new(big.Rat).SetString(b.String())
	if !ok {
		return false
	}
	return ra.Cmp(rb) == 0
}

// TestSemanticEqualNumberPrecision is the negative control for the
// comparison oracle: numbers that a float64 decode would conflate must
// compare unequal, and equal numbers spelled differently must compare
// equal.
func TestSemanticEqualNumberPrecision(t *testing.T) {
	t.Run("values beyond 2^53 differ", func(t *testing.T) {
		// 2^53+1 and 2^53 are the same float64.
		if semanticEqual(t, []byte(`{"n":9007199254740993}`), []byte(`{"n":9007199254740992}`)) {
			t.Fatal("2^53+1 and 2^53 compared equal (float64 precision loss)")
		}
	})
	t.Run("huge integers match exactly", func(t *testing.T) {
		n := []byte(`{"n":123456789012345678901234567890}`)
		if !semanticEqual(t, n, n) {
			t.Fatal("identical huge integer compared unequal")
		}
	})
	t.Run("huge integers one apart differ", func(t *testing.T) {
		if semanticEqual(t, []byte(`{"n":123456789012345678901234567891}`), []byte(`{"n":123456789012345678901234567890}`)) {
			t.Fatal("huge integers one apart compared equal")
		}
	})
	t.Run("equivalent spellings match", func(t *testing.T) {
		if !semanticEqual(t, []byte(`{"n":6}`), []byte(`{"n":6.000}`)) {
			t.Fatal("6 and 6.000 compared unequal")
		}
	})
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

// TestEmitJSONIndexDate pins the Elasticsearch index-date sidecar (design
// §2.4): it holds the synthesised timestamp's first 10 bytes when the
// payload carries it, and stays zero when the timestamp is filtered out or
// overridden by an extra, so a consumer parses rather than trusting a
// mismatched date.
func TestEmitJSONIndexDate(t *testing.T) {
	cursor := "s=x;i=1a2b;b=1;m=2;t=3;x=4"
	realtime := uint64(1700000000500000) // 2023-11-14T22:13:20.500000+00:00 UTC
	timestamp, err := TimestampFromRealtime(float64(realtime) / 1e6)
	if err != nil {
		t.Fatalf("TimestampFromRealtime: %v", err)
	}
	want := pythonISOFormat(timestamp)[:10]

	t.Run("filled", func(t *testing.T) {
		b := buildBatch(t, realtime, cursor, "MESSAGE=hello", "PRIORITY=6")
		var got [10]byte
		if _, err := EmitJSON(nil, b, &b.Entries[0], &EmitOptions{IndexDate: &got}); err != nil {
			t.Fatalf("EmitJSON: %v", err)
		}
		if string(got[:]) != want {
			t.Fatalf("index date = %q, want %q", got[:], want)
		}
	})

	t.Run("timestamp filtered out", func(t *testing.T) {
		b := buildBatch(t, realtime, cursor, "MESSAGE=hello")
		var got [10]byte
		if _, err := EmitJSON(nil, b, &b.Entries[0], &EmitOptions{
			Filter:    blacklistFilter(t, "timestamp"),
			IndexDate: &got,
		}); err != nil {
			t.Fatalf("EmitJSON: %v", err)
		}
		if got != ([10]byte{}) {
			t.Fatalf("index date = %q, want zero (timestamp filtered)", got[:])
		}
	})

	t.Run("extra overrides timestamp", func(t *testing.T) {
		b := buildBatch(t, realtime, cursor, "MESSAGE=hello")
		var got [10]byte
		if _, err := EmitJSON(nil, b, &b.Entries[0], &EmitOptions{
			Extras:    extraFragments(t, []Field{{Key: "timestamp", Value: "2000-01-01T00:00:00"}}),
			IndexDate: &got,
		}); err != nil {
			t.Fatalf("EmitJSON: %v", err)
		}
		if got != ([10]byte{}) {
			t.Fatalf("index date = %q, want zero (extra override)", got[:])
		}
	})

	t.Run("nil sidecar is safe", func(t *testing.T) {
		b := buildBatch(t, realtime, cursor, "MESSAGE=hello")
		if _, err := EmitJSON(nil, b, &b.Entries[0], &EmitOptions{}); err != nil {
			t.Fatalf("EmitJSON: %v", err)
		}
	})
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
