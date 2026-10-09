package journal

import (
	"bytes"
	"encoding/json"
	"testing"
)

// TestConvertEntry ports the converter-table behavior of
// PumpReader.convert_entry: conversions apply, failures (Python
// ValueError) keep the value, and remaining bytes decode best-effort.
func TestConvertEntry(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		key   string
		value any
		want  any
	}{
		{"priority int", "PRIORITY", []byte("6"), int64(6)},
		{"code line int", "CODE_LINE", []byte("42"), int64(42)},
		{"pid int", "_PID", []byte("1234"), int64(1234)},
		{"priority garbage keeps bytes then decodes", "PRIORITY", []byte("abc"), "abc"},
		{"realtime bytes", "__REALTIME_TIMESTAMP", []byte("1728475023123456"), 1728475023.123456},
		{"realtime int", "__REALTIME_TIMESTAMP", int64(1000000), 1.0},
		{"monotonic None", "__MONOTONIC_TIMESTAMP", []byte("12345"), nil},
		{"uuid canonicalized", "MESSAGE_ID", []byte("550E8400-E29B-41D4-A716-446655440000"), "550e8400-e29b-41d4-a716-446655440000"},
		{"uuid no hyphens", "MESSAGE_ID", []byte("550e8400e29b41d4a716446655440000"), "550e8400-e29b-41d4-a716-446655440000"},
		{"uuid braces and urn", "MESSAGE_ID", []byte("urn:uuid:{550E8400-E29B-41D4-A716-446655440000}"), "550e8400-e29b-41d4-a716-446655440000"},
		{"uuid garbage decodes", "_BOOT_ID", []byte("not-a-uuid"), "not-a-uuid"},
		{"uuid undecodable keeps bytes", "MESSAGE_ID", []byte("\xff"), []byte("\xff")},
		{"unknown key bytes decoded", "MESSAGE", []byte("hello"), "hello"},
		{"unknown key invalid bytes kept", "MESSAGE", []byte("a\xffb"), []byte("a\xffb")},
		{"int whitespace", "_GID", []byte(" 1_0\n"), int64(10)},
		{"int leading zeros", "_GID", []byte("0012"), int64(12)},
		{"int negative", "_GID", []byte("-7"), int64(-7)},
		{"int huge stays precise", "_GID", []byte("10000000000000000000000000"), json.Number("10000000000000000000000000")},
	} {
		got := ConvertEntry([]Field{{Key: tc.key, Value: tc.value}})
		if len(got) != 1 || got[0].Key != tc.key {
			t.Fatalf("%s: unexpected conversion output %v", tc.name, got)
		}
		if !valueEqual(got[0].Value, tc.want) {
			t.Errorf("%s: want %#v, got %#v", tc.name, tc.want, got[0].Value)
		}
	}
}

// valueEqual compares field values including []byte, which is not
// comparable with ==.
func valueEqual(a, b any) bool {
	switch a := a.(type) {
	case []byte:
		b, ok := b.([]byte)
		return ok && bytes.Equal(a, b)
	default:
		return a == b
	}
}

// TestPythonIntString locks the Python int(s, base) quirks: whitespace,
// signs, base-16 prefixes and underscores between digits; rejected forms
// must be rejected like Python ValueError.
func TestPythonIntString(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		value string
		base  int
		want  any
		ok    bool
	}{
		{"42", 10, int64(42), true},
		{" 42\n", 10, int64(42), true},
		{"-7", 10, int64(-7), true},
		{"0012", 10, int64(12), true},
		{"1_0", 10, int64(10), true},
		{"1f4", 16, int64(500), true},
		{"0x1f4", 16, int64(500), true},
		{"+0X1F4", 16, int64(500), true},
		{"1_f4", 16, int64(500), true},
		{"", 10, nil, false},
		{"_", 10, nil, false},
		{"0_x1", 16, nil, false},
		{"1__0", 10, nil, false},
		{"1_", 10, nil, false},
		{"abc", 10, nil, false},
		{"0x10", 10, nil, false},
		{"10000000000000000000000000", 10, json.Number("10000000000000000000000000"), true},
		{"ffffffffffffffff", 16, json.Number("18446744073709551615"), true},
	} {
		got, ok := pythonIntString(tc.value, tc.base)
		if ok != tc.ok {
			t.Errorf("pythonIntString(%q, %d) ok = %v, want %v", tc.value, tc.base, ok, tc.ok)
			continue
		}
		if ok && got != tc.want {
			t.Errorf("pythonIntString(%q, %d) = %#v, want %#v", tc.value, tc.base, got, tc.want)
		}
	}
}
