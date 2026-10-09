package journal

import (
	"encoding/json"
	"math/big"
	"strings"
	"testing"
)

// TestParseSeqnumFromCursor ports the parity cases from
// test/test_journalpump.py test_pump_reader_get_next_includes_seqnum and
// test_pump_reader_get_next_seqnum_missing_or_invalid, plus Python
// int(_, 16) acceptance quirks.
func TestParseSeqnumFromCursor(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		cursor string
		want   any
		ok     bool
	}{
		{"s=abc;i=1f4;b=def;m=123;t=456;x=789", int64(500), true},
		{"s=abc;i=0x1f4;b=def", int64(500), true},
		{"s=abc;i=0x_1f4;b=def", int64(500), true},
		{"s=abc;i= 1f4 ;b=def", int64(500), true},
		{"s=abc;i=ffffffffffffffff;b=def", json.Number("18446744073709551615"), true},
		{"s=abc;b=def;m=123;t=456;x=789", nil, false},       // no i= field
		{"s=abc;i=;b=def;m=123;t=456;x=789", nil, false},    // empty i= value
		{"s=abc;i=xyz;b=def;m=123;t=456;x=789", nil, false}, // non-hex i= value
		{"", nil, false},
	} {
		got, ok := ParseSeqnumFromCursor(tc.cursor)
		if ok != tc.ok {
			t.Errorf("ParseSeqnumFromCursor(%q) ok = %v, want %v", tc.cursor, ok, tc.ok)
			continue
		}
		if ok && got != tc.want {
			t.Errorf("ParseSeqnumFromCursor(%q) = %#v, want %#v", tc.cursor, got, tc.want)
		}
	}
}

// FuzzParseSeqnumFromCursor is the Go-native fuzz test for the cursor
// parser (plan Task 4): the parser must never panic and must agree with a
// naive reference implementation.
func FuzzParseSeqnumFromCursor(f *testing.F) {
	for _, seed := range []string{
		"s=abc;i=1f4;b=def;m=123;t=456;x=789",
		"s=abc;b=def;m=123;t=456;x=789",
		"s=abc;i=;b=def;m=123;t=456;x=789",
		"s=abc;i=xyz;b=def;m=123;t=456;x=789",
		"i=0",
		";i=ff;i=1",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, cursor string) {
		got, ok := ParseSeqnumFromCursor(cursor)
		want, wantOK := referenceSeqnum(cursor)
		if ok != wantOK {
			t.Fatalf("ParseSeqnumFromCursor(%q) ok = %v, want %v", cursor, ok, wantOK)
		}
		if ok && got != want {
			t.Fatalf("ParseSeqnumFromCursor(%q) = %#v, want %#v", cursor, got, want)
		}
	})
}

// referenceSeqnum is an independent model of the Python behavior used
// only as the fuzz oracle: startswith("i=") matches (a bare "i="
// component parses int("", 16) -> ValueError), and int(s, 16) acceptance
// (whitespace, sign, 0x prefix, underscores between digits or after the
// prefix) is modeled by hand below rather than delegated to
// pythonIntString.
func referenceSeqnum(cursor string) (any, bool) {
	for _, part := range splitSemicolons(cursor) {
		if strings.HasPrefix(part, "i=") {
			return referenceHexInt(part[2:])
		}
	}
	return nil, false
}

func referenceHexInt(s string) (any, bool) {
	s = strings.Trim(s, " \t\n\r\v\f")
	if s == "" {
		return nil, false
	}
	sign := 1
	switch s[0] {
	case '-':
		sign = -1
		s = s[1:]
	case '+':
		s = s[1:]
	}
	if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
		s = strings.TrimPrefix(s, "0x")
		s = strings.TrimPrefix(s, "0X")
		s = strings.TrimPrefix(s, "_")
	}
	previousDigit := false
	var digits strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '_':
			if !previousDigit {
				return nil, false
			}
			previousDigit = false
		case strings.ContainsRune("0123456789abcdefABCDEF", rune(c)):
			digits.WriteByte(c)
			previousDigit = true
		default:
			return nil, false
		}
	}
	if !previousDigit {
		return nil, false
	}
	value, ok := new(big.Int).SetString(digits.String(), 16)
	if !ok {
		return nil, false
	}
	if sign < 0 {
		value.Neg(value)
	}
	if value.IsInt64() {
		return value.Int64(), true
	}
	return json.Number(value.Text(10)), true
}

func splitSemicolons(cursor string) []string {
	var parts []string
	start := 0
	for i := 0; i <= len(cursor); i++ {
		if i == len(cursor) || cursor[i] == ';' {
			parts = append(parts, cursor[start:i])
			start = i + 1
		}
	}
	return parts
}
