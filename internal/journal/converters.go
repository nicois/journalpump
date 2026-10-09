package journal

import (
	"encoding/hex"
	"encoding/json"
	"math"
	"math/big"
	"strconv"
	"strings"
	"unicode/utf8"
)

// converters is the port of the journalpump.py converters table
// (journalpump.py:52-69). Journal field values arrive as raw bytes from
// journald; the table converts known fields exactly like Python does, and
// a conversion failure (Python catches ValueError) leaves the value
// untouched for the later UTF-8 decode attempt.
var converters = map[string]func(any) (any, bool){
	"CODE_LINE":                   convertInt,
	"COREDUMP_TIMESTAMP":          convertRealtime,
	"MESSAGE_ID":                  convertUUID,
	"PRIORITY":                    convertInt,
	"_AUDIT_ID":                   convertInt,
	"_AUDIT_LOGINUID":             convertInt,
	"_AUDIT_SESSION":              convertInt,
	"_AUDIT_TYPE":                 convertInt,
	"_BOOT_ID":                    convertUUID,
	"_GID":                        convertInt,
	"_MACHINE_ID":                 convertUUID,
	"_PID":                        convertInt,
	"_SOURCE_MONOTONIC_TIMESTAMP": convertMon,
	"_SOURCE_REALTIME_TIMESTAMP":  convertRealtime,
	"__MONOTONIC_TIMESTAMP":       convertMon,
	"__REALTIME_TIMESTAMP":        convertRealtime,
}

// ConvertEntry ports PumpReader.convert_entry (journalpump.py:89-109):
// applies the converters table to each field value, tolerating conversion
// failures like Python catches ValueError, and then best-effort decodes
// remaining bytes values to strings (Python bytes.decode).
func ConvertEntry(fields []Field) []Field {
	out := make([]Field, len(fields))
	for i, field := range fields {
		out[i] = Field{Key: field.Key, Value: convertValue(field.Key, field.Value)}
	}
	return out
}

func convertValue(key string, value any) any {
	if convert, ok := converters[key]; ok {
		if converted, convertedOK := convert(value); convertedOK {
			value = converted
		}
	}
	if raw, ok := value.([]byte); ok {
		if decoded, valid := decodeUTF8(raw); valid {
			return decoded
		}
	}
	return value
}

// decodeUTF8 mirrors Python bytes.decode() strict decoding: valid bytes
// become a string, invalid ones stay bytes.
func decodeUTF8(raw []byte) (string, bool) {
	if !utf8.Valid(raw) {
		return "", false
	}
	return string(raw), true
}

// convertInt ports Python int() applied to journal field values: int()
// accepts str/bytes with surrounding ASCII whitespace, an optional sign
// and (Python 3.6+) underscores between digits; ints pass through and
// floats truncate toward zero. Types int() rejects with TypeError are
// unreachable from journal-sourced byte values; the value is returned
// unchanged in that case.
func convertInt(value any) (any, bool) {
	switch v := value.(type) {
	case []byte:
		return pythonIntString(string(v), 10)
	case string:
		return pythonIntString(v, 10)
	case json.Number:
		return pythonIntString(v.String(), 10)
	case int64:
		return v, true
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			// Python int(nan) raises ValueError, which convert_entry catches.
			return value, false
		}
		return int64(v), true
	default:
		return value, false
	}
}

// convertRealtime ports convert_realtime (journalpump.py:48-49):
// int(t) / 1000000.0, producing Python float seconds.
func convertRealtime(value any) (any, bool) {
	switch v := value.(type) {
	case []byte:
		if n, ok := pythonIntString(string(v), 10); ok {
			return realtimeFloat(n), true
		}
	case string:
		if n, ok := pythonIntString(v, 10); ok {
			return realtimeFloat(n), true
		}
	case int64:
		return float64(v) / 1e6, true
	default:
		// Python raises TypeError for other types (uncaught, unreachable
		// from journal-sourced byte values).
	}
	return value, false
}

func realtimeFloat(n any) float64 {
	switch v := n.(type) {
	case int64:
		return float64(v) / 1e6
	case json.Number:
		if bigInt, ok := new(big.Int).SetString(v.String(), 10); ok {
			f, _ := new(big.Float).SetInt(bigInt).Float64()
			return f / 1e6
		}
	}
	return 0
}

// convertMon ports convert_mon (journalpump.py:44-45): monotonic
// timestamps always become None.
func convertMon(any) (any, bool) {
	return nil, true
}

// convertUUID ports _convert_uuid (journalpump.py:40-41): the bytes are
// decoded (a UnicodeDecodeError is a ValueError subclass, so undecodable
// values keep their bytes) and canonicalized through uuid.UUID(str) into
// the lowercase 8-4-4-4-12 form.
func convertUUID(value any) (any, bool) {
	var s string
	switch v := value.(type) {
	case []byte:
		decoded, valid := decodeUTF8(v)
		if !valid {
			return value, false
		}
		s = decoded
	case string:
		s = v
	default:
		return value, false
	}
	canonical, ok := canonicalUUID(s)
	if !ok {
		// Python uuid.UUID raises ValueError, which convert_entry catches.
		return value, false
	}
	return canonical, true
}

// canonicalUUID mirrors uuid.UUID(str) parsing: "urn:"/"uuid:" markers
// removed anywhere, []{} trimmed from the ends, hyphens removed, then
// exactly 32 hexadecimal digits.
func canonicalUUID(s string) (string, bool) {
	s = strings.ReplaceAll(s, "urn:", "")
	s = strings.ReplaceAll(s, "uuid:", "")
	s = strings.Trim(s, "[]{}")
	s = strings.ReplaceAll(s, "-", "")
	if len(s) != 32 {
		return "", false
	}
	raw, err := hex.DecodeString(s)
	if err != nil {
		return "", false
	}
	canonical := hex.EncodeToString(raw)
	return canonical[:8] + "-" + canonical[8:12] + "-" + canonical[12:16] + "-" +
		canonical[16:20] + "-" + canonical[20:], true
}

// pythonIntString parses s the way Python int(s, base) does: surrounding
// ASCII whitespace is ignored, an optional sign and (for base 16) an
// optional 0x prefix are accepted, and underscores may appear between
// digits (Python int() quirk, locked by tests). Values that do not fit
// int64 keep their full decimal precision as json.Number, matching
// Python's arbitrary-precision ints. Python's tolerance of non-ASCII
// Unicode digits is not replicated (unreachable from journald values).
func pythonIntString(s string, base int) (any, bool) {
	s = strings.Trim(s, " \t\n\r\v\f")
	if s == "" {
		return nil, false
	}
	negative := false
	switch s[0] {
	case '-':
		negative = true
		s = s[1:]
	case '+':
		s = s[1:]
	}
	if base == 16 && (strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X")) {
		s = s[2:]
	}
	digits, ok := stripUnderscores(s, base)
	if !ok || digits == "" {
		return nil, false
	}
	if negative {
		digits = "-" + digits
	}
	if n, err := strconv.ParseInt(digits, base, 64); err == nil {
		return n, true
	}
	bigInt, ok := new(big.Int).SetString(digits, base)
	if !ok {
		return nil, false
	}
	return json.Number(bigInt.Text(10)), true
}

// stripUnderscores removes Python int() underscores: allowed only between
// digits of the digit run (not leading, trailing, doubled, or adjacent to
// the sign or base prefix).
func stripUnderscores(s string, base int) (string, bool) {
	if !strings.Contains(s, "_") {
		return s, true
	}
	var b strings.Builder
	underscoreAllowed := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '_':
			if !underscoreAllowed {
				return "", false
			}
			underscoreAllowed = false
		case isDigitForBase(c, base):
			b.WriteByte(c)
			underscoreAllowed = true
		default:
			return "", false
		}
	}
	if !underscoreAllowed {
		return "", false
	}
	return b.String(), true
}

func isDigitForBase(c byte, base int) bool {
	switch {
	case c >= '0' && c <= '9':
		return true
	case base == 16 && (c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F'):
		return true
	}
	return false
}
