package journal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// Serialize renders the fields exactly like Python's
// json.dumps(data, default=default_json_serialization) over the entry
// dict built by process()/_get_or_generate_json: default ", "/": "
// separators, ensure_ascii=True escaping, Python float repr, and the
// datetime default handler's isoformat. The output is byte-identical to
// the Python pipeline (golden-file test, spec risk 2).
//
// The expected field order is Python construction order: journald fields
// in entry order, then the "timestamp" assignment (SetField), then
// extra_field_values merged on top (MergeExtraFields).
func Serialize(fields []Field) ([]byte, error) {
	var out bytes.Buffer
	out.WriteByte('{')
	for i, field := range fields {
		if i > 0 {
			out.WriteString(", ")
		}
		out.WriteString(pythonJSONString(field.Key))
		out.WriteString(": ")
		if err := appendPythonJSON(&out, field.Key, field.Value); err != nil {
			return nil, err
		}
	}
	out.WriteByte('}')
	return out.Bytes(), nil
}

func appendPythonJSON(out *bytes.Buffer, key string, value any) error {
	switch v := value.(type) {
	case nil:
		out.WriteString("null")
	case bool:
		if v {
			out.WriteString("true")
		} else {
			out.WriteString("false")
		}
	case string:
		out.WriteString(pythonJSONString(v))
	case []byte:
		// default_json_serialization decodes bytes as UTF-8; after
		// ProcessEntry undecodable bytes are already repr() strings, so an
		// invalid decode here mirrors Python raising UnicodeDecodeError.
		if !utf8.Valid(v) {
			return fmt.Errorf("undecodable byte value for key %q", key)
		}
		out.WriteString(pythonJSONString(string(v)))
	case int64:
		out.WriteString(strconv.FormatInt(v, 10))
	case json.Number:
		out.WriteString(v.String())
	case float64:
		out.WriteString(pythonFloatRepr(v))
	case time.Time:
		// Python's default handler returns obj.isoformat(), which
		// json.dumps then encodes as a regular JSON string.
		out.WriteString(pythonJSONString(pythonISOFormat(v)))
	case []any:
		out.WriteByte('[')
		for i, item := range v {
			if i > 0 {
				out.WriteString(", ")
			}
			if err := appendPythonJSON(out, key, item); err != nil {
				return err
			}
		}
		out.WriteByte(']')
	case []Field:
		out.WriteByte('{')
		for i, field := range v {
			if i > 0 {
				out.WriteString(", ")
			}
			out.WriteString(pythonJSONString(field.Key))
			out.WriteString(": ")
			if err := appendPythonJSON(out, field.Key, field.Value); err != nil {
				return err
			}
		}
		out.WriteByte('}')
	case map[string]any:
		// Nested unordered objects are only reachable from
		// extra_field_values config values. Python serializes them in JSON
		// document order, which map[string]any cannot preserve; keys are
		// emitted sorted instead (recorded deviation; flat scalar extras,
		// the only shape real configs use, are unaffected).
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		out.WriteByte('{')
		for i, key := range keys {
			if i > 0 {
				out.WriteString(", ")
			}
			out.WriteString(pythonJSONString(key))
			out.WriteString(": ")
			if err := appendPythonJSON(out, key, v[key]); err != nil {
				return err
			}
		}
		out.WriteByte('}')
	default:
		return fmt.Errorf("unsupported value type %T", value)
	}
	return nil
}

// pythonJSONString escapes a string exactly like Python's
// json.dumps(ensure_ascii=True): short escapes for \b \f \n \r \t and the
// two JSON specials, \u00xx (lowercase) for other control characters and
// DEL, \uxxxx for non-ASCII BMP characters, and UTF-16 surrogate pairs
// for astral characters. There is deliberately no HTML escaping of
// < > & (Python does not, unlike Go encoding/json — the parity
// equivalent of SetEscapeHTML(false)).
func pythonJSONString(s string) string {
	var out bytes.Buffer
	out.WriteByte('"')
	for _, r := range s {
		switch {
		case r == '\\' || r == '"':
			out.WriteByte('\\')
			out.WriteRune(r)
		case r < 0x20 || r == 0x7f:
			switch r {
			case '\b':
				out.WriteString(`\b`)
			case '\f':
				out.WriteString(`\f`)
			case '\n':
				out.WriteString(`\n`)
			case '\r':
				out.WriteString(`\r`)
			case '\t':
				out.WriteString(`\t`)
			default:
				out.WriteString(`\u`)
				out.WriteString(lowerHex16(uint16(r)))
			}
		case r < 0x7f:
			out.WriteRune(r)
		case r <= 0xFFFF:
			out.WriteString(`\u`)
			out.WriteString(lowerHex16(uint16(r)))
		default:
			adjusted := r - 0x10000
			out.WriteString(`\u`)
			out.WriteString(lowerHex16(uint16(0xd800 + ((adjusted >> 10) & 0x3ff))))
			out.WriteString(`\u`)
			out.WriteString(lowerHex16(uint16(0xdc00 + (adjusted & 0x3ff))))
		}
	}
	out.WriteByte('"')
	return out.String()
}

func lowerHex16(v uint16) string {
	const digits = "0123456789abcdef"
	return string([]byte{
		digits[(v>>12)&0xf],
		digits[(v>>8)&0xf],
		digits[(v>>4)&0xf],
		digits[v&0xf],
	})
}

// pythonFloatRepr renders a float64 exactly like Python repr(), which
// json.dumps uses for float values: shortest round-trip digits with a
// switch to scientific notation when the decimal exponent is < -4 or
// >= 16 (Go's own formatting switches at different bounds, e.g. 1e-05
// and 1e+20 render as "0.00001"/"100000000000000000000").
func pythonFloatRepr(f float64) string {
	switch {
	case math.IsNaN(f):
		return "NaN"
	case math.IsInf(f, 1):
		return "Infinity"
	case math.IsInf(f, -1):
		return "-Infinity"
	}
	// strconv.FormatFloat with 'e' and precision -1 yields the shortest
	// round-trip digits as d[.ddd]e±dd; Python's repr re-formats the same
	// digits with its own notation rules.
	scientific := strconv.FormatFloat(f, 'e', -1, 64)
	mantissa, expPart, _ := strings.Cut(scientific, "e")
	exp, _ := strconv.Atoi(expPart)
	digits := strings.ReplaceAll(mantissa, ".", "")
	negative := mantissa[0] == '-'
	if negative {
		digits = digits[1:]
	}
	if exp >= -4 && exp < 16 {
		return pythonFloatDecimal(negative, digits, exp)
	}
	return scientific
}

// pythonFloatDecimal renders the non-scientific Python repr form: the
// decimal point sits after exp+1 digits, integral values end in ".0",
// and values below 1.0 start with "0.".
func pythonFloatDecimal(negative bool, digits string, exp int) string {
	var out strings.Builder
	if negative {
		out.WriteByte('-')
	}
	switch {
	case exp >= len(digits)-1:
		out.WriteString(digits)
		out.WriteString(strings.Repeat("0", exp-len(digits)+1))
		out.WriteString(".0")
	case exp >= 0:
		out.WriteString(digits[:exp+1])
		out.WriteByte('.')
		out.WriteString(digits[exp+1:])
	default:
		out.WriteString("0.")
		out.WriteString(strings.Repeat("0", -exp-1))
		out.WriteString(digits)
	}
	return out.String()
}

// pythonISOFormat renders a datetime like Python datetime.isoformat():
// second precision when the microsecond is 0, otherwise exactly six
// fractional digits. time.Time values must be microsecond-truncated
// (Python datetimes carry no nanoseconds); see TimestampFromRealtime.
func pythonISOFormat(t time.Time) string {
	if t.Nanosecond()/1000 == 0 {
		return t.Format("2006-01-02T15:04:05")
	}
	return t.Format("2006-01-02T15:04:05.000000")
}
