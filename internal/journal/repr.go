package journal

import "bytes"

// pythonReprBytes renders the Python repr() of a bytes value, e.g.
// repr(b"a'b\nc\xff") == `b'a\'b\\c\n\xff'`. CPython picks the delimiter
// like str repr does: `"` when the value contains `'` and no `"`
// (repr(b"don't\xff") == `b"don't\xff"`), otherwise `'`; only the chosen
// quote and backslash are escaped. \n \r \t have short escapes, printable
// ASCII is literal, and everything else is lowercase \xHH (Python has no
// short escapes for \a \b \f \v in bytes repr). This is the D8 fallback:
// raw invalid bytes never reach the wire (spec §4.4).
func pythonReprBytes(raw []byte) string {
	const hexDigits = "0123456789abcdef"
	quote := byte('\'')
	if bytes.IndexByte(raw, '\'') >= 0 && bytes.IndexByte(raw, '"') < 0 {
		quote = '"'
	}
	out := make([]byte, 0, len(raw)+3)
	out = append(out, 'b', quote)
	for _, c := range raw {
		switch {
		case c == quote || c == '\\':
			out = append(out, '\\', c)
		case c >= 0x20 && c < 0x7f:
			out = append(out, c)
		case c == '\n':
			out = append(out, '\\', 'n')
		case c == '\r':
			out = append(out, '\\', 'r')
		case c == '\t':
			out = append(out, '\\', 't')
		default:
			out = append(out, '\\', 'x', hexDigits[c>>4], hexDigits[c&0xf])
		}
	}
	return string(append(out, quote))
}
