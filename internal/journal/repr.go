package journal

// pythonReprBytes renders the Python repr() of a bytes value, e.g.
// repr(b"a'b\nc\xff") == `b'a\'b\\c\n\xff'`: single-quoted b'...' with
// \n \r \t short escapes, backslash-escaped ' and \, literal printable
// ASCII, and lowercase \xHH for everything else (Python has no short
// escapes for \a \b \f \v in bytes repr). This is the D8 fallback: raw
// invalid bytes never reach the wire (spec §4.4).
func pythonReprBytes(raw []byte) string {
	const hexDigits = "0123456789abcdef"
	out := make([]byte, 0, len(raw)+3)
	out = append(out, 'b', '\'')
	for _, c := range raw {
		switch {
		case c == '\'' || c == '\\':
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
	return string(append(out, '\''))
}
