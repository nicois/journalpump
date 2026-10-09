package journal

import "strings"

// ParseSeqnumFromCursor ports PumpReader._parse_seqnum_from_cursor
// (journalpump.py:110-118): the cursor format is
// s=...;i=<hex_seqnum>;b=...;m=...;t=...;x=... and the first i= component
// is parsed as Python int(component, 16). The bool reports whether a
// seqnum was found (get_next only appends __SEQNUM when it is).
func ParseSeqnumFromCursor(cursor string) (any, bool) {
	for _, part := range strings.Split(cursor, ";") {
		if hex, ok := strings.CutPrefix(part, "i="); ok {
			seqnum, parsed := pythonIntString(hex, 16)
			if !parsed {
				// Python returns None on the first invalid i= component
				// without trying later ones.
				return nil, false
			}
			return seqnum, true
		}
	}
	return nil, false
}
