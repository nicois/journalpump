package journal

import "strings"

// ProcessEntry ports the entry rewriting in JournalObjectHandler.process
// (journalpump.py:708-714): every key loses its leading underscores
// (Python key.lstrip("_")) and undecodable byte values are replaced by
// their Python repr() string so raw invalid bytes never reach the wire
// (D8, spec §4.4). Duplicate stripped keys follow Python dict assignment:
// the first construction position is kept and the last value wins.
func ProcessEntry(fields []Field) []Field {
	out := make([]Field, 0, len(fields))
	positions := make(map[string]int, len(fields))
	for _, field := range fields {
		key := strings.TrimLeft(field.Key, "_")
		value := field.Value
		if raw, ok := value.([]byte); ok {
			value = pythonReprBytes(raw)
		}
		if pos, seen := positions[key]; seen {
			out[pos].Value = value
		} else {
			positions[key] = len(out)
			out = append(out, Field{Key: key, Value: value})
		}
	}
	return out
}
