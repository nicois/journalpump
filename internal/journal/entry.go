// Package journal implements Python-faithful conversion and serialization
// of journald entries: the converter table, the repr()-style fallback for
// undecodable bytes, the i=<hex> cursor seqnum parse, and the JSON wire
// format that is byte-identical to Python's
// json.dumps(data, default=default_json_serialization)
// (journalpump.py PumpReader.convert_entry/get_next and
// JournalObjectHandler.process/_get_or_generate_json).
package journal

// Field is a single journald entry field. Fields are always handled as an
// ordered slice because Python's dict construction order determines the
// JSON key order on the wire.
//
// Value carries one of the Python value shapes the pipeline produces:
//
//	string       decoded UTF-8 (or a Python repr() fallback string)
//	[]byte       raw bytes that are not valid UTF-8
//	int64        Python int from the converter table or enrichment
//	json.Number  integer literal too large for int64 (Python's arbitrary
//	             precision int; marshaled verbatim as decimal digits)
//	float64      Python float (converted __REALTIME_TIMESTAMP, config extras)
//	nil          convert_mon() result (None)
//	bool         config extras
//	time.Time    the injected timestamp (Python datetime)
//	[]any        repeated journal fields (python-systemd list values)
//	[]Field      ordered object (extra_field_values nested objects) — the
//	             wire format is Python construction order, so unordered
//	             maps are deliberately not accepted; config decoding must
//	             capture document order (Task 16 wiring)
type Field struct {
	Key   string
	Value any
}

// Entry is an ordered journald entry plus the journal cursor it was read
// from (Python JournalObject).
type Entry struct {
	Cursor string
	Fields []Field
}

// SetField ports Python dict item assignment (entry[key] = value): the
// value is overwritten when the key already exists, keeping its original
// construction position, and appended otherwise. get_next's
// __REALTIME_TIMESTAMP overwrite and the timestamp/extra_field_values
// assignments in _get_or_generate_json all rely on these semantics.
func SetField(fields []Field, key string, value any) []Field {
	for i := range fields {
		if fields[i].Key == key {
			fields[i].Value = value
			return fields
		}
	}
	return append(fields, Field{Key: key, Value: value})
}

// MergeExtraFields ports data.update(extra_field_values) in
// _get_or_generate_json: every extra key overwrites the value at its
// existing position (Python dict update keeps the original position) or is
// appended in the extras' document order. extras must be supplied in
// Python document order.
func MergeExtraFields(fields, extras []Field) []Field {
	for _, extra := range extras {
		fields = SetField(fields, extra.Key, extra.Value)
	}
	return fields
}
