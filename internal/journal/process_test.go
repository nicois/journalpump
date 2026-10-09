package journal

import "testing"

// TestProcessEntry locks the process() rewriting (journalpump.py:708-714):
// leading underscores are stripped, undecodable bytes become their Python
// repr() string (D8: raw invalid bytes never reach the wire), and
// duplicate stripped keys keep their first position with the last value
// winning (Python dict assignment).
func TestProcessEntry(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		fields []Field
		want   []Field
	}{
		{
			name:   "leading underscores stripped",
			fields: []Field{{Key: "_PID", Value: int64(1)}, {Key: "__MONOTONIC_TIMESTAMP", Value: nil}},
			want:   []Field{{Key: "PID", Value: int64(1)}, {Key: "MONOTONIC_TIMESTAMP", Value: nil}},
		},
		{
			name:   "all-underscore key strips to empty",
			fields: []Field{{Key: "__", Value: int64(1)}},
			want:   []Field{{Key: "", Value: int64(1)}},
		},
		{
			name:   "undecodable bytes become python repr",
			fields: []Field{{Key: "MESSAGE", Value: []byte("\xff\xfe invalid \x00 bytes")}},
			want:   []Field{{Key: "MESSAGE", Value: `b'\xff\xfe invalid \x00 bytes'`}},
		},
		{
			name:   "repr escapes quotes backslash and short escapes",
			fields: []Field{{Key: "MESSAGE", Value: []byte("a'b\\c\nd\te\x00\"")}},
			want:   []Field{{Key: "MESSAGE", Value: `b'a\'b\\c\nd\te\x00"'`}},
		},
		{
			name:   "repr has no short escapes for a b f v",
			fields: []Field{{Key: "MESSAGE", Value: []byte("\a\b\f\v")}},
			want:   []Field{{Key: "MESSAGE", Value: `b'\x07\x08\x0c\x0b'`}},
		},
		{
			name:   "duplicate stripped keys keep position last value wins",
			fields: []Field{{Key: "_PID", Value: int64(1)}, {Key: "MESSAGE", Value: "x"}, {Key: "PID", Value: int64(2)}},
			want:   []Field{{Key: "PID", Value: int64(2)}, {Key: "MESSAGE", Value: "x"}},
		},
		{
			name:   "any bytes value is repr'd (valid utf-8 cannot occur post-convert)",
			fields: []Field{{Key: "MESSAGE", Value: []byte("plain")}},
			want:   []Field{{Key: "MESSAGE", Value: "b'plain'"}},
		},
	} {
		got := ProcessEntry(tc.fields)
		assertFieldsEqual(t, tc.want, got)
	}
}
