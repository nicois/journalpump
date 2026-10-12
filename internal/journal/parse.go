// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package journal

import (
	"bytes"
	"encoding/binary"
	"errors"
)

// ErrFieldWithoutEquals matches python-systemd's _get_all failure on a
// data tuple that carries no '=': corrupted journal data can produce such
// tuples. Python's read_next does NOT tolerate this error text (only "Bad
// message") — it propagates to read_single_message's broad handler.
var ErrFieldWithoutEquals = errors.New("journal gave us a field without '='")

// ParseBatch parses count entry records from b.Buf[:used] into b.Entries
// (and the flat b.Fields backing store), applying the duplicate-key marks.
// The slice backing b.Entries and b.Fields is reused across calls.
//
// If an entry has a data tuple without '=', parsing stops there and
// ErrFieldWithoutEquals is returned together with the raw cursor view of
// the offending entry (b.Entries holds only the entries before it), so the
// caller can reposition exactly on it — the reader returns the entries so
// far plus the error, as today.
func ParseBatch(b *Batch, used, count int) (badCursor []byte, err error) {
	buf := b.Buf[:used]
	b.Entries = b.Entries[:0]
	b.Fields = b.Fields[:0]
	off := 0
	for i := 0; i < count; i++ {
		rt := binary.LittleEndian.Uint64(buf[off:])
		off += 8
		clen := int(binary.LittleEndian.Uint16(buf[off:]))
		off += 2
		cursorOff := off
		off += clen
		nfields := int(binary.LittleEndian.Uint32(buf[off:]))
		off += 4
		fieldStart := len(b.Fields)
		for f := 0; f < nfields; f++ {
			l := int(binary.LittleEndian.Uint32(buf[off:]))
			off += 4
			dataOff := off
			off += l
			eq := bytes.IndexByte(buf[dataOff:dataOff+l], '=')
			if eq < 0 {
				b.Fields = b.Fields[:fieldStart] // discard the partial entry
				return buf[cursorOff : cursorOff+clen], ErrFieldWithoutEquals
			}
			field := RawField{
				KeyOff: uint32(dataOff),
				KeyLen: uint32(eq),
				ValOff: uint32(dataOff + eq + 1),
				ValLen: uint32(l - eq - 1),
			}
			// Duplicate detection: repeated keys keep their first position
			// and merge into that key's array (python-systemd _get_all).
			key := buf[dataOff : dataOff+eq]
			for j := fieldStart; j < len(b.Fields); j++ {
				existing := b.Fields[j]
				if existing.Duplicate {
					continue
				}
				if bytes.Equal(buf[existing.KeyOff:existing.KeyOff+existing.KeyLen], key) {
					field.Duplicate = true
					break
				}
			}
			b.Fields = append(b.Fields, field)
		}
		b.Entries = append(b.Entries, RawEntry{
			Realtime:  rt,
			CursorOff: uint32(cursorOff),
			CursorLen: uint32(clen),
			Fields:    b.Fields[fieldStart:len(b.Fields):len(b.Fields)],
		})
	}
	return nil, nil
}
