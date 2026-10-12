// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package journal

// The raw view types (Task 32): the read path hands the handler a batch of
// entry bytes plus a compact per-field offset index, and JSON is emitted
// straight from those bytes. Nothing here owns memory — every offset and
// length is a view into Batch.Buf (or, for a redacted MESSAGE, into the
// handler's scratch buffer when RawField.Scratch is set).
//
// Lifetime contract: a RawEntry / RawField is valid only until the next
// ReadRaw call returns; senders must never retain a view past the
// handler's return. The one thing that crosses the boundary is the
// serialized payload (separately allocated) and the cursor string.

// RawField is one field of a raw entry: a key/value pair of byte views,
// plus two flags.
type RawField struct {
	KeyOff, KeyLen uint32
	ValOff, ValLen uint32
	// Duplicate marks a repeated key: it is skipped on emission and its
	// value merged into the earlier occurrence's array (python-systemd
	// _get_all's repeated-value list).
	Duplicate bool
	// Scratch marks a value that lives in the handler's scratch buffer
	// (a secret-filter-redacted MESSAGE) rather than Batch.Buf.
	Scratch bool
}

// RawEntry is one journal entry: the realtime usec, the cursor view and
// the field index, all views into the owning Batch.
type RawEntry struct {
	Realtime  uint64
	CursorOff uint32
	CursorLen uint32
	Fields    []RawField
}

// Batch is one ReadRaw result: the entry bytes followed by the parsed
// entry index. Both slices are reused across calls by the reader, so a
// caller must finish with a Batch before the next read.
type Batch struct {
	Buf []byte
	// Fields is the flat backing store for every entry's RawEntry.Fields
	// slice; it is reused across calls.
	Fields  []RawField
	Entries []RawEntry
}

// Cursor returns the entry's cursor as a view into b.Buf.
func (b *Batch) Cursor(e *RawEntry) []byte {
	return b.Buf[e.CursorOff : e.CursorOff+e.CursorLen]
}

// Key returns the field's key as a view.
func (b *Batch) Key(f *RawField) []byte {
	return b.Buf[f.KeyOff : f.KeyOff+f.KeyLen]
}

// ValueBytes returns the field's value as a view. Scratch fields are not
// backed by b.Buf; callers handling scratch must read the scratch buffer
// directly (the handler does).
func (b *Batch) ValueBytes(f *RawField) []byte {
	return b.Buf[f.ValOff : f.ValOff+f.ValLen]
}
