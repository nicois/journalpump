// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package reader

import (
	"github.com/Aiven-Open/journalpump/internal/journal"
)

// ReadRaw reads up to limit entries as one raw batch (one cgo crossing plus
// the parse), reusing the reader's buffer and index across calls. The
// returned Batch's views are valid only until the next ReadRaw call.
//
// Errors mirror ReadBatch: entries read before a failure are returned
// together with the error, and the journal position is left on the entry
// that failed.
func (s *SDJournal) ReadRaw(limit int) (*journal.Batch, error) {
	if limit <= 0 {
		limit = 1
	}
	if _, err := resolvedBatchAPI(); err != nil {
		return nil, err
	}
	for {
		if s.rawBuf == nil {
			s.rawBuf = make([]byte, 256*1024)
		}
		count, used, status, err := s.h.readBatchRaw(s.rawBuf, limit)
		if err != nil {
			return nil, err
		}
		if count == 0 && status == 0 {
			// Not even one entry fit: grow and retry (the C loop stepped
			// the position back onto it).
			if len(s.rawBuf) >= 8*1024*1024 {
				return nil, ErrBadMessage
			}
			s.rawBuf = make([]byte, len(s.rawBuf)*2)
			continue
		}
		s.rawBatch.Buf = s.rawBuf
		badCursor, err := journal.ParseBatch(&s.rawBatch, used, count)
		if err != nil {
			// Reposition exactly on the failing entry so a later read sees
			// it again, as the enumerating path leaves the position.
			if badCursor != nil {
				if seekErr := s.Seek(string(badCursor)); seekErr != nil {
					return &s.rawBatch, seekErr
				}
				// seek_cursor positions BEFORE the entry; step onto it so
				// the position lands exactly where the enumerating path
				// left it (ON the failing entry, which the next read then
				// skips past).
				if _, advErr := s.h.next(); advErr != nil {
					return &s.rawBatch, advErr
				}
			}
			return &s.rawBatch, err
		}
		if status < 0 {
			return &s.rawBatch, sdErr("read batch", status)
		}
		return &s.rawBatch, nil
	}
}

// ReadBatch implements Reader.ReadBatch as an adapter over ReadRaw: the
// converted journal.Entry view is what the run loop consumed before the
// raw path existed, so every existing reader test keeps its meaning.
//
// TEST SEAM, not a production read path: the pump's readSingleMessage
// prefers readSingleRaw whenever the reader implements rawReader
// (*SDJournal does), so production never reaches this. It is kept (Task
// 32 step-7 ruling, option B) because the converted-entry fakes and
// sdjournal_test.go still exercise it, and rewiring them onto the raw
// Batch is riskier than keeping the thin adapter.
func (s *SDJournal) ReadBatch(n int) ([]journal.Entry, error) {
	batch, err := s.ReadRaw(n)
	out := make([]journal.Entry, 0, n)
	if batch != nil {
		for i := range batch.Entries {
			entry, ok := s.entryFromRaw(batch, &batch.Entries[i])
			if ok {
				out = append(out, entry)
			}
		}
	}
	if err != nil {
		return out, err
	}
	return out, nil
}

// entryFromRaw rebuilds the converted journal.Entry for one raw entry:
// the fields with their duplicate-key lists, the __REALTIME_TIMESTAMP /
// __SEQNUM enrichment and ConvertEntry — byte-for-byte what readNext
// produced. ok is false for a field-less entry (Python's falsy skip).
// Part of the ReadBatch TEST SEAM above; not on the production path.
func (s *SDJournal) entryFromRaw(batch *journal.Batch, e *journal.RawEntry) (journal.Entry, bool) {
	if len(e.Fields) == 0 {
		return journal.Entry{}, false
	}
	fields := make([]journal.Field, 0, len(e.Fields))
	for i := range e.Fields {
		field := &e.Fields[i]
		key := string(batch.Key(field))
		value := append([]byte(nil), batch.ValueBytes(field)...)
		if field.Duplicate {
			for j := range fields {
				if fields[j].Key != key {
					continue
				}
				switch existing := fields[j].Value.(type) {
				case []any:
					merged := make([]any, 0, len(existing)+1)
					merged = append(merged, existing...)
					merged = append(merged, value)
					fields[j].Value = merged
				default:
					fields[j].Value = []any{existing, value}
				}
				break
			}
			continue
		}
		fields = append(fields, journal.Field{Key: key, Value: value})
	}
	fields = journal.SetField(fields, "__REALTIME_TIMESTAMP", int64(e.Realtime))
	cursor := string(batch.Cursor(e))
	if seqnum, ok := journal.ParseSeqnumFromCursor(cursor); ok {
		fields = journal.SetField(fields, "__SEQNUM", seqnum)
	}
	return journal.Entry{Cursor: cursor, Fields: journal.ConvertEntry(fields)}, true
}
