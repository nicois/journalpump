// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package reader

import (
	"testing"

	"github.com/Aiven-Open/journalpump/internal/journal"
)

// rawFieldValue returns one raw field's value (as a string) by key.
func rawFieldValue(t *testing.T, batch *journal.Batch, entry *journal.RawEntry, key string) (string, bool) {
	t.Helper()
	for i := range entry.Fields {
		field := &entry.Fields[i]
		if string(batch.Key(field)) == key {
			return string(batch.ValueBytes(field)), true
		}
	}
	return "", false
}

// TestReadRawChunkingInvariant reads the rotated_logs fixture once in
// small batches and once in one big batch; the entry sequence (cursor +
// MESSAGE) must be identical — chunking must not change what is read.
func TestReadRawChunkingInvariant(t *testing.T) {
	collect := func(limit int) []string {
		t.Helper()
		r := openFixture(t, "rotated_logs", nil)
		var out []string
		for {
			batch, err := r.ReadRaw(limit)
			if err != nil {
				t.Fatalf("ReadRaw: %v", err)
			}
			if len(batch.Entries) == 0 {
				return out
			}
			for i := range batch.Entries {
				entry := &batch.Entries[i]
				message, _ := rawFieldValue(t, batch, entry, "MESSAGE")
				out = append(out, string(batch.Cursor(entry))+"|"+message)
			}
			if len(batch.Entries) < limit {
				return out
			}
		}
	}
	one := collect(1000)
	many := collect(3)
	if len(one) != 40 {
		t.Fatalf("big batch: %d entries, want 40", len(one))
	}
	if len(many) != len(one) {
		t.Fatalf("small batches: %d entries, want %d", len(many), len(one))
	}
	for i := range one {
		if one[i] != many[i] {
			t.Fatalf("entry %d differs: %q vs %q", i, one[i], many[i])
		}
	}
}

// TestReadRawGrowsForOversizedEntry starts the reader with a buffer too
// small for even one entry and checks the grow-and-retry path still
// yields the entry.
func TestReadRawGrowsForOversizedEntry(t *testing.T) {
	r := openFixture(t, "rotated_logs", nil)
	r.rawBuf = make([]byte, 8) // smaller than any entry record
	batch, err := r.ReadRaw(1)
	if err != nil {
		t.Fatalf("ReadRaw: %v", err)
	}
	if len(batch.Entries) != 1 {
		t.Fatalf("got %d entries, want 1", len(batch.Entries))
	}
	message, ok := rawFieldValue(t, batch, &batch.Entries[0], "MESSAGE")
	if !ok || message != "Message 0" {
		t.Fatalf("MESSAGE = %q (present %v), want %q", message, ok, "Message 0")
	}
	if len(r.rawBuf) <= 8 {
		t.Fatalf("buffer did not grow: %d bytes", len(r.rawBuf))
	}
}

// TestParseBatchMarksDuplicates pins the duplicate-key marking the
// emission path relies on (python-systemd _get_all repeated values).
func TestParseBatchMarksDuplicates(t *testing.T) {
	var b journal.Batch
	// Entry 1: realtime 0, cursor "c", fields A=1, B=2, A=3.
	b.Buf = buildRawEntry(0, "c", []string{"A=1", "B=2", "A=3"})
	if _, err := journal.ParseBatch(&b, len(b.Buf), 1); err != nil {
		t.Fatalf("ParseBatch: %v", err)
	}
	if len(b.Entries) != 1 || len(b.Entries[0].Fields) != 3 {
		t.Fatalf("unexpected parse: %+v", b.Entries)
	}
	fields := b.Entries[0].Fields
	if fields[0].Duplicate || fields[1].Duplicate {
		t.Fatalf("first occurrences marked duplicate: %+v", fields)
	}
	if !fields[2].Duplicate {
		t.Fatalf("repeated key not marked duplicate: %+v", fields)
	}
}

// buildRawEntry encodes one jp_read_batch entry record.
func buildRawEntry(realtime uint64, cursor string, fields []string) []byte {
	out := make([]byte, 0, 64)
	put := func(v uint64, n int) {
		for i := 0; i < n; i++ {
			out = append(out, byte(v>>(8*i)))
		}
	}
	put(realtime, 8)
	put(uint64(len(cursor)), 2)
	out = append(out, cursor...)
	put(uint64(len(fields)), 4)
	for _, field := range fields {
		put(uint64(len(field)), 4)
		out = append(out, field...)
	}
	return out
}
