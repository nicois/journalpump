// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package reader

import (
	"errors"
	"fmt"
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

// TestReadRawEnumerationErrorConsumesEntry is item 10's regression: a
// libsystemd enumerate_data failure must surface the error WITHOUT
// rewinding onto the corrupt entry, so the next read reaches the
// following entry instead of retrying the bad one forever.
func TestReadRawEnumerationErrorConsumesEntry(t *testing.T) {
	r := openFixture(t, "rotated_logs", nil)
	enableBatchAPIFault(batchFaultEnumerate, 0)
	defer enableBatchAPIFault(0, 0)

	batch, err := r.ReadRaw(1)
	if err == nil {
		t.Fatal("want the injected enumerate failure")
	}
	if len(batch.Entries) != 0 {
		t.Fatalf("rejected entry delivered %d payloads", len(batch.Entries))
	}

	// The rejected entry is consumed; the next read advances past it.
	enableBatchAPIFault(0, 0)
	batch, err = r.ReadRaw(1)
	if err != nil {
		t.Fatalf("follow-up ReadRaw: %v", err)
	}
	if len(batch.Entries) != 1 {
		t.Fatalf("follow-up: %d entries, want 1", len(batch.Entries))
	}
	if msg, _ := rawFieldValue(t, batch, &batch.Entries[0], "MESSAGE"); msg != "Message 1" {
		t.Fatalf("follow-up MESSAGE = %q, want %q (entry 0 was not consumed)", msg, "Message 1")
	}
}

// TestReadRawMetadataFailurePropagates is item 9's regression: a failing
// get_realtime/get_cursor return code must be propagated (never a bogus
// epoch-timestamp or empty-cursor entry) while the entries completed
// before the failure are preserved and the failing entry is consumed.
func TestReadRawMetadataFailurePropagates(t *testing.T) {
	for _, tc := range []struct {
		name string
		bit  int
	}{
		{"realtime", batchFaultRealtime},
		{"cursor", batchFaultCursor},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := openFixture(t, "rotated_logs", nil)
			// Let two entries through, then fail the third entry's metadata.
			enableBatchAPIFault(tc.bit, 2)
			defer enableBatchAPIFault(0, 0)

			batch, err := r.ReadRaw(5)
			if err == nil {
				t.Fatal("want the injected metadata failure")
			}
			if got := len(batch.Entries); got != 2 {
				t.Fatalf("completed entries = %d, want 2", got)
			}
			for i := range batch.Entries {
				want := fmt.Sprintf("Message %d", i)
				if msg, _ := rawFieldValue(t, batch, &batch.Entries[i], "MESSAGE"); msg != want {
					t.Fatalf("entry %d MESSAGE = %q, want %q", i, msg, want)
				}
			}
			// The failed entry (index 2) is consumed: the next read starts
			// at entry 3.
			enableBatchAPIFault(0, 0)
			batch, err = r.ReadRaw(1)
			if err != nil {
				t.Fatalf("follow-up ReadRaw: %v", err)
			}
			if len(batch.Entries) != 1 {
				t.Fatalf("follow-up: %d entries, want 1", len(batch.Entries))
			}
			if msg, _ := rawFieldValue(t, batch, &batch.Entries[0], "MESSAGE"); msg != "Message 3" {
				t.Fatalf("follow-up MESSAGE = %q, want %q (failed entry not consumed)", msg, "Message 3")
			}
		})
	}
}

// TestReadRawOversizedEntryIsConsumed is item 10's other half: an entry
// too large for the cap must be rejected once and CONSUMED, not retried
// forever while the position stays before it.
func TestReadRawOversizedEntryIsConsumed(t *testing.T) {
	oldCap := maxRawBatchBuffer
	maxRawBatchBuffer = 16
	defer func() { maxRawBatchBuffer = oldCap }()

	r := openFixture(t, "rotated_logs", nil)
	r.rawBuf = make([]byte, 8) // too small for any entry

	if _, err := r.ReadRaw(1); !errors.Is(err, ErrBadMessage) {
		t.Fatalf("oversized entry: err = %v, want ErrBadMessage", err)
	}

	// The oversized first entry is consumed; with a normal buffer the next
	// read reaches the second entry.
	maxRawBatchBuffer = oldCap
	r.rawBuf = nil
	batch, err := r.ReadRaw(1)
	if err != nil {
		t.Fatalf("follow-up ReadRaw: %v", err)
	}
	if len(batch.Entries) != 1 {
		t.Fatalf("follow-up: %d entries, want 1", len(batch.Entries))
	}
	if msg, _ := rawFieldValue(t, batch, &batch.Entries[0], "MESSAGE"); msg != "Message 1" {
		t.Fatalf("follow-up MESSAGE = %q, want %q (oversized entry not consumed)", msg, "Message 1")
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
