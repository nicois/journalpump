// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package reader

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Aiven-Open/journalpump/internal/journal"
)

// fixturePath resolves the systest data fixtures from the package dir.
func fixturePath(name string) string {
	return filepath.Join("..", "..", "systest", "data", name)
}

// openFixture opens a fixture journal, enabling the same escape hatch
// the Python systest harness uses (groundtruth_support.py:77-78 patches
// has_persistent_files/has_runtime_files to True — arbitrary fixture
// directories are never journal directories to libsystemd).
func openFixture(t *testing.T, name string, mutate func(*OpenConfig)) *SDJournal {
	t.Helper()
	FilesAvailableOverride = func(bool, bool) bool { return true }
	t.Cleanup(func() { FilesAvailableOverride = nil })
	cfg := OpenConfig{Path: fixturePath(name)}
	if mutate != nil {
		mutate(&cfg)
	}
	r, err := Open(cfg)
	if err != nil {
		t.Fatalf("open fixture %s: %v", name, err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

// fieldValue returns the converted value of one field.
func fieldValue(t *testing.T, entry journal.Entry, key string) (any, bool) {
	t.Helper()
	for _, field := range entry.Fields {
		if field.Key == key {
			return field.Value, true
		}
	}
	return nil, false
}

func wantField(t *testing.T, entry journal.Entry, key string, want any) {
	t.Helper()
	got, ok := fieldValue(t, entry, key)
	if !ok {
		t.Fatalf("entry %s: field %s missing", entry.Cursor, key)
	}
	if fmt.Sprintf("%v", got) != fmt.Sprintf("%v", want) {
		t.Fatalf("field %s = %#v, want %#v", key, got, want)
	}
}

// TestFixtureRotatedLogs is the plan's acceptance check: the reader
// walks systest/data/rotated_logs identically to Python's PumpReader
// (probed ground truth: 40 entries, "Message 0" .. "Message 39", entry
// zero carrying seqnum 3 and realtime 1640610602.142779).
func TestFixtureRotatedLogs(t *testing.T) {
	r := openFixture(t, "rotated_logs", nil)

	var entries []journal.Entry
	for pass := 0; len(entries) < 40; pass++ {
		if pass > 10 {
			t.Fatalf("reading did not terminate after %d batches (%d entries)", pass, len(entries))
		}
		batch, err := r.ReadBatch(10)
		entries = append(entries, batch...)
		if err != nil && !errors.Is(err, ErrBadMessage) {
			t.Fatalf("ReadBatch: %v", err)
		}
		if len(batch) == 0 && err == nil {
			t.Fatalf("journal exhausted after %d entries, want 40", len(entries))
		}
	}
	if len(entries) != 40 {
		t.Fatalf("read %d entries, want 40", len(entries))
	}

	first := entries[0]
	wantField(t, first, "MESSAGE", "Message 0")
	wantField(t, first, "PRIORITY", 6)
	wantField(t, first, "SYSLOG_IDENTIFIER", "journald-gen-logs")
	wantField(t, first, "_TRANSPORT", "journal")
	wantField(t, first, "_COMM", "python3")
	wantField(t, first, "_PID", 1270)
	wantField(t, first, "_UID", "1000") // _UID has no converter: stays a string
	wantField(t, first, "_GID", 0)
	wantField(t, first, "__REALTIME_TIMESTAMP", 1640610602.142779)
	wantField(t, first, "__SEQNUM", 3)
	if !strings.Contains(first.Cursor, "i=3;") {
		t.Fatalf("first cursor %q does not carry seqnum 3", first.Cursor)
	}

	// Python get_next enriches in place: __REALTIME_TIMESTAMP (then
	// __SEQNUM) are the LAST keys of the converted entry.
	n := len(first.Fields)
	if got := first.Fields[n-2].Key; got != "__REALTIME_TIMESTAMP" {
		t.Fatalf("second-to-last key = %q, want __REALTIME_TIMESTAMP", got)
	}
	if got := first.Fields[n-1].Key; got != "__SEQNUM" {
		t.Fatalf("last key = %q, want __SEQNUM", got)
	}

	for i, entry := range entries {
		wantField(t, entry, "MESSAGE", fmt.Sprintf("Message %d", i))
	}

	// The final batch must report exhaustion cleanly.
	batch, err := r.ReadBatch(10)
	if len(batch) != 0 || err != nil {
		t.Fatalf("after exhaustion: batch=%d err=%v, want 0/nil", len(batch), err)
	}
}

// TestResumeCursor pins the resume semantics: after reopening with the
// cursor of the first entry, the next read starts at the SECOND entry
// (seek_cursor positions before the cursor entry; the one-entry skip
// consumes it).
func TestResumeCursor(t *testing.T) {
	r := openFixture(t, "rotated_logs", nil)
	first, err := r.ReadBatch(1)
	if err != nil || len(first) != 1 {
		t.Fatalf("first read: %d entries, err %v", len(first), err)
	}
	_ = r.Close()

	r2 := openFixture(t, "rotated_logs", func(cfg *OpenConfig) {
		cfg.ResumeCursor = first[0].Cursor
	})
	next, err := r2.ReadBatch(1)
	if err != nil || len(next) != 1 {
		t.Fatalf("resume read: %d entries, err %v", len(next), err)
	}
	wantField(t, next[0], "MESSAGE", "Message 1")
}

// TestInitialPositionModes covers head and tail; the seconds mode is
// exercised for its error path only (the fixtures carry a 2021 boot id,
// so a current-boot monotonic seek can never land on them — the same
// holds for Python).
func TestInitialPositionModes(t *testing.T) {
	t.Run("head", func(t *testing.T) {
		r := openFixture(t, "rotated_logs", func(cfg *OpenConfig) {
			cfg.Initial = InitialPosition{Mode: PositionHead}
		})
		batch, err := r.ReadBatch(1)
		if err != nil || len(batch) != 1 {
			t.Fatalf("head read: %d entries, err %v", len(batch), err)
		}
		wantField(t, batch[0], "MESSAGE", "Message 0")
	})
	t.Run("tail", func(t *testing.T) {
		r := openFixture(t, "rotated_logs", func(cfg *OpenConfig) {
			cfg.Initial = InitialPosition{Mode: PositionTail}
		})
		batch, err := r.ReadBatch(10)
		if err != nil || len(batch) != 0 {
			t.Fatalf("tail read: %d entries, err %v, want empty-at-tail", len(batch), err)
		}
	})
	t.Run("seconds-from-boot", func(t *testing.T) {
		r := openFixture(t, "rotated_logs", func(cfg *OpenConfig) {
			cfg.Initial = InitialPosition{Mode: PositionSeconds, Seconds: 0}
		})
		batch, err := r.ReadBatch(10)
		if err != nil || len(batch) != 0 {
			t.Fatalf("seconds seek read: %d entries, err %v (fixture boot != current boot)", len(batch), err)
		}
	})
}

// TestBadMessageFixture pins the corrupt-data behavior on
// systest/data/bad_msg (probed against python-systemd): the good file's
// ten entries read fine; the corrupt file's entries raise
// "journal gave us a field without '='" — the error text Python's
// read_next does NOT tolerate (only "Bad message"), so it propagates
// to read_single_message's broad handler (Task 16: log +
// unexpected_exception where="mainloop" + sleep 0.5 + stop chunk).
func TestBadMessageFixture(t *testing.T) {
	r := openFixture(t, "bad_msg", func(cfg *OpenConfig) {
		cfg.Initial = InitialPosition{Mode: PositionHead}
	})

	// One big batch stops at the first corrupt entry: the ten good
	// entries come back together with the failure.
	good, err := r.ReadBatch(100)
	if err == nil || errors.Is(err, ErrBadMessage) {
		t.Fatalf("first batch: err %v, want the field-without-= failure", err)
	}
	if err.Error() != "journal gave us a field without '='" {
		t.Fatalf("first batch: err %q", err)
	}
	if len(good) != 10 {
		t.Fatalf("first batch: %d entries, want the 10 good ones", len(good))
	}
	for i, entry := range good {
		wantField(t, entry, "MESSAGE", fmt.Sprintf("Message %d", i+10))
	}

	// The corrupt file: every read attempt fails until the position
	// moves past its entries — one failed poll per corrupt entry, like
	// Python's pump. The first corrupt entry was already consumed by
	// the batch above; ten corrupt entries total, nine to go.
	failures := 0
	for attempt := 0; attempt < 20; attempt++ {
		batch, err := r.ReadBatch(1)
		if len(batch) != 0 {
			t.Fatalf("corrupt entry delivered %d entries", len(batch))
		}
		if err == nil {
			break // exhausted
		}
		if errors.Is(err, ErrBadMessage) {
			t.Fatalf("attempt %d: err %v, want the field-without-= failure", attempt, err)
		}
		if err.Error() != "journal gave us a field without '='" {
			t.Fatalf("attempt %d: err %q", attempt, err)
		}
		failures++
	}
	if failures != 9 {
		t.Fatalf("corrupt-entry failures after the batch = %d, want 9 (10 total)", failures)
	}

	// After the corrupt entries the reader continues cleanly (nothing
	// follows in this fixture: exhausted).
	batch, err := r.ReadBatch(1)
	if len(batch) != 0 || err != nil {
		t.Fatalf("after corruption: %d entries, err %v", len(batch), err)
	}
}

// TestSdErrClassification pins the errno → error mapping that the
// callers branch on ("Bad message" tolerance, FileNotFoundError
// parity).
func TestSdErrClassification(t *testing.T) {
	err := sdErr("enumerate data", -74) // -EBADMSG
	if !errors.Is(err, ErrBadMessage) {
		t.Fatalf("EBADMSG not classified: %v", err)
	}
	err = sdErr("open journal", -2) // -ENOENT
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ENOENT not classified: %v", err)
	}
	if sdErr("x", 0) != nil {
		t.Fatal("rc=0 must be nil")
	}
}

// captureLogger records Printf lines.
type captureLogger struct {
	lines []string
}

func (c *captureLogger) Printf(format string, args ...any) {
	c.lines = append(c.lines, fmt.Sprintf(format, args...))
}

func (c *captureLogger) count(substr string) int {
	n := 0
	for _, line := range c.lines {
		if strings.Contains(line, substr) {
			n++
		}
	}
	return n
}

// TestIncLineStats covers the throttle/diff/print machinery of
// inc_line_stats with an injected clock (the gauge emission side is
// covered by the statsd package tests).
func TestIncLineStats(t *testing.T) {
	logs := &captureLogger{}
	var refreshes int
	stats := NewStats("r", map[string]string{}, nil, logs, func() { refreshes++ })

	clock := time.Unix(0, 0)
	stats.now = func() time.Time { return clock }
	// Rebase the construction-time stamps onto the fake clock.
	stats.lastStatsSend = clock
	stats.lastStatsPrint = clock
	stats.lastJournalMsg = clock

	// First call at t=0: throttled (last_stats_send_time initialized
	// to now) — counters accumulate, nothing fires.
	stats.IncLineStats(5, 100)
	if lines, bytes := stats.Totals(); lines != 5 || bytes != 100 {
		t.Fatalf("totals after throttled call: %d/%d", lines, bytes)
	}
	if refreshes != 0 || logs.count("Processed") != 0 {
		t.Fatalf("throttled call fired: refreshes=%d logs=%v", refreshes, logs.lines)
	}

	// After 10s the stats path runs: refresh fires, diff bookkeeping
	// runs, but the 120s print does not.
	clock = clock.Add(11 * time.Second)
	stats.IncLineStats(5, 100)
	if refreshes != 1 {
		t.Fatalf("refresh calls = %d, want 1", refreshes)
	}
	if logs.count("Processed") != 0 {
		t.Fatalf("unexpected 120s print: %v", logs.lines)
	}

	// After 120s from the print baseline the summary line appears with
	// the since-last-print diffs.
	clock = clock.Add(121 * time.Second)
	stats.IncLineStats(0, 0)
	if logs.count("Processed 10 journal lines (200 bytes)") != 1 {
		t.Fatalf("missing summary line: %v", logs.lines)
	}

	// The last_read_ago baseline moves only on cursor reads.
	clock = clock.Add(3 * time.Second)
	if ago := stats.LastJournalMsgAgo(); ago <= 130*time.Second {
		t.Fatalf("last_read_ago = %v, want > 130s before any cursor", ago)
	}
	stats.NoteCursor()
	if ago := stats.LastJournalMsgAgo(); ago > time.Second {
		t.Fatalf("last_read_ago after NoteCursor = %v", ago)
	}
}
