// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package reader

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Aiven-Open/journalpump/internal/journal"
)

func TestCheckMatch(t *testing.T) {
	fields := []journal.Field{
		{Key: "MESSAGE", Value: "hello"},
		{Key: "PRIORITY", Value: int64(6)},
	}
	cases := []struct {
		name       string
		matchKey   string
		matchValue string
		want       bool
	}{
		{"no match_key configured", "", "anything", true},
		{"string field equals", "MESSAGE", "hello", true},
		{"string field differs", "MESSAGE", "goodbye", false},
		{"missing field", "_HOSTNAME", "host", false},
		// Python: entry.get("PRIORITY") == match_value is int vs str —
		// never equal.
		{"int field never matches a string value", "PRIORITY", "6", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := CheckMatch(fields, tc.matchKey, tc.matchValue); got != tc.want {
				t.Fatalf("CheckMatch(%q, %q) = %v, want %v", tc.matchKey, tc.matchValue, got, tc.want)
			}
		})
	}
}

func TestFlagsFromNames(t *testing.T) {
	cases := []struct {
		name    string
		names   []string
		want    int
		wantErr bool
	}{
		{"absent", nil, 0, false},
		{"single", []string{"SYSTEM"}, FlagSystem, false},
		{"or-combined", []string{"LOCAL_ONLY", "OS_ROOT"}, FlagLocalOnly | FlagOSRoot, false},
		{"system_only alias value", []string{"SYSTEM_ONLY"}, 4, false},
		{"spaces stripped", []string{" SYSTEM "}, FlagSystem, false},
		{"unknown name", []string{"WAT"}, 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := FlagsFromNames(tc.names)
			if got != tc.want || (err != nil) != tc.wantErr {
				t.Fatalf("FlagsFromNames(%v) = %d, %v; want %d, err=%v", tc.names, got, err, tc.want, tc.wantErr)
			}
		})
	}
}

// fakeReader is the minimal Reader seam stub for reopener tests.
type fakeReader struct {
	closed bool
}

func (f *fakeReader) Wait(context.Context, time.Duration) (Event, error) { return EventNOP, nil }
func (f *fakeReader) ReadBatch(int) ([]journal.Entry, error)             { return nil, nil }
func (f *fakeReader) Seek(string) error                                  { return nil }
func (f *fakeReader) Close() error                                       { f.closed = true; return nil }

// TestReopenerCadence pins create_journald_reader_if_missing's ≤1
// attempt per 2 s (journalpump.py:201-206) on the synctest clock.
func TestReopenerCadence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var attempts int
		var readers []*fakeReader
		r := NewReopener(func() OpenConfig { return OpenConfig{} }, nil)
		r.open = func(OpenConfig) (Reader, error) {
			attempts++
			if attempts <= 2 {
				return nil, ErrFilesNotAvailable
			}
			reader := &fakeReader{}
			readers = append(readers, reader)
			return reader, nil
		}

		// First Ensure attempts immediately (last attempt is zero time).
		reader, err := r.Ensure()
		if reader != nil || !errors.Is(err, ErrFilesNotAvailable) || attempts != 1 {
			t.Fatalf("first Ensure: reader=%v err=%v attempts=%d", reader, err, attempts)
		}

		// An immediate retry is rate-limited to silence.
		if reader, err := r.Ensure(); reader != nil || err != nil {
			t.Fatalf("rate-limited Ensure returned reader=%v err=%v", reader, err)
		}
		if attempts != 1 {
			t.Fatalf("attempts = %d after rate-limited call", attempts)
		}

		// After 2 s (strictly greater) a new attempt fires.
		time.Sleep(2*time.Second + time.Nanosecond)
		if _, err := r.Ensure(); !errors.Is(err, ErrFilesNotAvailable) || attempts != 2 {
			t.Fatalf("second attempt: err=%v attempts=%d", err, attempts)
		}

		// The third attempt succeeds and sticks.
		time.Sleep(2*time.Second + time.Nanosecond)
		reader, err = r.Ensure()
		if err != nil || reader == nil || attempts != 3 {
			t.Fatalf("third attempt: err=%v reader=%v attempts=%d", err, reader, attempts)
		}
		if again, err := r.Ensure(); err != nil || again != reader {
			t.Fatal("Ensure stopped returning the established reader")
		}

		// Invalidate forgets the reader; the next Ensure recreates
		// (rate-limited).
		r.Invalidate()
		if !readers[0].closed {
			t.Fatal("Invalidate did not close the reader")
		}
		if _, err := r.Ensure(); err != nil {
			t.Fatalf("rate-limited Ensure after Invalidate: %v", err)
		}
		time.Sleep(2*time.Second + time.Nanosecond)
		reader, err = r.Ensure()
		if err != nil || reader == nil || attempts != 4 {
			t.Fatalf("recreate after Invalidate: err=%v reader=%v attempts=%d", err, reader, attempts)
		}
	})
}

// TestWaitTimeout exercises Wait against a real (static) journal: the
// freshly opened reader may report an initial APPEND/INVALIDATE (its
// view of the files just changed), but with nothing further pending
// Wait must settle at EventNOP across the timeout.
func TestWaitTimeout(t *testing.T) {
	r := openFixture(t, "rotated_logs", nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	start := time.Now()
	var settled bool
	for attempt := 0; attempt < 10; attempt++ {
		event, err := r.Wait(ctx, 50*time.Millisecond)
		if err != nil {
			t.Fatalf("Wait: %v", err)
		}
		if event == EventNOP {
			settled = true
			break
		}
	}
	if !settled {
		t.Fatal("Wait never settled at NOP on a static journal")
	}
	if elapsed := time.Since(start); elapsed < 40*time.Millisecond {
		t.Fatalf("Wait returned after %v, waited less than requested", elapsed)
	}
}
