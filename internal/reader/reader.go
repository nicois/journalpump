// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

// Package reader ports journalpump's journal reading (plan Task 15,
// spec §4.4): the sd_journal interop via libsystemd, the pump's open
// parameters and initial position semantics, the bad-message
// tolerance, and the read-path observability (inc_line_stats /
// set_persistent_gauge).
//
// Python shape, for reference (journalpump.py:PumpReader +
// python-systemd's journal.Reader):
//
//   - get_reader (journalpump.py:409-478) opens the reader from
//     journal_flags / journal_path / journal_files /
//     journal_namespace, adds units_to_match and journald_filters as
//     add_match constraints, refuses to start without persistent or
//     runtime files, then applies the initial position (head / tail /
//     seconds-from-boot / resume cursor).
//   - read_next (journalpump.py:384-408) tolerates corrupt entries
//     ("Bad message") by counting journal.corrupted_log_entry and
//     returning None — which the caller reads as "no more entries".
//   - check_match (journalpump.py:642-653) is entry-level field
//     equality, NOT an sd_journal add_match.
//
// The consumer seam is deliberately narrow (Wait / ReadBatch / Seek /
// Close) so the Task 16 run loop and tests can substitute fakes; the
// concrete type is *SDJournal.
package reader

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Aiven-Open/journalpump/internal/journal"
)

// Event mirrors the sd_journal_wait results (SD_JOURNAL_NOP=0,
// APPEND=1, INVALIDATE=2): the caller may need to re-check its
// seek/rotation bookkeeping.
type Event int

const (
	// EventNOP means nothing changed during the wait.
	EventNOP Event = 0
	// EventAppend means new entries were appended to the journal.
	EventAppend Event = 1
	// EventInvalidate means files changed underneath the reader
	// (rotation): stored positions may be stale.
	EventInvalidate Event = 2
)

// Journal flag values from <systemd/sd-journal.h>. Python resolves the
// configured names with getattr (journalpump.py:424-434); the table
// below is the probed python-systemd surface, including SYSTEM_ONLY's
// alias value 4.
const (
	FlagLocalOnly   = 1
	FlagRuntimeOnly = 2
	FlagSystem      = 4
	FlagSystemOnly  = 4
	FlagCurrentUser = 8
	FlagOSRoot      = 16
)

// flagNames maps journal_flags names to values, exactly the probed
// python-systemd constants (SYSTEM_ONLY is 4 — it does not exclude
// runtime files despite its name).
var flagNames = map[string]int{
	"LOCAL_ONLY":   FlagLocalOnly,
	"RUNTIME_ONLY": FlagRuntimeOnly,
	"SYSTEM":       FlagSystem,
	"SYSTEM_ONLY":  FlagSystemOnly,
	"CURRENT_USER": FlagCurrentUser,
	"OS_ROOT":      FlagOSRoot,
}

// FlagsFromNames resolves the journal_flags config list to the OR of
// the named constants. nil (config journal_flags absent or not a
// list) yields 0 — Python passes None through to python-systemd, whose
// C layer opens with flags 0 (probed: LOCAL_ONLY would make
// journal_path opening fail with "Opening the journal failed"). An
// empty list is also 0 here (Python's reduce over [] would raise
// TypeError, unreachable through config decoding). Unknown names are
// an error — Python's getattr would raise AttributeError at reader
// construction.
func FlagsFromNames(names []string) (int, error) {
	if len(names) == 0 {
		return 0, nil
	}
	flags := 0
	for _, name := range names {
		value, ok := flagNames[strings.TrimSpace(name)]
		if !ok {
			return 0, fmt.Errorf("unknown journal flag '%s'", name)
		}
		flags |= value
	}
	return flags, nil
}

// Match is one add_match constraint ("FIELD=value").
type Match struct {
	Field string
	Value string
}

// ErrBadMessage classifies sd_journal's -EBADMSG ("Bad message"):
// Python's read_next turns it into journal.corrupted_log_entry + a
// None read (journalpump.py:392-397).
var ErrBadMessage = errors.New("bad message") // -EBADMSG

// Logger is the logging subset the reader needs; the standard library
// *log.Logger satisfies it (same seam as internal/sender).
type Logger interface {
	Printf(format string, args ...any)
}

type discardLogger struct{}

func (discardLogger) Printf(_ string, _ ...any) {}

// Reader is the consumer seam the run loop polls (spec §4.4). Entries
// are journal.Entry — the converted fields (journal.ConvertEntry over
// the raw bytes values), i.e. Python's JournalObject.entry after the
// reader's __REALTIME_TIMESTAMP / __SEQNUM enrichment.
type Reader interface {
	// Wait blocks until the journal changes, the timeout elapses, or
	// ctx is done, then reports what changed (Python's
	// journal.wait(timeout)).
	Wait(ctx context.Context, timeout time.Duration) (Event, error)
	// ReadBatch returns up to n entries starting at the reader's
	// position, advancing it. Fewer entries mean the journal is
	// currently exhausted. When the batch ends on a corrupt entry the
	// entries read so far are returned together with ErrBadMessage —
	// the caller counts journal.corrupted_log_entry and treats the
	// batch end exactly like an exhaustion (Python read_next's None).
	ReadBatch(n int) ([]journal.Entry, error)
	// Seek positions the reader at cursor; the NEXT read returns the
	// entry AT the cursor (sd_journal seek_cursor semantics — Python's
	// resume skip relies on this, journalpump.py:448-452).
	Seek(cursor string) error
	// Close releases the underlying journal handle.
	Close() error
}

// CheckMatch ports the module-level check_match (journalpump.py:642-653):
// an entry passes when no match_key is configured or the entry's field
// equals match_value — Python's `entry.get(match_key) == match_value`
// against the CONVERTED field, so only fields whose converted value is
// a string can match (an int 6 never equals "6" in Python). This is
// entry-level equality — it is NOT an sd_journal add_match, which
// would filter at the journal level.
func CheckMatch(fields []journal.Field, matchKey, matchValue string) bool {
	if matchKey == "" {
		return true
	}
	for _, field := range fields {
		if field.Key == matchKey {
			value, ok := field.Value.(string)
			return ok && value == matchValue
		}
	}
	return false
}
