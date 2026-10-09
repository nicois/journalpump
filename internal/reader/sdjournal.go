// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package reader

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/Aiven-Open/journalpump/internal/journal"
)

// SDJournal is the libsystemd implementation of Reader — the port of
// PumpReader (journalpump.py:88-133, python-systemd's journal.Reader
// underneath). Not safe for concurrent use: Python's reader lives on
// the single pump thread, and the Go run loop (Task 16) keeps that
// shape.
type SDJournal struct {
	h *ffiJournal
}

// PositionMode selects the initial_position behavior
// (journalpump.py:453-474). Python's "tail"/"head" strings and int
// seconds-from-boot map onto these.
type PositionMode int

const (
	// PositionNone leaves the position untouched (Python
	// initial_position None: no initial seek).
	PositionNone PositionMode = iota
	// PositionHead seeks to the first entry; the loop's first read
	// returns the very first entry of the journal.
	PositionHead
	// PositionTail lands the reader at the true tail: seek_tail, then
	// previous, then the step past the last entry.
	PositionTail
	// PositionSeconds seeks monotonic seconds from the current boot.
	PositionSeconds
)

// InitialPosition is the pump's initial_position config value.
type InitialPosition struct {
	Mode    PositionMode
	Seconds int // seconds from the current boot (PositionSeconds)
}

// OpenConfig carries the pump's reader-opening parameters
// (journalpump.py:409-481). At most one of Path / Files / Namespace is
// meaningful, like Python's reader_kwargs.
type OpenConfig struct {
	// Flags is the OR of Flag* constants (journal_flags); 0 means the
	// python-systemd default (LOCAL_ONLY).
	Flags int
	Path  string   // journal_path — open the journal directory
	Files []string // journal_files — open individual files
	// Namespace is journal_namespace (sd_journal_open_namespace).
	Namespace string
	// Matches are the add_match constraints: units_to_match entries
	// become _SYSTEMD_UNIT=<unit> matches, journald_filters entries are
	// field/value pairs (journalpump.py:479-481).
	Matches []Match
	// ResumeCursor is the seek_to cursor: seek_cursor + the one-entry
	// skip so the last sent entry is not delivered twice. On reader
	// recreations the pump always passes its last cursor.
	ResumeCursor string
	// Initial applies only when ResumeCursor is empty (the first
	// start); Python clears initial_position after the first
	// successful get_reader.
	Initial InitialPosition
	// Stats, when non-nil, receives the corrupted-entry count of a
	// corrupt TAIL entry during initialization (journalpump.py:466-468).
	// Corruption during reads is reported by ReadBatch's error instead
	// (Python counts it in JournalReader.read_next, pump level).
	Stats *Stats
}

// ErrFilesNotAvailable mirrors get_reader's "Journal files for %r are
// not yet available" arm (journalpump.py:432-438): the reader opened
// successfully but no persistent or runtime files are attached — the
// pump must close it and retry later.
var ErrFilesNotAvailable = errors.New("journal files not yet available")

// FilesAvailableOverride is the fixture/test escape hatch for the
// files-available gate: when non-nil, it replaces the
// has_persistent_files/has_runtime_files gate result. Nil (the
// default) uses the real libsystemd semantics. The Python systest
// harness patches the identical check on the python class
// (systest/groundtruth_support.py:77-78) because arbitrary fixture
// directories are never "journal directories" to libsystemd; the Go
// test harness needs the same escape hatch. Production wiring must
// leave this nil.
var FilesAvailableOverride func(hasPersistent, hasRuntime bool) bool

// sdErr converts a libsystemd negative-errno return into an error.
// "Bad message" (EBADMSG) maps to ErrBadMessage, and missing paths map
// to os.ErrNotExist so callers can reproduce Python's
// FileNotFoundError handling (journalpump.py:439-446).
func sdErr(what string, rc int) error {
	if rc == 0 {
		return nil
	}
	switch errno := syscall.Errno(-rc); errno {
	case syscall.EBADMSG:
		return fmt.Errorf("%s: %w", what, ErrBadMessage)
	case syscall.ENOENT:
		return fmt.Errorf("%s: %w: %s", what, os.ErrNotExist, errno)
	default:
		return fmt.Errorf("%s: %s", what, errno)
	}
}

// Open builds a reader per cfg, including the files-available gate and
// the initial position application — the full get_reader sequence
// minus the pump-owned bookkeeping (initial_position clearing and the
// recreate cadence live in the Task 16 pump).
//
// Flag semantics follow the probed python-systemd surface: flags pass
// through verbatim to the matching sd_journal_open* call, where 0 is
// the None default. Note LOCAL_ONLY/RUNTIME_ONLY are rejected by
// sd_journal_open_directory with EINVAL — Python fails identically
// (ValueError "Opening the journal failed"), so no masking.
func Open(cfg OpenConfig) (*SDJournal, error) {
	flags := cfg.Flags
	var h *ffiJournal
	var rc int
	switch {
	case cfg.Namespace != "":
		h, rc = ffiOpenNamespace(cfg.Namespace, flags)
	case cfg.Path != "":
		h, rc = ffiOpenDirectory(cfg.Path, flags)
	case len(cfg.Files) > 0:
		h, rc = ffiOpenFiles(cfg.Files, flags)
	default:
		h, rc = ffiOpen(flags)
	}
	if rc < 0 {
		return nil, sdErr("open journal", rc)
	}
	s := &SDJournal{h: h}
	fail := func(err error) (*SDJournal, error) {
		_ = s.Close()
		return nil, err
	}
	for _, m := range cfg.Matches {
		if rc := s.h.addMatch(m.Field + "=" + m.Value); rc < 0 {
			return fail(sdErr("add match", rc))
		}
	}

	hasPersistent := s.h.hasPersistentFiles()
	hasRuntime := s.h.hasRuntimeFiles()
	available := hasPersistent != 0 || hasRuntime != 0
	if FilesAvailableOverride != nil {
		available = FilesAvailableOverride(hasPersistent != 0, hasRuntime != 0)
	}
	if !available {
		// Journal files are not ready (e.g. files with namespace are
		// not yet created): the reader would silently deliver nothing,
		// so refuse and let the pump recreate later
		// (journalpump.py:432-438).
		return fail(ErrFilesNotAvailable)
	}

	if err := s.applyInitial(cfg); err != nil {
		return fail(err)
	}
	return s, nil
}

// applyInitial ports the position block of get_reader
// (journalpump.py:448-474).
func (s *SDJournal) applyInitial(cfg OpenConfig) error {
	if cfg.ResumeCursor != "" {
		// Now the cursor points to the last read item; step over it so
		// that we do not read the same item twice. sd_journal's
		// seek_cursor positions BEFORE the cursor entry, so the skip
		// consumes exactly that entry.
		if err := s.Seek(cfg.ResumeCursor); err != nil {
			return err
		}
		_, ok, err := s.readNext()
		if err != nil && !errors.Is(err, ErrBadMessage) {
			return err
		}
		if ok && cfg.Stats != nil {
			// Python's read_next updates last_journal_msg_time for the
			// skipped entry too (journalpump.py:403-405).
			cfg.Stats.NoteCursor()
		}
		return nil
	}
	switch cfg.Initial.Mode {
	case PositionTail:
		// sd_journal_next is not allowed immediately after
		// sd_journal_seek_tail: land on the last entry first, then
		// step past it.
		if rc := s.h.seekTail(); rc < 0 {
			return sdErr("seek tail", rc)
		}
		if s.h.previous() == 0 {
			return nil // empty journal
		}
		// Materializing the last entry can hit "Bad message"; the
		// cursor has still advanced, so count it like read_next does
		// and carry on instead of failing reader init
		// (journalpump.py:458-468).
		if _, err := s.materialize(); err != nil {
			if errors.Is(err, ErrBadMessage) {
				if cfg.Stats != nil {
					cfg.Stats.CorruptedEntry()
				}
			} else {
				return err
			}
		}
		// Step past the last entry so the reader sits at the true tail
		// (journalpump.py:469).
		if _, _, err := s.readNext(); err != nil && !errors.Is(err, ErrBadMessage) {
			return err
		}
	case PositionHead:
		if rc := s.h.seekHead(); rc < 0 {
			return sdErr("seek head", rc)
		}
	case PositionSeconds:
		if err := s.SeekMonotonicSeconds(cfg.Initial.Seconds); err != nil {
			return err
		}
	case PositionNone:
	}
	return nil
}

// SeekMonotonicSeconds ports seek_monotonic(seconds-from-boot):
// python-systemd resolves the current boot id and converts seconds to
// microseconds.
func (s *SDJournal) SeekMonotonicSeconds(seconds int) error {
	boot, rc := bootID()
	if rc < 0 {
		return sdErr("get boot id", rc)
	}
	if rc := s.h.seekMonotonicUsec(boot, uint64(seconds)*1_000_000); rc < 0 {
		return sdErr("seek monotonic", rc)
	}
	return nil
}

// Wait implements Reader.Wait. sd_journal_wait blocks in C and cannot
// be interrupted, so the wait runs in slices (cancellation is honored
// within one 100 ms slice; event wakeups return immediately).
func (s *SDJournal) Wait(ctx context.Context, timeout time.Duration) (Event, error) {
	const slice = 100 * time.Millisecond
	deadline := time.Now().Add(timeout)
	for {
		if err := ctx.Err(); err != nil {
			return EventNOP, err
		}
		usec := uint64(slice / time.Microsecond)
		if timeout > 0 {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				return EventNOP, nil
			}
			usec = uint64(min(remaining, slice) / time.Microsecond)
		}
		rc := s.h.waitUsec(usec)
		if rc < 0 {
			return EventNOP, sdErr("journal wait", rc)
		}
		if rc != 0 {
			return Event(rc), nil
		}
	}
}

// ReadBatch implements Reader.ReadBatch: up to n entries from the
// current position. It returns the entries read so far together with
// ErrBadMessage when the batch ended on a corrupt entry (the caller
// counts it — Python's read_next), and fewer entries than requested
// when the journal is exhausted.
func (s *SDJournal) ReadBatch(n int) ([]journal.Entry, error) {
	out := make([]journal.Entry, 0, n)
	for i := 0; i < n; i++ {
		entry, ok, err := s.readNext()
		if err != nil {
			return out, err
		}
		if !ok {
			return out, nil
		}
		out = append(out, entry)
	}
	return out, nil
}

// Seek implements Reader.Seek.
func (s *SDJournal) Seek(cursor string) error {
	return sdErr("seek cursor", s.h.seekCursor(cursor))
}

// Close implements Reader.Close.
func (s *SDJournal) Close() error {
	if s.h != nil {
		s.h.close()
		s.h = nil
	}
	return nil
}

// readNext ports PumpReader.get_next (journalpump.py:120-133): advance
// one entry, materialize its fields, enrich with __REALTIME_TIMESTAMP
// and __SEQNUM, convert. ok is false at the end of the journal or when
// the entry has no fields (Python's falsy-entry skip).
func (s *SDJournal) readNext() (journal.Entry, bool, error) {
	if s.h.next() == 0 {
		return journal.Entry{}, false, nil
	}
	fields, err := s.materialize()
	if err != nil {
		return journal.Entry{}, false, err
	}
	if len(fields) == 0 {
		return journal.Entry{}, false, nil
	}
	usec, rc := s.h.realtimeUsec()
	if rc < 0 {
		return journal.Entry{}, false, sdErr("get realtime", rc)
	}
	fields = journal.SetField(fields, "__REALTIME_TIMESTAMP", int64(usec))
	cursor, rc := s.h.getCursor()
	if rc < 0 {
		return journal.Entry{}, false, sdErr("get cursor", rc)
	}
	if seqnum, ok := journal.ParseSeqnumFromCursor(cursor); ok {
		fields = journal.SetField(fields, "__SEQNUM", seqnum)
	}
	return journal.Entry{Cursor: cursor, Fields: journal.ConvertEntry(fields)}, true, nil
}

// errFieldWithoutEquals matches python-systemd's _get_all failure on a
// data tuple that carries no '=': corrupted journal data can produce
// such tuples. Python's read_next does NOT tolerate this error text
// (only "Bad message") — it propagates to read_single_message's broad
// handler (journalpump.py:1012-1017), which logs, counts
// unexpected_exception with where="mainloop", sleeps 0.5 s and stops
// the chunk. The Task 16 run loop owns that handler.
var errFieldWithoutEquals = errors.New("journal gave us a field without '='")

// materialize ports python-systemd's _get_all: enumerate the current
// entry's data tuples in order; repeated fields collapse into a value
// list, first occurrences stay scalar bytes. Field keys are ASCII.
func (s *SDJournal) materialize() ([]journal.Field, error) {
	var fields []journal.Field
	for {
		data, rc := s.h.enumerateData()
		if rc == 0 {
			return fields, nil
		}
		if rc < 0 {
			return nil, sdErr("enumerate data", rc)
		}
		idx := bytes.IndexByte(data, '=')
		if idx < 0 {
			// Python raises (read_next only tolerates "Bad message");
			// silently skipping would deliver entries Python rejects.
			return nil, errFieldWithoutEquals
		}
		key := string(data[:idx])
		value := append([]byte(nil), data[idx+1:]...)
		replaced := false
		for i := range fields {
			if fields[i].Key == key {
				switch existing := fields[i].Value.(type) {
				case []any:
					merged := make([]any, 0, len(existing)+1)
					merged = append(merged, existing...)
					merged = append(merged, value)
					fields[i].Value = merged
				default:
					fields[i].Value = []any{existing, value}
				}
				replaced = true
				break
			}
		}
		if !replaced {
			fields = append(fields, journal.Field{Key: key, Value: value})
		}
	}
}
