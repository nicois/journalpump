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
	// rawBuf and rawBatch are the reusable buffer and index behind
	// ReadRaw (Task 32). They grow to the largest batch seen and are not
	// safe for concurrent use (matching the single-pump-thread shape).
	rawBuf   []byte
	rawBatch journal.Batch
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
	// Flags is the OR of Flag* constants (journal_flags). Zero means
	// Python's flags=None; Open applies python-systemd's default
	// (LOCAL_ONLY) for plain and namespace opens — see Open.
	Flags int
	Path  string   // journal_path — open the journal directory
	Files []string // journal_files — open individual files
	// Namespace is journal_namespace (sd_journal_open_namespace).
	Namespace string
	// Matches are the add_match constraints: units_to_match entries
	// become _SYSTEMD_UNIT=<unit> matches, journald_filters entries are
	// field/value pairs (journalpump.py:479-481). They are added AFTER
	// the initial position, in Python's order.
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
// has_persistent_files/has_runtime_files result. Nil (the default)
// uses the real libsystemd semantics. The Python systest harness
// patches the identical check on the python class
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

// stepFromRc converts an sd_journal_next/previous return (INT ABI):
// rc > 0 the read position moved onto an entry, rc == 0 the end was
// reached, rc < 0 an error. Separated out because a naive
// nonzero-means-success reading silently turns a negative errno into
// "entry present" once zero-extension is involved.
func stepFromRc(rc int) (present bool, err error) {
	if rc < 0 {
		return false, sdErr("step", rc)
	}
	return rc > 0, nil
}

// Open builds a reader per cfg, including the files-available gate and
// the initial position application — the full get_reader sequence
// minus the pump-owned bookkeeping (initial_position clearing and the
// recreate cadence live in the Task 16 pump).
//
// Flag semantics follow python-systemd's journal.py:168-174: flags
// pass through verbatim, and flags=None (Go zero) means LOCAL_ONLY for
// plain and namespace opens. When a path or files list is given, the
// zero passes through unchanged — probed: sd_journal_open_directory
// rejects LOCAL_ONLY with EINVAL (Python fails identically with
// "Opening the journal failed"), so no masking there.
//
// Order parity note: Python leaves a partially-initialized reader
// behind when a mid-open step fails (its get_reader resets
// journald_reader only on some paths); Go closes the handle and
// returns the error — recorded in the parity doc as a divergence.
func Open(cfg OpenConfig) (*SDJournal, error) {
	flags := cfg.Flags
	if flags == 0 && cfg.Path == "" && cfg.Files == nil {
		// python-systemd's default: LOCAL_ONLY for plain and
		// namespace opens when flags is None (journal.py:168-174).
		// nil distinguishes Python's files=None from a configured
		// empty list ([] passes flags through, like Python's
		// files=[]).
		flags = FlagLocalOnly
	}
	var h *ffiJournal
	var rc int
	var err error
	switch {
	case cfg.Namespace != "":
		h, rc, err = ffiOpenNamespace(cfg.Namespace, flags)
	case cfg.Path != "":
		h, rc, err = ffiOpenDirectory(cfg.Path, flags)
	case cfg.Files != nil:
		h, rc, err = ffiOpenFiles(cfg.Files, flags)
	default:
		h, rc, err = ffiOpen(flags)
	}
	if err != nil {
		return nil, err
	}
	if rc < 0 {
		return nil, sdErr("open journal", rc)
	}
	s := &SDJournal{h: h}
	fail := func(err error) (*SDJournal, error) {
		_ = s.Close()
		return nil, err
	}

	hasPersistent, err := s.h.hasPersistentFiles()
	if err != nil {
		return fail(err)
	}
	hasRuntime, err := s.h.hasRuntimeFiles()
	if err != nil {
		return fail(err)
	}
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
	// Matches are added AFTER the initial position, in Python's order
	// (journalpump.py:479-481): with match-then-seek the skip would
	// consume the first MATCHING entry after the resume cursor and
	// silently lose it whenever the cursor entry does not match the
	// current filters.
	for _, m := range cfg.Matches {
		if rc, err := s.h.addMatch(m.Field + "=" + m.Value); err != nil {
			return fail(err)
		} else if rc < 0 {
			return fail(sdErr("add match", rc))
		}
	}
	if err := s.RegisterForPoll(); err != nil {
		return fail(err)
	}
	return s, nil
}

// RegisterForPoll initializes the journal's notification tracking exactly
// like Python's fd-registration path: python-systemd's Reader.fileno()
// calls sd_journal_get_fd, which runs sd_journal_process internally and
// thereby consumes the open-time file-snapshot INVALIDATE before the pump
// ever sees it (journalpump.py:1177 register_for_poll → fileno()). Without
// this call a freshly opened reader's first sd_journal_wait manufactures
// an INVALIDATE that Python never observes, closing and recreating an
// unchanged journal — and masking it in the run loop would also swallow a
// genuine rotation occurring before the first wait. Registering here makes
// both cases behave correctly: an unchanged journal's first wait is NOP,
// and a real rotation/deletion after the open still reports INVALIDATE.
//
// It is exported so the pump can re-register a reader it reopens, but
// Open already calls it, so callers normally need not.
func (s *SDJournal) RegisterForPoll() error {
	rc, err := s.h.getFd()
	if err != nil {
		return err
	}
	if rc < 0 {
		return sdErr("journal get fd", rc)
	}
	return nil
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
		if rc, err := s.h.seekTail(); err != nil {
			return err
		} else if rc < 0 {
			return sdErr("seek tail", rc)
		}
		prevRc, err := s.h.previous()
		if err != nil {
			return err
		}
		present, err := stepFromRc(prevRc)
		if err != nil {
			return err
		}
		if !present {
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
		if rc, err := s.h.seekHead(); err != nil {
			return err
		} else if rc < 0 {
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
	boot, rc, err := bootID()
	if err != nil {
		return err
	}
	if rc < 0 {
		return sdErr("get boot id", rc)
	}
	rc, err = s.h.seekMonotonicUsec(boot, uint64(seconds)*1_000_000)
	if err != nil {
		return err
	}
	if rc < 0 {
		return sdErr("seek monotonic", rc)
	}
	return nil
}

// Wait implements Reader.Wait. Timeout semantics follow
// python-systemd's wait(): timeout < 0 blocks indefinitely (Python
// wait(None)), timeout == 0 is an immediate non-blocking poll of the
// current state (Python wait(0)), timeout > 0 waits at most that long.
// sd_journal_wait blocks in C and cannot be interrupted, so the
// bounded waits run in slices (cancellation is honored within one
// 100 ms slice; event wakeups return immediately).
func (s *SDJournal) Wait(ctx context.Context, timeout time.Duration) (Event, error) {
	const slice = 100 * time.Millisecond
	if timeout == 0 {
		rc, err := s.h.waitUsec(0)
		if err != nil {
			return EventNOP, err
		}
		if rc < 0 {
			return EventNOP, sdErr("journal wait", rc)
		}
		return Event(rc), nil
	}
	var deadline time.Time
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
	}
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
		rc, err := s.h.waitUsec(usec)
		if err != nil {
			return EventNOP, err
		}
		if rc < 0 {
			return EventNOP, sdErr("journal wait", rc)
		}
		if rc != 0 {
			return Event(rc), nil
		}
	}
}

// Seek implements Reader.Seek.
func (s *SDJournal) Seek(cursor string) error {
	rc, err := s.h.seekCursor(cursor)
	if err != nil {
		return err
	}
	return sdErr("seek cursor", rc)
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
//
// The per-entry read loop no longer uses this (the pump runs ReadRaw →
// ProcessRaw); it remains LIVE for positioning — applyInitial's
// resume-skip and tail landing — and as the converted oracle. Task 32
// step 7 (option B) keeps it: moving the positioning paths onto ReadRaw
// is a possible follow-up.
func (s *SDJournal) readNext() (journal.Entry, bool, error) {
	rc, err := s.h.next()
	if err != nil {
		return journal.Entry{}, false, err
	}
	present, err := stepFromRc(rc)
	if err != nil {
		return journal.Entry{}, false, err
	}
	if !present {
		return journal.Entry{}, false, nil
	}
	fields, err := s.materialize()
	if err != nil {
		return journal.Entry{}, false, err
	}
	if len(fields) == 0 {
		return journal.Entry{}, false, nil
	}
	usec, rc, err := s.h.realtimeUsec()
	if err != nil {
		return journal.Entry{}, false, err
	}
	if rc < 0 {
		return journal.Entry{}, false, sdErr("get realtime", rc)
	}
	fields = journal.SetField(fields, "__REALTIME_TIMESTAMP", int64(usec))
	cursor, rc, err := s.h.getCursor()
	if err != nil {
		return journal.Entry{}, false, err
	}
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
// list, first occurrences stay scalar bytes. Field keys are ASCII. Used
// by readNext (positioning + oracle); see its note.
func (s *SDJournal) materialize() ([]journal.Field, error) {
	var fields []journal.Field
	for {
		data, rc, err := s.h.enumerateData()
		if err != nil {
			return nil, err
		}
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
