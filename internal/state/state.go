// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

// Package state ports the state-file I/O of journalpump.py (D3): the
// JSON file journalpump writes every state-save cadence and reads at
// reload/restart to pick each reader's resume cursor
// (save_state journalpump.py:1057-1073, load_state 987-995, resume
// selection 889-905).
//
// Bidirectional compatibility with Python is locked by tests in both
// directions: the Go reader reads a committed Python-written fixture,
// a committed Go-written fixture is read by the permanent pytest
// test/test_state_file_interop.py, and the Go rewrite of the Python
// fixture is byte-identical.
package state

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"unicode/utf8"

	"github.com/Aiven-Open/journalpump/internal/util"
)

// BufferState mirrors senders/base.py Sender.get_state "buffer" object.
// Cursor is decoded leniently as raw JSON bytes: the buffer cursor is
// written but never read back in Python, so the reader must accept any
// value Python could have written rather than reject the file.
type BufferState struct {
	Cursor       json.RawMessage `json:"cursor"`
	TotalSize    int64           `json:"total_size"`
	EntryNum     int64           `json:"entry_num"`
	CurrentQueue int64           `json:"current_queue"`
}

// SentState mirrors the "sent" object.
type SentState struct {
	Cursor *string `json:"cursor"`
	Count  int64   `json:"count"`
	Bytes  int64   `json:"bytes"`
}

// HealthState mirrors the "health" object. Elapsed is a monotonic
// float exactly like Python's (meaningless across restarts, but part
// of the file); Errors is a bounded list of the most recent error
// messages — lists are NOT sorted on write (Python's sort_keys only
// sorts object keys).
type HealthState struct {
	Status  string   `json:"status"`
	Elapsed float64  `json:"elapsed"`
	Errors  []string `json:"errors"`
}

// SenderState mirrors one sender's get_state() value. sentCursorMissing
// records whether the raw sender value lacked the sent.cursor key path
// Python indexes unconditionally at resume-selection time
// (journalpump.py:896); the check happens at the WALK position in
// ResumeCursor, not at decode (see ResumeCursor). It is false for
// states constructed in Go, which always carry sent.cursor.
type SenderState struct {
	Type   string      `json:"type"`
	Buffer BufferState `json:"buffer"`
	Sent   SentState   `json:"sent"`
	Health HealthState `json:"health"`

	sentCursorMissing bool
}

// NamedSenderState pairs a sender name with its state; Senders keeps
// the FILE order on decode — the resume quirk reads the file's key
// order, not the sorted order (see ResumeCursor).
type NamedSenderState struct {
	Name  string
	State SenderState
}

// ReaderState mirrors JournalReader.get_state (journalpump.py:326-335).
// The write-only fields (searches hit counters, total_lines,
// total_bytes) are decoded leniently as raw JSON bytes: Python never
// reads them back, and strict typing would make Go refuse state files
// Python accepts (e.g. a float hit counter written by a Python with a
// slightly different counter type). The writer emits the raw bytes
// verbatim, so lenient decode is also byte-faithful. Cursor IS
// consumed by the resume selection, so it stays strictly a string.
type ReaderState struct {
	Cursor     *string
	Searches   map[string]json.RawMessage
	Senders    []NamedSenderState
	TotalLines json.RawMessage
	TotalBytes json.RawMessage
}

// State mirrors the whole file: {"readers": {...}, "start_time": str}.
// StartTime is decoded leniently (raw JSON bytes) like the other
// write-only fields — Python never reads it back.
type State struct {
	Readers   map[string]*ReaderState
	StartTime json.RawMessage
}

// Logger is the logging subset the resume selection uses for the
// "needs full sync" info line; the standard library *log.Logger
// satisfies it.
type Logger interface {
	Printf(format string, args ...any)
}

// StateFileMode is the mode Python's atomic_replace_file produces for
// the state file: mkstemp defaults to 0600 and journalpump.py calls it
// without an explicit mode.
const StateFileMode os.FileMode = 0o600

// Write renders state exactly like Python's
// json.dump(state, fp, indent=4, sort_keys=True) — alphabetically
// sorted keys at every nesting level, 4-space indent, ensure_ascii
// string escaping, Python float repr — and replaces the file atomically
// with mode 0600 via util.AtomicReplaceFile (Python's
// atomic_replace_file without an explicit mode). Sender lists are
// sorted by name on write: sort_keys sorts OBJECT keys, so the file
// order is alphabetical regardless of the in-memory order — but lists
// (health.errors) keep their order.
func Write(path string, state *State) error {
	return util.AtomicReplaceFile(path, StateFileMode, func(w io.Writer) error {
		_, err := w.Write([]byte(render(state)))
		return err
	})
}

// Read parses a state file the way Python's load_state does: a missing
// file is an empty state (FileNotFoundError → {}), every other error —
// including invalid JSON — propagates to the caller. In Python that
// propagation is NOT uniform: at startup the exception escapes and
// crashes the daemon (daemon.py:48 calls reload_config before anything
// catches it), but on a SIGHUP reload it is caught and logged
// ("Reloading configuration failed", daemon.py:80-83) and the old
// configuration keeps running. The Task 17 caller must therefore catch
// Read errors on reload instead of crashing.
//
// The per-reader senders object is decoded with token streaming to
// preserve the FILE key order (encoding/json into a map would discard
// it and silently fix the resume quirk); every other object is decoded
// as plain maps/structs since only the senders order is
// order-sensitive. Duplicate reader names follow the map decode
// (last wins); a duplicate sender NAME keeps its first position with
// the last value, and a duplicate "senders" key replaces the whole
// previous object, both matching Python's dict re-assignment semantics.
func Read(path string) (*State, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return &State{Readers: map[string]*ReaderState{}}, nil
		}
		return nil, fmt.Errorf("reading state file %s: %w", path, err)
	}
	if !utf8.Valid(data) {
		// Python opens the file with encoding="utf-8" and lets the
		// UnicodeDecodeError propagate.
		return nil, fmt.Errorf("state file %s: invalid UTF-8", path)
	}
	return parse(data)
}

func parse(data []byte) (*State, error) {
	state := &State{Readers: map[string]*ReaderState{}}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	tok, err := dec.Token()
	if err != nil {
		return nil, fmt.Errorf("state file: %w", err)
	}
	if delim, ok := tok.(json.Delim); !ok || delim != '{' {
		return nil, fmt.Errorf("state file: must be a JSON object")
	}
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return nil, fmt.Errorf("state file: %w", err)
		}
		key, _ := keyTok.(string)
		switch key {
		case "readers":
			if err := parseReaders(dec, state.Readers); err != nil {
				return nil, err
			}
		case "start_time":
			// Write-only field: decoded leniently as raw bytes (Python
			// never reads it back).
			value, err := decodeRawValue(dec)
			if err != nil {
				return nil, fmt.Errorf("state file: start_time: %w", err)
			}
			state.StartTime = value
		default:
			// Unknown top-level keys are kept by Python's dict but never
			// consumed; skipping the value matches the observable
			// behavior.
			if err := skipValue(dec); err != nil {
				return nil, fmt.Errorf("state file: %w", err)
			}
		}
	}
	if _, err := dec.Token(); err != nil { // closing }
		return nil, fmt.Errorf("state file: %w", err)
	}
	// Python's json.load parses the WHOLE file; trailing garbage is a
	// JSONDecodeError. Verify nothing remains.
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("state file: trailing data after JSON object")
	}
	return state, nil
}

func parseReaders(dec *json.Decoder, readers map[string]*ReaderState) error {
	tok, err := dec.Token()
	if err != nil {
		return fmt.Errorf("state file: readers: %w", err)
	}
	if delim, ok := tok.(json.Delim); !ok || delim != '{' {
		return fmt.Errorf("state file: readers: must be a JSON object")
	}
	for dec.More() {
		nameTok, err := dec.Token()
		if err != nil {
			return fmt.Errorf("state file: readers: %w", err)
		}
		name, ok := nameTok.(string)
		if !ok {
			return fmt.Errorf("state file: readers: key must be a string")
		}
		reader, err := parseReader(dec)
		if err != nil {
			return err
		}
		// Duplicate reader names: last wins (Python dict semantics).
		readers[name] = reader
	}
	if _, err := dec.Token(); err != nil { // closing }
		return fmt.Errorf("state file: readers: %w", err)
	}
	return nil
}

// parseReader decodes one reader state. The senders object is walked
// with tokens for its key order; the sibling fields decode from the
// raw value bytes (their key order is irrelevant).
func parseReader(dec *json.Decoder) (*ReaderState, error) {
	raw, err := decodeRawValue(dec)
	if err != nil {
		return nil, fmt.Errorf("state file: reader: %w", err)
	}
	reader := &ReaderState{Searches: map[string]json.RawMessage{}}
	// Pass 1: the order-insensitive fields via a struct decode. The
	// write-only fields decode leniently as raw JSON bytes (see
	// ReaderState); cursor stays strict because the resume selection
	// consumes it.
	var fields struct {
		Cursor     *string                    `json:"cursor"`
		Searches   map[string]json.RawMessage `json:"searches"`
		TotalLines json.RawMessage            `json:"total_lines"`
		TotalBytes json.RawMessage            `json:"total_bytes"`
	}
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, fmt.Errorf("state file: reader: %w", err)
	}
	reader.Cursor = fields.Cursor
	reader.Searches = fields.Searches
	if reader.Searches == nil {
		reader.Searches = map[string]json.RawMessage{}
	}
	reader.TotalLines = fields.TotalLines
	reader.TotalBytes = fields.TotalBytes
	// Pass 2: the senders key order, by token.
	sdec := json.NewDecoder(bytes.NewReader(raw))
	sdec.UseNumber()
	if err := walkReaderObject(sdec, reader); err != nil {
		return nil, err
	}
	return reader, nil
}

// walkReaderObject token-walks one reader object (the '{' must not be
// consumed yet) to capture the senders object in FILE key order; the
// other keys are skipped here (they were already decoded in pass 1).
func walkReaderObject(dec *json.Decoder, reader *ReaderState) error {
	tok, err := dec.Token()
	if err != nil {
		return fmt.Errorf("state file: reader: %w", err)
	}
	if delim, ok := tok.(json.Delim); !ok || delim != '{' {
		return fmt.Errorf("state file: reader: must be a JSON object")
	}
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return fmt.Errorf("state file: reader: %w", err)
		}
		key, _ := keyTok.(string)
		if key != "senders" {
			if err := skipValue(dec); err != nil {
				return fmt.Errorf("state file: reader: %w", err)
			}
			continue
		}
		// A duplicate "senders" key replaces the whole previous object
		// (Python dict re-assignment keeps only the last value).
		reader.Senders = nil
		if err := walkSenders(dec, reader); err != nil {
			return err
		}
	}
	if _, err := dec.Token(); err != nil { // closing }
		return fmt.Errorf("state file: reader: %w", err)
	}
	return nil
}

// walkSenders decodes the reader's senders object (the '{' must not be
// consumed yet) preserving the file key order. A duplicate sender name
// keeps its FIRST position with the LAST value — Python's dict
// re-assignment semantics, which the resume loop iterates.
func walkSenders(dec *json.Decoder, reader *ReaderState) error {
	tok, err := dec.Token()
	if err != nil {
		return fmt.Errorf("state file: reader senders: %w", err)
	}
	if delim, ok := tok.(json.Delim); !ok || delim != '{' {
		return fmt.Errorf("state file: reader senders: must be a JSON object")
	}
	positions := map[string]int{}
	for dec.More() {
		nameTok, err := dec.Token()
		if err != nil {
			return fmt.Errorf("state file: reader senders: %w", err)
		}
		name, ok := nameTok.(string)
		if !ok {
			return fmt.Errorf("state file: reader senders: key must be a string")
		}
		raw, err := decodeRawValue(dec)
		if err != nil {
			return fmt.Errorf("state file: reader sender %q: %w", name, err)
		}
		var sender SenderState
		// Lenient decode: Python never touches a sender object here
		// beyond sent.cursor at resume-selection time, so mismatched or
		// malformed write-only fields must not make Go reject a file
		// Python accepts. Whatever fails decodes as zero values; the
		// only consumed key path, sent.cursor, has its PRESENCE probed
		// separately (the probe handles non-object "sent" values the
		// way Python's TypeError would — missing at the walk position).
		_ = json.Unmarshal(raw, &sender)
		sender.sentCursorMissing = rawLacksSentCursor(raw)
		if position, ok := positions[name]; ok {
			reader.Senders[position].State = sender
			continue
		}
		positions[name] = len(reader.Senders)
		reader.Senders = append(reader.Senders, NamedSenderState{Name: name, State: sender})
	}
	if _, err := dec.Token(); err != nil { // closing }
		return fmt.Errorf("state file: reader senders: %w", err)
	}
	return nil
}

// rawLacksSentCursor reports whether a sender value lacks the
// "sent"."cursor" key path Python indexes unconditionally at resume
// selection (journalpump.py:896). The key PRESENCE matters, not its
// value: a present null cursor is a legal full-resync marker, so the
// probe uses a non-pointer RawMessage (a `null` value decodes to the
// literal bytes "null", an absent key to nil). A raw value that does
// not unmarshal at all (e.g. "sent" is not an object) also counts as
// lacking — Python would raise KeyError/TypeError at the same walk
// position.
func rawLacksSentCursor(raw []byte) bool {
	var probe struct {
		Sent *struct {
			Cursor json.RawMessage `json:"cursor"`
		} `json:"sent"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return true
	}
	return probe.Sent == nil || probe.Sent.Cursor == nil
}

// pythonReprString ports Python's %r for the strings the resume log
// lines interpolate (names, cursor values): CPython repr prefers
// single quotes and only switches to double quotes when the string
// contains a single quote but no double quote; backslash, the quote
// character and the common control characters are escaped. Plain
// config names and cursors render as 'kafka_backup', matching Python.
func pythonReprString(s string) string {
	quote := byte('\'')
	if strings.Contains(s, "'") && !strings.Contains(s, "\"") {
		quote = '"'
	}
	var out strings.Builder
	out.Grow(len(s) + 2)
	out.WriteByte(quote)
	for _, r := range s {
		switch r {
		case '\\', rune(quote):
			out.WriteByte('\\')
			out.WriteRune(r)
		case '\n':
			out.WriteString(`\n`)
		case '\r':
			out.WriteString(`\r`)
		case '\t':
			out.WriteString(`\t`)
		default:
			out.WriteRune(r)
		}
	}
	out.WriteByte(quote)
	return out.String()
}

// ResumeCursor ports the resume-cursor selection of
// journalpump.py:889-905: it starts at the reader's own cursor and
// then walks the senders IN FILE KEY ORDER — the LAST listed sender
// wins, and any sender whose sent.cursor is null aborts the walk with
// a full resync (nil) after logging "Sender %r for reader %r needs
// full sync from beginning" (journalpump.py:898-902). A nil result
// mirrors Python's None. The per-reader resuming line (909) is emitted
// after the walk on BOTH paths — the null-cursor abort only breaks the
// sender loop in Python, it does not skip the line.
//
// The malformed-sender check happens at the WALK position, exactly
// where Python's unconditional sender["sent"]["cursor"] (896) raises:
// a sender listed BEFORE a null-cursor sender (or under a reader not
// in the new config, which the caller never walks) is never touched
// and therefore harmless — decode is lenient and the error surfaces
// only if the walk actually reaches the malformed sender.
//
// The walk ends with Python's per-reader info line
// "Reader %r resuming from cursor position: %r" (journalpump.py:909).
// TODO(migration Task 17): the reader-construction call site must call
// ResumeCursor for every reader in the new config — including readers
// absent from the state file (empty ReaderState → logs
// "... position: None") — and must NOT emit that line itself, so the
// logging stays exactly where Python emits it.
func ResumeCursor(reader *ReaderState, readerName string, log Logger) (*string, error) {
	resume := reader.Cursor
	for _, sender := range reader.Senders {
		if sender.State.sentCursorMissing {
			// Python: sender["sent"]["cursor"] raises KeyError right here
			// (journalpump.py:896) and the daemon crashes — reproduce as
			// an error instead of silently treating it as a nil cursor
			// (which would trigger a full resync).
			return nil, fmt.Errorf("state file: reader %s sender %s: missing sent.cursor",
				pythonReprString(readerName), pythonReprString(sender.Name))
		}
		cursor := sender.State.Sent.Cursor
		if cursor == nil {
			if log != nil {
				log.Printf("Sender %s for reader %s needs full sync from beginning",
					pythonReprString(sender.Name), pythonReprString(readerName))
			}
			resume = nil
			break
		}
		resume = cursor
	}
	if log != nil {
		// journalpump.py:909 — emitted for every walked reader, after the
		// walk (see the Task 17 TODO above).
		cursorRepr := "None"
		if resume != nil {
			cursorRepr = pythonReprString(*resume)
		}
		log.Printf("Reader %s resuming from cursor position: %s", pythonReprString(readerName), cursorRepr)
	}
	return resume, nil
}
