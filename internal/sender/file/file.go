// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

// Package file implements the file sender: the port of
// journalpump/senders/file.py (plan Task 16). One JSON payload per line,
// appended to a regular file that is never rotated or truncated by the
// sender itself.
package file

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/Aiven-Open/journalpump/internal/sender"
)

// defaultMaxSendInterval is file.py:7's config.get("max_send_interval", 0.3).
const defaultMaxSendInterval = 0.3

// pythonBufferSize mirrors io.DEFAULT_BUFFER_SIZE (131072 on this
// platform): Python's open(path, "ab") returns a BufferedWriter with
// that buffer, so small batches are not visible on disk until the
// buffer fills. FileSender never flushes mid-run.
const pythonBufferSize = 131072

// Sender writes one payload plus a newline per message (file.py:13-18).
// The handle is opened once at construction and outlives the scope by
// design — every send writes to it (file.py:9-10). Writes go through a
// BufferedWriter-equivalent buffer (Python's open "ab" semantics); the
// only explicit flush happens on Close during the D9 shutdown grace
// window.
type Sender struct {
	output *os.File
	buf    *bufio.Writer
}

// SendMessages implements sender.Sender: append each message with a
// trailing b"\n", then the shared loop marks the batch sent. Python's
// send_messages returns True unconditionally — the only failure mode is
// the write itself raising, which surfaces here as a returned error the
// loop retries from the batch head (Python's blind except + retry of the
// same messages, duplicates included).
func (s *Sender) SendMessages(_ context.Context, batch [][]byte, _ string) error {
	for _, msg := range batch {
		// msg + b"\n" — a fresh buffer, never an in-place append to the
		// payload slice.
		line := make([]byte, 0, len(msg)+1)
		line = append(line, msg...)
		line = append(line, '\n')
		if _, err := s.buf.Write(line); err != nil {
			return fmt.Errorf("file sender: write: %w", err)
		}
	}
	return nil
}

// MaintenanceOperations implements sender.Sender; the file sender has
// none (no base.py override in file.py).
func (s *Sender) MaintenanceOperations(context.Context) error { return nil }

// Finalize flushes the buffered tail without releasing the handle. The
// supervisor calls it at shutdown BEFORE the final state save (the D9
// deviation: Python's BufferedWriter is never flushed and its buffered
// bytes are lost when the process exits on SIGTERM; Go flushes them
// within the shutdown grace window) so a failed flush is surfaced and
// the persisted sent cursor is rolled back rather than claiming the
// buffered bytes were delivered.
func (s *Sender) Finalize() error {
	return s.buf.Flush()
}

// Close flushes the buffered tail and releases the output handle.
func (s *Sender) Close() error {
	if err := s.buf.Flush(); err != nil {
		_ = s.output.Close()
		return err
	}
	return s.output.Close()
}

// New is the wiring-map constructor for output_type "file"
// (journalpump/senders/__init__.py: "file" → senders.file.FileSender).
// Construction failures — a missing or non-string file_output, an
// unopenable path, a non-numeric max_send_interval — map to Python's
// "Sender %r failed to initialize" path (file.py raises inside
// __init__, initialize_senders catches and counts the sender in the
// sender.failed_to_start persistent gauge).
func New(args sender.ConstructorArgs) (sender.Instance, error) {
	rawInterval, present := args.Config["max_send_interval"]
	seconds := defaultMaxSendInterval
	if present {
		parsed, ok := intervalSeconds(rawInterval)
		if !ok {
			// Python: a non-numeric max_send_interval reaches the run
			// loop's float comparison and crashes the daemon with
			// TypeError; contained here as an initialization failure
			// instead (failed-sender path).
			return sender.Instance{}, fmt.Errorf("max_send_interval: %v is not a number", rawInterval)
		}
		seconds = parsed
	}
	rawPath, present := args.Config["file_output"]
	if !present {
		// Python: config["file_output"] raises KeyError inside
		// FileSender.__init__.
		return sender.Instance{}, errors.New("config key 'file_output' missing")
	}
	path, ok := rawPath.(string)
	if !ok {
		// Python: open(<non-str>) raises TypeError inside __init__.
		return sender.Instance{}, fmt.Errorf("file_output: %v is not a string", rawPath)
	}
	// open(path, "ab"): append, create when missing, 0666 & umask —
	// Python's open() default mode. Parent directories are NOT created
	// (Python's open doesn't either).
	output, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o666)
	if err != nil {
		return sender.Instance{}, fmt.Errorf("opening %s: %w", path, err)
	}
	return sender.Instance{
		Transport: &Sender{output: output, buf: bufio.NewWriterSize(output, pythonBufferSize)},
		// file.py:8-11 — mark_disconnected() (already disconnected,
		// no-op) then mark_connected() right after the open.
		ConnectOnInit:   true,
		MaxSendInterval: time.Duration(seconds * float64(time.Second)),
		Type:            "FileSender",
	}, nil
}

// intervalSeconds renders a decoded JSON number as float seconds.
func intervalSeconds(value any) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case int64:
		return float64(v), true
	case json.Number:
		f, err := v.Float64()
		return f, err == nil
	default:
		return 0, false
	}
}
