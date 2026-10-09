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
	"io"
	"os"
	"sync"
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
// only explicit flush happens at Finalize/Close, and the buffer also
// auto-flushes when it fills (bufio semantics, like Python's
// BufferedWriter).
//
// Delivery checkpoint (Task 17, user-approved deviation): the sender
// tracks, per accepted line, the raw byte end and the cumulative payload
// byte end, and a counting writer over the real file reports the raw
// bytes actually flushed. FlushedPayloadBytes maps the raw watermark back
// to a payload-byte watermark (a prefix — lines are written in order, so
// if a line's bytes are flushed, every earlier line's are too), which the
// shared loop turns into the persisted sent cursor. The result: the state
// file's cursor reflects DELIVERY (bytes on disk), not send() completion.
type Sender struct {
	output outputFile
	buf    *bufio.Writer
	count  *countingWriter

	// mu guards buf, count.n and the flush queue (SendMessages runs on the
	// loop goroutine; State, Finalize and Close run on the supervisor).
	mu sync.Mutex
	// queue holds the not-yet-flushed line marks, oldest first.
	queue []flushMark
	// rawEnd and payloadEnd are cumulative totals of accepted lines (raw
	// bytes including the newline; payload bytes excluding it).
	rawEnd     int64
	payloadEnd int64
	// flushedPayload is the payload watermark derived from count.n.
	flushedPayload int64
	// boundary is the raw byte offset of the last COMPLETE line written
	// to the file. A bufio auto-flush can end in the middle of a line, so
	// the file may hold a partial line; discarding or a failed flush
	// truncates back to boundary, guaranteeing the file only ever ends on
	// a complete-line edge (replay then appends a clean line).
	boundary int64
	// base is the file size at construction (the append offset). All the
	// byte counters below are RUN-relative (they start at zero for this
	// handle), so an absolute Truncate needs base + the run-relative
	// boundary. Without this a retire that has not written anything yet
	// would truncate the file to zero, deleting previously delivered lines.
	base int64
	// repairPending records that a discard truncate failed: a partial line
	// still sits past boundary on disk and the handle was left open for a
	// retry. Finalize and Close honor it (Close refuses to release an
	// unrepaired handle) so a shutdown after a failed reload cleanup cannot
	// let a restart append onto the corrupt prefix.
	repairPending bool
}

// outputFile is the output-handle subset the Sender uses: the *os.File an
// "ab" open returns, or a test double that can fail a write/truncate.
type outputFile interface {
	io.Writer
	Truncate(size int64) error
	Close() error
	Stat() (os.FileInfo, error)
}

// flushMark is one accepted line's cumulative byte ends.
type flushMark struct {
	rawEnd     int64
	payloadEnd int64
}

// countingWriter counts the raw bytes handed to the underlying writer.
type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
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
		s.mu.Lock()
		_, err := s.buf.Write(line)
		if err == nil {
			s.rawEnd += int64(len(line))
			s.payloadEnd += int64(len(msg))
			s.queue = append(s.queue, flushMark{rawEnd: s.rawEnd, payloadEnd: s.payloadEnd})
		}
		s.mu.Unlock()
		if err != nil {
			return fmt.Errorf("file sender: write: %w", err)
		}
	}
	return nil
}

// MaintenanceOperations implements sender.Sender; the file sender has
// none (no base.py override in file.py).
func (s *Sender) MaintenanceOperations(context.Context) error { return nil }

// FlushedPayloadBytes implements sender.DurableReporter: the payload
// bytes whose lines have reached the file (a prefix watermark). It
// advances when the buffer auto-flushes on fill and on a successful
// Finalize.
func (s *Sender) FlushedPayloadBytes() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flushedLocked()
}

// flushedLocked advances flushedPayload over the queued line marks whose
// raw bytes have been flushed, dropping the consumed marks. Caller holds
// s.mu.
func (s *Sender) flushedLocked() int64 {
	n := s.count.n
	for len(s.queue) > 0 && s.queue[0].rawEnd <= n {
		s.flushedPayload = s.queue[0].payloadEnd
		s.boundary = s.queue[0].rawEnd
		s.queue = s.queue[1:]
	}
	return s.flushedPayload
}

// discardUnflushedLocked drops the buffered, not-yet-written bytes plus
// the unflushed tail, and truncates any partially-written line back to the
// last complete-line boundary, so the file ends on a clean edge. Caller
// holds s.mu.
func (s *Sender) discardUnflushedLocked() error {
	// Settle the complete-line boundary from what actually reached disk
	// (while the queue still holds the marks), then remove any partial
	// line past it.
	s.flushedLocked()
	// Drop buffered, unwritten bytes (bufio.Reset does not write them).
	s.buf.Reset(s.count)
	s.queue = nil
	return s.retryRepairLocked()
}

// retryRepairLocked truncates the output back to the last complete-line
// boundary (base + the run-relative boundary) and clears the pending-repair
// flag; on failure it keeps the flag set and returns the error so Finalize
// and Close surface an unrepaired partial line. Caller holds s.mu.
func (s *Sender) retryRepairLocked() error {
	if err := s.output.Truncate(s.base + s.boundary); err != nil {
		s.repairPending = true
		return err
	}
	s.repairPending = false
	s.count.n = s.boundary
	return nil
}

// Finalize flushes the buffered tail without releasing the handle. The
// supervisor calls it at shutdown BEFORE the final state save (the D9
// deviation: Python's BufferedWriter is never flushed and its buffered
// bytes are lost when the process exits on SIGTERM; Go flushes them
// within the shutdown grace window). A successful flush advances
// FlushedPayloadBytes so the persisted cursor catches up to the full sent
// position; a failed flush leaves the durable cursor at the last flushed
// boundary (the tail is replayed on restart).
//
// When an earlier discard left an unrepaired partial line (repairPending),
// Finalize retries the truncation FIRST — a SIGTERM after a failed reload
// cleanup must not let the corrupt prefix survive into the next run — and
// surfaces an error if it still cannot be repaired.
func (s *Sender) Finalize() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.repairPending {
		return s.retryRepairLocked()
	}
	err := s.buf.Flush()
	if err != nil {
		// A partial line may have reached the file before the failure:
		// roll back to the last complete-line boundary so a restart's
		// replay appends a clean line rather than corrupting this one.
		return errors.Join(err, s.discardUnflushedLocked())
	}
	_ = s.flushedLocked()
	return nil
}

// Close flushes the buffered tail and releases the output handle. Like
// Finalize it first retries any pending boundary repair, and it will NOT
// release the handle while a partial line remains unrepaired: silently
// closing would leave a corrupt prefix a restart would append onto, so the
// error is returned instead.
func (s *Sender) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.repairPending {
		if err := s.retryRepairLocked(); err != nil {
			return err
		}
	}
	if err := s.buf.Flush(); err != nil {
		_ = s.output.Close()
		return err
	}
	return s.output.Close()
}

// CloseDiscarding implements sender.Discarder: it drops the buffered,
// not-yet-flushed tail and releases the handle, so a reload retire does
// not persist lines the delivery checkpoint never claimed (Python
// request_stop drops the old sender's buffered messages identically).
//
// When the truncate fails the handle is deliberately LEFT OPEN and the
// pending repair is recorded (repairPending): the partial line is still on
// disk, so a later retire's retry — or Finalize/Close at shutdown — must be
// able to truncate it. If the handle were closed here no retry could
// truncate, so the output would be blocked (or corrupted) forever.
func (s *Sender) CloseDiscarding() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.discardUnflushedLocked(); err != nil {
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
	// open(path, "ab") appends; record where this handle starts so an
	// absolute truncate can preserve pre-existing complete lines.
	info, err := output.Stat()
	if err != nil {
		_ = output.Close()
		return sender.Instance{}, fmt.Errorf("stat %s: %w", path, err)
	}
	return sender.Instance{
		Transport: newSender(output, info.Size()),
		// file.py:8-11 — mark_disconnected() (already disconnected,
		// no-op) then mark_connected() right after the open.
		ConnectOnInit:   true,
		MaxSendInterval: time.Duration(seconds * float64(time.Second)),
		Type:            "FileSender",
	}, nil
}

// newSender wires a Sender around an output handle; base is the handle's
// current size (the append offset).
func newSender(output outputFile, base int64) *Sender {
	count := &countingWriter{w: output}
	return &Sender{
		output: output,
		buf:    bufio.NewWriterSize(count, pythonBufferSize),
		count:  count,
		base:   base,
	}
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
