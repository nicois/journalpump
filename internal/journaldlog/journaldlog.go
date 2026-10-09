// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

// Package journaldlog provides the native systemd-journal logging sink of
// spec §4.5: when the daemon's stdout is not a tty, log lines go to
// journald through the native datagram socket
// (/run/systemd/journal/socket) instead of stdout — the Go counterpart
// of Python's journal.JournalHandler (daemon.py:58-70).
//
// The datagram format is the journal "native protocol"
// (sd_journal_send-style, D10): a sequence of NAME=value fields, one per
// line. A value containing a newline is written in the binary field form:
// the field name, a newline, an 8-byte little-endian value length, the
// value bytes, and a trailing newline. Multi-line log messages therefore
// stay a single journal entry.
package journaldlog

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

// DefaultSocket is the well-known systemd journal datagram socket.
var DefaultSocket = "/run/systemd/journal/socket"

// Journal priority levels (syslog PRIORITY values, journal.py LOG_*).
const (
	PriorityAlert   = 1
	PriorityCrit    = 2
	PriorityErr     = 3
	PriorityWarning = 4
	PriorityNotice  = 5
	PriorityInfo    = 6
	PriorityDebug   = 7
)

// Python logging level numbers (logging._nameToLevel).
const (
	LevelDebug    = 10
	LevelInfo     = 20
	LevelWarning  = 30
	LevelError    = 40
	LevelCritical = 50
)

// Priority maps a Python logging level number to a journald priority,
// exactly like systemd.journal.JournalHandler.map_priority.
func Priority(level int) int {
	switch {
	case level <= LevelDebug:
		return PriorityDebug
	case level <= LevelInfo:
		return PriorityInfo
	case level <= LevelWarning:
		return PriorityWarning
	case level <= LevelError:
		return PriorityErr
	case level <= LevelCritical:
		return PriorityCrit
	default:
		return PriorityAlert
	}
}

// LevelName maps a Python logging level number to its uppercase name
// (logging.getLevelName for the standard levels).
func LevelName(level int) string {
	switch level {
	case LevelDebug:
		return "DEBUG"
	case LevelInfo:
		return "INFO"
	case LevelWarning:
		return "WARNING"
	case LevelError:
		return "ERROR"
	case LevelCritical:
		return "CRITICAL"
	default:
		return "Level " + strconv.Itoa(level)
	}
}

// Logger name Python uses at the root (daemon.py: self.name =
// self.__class__.__name__.lower()).
const defaultIdentifier = "journalpump"

// Writer is an io.Writer that sends each Write as one journal entry.
// Identifier is the SYSLOG_IDENTIFIER field, Priority the numeric
// PRIORITY field, and Name the optional LOGGER field (Python's
// JournalHandler passes LOGGER=record.name).
type Writer struct {
	Socket     string
	Identifier string
	Priority   int
	Name       string
}

// NewWriter builds a Writer with the daemon defaults
// (daemon.py:64: SYSLOG_IDENTIFIER=self.name == "journalpump").
func NewWriter() Writer {
	return Writer{Socket: DefaultSocket, Identifier: "journalpump", Priority: PriorityInfo}
}

// Write sends one datagram for p at the writer's default priority,
// returning len(p) on success so it satisfies io.Writer. A missing socket
// is an error; a caller that must not propagate it (logging) ignores it,
// matching the "logging never takes the daemon down" property.
func (w Writer) Write(p []byte) (int, error) {
	if err := w.Send(strings.TrimRight(string(p), "\n"), w.Priority); err != nil {
		return 0, err
	}
	return len(p), nil
}

// Send writes one journal entry with the given priority.
func (w Writer) Send(message string, priority int) error {
	socket := w.Socket
	if socket == "" {
		socket = DefaultSocket
	}
	conn, err := net.DialUnix("unixgram", nil, &net.UnixAddr{Name: socket, Net: "unixgram"})
	if err != nil {
		return err
	}
	defer conn.Close() //nolint:errcheck // datagram socket, nothing to flush
	_, err = conn.Write(w.datagram(message, priority))
	return err
}

// datagram renders the native-protocol field sequence: MESSAGE (the log
// line, newlines encoded per the protocol), SYSLOG_IDENTIFIER, PRIORITY,
// and LOGGER when a name is set.
func (w Writer) datagram(message string, priority int) []byte {
	identifier := w.Identifier
	if identifier == "" {
		identifier = defaultIdentifier
	}
	var b bytes.Buffer
	writeField(&b, "MESSAGE", message)
	fmt.Fprintf(&b, "SYSLOG_IDENTIFIER=%s\n", sanitizeFieldValue(identifier))
	fmt.Fprintf(&b, "PRIORITY=%s\n", strconv.Itoa(priority))
	if w.Name != "" {
		fmt.Fprintf(&b, "LOGGER=%s\n", sanitizeFieldValue(w.Name))
	}
	return b.Bytes()
}

// writeField writes one native-protocol field. A value containing a
// newline is written in the binary form: the field name, a newline, the
// value length as an 8-byte little-endian integer, the value, and a
// trailing newline. A single-line value uses the simple NAME=value form.
func writeField(b *bytes.Buffer, name, value string) {
	if !strings.ContainsRune(value, '\n') {
		fmt.Fprintf(b, "%s=%s\n", name, value)
		return
	}
	b.WriteString(name)
	b.WriteByte('\n')
	var size [8]byte
	binary.LittleEndian.PutUint64(size[:], uint64(len(value)))
	b.Write(size[:])
	b.WriteString(value)
	b.WriteByte('\n')
}

// sanitizeFieldValue removes the newline characters a single-line field
// value may not carry.
func sanitizeFieldValue(value string) string {
	return strings.NewReplacer("\n", " ", "\r", " ").Replace(value)
}

// IsTerminal reports whether f is a terminal — the Go port of Python's
// os.isatty(sys.stdout.fileno()) (daemon.py:59) used to pick stdout vs
// journald logging. It probes with TCGETS rather than checking the file
// mode, so a redirect to /dev/null (a char device but not a tty) still
// selects journald logging, exactly like os.isatty.
func IsTerminal(f *os.File) bool {
	if f == nil {
		return false
	}
	_, err := unix.IoctlGetTermios(int(f.Fd()), unix.TCGETS)
	return err == nil
}

// Log is the daemon's level-aware logging seam (daemon.py:configure_logging):
// it applies Python's level threshold and message formats, maps the level
// to a journald PRIORITY (see Priority), and carries the logger-name
// context (the LOGGER field and the %(name)s in the message). On a tty it
// writes to an io.Writer; otherwise it sends journal datagrams. SetLevel
// is honoured on config reload (daemon.py:reload_config sets log_level
// then reconfigures logging).
//
// Loggers derived with Named share one core (sink + level), exactly like
// Python's logging.getLogger(name) sharing the root handlers: the level
// threshold and the SYSLOG_IDENTIFIER stay the daemon's, while the LOGGER
// field and %(name)s take the component name (Python's
// JournalReader:<name>, LogSender:<reader>, MsgBuffer, ...).
type Log struct {
	core *logCore
	name string
}

// logCore is the shared state of a logger tree.
type logCore struct {
	identifier string    // SYSLOG_IDENTIFIER (daemon.py:64, self.name)
	out        io.Writer // non-nil => tty sink
	threadName string    // %(threadName)s of the multi-thread format

	mu    sync.Mutex
	level int
}

// NewJournalLog builds a journald-backed Log for the given logger name and
// minimum Python logging level. The socket is resolved per write from
// DefaultSocket so tests can redirect it.
func NewJournalLog(name string, level int) *Log {
	if name == "" {
		name = defaultIdentifier
	}
	return &Log{
		core: &logCore{identifier: defaultIdentifier, threadName: "MainThread", level: level},
		name: name,
	}
}

// NewTTYLog builds an io.Writer-backed Log with Python's tty format
// (LOG_FORMAT: asctime\tname\tlevelname\tmessage).
func NewTTYLog(name string, out io.Writer, level int) *Log {
	if name == "" {
		name = defaultIdentifier
	}
	return &Log{
		core: &logCore{identifier: defaultIdentifier, out: out, threadName: "MainThread", level: level},
		name: name,
	}
}

// Named returns a child logger with the given component name that shares
// this logger's sink and level (Python logging.getLogger(name) sharing
// the root handlers). The SYSLOG_IDENTIFIER stays the daemon's; only the
// LOGGER field and the %(name)s change.
func (l *Log) Named(name string) *Log {
	return &Log{core: l.core, name: name}
}

// Namer is implemented by loggers that can derive a named child with the
// daemon's shared sink and level. The daemon uses it to attach the
// per-component logger names Python emits.
type Namer interface {
	Named(name string) *Log
}

// SetLevel updates the minimum level (config reload). It applies to every
// logger sharing this core.
func (l *Log) SetLevel(level int) {
	l.core.mu.Lock()
	l.core.level = level
	l.core.mu.Unlock()
}

// Level reports the current minimum level.
func (l *Log) Level() int {
	l.core.mu.Lock()
	defer l.core.mu.Unlock()
	return l.core.level
}

// Printf logs at INFO (the daemon's default logging level).
func (l *Log) Printf(format string, args ...any) { l.logf(LevelInfo, format, args...) }

// Errorf logs at ERROR (Python's log.exception / log.error).
func (l *Log) Errorf(format string, args ...any) { l.logf(LevelError, format, args...) }

// Warningf logs at WARNING (Python's log.warning).
func (l *Log) Warningf(format string, args ...any) { l.logf(LevelWarning, format, args...) }

// Debugf logs at DEBUG (Python's log.debug).
func (l *Log) Debugf(format string, args ...any) { l.logf(LevelDebug, format, args...) }

// logf filters by level, then emits through the selected sink.
func (l *Log) logf(level int, format string, args ...any) {
	l.core.mu.Lock()
	threshold := l.core.level
	l.core.mu.Unlock()
	if level < threshold {
		return
	}
	message := fmt.Sprintf(format, args...)
	if l.core.out != nil {
		// LOG_FORMAT: "%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s".
		asctime := time.Now().Format("2006-01-02 15:04:05,000")
		_, _ = fmt.Fprintf(l.core.out, "%s\t%s\t%s\t%s\n", asctime, l.name, LevelName(level), message)
		return
	}
	// LOG_FORMAT_JOURNAL_MULTI_THREAD (journalpump.py:858 constructs
	// JournalPump with multi_threaded=True):
	// "%(name)-20s  %(threadName)-15s %(levelname)-8s  %(message)s".
	writer := Writer{
		Socket:     DefaultSocket,
		Identifier: l.core.identifier,
		Priority:   Priority(level),
		Name:       l.name,
	}
	_ = writer.Send(
		fmt.Sprintf("%-20s  %-15s %-8s  %s", l.name, l.core.threadName, LevelName(level), message),
		Priority(level),
	)
}
