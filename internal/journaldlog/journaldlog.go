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
// line. Field values containing a newline are written as the field name
// followed by the value on the following lines. Multi-line log messages
// therefore stay a single journal entry.
package journaldlog

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// DefaultSocket is the well-known systemd journal datagram socket.
var DefaultSocket = "/run/systemd/journal/socket"

// Journal priority levels (syslog PRIORITY values).
const (
	PriorityErr  = 3
	PriorityInfo = 6
)

// Writer is an io.Writer that sends each Write as one journal entry.
// Identifier is the SYSLOG_IDENTIFIER field (Python uses the daemon
// class name lowercased, "journalpump"); Priority is the numeric
// PRIORITY field.
type Writer struct {
	Socket     string
	Identifier string
	Priority   int
}

// NewWriter builds a Writer with the daemon defaults
// (daemon.py:64: SYSLOG_IDENTIFIER=self.name == "journalpump").
func NewWriter() Writer {
	return Writer{Socket: DefaultSocket, Identifier: "journalpump", Priority: PriorityInfo}
}

// Write sends one datagram for p, returning len(p) on success so it
// satisfies io.Writer. A missing socket is an error; log.Logger ignores
// it, matching the "logging must never take the daemon down" property.
func (w Writer) Write(p []byte) (int, error) {
	socket := w.Socket
	if socket == "" {
		socket = DefaultSocket
	}
	conn, err := net.DialUnix("unixgram", nil, &net.UnixAddr{Name: socket, Net: "unixgram"})
	if err != nil {
		return 0, err
	}
	defer conn.Close() //nolint:errcheck // datagram socket, nothing to flush
	if _, err := conn.Write(w.datagram(p)); err != nil {
		return 0, err
	}
	return len(p), nil
}

// datagram renders the native-protocol field sequence: MESSAGE (the log
// line, newlines encoded per the protocol), SYSLOG_IDENTIFIER and
// PRIORITY. The trailing newline log.Logger appends is stripped so the
// journal entry does not gain an empty line.
func (w Writer) datagram(p []byte) []byte {
	message := strings.TrimRight(string(p), "\n")
	identifier := w.Identifier
	if identifier == "" {
		identifier = "journalpump"
	}
	var b strings.Builder
	writeField(&b, "MESSAGE", message)
	fmt.Fprintf(&b, "SYSLOG_IDENTIFIER=%s\n", sanitizeFieldValue(identifier))
	fmt.Fprintf(&b, "PRIORITY=%s\n", strconv.Itoa(w.Priority))
	return []byte(b.String())
}

// writeField writes one native-protocol field. Newlines in the value are
// encoded as the field name on its own line followed by the value
// (journal's binary-safe multiline form); an embedded NUL would need the
// binary form, which log lines never carry.
func writeField(b *strings.Builder, name, value string) {
	if !strings.ContainsRune(value, '\n') {
		fmt.Fprintf(b, "%s=%s\n", name, value)
		return
	}
	fmt.Fprintf(b, "%s\n%s\n", name, value)
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
