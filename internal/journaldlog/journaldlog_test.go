// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package journaldlog

import (
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// listenDatagram opens a unixgram socket the writer under test can send
// to, returning the listener and its address.
func listenDatagram(t *testing.T) *net.UnixConn {
	t.Helper()
	path := filepath.Join(t.TempDir(), "journal.sock")
	conn, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: path, Net: "unixgram"})
	if err != nil {
		t.Fatalf("ListenUnixgram: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func readDatagram(t *testing.T, conn *net.UnixConn) string {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4096)
	n, _, err := conn.ReadFromUnix(buf)
	if err != nil {
		t.Fatalf("ReadFromUnix: %v", err)
	}
	return string(buf[:n])
}

// TestWriterSendsJournalFields locks the native-protocol datagram: one
// MESSAGE field carrying the log line plus SYSLOG_IDENTIFIER and
// PRIORITY, byte-exact for a single-line message.
func TestWriterSendsJournalFields(t *testing.T) {
	conn := listenDatagram(t)
	w := Writer{Socket: conn.LocalAddr().String(), Identifier: "journalpump", Priority: PriorityInfo}
	if _, err := w.Write([]byte("Loading configuration\n")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	got := readDatagram(t, conn)
	if !strings.Contains(got, "MESSAGE=Loading configuration\n") {
		t.Errorf("datagram missing MESSAGE field: %q", got)
	}
	if !strings.Contains(got, "SYSLOG_IDENTIFIER=journalpump\n") {
		t.Errorf("datagram missing SYSLOG_IDENTIFIER: %q", got)
	}
	if !strings.Contains(got, "PRIORITY=6\n") {
		t.Errorf("datagram missing PRIORITY: %q", got)
	}
	// The trailing newline log.Logger appends must not produce an empty
	// extra line inside MESSAGE.
	if strings.Contains(got, "MESSAGE=Loading configuration\n\n") {
		t.Errorf("MESSAGE carries a trailing blank line: %q", got)
	}
}

// TestWriterEncodesMultilineMessage locks the native-protocol multiline
// form: the field name on its own line followed by the value.
func TestWriterEncodesMultilineMessage(t *testing.T) {
	conn := listenDatagram(t)
	w := NewWriter()
	w.Socket = conn.LocalAddr().String()
	if _, err := w.Write([]byte("line one\nline two\n")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	got := readDatagram(t, conn)
	want := "MESSAGE\nline one\nline two\n"
	if !strings.Contains(got, want) {
		t.Errorf("datagram = %q, want to contain %q", got, want)
	}
}

// TestWriterDefaultsIdentifier locks NewWriter's daemon defaults
// (daemon.py:64: SYSLOG_IDENTIFIER="journalpump").
func TestWriterDefaultsIdentifier(t *testing.T) {
	w := NewWriter()
	if w.Identifier != "journalpump" {
		t.Errorf("Identifier = %q, want journalpump", w.Identifier)
	}
	if w.Socket != DefaultSocket {
		t.Errorf("Socket = %q, want %q", w.Socket, DefaultSocket)
	}
}

// TestIsTerminalPipe locks the tty probe: a pipe is not a terminal (so
// the daemon selects journald logging), matching os.isatty.
func TestIsTerminalPipe(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	defer func() { _ = w.Close() }()
	if IsTerminal(w) {
		t.Error("IsTerminal(pipe write end) = true, want false")
	}
	if IsTerminal(nil) {
		t.Error("IsTerminal(nil) = true, want false")
	}
}
