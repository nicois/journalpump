// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package journaldlog

import (
	"bytes"
	"encoding/binary"
	"fmt"
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

// TestWriterEncodesMultilineMessage locks the native-protocol binary
// field form for a value containing newlines: the field name, a newline,
// an 8-byte little-endian value length, the value, and a trailing newline
// (sd_journal_send / journal-send.c).
func TestWriterEncodesMultilineMessage(t *testing.T) {
	conn := listenDatagram(t)
	w := NewWriter()
	w.Socket = conn.LocalAddr().String()
	if _, err := w.Write([]byte("line one\nline two\n")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	got := readDatagram(t, conn)
	value := "line one\nline two"
	var size [8]byte
	binary.LittleEndian.PutUint64(size[:], uint64(len(value)))
	want := "MESSAGE\n" + string(size[:]) + value + "\n"
	if !strings.Contains(got, want) {
		t.Errorf("datagram = %q, want to contain the length-prefixed MESSAGE %q", got, want)
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

// TestPriorityMapping locks systemd.journal.JournalHandler.map_priority,
// including the "sparse" levels between the standard ones.
func TestPriorityMapping(t *testing.T) {
	cases := []struct {
		level int
		want  int
	}{
		{LevelDebug, PriorityDebug},
		{LevelDebug - 1, PriorityDebug},
		{LevelInfo, PriorityInfo},
		{LevelInfo + 5, PriorityWarning}, // 25 <= WARNING: map_priority rounds up
		{LevelWarning, PriorityWarning},
		{LevelError, PriorityErr},
		{LevelCritical, PriorityCrit},
		{LevelCritical + 1, PriorityAlert},
	}
	for _, tc := range cases {
		if got := Priority(tc.level); got != tc.want {
			t.Errorf("Priority(%d) = %d, want %d", tc.level, got, tc.want)
		}
	}
}

// TestLogJournalFormatAndPriority locks the journald sink of the
// level-aware Log: Python's LOG_FORMAT_JOURNAL_MULTI_THREAD message
// (journalpump is constructed multi_threaded=True, so name padded to 20,
// threadName padded to 15, level padded to 8), the mapped PRIORITY, the
// LOGGER field, and SYSLOG_IDENTIFIER.
func TestLogJournalFormatAndPriority(t *testing.T) {
	conn := listenDatagram(t)
	previous := DefaultSocket
	DefaultSocket = conn.LocalAddr().String()
	t.Cleanup(func() { DefaultSocket = previous })

	log := NewJournalLog("journalpump", LevelDebug)
	log.Errorf("boom %d", 7)
	got := readDatagram(t, conn)
	wantMessage := fmt.Sprintf("%-20s  %-15s %-8s  %s", "journalpump", "MainThread", "ERROR", "boom 7")
	if !strings.Contains(got, "MESSAGE="+wantMessage+"\n") {
		t.Errorf("MESSAGE = %q, want %q", got, wantMessage)
	}
	if !strings.Contains(got, "PRIORITY=3\n") {
		t.Errorf("datagram = %q, want PRIORITY=3 (ERROR)", got)
	}
	if !strings.Contains(got, "LOGGER=journalpump\n") {
		t.Errorf("datagram = %q, want LOGGER=journalpump", got)
	}
	if !strings.Contains(got, "SYSLOG_IDENTIFIER=journalpump\n") {
		t.Errorf("datagram = %q, want SYSLOG_IDENTIFIER", got)
	}
}

// TestLogNamedChildLocks the per-component logger context: Named derives
// a child that shares the core (SYSLOG_IDENTIFIER, level) but emits the
// component name as LOGGER and in the message, like Python's
// logging.getLogger("JournalReader:r").
func TestLogNamedChild(t *testing.T) {
	conn := listenDatagram(t)
	previous := DefaultSocket
	DefaultSocket = conn.LocalAddr().String()
	t.Cleanup(func() { DefaultSocket = previous })

	root := NewJournalLog("journalpump", LevelDebug)
	child := root.Named("JournalReader:r")
	// SetLevel on the root applies to the child too (shared core).
	root.SetLevel(LevelWarning)
	child.Printf("dropped-info") // suppressed via the shared level
	child.Warningf("kept %d", 1)
	got := readDatagram(t, conn)
	if strings.Contains(got, "dropped-info") {
		t.Fatalf("child did not share the root level: %q", got)
	}
	wantMessage := fmt.Sprintf("%-20s  %-15s %-8s  %s", "JournalReader:r", "MainThread", "WARNING", "kept 1")
	if !strings.Contains(got, "MESSAGE="+wantMessage+"\n") {
		t.Errorf("MESSAGE = %q, want %q", got, wantMessage)
	}
	if !strings.Contains(got, "LOGGER=JournalReader:r\n") {
		t.Errorf("datagram = %q, want LOGGER=JournalReader:r", got)
	}
	if !strings.Contains(got, "SYSLOG_IDENTIFIER=journalpump\n") {
		t.Errorf("datagram = %q, want SYSLOG_IDENTIFIER=journalpump (root identifier)", got)
	}
}

// TestLogLevelFilteringAndReload locks the level threshold and its
// update: a message below the minimum is dropped, and raising the level
// (config reload) suppresses later INFO lines.
func TestLogLevelFilteringAndReload(t *testing.T) {
	var buf bytes.Buffer
	log := NewTTYLog("journalpump", &buf, LevelWarning)
	log.Printf("info-line")   // below WARNING: dropped
	log.Warningf("warn-line") // kept
	if strings.Contains(buf.String(), "info-line") {
		t.Errorf("INFO line emitted below the WARNING threshold: %q", buf.String())
	}
	if !strings.Contains(buf.String(), "warn-line") {
		t.Errorf("WARNING line dropped: %q", buf.String())
	}

	log.SetLevel(LevelDebug)
	log.Printf("later-info")
	if !strings.Contains(buf.String(), "later-info") {
		t.Errorf("INFO line dropped after SetLevel(DEBUG): %q", buf.String())
	}
}

// TestLogTTYFormat locks Python's LOG_FORMAT on the tty sink:
// asctime\tname\tlevelname\tmessage.
func TestLogTTYFormat(t *testing.T) {
	var buf bytes.Buffer
	log := NewTTYLog("journalpump", &buf, LevelDebug)
	log.Printf("hello")
	line := strings.TrimRight(buf.String(), "\n")
	parts := strings.Split(line, "\t")
	if len(parts) != 4 {
		t.Fatalf("line = %q, want 4 tab-separated fields", line)
	}
	if parts[1] != "journalpump" || parts[2] != "INFO" || parts[3] != "hello" {
		t.Errorf("fields = %v, want [_, journalpump, INFO, hello]", parts)
	}
}
