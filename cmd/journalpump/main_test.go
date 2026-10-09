// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package main

import (
	"bytes"
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Aiven-Open/journalpump/internal/daemon"
	"github.com/Aiven-Open/journalpump/internal/journaldlog"
)

// runInTempDir invokes run and captures its stdout/stderr output for
// assertion.
func runInTempDir(t *testing.T, args []string) (exitCode int, stdout, stderr string) {
	t.Helper()
	var outBuf, errBuf bytes.Buffer
	code := run(args, &outBuf, &errBuf)
	return code, outBuf.String(), errBuf.String()
}

// TestRunExitCodes locks the exit-code parity with daemon.py:main:
// usage errors and startup failures exit 1, a valid config path exits 0.
func TestRunExitCodes(t *testing.T) {
	t.Run("no arguments", func(t *testing.T) {
		t.Parallel()
		code, stdout, _ := runInTempDir(t, nil)
		if code != 1 {
			t.Errorf("exit code = %d, want 1", code)
		}
		if stdout != "config file argument required\n" {
			t.Errorf("stdout = %q, want %q", stdout, "config file argument required\n")
		}
	})

	t.Run("two arguments", func(t *testing.T) {
		t.Parallel()
		code, stdout, _ := runInTempDir(t, []string{"a.json", "b.json"})
		if code != 1 {
			t.Errorf("exit code = %d, want 1", code)
		}
		if stdout != "config file argument required\n" {
			t.Errorf("stdout = %q, want %q", stdout, "config file argument required\n")
		}
	})

	t.Run("missing config file", func(t *testing.T) {
		t.Parallel()
		missing := filepath.Join(t.TempDir(), "nonexistent.json")
		code, _, stderr := runInTempDir(t, []string{missing})
		if code != 1 {
			t.Errorf("exit code = %d, want 1", code)
		}
		// Locked fixture: mirrors Python's Path!r rendering, i.e.
		// PosixPath('<path>') (daemon.py:103,137).
		want := "Journalpump failed to start: Cannot start without json config file at " +
			"PosixPath(\"" + missing + "\")\n"
		if stderr != want {
			t.Errorf("stderr = %q, want %q", stderr, want)
		}
	})

	t.Run("config path is a directory", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		code, _, stderr := runInTempDir(t, []string{dir})
		if code != 1 {
			t.Errorf("exit code = %d, want 1", code)
		}
		if !bytes.Contains([]byte(stderr), []byte("Cannot start without json config file")) {
			t.Errorf("stderr = %q, want missing-config error", stderr)
		}
	})

	t.Run("non-numeric max_send_interval exits 1 at load", func(t *testing.T) {
		t.Parallel()
		path := filepath.Join(t.TempDir(), "journalpump.json")
		bad := `{"readers": {"r": {"senders": {"s": {"output_type": "file", "file_output": "/tmp/x", "max_send_interval": "fast"}}}}}`
		if err := os.WriteFile(path, []byte(bad), 0o600); err != nil {
			t.Fatal(err)
		}
		code, _, stderr := runInTempDir(t, []string{path})
		if code != 1 {
			t.Errorf("exit code = %d, want 1 (rejected at load, never reported ready)", code)
		}
		if !bytes.Contains([]byte(stderr), []byte("max_send_interval: must be a number")) {
			t.Errorf("stderr = %q, want the max_send_interval load error", stderr)
		}
	})

	t.Run("existing config file", func(t *testing.T) {
		t.Parallel()
		path := filepath.Join(t.TempDir(), "journalpump.json")
		if err := os.WriteFile(path, []byte("{}"), 0o600); err != nil {
			t.Fatal(err)
		}
		// The run loop is real since Task 16: pre-cancel the run context
		// so the pump starts, shuts down cleanly (D9 sequence over zero
		// readers/senders) and exits 0 — the Python parity of
		// "config loads, loop exits".
		restore := overrideSignals(t, preCancelledSignals())
		defer restore()
		code, _, _ := runInTempDir(t, []string{path})
		if code != 0 {
			t.Errorf("exit code = %d, want 0", code)
		}
	})
}

// TestNewLoggerSelectsSink locks the stdout-vs-journald choice
// (daemon.py:58-70): a non-tty *os.File selects the journald sink, and a
// non-file writer (the test seam) is used as-is.
func TestNewLoggerSelectsSink(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "journal.sock")
	conn, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: sock, Net: "unixgram"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	previous := journaldlog.DefaultSocket
	journaldlog.DefaultSocket = sock
	defer func() { journaldlog.DefaultSocket = previous }()

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	defer func() { _ = w.Close() }()

	newLogger(w, journaldlog.LevelDebug).Printf("to-journal")
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4096)
	n, _, err := conn.ReadFromUnix(buf)
	if err != nil {
		t.Fatalf("non-tty file did not select the journald sink: %v", err)
	}
	if !strings.Contains(string(buf[:n]), "to-journal") {
		t.Errorf("datagram = %q, want the message", string(buf[:n]))
	}

	ttyBuf := &bytes.Buffer{}
	newLogger(ttyBuf, journaldlog.LevelDebug).Printf("to-tty")
	if !strings.Contains(ttyBuf.String(), "to-tty") {
		t.Error("non-file writer was not used as-is")
	}
}

// TestNewLoggerHonorsLevel locks the startup level wiring: a config
// log_level above INFO suppresses the INFO "Loading configuration" line.
func TestNewLoggerHonorsLevel(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "journal.sock")
	conn, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: sock, Net: "unixgram"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	previous := journaldlog.DefaultSocket
	journaldlog.DefaultSocket = sock
	defer func() { journaldlog.DefaultSocket = previous }()

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	defer func() { _ = w.Close() }()

	newLogger(w, journaldlog.LevelWarning).Printf("suppressed")
	if err := conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4096)
	if n, _, err := conn.ReadFromUnix(buf); err == nil {
		t.Fatalf("INFO line emitted at WARNING level: %q", string(buf[:n]))
	}
}

// TestRunLogsToJournaldWhenStdoutNotTTY locks the daemon's journald log
// path end to end: with a non-tty *os.File stdout, the "Loading
// configuration" line is delivered as a journal datagram carrying
// SYSLOG_IDENTIFIER=journalpump.
func TestRunLogsToJournaldWhenStdoutNotTTY(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journalpump.json")
	if err := os.WriteFile(path, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	sock := filepath.Join(t.TempDir(), "journal.sock")
	conn, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: sock, Net: "unixgram"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	previous := journaldlog.DefaultSocket
	journaldlog.DefaultSocket = sock
	defer func() { journaldlog.DefaultSocket = previous }()

	restore := overrideSignals(t, preCancelledSignals())
	defer restore()

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	defer func() { _ = w.Close() }()

	if code := run([]string{path}, w, w); code != 0 {
		t.Fatalf("exit code = %d, want 0", code)
	}
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4096)
	n, _, err := conn.ReadFromUnix(buf)
	if err != nil {
		t.Fatalf("journal datagram not received: %v", err)
	}
	got := string(buf[:n])
	if !strings.Contains(got, "Loading configuration") {
		t.Errorf("datagram = %q, want the loading log line", got)
	}
	if !strings.Contains(got, "PRIORITY=6\n") {
		t.Errorf("datagram = %q, want PRIORITY=6 (INFO)", got)
	}
	if !strings.Contains(got, "LOGGER=journalpump\n") {
		t.Errorf("datagram = %q, want LOGGER=journalpump", got)
	}
	if !strings.Contains(got, "SYSLOG_IDENTIFIER=journalpump\n") {
		t.Errorf("datagram = %q, want SYSLOG_IDENTIFIER=journalpump", got)
	}
}

// preCancelledSignals builds a Signals whose run context is already
// done — the test seam for the real ListenSignals.
func preCancelledSignals() *daemon.Signals {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return &daemon.Signals{Run: ctx, Hup: nil}
}

// overrideSignals installs a fake signal factory for the test.
func overrideSignals(t *testing.T, signals *daemon.Signals) func() {
	t.Helper()
	previous := newSignals
	newSignals = func() *daemon.Signals { return signals }
	return func() { newSignals = previous }
}
