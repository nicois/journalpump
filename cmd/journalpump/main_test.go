// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/Aiven-Open/journalpump/internal/daemon"
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
