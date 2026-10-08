// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// runInTempDir isolates config-path lookups from the host filesystem.
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
		code, stdout, _ := runInTempDir(t, nil)
		if code != 1 {
			t.Errorf("exit code = %d, want 1", code)
		}
		if stdout != "config file argument required\n" {
			t.Errorf("stdout = %q, want %q", stdout, "config file argument required\n")
		}
	})

	t.Run("two arguments", func(t *testing.T) {
		code, stdout, _ := runInTempDir(t, []string{"a.json", "b.json"})
		if code != 1 {
			t.Errorf("exit code = %d, want 1", code)
		}
		if stdout != "config file argument required\n" {
			t.Errorf("stdout = %q, want %q", stdout, "config file argument required\n")
		}
	})

	t.Run("missing config file", func(t *testing.T) {
		missing := filepath.Join(t.TempDir(), "nonexistent.json")
		code, _, stderr := runInTempDir(t, []string{missing})
		if code != 1 {
			t.Errorf("exit code = %d, want 1", code)
		}
		want := "Journalpump failed to start: Cannot start without json config file at " +
			"\"" + missing + "\"\n"
		if stderr != want {
			t.Errorf("stderr = %q, want %q", stderr, want)
		}
	})

	t.Run("config path is a directory", func(t *testing.T) {
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
		path := filepath.Join(t.TempDir(), "journalpump.json")
		if err := os.WriteFile(path, []byte("{}"), 0o600); err != nil {
			t.Fatal(err)
		}
		code, _, _ := runInTempDir(t, []string{path})
		if code != 0 {
			t.Errorf("exit code = %d, want 0", code)
		}
	})
}