// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// TestRunningConfigPath parity-ports test/test_running_config.py's four
// path-resolution tests.
func TestRunningConfigPath(t *testing.T) {
	t.Parallel()
	t.Run("configured wins over RUNTIME_DIRECTORY", func(t *testing.T) {
		t.Parallel()
		got := RunningConfigPath("/configured/running.json", "/runtime")
		if got != "/configured/running.json" {
			t.Errorf("path = %q, want /configured/running.json", got)
		}
	})
	t.Run("RUNTIME_DIRECTORY fallback", func(t *testing.T) {
		t.Parallel()
		got := RunningConfigPath("", "/runtime")
		if got != filepath.Join("/runtime", "running_config.json") {
			t.Errorf("path = %q, want /runtime/running_config.json", got)
		}
	})
	t.Run("both unset", func(t *testing.T) {
		t.Parallel()
		if got := RunningConfigPath("", ""); got != "" {
			t.Errorf("path = %q, want empty", got)
		}
	})
}

// TestWriteRunningConfigSkips parity-ports
// test/test_running_config.py::test_write_running_config_warns_when_path_set_without_enable.
func TestWriteRunningConfigSkips(t *testing.T) {
	t.Parallel()
	t.Run("path set without enable warns", func(t *testing.T) {
		t.Parallel()
		notWritten := filepath.Join(t.TempDir(), "running.json")
		cfg := mustDecode(t, `{"json_running_config_path": "`+notWritten+`"}`)
		reason, err := cfg.WriteRunningConfigFile()
		if err != nil {
			t.Fatalf("WriteRunningConfig failed: %v", err)
		}
		want := "json_running_config_path is set but write_running_config is not true. Not writing running config."
		if reason != want {
			t.Errorf("skip reason = %q, want %q", reason, want)
		}
		if _, err := os.Stat(notWritten); err == nil {
			t.Error("running config must not be written without opt-in")
		}
	})
	t.Run("enabled without path warns", func(t *testing.T) {
		t.Parallel()
		cfg := mustDecode(t, `{"write_running_config": true}`)
		reason, err := cfg.WriteRunningConfigFile()
		if err != nil {
			t.Fatalf("WriteRunningConfig failed: %v", err)
		}
		want := "write_running_config is set but json_running_config_path is unset and RUNTIME_DIRECTORY is unset. " +
			"Not writing running config."
		if reason != want {
			t.Errorf("skip reason = %q, want %q", reason, want)
		}
	})
	t.Run("not enabled and no path is silent", func(t *testing.T) {
		t.Parallel()
		cfg := mustDecode(t, `{}`)
		reason, err := cfg.WriteRunningConfigFile()
		if err != nil || reason != "" {
			t.Errorf("skip reason = %q, err = %v, want empty/nil", reason, err)
		}
	})
}

// TestWriteRunningConfigWrites locks the writer behavior: the full
// parsed document (including unknown keys) round-trips parsed-equal,
// the four-space Python-style indentation is kept, there is no
// trailing newline, the file mode is 0600, and the parent directory is
// created with mode 0700 (existing directories untouched, like
// Python's exist_ok=True). Key order is deliberately NOT asserted: the
// writer is Go-native (user decision 2026-10-09, charter §2) and its
// map key order is nondeterministic across writes — tests compare
// parsed structures, never bytes or key sequences.
func TestWriteRunningConfigWrites(t *testing.T) {
	t.Parallel()
	t.Run("full parsed document with modes", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		target := filepath.Join(dir, "nested", "deeper", "running.json")
		input := `{
            "b": {"x": [1, 2.5, null], "y": {}},
            "a": 1,
            "write_running_config": true,
            "json_running_config_path": "` + target + `",
            "unknown_key": {"kept": [true]}
        }`
		cfg := mustDecode(t, input)
		reason, err := cfg.WriteRunningConfigFile()
		if err != nil || reason != "" {
			t.Fatalf("WriteRunningConfig = %q, %v", reason, err)
		}
		content, err := os.ReadFile(target)
		if err != nil {
			t.Fatalf("reading running config: %v", err)
		}
		// Parsed equality with the input document (order-independent).
		var want any
		if err := json.Unmarshal([]byte(input), &want); err != nil {
			t.Fatalf("parsing input document: %v", err)
		}
		var got any
		if err := json.Unmarshal(content, &got); err != nil {
			t.Fatalf("parsing written running config: %v", err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("running config parses differently:\ngot:\n%s\nwant:\n%s", content, input)
		}
		// Python-parity shape: four-space indentation, no trailing
		// newline (Python's json.dump appends none).
		if !strings.Contains(string(content), "\n    \"") {
			t.Errorf("running config is not four-space indented:\n%s", content)
		}
		if strings.HasSuffix(string(content), "\n") {
			t.Errorf("running config has a trailing newline (Python's json.dump has none):\n%s", content)
		}
		info, err := os.Stat(target)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o600 {
			t.Errorf("running config mode = %o, want 600", info.Mode().Perm())
		}
		deeper, err := os.Stat(filepath.Join(dir, "nested", "deeper"))
		if err != nil {
			t.Fatal(err)
		}
		if deeper.Mode().Perm() != 0o700 {
			t.Errorf("created directory mode = %o, want 700", deeper.Mode().Perm())
		}
	})
	t.Run("existing directory is not chmodded", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		runningDir := filepath.Join(dir, "running")
		if err := os.Mkdir(runningDir, 0o755); err != nil {
			t.Fatal(err)
		}
		target := filepath.Join(runningDir, "config.json")
		cfg := mustDecode(t, `{"write_running_config": true, "json_running_config_path": "`+target+`"}`)
		if _, err := cfg.WriteRunningConfigFile(); err != nil {
			t.Fatalf("WriteRunningConfig failed: %v", err)
		}
		info, err := os.Stat(runningDir)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o755 {
			t.Errorf("existing directory mode = %o, want 755 (Python exist_ok=True)", info.Mode().Perm())
		}
	})
}

// TestWriteRunningConfigRuntimeDirectory locks the RUNTIME_DIRECTORY
// fallback. It is a separate non-parallel test because t.Setenv cannot run
// in (or under) a parallel test.
func TestWriteRunningConfigRuntimeDirectory(t *testing.T) {
	runtimeDir := t.TempDir()
	t.Setenv("RUNTIME_DIRECTORY", runtimeDir)
	cfg := mustDecode(t, `{"write_running_config": true}`)
	if _, err := cfg.WriteRunningConfigFile(); err != nil {
		t.Fatalf("WriteRunningConfigFile failed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(runtimeDir, "running_config.json")); err != nil {
		t.Errorf("RUNTIME_DIRECTORY/running_config.json not written: %v", err)
	}
}
