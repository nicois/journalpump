// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

//go:build systest

package systestgo

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// writeConfig writes a config document and returns its path.
func writeConfig(t *testing.T, dir string, config map[string]any) string {
	t.Helper()
	path := filepath.Join(dir, "journalpump.json")
	raw, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func assertPrivateRunningConfig(t *testing.T, path string, config map[string]any) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat running config: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Errorf("running config mode = %o, want 0600", got)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read running config: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("parse running config: %v", err)
	}
	if !reflect.DeepEqual(got, config) {
		t.Errorf("running config = %v, want %v", got, config)
	}
}

// TestGoWritesRunningConfigToRuntimeDirectory ports
// test_process_writes_running_config_to_runtime_directory (plan Task 10):
// the Go binary writes the parsed config, 0600, into RUNTIME_DIRECTORY.
func TestGoWritesRunningConfigToRuntimeDirectory(t *testing.T) {
	config := map[string]any{"write_running_config": true, "readers": map[string]any{"reader": map[string]any{"senders": map[string]any{}}}}
	path := writeConfig(t, t.TempDir(), config)
	p := startJournalpump(t, path)
	assertPrivateRunningConfig(t, p.runningConfigPath(), config)
}

// TestGoWritesRunningConfigWithSecretFilters ports
// test_process_writes_running_config_with_secret_filters.
func TestGoWritesRunningConfigWithSecretFilters(t *testing.T) {
	config := map[string]any{
		"write_running_config": true,
		"readers": map[string]any{
			"host": map[string]any{
				"senders": map[string]any{},
				"secret_filters": []any{
					map[string]any{"pattern": "AVNS_[A-Za-z0-9-_]{15,123}", "replacement": "[REDACTED]"},
				},
			},
		},
	}
	path := writeConfig(t, t.TempDir(), config)
	p := startJournalpump(t, path)
	assertPrivateRunningConfig(t, p.runningConfigPath(), config)
}

// TestGoDoesNotWriteRunningConfigWithoutOptIn ports
// test_process_does_not_write_running_config_without_opt_in.
func TestGoDoesNotWriteRunningConfigWithoutOptIn(t *testing.T) {
	config := map[string]any{"readers": map[string]any{"reader": map[string]any{"senders": map[string]any{}}}}
	path := writeConfig(t, t.TempDir(), config)
	p := startJournalpump(t, path)
	if _, err := os.Stat(p.runningConfigPath()); !os.IsNotExist(err) {
		t.Errorf("running config written without opt-in: err = %v", err)
	}
}

// TestGoDoesNotWriteRunningConfigFromPathAlone ports
// test_process_does_not_write_running_config_from_path_alone.
func TestGoDoesNotWriteRunningConfigFromPathAlone(t *testing.T) {
	dir := t.TempDir()
	running := filepath.Join(dir, "running", "config.json")
	config := map[string]any{
		"json_running_config_path": running,
		"readers":                  map[string]any{"reader": map[string]any{"senders": map[string]any{}}},
	}
	path := writeConfig(t, dir, config)
	p := startJournalpump(t, path)
	if _, err := os.Stat(running); !os.IsNotExist(err) {
		t.Errorf("running config written from path alone: err = %v", err)
	}
	if _, err := os.Stat(p.runningConfigPath()); !os.IsNotExist(err) {
		t.Errorf("runtime-dir running config written from path alone: err = %v", err)
	}
}

// TestGoDoesNotWriteRunningConfigWhenEnabledWithoutPath ports
// test_process_does_not_write_running_config_when_enabled_without_path.
func TestGoDoesNotWriteRunningConfigWhenEnabledWithoutPath(t *testing.T) {
	config := map[string]any{"write_running_config": true, "readers": map[string]any{"reader": map[string]any{"senders": map[string]any{}}}}
	path := writeConfig(t, t.TempDir(), config)
	p := startJournalpump(t, path, withoutRuntimeDirectory())
	if _, err := os.Stat(p.runningConfigPath()); !os.IsNotExist(err) {
		t.Errorf("running config written without RUNTIME_DIRECTORY: err = %v", err)
	}
}

// TestGoWritesRunningConfigToConfiguredPath ports
// test_process_writes_running_config_to_configured_path, including the
// private 0700 parent directory.
func TestGoWritesRunningConfigToConfiguredPath(t *testing.T) {
	dir := t.TempDir()
	running := filepath.Join(dir, "running", "config.json")
	config := map[string]any{
		"write_running_config":     true,
		"json_running_config_path": running,
		"readers":                  map[string]any{"reader": map[string]any{"senders": map[string]any{}}},
	}
	path := writeConfig(t, dir, config)
	p := startJournalpump(t, path)
	assertPrivateRunningConfig(t, running, config)
	info, err := os.Stat(filepath.Dir(running))
	if err != nil {
		t.Fatalf("stat running dir: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o700 {
		t.Errorf("running dir mode = %o, want 0700", got)
	}
	if _, err := os.Stat(p.runningConfigPath()); !os.IsNotExist(err) {
		t.Errorf("runtime-dir running config written despite explicit path: err = %v", err)
	}
}

// TestGoDoesNotChmodExistingRunningConfigDirectory ports
// test_process_does_not_chmod_existing_running_config_directory.
func TestGoDoesNotChmodExistingRunningConfigDirectory(t *testing.T) {
	dir := t.TempDir()
	runningDir := filepath.Join(dir, "running")
	if err := os.Mkdir(runningDir, 0o755); err != nil {
		t.Fatal(err)
	}
	running := filepath.Join(runningDir, "config.json")
	config := map[string]any{
		"write_running_config":     true,
		"json_running_config_path": running,
		"readers":                  map[string]any{"reader": map[string]any{"senders": map[string]any{}}},
	}
	path := writeConfig(t, dir, config)
	startJournalpump(t, path)
	assertPrivateRunningConfig(t, running, config)
	info, err := os.Stat(runningDir)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o755 {
		t.Errorf("existing running dir mode = %o, want unchanged 0755", got)
	}
}
