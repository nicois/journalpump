// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package config

import (
	"bytes"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/Aiven-Open/journalpump/internal/util"
)

// RunningConfigPath mirrors journalpump.py:running_config_path: the
// configured json_running_config_path wins; otherwise the systemd
// RUNTIME_DIRECTORY holds running_config.json; otherwise there is nowhere
// to write (empty string).
func RunningConfigPath(configuredPath string, runtimeDirectory string) string {
	if configuredPath != "" {
		return configuredPath
	}
	if runtimeDirectory != "" {
		return filepath.Join(runtimeDirectory, "running_config.json")
	}
	return ""
}

// WarningNoRunningConfigPathWhenEnabled mirrors the warning Python logs
// when write_running_config is set but no destination can be resolved.
const WarningNoRunningConfigPathWhenEnabled = "write_running_config is set but json_running_config_path is unset and RUNTIME_DIRECTORY is unset. " +
	"Not writing running config."

// WarningRunningConfigPathWithoutEnable mirrors the warning Python logs
// when json_running_config_path is set but write_running_config is not
// true.
const WarningRunningConfigPathWithoutEnable = "json_running_config_path is set but write_running_config is not true. Not writing running config."

// WriteRunningConfigFile mirrors JournalPump.write_running_config: when the
// feature is not enabled, it returns the warning reason (if any) for the
// caller to log and writes nothing; when enabled, it resolves the
// destination, creates the parent directory with mode 0700 (existing
// directories are left untouched, like Python's exist_ok=True), and writes
// the full parsed config atomically (tmp + rename) with mode 0600.
func (c *Config) WriteRunningConfigFile() (skipReason string, err error) {
	if !c.WriteRunningConfig {
		if c.JSONRunningConfigPath != "" {
			return WarningRunningConfigPathWithoutEnable, nil
		}
		return "", nil
	}
	path := RunningConfigPath(c.JSONRunningConfigPath, os.Getenv("RUNTIME_DIRECTORY"))
	if path == "" {
		return WarningNoRunningConfigPathWhenEnabled, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", fmt.Errorf("creating directory for running config %s: %w", path, err)
	}
	if err := util.AtomicReplaceFile(path, 0o600, func(writer io.Writer) error {
		return writeJSON(writer, c.raw)
	}); err != nil {
		return "", fmt.Errorf("writing running config to %s: %w", path, err)
	}
	return "", nil
}

// writeJSON encodes the parsed document with Go 1.27's
// encoding/json/v2 encoder (user decision 2026-10-09, charter §2): a
// sanctioned deviation from Python's json.dump(indent=4,
// sort_keys=True). Kept from Python: the four-space json.dump layout,
// no trailing newline, no HTML escaping (Python never escapes <, >, &
// either), and the parsed document's equality with Python's dump.
// Go-native, order-independent by decision: object key order follows
// Go map iteration — NOT sorted and NOT deterministic across writes,
// so parsers (and tests) must compare parsed structures, never key
// order. Also Go-native: non-ASCII characters are raw UTF-8 (Python's
// ensure_ascii writes \uXXXX) and floats render Go-style (60.0 → 60).
// Recorded deviation (NEWS, Task 28); parity tests assert
// parsed-equality, which is what the systests assert too.
func writeJSON(writer io.Writer, raw map[string]any) error {
	var buf bytes.Buffer
	enc := jsontext.NewEncoder(&buf, jsontext.WithIndent("    "), jsontext.Multiline(true))
	if err := jsonv2.MarshalEncode(enc, raw); err != nil {
		return err
	}
	// The jsontext encoder terminates the value with a newline; Python's
	// json.dump does not.
	_, err := writer.Write(bytes.TrimSuffix(buf.Bytes(), []byte("\n")))
	return err
}
