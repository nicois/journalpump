// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package config

import (
	"bytes"
	"encoding/json"
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
		return writeSortedJSON(writer, c.raw)
	}); err != nil {
		return "", fmt.Errorf("writing running config to %s: %w", path, err)
	}
	return "", nil
}

// writeSortedJSON dumps the parsed document the way Python does:
// json.dump(config, fp, indent=4, sort_keys=True) — four-space indent,
// alphabetically sorted keys (encoding/json sorts map keys byte-wise, which
// matches Python's str ordering for UTF-8), no trailing newline, and no
// HTML escaping (Python never escapes <, >, & either). Recorded deviation
// (NEWS, Task 28): the output is parsed-JSON-equal to Python's dump, not
// byte-identical — Python's ensure_ascii=True writes non-ASCII as \uXXXX
// (Go emits raw UTF-8) and Python renders floats like 60.0 as "60.0"
// (Go renders the same value as "60"); the parity tests assert
// parsed-equality, which is what the systests assert too.
func writeSortedJSON(writer io.Writer, raw map[string]any) error {
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "    ")
	if err := encoder.Encode(raw); err != nil {
		return err
	}
	// json.Encoder.Encode appends a newline; Python's json.dump does not.
	_, err := writer.Write(bytes.TrimSuffix(buf.Bytes(), []byte("\n")))
	return err
}
