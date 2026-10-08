// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

// Package util holds small shared helpers ported from journalpump/util.py.
package util

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// AtomicReplaceFile mirrors journalpump/util.py:atomic_replace_file: it
// creates a unique temporary file in the target file's directory, calls
// write to produce the content, and renames the temporary file over the
// target. A non-zero mode is applied before writing (Python's fchmod); the
// temporary file is removed if writing fails.
func AtomicReplaceFile(path string, mode os.FileMode, write func(io.Writer) error) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".*.tmp")
	if err != nil {
		return fmt.Errorf("creating temp file for %s: %w", path, err)
	}
	tmpPath := tmp.Name()
	keep := false
	defer func() {
		if !keep {
			_ = os.Remove(tmpPath)
		}
	}()

	if mode != 0 {
		if err := tmp.Chmod(mode); err != nil {
			_ = tmp.Close()
			return fmt.Errorf("chmod %s: %w", tmpPath, err)
		}
	}

	if err := write(tmp); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("writing %s: %w", tmpPath, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("closing %s: %w", tmpPath, err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("renaming %s to %s: %w", tmpPath, path, err)
	}
	keep = true
	return nil
}
