// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

//go:build systest

package daemon

import (
	"os"
	"strconv"
	"time"
)

// Test-build shim, the Go analogue of the Python systest harness setting
// pump.poll_interval_ms = 100 (systest/test_rotation.py's
// journalpump_factory): Python's default is 1000 ms, and its in-process
// tests shorten it for speed. The Go binary has no such programmatic
// seam, so when built with -tags systest AND
// JOURNALPUMP_POLL_INTERVAL_MS is a positive integer, the pump cadence is
// shortened. The production binary (no systest tag) never compiles this
// file, so pollInterval stays the 1000 ms default there.
func init() {
	raw := os.Getenv("JOURNALPUMP_POLL_INTERVAL_MS")
	if raw == "" {
		return
	}
	ms, err := strconv.Atoi(raw)
	if err != nil || ms <= 0 {
		return
	}
	pollInterval = time.Duration(ms) * time.Millisecond
}
