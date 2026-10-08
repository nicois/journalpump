// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

// Command journalpump reads systemd-journald and forwards filtered entries
// to the configured senders. It is the Go rewrite of the Python journalpump
// daemon (migration plan, docs/plan-python-to-go-migration.md).
package main

import (
	"fmt"
	"io"
	"os"
)

// exitCode mirrors the exit codes of Python's journalpump.daemon.ServiceDaemon.main:
// 1 for a usage error or a startup failure, 0 for a clean run/shutdown.
const (
	exitOK  = 0
	exitErr = 1
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run mirrors journalpump/daemon.py:ServiceDaemon.main:
//
//   - exactly one argument (the config file path) is required; anything
//     else prints "config file argument required" and exits 1;
//   - a missing or non-regular config file raises "Cannot start without
//     json config file at ..." (Python ServiceDaemonError) which is
//     reported fatally and exits 1.
//
// The daemon run loop itself is wired in later migration tasks (plan
// Tasks 9-14); for now a present config file is a successful exit.
func run(args []string, stdout, stderr io.Writer) int {
	if len(args) != 1 {
		_, _ = fmt.Fprintln(stdout, "config file argument required")
		return exitErr
	}

	configPath := args[0]
	if !isRegularFile(configPath) {
		// Python: ServiceDaemonError("Cannot start without json config file at
		// {self.config_path!r}") reported via logging.fatal("%s failed to
		// start: %s", "Journalpump", ex). The path renders as pathlib's repr
		// form, e.g. PosixPath('/var/lib/journalpump/journalpump.json'),
		// reproduced here for log-parity with the Python daemon.
		_, _ = fmt.Fprintf(stderr, "Journalpump failed to start: Cannot start without json config file at PosixPath(%q)\n", configPath)
		return exitErr
	}

	return exitOK
}

// isRegularFile reports whether path names an existing regular file.
// Deviation note: Python only raises the "Cannot start" error on
// FileNotFoundError (daemon.py:101-103); a non-regular config path (e.g. a
// directory) proceeds and fails later with a different error. The exit code
// stays 1 either way; the non-regular-path failure mode is handled properly
// by the config loader (migration Task 3).
func isRegularFile(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Mode().IsRegular()
}
