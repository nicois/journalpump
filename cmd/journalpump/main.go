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
	"log"
	"os"

	"github.com/Aiven-Open/journalpump/internal/config"
	"github.com/Aiven-Open/journalpump/internal/daemon"
	"github.com/Aiven-Open/journalpump/internal/sender"
	"github.com/Aiven-Open/journalpump/internal/sender/file"
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

// newSignals is the signal seam: the real binary installs
// ListenSignals (Run context from NotifyContext over SIGTERM/SIGINT —
// cancellation IS shutdown — plus the SIGHUP event channel).
// Tests replace it with a pre-cancelled Signals to drive a clean
// immediate exit through the same path.
var newSignals = daemon.ListenSignals

// run mirrors journalpump/daemon.py:ServiceDaemon.main:
//
//   - exactly one argument (the config file path) is required; anything
//     else prints "config file argument required" and exits 1;
//   - a missing or non-regular config file raises "Cannot start without
//     json config file at ..." (Python ServiceDaemonError) which is
//     reported fatally and exits 1;
//   - the first config load notifies RELOADING=1 ("Reloading"/"Loading
//     configuration", daemon.py:105-114), the pump is constructed
//     (handle_new_config — construction errors are startup failures),
//     READY=1 is notified once by the config load and once by main
//     (Python notifies twice at startup), and the run loop drives until
//     SIGTERM/SIGINT.
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

	notifier := daemon.SocketNotifier{}
	// First config load (daemon.py:105-114): RELOADING=1.
	if _, err := notifier.Notify(daemon.StateReloading); err != nil {
		_, _ = fmt.Fprintf(stderr, "Journalpump failed to start: %v\n", err)
		return exitErr
	}
	cfg, err := config.Load(configPath)
	if err != nil {
		// Python: json.load errors propagate uncaught (traceback, exit 1).
		_, _ = fmt.Fprintf(stderr, "Journalpump failed to start: %v\n", err)
		return exitErr
	}
	// Python logs "Loading configuration" (daemon.py:107) through the
	// logging module — stdout in Task 16 (journald logging is Task 17).
	logger := log.New(os.Stdout, "", log.LstdFlags)
	logger.Printf("Loading configuration")
	registerSenders()
	pump, err := daemon.NewPump(cfg, daemon.PumpDeps{Log: logger})
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "Journalpump failed to start: %v\n", err)
		return exitErr
	}
	// daemon.py:114 + main(): READY=1 twice at startup (parity).
	for i := 0; i < 2; i++ {
		if _, err := notifier.Notify(daemon.StateReady); err != nil {
			_, _ = fmt.Fprintf(stderr, "Journalpump failed to start: %v\n", err)
			return exitErr
		}
	}

	signals := newSignals()
	defer signals.Close()
	if err := pump.Run(signals.Run, signals.Hup); err != nil {
		// Runtime failures (reader open errors, watchdog notify errors):
		// Python crashes with a traceback — exit 1.
		_, _ = fmt.Fprintf(stderr, "Journalpump: %v\n", err)
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

// registerSenders wires the implemented sender packages into the
// dispatch map — one explicit import + Register call per sender, the
// greppable counterpart of
// journalpump/senders/__init__.py:output_type_to_sender_class_path.
// Called before the pump construction.
//
// Registered senders (plan Tasks 16+):
//   - file (Task 16)
func registerSenders() {
	sender.Register("file", file.New)
}
