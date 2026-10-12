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

	"github.com/Aiven-Open/journalpump/internal/config"
	"github.com/Aiven-Open/journalpump/internal/daemon"
	"github.com/Aiven-Open/journalpump/internal/journaldlog"
	"github.com/Aiven-Open/journalpump/internal/sender"
	"github.com/Aiven-Open/journalpump/internal/sender/cloudwatch"
	"github.com/Aiven-Open/journalpump/internal/sender/elasticsearch"
	"github.com/Aiven-Open/journalpump/internal/sender/file"
	"github.com/Aiven-Open/journalpump/internal/sender/gcl"
	"github.com/Aiven-Open/journalpump/internal/sender/kafka"
	"github.com/Aiven-Open/journalpump/internal/sender/logplex"
	"github.com/Aiven-Open/journalpump/internal/sender/rsyslog"
	"github.com/Aiven-Open/journalpump/internal/sender/websocket"
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

// newLogger selects the log sink and applies the config log level
// (daemon.py:configure_logging, 58-70). When stdout is a terminal the
// daemon logs to it with Python's LOG_FORMAT; otherwise it logs to the
// systemd journal through the native datagram socket, applying
// LOG_FORMAT_JOURNAL and mapping each level to a journald PRIORITY. A
// non-*os.File stdout (the test seam) is used as-is so tests stay
// deterministic without a live journald.
var newLogger = func(stdout io.Writer, level int64) *journaldlog.Log {
	if f, ok := stdout.(*os.File); ok && !journaldlog.IsTerminal(f) {
		return journaldlog.NewJournalLog("journalpump", int(level))
	}
	return journaldlog.NewTTYLog("journalpump", stdout, int(level))
}

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
func run(args []string, stdout, stderr io.Writer) (code int) {
	// Profiling is env-gated and inert unless JOURNALPUMP_CPU_PROFILE /
	// JOURNALPUMP_MEM_PROFILE are set (bench/profile.go); the defer fires
	// on every return path, so a clean SIGTERM shutdown flushes the
	// profile. os.Exit in main would skip a defer here, hence the hook
	// lives in run().
	stopProfiling, err := startProfiling()
	if err != nil {
		// An env-gated profiling run whose profile cannot be created must
		// fail the run, not silently produce no profile (bench/profile.go).
		_, _ = fmt.Fprintf(stderr, "Journalpump failed to start: %v\n", err)
		return exitErr
	}
	defer func() {
		if err := stopProfiling(); err != nil {
			// The deferred stop runs after the return value is set, so a
			// failed profile flush must override the exit code explicitly.
			_, _ = fmt.Fprintf(stderr, "Journalpump failed to stop profiling: %v\n", err)
			code = exitErr
		}
	}()

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
	// logging module: stdout on a tty, the systemd journal otherwise
	// (daemon.py:58-70), filtered by the configured log_level.
	logger := newLogger(stdout, cfg.LogLevel)
	logger.Printf("Loading configuration")
	registerSenders()
	pump, err := daemon.NewPump(cfg, daemon.PumpDeps{Log: logger, ConfigPath: configPath})
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "Journalpump failed to start: %v\n", err)
		return exitErr
	}
	// Install the signal handlers BEFORE the READY=1 notifications so a
	// supervisor that reacts to readiness (systemd sends SIGHUP on reload)
	// can never race ahead of handler installation. Python installs its
	// SIGHUP/SIGTERM handlers in ServiceDaemon.__init__ (daemon.py:52-54)
	// after the constructor's first RELOADING/READY pair and before main's
	// final READY=1, so its handlers are live by the time the last READY is
	// sent. The Go port previously called newSignals() only after both
	// READYs, leaving a window in which an immediate post-READY SIGHUP was
	// dropped; installing them here closes that window.
	signals := newSignals()
	defer signals.Close()

	// daemon.py:114 + main(): READY=1 twice at startup (parity).
	for i := 0; i < 2; i++ {
		if _, err := notifier.Notify(daemon.StateReady); err != nil {
			_, _ = fmt.Fprintf(stderr, "Journalpump failed to start: %v\n", err)
			return exitErr
		}
	}

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
//   - rsyslog (Task 20)
//   - logplex (Task 21)
//   - kafka (Task 22)
//   - elasticsearch, opensearch (Task 23)
//   - websocket (Task 24)
//   - aws_cloudwatch (Task 25)
//   - google_cloud_logging (Task 26)
func registerSenders() {
	sender.Register("file", file.New)
	sender.Register("rsyslog", rsyslog.New)
	sender.Register("logplex", logplex.New)
	sender.Register("kafka", kafka.New)
	sender.Register("elasticsearch", elasticsearch.NewElasticsearch)
	sender.Register("opensearch", elasticsearch.NewOpenSearch)
	sender.Register("websocket", websocket.New)
	sender.Register("aws_cloudwatch", cloudwatch.New)
	sender.Register("google_cloud_logging", gcl.New)
}
