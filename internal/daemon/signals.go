// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package daemon

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

// Signals bundles the two signal paths of the spec §4.3 concurrency
// model (migration plan Task 9):
//
//   - Run: the run context, from signal.NotifyContext over
//     SIGTERM/SIGINT. Cancellation IS shutdown; the supervisor's
//     errgroup trees derive from it.
//   - Hup: a separate buffered signal.Notify channel for SIGHUP — a
//     reload EVENT, not cancellation. The supervisor's select consumes
//     it and the reload handler does the config ctime check at event
//     time (daemon.py's sighup sets reload_requested=True, daemon.py:72-74,
//     consumed by reload_if_requested — a coalescing flag; the
//     capacity-1 channel with Go's drop-when-full signal delivery
//     coalesces the same way).
type Signals struct {
	Run     context.Context
	Hup     <-chan os.Signal
	stopRun context.CancelFunc
	// hupRaw is the channel registered with signal.Notify (Hup is the
	// receive-only view); Close needs the bidirectional form.
	hupRaw chan os.Signal
}

// ListenSignals installs the handlers and returns the live Signals.
// Callers must Close before process exit.
func ListenSignals() *Signals {
	runCtx, stopRun := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	hupCh := make(chan os.Signal, 1)
	signal.Notify(hupCh, syscall.SIGHUP)
	return &Signals{Run: runCtx, Hup: hupCh, stopRun: stopRun, hupRaw: hupCh}
}

// Close unregisters the signal handlers (the supervisor calls it on
// exit). It is nil-safe, so a Signals built without live handlers — the
// test seam — closes cleanly.
func (s *Signals) Close() {
	if s == nil {
		return
	}
	if s.stopRun != nil {
		s.stopRun()
	}
	if s.hupRaw != nil {
		signal.Stop(s.hupRaw)
	}
}
