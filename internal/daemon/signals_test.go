// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package daemon

import (
	"os"
	"syscall"
	"testing"
	"time"
)

// TestMain re-executes this test binary in notify-stub mode for the
// harness test; otherwise it runs the suite.
func TestMain(m *testing.M) {
	if os.Getenv(notifyStubEnv) == "1" {
		os.Exit(stubMain())
	}
	os.Exit(m.Run())
}

// sendSelf delivers a signal to this process. The ListenSignals
// handlers are installed, so nothing takes the default disposition.
func sendSelf(t *testing.T, sig os.Signal) {
	t.Helper()
	if err := syscall.Kill(os.Getpid(), sig.(syscall.Signal)); err != nil {
		t.Fatalf("kill %v: %v", sig, err)
	}
}

// TestSighupDoesNotCancelRunContext pins the spec §4.3 composition:
// SIGHUP is a reload EVENT on the buffered Hup channel and does NOT
// cancel the run context.
func TestSighupDoesNotCancelRunContext(t *testing.T) {
	signals := ListenSignals()
	t.Cleanup(signals.Close)

	sendSelf(t, syscall.SIGHUP)
	select {
	case sig := <-signals.Hup:
		if sig != syscall.SIGHUP {
			t.Errorf("Hup delivered %v, want SIGHUP", sig)
		}
	case <-time.After(time.Second):
		t.Fatal("SIGHUP not delivered on Hup channel")
	}
	select {
	case <-signals.Run.Done():
		t.Error("SIGHUP canceled the run context (it must not — SIGHUP is a reload event)")
	case <-time.After(100 * time.Millisecond):
	}
}

// TestSigtermCancelsRunContext: SIGTERM cancels the run context
// (signal.NotifyContext), which is the shutdown trigger.
func TestSigtermCancelsRunContext(t *testing.T) {
	signals := ListenSignals()
	t.Cleanup(signals.Close)

	sendSelf(t, syscall.SIGTERM)
	select {
	case <-signals.Run.Done():
	case <-time.After(time.Second):
		t.Fatal("SIGTERM did not cancel the run context")
	}
}

// TestSigintCancelsRunContext: SIGINT behaves like SIGTERM (the
// Python sigterm handler covers both, daemon.py:85-92).
func TestSigintCancelsRunContext(t *testing.T) {
	signals := ListenSignals()
	t.Cleanup(signals.Close)

	sendSelf(t, syscall.SIGINT)
	select {
	case <-signals.Run.Done():
	case <-time.After(time.Second):
		t.Fatal("SIGINT did not cancel the run context")
	}
}
