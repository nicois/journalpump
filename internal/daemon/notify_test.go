// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package daemon

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// socketPair binds an abstract-namespace unixgram listener and returns
// its NOTIFY_SOCKET value (the '@' form the systest harness uses).
func socketPair(t *testing.T) (listener *net.UnixConn, notifySocket string) {
	t.Helper()
	name := fmt.Sprintf("jp-daemon-test-%d-%x", os.Getpid(), time.Now().UnixNano())
	addr := &net.UnixAddr{Name: "@" + name, Net: "unixgram"}
	listener, err := net.ListenUnixgram("unixgram", addr)
	if err != nil {
		t.Fatalf("ListenUnixgram: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return listener, "@" + name
}

// recvWithTimeout reads one datagram with a deadline.
func recvWithTimeout(t *testing.T, listener *net.UnixConn) string {
	t.Helper()
	_ = listener.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 4096)
	n, _, err := listener.ReadFromUnix(buf)
	if err != nil {
		t.Fatalf("ReadFromUnix: %v", err)
	}
	return string(buf[:n])
}

// TestNotifyUnsetSocketIsNoOp: no NOTIFY_SOCKET in the environment at
// all → no-op, no error (probed against python-systemd: notify()
// returns False). Explicitly UNSETS the variable — t.Setenv("",) would
// be the different EMPTY case (see TestNotifyEmptySocketFailsLikePython).
func TestNotifyUnsetSocketIsNoOp(t *testing.T) {
	orig, had := os.LookupEnv("NOTIFY_SOCKET")
	if err := os.Unsetenv("NOTIFY_SOCKET"); err != nil {
		t.Fatalf("Unsetenv: %v", err)
	}
	t.Cleanup(func() {
		if had {
			_ = os.Setenv("NOTIFY_SOCKET", orig)
		} else {
			_ = os.Unsetenv("NOTIFY_SOCKET")
		}
	})

	sent, err := (SocketNotifier{}).Notify(StateReady)
	if err != nil {
		t.Fatalf("Notify: %v", err)
	}
	if sent {
		t.Error("sent = true, want false (no-op without NOTIFY_SOCKET)")
	}
}

// TestNotifyEmptySocketFailsLikePython: NOTIFY_SOCKET="" (set but
// empty) is NOT the unset no-op — Python raises OSError EPROTO
// "Protocol error" (probed), so the Go client must error too.
func TestNotifyEmptySocketFailsLikePython(t *testing.T) {
	t.Setenv("NOTIFY_SOCKET", "")
	if _, err := (SocketNotifier{}).Notify(StateReady); err == nil {
		t.Error("empty NOTIFY_SOCKET must fail like Python's OSError EPROTO")
	}
}

// TestNotifySendsOverAbstractSocket: '@' NOTIFY_SOCKET delivers the
// state string byte-exact to the abstract unixgram listener, repeated
// calls all deliver (the env var is never unset or cached — probed
// against python-systemd). The sequence includes the full daemon
// lifecycle incl. the RELOADING=1 → READY=1 reload pair.
func TestNotifySendsOverAbstractSocket(t *testing.T) {
	listener, notifySocket := socketPair(t)
	t.Setenv("NOTIFY_SOCKET", notifySocket)
	notifier := SocketNotifier{}

	for _, state := range []string{StateReady, StateReloading, StateWatchdog, StateStopping} {
		sent, err := notifier.Notify(state)
		if err != nil {
			t.Fatalf("Notify(%s): %v", state, err)
		}
		if !sent {
			t.Fatalf("Notify(%s) = false, want true", state)
		}
		if got := recvWithTimeout(t, listener); got != state {
			t.Errorf("received %q, want %q", got, state)
		}
	}
}

// TestNotifySendsOverPathSocket: a filesystem socket path works the
// same (systemd's socket-activated case is abstract, but
// NOTIFY_SOCKET may be a path).
func TestNotifySendsOverPathSocket(t *testing.T) {
	path := filepath.Join(t.TempDir(), "notify.sock")
	listener, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: path, Net: "unixgram"})
	if err != nil {
		t.Fatalf("ListenUnixgram: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	t.Setenv("NOTIFY_SOCKET", path)

	if _, err := (SocketNotifier{}).Notify(StateReady); err != nil {
		t.Fatalf("Notify: %v", err)
	}
	if got := recvWithTimeout(t, listener); got != StateReady {
		t.Errorf("received %q, want %q", got, StateReady)
	}
}

// TestNotifyInvalidSocketFails: NOTIFY_SOCKET that is neither a path
// nor '@'-abstract fails like Python (probed: OSError EPROTO).
func TestNotifyInvalidSocketFails(t *testing.T) {
	t.Setenv("NOTIFY_SOCKET", "garbage")
	if _, err := (SocketNotifier{}).Notify(StateReady); err == nil {
		t.Fatal("expected an error for an invalid NOTIFY_SOCKET")
	}
}

// TestNotifyAbsentListenerFails: a socket with no listener fails like
// Python (probed: ECONNREFUSED abstract, ENOENT filesystem path).
func TestNotifyAbsentListenerFails(t *testing.T) {
	t.Setenv("NOTIFY_SOCKET", "@jp-daemon-test-no-listener")
	if _, err := (SocketNotifier{}).Notify(StateReady); err == nil {
		t.Error("abstract: expected an error (Python raises ECONNREFUSED)")
	}
	t.Setenv("NOTIFY_SOCKET", filepath.Join(t.TempDir(), "absent.sock"))
	if _, err := (SocketNotifier{}).Notify(StateReady); err == nil {
		t.Error("filesystem path: expected an error (Python raises ENOENT)")
	}
}

// Fake notifier with per-state delay — the Go-native equivalent of the
// systest sitecustomize trick that holds ping_watchdog so SIGTERM
// arrives mid-loop instead of inside poll (systest/test_sigterm.py).
type delayNotifier struct {
	delay time.Duration
	sent  []string
}

func (f *delayNotifier) Notify(state string) (bool, error) {
	f.sent = append(f.sent, state)
	if state == StateWatchdog {
		time.Sleep(f.delay)
	}
	return true, nil
}

// TestFakeNotifierCanHoldWatchdog pins the seam Task 10's harness
// needs: a fake that delays on WATCHDOG=1 compiles into the same
// Notifier interface the run loop will call.
func TestFakeNotifierCanHoldWatchdog(t *testing.T) {
	notifier := &delayNotifier{delay: 5 * time.Millisecond}
	start := time.Now()
	for _, state := range []string{StateReady, StateReloading, StateWatchdog, StateStopping} {
		if _, err := notifier.Notify(state); err != nil {
			t.Fatalf("Notify(%s): %v", state, err)
		}
	}
	if elapsed := time.Since(start); elapsed < 5*time.Millisecond {
		t.Errorf("elapsed = %v, want >= the WATCHDOG delay", elapsed)
	}
	if fmt.Sprint(notifier.sent) != fmt.Sprint([]string{StateReady, StateReloading, StateWatchdog, StateStopping}) {
		t.Errorf("sent = %v", notifier.sent)
	}
}

// notifyStubEnv marks the re-executed test binary as the notify stub
// (TestMain runs stubMain instead of the tests).
const notifyStubEnv = "GO_DAEMON_NOTIFY_STUB"

// stubMain is the in-binary stub the harness test spawns: it sends
// READY=1, RELOADING=1, WATCHDOG=1, STOPPING=1 through the real SocketNotifier
// against the NOTIFY_SOCKET it inherited, then exits 0 — the datagram
// sequence journalpump_process.py expects from the daemon. A failure
// exits nonzero with a message on stderr.
func stubMain() int {
	notifier := SocketNotifier{}
	for _, state := range []string{StateReady, StateReloading, StateWatchdog, StateStopping} {
		if _, err := notifier.Notify(state); err != nil {
			fmt.Fprintf(os.Stderr, "stub notify %s: %v\n", state, err)
			return 3
		}
	}
	return 0
}

// TestStubBinarySendsNotifyOverAbstractSocket is the plan-Task-9
// harness test: it spawns a stub binary (this test binary re-executed
// in stub mode) with an abstract-socket NOTIFY_SOCKET and asserts the
// same READY=1 / WATCHDOG=1 / STOPPING=1 datagrams the systest harness
// asserts against the real daemon.
func TestStubBinarySendsNotifyOverAbstractSocket(t *testing.T) {
	listener, notifySocket := socketPair(t)

	cmd := exec.CommandContext(context.Background(), os.Args[0])
	cmd.Env = append(os.Environ(), notifyStubEnv+"=1", "NOTIFY_SOCKET="+notifySocket)
	stderr := &bytes.Buffer{}
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Read the datagrams concurrently; the stub exits right after.
	got := make(chan string, 3)
	go func() {
		_ = listener.SetReadDeadline(time.Now().Add(10 * time.Second))
		buf := make([]byte, 4096)
		for {
			n, _, err := listener.ReadFromUnix(buf)
			if err != nil {
				close(got)
				return
			}
			got <- string(buf[:n])
		}
	}()

	want := []string{StateReady, StateReloading, StateWatchdog, StateStopping}
	received := make(map[string]bool)
	for range want {
		select {
		case state := <-got:
			received[state] = true
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out; received so far %v; stub stderr:\n%s", received, stderr.String())
		}
	}
	for _, state := range want {
		if !received[state] {
			t.Errorf("missing datagram %q; stub stderr:\n%s", state, stderr.String())
		}
	}

	errCh := make(chan error, 1)
	go func() { errCh <- cmd.Wait() }()
	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("stub exit: %v; stderr:\n%s", err, stderr.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("stub did not exit")
	}
}
