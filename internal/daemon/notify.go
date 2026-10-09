// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

// Package daemon ports the process-lifecycle pieces of
// journalpump/daemon.py that live outside the supervisor run loop: the
// sd_notify client (systemd.daemon.notify) and the signal plumbing
// (SIGTERM/SIGINT run context, SIGHUP reload channel) prescribed by the
// spec §4.3 concurrency model. The supervisor that calls them is wired
// in migration plan Tasks 10/17.
package daemon

import (
	"fmt"
	"net"
	"os"
)

// The sd_notify states journalpump emits, byte-exact as
// journalpump/daemon.py sends them: the systest harness splits each
// datagram on "\n" and matches these lines
// (systest/journalpump_process.py:39-45).
const (
	StateReady     = "READY=1"
	StateReloading = "RELOADING=1"
	StateStopping  = "STOPPING=1"
	StateWatchdog  = "WATCHDOG=1"
)

// Notifier is the sd_notify client seam. Tests use a fake (mock
// strategy, plan Task 9) — e.g. a notifier that delays on WATCHDOG=1
// is the Go-native equivalent of the systest sitecustomize trick that
// holds ping_watchdog so SIGTERM lands mid-loop
// (systest/test_sigterm.py:11-23). Seam placement is deliberate and
// producer-side for now: the consumers (the Task 10 systest harness
// and the Task 17 supervisor run loop) land in later tasks and may
// want the interface moved to the layer they both see; nothing outside
// this package may take a concrete SocketNotifier.
type Notifier interface {
	// Notify sends one sd_notify state datagram. It returns false when
	// NOTIFY_SOCKET is unset (no-op — systemd.daemon.notify returns
	// False without an error), true when the datagram was sent, and an
	// error on failure (Python's unguarded daemon.notify calls let
	// OSError propagate and crash the daemon).
	Notify(state string) (bool, error)
}

// SocketNotifier is the real client. It mirrors systemd.daemon.notify
// (python-systemd _daemon.c → sd_notify(3)):
//
//   - NOTIFY_SOCKET is read from the environment on EVERY call (never
//     cached or unset — probed: repeated Python notifies all deliver);
//   - an UNSET NOTIFY_SOCKET is a silent no-op returning (false, nil)
//     — probed: notify() returns False when unset;
//   - a leading '@' addresses an abstract-namespace socket (the byte
//     becomes NUL) — the systest harness socket form;
//   - otherwise the value is a filesystem socket path;
//   - anything else fails, INCLUDING an empty value (Python raises
//     OSError EPROTO "Protocol error" — probed with both
//     NOTIFY_SOCKET="" and "garbage");
//   - delivery is an AF_UNIX/SOCK_DGRAM datagram (connect + send);
//   - a dead or absent listener is an error (Python raises OSError:
//     ECONNREFUSED for a missing abstract listener, ENOENT for a
//     missing filesystem path — both probed).
type SocketNotifier struct{}

// Notify implements Notifier with the real socket.
func (SocketNotifier) Notify(state string) (bool, error) {
	addr, present := os.LookupEnv("NOTIFY_SOCKET")
	if !present {
		return false, nil
	}
	if addr == "" || (addr[0] != '/' && addr[0] != '@') {
		// Python (probed): OSError EPROTO for both the empty value and
		// an address that is neither a path nor '@'-abstract.
		return false, fmt.Errorf("sd_notify: invalid NOTIFY_SOCKET %q: must start with / or @", addr)
	}
	// net.UnixAddr interprets a leading '@' as an abstract-namespace
	// socket (the '@' byte is replaced by NUL), matching python-systemd.
	conn, err := net.DialUnix("unixgram", nil, &net.UnixAddr{Name: addr, Net: "unixgram"})
	if err != nil {
		return false, fmt.Errorf("sd_notify: connect: %w", err)
	}
	defer conn.Close() //nolint:errcheck // datagram socket, nothing to flush
	if _, err := conn.Write([]byte(state)); err != nil {
		return false, fmt.Errorf("sd_notify: send: %w", err)
	}
	return true, nil
}
