// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

//go:build systest

package systestgo

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// goBinaryPath is the shared, -tags systest build of the real Go binary
// used by every systest suite. Built once in TestMain.
var goBinaryPath string

// TestMain builds the systest-tagged binary once (the files-available
// shim is compiled in behind JOURNALPUMP_FILES_AVAILABLE, like the
// Python harness's sitecustomize patch) and runs the suites.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "journalpump-systest-bin-")
	if err != nil {
		fmt.Fprintf(os.Stderr, "mkdtemp: %v\n", err)
		os.Exit(1)
	}
	goBinaryPath = filepath.Join(dir, "journalpump")
	repoRoot, err := filepath.Abs("..")
	if err != nil {
		fmt.Fprintf(os.Stderr, "repo root: %v\n", err)
		os.Exit(1)
	}
	build := exec.Command("go", "build", "-tags", "systest", "-o", goBinaryPath, "./cmd/journalpump")
	build.Dir = repoRoot
	build.Env = append(os.Environ(), "GOFLAGS=-mod=mod")
	if out, err := build.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "go build: %v\n%s\n", err, out)
		os.Exit(1)
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

// journalpumpProcess is the Go port of the implementation-agnostic
// systest harness (systest/journalpump_process.py, plan Task 10): it
// spawns a journalpump BINARY, asserts the sd_notify sequence over an
// abstract-socket NOTIFY_SOCKET, drives SIGHUP reloads, and captures the
// SIGTERM exit code.
type journalpumpProcess struct {
	t          *testing.T
	runtimeDir string
	notifyName string
	notifyConn *net.UnixConn
	// notifications is drained by a dedicated goroutine that must NEVER
	// block: a full socket buffer would block the daemon's sd_notify
	// write (WATCHDOG=pings flood it), stalling shutdown. The queue is
	// therefore append-only with a wake channel and a read cursor.
	notifyMu      sync.Mutex
	notifications []string
	notifyCursor  int
	notifyWake    chan struct{}
	cmd           *exec.Cmd
	stderr        bytes.Buffer
	waitOnce      sync.Once
	waitErr       error
}

// processOption customizes start.
type processOption func(*processConfig)

type processConfig struct {
	env                    map[string]string
	exportRuntimeDirectory bool
}

func withEnv(env map[string]string) processOption {
	return func(c *processConfig) { c.env = env }
}

func withoutRuntimeDirectory() processOption {
	return func(c *processConfig) { c.exportRuntimeDirectory = false }
}

// startJournalpump launches the Go binary with configPath and waits for
// READY=1. The caller must defer stop().
func startJournalpump(t *testing.T, configPath string, opts ...processOption) *journalpumpProcess {
	t.Helper()
	cfg := processConfig{exportRuntimeDirectory: true}
	for _, opt := range opts {
		opt(&cfg)
	}
	runtimeDir := t.TempDir()
	name := fmt.Sprintf("jp-systest-%d-%d", os.Getpid(), time.Now().UnixNano())
	conn, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: "@" + name, Net: "unixgram"})
	if err != nil {
		t.Fatalf("listen notify socket: %v", err)
	}
	p := &journalpumpProcess{
		t:          t,
		runtimeDir: runtimeDir,
		notifyName: name,
		notifyConn: conn,
		notifyWake: make(chan struct{}, 1),
	}
	go p.readNotifications()
	t.Cleanup(func() {
		p.stop()
		_ = conn.Close()
	})

	env := os.Environ()
	env = append(env, "JOURNALPUMP_FILES_AVAILABLE=1")
	// Match the Python systest harness's pump.poll_interval_ms = 100
	// (the Go analogue is the systest-tagged timing-control shim); the
	// production default stays 1000 ms. Callers may override via opts.
	env = append(env, "JOURNALPUMP_POLL_INTERVAL_MS=100")
	if cfg.exportRuntimeDirectory {
		env = append(env, "RUNTIME_DIRECTORY="+runtimeDir)
	}
	env = append(env, "NOTIFY_SOCKET=@"+name)
	for k, v := range cfg.env {
		env = append(env, k+"="+v)
	}
	p.cmd = exec.Command(goBinaryPath, configPath)
	p.cmd.Env = env
	p.cmd.Stderr = &p.stderr
	if err := p.cmd.Start(); err != nil {
		t.Fatalf("start journalpump: %v", err)
	}
	if !p.waitNotification("READY=1", 10*time.Second) {
		t.Fatalf("journalpump did not send READY=1; stderr:\n%s", p.stderr.String())
	}
	return p
}

// readNotifications drains the notify socket into the queue without ever
// blocking (a blocked read loop would fill the socket buffer and block
// the daemon's sd_notify writes).
func (p *journalpumpProcess) readNotifications() {
	buf := make([]byte, 4096)
	for {
		n, _, err := p.notifyConn.ReadFromUnix(buf)
		if err != nil {
			return // socket closed by cleanup
		}
		p.notifyMu.Lock()
		cursor := p.notifyCursor
		for _, line := range strings.Split(string(buf[:n]), "\n") {
			if line != "" {
				p.notifications = append(p.notifications, line)
			}
		}
		// Bound memory for long runs: drop the oldest entries once the
		// backlog is huge and rebase the cursor.
		if len(p.notifications) > 10000 {
			drop := len(p.notifications) - 10000
			if drop > cursor {
				drop = cursor
			}
			p.notifications = append([]string(nil), p.notifications[drop:]...)
			p.notifyCursor = cursor - drop
		}
		p.notifyMu.Unlock()
		select {
		case p.notifyWake <- struct{}{}:
		default:
		}
	}
}

// waitNotification reports whether a state was seen within timeout,
// consuming the queue in order.
func (p *journalpumpProcess) waitNotification(state string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		p.notifyMu.Lock()
		for i := p.notifyCursor; i < len(p.notifications); i++ {
			if p.notifications[i] == state {
				p.notifyCursor = i + 1
				p.notifyMu.Unlock()
				return true
			}
		}
		p.notifyCursor = len(p.notifications)
		p.notifyMu.Unlock()
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return false
		}
		select {
		case <-p.notifyWake:
		case <-time.After(remaining):
			return false
		}
	}
}

// reload sends SIGHUP and asserts the RELOADING=1 → READY=1 pair.
func (p *journalpumpProcess) reload() {
	p.t.Helper()
	if p.cmd.Process == nil {
		p.t.Fatal("journalpump is not running")
	}
	if err := p.cmd.Process.Signal(syscall.SIGHUP); err != nil {
		p.t.Fatalf("SIGHUP: %v", err)
	}
	if !p.waitNotification("RELOADING=1", 3*time.Second) {
		p.t.Fatalf("no RELOADING=1 after SIGHUP; notifications=%v; stderr:\n%s", p.snapshot(), p.stderr.String())
	}
	if !p.waitNotification("READY=1", 3*time.Second) {
		p.t.Fatalf("no READY=1 after RELOADING=1; notifications=%v; stderr:\n%s", p.snapshot(), p.stderr.String())
	}
}

// snapshot returns a copy of the notifications seen so far.
func (p *journalpumpProcess) snapshot() []string {
	p.notifyMu.Lock()
	defer p.notifyMu.Unlock()
	return append([]string(nil), p.notifications...)
}

// stop sends SIGTERM (once) and returns the captured exit code.
func (p *journalpumpProcess) stop() int {
	var code int
	p.waitOnce.Do(func() {
		if p.cmd == nil || p.cmd.Process == nil {
			return
		}
		if p.cmd.ProcessState == nil {
			_ = p.cmd.Process.Signal(syscall.SIGTERM)
			done := make(chan error, 1)
			go func() { done <- p.cmd.Wait() }()
			select {
			case err := <-done:
				p.waitErr = err
			case <-time.After(8 * time.Second):
				_ = p.cmd.Process.Kill()
				p.waitErr = <-done
			}
		}
		if p.cmd.ProcessState != nil {
			code = p.cmd.ProcessState.ExitCode()
		}
	})
	return code
}

// runningConfigPath returns the running config path the daemon writes
// into the exported RUNTIME_DIRECTORY.
func (p *journalpumpProcess) runningConfigPath() string {
	return filepath.Join(p.runtimeDir, "running_config.json")
}
