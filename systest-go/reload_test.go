// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

//go:build systest

package systestgo

import (
	"encoding/json"
	"os"
	"syscall"
	"testing"
	"time"
)

// TestGoSighupWithChangedReadersKeepsProcessAlive ports
// systest/test_reload.py::test_sighup_with_changed_readers_keeps_process_alive:
// a SIGHUP that swaps the whole reader set must be applied without
// crashing the process, and the process must still stop cleanly.
func TestGoSighupWithChangedReadersKeepsProcessAlive(t *testing.T) {
	configDir := t.TempDir()
	configPath := writeConfig(t, configDir, map[string]any{
		"readers": map[string]any{"before_reload": map[string]any{"senders": map[string]any{}}},
	})
	p := startJournalpump(t, configPath)

	// Rewrite the config on disk, then reload.
	rewritten, err := json.Marshal(map[string]any{
		"readers": map[string]any{"after_reload": map[string]any{"senders": map[string]any{}}},
	})
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	if err := os.WriteFile(configPath, rewritten, 0o644); err != nil {
		t.Fatalf("rewrite config: %v", err)
	}
	p.reload()

	// The process survived the reload.
	if p.cmd.ProcessState != nil {
		t.Fatalf("process exited during reload: %v; stderr:\n%s", p.cmd.ProcessState, p.stderr.String())
	}

	// A second SIGHUP with an unchanged config is a no-op (daemon.py's
	// config-ctime guard: no RELOADING=1 is emitted), so the process must
	// still be alive and stop cleanly.
	if err := p.cmd.Process.Signal(syscall.SIGHUP); err != nil {
		t.Fatalf("second SIGHUP: %v", err)
	}
	time.Sleep(500 * time.Millisecond)

	if code := p.stop(); code != 0 {
		t.Fatalf("exit status = %d, want 0; stderr:\n%s", code, p.stderr.String())
	}

	// A short settle lets any post-stop write flush; the process must be
	// gone rather than a lingering zombie.
	time.Sleep(100 * time.Millisecond)
}
