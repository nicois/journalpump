// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

//go:build systest

package systestgo

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
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

// TestGoReloadPreservesDeliveryCursors is the Milestone A delivery-cursor
// requirement the notification-only reload tests did not cover: with a
// journal→file pipeline, an established delivery must survive a
// CHANGED-reader SIGHUP (same reader name, changed config blob → the
// reader set is replaced) via the persisted state cursor, and subsequent
// journal entries must then arrive with no missing or duplicated message.
//
// The test is deliberately NON-VACUOUS: the reload config keeps
// initial_position="head" and changes a DIFFERENT reader setting (a
// search is added) to force the rebuild. If the reload ignored the saved
// cursor, the rebuilt reader would seek the journal HEAD and replay
// Messages 0..9 — the line counts below would then be 20 (not 10) and the
// final restart 20 (not 0). The final fresh-process check runs with
// initial_position="head" too, so it can only yield 0 lines if the saved
// cursor is actually honored.
func TestGoReloadPreservesDeliveryCursors(t *testing.T) {
	logs := t.TempDir()
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.json")
	configPath := filepath.Join(dir, "config.json")

	// readerExtra is merged into the reader config: nil for the first
	// config, a search (a reader-level blob change) for the reload.
	writeCfg := func(out string, readerExtra map[string]any) {
		t.Helper()
		reader := map[string]any{
			"journal_path":     logs,
			"initial_position": "head",
			"senders": map[string]any{
				"out": map[string]any{
					"output_type": "file",
					"file_output": out,
					// Pad past the file sender's 128 KiB buffer so
					// delivery is observable on disk mid-run.
					"extra_field_values": map[string]any{"PADDING": strings.Repeat("x", 131088)},
				},
			},
		}
		for k, v := range readerExtra {
			reader[k] = v
		}
		cfg := map[string]any{
			"readers":              map[string]any{"my-reader": reader},
			"json_state_file_path": statePath,
		}
		data, err := json.Marshal(cfg)
		if err != nil {
			t.Fatalf("marshal config: %v", err)
		}
		if err := os.WriteFile(configPath, data, 0o644); err != nil {
			t.Fatalf("write config: %v", err)
		}
	}

	out := filepath.Join(dir, "out.jsonl")
	r := newRotator(t, logs, "rotated_logs")
	r.rotate() // file A -> Messages 0..9
	writeCfg(out, nil)
	p := startJournalpump(t, configPath)
	waitForAtLeast(t, out, 10, 15*time.Second)

	// The state file must hold a cursor before the reload, exactly like
	// Python (whose reload reads the periodically-saved state, journalpump.py:886).
	deadline := time.Now().Add(15 * time.Second)
	for {
		if cursor, ok := stateCursorMaybe(statePath, "my-reader", "out"); ok && cursor != "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("state file never recorded a cursor; stderr:\n%s", p.stderr.String())
		}
		time.Sleep(100 * time.Millisecond)
	}
	_, cursorBefore, _ := senderSent(t, statePath, "my-reader", "out")

	// CHANGED-reader SIGHUP: same reader name, changed config blob (a
	// search is added; initial_position stays "head"), so the reader is
	// retired and rebuilt. It must resume from the saved cursor, not
	// re-read the journal head.
	writeCfg(out, map[string]any{
		"searches": []any{map[string]any{"name": "all", "fields": map[string]any{"MESSAGE": "Message"}}},
	})
	p.reload()

	// No replay: a cursor-preserving rebuild delivers nothing new for the
	// already-consumed file A. With initial_position="head", a rebuild that
	// ignored the cursor would re-emit Messages 0..9 and this would read 20.
	time.Sleep(2 * time.Second)
	if got := lineCount(out); got != 10 {
		t.Fatalf("after reload wrote %d lines, want 10 (no replay of Messages 0..9)", got)
	}

	// Subsequent entries after the reload: rotate in file B (10..19).
	r.rotate()
	waitForAtLeast(t, out, 20, 15*time.Second)
	if code := p.stop(); code != 0 {
		t.Fatalf("exit status = %d, want 0; stderr:\n%s", code, p.stderr.String())
	}

	// No message is missing or duplicated across the reload boundary.
	assertMessageSet(t, outputMessages(t, out), messageRange(0, 20))

	// The final saved cursor advanced past the pre-reload position and is a
	// correct resume point: a fresh process from the same state replays
	// nothing (again with initial_position="head", so 0 lines requires the
	// cursor).
	_, cursorAfter, present := senderSent(t, statePath, "my-reader", "out")
	if !present || cursorAfter == "" {
		t.Fatalf("state after reload has no cursor (present=%v)", present)
	}
	if cursorAfter == cursorBefore {
		t.Errorf("saved cursor = %q, did not advance after Messages 10..19", cursorAfter)
	}

	out2 := filepath.Join(t.TempDir(), "out2.jsonl")
	writeCfg(out2, nil)
	p2 := startJournalpump(t, configPath)
	time.Sleep(2 * time.Second)
	if code := p2.stop(); code != 0 {
		t.Fatalf("resume exit status = %d, want 0; stderr:\n%s", code, p2.stderr.String())
	}
	if got := lineCount(out2); got != 0 {
		t.Errorf("fresh process resumed from the saved cursor wrote %d lines, want 0", got)
	}
}
