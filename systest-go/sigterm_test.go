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
	"testing"
	"time"
)

// TestGoSigtermGraceFlushDeliversBufferedTail is the D9 grace-flush
// evidence (plan Task 18): with payloads small enough to stay inside the
// file sender's BufferedWriter, no line reaches disk during the run; a
// SIGTERM then flushes the whole buffered tail within the grace window
// and the process exits 0. Python loses this tail (the sanctioned D9
// deviation), so this suite has no Python-side counterpart.
func TestGoSigtermGraceFlushDeliversBufferedTail(t *testing.T) {
	logs := stageJournals(t, "rotated_logs")
	out := filepath.Join(t.TempDir(), "out.jsonl")
	config := map[string]any{
		"readers": map[string]any{
			"my-reader": map[string]any{
				"journal_path":     logs,
				"initial_position": "head",
				"senders": map[string]any{
					"out": map[string]any{
						"output_type":        "file",
						"file_output":        out,
						"extra_field_values": map[string]any{"PADDING": strings.Repeat("x", 100)},
					},
				},
			},
		},
	}
	p := startJournalpump(t, writeConfig(t, t.TempDir(), config))
	time.Sleep(time.Second) // let the reader consume the journal into the buffer

	before := lineCount(out)
	if before >= 40 {
		t.Fatalf("all 40 lines flushed before SIGTERM; cannot observe the grace window")
	}

	// SIGTERM mid-run: the reader is either still draining or just done,
	// the sender still holds the buffered tail.
	if code := p.stop(); code != 0 {
		t.Fatalf("exit status = %d, want 0; stderr:\n%s", code, p.stderr.String())
	}

	messages := outputMessages(t, out)
	assertMessageSet(t, messages, messageRange(0, 40))
}

// TestGoCleanShutdownAfterReloads exercises the Task 17 acceptance
// "clean shutdown after reloads" with CHANGED-reader reloads: each SIGHUP
// replaces the reader (new name), so the previous reader and its already
// initialized sender goroutines must be retired and joined before the new
// ones start. The process must then still exit 0 within the shutdown
// timeout rather than hang. This is a shutdown/hang check, not a
// goroutine-leak check: os.Exit abandons any remaining goroutines
// regardless of exit status, so the real leak assertion lives in the
// Go-native daemon test (TestReloadJoinsRetiredWorkers). The child is
// built with -race, and stop() fails the test on a detected race.
func TestGoCleanShutdownAfterReloads(t *testing.T) {
	logs := stageJournals(t, "rotated_logs")
	dir := t.TempDir()
	out := filepath.Join(dir, "out.jsonl")
	configPath := filepath.Join(dir, "config.json")

	namedReaderConfig := func(name string) map[string]any {
		return map[string]any{
			"readers": map[string]any{
				name: map[string]any{
					"journal_path":     logs,
					"initial_position": "head",
					"senders": map[string]any{
						"out": map[string]any{
							"output_type": "file",
							"file_output": out,
							// Pad past the file sender's buffer so the
							// replaced reader's sender has flushed a line
							// before the next reload retires it.
							"extra_field_values": map[string]any{"PADDING": strings.Repeat("x", 131088)},
						},
					},
				},
			},
		}
	}
	write := func(name string) {
		data, err := json.Marshal(namedReaderConfig(name))
		if err != nil {
			t.Fatalf("marshal config: %v", err)
		}
		if err := os.WriteFile(configPath, data, 0o644); err != nil {
			t.Fatalf("write config: %v", err)
		}
	}
	write("reader-1")
	p := startJournalpump(t, configPath)
	// Let reader-1 open its journal and initialize its sender before the
	// first replacement: a changed-reader reload only exercises retirement
	// if the old reader has live workers to join.
	waitForAtLeast(t, out, 1, 5*time.Second)

	for _, name := range []string{"reader-2", "reader-3", "reader-4"} {
		write(name)
		p.reload()
		// Give the replacement reader time to initialize its sender so the
		// next reload retires a live worker set too.
		time.Sleep(300 * time.Millisecond)
	}

	if code := p.stop(); code != 0 {
		t.Fatalf("exit status = %d, want 0; stderr:\n%s", code, p.stderr.String())
	}
}
