// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

//go:build systest

package systestgo

import (
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
// "no leaked goroutines on exit (errgroup wait verified)": after several
// SIGHUP reloads the process must still join every goroutine and exit 0
// within the shutdown timeout, rather than hang.
func TestGoCleanShutdownAfterReloads(t *testing.T) {
	logs := stageJournals(t, "rotated_logs")
	out := filepath.Join(t.TempDir(), "out.jsonl")
	config := journalConfig(logs, out, "", nil)
	configPath := writeConfig(t, t.TempDir(), config)
	p := startJournalpump(t, configPath)

	// Each SIGHUP only reloads when the config file's ctime changed
	// (daemon.py's guard), so touch the file before every reload.
	for range 3 {
		data, err := os.ReadFile(configPath)
		if err != nil {
			t.Fatalf("read config: %v", err)
		}
		if err := os.WriteFile(configPath, data, 0o644); err != nil {
			t.Fatalf("touch config: %v", err)
		}
		p.reload()
	}

	if code := p.stop(); code != 0 {
		t.Fatalf("exit status = %d, want 0; stderr:\n%s", code, p.stderr.String())
	}
}
