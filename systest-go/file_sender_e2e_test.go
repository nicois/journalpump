// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

//go:build systest

package systestgo

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// fileSenderFixture mirrors the committed capture of
// systest/test_groundtruth_file.py (all 40 messages).
type fileSenderFixture struct {
	Capture struct {
		Sender struct {
			OutputType       string            `json:"output_type"`
			ExtraFieldValues map[string]string `json:"extra_field_values"`
		} `json:"sender"`
	} `json:"capture"`
	Expected struct {
		Lines      []string `json:"lines"`
		SentCount  int64    `json:"sent_count"`
		SentCursor string   `json:"sent_cursor"`
	} `json:"expected"`
}

// stateFile is the minimal state-file view the e2e test asserts.
type stateFile struct {
	Readers map[string]struct {
		Senders map[string]struct {
			Sent struct {
				Count  int64  `json:"count"`
				Cursor string `json:"cursor"`
			} `json:"sent"`
		} `json:"senders"`
	} `json:"readers"`
}

// TestFileSenderEndToEnd is the committed value demonstration: it
// stages the fixture journals, launches the REAL Go binary built with
// the systest tag, drives it with SIGTERM, and asserts the full
// 40-message output bytes, the state file, and the exit status.
//
// Test-shim path: arbitrary fixture directories do not carry the
// persistent/runtime journal files libsystemd probes, so this systest
// binary activates the files-available override via
// JOURNALPUMP_FILES_AVAILABLE=1 (internal/reader/files_available_systest.go,
// the analog of the Python ground-truth sitecustomize patch). This is
// shim evidence, not a production-binary demonstration: a production
// build has no such override and gates on the real libsystemd probe.
func TestFileSenderEndToEnd(t *testing.T) {
	repoRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatalf("repo root: %v", err)
	}
	rawFixture, err := os.ReadFile(filepath.Join(repoRoot, "systest-go", "fixtures", "file_sender.json"))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var fixture fileSenderFixture
	if err := json.Unmarshal(rawFixture, &fixture); err != nil {
		t.Fatalf("parse fixture: %v", err)
	}
	if len(fixture.Expected.Lines) != 40 {
		t.Fatalf("fixture lines = %d, want 40", len(fixture.Expected.Lines))
	}

	// Binary. It is built with the systest tag so the reader's
	// files-available shim compiles in (the Go analogue of the Python
	// harness's sitecustomize patch); the production binary never
	// includes it. The shim activates only via the env var below. The
	// build is shared via TestMain (journalpump_process_test.go).
	bin := goBinaryPath

	// Stage the fixture journals.
	logsDir := filepath.Join(t.TempDir(), "logs")
	if err := os.MkdirAll(logsDir, 0o755); err != nil {
		t.Fatalf("mkdir logs: %v", err)
	}
	journals, err := filepath.Glob(filepath.Join(repoRoot, "systest", "data", "rotated_logs", "*.journal"))
	if err != nil || len(journals) == 0 {
		t.Fatalf("glob journals: %v (%d)", err, len(journals))
	}
	for _, journal := range journals {
		data, err := os.ReadFile(journal)
		if err != nil {
			t.Fatalf("read journal: %v", err)
		}
		if err := os.WriteFile(filepath.Join(logsDir, filepath.Base(journal)), data, 0o644); err != nil {
			t.Fatalf("stage journal: %v", err)
		}
	}

	outputPath := filepath.Join(t.TempDir(), "out.jsonl")
	statePath := filepath.Join(t.TempDir(), "state.json")
	config := map[string]any{
		"readers": map[string]any{
			"my-reader": map[string]any{
				"journal_path":     logsDir,
				"initial_position": "head",
				"senders": map[string]any{
					"out": map[string]any{
						"output_type":        "file",
						"file_output":        outputPath,
						"extra_field_values": map[string]any{"PADDING": strings.Repeat("x", 3072)},
					},
				},
			},
		},
		"json_state_file_path": statePath,
	}
	configPath := filepath.Join(t.TempDir(), "journalpump.json")
	configBytes, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	if err := os.WriteFile(configPath, configBytes, 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cmd := exec.Command(bin, configPath)
	cmd.Env = append(os.Environ(), "JOURNALPUMP_FILES_AVAILABLE=1")
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start binary: %v", err)
	}
	defer func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_, _ = cmd.Process.Wait()
		}
	}()

	// Wait for the reader/sender to make progress: the file sender
	// buffers (Python-faithful 131072 bytes), so the first visible line
	// appears only once the buffer overflows — by which point the reader
	// has consumed nearly the whole journal. Give it a moment to finish,
	// then SIGTERM: the D9 grace window flushes the buffered tail, which
	// is how all 40 lines reach the file.
	deadline := time.Now().Add(15 * time.Second)
	for lineCount(outputPath) < 1 {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for output; stderr:\n%s", stderr.String())
		}
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(time.Second)

	// SIGTERM drives the D9 shutdown (and the file-sender flush).
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("SIGTERM: %v", err)
	}
	waitErr := cmd.Wait()
	if cmd.ProcessState == nil {
		t.Fatalf("no process state after wait")
	}
	if code := cmd.ProcessState.ExitCode(); code != 0 {
		t.Fatalf("exit status = %d, want 0; wait err = %v; stderr:\n%s", code, waitErr, stderr.String())
	}

	// Output bytes: all 40 lines, byte-for-byte against the Python
	// capture.
	raw, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	lines := strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n")
	if len(lines) != len(fixture.Expected.Lines) {
		t.Fatalf("output lines = %d, want %d", len(lines), len(fixture.Expected.Lines))
	}
	for i, want := range fixture.Expected.Lines {
		if lines[i] != want {
			t.Errorf("line %d mismatch:\n got %s\nwant %s", i, lines[i], want)
		}
	}

	// State file: sent count + cursor.
	stateBytes, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatalf("read state: %v", err)
	}
	var st stateFile
	if err := json.Unmarshal(stateBytes, &st); err != nil {
		t.Fatalf("parse state: %v", err)
	}
	sent := st.Readers["my-reader"].Senders["out"].Sent
	if sent.Count != fixture.Expected.SentCount {
		t.Errorf("state sent.count = %d, want %d", sent.Count, fixture.Expected.SentCount)
	}
	if sent.Cursor != fixture.Expected.SentCursor {
		t.Errorf("state sent.cursor = %q, want %q", sent.Cursor, fixture.Expected.SentCursor)
	}
}

// lineCount counts the complete lines currently in path (0 when absent).
func lineCount(path string) int {
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	trimmed := strings.TrimSuffix(string(raw), "\n")
	if trimmed == "" {
		return 0
	}
	return strings.Count(trimmed, "\n") + 1
}
