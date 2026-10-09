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
	"testing"
	"time"
)

// stateFixture is the committed Python-written state-file capture
// (systest-go/fixtures/state_file.json): the outer leg of the D3
// round trip (plan Tasks 11/18).
type stateFixture struct {
	State json.RawMessage `json:"state"`
}

// loadStateFixture reads and returns the fixture's raw `state` document.
func loadStateFixture(t *testing.T) json.RawMessage {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(repoRootDir(t), "systest-go", "fixtures", "state_file.json"))
	if err != nil {
		t.Fatalf("read state fixture: %v", err)
	}
	var fixture stateFixture
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatalf("parse state fixture: %v", err)
	}
	if len(fixture.State) == 0 {
		t.Fatal("state fixture has no state document")
	}
	return fixture.State
}

// senderSent extracts readers.<reader>.senders.<sender>.sent from a
// state file.
func senderSent(t *testing.T, path, reader, sender string) (count int64, cursor string, cursorPresent bool) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read state file: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse state file: %v", err)
	}
	readers, _ := doc["readers"].(map[string]any)
	readerState, _ := readers[reader].(map[string]any)
	senders, _ := readerState["senders"].(map[string]any)
	senderState, _ := senders[sender].(map[string]any)
	sent, _ := senderState["sent"].(map[string]any)
	if sent == nil {
		t.Fatalf("state has no readers.%s.senders.%s.sent", reader, sender)
	}
	if c, ok := sent["count"].(float64); ok {
		count = int64(c)
	}
	cursor, cursorPresent = sent["cursor"].(string)
	return count, cursor, cursorPresent
}

// stateConfig builds a reader over logsDir with a json_state_file_path.
func stateConfig(logsDir, outputPath, statePath string) map[string]any {
	return map[string]any{
		"readers": map[string]any{
			"my-reader": map[string]any{
				"journal_path":     logsDir,
				"initial_position": "head",
				"senders": map[string]any{
					"out": map[string]any{"output_type": "file", "file_output": outputPath},
				},
			},
		},
		"json_state_file_path": statePath,
	}
}

// TestGoResumesFromPythonStateFile is the Python→Go leg of the D3 round
// trip: the committed Python-written state file is handed to the Go
// binary, which must resume at the recorded cursor (the journal is fully
// consumed), emit no new lines, and rewrite the state preserving the
// cursor. It then shells out to python3 to prove the Go-written state is
// still readable by Python (the Go→Python leg), closing the loop.
func TestGoResumesFromPythonStateFile(t *testing.T) {
	stateDoc := loadStateFixture(t)

	logs := stageJournals(t, "rotated_logs")
	statePath := filepath.Join(t.TempDir(), "state.json")
	if err := os.WriteFile(statePath, stateDoc, 0o644); err != nil {
		t.Fatalf("write state file: %v", err)
	}
	// The fixture's cursor is the end-of-journal position.
	_, wantCursor, ok := senderSent(t, statePath, "my-reader", "out")
	if !ok {
		t.Fatal("fixture state has no sent.cursor")
	}

	out := filepath.Join(t.TempDir(), "out.jsonl")
	p := startJournalpump(t, writeConfig(t, t.TempDir(), stateConfig(logs, out, statePath)))
	time.Sleep(2 * time.Second) // let the reader resume and idle
	if code := p.stop(); code != 0 {
		t.Fatalf("exit status = %d, want 0; stderr:\n%s", code, p.stderr.String())
	}

	// Resumed at the end: nothing new is written.
	if got := lineCount(out); got != 0 {
		t.Errorf("resumed run wrote %d lines, want 0 (cursor resume)", got)
	}
	// The Go rewrite preserves the Python cursor; its send count is the
	// run-relative count (0, nothing new).
	count, cursor, present := senderSent(t, statePath, "my-reader", "out")
	if !present || cursor != wantCursor {
		t.Errorf("rewritten cursor = %q (present=%v), want %q", cursor, present, wantCursor)
	}
	if count != 0 {
		t.Errorf("run-relative sent.count = %d, want 0", count)
	}

	// Go→Python: plain Python json.load accepts the Go-written file.
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available; Go→Python leg covered by pytest test/test_state_file_interop.py")
	}
	script := `import json,sys; d=json.load(open(sys.argv[1])); print(d["readers"]["my-reader"]["senders"]["out"]["sent"]["cursor"])`
	readCmd := exec.Command(python, "-c", script, statePath)
	readOut, err := readCmd.Output()
	if err != nil {
		t.Fatalf("python3 json.load of Go state failed: %v", err)
	}
	if got := string(readOut); got != wantCursor+"\n" {
		t.Errorf("python3 read cursor = %q, want %q", got, wantCursor+"\n")
	}
}

// TestGoCursorSurvivesRestart is the Go→Go restart leg: a full first run
// consumes the journal and saves its cursor; a second run resumed from
// that state emits nothing and preserves the cursor.
func TestGoCursorSurvivesRestart(t *testing.T) {
	logs := stageJournals(t, "rotated_logs")
	statePath := filepath.Join(t.TempDir(), "state.json")
	out1 := filepath.Join(t.TempDir(), "out1.jsonl")

	// journalConfig (not stateConfig) adds the large PADDING field so every
	// line exceeds the file sender's buffer and flushes mid-run; without it
	// the first run's 40 lines stay buffered until SIGTERM and the poll below
	// never sees them on disk.
	p1 := startJournalpump(t, writeConfig(t, t.TempDir(), journalConfig(logs, out1, statePath, nil)))
	deadline := time.Now().Add(30 * time.Second)
	for lineCount(out1) < 40 {
		if time.Now().After(deadline) {
			t.Fatalf("first run wrote %d lines, want 40", lineCount(out1))
		}
		time.Sleep(50 * time.Millisecond)
	}
	if code := p1.stop(); code != 0 {
		t.Fatalf("first run exit status = %d, want 0; stderr:\n%s", code, p1.stderr.String())
	}
	count1, cursor1, ok := senderSent(t, statePath, "my-reader", "out")
	if !ok || count1 != 40 {
		t.Fatalf("first run state count = %d (present=%v), want 40", count1, ok)
	}

	// Second run: same journals and state, fresh output.
	out2 := filepath.Join(t.TempDir(), "out2.jsonl")
	p2 := startJournalpump(t, writeConfig(t, t.TempDir(), journalConfig(logs, out2, statePath, nil)))
	time.Sleep(2 * time.Second)
	if code := p2.stop(); code != 0 {
		t.Fatalf("second run exit status = %d, want 0; stderr:\n%s", code, p2.stderr.String())
	}
	if got := lineCount(out2); got != 0 {
		t.Errorf("resumed restart wrote %d lines, want 0", got)
	}
	count2, cursor2, _ := senderSent(t, statePath, "my-reader", "out")
	if count2 != 0 {
		t.Errorf("restart run-relative sent.count = %d, want 0", count2)
	}
	if cursor2 != cursor1 {
		t.Errorf("restart cursor = %q, want preserved %q", cursor2, cursor1)
	}
}
