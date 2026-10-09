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

	// Go→Python: run REAL Python journalpump resume behavior against the
	// Go-written state — the same resume_cursor walk journalpump.py:889-905
	// performs, then an actual PumpReader seek_cursor + read. Resuming from
	// the written cursor must find no remaining messages (the cursor is the
	// end of the journal), proving the state drives Python's reader, not
	// merely that json.load accepts the bytes.
	resumed := runPython(t, interopHelperScript, "resume", logs, statePath)
	var got struct {
		ResumeCursor string   `json:"resume_cursor"`
		Remaining    []string `json:"remaining"`
	}
	if err := json.Unmarshal([]byte(resumed), &got); err != nil {
		t.Fatalf("parse python resume output: %v (%s)", err, resumed)
	}
	if got.ResumeCursor != wantCursor {
		t.Errorf("python resume_cursor = %q, want %q", got.ResumeCursor, wantCursor)
	}
	if len(got.Remaining) != 0 {
		t.Errorf("python resumed from the Go cursor and found %d remaining messages, want 0", len(got.Remaining))
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

// interopHelperScript drives REAL Python journalpump code for the
// Python↔Go state round trip. "write" reads the first N messages of a
// journal directory and emits a genuine Python-shaped state file with an
// INTERMEDIATE cursor (not the end of the journal) and the search hit
// counter Python's get_state records (journalpump.py:326-335). "resume"
// performs journalpump.py:889-905's resume_cursor walk over a state file
// and then actually seeks a PumpReader to that cursor and reads the
// remaining messages (get_reader's seek_cursor + skip, journalpump.py:448-452).
// Neither mode goes through the storage-origin gate, so no shim is needed.
const interopHelperScript = `
import json, sys
import systemd.journal as _journal
from journalpump.journalpump import PumpReader

_journal.Reader.has_persistent_files = lambda self: True
_journal.Reader.has_runtime_files = lambda self: True


def _reader(path):
    return PumpReader(path=path)


def _resume_cursor(reader_state):
    c = reader_state.get("cursor")
    for _name, sender in reader_state.get("senders", {}).items():
        sc = sender["sent"]["cursor"]
        if sc is None:
            return None
        c = sc
    return c


mode = sys.argv[1]
if mode == "write":
    logs, out_path, count, search_name = sys.argv[2], sys.argv[3], int(sys.argv[4]), sys.argv[5]
    r = _reader(logs)
    r.seek_head()
    messages = []
    cursor = None
    hits = 0
    while len(messages) < count:
        o = r.get_next()
        if o is None:
            break
        cursor = o.cursor
        m = o.entry.get("MESSAGE")
        if m is not None:
            messages.append(m)
            hits += 1
    state = {"readers": {"my-reader": {
        "cursor": cursor,
        "searches": {search_name: hits},
        "senders": {"out": {"sent": {"count": hits, "cursor": cursor}, "health": {}}},
        "total_lines": hits,
        "total_bytes": 0,
    }}, "start_time": "2026-10-09T13:00:00.000000"}
    with open(out_path, "w") as f:
        json.dump(state, f, indent=4, sort_keys=True)
    print(json.dumps({"messages": messages, "cursor": cursor, "hits": hits}))
else:
    logs, state_path = sys.argv[2], sys.argv[3]
    with open(state_path) as f:
        state = json.load(f)
    rs = state["readers"]["my-reader"]
    c = _resume_cursor(rs)
    remaining = []
    if c is not None:
        r = _reader(logs)
        r.seek_cursor(c)
        r.get_next()
        while True:
            o = r.get_next()
            if o is None:
                break
            m = o.entry.get("MESSAGE")
            if m is not None:
                remaining.append(m)
    print(json.dumps({"resume_cursor": c, "remaining": remaining, "searches": rs.get("searches", {}), "senders_cursor": rs["senders"]["out"]["sent"]["cursor"]}))
`

// runPython executes the interop helper script (from the repo root, so
// journalpump and the fixture journals resolve) and returns trimmed stdout.
func runPython(t *testing.T, script string, args ...string) string {
	t.Helper()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	root := repoRootDir(t)
	cmd := exec.Command(python, append([]string{"-c", script}, args...)...)
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "PYTHONPATH="+root)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("python helper %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

// stateCursorMaybe returns a reader/sender's saved cursor without failing
// when the state file is absent or not yet written (the reload poll).
func stateCursorMaybe(path, reader, sender string) (string, bool) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", false
	}
	var doc map[string]any
	if json.Unmarshal(raw, &doc) != nil {
		return "", false
	}
	readers, _ := doc["readers"].(map[string]any)
	readerState, _ := readers[reader].(map[string]any)
	senders, _ := readerState["senders"].(map[string]any)
	senderState, _ := senders[sender].(map[string]any)
	sent, _ := senderState["sent"].(map[string]any)
	if sent == nil {
		return "", false
	}
	cursor, _ := sent["cursor"].(string)
	return cursor, true
}

// stateSentCountMaybe returns a sender's saved sent.count without failing
// when the state file is absent or not yet written (a poll helper).
func stateSentCountMaybe(path, reader, sender string) (int64, bool) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	var doc map[string]any
	if json.Unmarshal(raw, &doc) != nil {
		return 0, false
	}
	readers, _ := doc["readers"].(map[string]any)
	readerState, _ := readers[reader].(map[string]any)
	senders, _ := readerState["senders"].(map[string]any)
	senderState, _ := senders[sender].(map[string]any)
	sent, _ := senderState["sent"].(map[string]any)
	if sent == nil {
		return 0, false
	}
	n, _ := sent["count"].(float64)
	return int64(n), true
}

// senderSearchHits reads readers.<reader>.searches.<search> from a state file.
func senderSearchHits(t *testing.T, path, reader, search string) (int64, bool) {
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
	searches, _ := readerState["searches"].(map[string]any)
	v, ok := searches[search].(float64)
	return int64(v), ok
}

// stateConfigWithSearch is stateConfig plus a reader-level search whose
// MESSAGE regex matches every fixture message, so its hit counter is a
// deterministic function of the messages delivered in the run.
func stateConfigWithSearch(logsDir, outputPath, statePath, searchName string) map[string]any {
	cfg := stateConfig(logsDir, outputPath, statePath)
	cfg["readers"].(map[string]any)["my-reader"].(map[string]any)["searches"] = []any{
		map[string]any{"name": searchName, "fields": map[string]any{"MESSAGE": "Message"}},
	}
	return cfg
}

// TestPythonToGoToPythonRoundTrip is the full D3 round trip with an
// INTERMEDIATE cursor: Python writes a state file positioned after message
// 19 (so 20 messages remain) with a recorded search hit count; Go resumes
// from it and must deliver exactly the remaining messages 20..39 (real
// resumed delivery, not an EOF no-op) and advance the cursor + record this
// run's search hits; Python then loads Go's state, runs the resume_cursor
// walk, and actually resumes from the end cursor, finding nothing left.
func TestPythonToGoToPythonRoundTrip(t *testing.T) {
	logs := stageJournals(t, "rotated_logs")
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.json")

	// Step 1: Python writes an intermediate state (cursor after Message 19).
	written := runPython(t, interopHelperScript, "write", logs, statePath, "20", "all-messages")
	var py struct {
		Messages []string `json:"messages"`
		Cursor   string   `json:"cursor"`
		Hits     int      `json:"hits"`
	}
	if err := json.Unmarshal([]byte(written), &py); err != nil {
		t.Fatalf("parse python write output: %v (%s)", err, written)
	}
	if py.Cursor == "" || py.Hits != 20 {
		t.Fatalf("python intermediate state: cursor=%q hits=%d, want a cursor and 20 hits", py.Cursor, py.Hits)
	}

	// Step 2: Go resumes from the intermediate cursor over the full journal.
	out := filepath.Join(dir, "out.jsonl")
	cfg := stateConfigWithSearch(logs, out, statePath, "all-messages")
	// Pad each line past the file sender's 128 KiB buffer so every message
	// flushes to disk as it is sent and the poll below observes real
	// delivery rather than waiting for the shutdown flush.
	cfg["readers"].(map[string]any)["my-reader"].(map[string]any)["senders"].(map[string]any)["out"].(map[string]any)["extra_field_values"] =
		map[string]any{"PADDING": strings.Repeat("x", 131088)}
	p := startJournalpump(t, writeConfig(t, t.TempDir(), cfg))
	waitForAtLeast(t, out, 20, 30*time.Second)
	if code := p.stop(); code != 0 {
		t.Fatalf("resumed Go exit status = %d, want 0; stderr:\n%s", code, p.stderr.String())
	}
	// Real resumed delivery: exactly the REMAINING messages, no re-send of
	// the already-consumed prefix.
	assertMessageSet(t, outputMessages(t, out), messageRange(20, 40))

	count, cursor, present := senderSent(t, statePath, "my-reader", "out")
	if !present || count != 20 {
		t.Fatalf("Go state sent.count = %d (present=%v), want 20 (the messages it delivered)", count, present)
	}
	if cursor == py.Cursor {
		t.Errorf("Go state cursor = %q, unchanged from the intermediate position", cursor)
	}
	if hits, ok := senderSearchHits(t, statePath, "my-reader", "all-messages"); !ok || hits != 20 {
		t.Errorf("Go state search hits = %d (present=%v), want 20 (this run's matches)", hits, ok)
	}

	// Step 3: Python loads Go's state and actually resumes read-only.
	resumed := runPython(t, interopHelperScript, "resume", logs, statePath)
	var back struct {
		ResumeCursor string           `json:"resume_cursor"`
		Remaining    []string         `json:"remaining"`
		Searches     map[string]int64 `json:"searches"`
	}
	if err := json.Unmarshal([]byte(resumed), &back); err != nil {
		t.Fatalf("parse python resume output: %v (%s)", err, resumed)
	}
	if back.ResumeCursor != cursor {
		t.Errorf("python resume_cursor = %q, want Go's %q", back.ResumeCursor, cursor)
	}
	if len(back.Remaining) != 0 {
		t.Errorf("python resumed from Go's cursor and found %d messages, want 0", len(back.Remaining))
	}
	if back.Searches["all-messages"] != 20 {
		t.Errorf("python read Go search counter = %d, want 20", back.Searches["all-messages"])
	}
}
