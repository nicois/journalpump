// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

//go:build systest

package systestgo

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// rotateSettle is a conservative wait after each rotation: the pump
// re-opens the journal handle ~2 s after libsystemd reports an
// invalidation, and the reader drains a whole batch within one poll
// once re-opened.
const rotateSettle = 3 * time.Second

// rotatePadding inflates each output line past the file sender's buffer
// boundary so lines reach disk mid-run and the tests can observe
// delivery progress (rather than only at SIGTERM). It also keeps every
// line far above the tiny byte budgets the boundary tests configure.
const rotatePadding = 100000

// rotator emulates journald's rotation algorithm (systest/test_rotation.py
// LogFiles, the behavior we port — not the Python monkeypatch mechanism):
//
//   - move user-1000.journal -> user-1000@<prev>.journal
//   - copy the next fixture journal to user-1000.journal
//   - delete old files
type rotator struct {
	t        *testing.T
	dest     string
	source   []string
	rotateTo string
	current  []string
}

// newRotator stages rotations from systest/data/<subdir>/*.journal in
// the same order as the Python LogFiles helper (reverse-sorted, popped
// from the end).
func newRotator(t *testing.T, dest, subdir string) *rotator {
	t.Helper()
	pattern := filepath.Join(repoRootDir(t), "systest", "data", subdir, "*.journal")
	source, err := filepath.Glob(pattern)
	if err != nil || len(source) == 0 {
		t.Fatalf("glob %s: %v (%d)", pattern, err, len(source))
	}
	// Reverse sort, matching Python's sorted(..., reverse=True).
	for i, j := 0, len(source)-1; i < j; i, j = i+1, j-1 {
		source[i], source[j] = source[j], source[i]
	}
	return &rotator{t: t, dest: dest, source: source}
}

func (r *rotator) tail() string { return filepath.Join(r.dest, "user-1000.journal") }

// rotate performs one rotation step.
func (r *rotator) rotate() {
	r.t.Helper()
	if len(r.source) == 0 {
		r.t.Fatal("no more source journals to rotate in")
	}
	tail := r.tail()
	if r.rotateTo != "" {
		if err := os.Rename(tail, r.rotateTo); err != nil {
			r.t.Fatalf("move %s -> %s: %v", tail, r.rotateTo, err)
		}
		// Python inserts the moved file at index 1 (current[0] stays the
		// tail path, which the copy below repoints at the new file).
		r.current = append(r.current, "")
		copy(r.current[2:], r.current[1:])
		r.current[1] = r.rotateTo
	} else {
		r.current = append(r.current, tail)
	}
	src := r.source[len(r.source)-1]
	r.source = r.source[:len(r.source)-1]
	data, err := os.ReadFile(src)
	if err != nil {
		r.t.Fatalf("read source %s: %v", src, err)
	}
	if err := os.WriteFile(tail, data, 0o644); err != nil {
		r.t.Fatalf("write tail %s: %v", tail, err)
	}
	r.rotateTo = filepath.Join(r.dest, filepath.Base(src))
}

// remove deletes the last `last` known files, mirroring LogFiles.remove.
func (r *rotator) remove(last int) {
	r.t.Helper()
	for range last {
		if len(r.current) == 0 {
			r.t.Fatal("no known files to remove")
		}
		victim := r.current[len(r.current)-1]
		r.current = r.current[:len(r.current)-1]
		if err := os.Remove(victim); err != nil {
			r.t.Fatalf("remove %s: %v", victim, err)
		}
		if len(r.current) == 0 {
			r.rotateTo = ""
		}
	}
}

// repoRootDir returns the repository root (the parent of systest-go).
func repoRootDir(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatalf("repo root: %v", err)
	}
	return root
}

// stageJournals copies every fixture journal of systest/data/<subdir>
// into a fresh directory and returns it (the static, non-rotating
// fixture layout).
func stageJournals(t *testing.T, subdir string) string {
	t.Helper()
	pattern := filepath.Join(repoRootDir(t), "systest", "data", subdir, "*.journal")
	journals, err := filepath.Glob(pattern)
	if err != nil || len(journals) == 0 {
		t.Fatalf("glob %s: %v (%d)", pattern, err, len(journals))
	}
	logs := filepath.Join(t.TempDir(), "logs")
	if err := os.MkdirAll(logs, 0o755); err != nil {
		t.Fatalf("mkdir logs: %v", err)
	}
	for _, journal := range journals {
		data, err := os.ReadFile(journal)
		if err != nil {
			t.Fatalf("read journal: %v", err)
		}
		if err := os.WriteFile(filepath.Join(logs, filepath.Base(journal)), data, 0o644); err != nil {
			t.Fatalf("stage journal: %v", err)
		}
	}
	return logs
}

// journalConfig builds a reader config over logsDir writing JSONL to
// outputPath, with the given top-level extra keys (msg_buffer_max_*).
func journalConfig(logsDir, outputPath, statePath string, extra map[string]any) map[string]any {
	config := map[string]any{
		"readers": map[string]any{
			"my-reader": map[string]any{
				"journal_path":     logsDir,
				"initial_position": "head",
				"senders": map[string]any{
					"out": map[string]any{
						"output_type":        "file",
						"file_output":        outputPath,
						"extra_field_values": map[string]any{"PADDING": strings.Repeat("x", rotatePadding)},
					},
				},
			},
		},
	}
	if statePath != "" {
		config["json_state_file_path"] = statePath
	}
	for k, v := range extra {
		config[k] = v
	}
	return config
}

// waitForAtLeast blocks until path holds at least want complete lines.
//
// The file sender buffers, so at any instant the most recent line (or
// two) may not yet be on disk; callers waiting for N complete lines use
// want = N-1 as the progress signal and assert the exact N after
// SIGTERM flushes the tail.
func waitForAtLeast(t *testing.T, path string, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for countNewlines(path) < want {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d lines; got %d", want, countNewlines(path))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// countNewlines counts complete (newline-terminated) output lines.
func countNewlines(path string) int {
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	return strings.Count(string(raw), "\n")
}

// outputMessages reads outputPath and returns the MESSAGE field of every
// complete JSON line.
func outputMessages(t *testing.T, outputPath string) []string {
	t.Helper()
	raw, err := os.ReadFile(outputPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatalf("read output: %v", err)
	}
	var messages []string
	for _, line := range splitLines(string(raw)) {
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("parse output line %q: %v", line, err)
		}
		message, _ := entry["MESSAGE"].(string)
		messages = append(messages, message)
	}
	return messages
}

func splitLines(s string) []string {
	var lines []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			if i > start {
				lines = append(lines, s[start:i])
			}
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}

// assertMessageSet asserts the delivered MESSAGE set equals want.
func assertMessageSet(t *testing.T, got, want []string) {
	t.Helper()
	seen := map[string]bool{}
	for _, m := range got {
		seen[m] = true
	}
	if len(got) != len(want) {
		t.Errorf("delivered %d messages, want %d: %v", len(got), len(want), got)
	}
	for _, w := range want {
		if !seen[w] {
			t.Errorf("missing message %q", w)
		}
	}
}

// assertMessageOrder asserts the delivered messages equal want in order —
// a reordered delivery (a broken admission/drain sequence) passes the
// set-based assertion above but fails here.
func assertMessageOrder(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("delivered %d messages, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("message %d = %q, want %q (out of order)", i, got[i], want[i])
		}
	}
}

func messageRange(lo, hi int) []string {
	out := make([]string, 0, hi-lo)
	for i := lo; i < hi; i++ {
		out = append(out, fmt.Sprintf("Message %d", i))
	}
	return out
}

// TestGoRotationThirtyMessagesAcrossRotation ports the behavior of
// test_journalpump_rotated_files: one rotation delivers Messages 0..9,
// two more deliver Messages 10..29, driven through the real binary.
func TestGoRotationThirtyMessagesAcrossRotation(t *testing.T) {
	logs := t.TempDir()
	out := filepath.Join(t.TempDir(), "out.jsonl")
	config := journalConfig(logs, out, "", nil)
	p := startJournalpump(t, writeConfig(t, t.TempDir(), config))
	r := newRotator(t, logs, "rotated_logs")

	r.rotate()
	time.Sleep(rotateSettle)

	r.rotate()
	r.rotate()
	time.Sleep(rotateSettle)

	if code := p.stop(); code != 0 {
		t.Fatalf("exit status = %d, want 0; stderr:\n%s", code, p.stderr.String())
	}
	assertMessageSet(t, outputMessages(t, out), messageRange(0, 30))
}

// TestGoRotationBufferLengthBoundaries ports
// test_journalpump_rotated_files_threshold: every msg_buffer_max_length
// still delivers all 30 rotated messages (the batching boundaries lose
// nothing).
func TestGoRotationBufferLengthBoundaries(t *testing.T) {
	for _, maxLength := range []int{3, 5, 10} {
		t.Run(fmt.Sprintf("max_length_%d", maxLength), func(t *testing.T) {
			logs := t.TempDir()
			out := filepath.Join(t.TempDir(), "out.jsonl")
			config := journalConfig(logs, out, "", map[string]any{"msg_buffer_max_length": maxLength})
			p := startJournalpump(t, writeConfig(t, t.TempDir(), config))
			r := newRotator(t, logs, "rotated_logs")
			r.rotate()
			time.Sleep(rotateSettle)
			r.rotate()
			r.rotate()
			time.Sleep(rotateSettle)
			waitForAtLeast(t, out, 29, 150*time.Second)
			if code := p.stop(); code != 0 {
				t.Fatalf("exit status = %d, want 0; stderr:\n%s", code, p.stderr.String())
			}
			// Ordered, not just set-equal: a broken admission/drain
			// sequence that reorders messages must fail here.
			assertMessageOrder(t, outputMessages(t, out), messageRange(0, 30))
		})
	}
}

// bufferBytesPadding sizes each output line to ~600 bytes so that a
// msg_buffer_max_bytes budget of 50 admits exactly one message and 1000
// admits exactly two (ceil(B/P) ≈ 1 and 2, the Python suite's
// size/num_messages pairs). The previous value (rotatePadding, 100000)
// made every message exceed BOTH budgets, collapsing the two cases into
// one oversized-message admission.
const bufferBytesPadding = 200

// TestGoRotationBufferBytesBoundaries ports
// test_journalpump_rotated_files_threshold_bytes: byte-limited buffers of
// 50 (one message per drain) and 1000 (two per drain) still deliver all
// 30 rotated messages, in order and without loss. The EXACT per-drain
// admission boundary is asserted byte-for-byte by the in-process test
// TestReadMessagesHonorsBufferBoundaries (internal/daemon): a
// file-sender systest observes only end-to-end output, so an
// end-to-end test alone cannot pin the boundary counts.
func TestGoRotationBufferBytesBoundaries(t *testing.T) {
	for _, tc := range []struct {
		size        int
		numMessages int
	}{{50, 1}, {1000, 2}} {
		t.Run(fmt.Sprintf("max_bytes_%d_%d_messages", tc.size, tc.numMessages), func(t *testing.T) {
			logs := t.TempDir()
			out := filepath.Join(t.TempDir(), "out.jsonl")
			statePath := filepath.Join(t.TempDir(), "state.json")
			config := journalConfig(logs, out, statePath, map[string]any{"msg_buffer_max_bytes": tc.size})
			// Replace the oversized default padding with a controlled size
			// that keeps both byte budgets non-degenerate (the Python
			// suite's 50→1 and 1000→2 message pairs). Small lines never
			// fill the file sender's 128 KiB buffer and it has no mid-run
			// flush, so delivery is observed via the state file's
			// sent.count and asserted after SIGTERM flushes the tail.
			sender := config["readers"].(map[string]any)["my-reader"].(map[string]any)["senders"].(map[string]any)["out"].(map[string]any)
			sender["extra_field_values"] = map[string]any{"PADDING": strings.Repeat("x", bufferBytesPadding)}

			p := startJournalpump(t, writeConfig(t, t.TempDir(), config))
			r := newRotator(t, logs, "rotated_logs")
			r.rotate()
			time.Sleep(rotateSettle)
			r.rotate()
			r.rotate()
			time.Sleep(rotateSettle)

			// Wait until the byte-gated reader has actually drained all 30
			// messages into the sender (recorded by the periodic state save).
			deadline := time.Now().Add(90 * time.Second)
			for {
				if n, ok := stateSentCountMaybe(statePath, "my-reader", "out"); ok && n >= 30 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("reader did not deliver 30 messages under max_bytes=%d; stderr:\n%s", tc.size, p.stderr.String())
				}
				time.Sleep(200 * time.Millisecond)
			}
			if code := p.stop(); code != 0 {
				t.Fatalf("exit status = %d, want 0; stderr:\n%s", code, p.stderr.String())
			}
			assertMessageOrder(t, outputMessages(t, out), messageRange(0, 30))
		})
	}
}

// TestGoRotationCorruptEntrySkip ports test_journalpump_bad_message:
// corrupt entries are skipped without aborting; after two rotations the
// surviving Messages 10..19 arrive.
func TestGoRotationCorruptEntrySkip(t *testing.T) {
	logs := t.TempDir()
	out := filepath.Join(t.TempDir(), "out.jsonl")
	config := journalConfig(logs, out, "", nil)
	p := startJournalpump(t, writeConfig(t, t.TempDir(), config))
	r := newRotator(t, logs, "bad_msg")

	r.rotate()
	time.Sleep(rotateSettle)
	r.rotate()
	time.Sleep(rotateSettle)

	if code := p.stop(); code != 0 {
		t.Fatalf("exit status = %d, want 0; stderr:\n%s", code, p.stderr.String())
	}
	assertMessageSet(t, outputMessages(t, out), messageRange(10, 20))
}

// TestGoRotationDeletedFileFdRelease ports the observable intent of
// test_journalpump_rotated_files_deletion: after an already-read rotated
// file is deleted, the process keeps running, holds no open descriptor
// referencing the deleted path, and keeps delivering later messages.
func TestGoRotationDeletedFileFdRelease(t *testing.T) {
	logs := t.TempDir()
	out := filepath.Join(t.TempDir(), "out.jsonl")
	config := journalConfig(logs, out, "", nil)
	p := startJournalpump(t, writeConfig(t, t.TempDir(), config))
	r := newRotator(t, logs, "rotated_logs")

	r.rotate() // file A -> Messages 0..9
	time.Sleep(rotateSettle)

	r.rotate() // file B -> Messages 10..19, A renamed aside
	time.Sleep(rotateSettle)

	// The oldest known file (A, fully read) is removed.
	if len(r.current) < 2 {
		t.Fatalf("rotation bookkeeping wrong: %v", r.current)
	}
	oldest := r.current[len(r.current)-1]
	r.remove(1)

	// No open descriptor may reference the deleted file.
	for _, target := range openFdTargets(t, p.cmd.Process.Pid) {
		if target == oldest || target == oldest+" (deleted)" {
			t.Errorf("deleted journal %s still open as %s", oldest, target)
		}
	}

	// The process survives and keeps serving the reader.
	time.Sleep(rotateSettle)
	r.rotate() // file C -> Messages 20..29
	time.Sleep(rotateSettle)

	if code := p.stop(); code != 0 {
		t.Fatalf("exit status = %d, want 0; stderr:\n%s", code, p.stderr.String())
	}
	assertMessageSet(t, outputMessages(t, out), messageRange(0, 30))
}

// openFdTargets returns the readlink target of every open fd of pid.
func openFdTargets(t *testing.T, pid int) []string {
	t.Helper()
	entries, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", pid))
	if err != nil {
		return nil
	}
	var targets []string
	for _, e := range entries {
		target, err := os.Readlink(filepath.Join(fmt.Sprintf("/proc/%d/fd", pid), e.Name()))
		if err == nil {
			targets = append(targets, target)
		}
	}
	return targets
}
