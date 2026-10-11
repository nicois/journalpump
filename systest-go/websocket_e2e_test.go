// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

//go:build systest

package systestgo

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/gorilla/websocket"
)

// websocketFixture mirrors the committed capture of
// systest/test_groundtruth_websocket.py (all 40 messages, default config:
// batching on + snappy compression).
type websocketFixture struct {
	ConnectHeaders map[string]string `json:"connect_headers"`
	Messages       []string          `json:"messages"`
	Expected       struct {
		SentCount  int64  `json:"sent_count"`
		SentCursor string `json:"sent_cursor"`
	} `json:"expected"`
}

// wsCaptureServer records the handshake headers and every binary frame.
type wsCaptureServer struct {
	srv      *httptest.Server
	upgrader websocket.Upgrader

	mu      sync.Mutex
	headers http.Header
	frames  [][]byte
}

func newWSCaptureServer() *wsCaptureServer {
	s := &wsCaptureServer{}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.headers = r.Header.Clone()
		s.mu.Unlock()
		conn, err := s.upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			s.mu.Lock()
			s.frames = append(s.frames, append([]byte(nil), data...))
			s.mu.Unlock()
		}
	}))
	return s
}

func (s *wsCaptureServer) url() string {
	return "ws" + strings.TrimPrefix(s.srv.URL, "http") + "/pump-pump"
}

func (s *wsCaptureServer) capturedFrames() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([][]byte, len(s.frames))
	copy(out, s.frames)
	return out
}

func (s *wsCaptureServer) firstHeaders() http.Header {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.headers.Clone()
}

// messageCount decompresses every frame and counts the NUL-separated
// messages (the fixture stores the decompressed stream because batch
// boundaries are timing-dependent).
func (s *wsCaptureServer) messageCount() int {
	n := 0
	for _, frame := range s.capturedFrames() {
		dec, err := snappy.Decode(nil, frame)
		if err != nil {
			continue
		}
		n += len(strings.Split(string(dec), "\x00"))
	}
	return n
}

// TestWebsocketSenderEndToEnd launches the REAL Go binary against an
// in-process websocket server and asserts the frames byte-match the Task
// 11 capture: snappy(NUL-join(batch)) frames whose decompressed stream is
// the fixture's 40 messages, the connect headers, and the state file.
func TestWebsocketSenderEndToEnd(t *testing.T) {
	repoRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatalf("repo root: %v", err)
	}
	rawFixture, err := os.ReadFile(filepath.Join(repoRoot, "systest-go", "fixtures", "websocket.json"))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var fixture websocketFixture
	if err := json.Unmarshal(rawFixture, &fixture); err != nil {
		t.Fatalf("parse fixture: %v", err)
	}
	if len(fixture.Messages) != 40 {
		t.Fatalf("fixture messages = %d, want 40", len(fixture.Messages))
	}

	srv := newWSCaptureServer()
	defer srv.srv.Close()

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

	statePath := filepath.Join(t.TempDir(), "state.json")
	config := map[string]any{
		"readers": map[string]any{
			"my-reader": map[string]any{
				"journal_path":     logsDir,
				"initial_position": "head",
				"senders": map[string]any{
					"out": map[string]any{
						"output_type":   "websocket",
						"websocket_uri": srv.url(),
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

	deadline := time.Now().Add(30 * time.Second)
	for srv.messageCount() < 40 {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for 40 messages (got %d); stderr:\n%s", srv.messageCount(), stderr.String())
		}
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(time.Second)

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

	// Frames: decompress each snappy frame, NUL-split, and compare the
	// concatenated stream to the fixture.
	var messages []string
	for _, frame := range srv.capturedFrames() {
		dec, err := snappy.Decode(nil, frame)
		if err != nil {
			t.Fatalf("frame is not snappy: %v", err)
		}
		messages = append(messages, strings.Split(string(dec), "\x00")...)
	}
	if len(messages) != len(fixture.Messages) {
		t.Fatalf("messages = %d, want %d", len(messages), len(fixture.Messages))
	}
	for i, want := range fixture.Messages {
		if messages[i] != want {
			t.Errorf("message %d mismatch:\n got %s\nwant %s", i, messages[i], want)
		}
	}

	// Headers: batch-size and compression exact; user-agent has the Go
	// binary's version (the fixture carries the Python version string, so
	// only the journalpump/ prefix is compared).
	h := srv.firstHeaders()
	if got := h.Get("Journalpump-Batch-Size"); got != fixture.ConnectHeaders["journalpump-batch-size"] {
		t.Errorf("Journalpump-Batch-Size = %q, want %q", got, fixture.ConnectHeaders["journalpump-batch-size"])
	}
	if got := h.Get("Journalpump-Compression"); got != fixture.ConnectHeaders["journalpump-compression"] {
		t.Errorf("Journalpump-Compression = %q, want %q", got, fixture.ConnectHeaders["journalpump-compression"])
	}
	if got := h.Get("User-Agent"); !strings.HasPrefix(got, "journalpump/") {
		t.Errorf("User-Agent = %q, want journalpump/<version>", got)
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
