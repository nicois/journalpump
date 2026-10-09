// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package file

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Aiven-Open/journalpump/internal/buffer"
	"github.com/Aiven-Open/journalpump/internal/journal"
	"github.com/Aiven-Open/journalpump/internal/reader"
	"github.com/Aiven-Open/journalpump/internal/sender"
	"github.com/Aiven-Open/journalpump/internal/tagged"
)

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

// fixturePath resolves the systest data fixture from the package dir
// (internal/sender/file -> repo root).
func fixturePath(parts ...string) string {
	return filepath.Join(append([]string{"..", "..", ".."}, parts...)...)
}

// TestFileSenderGoldenPipeline drives the real reader -> handler ->
// buffer -> file sender pipeline over the Task 11 rotated-log fixture and
// asserts ALL 40 output lines byte-for-byte against the captured ground
// truth, plus the loop's sent count and cursor.
func TestFileSenderGoldenPipeline(t *testing.T) {
	rawFixture, err := os.ReadFile(fixturePath("systest-go", "fixtures", "file_sender.json"))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var fixture fileSenderFixture
	if err := json.Unmarshal(rawFixture, &fixture); err != nil {
		t.Fatalf("parse fixture: %v", err)
	}

	reader.FilesAvailableOverride = func(bool, bool) bool { return true }
	t.Cleanup(func() { reader.FilesAvailableOverride = nil })

	r, err := reader.Open(reader.OpenConfig{Path: fixturePath("systest", "data", "rotated_logs")})
	if err != nil {
		t.Fatalf("open fixture journal: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })

	outPath := filepath.Join(t.TempDir(), "out.log")
	inst, err := New(sender.ConstructorArgs{
		Name:   "s",
		Config: map[string]any{"output_type": "file", "file_output": outPath},
		Tags:   tagged.New(nil),
	})
	if err != nil {
		t.Fatalf("file.New: %v", err)
	}

	buf := buffer.NewSenderBuffer("s", 50000)
	loop := sender.NewLoop(sender.LoopConfig{Name: "s", Tags: map[string]string{}}, inst, buf.Chan())
	buf.Attach(loop)
	if inst.ConnectOnInit {
		loop.MarkConnected()
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = loop.Run(ctx)
	}()

	var extras []journal.Field
	for key, value := range fixture.Capture.Sender.ExtraFieldValues {
		extras = append(extras, journal.Field{Key: key, Value: value})
	}
	target := journal.SenderTarget{
		Name:             "s",
		ExtraFieldValues: extras,
		AddItem: func(item []byte, cursor string) {
			if err := buf.Enqueue(ctx, sender.Item{Payload: item, Cursor: cursor}); err != nil {
				t.Errorf("enqueue: %v", err)
			}
		},
	}

	processed := 0
	for {
		batch, err := r.ReadBatch(5000)
		if err != nil {
			t.Fatalf("ReadBatch: %v", err)
		}
		if len(batch) == 0 {
			break
		}
		for _, entry := range batch {
			// One handler per message, like the pump (and Python's
			// per-message JournalObjectHandler): the serialization cache
			// must never span entries.
			handler := journal.NewHandler(journal.HandlerConfig{ReaderName: "r"})
			if _, _, _, err := handler.Process(entry, []journal.SenderTarget{target}); err != nil {
				t.Fatalf("Process: %v", err)
			}
			processed++
		}
	}
	if processed != int(fixture.Expected.SentCount) {
		t.Fatalf("processed = %d, want %d", processed, fixture.Expected.SentCount)
	}

	// Let the sender loop reach its flush tick, then stop it.
	time.Sleep(300 * time.Millisecond)
	cancel()
	wg.Wait()
	if closer, ok := inst.Transport.(interface{ Close() error }); ok {
		if err := closer.Close(); err != nil {
			t.Fatalf("close sender: %v", err)
		}
	}

	rawOutput, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	if len(rawOutput) == 0 || rawOutput[len(rawOutput)-1] != '\n' {
		t.Fatalf("output is empty or unterminated")
	}
	lines := strings.Split(strings.TrimSuffix(string(rawOutput), "\n"), "\n")
	if len(lines) != int(fixture.Expected.SentCount) {
		t.Fatalf("output lines = %d, want %d", len(lines), fixture.Expected.SentCount)
	}
	for i, want := range fixture.Expected.Lines {
		if lines[i] != want {
			t.Errorf("line %d mismatch:\n got %s\nwant %s", i, lines[i], want)
		}
	}

	st := loop.State()
	if st.Sent.Count != fixture.Expected.SentCount {
		t.Errorf("sent.count = %d, want %d", st.Sent.Count, fixture.Expected.SentCount)
	}
	if st.Sent.Cursor == nil || *st.Sent.Cursor != fixture.Expected.SentCursor {
		t.Errorf("sent.cursor = %v, want %s", st.Sent.Cursor, fixture.Expected.SentCursor)
	}
	if st.Type != "FileSender" {
		t.Errorf("state type = %q, want FileSender", st.Type)
	}
}

// TestBufferedVisibilityAndFlushOnClose locks Python's BufferedWriter
// semantics: a small batch stays in the buffer (not visible on disk),
// and the explicit flush on Close (the D9 deviation) makes it visible.
func TestBufferedVisibilityAndFlushOnClose(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "out.log")
	inst, err := New(sender.ConstructorArgs{
		Name:   "s",
		Config: map[string]any{"output_type": "file", "file_output": outPath},
		Tags:   tagged.New(nil),
	})
	if err != nil {
		t.Fatalf("file.New: %v", err)
	}
	tr, ok := inst.Transport.(*Sender)
	if !ok {
		t.Fatalf("transport type %T", inst.Transport)
	}

	if err := tr.SendMessages(context.Background(), [][]byte{[]byte("a"), []byte("b")}, ""); err != nil {
		t.Fatalf("SendMessages: %v", err)
	}
	// Small writes stay buffered (Python never flushes mid-run).
	info, err := os.Stat(outPath)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Size() != 0 {
		t.Errorf("size after buffered write = %d, want 0 (not yet flushed)", info.Size())
	}

	if err := tr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	raw, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got := string(raw); got != "a\nb\n" {
		t.Errorf("output = %q, want \"a\\nb\\n\"", got)
	}
}

// newTestSender builds a file Sender writing to a fresh temp path.
func newTestSender(t *testing.T) (*Sender, string) {
	t.Helper()
	return newTestSenderAt(t, filepath.Join(t.TempDir(), "out.log"))
}

// newTestSenderAt builds a file Sender appending to path (a restart on the
// same output).
func newTestSenderAt(t *testing.T, path string) (*Sender, string) {
	t.Helper()
	inst, err := New(sender.ConstructorArgs{
		Name:   "s",
		Config: map[string]any{"output_type": "file", "file_output": path},
		Tags:   tagged.New(nil),
	})
	if err != nil {
		t.Fatalf("file.New: %v", err)
	}
	tr, ok := inst.Transport.(*Sender)
	if !ok {
		t.Fatalf("transport type %T", inst.Transport)
	}
	return tr, path
}

// TestFlushedPayloadBytesLagsUntilFlush locks the delivery checkpoint at
// the file layer: accepted (sent) lines are not durable while they sit in
// the buffer, so the reported flushed watermark stays at zero until the
// explicit Finalize flush (the D9 shutdown grace). This is what makes the
// persisted cursor reflect bytes on disk, not send() completion.
func TestFlushedPayloadBytesLagsUntilFlush(t *testing.T) {
	tr, _ := newTestSender(t)

	if err := tr.SendMessages(context.Background(), [][]byte{[]byte("a"), []byte("bb")}, ""); err != nil {
		t.Fatalf("SendMessages: %v", err)
	}
	// Both lines are accepted but buffered: no payload byte is durable.
	if got := tr.FlushedPayloadBytes(); got != 0 {
		t.Errorf("flushed = %d, want 0 (still buffered)", got)
	}

	if err := tr.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	if got := tr.FlushedPayloadBytes(); got != 3 {
		t.Errorf("flushed = %d after Finalize, want 3 (a + bb)", got)
	}
}

// TestFlushedPayloadBytesAdvancesWhenBufferFills locks the auto-flush
// boundary: like Python's BufferedWriter, the output flushes when the
// 131072-byte buffer fills, so the durable watermark advances without an
// explicit Finalize while the tail stays unflushed.
func TestFlushedPayloadBytesAdvancesWhenBufferFills(t *testing.T) {
	tr, _ := newTestSender(t)

	const chunkSize = 20000
	const chunks = 8 // 160000 > pythonBufferSize (131072)
	chunk := bytes.Repeat([]byte("x"), chunkSize)
	batch := make([][]byte, chunks)
	for i := range batch {
		batch[i] = chunk
	}
	if err := tr.SendMessages(context.Background(), batch, ""); err != nil {
		t.Fatalf("SendMessages: %v", err)
	}

	total := int64(chunkSize) * chunks
	flushed := tr.FlushedPayloadBytes()
	if flushed <= 0 {
		t.Fatalf("flushed = %d, want > 0 once the buffer filled", flushed)
	}
	if flushed >= total {
		t.Fatalf("flushed = %d, want < %d (the tail is still buffered)", flushed, total)
	}

	if err := tr.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	if got := tr.FlushedPayloadBytes(); got != total {
		t.Errorf("flushed = %d after Finalize, want %d", got, total)
	}
}

// TestCloseDiscardingDropsUnflushedTail locks the reload-retire contract:
// the unflushed tail is DROPPED (not flushed to disk), matching the fact
// that the persisted cursor never claimed those lines — so a replay after
// the reload is safe and cannot duplicate them.
func TestCloseDiscardingDropsUnflushedTail(t *testing.T) {
	tr, outPath := newTestSender(t)

	if err := tr.SendMessages(context.Background(), [][]byte{[]byte("dropped")}, ""); err != nil {
		t.Fatalf("SendMessages: %v", err)
	}
	if got := tr.FlushedPayloadBytes(); got != 0 {
		t.Fatalf("flushed = %d, want 0 (buffered)", got)
	}

	if err := tr.CloseDiscarding(); err != nil {
		t.Fatalf("CloseDiscarding: %v", err)
	}
	info, err := os.Stat(outPath)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Size() != 0 {
		t.Errorf("size after discarding = %d, want 0 (tail dropped)", info.Size())
	}
	if got := tr.FlushedPayloadBytes(); got != 0 {
		t.Errorf("flushed = %d after discarding, want 0", got)
	}
}

// TestRestartAfterPartialFlushReplaysCleanly locks the end-to-end
// guarantee: a bufio auto-flush can cut through a line, leaving a JSON
// prefix on disk; discarding truncates it back to the last complete line,
// so a replay from that edge appends a clean line (valid JSONL, no
// corrupted partial object).
func TestRestartAfterPartialFlushReplaysCleanly(t *testing.T) {
	tr, outPath := newTestSender(t)

	if err := tr.SendMessages(context.Background(), [][]byte{[]byte("first")}, ""); err != nil {
		t.Fatalf("SendMessages: %v", err)
	}
	big := bytes.Repeat([]byte("x"), pythonBufferSize+10)
	if err := tr.SendMessages(context.Background(), [][]byte{big}, ""); err != nil {
		t.Fatalf("SendMessages: %v", err)
	}
	info, err := os.Stat(outPath)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Size() <= int64(len("first\n")) {
		t.Fatalf("size = %d, want the complete line plus a partial prefix", info.Size())
	}

	// Discard (the failed-flush/reload path) removes the partial prefix.
	if err := tr.CloseDiscarding(); err != nil {
		t.Fatalf("CloseDiscarding: %v", err)
	}

	// Restart: a fresh sender re-appends the unflushed line.
	tr2, _ := newTestSenderAt(t, outPath)
	if err := tr2.SendMessages(context.Background(), [][]byte{big}, ""); err != nil {
		t.Fatalf("replay SendMessages: %v", err)
	}
	if err := tr2.Close(); err != nil {
		t.Fatalf("replay Close: %v", err)
	}

	raw, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	want := "first\n" + string(big) + "\n"
	if string(raw) != want {
		t.Fatalf("output length = %d, want %d (clean replay, no corrupt partial)", len(raw), len(want))
	}
}

// TestDiscardTruncatesToLastCompleteLine locks the clean-edge guarantee:
// when a flush cuts through a line, only the partial line is removed — the
// preceding complete line stays, so replay resumes from a valid edge.
func TestDiscardTruncatesToLastCompleteLine(t *testing.T) {
	tr, outPath := newTestSender(t)

	if err := tr.SendMessages(context.Background(), [][]byte{[]byte("first")}, ""); err != nil {
		t.Fatalf("SendMessages: %v", err)
	}
	// The next write overflows the buffer, flushing "first\n" plus a
	// partial prefix of the big line.
	big := bytes.Repeat([]byte("x"), pythonBufferSize+10)
	if err := tr.SendMessages(context.Background(), [][]byte{big}, ""); err != nil {
		t.Fatalf("SendMessages: %v", err)
	}
	if got := tr.FlushedPayloadBytes(); got != int64(len("first")) {
		t.Fatalf("flushed = %d, want %d (only the complete line)", got, len("first"))
	}

	if err := tr.CloseDiscarding(); err != nil {
		t.Fatalf("CloseDiscarding: %v", err)
	}
	raw, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(raw) != "first\n" {
		t.Errorf("output = %q, want %q (partial truncated, complete line kept)", raw, "first\n")
	}
}

// TestNewRejectsNonNumericInterval locks the constructor guard kept as
// defense-in-depth behind the load-time strict typing: a non-numeric
// max_send_interval fails initialization.
func TestNewRejectsNonNumericInterval(t *testing.T) {
	_, err := New(sender.ConstructorArgs{
		Name:   "s",
		Config: map[string]any{"output_type": "file", "file_output": filepath.Join(t.TempDir(), "x"), "max_send_interval": "fast"},
		Tags:   tagged.New(nil),
	})
	if err == nil || !strings.Contains(err.Error(), "not a number") {
		t.Fatalf("err = %v, want a not-a-number error", err)
	}
}
