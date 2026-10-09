// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package file

import (
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
