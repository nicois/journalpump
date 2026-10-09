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
// asserts the first ten output lines byte-for-byte against the captured
// ground truth, plus the loop's sent count and cursor.
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
