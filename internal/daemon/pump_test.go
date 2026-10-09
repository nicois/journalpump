// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Aiven-Open/journalpump/internal/buffer"
	"github.com/Aiven-Open/journalpump/internal/config"
	"github.com/Aiven-Open/journalpump/internal/journal"
	"github.com/Aiven-Open/journalpump/internal/reader"
	"github.com/Aiven-Open/journalpump/internal/sender"
)

// ---------------------------------------------------------------------------
// fakes

// fakeNotifier records sd_notify states instead of writing a socket.
type fakeNotifier struct {
	mu     sync.Mutex
	states []string
}

func (f *fakeNotifier) Notify(state string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.states = append(f.states, state)
	return true, nil
}

func (f *fakeNotifier) notified(state string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, s := range f.states {
		if s == state {
			return true
		}
	}
	return false
}

// fakeTransport records the batches a sender loop flushed.
type fakeTransport struct {
	mu      sync.Mutex
	batches [][][]byte
	cursors []string
}

func (f *fakeTransport) SendMessages(_ context.Context, batch [][]byte, cursor string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.batches = append(f.batches, batch)
	f.cursors = append(f.cursors, cursor)
	return nil
}

func (f *fakeTransport) MaintenanceOperations(context.Context) error { return nil }

func (f *fakeTransport) messages() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, b := range f.batches {
		n += len(b)
	}
	return n
}

var registerFakes sync.Once

// registerFakeSenders wires the test output types once.
func registerFakeSenders() {
	registerFakes.Do(func() {
		sender.Register("fake", func(sender.ConstructorArgs) (sender.Instance, error) {
			return sender.Instance{
				Transport:       &fakeTransport{},
				MaxSendInterval: 50 * time.Millisecond,
				Type:            "FakeSender",
				ConnectOnInit:   true,
			}, nil
		})
		sender.Register("failinit", func(sender.ConstructorArgs) (sender.Instance, error) {
			return sender.Instance{}, errors.New("constructor exploded")
		})
	})
}

// captureLog records the pump's log lines for assertion.
type captureLog struct {
	mu    sync.Mutex
	lines []string
}

func (c *captureLog) Printf(format string, args ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lines = append(c.lines, fmt.Sprintf(format, args...))
}

func (c *captureLog) contains(substr string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, line := range c.lines {
		if strings.Contains(line, substr) {
			return true
		}
	}
	return false
}

// fakeReader serves a fixed entry list; Wait returns one append event then
// blocks until cancellation.
type fakeReader struct {
	entries   []journal.Entry
	idx       int
	readErr   error
	firstWait bool
	closed    bool
}

func (r *fakeReader) Wait(ctx context.Context, _ time.Duration) (reader.Event, error) {
	if r.firstWait {
		r.firstWait = false
		return reader.EventAppend, nil
	}
	<-ctx.Done()
	return reader.EventNOP, ctx.Err()
}

func (r *fakeReader) ReadBatch(n int) ([]journal.Entry, error) {
	if r.readErr != nil {
		return nil, r.readErr
	}
	if r.idx >= len(r.entries) {
		return nil, nil
	}
	end := r.idx + n
	if end > len(r.entries) {
		end = len(r.entries)
	}
	out := r.entries[r.idx:end]
	r.idx = end
	return out, nil
}

func (r *fakeReader) Seek(string) error { return nil }

func (r *fakeReader) Close() error {
	r.closed = true
	return nil
}

// fakeSource is the ReaderSource seam: a scripted Ensure result plus an
// Invalidate counter.
type fakeSource struct {
	reader      reader.Reader
	err         error
	ensureCalls int
	invalidated int
}

func (f *fakeSource) Ensure() (reader.Reader, error) {
	f.ensureCalls++
	return f.reader, f.err
}

func (f *fakeSource) Invalidate() { f.invalidated++ }

// ---------------------------------------------------------------------------
// helpers

func testPumpConfig(readers map[string]config.ReaderConfig) *config.Config {
	return &config.Config{
		Readers:            readers,
		MsgBufferMaxLength: buffer.DefaultMsgBufferMaxLength,
		MsgBufferMaxBytes:  buffer.DefaultMsgBufferMaxBytes,
	}
}

func fakeSenderConfig() map[string]any {
	return map[string]any{"output_type": "fake"}
}

func fakeReaderConfig(senders map[string]any) config.ReaderConfig {
	return config.ReaderConfig{
		Name:                "r",
		Senders:             senders,
		ExtraFieldValuesRaw: map[string]json.RawMessage{},
	}
}

// newTestRuntime builds a pump with one reader and returns its runtime.
func newTestRuntime(t *testing.T, cfg *config.Config, log Logger) (*Pump, *ReaderRuntime) {
	t.Helper()
	registerFakeSenders()
	p, err := NewPump(cfg, PumpDeps{Log: log})
	if err != nil {
		t.Fatalf("NewPump: %v", err)
	}
	if len(p.readers) != 1 {
		t.Fatalf("readers = %d, want 1", len(p.readers))
	}
	return p, p.readers[0]
}

// stopSenders cancels every live sender and waits for it, without the
// production grace window.
func stopSenders(t *testing.T, p *Pump) {
	t.Helper()
	t.Cleanup(func() {
		p.senderMu.Lock()
		entries := make([]*senderEntry, 0, len(p.senders))
		for _, readerEntries := range p.senders {
			entries = append(entries, readerEntries...)
		}
		p.senderMu.Unlock()
		for _, entry := range entries {
			entry.cancel()
		}
		p.senderWG.Wait()
	})
}

// ---------------------------------------------------------------------------
// construction

// TestNewPumpUnknownOutputType locks the construction-time lookup: an
// unknown output_type is Python's get_reader KeyError/ValueError crash,
// reported here as a startup error.
func TestNewPumpUnknownOutputType(t *testing.T) {
	cfg := testPumpConfig(map[string]config.ReaderConfig{
		"r": fakeReaderConfig(map[string]any{"s": map[string]any{"output_type": "nope"}}),
	})
	_, err := NewPump(cfg, PumpDeps{})
	if err == nil || !strings.Contains(err.Error(), "Unknown sender type") {
		t.Fatalf("err = %v, want Unknown sender type", err)
	}
}

// TestNewPumpMissingOutputType locks the required key: Python's
// sender_config["output_type"] KeyError.
func TestNewPumpMissingOutputType(t *testing.T) {
	cfg := testPumpConfig(map[string]config.ReaderConfig{
		"r": fakeReaderConfig(map[string]any{"s": map[string]any{}}),
	})
	_, err := NewPump(cfg, PumpDeps{})
	if err == nil || !strings.Contains(err.Error(), "output_type") {
		t.Fatalf("err = %v, want an output_type error", err)
	}
}

// TestNewPumpJournaldFiltersRejected locks the add_match parity decision:
// a journald_filters dict is rejected at construction (Python raises
// TypeError mid-run).
func TestNewPumpJournaldFiltersRejected(t *testing.T) {
	rc := fakeReaderConfig(nil)
	rc.JournaldFilters = []map[string]any{{"_SYSTEMD_UNIT": "x"}}
	cfg := testPumpConfig(map[string]config.ReaderConfig{"r": rc})
	_, err := NewPump(cfg, PumpDeps{})
	if err == nil || !strings.Contains(err.Error(), "journald_filters") {
		t.Fatalf("err = %v, want a journald_filters error", err)
	}
}

// ---------------------------------------------------------------------------
// sender initialization / limits

// TestInitSendersFailedGauge locks _failed_senders accounting and the
// sender.failed_to_start persistent gauge, with the live sender keeping
// the write limits open.
func TestInitSendersFailedGauge(t *testing.T) {
	cfg := testPumpConfig(map[string]config.ReaderConfig{
		"r": fakeReaderConfig(map[string]any{
			"good": fakeSenderConfig(),
			"bad":  map[string]any{"output_type": "failinit"},
		}),
	})
	log := &captureLog{}
	p, rt := newTestRuntime(t, cfg, log)
	rt.initSenders()
	stopSenders(t, p)

	if rt.failedSenders != 1 {
		t.Errorf("failedSenders = %d, want 1", rt.failedSenders)
	}
	if len(rt.senders) != 1 {
		t.Errorf("live senders = %d, want 1", len(rt.senders))
	}
	if !log.contains(`Sender "bad" failed to initialize`) {
		t.Errorf("missing failure log; got %v", log.lines)
	}
	gauge := rt.stats.Gauges.Snapshot()["sender.failed_to_start"]
	if gauge.Value != 1 {
		t.Errorf("sender.failed_to_start = %d, want 1", gauge.Value)
	}
	if gauge.Tags["reader"] != "r" {
		t.Errorf("gauge tags = %v, want reader=r", gauge.Tags)
	}
	if rt.writeLimitCount() <= 0 || rt.writeLimitBytes() <= 0 {
		t.Errorf("limits = %d/%d, want > 0 with a live sender", rt.writeLimitCount(), rt.writeLimitBytes())
	}
}

// TestWriteLimitsAllSendersFailed locks the get_write_limit_* variant:
// failed senders AND no live senders → 0 on both limits (never read).
func TestWriteLimitsAllSendersFailed(t *testing.T) {
	cfg := testPumpConfig(map[string]config.ReaderConfig{
		"r": fakeReaderConfig(map[string]any{"bad": map[string]any{"output_type": "failinit"}}),
	})
	_, rt := newTestRuntime(t, cfg, &captureLog{})
	rt.initSenders()

	if rt.failedSenders != 1 || len(rt.senders) != 0 {
		t.Fatalf("failed=%d live=%d, want 1/0", rt.failedSenders, len(rt.senders))
	}
	if got := rt.writeLimitCount(); got != 0 {
		t.Errorf("writeLimitCount = %d, want 0", got)
	}
	if got := rt.writeLimitBytes(); got != 0 {
		t.Errorf("writeLimitBytes = %d, want 0", got)
	}
}

// ---------------------------------------------------------------------------
// update_status / readiness

// TestUpdateStatusOpenErrors locks the parse/log arms of
// create_journald_reader_if_missing.
func TestUpdateStatusOpenErrors(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"files not available", reader.ErrFilesNotAvailable, "Journal files for \"r\" are not yet available"},
		{"not found", fmt.Errorf("open: %w", os.ErrNotExist), "Journal for \"r\" not available yet"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			log := &captureLog{}
			_, rt := newTestRuntime(t, testPumpConfig(map[string]config.ReaderConfig{"r": fakeReaderConfig(nil)}), log)
			rt.reopener = &fakeSource{err: tc.err}
			if err := rt.updateStatus(context.Background()); err != nil {
				t.Fatalf("updateStatus: %v", err)
			}
			if rt.journaldReader != nil {
				t.Error("journaldReader set on error")
			}
			if !log.contains(tc.want) {
				t.Errorf("missing %q; got %v", tc.want, log.lines)
			}
		})
	}
}

// TestUpdateStatusReadinessTransitions locks the two readiness log lines
// with the falsy msg_buffer_max_bytes toggle.
func TestUpdateStatusReadinessTransitions(t *testing.T) {
	log := &captureLog{}
	cfg := testPumpConfig(map[string]config.ReaderConfig{
		"r": fakeReaderConfig(map[string]any{"s": fakeSenderConfig()}),
	})
	// A zero byte budget makes get_write_limit_bytes 0 — the pause arm —
	// but msg_buffer_max_bytes = 0 also makes Python skip the OR.
	cfg.MsgBufferMaxBytes = 0
	cfg.MsgBufferMaxLength = 0 // count limit 0 → pause
	p, rt := newTestRuntime(t, cfg, log)
	rt.reopener = &fakeSource{reader: &fakeReader{}}
	if err := rt.updateStatus(context.Background()); err != nil {
		t.Fatalf("updateStatus: %v", err)
	}
	stopSenders(t, p)
	if rt.readyFlag {
		t.Error("readyFlag = true, want pause (over limit)")
	}
	if !log.contains("Message buffer size for at least one sender over threshold, pause processing journal for \"r\"") {
		t.Errorf("missing pause log; got %v", log.lines)
	}
}

// ---------------------------------------------------------------------------
// reading

// TestReadMessagesZeroLimit locks the ≤0 gate: with a zero message-count
// budget the reader drains nothing.
func TestReadMessagesZeroLimit(t *testing.T) {
	cfg := testPumpConfig(map[string]config.ReaderConfig{
		"r": fakeReaderConfig(map[string]any{"s": fakeSenderConfig()}),
	})
	cfg.MsgBufferMaxLength = 0
	p, rt := newTestRuntime(t, cfg, &captureLog{})
	rt.initSenders()
	stopSenders(t, p)

	fr := &fakeReader{entries: []journal.Entry{
		{Cursor: "c1", Fields: []journal.Field{{Key: "MESSAGE", Value: "x"}}},
	}}
	rt.journaldReader = fr
	exhausted, lines, err := rt.readMessages(context.Background())
	if err != nil {
		t.Fatalf("readMessages: %v", err)
	}
	if exhausted || lines != 0 {
		t.Errorf("got exhausted=%v lines=%d, want false 0", exhausted, lines)
	}
	if fr.idx != 0 {
		t.Errorf("read %d entries, want 0", fr.idx)
	}
}

// TestReadMessagesExhaustion locks the full-drain path: all entries are
// consumed, the result is exhausted, and the cursor advances.
func TestReadMessagesExhaustion(t *testing.T) {
	cfg := testPumpConfig(map[string]config.ReaderConfig{
		"r": fakeReaderConfig(map[string]any{"s": fakeSenderConfig()}),
	})
	p, rt := newTestRuntime(t, cfg, &captureLog{})
	rt.initSenders()
	stopSenders(t, p)

	fr := &fakeReader{entries: []journal.Entry{
		{Cursor: "c1", Fields: []journal.Field{{Key: "MESSAGE", Value: "a"}}},
		{Cursor: "c2", Fields: []journal.Field{{Key: "MESSAGE", Value: "b"}}},
	}}
	rt.journaldReader = fr
	exhausted, lines, err := rt.readMessages(context.Background())
	if err != nil {
		t.Fatalf("readMessages: %v", err)
	}
	if !exhausted || lines != 2 {
		t.Errorf("got exhausted=%v lines=%d, want true 2", exhausted, lines)
	}
	if got := rt.currentCursor(); got != "c2" {
		t.Errorf("cursor = %q, want c2", got)
	}
}

// TestReadSingleMessageCorrupted locks the "Bad message" arm: the
// corrupted-entry counter fires and the chunk ends without a stash.
func TestReadSingleMessageCorrupted(t *testing.T) {
	log := &captureLog{}
	_, rt := newTestRuntime(t, testPumpConfig(map[string]config.ReaderConfig{"r": fakeReaderConfig(nil)}), log)
	rt.journaldReader = &fakeReader{readErr: reader.ErrBadMessage}

	hasMore, bytesRead, hasBytes, err := rt.readSingleMessage(context.Background())
	if err != nil {
		t.Fatalf("readSingleMessage: %v", err)
	}
	if hasMore || hasBytes || bytesRead != 0 {
		t.Errorf("got (%v,%d,%v), want (false,0,false)", hasMore, bytesRead, hasBytes)
	}
	if !log.contains("Corrupted log entry in r") {
		t.Errorf("missing corruption log; got %v", log.lines)
	}
}

// ---------------------------------------------------------------------------
// processing

// TestProcessEnqueues locks the pipeline-to-buffer handoff: the live
// senders become targets and the serialized payload lands in the buffer.
func TestProcessEnqueues(t *testing.T) {
	cfg := testPumpConfig(map[string]config.ReaderConfig{
		"r": fakeReaderConfig(map[string]any{"s": fakeSenderConfig()}),
	})
	p, rt := newTestRuntime(t, cfg, &captureLog{})

	// Inject a live sender without starting its loop, so the buffer
	// contents stay observable.
	buf := buffer.NewSenderBuffer("s", 100)
	loop := sender.NewLoop(sender.LoopConfig{Name: "s", Tags: map[string]string{}}, sender.Instance{}, buf.Chan())
	buf.Attach(loop)
	rt.mu.Lock()
	rt.senders["s"] = &senderEntry{name: "s", loop: loop, buffer: buf}
	rt.mu.Unlock()
	_ = p

	entry := journal.Entry{Cursor: "c1", Fields: []journal.Field{
		{Key: "MESSAGE", Value: "hello"},
	}}
	hasMore, bytesRead, hasBytes, err := rt.process(context.Background(), entry)
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if !hasMore || !hasBytes || bytesRead <= 0 {
		t.Fatalf("got (%v,%d,%v), want hasMore/hasBytes true and bytesRead>0", hasMore, bytesRead, hasBytes)
	}
	if got := buf.Count(); got != 1 {
		t.Fatalf("buffer count = %d, want 1", got)
	}
	item := <-buf.Chan()
	if !strings.Contains(string(item.Payload), "hello") {
		t.Errorf("payload = %s, want MESSAGE hello", item.Payload)
	}
	if item.Cursor != "c1" {
		t.Errorf("cursor = %q, want c1", item.Cursor)
	}
}

// ---------------------------------------------------------------------------
// state

// TestBuildState locks the state_to_save shape: cursor, search hits,
// sorted senders, and the reader totals.
func TestBuildState(t *testing.T) {
	cfg := testPumpConfig(map[string]config.ReaderConfig{
		"r": fakeReaderConfig(map[string]any{"s": fakeSenderConfig()}),
	})
	_, rt := newTestRuntime(t, cfg, &captureLog{})
	rt.setCursor("cursor-1")

	st := rt.pump.buildState()
	rs, ok := st.Readers["r"]
	if !ok {
		t.Fatal("state has no reader r")
	}
	if rs.Cursor == nil || *rs.Cursor != "cursor-1" {
		t.Errorf("cursor = %v, want cursor-1", rs.Cursor)
	}
	if string(rs.TotalLines) != "0" || string(rs.TotalBytes) != "0" {
		t.Errorf("totals = %s/%s, want 0/0", rs.TotalLines, rs.TotalBytes)
	}
	if len(rs.Senders) != 0 {
		t.Errorf("senders = %d, want 0 (not started)", len(rs.Senders))
	}
}

// ---------------------------------------------------------------------------
// loop + shutdown (synctest)

// TestRunLoopAndShutdownGrace drives one reader through the real run
// loop into a real sender loop under the fake synctest clock, then shuts
// the pump down and checks the D9 grace window and the final flush.
func TestRunLoopAndShutdownGrace(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := testPumpConfig(map[string]config.ReaderConfig{
			"r": fakeReaderConfig(map[string]any{"s": fakeSenderConfig()}),
		})
		notifier := &fakeNotifier{}
		p, rt := newTestRuntime(t, cfg, &captureLog{})
		p.notifier = notifier
		rt.reopener = &fakeSource{reader: &fakeReader{
			firstWait: true,
			entries: []journal.Entry{
				{Cursor: "c1", Fields: []journal.Field{{Key: "MESSAGE", Value: "a"}}},
				{Cursor: "c2", Fields: []journal.Field{{Key: "MESSAGE", Value: "b"}}},
			},
		}}

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- rt.run(ctx) }()

		synctest.Wait()
		// The first idle iteration waits one poll interval before
		// update_status opens the reader; advance the fake clock over
		// that wait so the append is drained.
		time.Sleep(2 * pollInterval)
		synctest.Wait()
		if !rt.ready() {
			t.Fatal("reader not ready after the first append")
		}
		if got := rt.currentCursor(); got != "c2" {
			t.Fatalf("cursor = %q, want c2", got)
		}

		cancel()
		start := time.Now()
		if err := p.shutdown(cancel, &sync.WaitGroup{}); err != nil {
			t.Fatalf("shutdown: %v", err)
		}
		elapsed := time.Since(start)

		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("run returned %v, want nil on cancellation", err)
			}
		default:
			t.Fatal("run did not return after cancellation")
		}

		wantGrace := 50*time.Millisecond + sender.TickInterval
		if elapsed != wantGrace {
			t.Errorf("shutdown grace = %v, want %v", elapsed, wantGrace)
		}
		if !notifier.notified(StateStopping) {
			t.Error("shutdown did not notify STOPPING=1")
		}
		transport, ok := rt.pump.senders["r"][0].inst.Transport.(*fakeTransport)
		if !ok {
			t.Fatalf("transport type %T", rt.pump.senders["r"][0].inst.Transport)
		}
		if got := transport.messages(); got != 2 {
			t.Errorf("flushed messages = %d, want 2 (grace-window final flush)", got)
		}
	})
}
