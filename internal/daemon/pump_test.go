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
	"net"
	"os"
	"path/filepath"
	"reflect"
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

// disableSignals stubs the os/signal seams so Run can execute inside a
// synctest bubble (the runtime signal goroutine would otherwise select
// on a bubble channel from outside it).
func disableSignals(t *testing.T) {
	t.Helper()
	notify, stop := signalNotify, signalStop
	signalNotify = func(chan<- os.Signal, ...os.Signal) {}
	signalStop = func(chan<- os.Signal) {}
	t.Cleanup(func() {
		signalNotify, signalStop = notify, stop
	})
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
	rt.senderOrder = append(rt.senderOrder, "s")
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
		var runErr error
		if err := p.shutdown(cancel, func() error { runErr = <-done; return runErr }); err != nil {
			t.Fatalf("shutdown: %v", err)
		}
		if runErr != nil {
			t.Fatalf("run returned %v, want nil on cancellation", runErr)
		}
		elapsed := time.Since(start)

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

// ---------------------------------------------------------------------------
// sender order (config document order)

// TestSenderNamesInOrder locks the document-order resolution: configured
// order wins, unknown keys are appended sorted, and a nil order falls
// back to sorted (synthetic configs / tests).
func TestSenderNamesInOrder(t *testing.T) {
	senders := map[string]any{"zulu": 1, "alpha": 2, "mike": 3}
	cases := []struct {
		name  string
		order []string
		want  []string
	}{
		{"document order", []string{"zulu", "alpha", "mike"}, []string{"zulu", "alpha", "mike"}},
		{"partial order appends sorted", []string{"mike"}, []string{"mike", "alpha", "zulu"}},
		{"nil order sorts", nil, []string{"alpha", "mike", "zulu"}},
		{"stale keys ignored", []string{"gone", "alpha"}, []string{"alpha", "mike", "zulu"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := senderNamesInOrder(senders, tc.order)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("senderNamesInOrder = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestProcessSenderOrderDecidesCacheKey locks the successful-init
// traversal order: when two senders share the (empty) field-filter cache
// key, whichever is processed FIRST captures the serialization, and the
// other reuses those bytes — so the traversal order determines whose
// extra_field_values reach both payloads.
func TestProcessSenderOrderDecidesCacheKey(t *testing.T) {
	build := func(order []string) (*ReaderRuntime, *buffer.SenderBuffer, *buffer.SenderBuffer) {
		rc := fakeReaderConfig(map[string]any{"zulu": fakeSenderConfig(), "alpha": fakeSenderConfig()})
		rc.SenderOrder = []string{"zulu", "alpha"}
		rc.ExtraFieldValuesRaw = map[string]json.RawMessage{
			"zulu":  json.RawMessage(`{"EXTRA":"from-zulu"}`),
			"alpha": json.RawMessage(`{"EXTRA":"from-alpha"}`),
		}
		cfg := testPumpConfig(map[string]config.ReaderConfig{"r": rc})
		_, rt := newTestRuntime(t, cfg, &captureLog{})
		zb, ab := buffer.NewSenderBuffer("zulu", 100), buffer.NewSenderBuffer("alpha", 100)
		zb.Attach(sender.NewLoop(sender.LoopConfig{Name: "zulu", Tags: map[string]string{}}, sender.Instance{}, zb.Chan()))
		ab.Attach(sender.NewLoop(sender.LoopConfig{Name: "alpha", Tags: map[string]string{}}, sender.Instance{}, ab.Chan()))
		rt.mu.Lock()
		rt.senders["zulu"] = &senderEntry{name: "zulu", buffer: zb}
		rt.senders["alpha"] = &senderEntry{name: "alpha", buffer: ab}
		rt.senderOrder = order
		rt.mu.Unlock()
		return rt, zb, ab
	}

	extra := func(t *testing.T, b *buffer.SenderBuffer) string {
		t.Helper()
		select {
		case item := <-b.Chan():
			var parsed map[string]any
			if err := json.Unmarshal(item.Payload, &parsed); err != nil {
				t.Fatalf("payload: %v", err)
			}
			value, _ := parsed["EXTRA"].(string)
			return value
		default:
			t.Fatal("no enqueued item")
			return ""
		}
	}

	entry := journal.Entry{Cursor: "c", Fields: []journal.Field{{Key: "MESSAGE", Value: "x"}}}

	rt, zb, ab := build([]string{"alpha", "zulu"})
	if _, _, _, err := rt.process(context.Background(), entry); err != nil {
		t.Fatalf("process: %v", err)
	}
	if got := extra(t, ab); got != "from-alpha" {
		t.Errorf("alpha EXTRA = %q, want from-alpha", got)
	}
	if got := extra(t, zb); got != "from-alpha" {
		t.Errorf("zulu EXTRA = %q, want from-alpha (shares alpha's cached bytes)", got)
	}

	rt, zb, ab = build([]string{"zulu", "alpha"})
	if _, _, _, err := rt.process(context.Background(), entry); err != nil {
		t.Fatalf("process: %v", err)
	}
	if got := extra(t, ab); got != "from-zulu" {
		t.Errorf("alpha EXTRA = %q, want from-zulu (shares zulu's cached bytes)", got)
	}
	if got := extra(t, zb); got != "from-zulu" {
		t.Errorf("zulu EXTRA = %q, want from-zulu", got)
	}
}

// ---------------------------------------------------------------------------
// cancellation / shutdown

// TestReadMessagesStopsAtCancellation locks the per-read ctx check: a
// cancelled context stops the chunk before reading anything, so a
// senderless or filtered reader can never consume (and advance past) a
// chunk after SIGTERM.
func TestReadMessagesStopsAtCancellation(t *testing.T) {
	cfg := testPumpConfig(map[string]config.ReaderConfig{"r": fakeReaderConfig(nil)})
	_, rt := newTestRuntime(t, cfg, &captureLog{})
	rt.initSenders()

	fr := &fakeReader{entries: []journal.Entry{
		{Cursor: "c1", Fields: []journal.Field{{Key: "MESSAGE", Value: "a"}}},
		{Cursor: "c2", Fields: []journal.Field{{Key: "MESSAGE", Value: "b"}}},
		{Cursor: "c3", Fields: []journal.Field{{Key: "MESSAGE", Value: "c"}}},
	}}
	rt.journaldReader = fr

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	exhausted, lines, err := rt.readMessages(ctx)
	if err != nil {
		t.Fatalf("readMessages: %v", err)
	}
	if exhausted || lines != 0 {
		t.Errorf("got exhausted=%v lines=%d, want false 0", exhausted, lines)
	}
	if fr.idx != 0 {
		t.Errorf("read %d entries after cancellation, want 0", fr.idx)
	}
	if got := rt.currentCursor(); got != "" {
		t.Errorf("cursor = %q, want unchanged (empty)", got)
	}
}

// failingNotifier fails a specific sd_notify state.
type failingNotifier struct {
	fakeNotifier
	fail string
}

func (f *failingNotifier) Notify(state string) (bool, error) {
	if state == f.fail {
		return false, errors.New("notify failed")
	}
	return f.fakeNotifier.Notify(state)
}

// closeableTransport records Close for the shutdown-cleanup assertion.
type closeableTransport struct {
	fakeTransport
	closed bool
}

func (c *closeableTransport) Close() error {
	c.closed = true
	return nil
}

// TestShutdownCleanupDespiteNotifyError locks the cleanup guarantee: a
// STOPPING notification failure is returned as Run's result but never
// skips sender cancellation, state save, or transport closure.
func TestShutdownCleanupDespiteNotifyError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := testPumpConfig(map[string]config.ReaderConfig{"r": fakeReaderConfig(nil)})
		statePath := filepath.Join(t.TempDir(), "state.json")
		cfg.JSONStateFilePath = statePath
		p, _ := newTestRuntime(t, cfg, &captureLog{})
		p.statePath = statePath

		canceled := false
		transport := &closeableTransport{}
		entry := &senderEntry{
			name:   "s",
			cancel: func() { canceled = true },
			inst:   sender.Instance{Transport: transport},
		}
		p.senderMu.Lock()
		p.senders["r"] = []*senderEntry{entry}
		p.senderMu.Unlock()
		p.notifier = &failingNotifier{fail: StateStopping}

		err := p.shutdown(func() {}, func() error { return nil })
		if err == nil || !strings.Contains(err.Error(), "notify failed") {
			t.Fatalf("shutdown err = %v, want the STOPPING notify error", err)
		}
		if !canceled {
			t.Error("sender not cancelled despite the notify error")
		}
		if !transport.closed {
			t.Error("transport not closed despite the notify error")
		}
		if _, statErr := os.Stat(statePath); statErr != nil {
			t.Errorf("state not saved despite the notify error: %v", statErr)
		}
	})
}

// ---------------------------------------------------------------------------
// Pump.Run supervisor

// TestPumpRunSupervisor drives Pump.Run itself: readers, the sender grace
// flush, the STOPPING notify, and the final state save.
func TestPumpRunSupervisor(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		disableSignals(t)
		cfg := testPumpConfig(map[string]config.ReaderConfig{
			"r": fakeReaderConfig(map[string]any{"s": fakeSenderConfig()}),
		})
		cfg.JSONStateFilePath = filepath.Join(t.TempDir(), "state.json")
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
		go func() { done <- p.Run(ctx, make(chan os.Signal)) }()

		synctest.Wait()
		time.Sleep(2 * pollInterval)
		synctest.Wait()
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("Run did not return after cancellation")
		}

		if !notifier.notified(StateStopping) {
			t.Error("Run did not notify STOPPING=1")
		}
		transport, ok := rt.pump.senders["r"][0].inst.Transport.(*fakeTransport)
		if !ok {
			t.Fatalf("transport type %T", rt.pump.senders["r"][0].inst.Transport)
		}
		if got := transport.messages(); got != 2 {
			t.Errorf("flushed messages = %d, want 2 (grace-window final flush)", got)
		}
		if _, err := os.Stat(cfg.JSONStateFilePath); err != nil {
			t.Errorf("state file not saved: %v", err)
		}
	})
}

// TestPumpRunReaderErrorPropagates locks the errgroup supervisor: a
// reader failure is propagated as Run's result after a full shutdown.
func TestPumpRunReaderErrorPropagates(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		disableSignals(t)
		cfg := testPumpConfig(map[string]config.ReaderConfig{"r": fakeReaderConfig(nil)})
		p, rt := newTestRuntime(t, cfg, &captureLog{})
		p.notifier = &fakeNotifier{}
		rt.reopener = &fakeSource{err: errors.New("open boom")}

		done := make(chan error, 1)
		go func() { done <- p.Run(context.Background(), make(chan os.Signal)) }()

		synctest.Wait()
		time.Sleep(2 * pollInterval)
		synctest.Wait()
		select {
		case err := <-done:
			if err == nil || !strings.Contains(err.Error(), "open boom") {
				t.Fatalf("Run = %v, want the reader error", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("Run did not return after the reader failure")
		}
	})
}

// ---------------------------------------------------------------------------
// backpressure (composed) and byte budgets

var slowRelease = make(chan struct{})

// registerSlowSender wires the blocking sender once: its
// MaintenanceOperations blocks until slowRelease is closed, so the loop
// never reaches its drain and the channel fills like a slow consumer.
func registerSlowSender() {
	registerFakeSenders()
	registerSlowOnce.Do(func() {
		sender.Register("slowblock", func(sender.ConstructorArgs) (sender.Instance, error) {
			return sender.Instance{
				Transport:       &slowTransport{},
				MaxSendInterval: 50 * time.Millisecond,
				Type:            "SlowSender",
			}, nil
		})
	})
}

var registerSlowOnce sync.Once

type slowTransport struct{}

func (s *slowTransport) SendMessages(context.Context, [][]byte, string) error { return nil }
func (s *slowTransport) MaintenanceOperations(ctx context.Context) error {
	select {
	case <-slowRelease:
	case <-ctx.Done():
	}
	return nil
}

// TestComposedBackpressure locks the fill/pause/drain/resume cycle: a
// full sender buffer pauses the reader (readiness false + pause log);
// once the sender drains, readiness returns (resume log).
func TestComposedBackpressure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		registerSlowSender()
		cfg := testPumpConfig(map[string]config.ReaderConfig{
			"r": fakeReaderConfig(map[string]any{
				"slow": map[string]any{"output_type": "slowblock"},
				"fast": fakeSenderConfig(),
			}),
		})
		cfg.MsgBufferMaxLength = 3
		cfg.MsgBufferMaxBytes = 0 // disable the byte budget; count gates
		log := &captureLog{}
		p, rt := newTestRuntime(t, cfg, log)
		rt.reopener = &fakeSource{reader: &fakeReader{}}
		if err := rt.updateStatus(context.Background()); err != nil {
			t.Fatalf("updateStatus: %v", err)
		}
		t.Cleanup(func() {
			p.senderMu.Lock()
			var entries []*senderEntry
			for _, es := range p.senders {
				entries = append(entries, es...)
			}
			p.senderMu.Unlock()
			for _, e := range entries {
				e.cancel()
			}
			p.senderWG.Wait()
		})

		rt.mu.Lock()
		slow := rt.senders["slow"]
		rt.mu.Unlock()
		if slow == nil {
			t.Fatal("slow sender not initialized")
		}
		ctx := context.Background()
		for i := 0; i < 3; i++ {
			if err := slow.buffer.Enqueue(ctx, sender.Item{Payload: []byte("x"), Cursor: "c"}); err != nil {
				t.Fatalf("enqueue: %v", err)
			}
		}
		if err := rt.updateStatus(ctx); err != nil {
			t.Fatalf("updateStatus: %v", err)
		}
		if rt.readyFlag {
			t.Error("readyFlag = true, want paused with the slow sender full")
		}
		if !log.contains("pause processing journal for \"r\"") {
			t.Errorf("missing pause log; got %v", log.lines)
		}

		// Drain: release the slow sender and let its loop run.
		close(slowRelease)
		time.Sleep(2 * time.Second)
		synctest.Wait()
		if err := rt.updateStatus(ctx); err != nil {
			t.Fatalf("updateStatus: %v", err)
		}
		if !rt.readyFlag {
			t.Error("readyFlag = false, want resumed after the drain")
		}
		if !log.contains("ready to process journal for \"r\"") {
			t.Errorf("missing resume log; got %v", log.lines)
		}
	})
}

// TestByteBudgetPausesReader locks the multi-sender maximum-byte budget:
// one sender's payload bytes over msg_buffer_max_bytes pause the reader
// even with the other sender empty.
func TestByteBudgetPausesReader(t *testing.T) {
	cfg := testPumpConfig(map[string]config.ReaderConfig{
		"r": fakeReaderConfig(map[string]any{"a": fakeSenderConfig(), "b": fakeSenderConfig()}),
	})
	cfg.MsgBufferMaxLength = 50000
	cfg.MsgBufferMaxBytes = 100
	p, rt := newTestRuntime(t, cfg, &captureLog{})
	rt.initSenders()
	stopSenders(t, p)

	rt.mu.Lock()
	a := rt.senders["a"]
	rt.mu.Unlock()
	payload := strings.Repeat("x", 200)
	if err := a.buffer.Enqueue(context.Background(), sender.Item{Payload: []byte(payload), Cursor: "c"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if got := rt.writeLimitBytes(); got > 0 {
		t.Errorf("writeLimitBytes = %d, want <= 0 with 200 bytes over a 100-byte budget", got)
	}
	if got := rt.writeLimitCount(); got <= 0 {
		t.Errorf("writeLimitCount = %d, want > 0 (count budget untouched)", got)
	}
}

// ---------------------------------------------------------------------------
// broad handler (exception tags + delay)

// udpCapture is a loopback UDP statsd sink for asserting emitted
// datagrams.
type udpCapture struct {
	conn *net.UDPConn
}

func newUDPCapture(t *testing.T) *udpCapture {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("listen udp: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return &udpCapture{conn: conn}
}

func (c *udpCapture) addr() (string, int64) {
	addr := c.conn.LocalAddr().(*net.UDPAddr)
	return "127.0.0.1", int64(addr.Port)
}

func (c *udpCapture) read(t *testing.T) string {
	t.Helper()
	_ = c.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 4096)
	n, _, err := c.conn.ReadFromUDP(buf)
	if err != nil {
		t.Fatalf("read statsd datagram: %v", err)
	}
	return string(buf[:n])
}

// TestBroadHandlerDelayAndTags locks read_single_message's except-arm:
// the unexpected_exception counter fires with where=mainloop and
// app=journalpump, and the handler sleeps 500 ms.
func TestBroadHandlerDelayAndTags(t *testing.T) {
	capture := newUDPCapture(t)
	host, port := capture.addr()
	cfg := testPumpConfig(map[string]config.ReaderConfig{"r": fakeReaderConfig(nil)})
	cfg.Statsd = config.StatsdConfig{Host: &host, Port: &port, Tags: map[string]string{}}
	log := &captureLog{}
	_, rt := newTestRuntime(t, cfg, log)

	start := time.Now()
	rt.broadHandler(errors.New("boom"))
	if elapsed := time.Since(start); elapsed < 500*time.Millisecond {
		t.Errorf("broadHandler delay = %v, want >= 500ms", elapsed)
	}
	if !log.contains("Unexpected exception while handling entry for r") {
		t.Errorf("missing broad-handler log; got %v", log.lines)
	}
	line := capture.read(t)
	if !strings.Contains(line, "where=mainloop") {
		t.Errorf("datagram = %q, want where=mainloop", line)
	}
	if !strings.Contains(line, "app=journalpump") {
		t.Errorf("datagram = %q, want app=journalpump", line)
	}
}
