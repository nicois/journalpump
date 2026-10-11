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
	"github.com/Aiven-Open/journalpump/internal/journaldlog"
	"github.com/Aiven-Open/journalpump/internal/reader"
	"github.com/Aiven-Open/journalpump/internal/sender"
	senderfile "github.com/Aiven-Open/journalpump/internal/sender/file"
	senderkafka "github.com/Aiven-Open/journalpump/internal/sender/kafka"
	"github.com/Aiven-Open/journalpump/internal/state"
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
		// probe emits a metric through the stats client it was constructed
		// with, so a test can observe which client (captured vs live)
		// reached a lazily-initialized sender.
		sender.Register("probe", func(args sender.ConstructorArgs) (sender.Instance, error) {
			if args.Stats != nil {
				args.Stats.Increase("sender.init_probe", 1, nil)
			}
			return sender.Instance{
				Transport:       &fakeTransport{},
				MaxSendInterval: 50 * time.Millisecond,
				Type:            "ProbeSender",
				ConnectOnInit:   true,
			}, nil
		})
		// flaky fails its first construction attempt and succeeds on the
		// retry, so a test can exercise late successful initialization.
		sender.Register("flaky", func(sender.ConstructorArgs) (sender.Instance, error) {
			flakyAttemptsMu.Lock()
			flakyAttempts++
			attempt := flakyAttempts
			flakyAttemptsMu.Unlock()
			if attempt == 1 {
				return sender.Instance{}, errors.New("first attempt fails")
			}
			return sender.Instance{
				Transport:       &fakeTransport{},
				MaxSendInterval: 50 * time.Millisecond,
				Type:            "FlakySender",
				ConnectOnInit:   true,
			}, nil
		})
		// flushfail acks every batch into a buffer and fails the final
		// flush, so a test can lock the flush-ordering/state-rollback.
		sender.Register("flushfail", func(sender.ConstructorArgs) (sender.Instance, error) {
			return sender.Instance{
				Transport:       &flushFailTransport{},
				MaxSendInterval: 50 * time.Millisecond,
				Type:            "FlushFailSender",
				ConnectOnInit:   true,
			}, nil
		})
		// gatedblock's loop parks in maintenance until its release
		// channel closes, so a test controls when it delivers.
		sender.Register("gatedblock", func(sender.ConstructorArgs) (sender.Instance, error) {
			tr := &gatedTransport{release: currentGated()}
			gatedMu.Lock()
			gatedSent = tr
			gatedMu.Unlock()
			return sender.Instance{
				Transport:       tr,
				MaxSendInterval: 2 * time.Second,
				Type:            "GatedSender",
				ConnectOnInit:   true,
			}, nil
		})
		// durable exposes the delivery-checkpoint contract (DurableReporter
		// + Finalizer) with a controllable flush.
		sender.Register("durable", func(sender.ConstructorArgs) (sender.Instance, error) {
			tr := &durableTransport{}
			durableMu.Lock()
			durableSent = tr
			durableMu.Unlock()
			return sender.Instance{
				Transport:       tr,
				MaxSendInterval: 50 * time.Millisecond,
				Type:            "DurableSender",
				ConnectOnInit:   true,
			}, nil
		})
		// file is the production file sender, registered so the reload
		// retire path can be exercised against the real Discarder
		// (CloseDiscarding) implementation.
		sender.Register("file", senderfile.New)
		// kafka is the production sender registered so version-floor
		// load/reload tests exercise the real constructor path.
		sender.Register("kafka", senderkafka.New)
	})
}

// flakyAttempts counts flaky-sender construction attempts; reset per
// test that uses it.
var (
	flakyAttemptsMu sync.Mutex
	flakyAttempts   int
)

func resetFlakyAttempts() {
	flakyAttemptsMu.Lock()
	flakyAttempts = 0
	flakyAttemptsMu.Unlock()
}

// flushFailTransport accepts and buffers batches, then fails Finalize.
type flushFailTransport struct {
	mu       sync.Mutex
	messages int
}

func (f *flushFailTransport) SendMessages(_ context.Context, batch [][]byte, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.messages += len(batch)
	return nil
}

func (f *flushFailTransport) MaintenanceOperations(context.Context) error { return nil }

func (f *flushFailTransport) Finalize() error { return errors.New("final flush boom") }

// gatedTransport buffers everything until its release channel closes:
// MaintenanceOperations blocks, so the loop never reaches its drain and
// the caller controls exactly when the flush happens.
type gatedTransport struct {
	mu      sync.Mutex
	msgs    int
	ctx     context.Context
	release <-chan struct{}
}

func (g *gatedTransport) SendMessages(_ context.Context, batch [][]byte, _ string) error {
	g.mu.Lock()
	g.msgs += len(batch)
	g.mu.Unlock()
	return nil
}

func (g *gatedTransport) MaintenanceOperations(ctx context.Context) error {
	g.mu.Lock()
	g.ctx = ctx
	g.mu.Unlock()
	select {
	case <-g.release:
	case <-ctx.Done():
	}
	return nil
}

func (g *gatedTransport) messages() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.msgs
}

// ctxCancelled reports whether the loop context captured by the blocked
// maintenance call has been cancelled.
func (g *gatedTransport) ctxCancelled() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.ctx != nil && g.ctx.Err() != nil
}

// gatedSent is the transport built for the most recent "gatedblock"
// construction; the fail-fast test reads it to observe delivery.
var (
	gatedMu        sync.Mutex
	gatedReleaseCh = make(chan struct{})
	gatedSent      *gatedTransport
)

func setGatedRelease(ch chan struct{}) {
	gatedMu.Lock()
	gatedReleaseCh = ch
	gatedSent = nil
	gatedMu.Unlock()
}

func currentGated() <-chan struct{} {
	gatedMu.Lock()
	defer gatedMu.Unlock()
	return gatedReleaseCh
}

func gatedTransportRef() *gatedTransport {
	gatedMu.Lock()
	defer gatedMu.Unlock()
	return gatedSent
}

// durableTransport models a sender with the file sender's delivery
// checkpoint: SendMessages only records the logical payload total,
// FlushedPayloadBytes reports the durable watermark (advancing on a
// successful Finalize), and Finalize can be made to fail. It lets the
// daemon tests exercise the persisted-cursor-is-the-flushed-boundary
// contract without real files.
type durableTransport struct {
	mu       sync.Mutex
	logical  int64
	flushed  int64
	flushErr error
}

func (d *durableTransport) SendMessages(_ context.Context, batch [][]byte, _ string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, m := range batch {
		d.logical += int64(len(m))
	}
	return nil
}

func (d *durableTransport) MaintenanceOperations(context.Context) error { return nil }

func (d *durableTransport) FlushedPayloadBytes() int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.flushed
}

func (d *durableTransport) Finalize() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.flushErr != nil {
		return d.flushErr
	}
	d.flushed = d.logical
	return nil
}

func (d *durableTransport) setFlushed(n int64) {
	d.mu.Lock()
	d.flushed = n
	d.mu.Unlock()
}

func (d *durableTransport) setFlushErr(err error) {
	d.mu.Lock()
	d.flushErr = err
	d.mu.Unlock()
}

func (d *durableTransport) logicalBytes() int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.logical
}

// durableSent is the transport built for the most recent "durable"
// construction; the checkpoint tests read and steer it.
var (
	durableMu   sync.Mutex
	durableSent *durableTransport
)

func durableTransportRef() *durableTransport {
	durableMu.Lock()
	defer durableMu.Unlock()
	return durableSent
}

// captureLog records the pump's log lines for assertion. It is
// level-aware (Errorf/Warningf) so tests can assert the severity routing.
type captureLog struct {
	mu       sync.Mutex
	lines    []string
	errors   []string
	warnings []string
}

func (c *captureLog) Printf(format string, args ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lines = append(c.lines, fmt.Sprintf(format, args...))
}

func (c *captureLog) Errorf(format string, args ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	line := fmt.Sprintf(format, args...)
	c.lines = append(c.lines, line)
	c.errors = append(c.errors, line)
}

func (c *captureLog) Warningf(format string, args ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	line := fmt.Sprintf(format, args...)
	c.lines = append(c.lines, line)
	c.warnings = append(c.warnings, line)
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

func (c *captureLog) containsError(substr string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, line := range c.errors {
		if strings.Contains(line, substr) {
			return true
		}
	}
	return false
}

// fakeReader serves a fixed entry list; Wait returns one append event then
// blocks until cancellation.
type fakeReader struct {
	mu      sync.Mutex
	entries []journal.Entry
	idx     int
	readErr error
	// firstWait returns one append event once, then blocks; repeatAppend
	// keeps returning appends (paced by the fake clock) until the entries
	// are consumed, so the reader loop keeps draining like a real journal.
	firstWait    bool
	repeatAppend bool
	// invalidateFirst models sd_journal_wait's spurious opening
	// INVALIDATE: the first wait after an open reports it, then nopPoll
	// keeps returning NOP (paced by the fake clock) so the loop keeps
	// iterating over a stable journal.
	invalidateFirst bool
	firstDone       bool
	nopPoll         bool
	closed          bool
}

func (r *fakeReader) Wait(ctx context.Context, _ time.Duration) (reader.Event, error) {
	r.mu.Lock()
	if r.invalidateFirst && !r.firstDone {
		r.firstDone = true
		r.mu.Unlock()
		return reader.EventInvalidate, nil
	}
	r.mu.Unlock()
	if r.nopPoll {
		select {
		case <-time.After(20 * time.Millisecond):
			return reader.EventNOP, nil
		case <-ctx.Done():
			return reader.EventNOP, ctx.Err()
		}
	}
	if r.repeatAppend {
		r.mu.Lock()
		done := r.idx >= len(r.entries)
		r.mu.Unlock()
		if !done {
			select {
			case <-time.After(20 * time.Millisecond):
				return reader.EventAppend, nil
			case <-ctx.Done():
				return reader.EventNOP, ctx.Err()
			}
		}
		<-ctx.Done()
		return reader.EventNOP, ctx.Err()
	}
	if r.firstWait {
		r.firstWait = false
		return reader.EventAppend, nil
	}
	<-ctx.Done()
	return reader.EventNOP, ctx.Err()
}

func (r *fakeReader) ReadBatch(n int) ([]journal.Entry, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
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

// readCount reports how many entries have been served.
func (r *fakeReader) readCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.idx
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
		_ = p.senderGroup.Wait()
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

// TestSenderKafkaPre010RejectedAtLoadAndReload locks item 3 at the daemon
// level: a kafka sender configured below the 0.10.0 floor is rejected when its
// configuration is loaded (initialize_senders, strict-at-load), counted as a
// failed sender and never made live; and because senders are re-initialized on
// every reader recreation, a reload that recreates the reader rejects it again
// rather than accepting it late. The 0.10.0 floor itself is accepted.
func TestSenderKafkaPre010RejectedAtLoadAndReload(t *testing.T) {
	registerFakeSenders()
	overrideReopener(t)
	readerBlob := func(topic, version string) string {
		return fmt.Sprintf(`{"readers":{"r":{"senders":{"s":{"output_type":"kafka","kafka_address":"127.0.0.1:9092","kafka_topic":%q,"kafka_api_version":%q}}}}}`, topic, version)
	}

	// Floor acceptance at the daemon wiring: 0.10.0 constructs.
	okCtor, err := sender.Lookup("kafka")
	if err != nil {
		t.Fatalf("Lookup(kafka): %v", err)
	}
	if _, err := okCtor(sender.ConstructorArgs{Name: "ok", Config: map[string]any{
		"kafka_address":     "127.0.0.1:9092",
		"kafka_topic":       "t",
		"kafka_api_version": "0.10.0",
	}}); err != nil {
		t.Fatalf("0.10.0 sender construct: %v, want acceptance", err)
	}

	log := &captureLog{}
	initial := decodeConfig(t, readerBlob("old", "0.9.0"))
	p, err := NewPump(initial, PumpDeps{Log: log})
	if err != nil {
		t.Fatal(err)
	}
	cancel := startReaderGroup(p)
	defer cancel()
	t.Cleanup(func() { retireAll(p) })

	// Startup: the pre-0.10 version is rejected during sender loading.
	p.readers[0].initSenders()
	if p.readers[0].failedSenders != 1 {
		t.Fatalf("startup failedSenders = %d, want 1", p.readers[0].failedSenders)
	}
	if len(p.senders) != 0 {
		t.Fatalf("startup live senders = %d, want 0", len(p.senders))
	}
	if !log.contains(`Sender "s" failed to initialize`) || !log.contains("minimum 0.10.0") {
		t.Errorf("startup rejection not logged with the floor; got %v", log.lines)
	}

	// Reload with a changed readers blob recreates the reader; the same
	// invalid version must be rejected again, not accepted late.
	updated := decodeConfig(t, readerBlob("new", "0.9.0"))
	if err := p.handleNewConfig(updated); err != nil {
		t.Fatalf("handleNewConfig: %v", err)
	}
	if len(p.readers) != 1 {
		t.Fatalf("readers = %d, want 1", len(p.readers))
	}
	// The replacement reader opens asynchronously and initializes its
	// senders at the tail of get_reader; wait for that attempt to land.
	// failedSenders is written under rt.mu by the reader goroutine.
	failedSenders := func(rt *ReaderRuntime) int {
		rt.mu.Lock()
		defer rt.mu.Unlock()
		return rt.failedSenders
	}
	deadline := time.Now().Add(5 * time.Second)
	for failedSenders(p.readers[0]) != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if failedSenders(p.readers[0]) != 1 {
		t.Fatalf("reload failedSenders = %d, want 1", failedSenders(p.readers[0]))
	}
	p.senderMu.Lock()
	liveSenders := len(p.senders)
	p.senderMu.Unlock()
	if liveSenders != 0 {
		t.Fatalf("reload live senders = %d, want 0", liveSenders)
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
// read_messages admission boundaries

// TestReadMessagesHonorsBufferBoundaries ports the granularity of the
// Python rotation threshold tests (test_journalpump_rotated_files_threshold
// and _threshold_bytes): the reader admits at most msg_buffer_max_length
// messages per chunk, and stops AFTER the message that crosses the byte
// budget (journalpump.py:1027-1042). A test that asserted only eventual
// delivery would pass if a limit were ignored entirely; these assert the
// exact admitted count and admission order, and would fail on a broken
// gate.
func TestReadMessagesHonorsBufferBoundaries(t *testing.T) {
	build := func(t *testing.T, maxLength, maxBytes, entries int) (*ReaderRuntime, *buffer.SenderBuffer) {
		t.Helper()
		cfg := testPumpConfig(map[string]config.ReaderConfig{
			"r": fakeReaderConfig(map[string]any{"s": fakeSenderConfig()}),
		})
		_, rt := newTestRuntime(t, cfg, &captureLog{})
		// Inject a live sender whose loop never drains, so the buffer
		// occupancy (and therefore the gate) is observable in place.
		buf := buffer.NewSenderBuffer("s", maxLength)
		loop := sender.NewLoop(sender.LoopConfig{Name: "s", Tags: map[string]string{}}, sender.Instance{}, buf.Chan())
		buf.Attach(loop)
		rt.mu.Lock()
		rt.senders["s"] = &senderEntry{name: "s", loop: loop, buffer: buf}
		rt.senderOrder = append(rt.senderOrder, "s")
		rt.mu.Unlock()
		rt.gate = buffer.NewGate(maxLength, maxBytes, buf)
		rt.msgBufferMaxBytes = maxBytes

		items := make([]journal.Entry, 0, entries)
		for i := 0; i < entries; i++ {
			items = append(items, journal.Entry{
				Cursor: fmt.Sprintf("c%d", i),
				Fields: []journal.Field{{Key: "MESSAGE", Value: fmt.Sprintf("message-%02d", i)}},
			})
		}
		rt.journaldReader = &fakeReader{entries: items}
		return rt, buf
	}

	t.Run("length limit admits exactly the configured count in order", func(t *testing.T) {
		rt, buf := build(t, 3, buffer.DefaultMsgBufferMaxBytes, 10)
		exhausted, lines, err := rt.readMessages(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if exhausted {
			t.Error("exhausted = true, want false (a full buffer pauses, it does not exhaust)")
		}
		if lines != 3 {
			t.Fatalf("admitted %d messages, want 3 (10 if the limit were ignored)", lines)
		}
		var got []string
		for buf.Count() > 0 {
			got = append(got, (<-buf.Chan()).Cursor)
		}
		if want := []string{"c0", "c1", "c2"}; !reflect.DeepEqual(got, want) {
			t.Errorf("admitted cursors = %v, want %v", got, want)
		}
	})

	t.Run("a limit above the backlog admits everything, in order", func(t *testing.T) {
		rt, _ := build(t, 100, buffer.DefaultMsgBufferMaxBytes, 10)
		exhausted, lines, err := rt.readMessages(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if !exhausted {
			t.Error("exhausted = false, want true (backlog fully drained)")
		}
		if lines != 10 {
			t.Fatalf("admitted %d messages, want 10", lines)
		}
	})

	t.Run("byte limit admits up to and including the crossing message", func(t *testing.T) {
		// Measure one serialized payload first (all entries are equal).
		rt, buf := build(t, 1000, buffer.DefaultMsgBufferMaxBytes, 6)
		_, lines, err := rt.readMessages(context.Background())
		if err != nil || lines != 6 {
			t.Fatalf("measure run: lines=%d err=%v, want 6", lines, err)
		}
		var sizes []int
		for buf.Count() > 0 {
			sizes = append(sizes, len((<-buf.Chan()).Payload))
		}
		if len(sizes) != 6 {
			t.Fatalf("measured %d payloads, want 6", len(sizes))
		}
		p := sizes[0]
		for _, s := range sizes {
			if s != p {
				t.Fatalf("payload sizes differ: %v", sizes)
			}
		}

		cases := []struct {
			budget int
			want   int
		}{
			{p, 1}, {p - 1, 1}, {2 * p, 2}, {2*p - 1, 2}, {3 * p, 3}, {3*p + 1, 4},
		}
		for _, tc := range cases {
			rt, _ := build(t, 1000, tc.budget, 6)
			_, got, err := rt.readMessages(context.Background())
			if err != nil {
				t.Fatalf("budget %d: %v", tc.budget, err)
			}
			if got != tc.want {
				t.Errorf("budget %d (payload %d): admitted %d, want %d", tc.budget, p, got, tc.want)
			}
		}
	})
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
		// The reader opens on the FIRST iteration (no initial poll
		// delay); advance the fake clock over the subsequent reader wait
		// so the append is drained.
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

// TestRunLoopFirstWaitInvalidateRecreates is the regression guard for the
// root-cause fix on the pump side: a FIRST-wait INVALIDATE is a genuine
// rotation/deletion (the open-time snapshot INVALIDATE is absorbed at the
// reader layer by RegisterForPoll, not here), so the run loop must honor
// it — drain, invalidate the source, and recreate the reader. The earlier
// blanket "mask the first INVALIDATE" approach swallowed exactly this
// event, so this test fails on that code (invalidated stays 0).
//
// The complementary stable-journal guarantee (no spurious recreation) is
// asserted at the reader layer by TestOpenAbsorbsInitialNotification,
// which pins that a freshly opened unchanged journal's first wait is NOP.
func TestRunLoopFirstWaitInvalidateRecreates(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := testPumpConfig(map[string]config.ReaderConfig{"r": fakeReaderConfig(nil)})
		_, rt := newTestRuntime(t, cfg, &captureLog{})

		fr := &fakeReader{
			invalidateFirst: true,
			nopPoll:         true,
			entries: []journal.Entry{
				{Cursor: "c1", Fields: []journal.Field{{Key: "MESSAGE", Value: "a"}}},
				{Cursor: "c2", Fields: []journal.Field{{Key: "MESSAGE", Value: "b"}}},
				{Cursor: "c3", Fields: []journal.Field{{Key: "MESSAGE", Value: "c"}}},
			},
		}
		src := &fakeSource{reader: fr}
		rt.reopener = src

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- rt.run(ctx) }()

		synctest.Wait()
		// Several iterations: open, first-wait INVALIDATE -> recreate, idle.
		time.Sleep(10 * pollInterval)
		synctest.Wait()
		cancel()
		if err := <-done; err != nil {
			t.Fatalf("run returned %v, want nil on cancellation", err)
		}

		if src.invalidated < 1 {
			t.Errorf("invalidated = %d, want >= 1 (first-wait INVALIDATE must trigger recreation)", src.invalidated)
		}
		if src.ensureCalls < 2 {
			t.Errorf("Ensure calls = %d, want >= 2 (reader recreated after invalidation)", src.ensureCalls)
		}
		if got := rt.currentCursor(); got != "c3" {
			t.Errorf("cursor = %q, want c3 (backlog drained before recreation)", got)
		}
	})
}

// TestRunLoopOpensReaderWithoutInitialPollDelay locks Python's immediate
// first poll (poll_timeout_ms initialised to 0.0, journalpump.py:1104): the
// reader is opened on the FIRST loop iteration, before any poll interval
// elapses. If the old initial delay returns (sleeping one pollInterval
// before update_status opens the reader), ensureCalls stays 0 until the
// fake clock advances and this fails — with initial_position "tail" that
// delay is an operator-visible skipped-message window after READY. It is
// independent of the systest driver's readiness probe.
func TestRunLoopOpensReaderWithoutInitialPollDelay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := testPumpConfig(map[string]config.ReaderConfig{"r": fakeReaderConfig(nil)})
		_, rt := newTestRuntime(t, cfg, &captureLog{})
		src := &fakeSource{reader: &fakeReader{
			nopPoll: true,
			entries: []journal.Entry{{Cursor: "c1", Fields: []journal.Field{{Key: "MESSAGE", Value: "a"}}}},
		}}
		rt.reopener = src

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- rt.run(ctx) }()

		synctest.Wait()
		// No clock advance: the first iteration must already have opened.
		if src.ensureCalls < 1 {
			t.Fatalf("reader not opened before the first poll interval (ensureCalls=%d)", src.ensureCalls)
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatalf("run returned %v, want nil on cancellation", err)
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

// TestSenderOrderFollowsDelayedInitialization locks the successful-init
// ordering through DELAYED initialization: the flaky sender fails its
// first construction and succeeds on the reader's next open, so it lands
// AFTER alpha in senderOrder (and process follows that order).
func TestSenderOrderFollowsDelayedInitialization(t *testing.T) {
	registerFakeSenders()
	resetFlakyAttempts()
	rc := fakeReaderConfig(map[string]any{
		"zulu":  map[string]any{"output_type": "flaky"},
		"alpha": fakeSenderConfig(),
	})
	rc.SenderOrder = []string{"zulu", "alpha"}
	rc.ExtraFieldValuesRaw = map[string]json.RawMessage{
		"zulu":  json.RawMessage(`{"EXTRA":"from-zulu"}`),
		"alpha": json.RawMessage(`{"EXTRA":"from-alpha"}`),
	}
	cfg := testPumpConfig(map[string]config.ReaderConfig{"r": rc})
	log := &captureLog{}
	p, rt := newTestRuntime(t, cfg, log)
	rt.reopener = &fakeSource{reader: &fakeReader{firstWait: true}}

	if err := rt.updateStatus(context.Background()); err != nil {
		t.Fatalf("updateStatus: %v", err)
	}
	if got := rt.senderOrder; !reflect.DeepEqual(got, []string{"alpha"}) {
		t.Fatalf("senderOrder after first init = %v, want [alpha] (zulu failed)", got)
	}
	if !log.contains(`Sender "zulu" failed to initialize`) {
		t.Errorf("missing failed-init log; got %v", log.lines)
	}

	// Recreate the reader: the retried constructor succeeds and zulu
	// lands after alpha.
	rt.journaldReader = nil
	if err := rt.updateStatus(context.Background()); err != nil {
		t.Fatalf("updateStatus (reopen): %v", err)
	}
	if got := rt.senderOrder; !reflect.DeepEqual(got, []string{"alpha", "zulu"}) {
		t.Fatalf("senderOrder after retry = %v, want [alpha zulu]", got)
	}
	stopSenders(t, p)

	// process follows the successful-init order: alpha captures the
	// shared serialization first, so zulu inherits alpha's EXTRA.
	entry := journal.Entry{Cursor: "c", Fields: []journal.Field{{Key: "MESSAGE", Value: "x"}}}
	if _, _, _, err := rt.process(context.Background(), entry); err != nil {
		t.Fatalf("process: %v", err)
	}
	rt.mu.Lock()
	zb, ab := rt.senders["zulu"].buffer, rt.senders["alpha"].buffer
	rt.mu.Unlock()
	extra := func(b *buffer.SenderBuffer) string {
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
	if got := extra(ab); got != "from-alpha" {
		t.Errorf("alpha EXTRA = %q, want from-alpha", got)
	}
	if got := extra(zb); got != "from-alpha" {
		t.Errorf("zulu EXTRA = %q, want from-alpha (shares alpha's cached bytes)", got)
	}
}

// TestFilterCaptureQuirkLock locks the construction-time filter capture:
// senders sharing a field-filter NAME share the ONE *filter.FieldFilter
// instance captured at resolve time, and process applies it byte-for-byte
// to both.
func TestFilterCaptureQuirkLock(t *testing.T) {
	cfg := testPumpConfig(map[string]config.ReaderConfig{
		"r": fakeReaderConfig(map[string]any{
			"a": map[string]any{"output_type": "fake", "field_filter": "shared"},
			"b": map[string]any{"output_type": "fake", "field_filter": "shared"},
		}),
	})
	cfg.FieldFilters = map[string]config.FieldFilterConfig{
		"shared": {Name: "shared", Whitelist: true, Fields: []string{"MESSAGE"}},
	}
	p, rt := newTestRuntime(t, cfg, &captureLog{})
	if len(rt.resolved) != 2 {
		t.Fatalf("resolved senders = %d, want 2", len(rt.resolved))
	}
	shared := rt.pump.fieldFilters["shared"]
	for _, r := range rt.resolved {
		if r.fieldFilter == nil || r.fieldFilter != shared {
			t.Errorf("sender %s did not capture the pump-level filter pointer", r.name)
		}
	}

	rt.initSenders()
	stopSenders(t, p)
	entry := journal.Entry{Cursor: "c", Fields: []journal.Field{
		{Key: "MESSAGE", Value: "hi"},
		{Key: "SECRETVALUE", Value: "nope"},
	}}
	if _, _, _, err := rt.process(context.Background(), entry); err != nil {
		t.Fatalf("process: %v", err)
	}
	rt.mu.Lock()
	ab, bb := rt.senders["a"].buffer, rt.senders["b"].buffer
	rt.mu.Unlock()
	pa := (<-ab.Chan()).Payload
	pb := (<-bb.Chan()).Payload
	if string(pa) != string(pb) {
		t.Errorf("payloads differ despite the shared filter: %s vs %s", pa, pb)
	}
	if strings.Contains(string(pa), "SECRETVALUE") || strings.Contains(string(pa), "nope") {
		t.Errorf("whitelist filter not applied: %s", pa)
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

// blockingCleanupTransport models a transport whose send AND cleanup both stay
// blocked: Finalize and Close record the call and then block until released.
// Used to prove shutdown EXCLUDES a still-draining sender from synchronous
// cleanup (item 5) rather than wedging on it.
type blockingCleanupTransport struct {
	fakeTransport
	block          chan struct{}
	finalizeCalled bool
	closeCalled    bool
}

func (b *blockingCleanupTransport) Finalize() error {
	b.finalizeCalled = true
	<-b.block
	return nil
}

func (b *blockingCleanupTransport) Close() error {
	b.closeCalled = true
	<-b.block
	return nil
}

// TestShutdownSkipsCleanupForDrainingSender locks item 5: a sender that has
// not joined within the bound is excluded from the synchronous Finalize/Close
// cleanup, so a transport whose send AND close both stay blocked cannot wedge
// shutdown. The bounded join proceeds, state is saved, and the process exits.
func TestShutdownSkipsCleanupForDrainingSender(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := testPumpConfig(map[string]config.ReaderConfig{"r": fakeReaderConfig(nil)})
		statePath := filepath.Join(t.TempDir(), "state.json")
		cfg.JSONStateFilePath = statePath
		p, _ := newTestRuntime(t, cfg, &captureLog{})
		p.statePath = statePath

		// The sender group never finishes, forcing the hard stop.
		release := make(chan struct{})
		p.senderGroup.Go(func() error { <-release; return nil })
		t.Cleanup(func() { close(release) })

		transport := &blockingCleanupTransport{block: make(chan struct{})}
		t.Cleanup(func() { close(transport.block) })
		entry := &senderEntry{
			name:   "kafka",
			cancel: func() {},
			done:   make(chan struct{}), // never closed → still draining
			inst:   sender.Instance{Transport: transport},
		}
		p.senderMu.Lock()
		p.senders["r"] = []*senderEntry{entry}
		p.senderMu.Unlock()

		done := make(chan error, 1)
		go func() { done <- p.shutdown(func() {}, func() error { return nil }) }()
		select {
		case <-done:
		case <-time.After(2 * senderJoinHardStop):
			t.Fatal("shutdown did not return; a blocked transport wedged it")
		}
		if transport.finalizeCalled {
			t.Error("Finalize was called on a sender that did not join within the bound")
		}
		if transport.closeCalled {
			t.Error("Close was called on a sender that did not join within the bound")
		}
		if _, statErr := os.Stat(statePath); statErr != nil {
			t.Errorf("state not saved despite the draining sender: %v", statErr)
		}
	})
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

// TestWaitSendersBoundedProceedsAfterHardStop locks the sanctioned §2
// deviation (assessment docs/assess-sender-cancellation.md item 1): a sender
// that has not returned after cancel must NOT hang shutdown. The bounded join
// returns at senderJoinHardStop, logs the still-draining sender by name, and
// lets the supervisor finalize, save state and exit. synctest fast-forwards
// the hard-stop timer so the test does not wait 30s.
func TestWaitSendersBoundedProceedsAfterHardStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		log := &captureLog{}
		cfg := testPumpConfig(map[string]config.ReaderConfig{"r": fakeReaderConfig(nil)})
		p, _ := newTestRuntime(t, cfg, log)

		release := make(chan struct{})
		p.senderGroup.Go(func() error { <-release; return nil })
		t.Cleanup(func() { close(release) })

		// A live sender whose loop goroutine never closes done: still draining.
		entry := &senderEntry{name: "kafka", done: make(chan struct{})}
		live := []liveSender{{reader: "r", entry: entry}}

		start := time.Now()
		draining, err := p.waitSendersBounded(live)
		if err != nil {
			t.Fatalf("waitSendersBounded err = %v, want nil (proceed at the hard stop)", err)
		}
		if elapsed := time.Since(start); elapsed < senderJoinHardStop {
			t.Fatalf("returned after %v, want the %v hard stop", elapsed, senderJoinHardStop)
		}
		if !draining[entry] {
			t.Error("the still-draining sender was not reported, so shutdown would synchronously Finalize/Close it")
		}
		if !log.containsError("still draining") || !log.containsError("kafka") {
			t.Errorf("hard-stop warning missing or did not name the sender; lines = %v", log.lines)
		}
	})
}

// TestWaitSendersBoundedJoinsCleanlyWhenSendersFinish locks the fast path:
// when every sender has already finished, the bounded join returns the group's
// result and logs no warning.
func TestWaitSendersBoundedJoinsCleanlyWhenSendersFinish(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		log := &captureLog{}
		cfg := testPumpConfig(map[string]config.ReaderConfig{"r": fakeReaderConfig(nil)})
		p, _ := newTestRuntime(t, cfg, log)

		done := make(chan struct{})
		close(done)
		p.senderGroup.Go(func() error { return nil })

		entry := &senderEntry{name: "kafka", done: done}
		draining, err := p.waitSendersBounded([]liveSender{{reader: "r", entry: entry}})
		if err != nil {
			t.Fatalf("waitSendersBounded err = %v, want nil", err)
		}
		if len(draining) != 0 {
			t.Errorf("draining = %v, want empty on the clean path", draining)
		}
		if log.containsError("still draining") {
			t.Errorf("unexpected hard-stop warning on the clean path; lines = %v", log.lines)
		}
	})
}

// TestFailedFinalFlushRollsBackState locks the flush ordering (major 2):
// the buffered transport is finalized BEFORE the final state save; a
// failed final flush is surfaced as an error and the sender's sent
// cursor is rolled back, so the state file never records undelivered
// messages as sent (no silent exit 0).
func TestFailedFinalFlushRollsBackState(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := testPumpConfig(map[string]config.ReaderConfig{
			"r": fakeReaderConfig(map[string]any{"s": map[string]any{"output_type": "flushfail"}}),
		})
		statePath := filepath.Join(t.TempDir(), "state.json")
		cfg.JSONStateFilePath = statePath
		p, rt := newTestRuntime(t, cfg, &captureLog{})
		p.statePath = statePath
		rt.initSenders()

		fr := &fakeReader{}
		rt.journaldReader = fr
		entries := []journal.Entry{
			{Cursor: "c1", Fields: []journal.Field{{Key: "MESSAGE", Value: "a"}}},
			{Cursor: "c2", Fields: []journal.Field{{Key: "MESSAGE", Value: "b"}}},
		}
		for _, entry := range entries {
			if _, _, _, err := rt.process(context.Background(), entry); err != nil {
				t.Fatalf("process: %v", err)
			}
		}

		err := p.shutdown(func() {}, func() error { return nil })
		if err == nil || !strings.Contains(err.Error(), "final flush boom") {
			t.Fatalf("shutdown err = %v, want the final-flush error", err)
		}

		st, readErr := state.Read(statePath)
		if readErr != nil {
			t.Fatalf("read state: %v", readErr)
		}
		rs := st.Readers["r"]
		if rs == nil || len(rs.Senders) != 1 {
			t.Fatalf("state senders = %+v", rs)
		}
		if got := rs.Senders[0].State.Sent.Count; got != 0 {
			t.Errorf("persisted sent count = %d, want 0 (rolled back after the failed flush)", got)
		}
		if got := rs.Senders[0].State.Sent.Cursor; got != nil {
			t.Errorf("persisted sent cursor = %v, want nil", got)
		}
	})
}

// TestReaderLoggerCarriesComponentName locks the per-component logger
// context: the reader's logger is Python's JournalReader:<name> (so its
// lines carry that LOGGER and %(name)s), while the pump's own logger
// stays "journalpump".
func TestReaderLoggerCarriesComponentName(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "journal.sock")
	conn, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: sock, Net: "unixgram"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	previous := journaldlog.DefaultSocket
	journaldlog.DefaultSocket = sock
	t.Cleanup(func() { journaldlog.DefaultSocket = previous })

	cfg := testPumpConfig(map[string]config.ReaderConfig{"r": fakeReaderConfig(nil)})
	log := journaldlog.NewJournalLog("journalpump", journaldlog.LevelDebug)
	_, rt := newTestRuntime(t, cfg, log)

	rt.log.Printf("reader-line")
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var got string
	for i := 0; i < 10; i++ {
		buf := make([]byte, 4096)
		n, _, err := conn.ReadFromUnix(buf)
		if err != nil {
			t.Fatalf("read reader datagram: %v", err)
		}
		got = string(buf[:n])
		if strings.Contains(got, "reader-line") {
			break
		}
	}
	if !strings.Contains(got, "LOGGER=JournalReader:r\n") {
		t.Errorf("datagram = %q, want LOGGER=JournalReader:r", got)
	}
	if !strings.Contains(got, "SYSLOG_IDENTIFIER=journalpump\n") {
		t.Errorf("datagram = %q, want the root SYSLOG_IDENTIFIER", got)
	}
}

// a REAL state-save failure: Python logs "Saving state at shutdown
// failed" via log.exception (ERROR), so the failure must reach Errorf.
func TestShutdownStateSaveFailureLoggedAsError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		log := &captureLog{}
		cfg := testPumpConfig(map[string]config.ReaderConfig{"r": fakeReaderConfig(nil)})
		p, _ := newTestRuntime(t, cfg, log)
		// A path whose parent directory does not exist: the write fails.
		p.statePath = filepath.Join(t.TempDir(), "missing-dir", "state.json")

		_ = p.shutdown(func() {}, func() error { return nil })

		if !log.containsError("Saving state at shutdown failed") {
			t.Errorf("state-save failure not logged at ERROR; lines = %v", log.lines)
		}
	})
}

// ---------------------------------------------------------------------------
// delivery checkpoint (durable sender cursor)

// liveSenderByName returns the live sender entry for reader/name.
func liveSenderByName(t *testing.T, p *Pump, reader, name string) *senderEntry {
	t.Helper()
	p.senderMu.Lock()
	defer p.senderMu.Unlock()
	for _, e := range p.senders[reader] {
		if e.name == name {
			return e
		}
	}
	t.Fatalf("sender %s/%s not found", reader, name)
	return nil
}

// stopSendersNow cancels and joins every live sender immediately. Used as
// a defer inside synctest bubbles so a failing assertion cannot leave a
// sender goroutine blocked and deadlock the bubble during teardown.
func stopSendersNow(p *Pump) {
	p.senderMu.Lock()
	var entries []*senderEntry
	for _, es := range p.senders {
		entries = append(entries, es...)
	}
	p.senderMu.Unlock()
	for _, e := range entries {
		if e.cancel != nil {
			e.cancel()
		}
	}
	_ = p.senderGroup.Wait()
}

// waitSent advances time until the named sender has acked want messages
// (mark_sent ran), so the durable-cursor mapping has items to map.
func waitSent(t *testing.T, p *Pump, reader, name string, want int64) {
	t.Helper()
	for i := 0; i < 400; i++ {
		time.Sleep(5 * time.Millisecond)
		if got := liveSenderByName(t, p, reader, name).loop.State().Sent.Count; got >= want {
			return
		}
	}
	t.Fatalf("sender %s/%s never acked %d messages", reader, name, want)
}

// processEntries feeds entries through the reader runtime's handler, so
// they land in the running senders' buffers.
func processEntries(t *testing.T, rt *ReaderRuntime, cursors ...string) {
	t.Helper()
	for _, c := range cursors {
		entry := journal.Entry{Cursor: c, Fields: []journal.Field{{Key: "MESSAGE", Value: c}}}
		if _, _, _, err := rt.process(context.Background(), entry); err != nil {
			t.Fatalf("process %s: %v", c, err)
		}
	}
}

// persistedSenderCursor reads the state file's sent cursor for a sender.
func persistedSenderCursor(t *testing.T, statePath, reader, name string) *string {
	t.Helper()
	st, err := state.Read(statePath)
	if err != nil {
		t.Fatalf("read state: %v", err)
	}
	rs := st.Readers[reader]
	if rs == nil {
		t.Fatalf("reader %s missing from state", reader)
	}
	for i := range rs.Senders {
		if rs.Senders[i].Name == name {
			return rs.Senders[i].State.Sent.Cursor
		}
	}
	t.Fatalf("sender %s missing from state", name)
	return nil
}

// TestDurableShutdownFlushAdvancesCursor locks the happy path: while the
// lines are buffered the durable cursor is pinned at the resume baseline
// even though mark_sent advanced the logical position; a successful
// shutdown flush (D9) advances the persisted cursor to the full sent
// position.
func TestDurableShutdownFlushAdvancesCursor(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := testPumpConfig(map[string]config.ReaderConfig{
			"r": fakeReaderConfig(map[string]any{"s": map[string]any{"output_type": "durable"}}),
		})
		statePath := filepath.Join(t.TempDir(), "state.json")
		cfg.JSONStateFilePath = statePath
		p, rt := newTestRuntime(t, cfg, &captureLog{})
		p.statePath = statePath
		rt.resumeCursor = "resume"
		rt.initSenders()
		defer stopSendersNow(p)

		processEntries(t, rt, "c1", "c2")
		waitSent(t, p, "r", "s", 2)

		// Logical position reached, nothing flushed: the durable cursor
		// is pinned at the resume baseline, not the logical c2.
		if cur := liveSenderByName(t, p, "r", "s").loop.State().Sent.Cursor; cur == nil || *cur != "resume" {
			t.Fatalf("pre-flush durable cursor = %v, want resume", cur)
		}

		if err := p.shutdown(func() {}, func() error { return nil }); err != nil {
			t.Fatalf("shutdown: %v", err)
		}
		if cur := persistedSenderCursor(t, statePath, "r", "s"); cur == nil || *cur != "c2" {
			t.Fatalf("persisted cursor after a successful shutdown flush = %v, want c2", cur)
		}
	})
}

// TestDurableFailedFlushAfterPeriodicSaveResumesFromFlushedBoundary locks
// the restart contract: a periodic save persisted the flushed boundary
// (c1); a later line (c2) is sent but never flushed, and the final flush
// fails. The persisted cursor must stay at the flushed boundary c1 — so a
// restart RESUMES from c1 and replays c2, never skipping it.
func TestDurableFailedFlushAfterPeriodicSaveResumesFromFlushedBoundary(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := testPumpConfig(map[string]config.ReaderConfig{
			"r": fakeReaderConfig(map[string]any{"s": map[string]any{"output_type": "durable"}}),
		})
		statePath := filepath.Join(t.TempDir(), "state.json")
		cfg.JSONStateFilePath = statePath
		p, rt := newTestRuntime(t, cfg, &captureLog{})
		p.statePath = statePath
		rt.resumeCursor = "resume"
		rt.initSenders()
		defer stopSendersNow(p)
		tr := durableTransportRef()

		processEntries(t, rt, "c1")
		waitSent(t, p, "r", "s", 1)
		// The first line's bytes reached the sink (buffer auto-flush).
		tr.setFlushed(tr.logicalBytes())
		if cur := liveSenderByName(t, p, "r", "s").loop.State().Sent.Cursor; cur == nil || *cur != "c1" {
			t.Fatalf("durable cursor after the first flush = %v, want c1", cur)
		}
		// A periodic save persists that flushed boundary.
		if err := p.saveState(); err != nil {
			t.Fatalf("saveState: %v", err)
		}
		if cur := persistedSenderCursor(t, statePath, "r", "s"); cur == nil || *cur != "c1" {
			t.Fatalf("persisted cursor after the periodic save = %v, want c1", cur)
		}

		// A second line is accepted but never flushed.
		processEntries(t, rt, "c2")
		waitSent(t, p, "r", "s", 2)
		tr.setFlushErr(errors.New("flush boom"))

		err := p.shutdown(func() {}, func() error { return nil })
		if err == nil || !strings.Contains(err.Error(), "flush boom") {
			t.Fatalf("shutdown err = %v, want the flush error", err)
		}

		// No rollback for a durable sender: its cursor already lagged to
		// the flushed boundary.
		if cur := persistedSenderCursor(t, statePath, "r", "s"); cur == nil || *cur != "c1" {
			t.Fatalf("persisted cursor after the failed flush = %v, want c1", cur)
		}
		// Restarting resumes from c1 (replaying c2): no skip.
		st, readErr := state.Read(statePath)
		if readErr != nil {
			t.Fatalf("read state: %v", readErr)
		}
		resume, resumeErr := state.ResumeCursor(st.Readers["r"], "r", nil)
		if resumeErr != nil {
			t.Fatalf("ResumeCursor: %v", resumeErr)
		}
		if resume == nil || *resume != "c1" {
			t.Fatalf("restart resumes from %v, want c1", resume)
		}
	})
}

// TestDurableFailedFlushBeforeFirstSaveShowsPreRunPosition locks the
// no-progress case: a failed flush BEFORE any periodic save leaves the
// persisted cursor at the PRE-RUN position (the resume baseline), so the
// state file never claims delivery that did not happen.
func TestDurableFailedFlushBeforeFirstSaveShowsPreRunPosition(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := testPumpConfig(map[string]config.ReaderConfig{
			"r": fakeReaderConfig(map[string]any{"s": map[string]any{"output_type": "durable"}}),
		})
		statePath := filepath.Join(t.TempDir(), "state.json")
		cfg.JSONStateFilePath = statePath
		p, rt := newTestRuntime(t, cfg, &captureLog{})
		p.statePath = statePath
		rt.resumeCursor = "resume"
		rt.initSenders()
		defer stopSendersNow(p)
		durableTransportRef().setFlushErr(errors.New("flush boom"))

		processEntries(t, rt, "c1")
		waitSent(t, p, "r", "s", 1)

		err := p.shutdown(func() {}, func() error { return nil })
		if err == nil || !strings.Contains(err.Error(), "flush boom") {
			t.Fatalf("shutdown err = %v, want the flush error", err)
		}
		if cur := persistedSenderCursor(t, statePath, "r", "s"); cur == nil || *cur != "resume" {
			t.Fatalf("persisted cursor after an unflushed run = %v, want resume", cur)
		}
	})
}

// TestReloadRetireDiscardsUnflushedFileTail locks the reload/restart
// interaction with the real file sender: a line acked into the file
// sender's buffer (not yet on disk) is DROPPED when a SIGHUP reload
// retires the reader (sender.Discarder → CloseDiscarding), so the reload
// never delivers a line the persisted cursor does not cover.
func TestReloadRetireDiscardsUnflushedFileTail(t *testing.T) {
	registerFakeSenders()
	overrideReopener(t)
	outPath := filepath.Join(t.TempDir(), "out.log")
	initial := decodeConfig(t, fmt.Sprintf(`{"readers":{"r1":{"senders":{"s":{"output_type":"file","file_output":%q}}}}}`, outPath))
	updated := decodeConfig(t, fmt.Sprintf(`{"readers":{"r2":{"senders":{"s":{"output_type":"file","file_output":%q}}}}}`, outPath))
	p, err := NewPump(initial, PumpDeps{Log: &captureLog{}})
	if err != nil {
		t.Fatal(err)
	}
	cancel := startReaderGroup(p)
	defer cancel()

	rt := p.readers[0]
	rt.initSenders()
	rt.journaldReader = &fakeReader{}
	processEntries(t, rt, "c1")
	waitSent(t, p, "r1", "s", 1)

	// The line is acked but still buffered, not on disk.
	if info, statErr := os.Stat(outPath); statErr != nil {
		t.Fatalf("stat: %v", statErr)
	} else if info.Size() != 0 {
		t.Fatalf("unflushed tail visible on disk: size = %d", info.Size())
	}

	// A changed readers blob retires r1 and its file sender.
	if err := p.handleNewConfig(updated); err != nil {
		t.Fatalf("handleNewConfig: %v", err)
	}
	// The unclaimed tail must be discarded, not flushed: a replay after
	// the reload is then safe (no duplicate, no gap).
	if info, statErr := os.Stat(outPath); statErr != nil {
		t.Fatalf("stat after reload: %v", statErr)
	} else if info.Size() != 0 {
		t.Errorf("reload retire flushed the unclaimed tail: size = %d", info.Size())
	}
	retireAll(p)
}

// TestReloadRetireTruncatesPartialLineKeepingValidJSONL locks the
// partial-line fix end to end: a buffer auto-flush cuts through the second
// JSON line, leaving a corrupt prefix on disk; the reload retire truncates
// it back to the first complete line, so the file stays valid JSONL.
func TestReloadRetireTruncatesPartialLineKeepingValidJSONL(t *testing.T) {
	registerFakeSenders()
	overrideReopener(t)
	outPath := filepath.Join(t.TempDir(), "out.log")
	// A message size between half and all of the 131072-byte buffer makes
	// the second message overflow the buffer, so its flush cuts through
	// the line (the first line is complete, the second is partial).
	padding := strings.Repeat("x", 69000)
	cfgJSON := fmt.Sprintf(`{"readers":{"r1":{"senders":{"s":{"output_type":"file","file_output":%q,"extra_field_values":{"PADDING":%q}}}}}}`, outPath, padding)
	initial := decodeConfig(t, cfgJSON)
	updated := decodeConfig(t, `{"readers":{"r2":{"senders":{"s":{"output_type":"file","file_output":"`+outPath+`"}}}}}`)
	p, err := NewPump(initial, PumpDeps{Log: &captureLog{}})
	if err != nil {
		t.Fatal(err)
	}
	cancel := startReaderGroup(p)
	defer cancel()

	rt := p.readers[0]
	rt.initSenders()
	rt.journaldReader = &fakeReader{}
	processEntries(t, rt, "c1", "c2")
	waitSent(t, p, "r1", "s", 2)

	// The auto-flush left a partial second line on disk.
	if info, statErr := os.Stat(outPath); statErr != nil {
		t.Fatalf("stat: %v", statErr)
	} else if info.Size() == 0 {
		t.Fatalf("expected the auto-flush to write bytes")
	}

	if err := p.handleNewConfig(updated); err != nil {
		t.Fatalf("handleNewConfig: %v", err)
	}

	raw, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	if len(raw) > 0 && raw[len(raw)-1] != '\n' {
		t.Fatalf("output does not end on a line boundary: %q", raw[max(0, len(raw)-40):])
	}
	lines := strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n")
	if len(lines) == 0 || (len(lines) == 1 && lines[0] == "") {
		t.Fatalf("expected at least one complete line, got %q", raw)
	}
	seen := map[string]bool{}
	for i, line := range lines {
		if !json.Valid([]byte(line)) {
			t.Fatalf("line %d is not valid JSON: %q", i, line)
		}
		if seen[line] {
			t.Fatalf("duplicate output line %d", i)
		}
		seen[line] = true
	}
	retireAll(p)
}

// TestReloadGlobalMatchAppliesToUnchangedReader locks the global-settings
// semantics: a reload changing ONLY the pump-level match rules, with an
// unchanged readers blob, keeps the reader — but the new rules take effect,
// because the handler consults the current pump settings per entry
// (journalpump.py:723 self.pump.check_match).
func TestReloadGlobalMatchAppliesToUnchangedReader(t *testing.T) {
	registerFakeSenders()
	initial := decodeConfig(t, `{"match_key":"SYSLOG_IDENTIFIER","match_value":"old","readers":{"r1":{"senders":{"s":{"output_type":"fake"}}}}}`)
	updated := decodeConfig(t, `{"match_key":"SYSLOG_IDENTIFIER","match_value":"new","readers":{"r1":{"senders":{"s":{"output_type":"fake"}}}}}`)
	p, err := NewPump(initial, PumpDeps{Log: &captureLog{}})
	if err != nil {
		t.Fatal(err)
	}
	before := p.readers[0]
	if err := p.handleNewConfig(updated); err != nil {
		t.Fatalf("handleNewConfig: %v", err)
	}
	if p.readers[0] != before {
		t.Fatal("reader was replaced despite an unchanged readers blob")
	}
	rt := p.readers[0]
	rt.initSenders()
	defer stopSendersNow(p)

	hit := journal.Entry{Cursor: "c1", Fields: []journal.Field{
		{Key: "SYSLOG_IDENTIFIER", Value: "new"},
		{Key: "MESSAGE", Value: "m"},
	}}
	if _, _, hasBytes, err := rt.process(context.Background(), hit); err != nil || !hasBytes {
		t.Fatalf("new global match rule not applied: hasBytes=%v err=%v", hasBytes, err)
	}
	miss := journal.Entry{Cursor: "c2", Fields: []journal.Field{
		{Key: "SYSLOG_IDENTIFIER", Value: "old"},
		{Key: "MESSAGE", Value: "m"},
	}}
	if _, _, hasBytes, _ := rt.process(context.Background(), miss); hasBytes {
		t.Error("old global match value still accepted after the reload")
	}
}

// ---------------------------------------------------------------------------
// Pump.Run supervisor

// TestSenderErrorFailsFastAndHonorsGrace locks the fail-fast ruling: a
// sender-LOOP failure terminates the daemon on its own (NO external
// cancellation), propagates as Run's result, and still honors the full
// grace window for the OTHER senders.
//
// The survivor's flush is GATED (its loop parks in maintenance), so
// nothing is delivered while the daemon runs. The watcher releases it
// only after the graceWaitHook proves shutdown is parked in its grace
// window; at that point the survivor context must be uncancelled and
// zero messages delivered. It then releases the gate and asserts both
// buffered messages are delivered. If the failure cancelled every sender
// immediately, the survivor would be cancelled at that point and its
// drain would abort (delivery 0).
func TestSenderErrorFailsFastAndHonorsGrace(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		disableSignals(t)
		release := make(chan struct{})
		setGatedRelease(release)

		graceStarted := make(chan struct{})
		origHook := graceWaitHook
		graceWaitHook = func() { close(graceStarted) }
		t.Cleanup(func() { graceWaitHook = origHook })

		orig := runSenderLoop
		runSenderLoop = func(l *sender.Loop, ctx context.Context) error {
			if l.Name() == "boom" {
				// Delay so the reader drains its entries into the gated
				// sender's buffer before the failure surfaces.
				time.Sleep(5 * pollInterval)
				return errors.New("sender boom")
			}
			return l.Run(ctx)
		}
		t.Cleanup(func() { runSenderLoop = orig })

		cfg := testPumpConfig(map[string]config.ReaderConfig{
			"r": fakeReaderConfig(map[string]any{
				"boom":  map[string]any{"output_type": "fake"},
				"gated": map[string]any{"output_type": "gatedblock"},
			}),
		})
		p, rt := newTestRuntime(t, cfg, &captureLog{})
		notifier := &fakeNotifier{}
		p.notifier = notifier
		rt.reopener = &fakeSource{reader: &fakeReader{
			firstWait: true,
			entries: []journal.Entry{
				{Cursor: "c1", Fields: []journal.Field{{Key: "MESSAGE", Value: "a"}}},
				{Cursor: "c2", Fields: []journal.Field{{Key: "MESSAGE", Value: "b"}}},
			},
		}}

		released := make(chan struct{})
		go func() {
			// Wait until shutdown is observably parked in the grace
			// window, then quiesce.
			<-graceStarted
			synctest.Wait()
			g := gatedTransportRef()
			if g == nil {
				t.Error("gated transport missing")
				close(release)
				close(released)
				return
			}
			if g.ctxCancelled() {
				t.Error("survivor context cancelled before the grace window elapsed")
			}
			if got := g.messages(); got != 0 {
				t.Errorf("survivor delivered %d messages before the grace window", got)
			}
			close(release)
			close(released)
		}()

		// No external cancellation: the sender failure alone must stop it.
		done := make(chan error, 1)
		go func() { done <- p.Run(context.Background(), make(chan os.Signal)) }()

		select {
		case err := <-done:
			if err == nil || !strings.Contains(err.Error(), "sender boom") {
				t.Fatalf("Run = %v, want the sender error", err)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("Run did not return after the sender failure (no external cancellation)")
		}
		<-released
		if !notifier.notified(StateStopping) {
			t.Error("Run did not notify STOPPING=1")
		}
		// The OTHER sender got its full grace window: both buffered
		// messages were delivered.
		if got := gatedTransportRef().messages(); got != 2 {
			t.Errorf("gated sender delivered %d messages, want 2 (grace window honored)", got)
		}
	})
}

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

// slowReleaseMu guards slowReleaseCh so each test invocation gets its
// own release channel; a package-global channel closed by one run would
// panic the next under -count=2.
var (
	slowReleaseMu sync.Mutex
	slowReleaseCh = make(chan struct{})
)

func setSlowRelease(ch chan struct{}) {
	slowReleaseMu.Lock()
	slowReleaseCh = ch
	slowReleaseMu.Unlock()
}

func currentSlowRelease() <-chan struct{} {
	slowReleaseMu.Lock()
	defer slowReleaseMu.Unlock()
	return slowReleaseCh
}

// registerSlowSender wires the blocking sender once: its
// MaintenanceOperations blocks until its release channel is closed, so
// the loop never reaches its drain and the channel fills like a slow
// consumer. Each constructor call snapshots the current release channel.
func registerSlowSender() {
	registerFakeSenders()
	registerSlowOnce.Do(func() {
		sender.Register("slowblock", func(sender.ConstructorArgs) (sender.Instance, error) {
			return sender.Instance{
				Transport:       &slowTransport{release: currentSlowRelease()},
				MaxSendInterval: 50 * time.Millisecond,
				Type:            "SlowSender",
			}, nil
		})
	})
}

var registerSlowOnce sync.Once

type slowTransport struct{ release <-chan struct{} }

func (s *slowTransport) SendMessages(context.Context, [][]byte, string) error { return nil }
func (s *slowTransport) MaintenanceOperations(ctx context.Context) error {
	select {
	case <-s.release:
	case <-ctx.Done():
	}
	return nil
}

// TestComposedBackpressure drives the READER LOOP (not a manual enqueue):
// the slow sender's full channel makes the reader stop consuming entries
// (readiness false + pause log); once the sender drains, reads resume
// (readiness true + resume log).
func TestComposedBackpressure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		registerSlowSender()
		release := make(chan struct{})
		setSlowRelease(release)
		cfg := testPumpConfig(map[string]config.ReaderConfig{
			"r": fakeReaderConfig(map[string]any{
				"slow": map[string]any{"output_type": "slowblock"},
				"fast": fakeSenderConfig(),
			}),
		})
		cfg.MsgBufferMaxLength = 3
		cfg.MsgBufferMaxBytes = 1 << 20 // count gates; keep the byte budget out of the way
		log := &captureLog{}
		p, rt := newTestRuntime(t, cfg, log)

		entries := make([]journal.Entry, 40)
		for i := range entries {
			entries[i] = journal.Entry{Cursor: fmt.Sprintf("c%d", i), Fields: []journal.Field{{Key: "MESSAGE", Value: "x"}}}
		}
		fr := &fakeReader{repeatAppend: true, entries: entries}
		rt.reopener = &fakeSource{reader: fr}

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- rt.run(ctx) }()
		t.Cleanup(func() {
			cancel()
			<-done
			stopSenders(t, p)
		})

		// Let the loop fill the slow sender's channel and pause.
		time.Sleep(20 * pollInterval)
		synctest.Wait()
		if rt.readyFlag {
			t.Fatal("readyFlag = true, want paused with the slow sender full")
		}
		if !log.contains("pause processing journal for \"r\"") {
			t.Errorf("missing pause log; got %v", log.lines)
		}
		pausedAt := fr.readCount()
		if pausedAt == 0 {
			t.Fatal("reader consumed nothing before pausing")
		}
		if pausedAt >= len(entries) {
			t.Fatalf("reader consumed all %d entries despite the pause", len(entries))
		}

		// The pause must actually stop reads: no further consumption.
		time.Sleep(20 * pollInterval)
		synctest.Wait()
		if got := fr.readCount(); got != pausedAt {
			t.Errorf("reads continued while paused: %d -> %d", pausedAt, got)
		}

		// Drain: release the slow sender; reads resume.
		close(release)
		time.Sleep(20 * pollInterval)
		synctest.Wait()
		if !rt.readyFlag {
			t.Error("readyFlag = false, want resumed after the drain")
		}
		if !log.contains("ready to process journal for \"r\"") {
			t.Errorf("missing resume log; got %v", log.lines)
		}
		if got := fr.readCount(); got <= pausedAt {
			t.Errorf("reads did not resume: %d, paused at %d", got, pausedAt)
		}
	})
}

// TestReadMessagesDecrementsByMaxSerializedSize locks the byte-budget
// decrement composed THROUGH readMessages: the count/byte budgets are
// decremented by the MAX serialized size across the sender targets, not
// the min, so a budget set between the two stops the chunk after one
// message.
func TestReadMessagesDecrementsByMaxSerializedSize(t *testing.T) {
	newEntry := func(cursor string) journal.Entry {
		return journal.Entry{Cursor: cursor, Fields: []journal.Field{
			{Key: "MESSAGE", Value: "hi"},
			{Key: "VALUE", Value: strings.Repeat("v", 200)},
		}}
	}
	filtered := map[string]any{"output_type": "fake", "field_filter": "m"}

	// Measure the serialized size the handler reports for each sender
	// alone (separate runtimes avoid the shared serialization cache).
	measure := func(senderName string, cfgSender map[string]any) int {
		rc := fakeReaderConfig(map[string]any{senderName: cfgSender})
		rc.SenderOrder = []string{senderName}
		cfg := testPumpConfig(map[string]config.ReaderConfig{"r": rc})
		cfg.MsgBufferMaxLength = 50000
		cfg.MsgBufferMaxBytes = 1 << 20
		cfg.FieldFilters = map[string]config.FieldFilterConfig{"m": {Name: "m", Whitelist: true, Fields: []string{"MESSAGE"}}}
		p, rt := newTestRuntime(t, cfg, &captureLog{})
		rt.initSenders()
		stopSenders(t, p)
		_, bytesRead, hasBytes, err := rt.process(context.Background(), newEntry("c"))
		if err != nil {
			t.Fatalf("process: %v", err)
		}
		if !hasBytes || bytesRead <= 0 {
			t.Fatalf("sender %s: bytesRead = %d", senderName, bytesRead)
		}
		return bytesRead
	}
	sizeA := measure("a", fakeSenderConfig()) // no filter: MESSAGE + VALUE
	sizeB := measure("b", filtered)           // whitelist: MESSAGE only
	if sizeA < sizeB+2 {
		t.Fatalf("serialized sizes not distinct enough: a=%d b=%d", sizeA, sizeB)
	}

	// Budget strictly between the two: if the loop decremented by the
	// MIN size it would keep reading; by the MAX it stops after one.
	maxBytes := sizeB + 1
	rc := fakeReaderConfig(map[string]any{"a": fakeSenderConfig(), "b": filtered})
	rc.SenderOrder = []string{"a", "b"}
	cfg := testPumpConfig(map[string]config.ReaderConfig{"r": rc})
	cfg.MsgBufferMaxLength = 50000
	cfg.MsgBufferMaxBytes = int64(maxBytes)
	cfg.FieldFilters = map[string]config.FieldFilterConfig{"m": {Name: "m", Whitelist: true, Fields: []string{"MESSAGE"}}}
	p, rt := newTestRuntime(t, cfg, &captureLog{})
	rt.initSenders()
	stopSenders(t, p)

	fr := &fakeReader{entries: []journal.Entry{newEntry("c1"), newEntry("c2"), newEntry("c3")}}
	rt.journaldReader = fr
	_, lines, err := rt.readMessages(context.Background())
	if err != nil {
		t.Fatalf("readMessages: %v", err)
	}
	if lines != 1 {
		t.Errorf("lines = %d, want 1 (budget decremented by the max serialized size %d, not the min %d)", lines, sizeA, sizeB)
	}
	if got := fr.readCount(); got != 1 {
		t.Errorf("read %d entries, want 1", got)
	}
	if got := rt.writeLimitBytes(); got > 0 {
		t.Errorf("writeLimitBytes = %d, want <= 0 after the max-size decrement", got)
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

// collect drains every datagram that arrives within a short window, so a
// test can assert a capture received several distinct metrics.
func (c *udpCapture) collect(t *testing.T) []string {
	t.Helper()
	_ = c.conn.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	var out []string
	buf := make([]byte, 4096)
	for {
		n, _, err := c.conn.ReadFromUDP(buf)
		if err != nil {
			return out
		}
		out = append(out, string(buf[:n]))
	}
}

// containsDatagram reports whether any captured datagram contains sub.
func containsDatagram(datagrams []string, sub string) bool {
	for _, d := range datagrams {
		if strings.Contains(d, sub) {
			return true
		}
	}
	return false
}

// TestBroadHandlerDelayAndTags locks read_single_message's except-arm
// through the ENTRY POINT: a read failure routes into the broad handler,
// which logs, emits unexpected_exception with where=mainloop and
// app=journalpump, sleeps 500 ms (asserted on the fake clock), and
// returns hasMore=false.
func TestBroadHandlerDelayAndTags(t *testing.T) {
	capture := newUDPCapture(t)
	host, port := capture.addr()
	log := &captureLog{}

	synctest.Test(t, func(t *testing.T) {
		cfg := testPumpConfig(map[string]config.ReaderConfig{"r": fakeReaderConfig(nil)})
		cfg.Statsd = config.StatsdConfig{Host: &host, Port: &port, Tags: map[string]string{}}
		_, rt := newTestRuntime(t, cfg, log)
		rt.journaldReader = &fakeReader{readErr: errors.New("boom")}

		start := time.Now()
		hasMore, bytesRead, hasBytes, err := rt.readSingleMessage(context.Background())
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("readSingleMessage: %v", err)
		}
		if hasMore || hasBytes || bytesRead != 0 {
			t.Errorf("got (%v,%d,%v), want (false,0,false)", hasMore, bytesRead, hasBytes)
		}
		if elapsed < 500*time.Millisecond {
			t.Errorf("broad-handler delay = %v, want >= 500ms", elapsed)
		}
	})
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

// TestStatsdReloadSplitDirection locks the stats-client split across a
// statsd-only reload with an UNCHANGED readers blob: AFTER the reload the
// reader's CAPTURED client still takes the reader-owned metrics —
// secret-filter matches AND a lazily-initialized sender's construction
// metrics — while the supervisor's periodic gauge refresh goes to the LIVE
// pump client (Python refresh_gauges, journalpump.py:1102-1108). Emitting
// only before the reload would not prove the captured client survives it.
func TestStatsdReloadSplitDirection(t *testing.T) {
	registerFakeSenders()
	captureOld := newUDPCapture(t)
	oldHost, oldPort := captureOld.addr()
	captureNew := newUDPCapture(t)
	newHost, newPort := captureNew.addr()

	readers := `{"r":{"senders":{"s":{"output_type":"probe"}}}}`
	initial := decodeConfig(t, fmt.Sprintf(`{"statsd":{"host":%q,"port":%d},"readers":%s}`, oldHost, oldPort, readers))
	updated := decodeConfig(t, fmt.Sprintf(`{"statsd":{"host":%q,"port":%d},"readers":%s}`, newHost, newPort, readers))

	synctest.Test(t, func(t *testing.T) {
		p, err := NewPump(initial, PumpDeps{Log: &captureLog{}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { retireAll(p) })
		rt := p.readers[0]

		// Statsd-only reload: same readers blob (the reader is kept), new
		// statsd endpoint.
		if err := p.handleNewConfig(updated); err != nil {
			t.Fatalf("handleNewConfig: %v", err)
		}
		if len(p.readers) != 1 || p.readers[0] != rt {
			t.Fatalf("reader replaced on a statsd-only reload (readers=%d)", len(p.readers))
		}

		// AFTER the reload, initializing a sender must use the reader's
		// CAPTURED client (the probe constructor emits through it).
		rt.initSenders()

		// Past the 10 s reader stats window, then report secret-filter
		// matches: they too must land on the reader-CAPTURED (old) client.
		time.Sleep(11 * time.Second)
		rt.secretFilterMatches = 3
		rt.reportSecretFilterMetrics()

		// The supervisor's gauge refresh uses the LIVE client.
		rt.stats.Gauges.Set("reader.nonzero", 5, map[string]string{})
		if err := p.tick(); err != nil {
			t.Fatalf("tick: %v", err)
		}
	})

	// Reader-owned metrics (sender construction + secret filter) on the
	// OLD endpoint.
	old := captureOld.collect(t)
	if !containsDatagram(old, "sender.init_probe") {
		t.Errorf("old-client datagrams = %q, want the sender-construction probe", old)
	}
	if !containsDatagram(old, "journal.secret_filter_match") {
		t.Errorf("old-client datagrams = %q, want the secret-filter metric", old)
	}
	if containsDatagram(old, "reader.nonzero") {
		t.Errorf("old client received the live-client gauge refresh: %q", old)
	}
	// Supervisor-owned gauge on the NEW endpoint.
	if newLines := captureNew.collect(t); !containsDatagram(newLines, "reader.nonzero") {
		t.Errorf("new-client datagrams = %q, want the refreshed persistent gauge", newLines)
	}
}
