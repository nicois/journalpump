// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

// Package sender ports the shared sender framework of
// journalpump/senders/base.py (plan Task 13, spec §4.2): the Sender
// contract, the per-sender classifier seam, the shared flush/backoff
// loop that owns everything generic, and the health/state bookkeeping
// the state file serializes.
//
// Shape differences from Python are deliberate (spec §4.2): Python's
// bool-returning send_messages keeps the failure cause inside each
// sender, so the Go contract returns an error and the shared loop owns
// classification (retriable vs permanent), mark_disconnected, backoff
// and the retry of the same batch from its head. The per-sender
// buffering channel itself is internal/buffer (plan Task 14): the loop
// consumes an externally owned receive channel and reads its length as
// the flush trigger, exactly the signal Python's loop takes from
// len(msg_buffer).
//
// One goroutine runs one Loop; the lifecycle (errgroup per sender,
// shutdown grace window) is the supervisor's (spec §4.3). The loop
// returns when its context is canceled; senders get their grace-flush
// window because the supervisor cancels the sender context only after
// max(max_send_interval) (sanctioned deviation D9).
package sender

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"github.com/Aiven-Open/journalpump/internal/state"
	"github.com/Aiven-Open/journalpump/internal/statsd"
)

// Logger is the logging subset the loop needs; the standard library
// *log.Logger satisfies it.
type Logger interface {
	Printf(format string, args ...any)
}

type discardLogger struct{}

func (discardLogger) Printf(_ string, _ ...any) {}

// Item is one buffered journal entry: the serialized payload bytes
// (internal/journal wire format — the loop never re-serializes) and
// the journal cursor to advance to once the item is acked.
type Item struct {
	Payload []byte
	Cursor  string
}

// Sender is the transport-specific half of a sender (spec §4.2).
// SendMessages sends one batch: a non-nil error fails the batch, the
// shared loop classifies it via the sender's Classifier and retries
// the same batch from its head on the next flush. Batch payloads are
// the already-serialized wire bytes; cursor is the LAST item's cursor
// (sent-cursor bookkeeping advances to it on ack only).
type Sender interface {
	SendMessages(ctx context.Context, batch [][]byte, cursor string) error
	MaintenanceOperations(ctx context.Context) error
}

// Classifier is the per-sender retriable(error) seam — the Go
// equivalent of Python's per-sender except-clauses
// (e.g. kafka_sender's KAFKA_CONN_ERRORS set). The loop routes a
// failed batch through it: retriable errors log-and-backoff, permanent
// errors additionally emit stats.unexpected_exception.
type Classifier interface {
	Retriable(err error) bool
}

// ClassifierFunc adapts a function to the Classifier seam.
type ClassifierFunc func(err error) bool

// Retriable calls f.
func (f ClassifierFunc) Retriable(err error) bool { return f(err) }

// InitializationError ports base.py:SenderInitializationError: a
// sender raised it when initialization cannot succeed (e.g. CloudWatch
// AccessDenied after the retry budget). The default classifier routes
// it as PERMANENT — senders that wrap their init failures with it get
// the permanent routing without their own classifier.
type InitializationError struct{ Err error }

// Error implements error.
func (e *InitializationError) Error() string {
	if e.Err == nil {
		return "sender initialization failed"
	}
	return "sender initialization failed: " + e.Err.Error()
}

// Unwrap exposes the wrapped cause.
func (e *InitializationError) Unwrap() error { return e.Err }

// defaultClassifier routes like the generic Python sender path: every
// error retries with backoff except InitializationError.
type defaultClassifier struct{}

func (defaultClassifier) Retriable(err error) bool {
	var initErr *InitializationError
	return !errors.As(err, &initErr)
}

// Framework constants ported from base.py.
const (
	// DefaultMaxBatchSize is base.py:MAX_KAFKA_MESSAGE_SIZE (1 MiB; GCP
	// logging relies on the same value).
	DefaultMaxBatchSize = 1024 * 1024
	// DefaultMessageOverhead is base.py:KAFKA_COMPRESSED_MESSAGE_OVERHEAD
	// (30 B; websocket overrides it to 1).
	DefaultMessageOverhead = 30
	// TickInterval is the base.py:run poll step (time.sleep(0.1)).
	TickInterval = 100 * time.Millisecond
	// flushThreshold is base.py's len(msg_buffer) > 1000 trigger.
	flushThreshold = 1000
	// backoffBase and backoffCap are base.py:_backoff defaults
	// (0.5 s → 1800 s, jittered, reset on connect).
	backoffBase = 500 * time.Millisecond
	backoffCap  = 1800 * time.Second
	// maxBackoffExponent bounds the exponent (base.py caps 2**1024
	// float overflow at 2**100).
	maxBackoffExponent = 100
	// MaxErrorMessages and MaxErrorMessageLen are base.py's health-ring
	// bounds (≤8 deduped errors truncated to 128 chars).
	MaxErrorMessages   = 8
	MaxErrorMessageLen = 128
	// MaintenanceFailRetry is base.py:run's 60 s gate: a failed
	// maintenance operation is not retried before 60 s.
	MaintenanceFailRetry = time.Minute
)

// healthStatus maps (running, connected) to the state-file status
// string and the journal.status gauge value
// (base.py:_convert_to_health).
type healthStatus struct {
	name  string
	gauge int
}

func health(running, connected bool) healthStatus {
	switch {
	case running && connected:
		return healthStatus{"connected", 3}
	case running:
		return healthStatus{"disconnected", 2}
	case connected:
		return healthStatus{"stale", 2}
	default:
		return healthStatus{"stopped", 0}
	}
}

// LoopConfig carries the per-sender wiring inputs that are not
// transport-specific: identity, stats and logging. Tags must be the
// final base tag set (already merged and sanitized via tagged — the
// statsd layer does no sanitization).
type LoopConfig struct {
	// Name is the sender name (the config key under "senders").
	Name string
	// Stats may be nil (Python self.stats None): gauges are skipped.
	Stats *statsd.Client
	// Tags is the sanitized base tag set (Python self.make_tags()).
	Tags map[string]string
	// Log may be nil (discard).
	Log Logger
}

// Instance is a constructed sender ready to be wrapped in a Loop: the
// transport plus the per-sender loop parameters Python's sender
// constructors set on the base class (max_send_interval defaults
// differ per sender; the batch limits are overridable, e.g. the
// websocket sender's 1-byte message overhead).
type Instance struct {
	// Transport is the sender implementation.
	Transport Sender
	// Classifier routes failed batches; nil selects the default
	// routing (retriable except InitializationError).
	Classifier Classifier
	// MaxSendInterval is the idle-flush interval (Python
	// max_send_interval; must be positive).
	MaxSendInterval time.Duration
	// MaxBatchSize and MessageOverhead override the framework defaults
	// when non-zero.
	MaxBatchSize    int
	MessageOverhead int
	// Type is the state-file "type" string (Python: the sender class
	// name, e.g. "KafkaSender").
	Type string
}

// Loop is the shared flush/backoff loop wrapping one transport — the
// port of base.py:LogSender's generic half (run/get_and_send_messages/
// _backoff/mark_*/refresh_stats/get_state). All exported methods are
// safe for concurrent use with Run.
type Loop struct {
	cfg        LoopConfig
	inst       Instance
	transport  Sender
	classifier Classifier
	in         <-chan Item
	log        Logger

	// tickInterval and maintenanceRetry are loop constants made fields
	// for tests (defaults: TickInterval, MaintenanceFailRetry).
	tickInterval     time.Duration
	maintenanceRetry time.Duration
	// maxBatchSize and messageOverhead are the resolved Instance
	// overrides (defaults: DefaultMaxBatchSize, DefaultMessageOverhead).
	maxBatchSize    int
	messageOverhead int

	mu sync.Mutex
	// running mirrors base.py self.running (true from construction,
	// false once Run returns).
	running          bool
	connected        bool
	connectedChanged time.Time
	errors           []string
	backoffAttempt   int
	lastSend         time.Time
	lastMaintenance  time.Time // last failed maintenance, zero = never
	// sent bookkeeping: cursor nil until the first acked batch
	// (Python _sent_cursor None).
	sentCursor *string
	sentCount  int64
	sentBytes  int64
	// buffer accounting (Python MsgBuffer): cumulative counters fed by
	// TrackEnqueue, plus the last enqueued cursor as raw JSON (null
	// until the first item).
	bufferCursor json.RawMessage
	bufferEntry  int64
	bufferBytes  int64
	// held is the not-yet-acked tail of the current drain (retry from
	// its head on the next flush); loop-goroutine only.
	held []Item
	// onDequeue, when set (buffer layer, internal/buffer), is called by
	// the loop goroutine with every non-empty batch taken out of the
	// channel — the drain side of MsgBuffer.get_items' buffer_size
	// reset. Set before Run; loop-goroutine only.
	onDequeue func([]Item)
}

// SetDequeueObserver registers a callback invoked from the loop
// goroutine with every non-empty batch of items taken out of the
// buffer channel (the buffer layer uses it to maintain its in-flight
// byte accounting). The held retry tail is not re-reported — those
// items were already dequeued. Must be called before Run.
func (l *Loop) SetDequeueObserver(fn func(items []Item)) {
	l.onDequeue = fn
}

// NewLoop wires a transport into the shared loop. in is the per-sender
// buffered channel owned by internal/buffer (Task 14); the loop reads
// len(in) as the flush trigger and must never be handed a nil channel.
func NewLoop(cfg LoopConfig, inst Instance, in <-chan Item) *Loop {
	log := cfg.Log
	if log == nil {
		log = discardLogger{}
	}
	maxBatchSize := inst.MaxBatchSize
	if maxBatchSize == 0 {
		maxBatchSize = DefaultMaxBatchSize
	}
	overhead := inst.MessageOverhead
	if overhead == 0 {
		overhead = DefaultMessageOverhead
	}
	classifier := inst.Classifier
	if classifier == nil {
		classifier = defaultClassifier{}
	}
	return &Loop{
		cfg:              cfg,
		inst:             inst,
		transport:        inst.Transport,
		classifier:       classifier,
		in:               in,
		log:              log,
		tickInterval:     TickInterval,
		maintenanceRetry: MaintenanceFailRetry,
		maxBatchSize:     maxBatchSize,
		messageOverhead:  overhead,
		running:          true, // base.py __init__: self.running = True
		connectedChanged: time.Now(),
		lastSend:         time.Now(),
	}
}

// Run drives the loop until ctx is canceled (base.py:run): every tick
// it runs the gated maintenance operation and flushes when more than
// flushThreshold items are buffered or max_send_interval elapsed. It
// always returns nil: cancellation is the normal shutdown path.
func (l *Loop) Run(ctx context.Context) error {
	l.log.Printf("Initialized sender %q (%s)", l.cfg.Name, l.inst.Type)
	ticker := time.NewTicker(l.tickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			l.setRunning(false)
			l.log.Printf("Sender %q stopping", l.cfg.Name)
			return nil
		case <-ticker.C:
			l.tick(ctx)
		}
	}
}

// tick is one base.py:run iteration: the maintenance gate, then the
// flush trigger.
func (l *Loop) tick(ctx context.Context) {
	l.maybeMaintenance(ctx)
	if len(l.held) > 0 || l.buffered() > flushThreshold || time.Since(l.lastSendTime()) > l.inst.MaxSendInterval {
		l.drain(ctx)
	}
}

// buffered is the flush trigger's queue length: the buffered channel
// length (Python len(msg_buffer)) plus the loop-held retry tail.
func (l *Loop) buffered() int {
	l.mu.Lock()
	held := len(l.held)
	l.mu.Unlock()
	return len(l.in) + held
}

// maybeMaintenance runs MaintenanceOperations unless the last failure
// is younger than the retry gate (base.py:run — "Don't run maintenance
// operations again immediately if it just failed"). A failing
// maintenance operation never takes the loop down (Python's
// deliberately blind except).
func (l *Loop) maybeMaintenance(ctx context.Context) {
	l.mu.Lock()
	last := l.lastMaintenance
	l.mu.Unlock()
	if !last.IsZero() && time.Since(last) <= l.maintenanceRetry {
		return
	}
	if err := l.transport.MaintenanceOperations(ctx); err != nil {
		l.mu.Lock()
		l.lastMaintenance = time.Now()
		l.mu.Unlock()
		l.log.Printf("Maintenance operation failed for sender %q: %v", l.cfg.Name, err)
		l.unexpected(err, "maintenance_operation", nil)
	}
}

// drain ports base.py:get_and_send_messages: collect the pending
// items, slice them into batches under the size limit, and send each
// batch. On success the sent cursor advances to the acked batch's last
// cursor (advance-on-ack only); on failure the not-yet-acked tail is
// held and retried from its head on the next flush, after a classified
// backoff. lastSend advances only when the drain completes (Python
// sets it at the end of get_and_send_messages, which on failure only
// exits at shutdown).
func (l *Loop) drain(ctx context.Context) {
	items := l.collect()
	for len(items) > 0 {
		if ctx.Err() != nil {
			return
		}
		batch, rest := sliceBatch(items, l.maxBatchSize, l.messageOverhead)
		cursor := batch[len(batch)-1].Cursor
		bodies := make([][]byte, len(batch))
		for i, item := range batch {
			bodies[i] = item.Payload
		}
		if err := l.transport.SendMessages(ctx, bodies, cursor); err != nil {
			l.failBatch(err)
			l.hold(items)
			l.backoffSleep(ctx)
			return
		}
		l.markSent(batch)
		items = rest
	}
	l.setLastSend(time.Now())
}

// failBatch classifies a failed batch and owns the mark_disconnected /
// logging / stats routing (spec §4.2): retriable errors log-and-backoff
// like kafka_sender's KAFKA_CONN_ERRORS path; permanent errors
// additionally emit stats.unexpected_exception like the generic
// except path.
func (l *Loop) failBatch(err error) {
	retriable := l.classifier.Retriable(err)
	l.MarkDisconnected(err)
	if retriable {
		l.log.Printf("Sender %q retriable error during send: %v, waiting", l.cfg.Name, err)
		return
	}
	l.log.Printf("Sender %q unexpected exception during send: %v", l.cfg.Name, err)
	l.unexpected(err, "sender", map[string]string{"app": "journalpump"})
}

// hold parks the not-yet-acked tail for the next flush's head retry.
func (l *Loop) hold(items []Item) {
	l.mu.Lock()
	l.held = items
	l.mu.Unlock()
}

// collect takes the pending items: the held retry tail if any,
// otherwise everything currently buffered (base.py:MsgBuffer.get_items
// snapshot; items enqueued mid-drain stay for the next trigger). The
// dequeue observer fires only on the channel-drain path, with the
// taken batch.
func (l *Loop) collect() []Item {
	l.mu.Lock()
	held := l.held
	l.held = nil
	l.mu.Unlock()
	if held != nil {
		return held
	}
	var items []Item
	for {
		select {
		case item := <-l.in:
			items = append(items, item)
		default:
			if len(items) > 0 && l.onDequeue != nil {
				l.onDequeue(items)
			}
			return items
		}
	}
}

// sliceBatch ports base.py:get_and_send_messages' batch slicing: the
// first item always forms the batch; the next item is added unless the
// running size plus its size would reach maxBytes (>= comparison — a
// batch never reaches the limit, and an oversized item is sent alone).
// overhead is the per-message wire overhead added to every payload
// length.
func sliceBatch(items []Item, maxBytes, overhead int) (batch, rest []Item) {
	batchSize := len(items[0].Payload) + overhead
	index := 1
	for index < len(items) {
		itemSize := len(items[index].Payload) + overhead
		if batchSize+itemSize >= maxBytes {
			break
		}
		batchSize += itemSize
		index++
	}
	return items[:index], items[index:]
}

// backoffBaseDuration ports the pre-jitter half of base.py:_backoff:
// t = min(cap, base * 2**min(100, attempt)) / 2. Attempt 1 sleeps in
// [0.5s, 1.0s); the curve doubles per attempt and saturates so the
// sleep never exceeds the 1800 s cap.
func backoffBaseDuration(attempt int) time.Duration {
	if attempt < 1 {
		// 2**0 case (unreachable through _backoff, which increments
		// first): base * 1 / 2.
		return backoffBase / 2
	}
	doubled := backoffBase
	for range min(maxBackoffExponent, attempt) - 1 {
		doubled *= 2
		if doubled >= backoffCap/2 {
			return backoffCap / 2
		}
	}
	return doubled
}

// backoffSleep waits out the jittered exponential backoff
// (base.py:_backoff: random.random() * t + t, i.e. uniform in
// [t, 2t)), cancelable through ctx.
func (l *Loop) backoffSleep(ctx context.Context) {
	l.mu.Lock()
	l.backoffAttempt++
	attempt := l.backoffAttempt
	l.mu.Unlock()
	base := backoffBaseDuration(attempt)
	delay := base + time.Duration(rand.Float64()*float64(base))
	// base.py:148-149 logs the post-jitter duration.
	l.log.Printf("Sender %q sleeping for %.0f seconds", l.cfg.Name, delay.Seconds())
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
	}
}

// TrackEnqueue records one item's admission into the per-sender buffer
// — the accounting half of base.py:MsgBuffer.add_item (cumulative
// entry count and payload bytes, last cursor). The buffer layer (Task
// 14) calls it on every successful enqueue; the items themselves
// travel through the channel.
func (l *Loop) TrackEnqueue(item Item) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.bufferEntry++
	l.bufferBytes += int64(len(item.Payload))
	l.bufferCursor = rawString(item.Cursor)
}

// MarkConnected ports base.py:mark_connected: connected state with
// transition timestamp, cleared error ring, reset backoff.
func (l *Loop) MarkConnected() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.connected {
		l.connectedChanged = time.Now()
	}
	l.connected = true
	l.errors = nil
	l.backoffAttempt = 0
}

// MarkDisconnected ports base.py:mark_disconnected: disconnected state
// with transition timestamp, plus the error into the health ring
// (truncated to MaxErrorMessageLen characters, deduped, at most
// MaxErrorMessages entries with the FIRST one always kept — Python
// pops index 1).
func (l *Loop) MarkDisconnected(err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.connected {
		l.connectedChanged = time.Now()
	}
	l.connected = false
	if err == nil {
		return
	}
	msg := err.Error()
	if msg == "" {
		return
	}
	// Python truncates str[:128] — 128 codepoints, not bytes.
	if runes := []rune(msg); len(runes) > MaxErrorMessageLen {
		msg = string(runes[:MaxErrorMessageLen])
	}
	if slices.Contains(l.errors, msg) {
		return
	}
	l.errors = append(l.errors, msg)
	if len(l.errors) > MaxErrorMessages {
		l.errors = slices.Delete(l.errors, 1, 2) // always keep the first error
	}
}

// markSent ports base.py:mark_sent: sent counters advance and the
// cursor moves to the acked batch's LAST cursor; a successful send
// connects the sender.
func (l *Loop) markSent(batch []Item) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.sentCount += int64(len(batch))
	for _, item := range batch {
		l.sentBytes += int64(len(item.Payload))
	}
	cursor := batch[len(batch)-1].Cursor
	l.sentCursor = &cursor
	if !l.connected {
		l.connectedChanged = time.Now()
	}
	l.connected = true
	l.errors = nil
	l.backoffAttempt = 0
}

// setLastSend advances the idle-flush reference (Python updates
// last_send_time at the end of get_and_send_messages).
func (l *Loop) setLastSend(now time.Time) {
	l.mu.Lock()
	l.lastSend = now
	l.mu.Unlock()
}

func (l *Loop) lastSendTime() time.Time {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lastSend
}

func (l *Loop) setRunning(running bool) {
	l.mu.Lock()
	l.running = running
	l.mu.Unlock()
}

// unexpected emits stats.unexpected_exception with the sender's base
// tags merged under extra (which overrides), when stats are
// configured. The extra tag set mirrors the Python call sites: the
// send path passes app=journalpump (e.g. kafka_sender.py:186); the
// maintenance-failure site passes none (base.py:245).
func (l *Loop) unexpected(err error, where string, extra map[string]string) {
	if l.cfg.Stats == nil {
		return
	}
	tags := make(map[string]string, len(l.cfg.Tags)+len(extra))
	for k, v := range l.cfg.Tags {
		tags[k] = v
	}
	for k, v := range extra {
		tags[k] = v
	}
	l.cfg.Stats.UnexpectedException(err, where, tags)
}

// State builds the state-file value for one sender
// (base.py:get_state → internal/state.SenderState).
func (l *Loop) State() state.SenderState {
	l.mu.Lock()
	defer l.mu.Unlock()
	running, connected := l.running, l.connected
	status := health(running, connected)
	return state.SenderState{
		Type: l.inst.Type,
		Buffer: state.BufferState{
			Cursor:       l.bufferCursor,
			TotalSize:    l.bufferBytes,
			EntryNum:     l.bufferEntry,
			CurrentQueue: int64(len(l.in)),
		},
		Sent: state.SentState{
			Cursor: l.sentCursor,
			Count:  l.sentCount,
			Bytes:  l.sentBytes,
		},
		Health: state.HealthState{
			Status:  status.name,
			Elapsed: time.Since(l.connectedChanged).Seconds(),
			Errors:  slices.Clone(l.errors),
		},
	}
}

// RefreshStats ports base.py:refresh_stats: the four per-sender
// gauges. The reader's stats path drives it (Python calls it from
// inc_line_stats behind the 10 s throttle).
func (l *Loop) RefreshStats() {
	if l.cfg.Stats == nil {
		return
	}
	l.mu.Lock()
	lastSend, sentBytes, sentCount := l.lastSend, l.sentBytes, l.sentCount
	running, connected := l.running, l.connected
	l.mu.Unlock()
	tags := l.cfg.Tags
	l.cfg.Stats.Gauge("journal.last_sent_ago", time.Since(lastSend).Seconds(), tags)
	l.cfg.Stats.GaugeInt("journal.sent_bytes", sentBytes, tags)
	l.cfg.Stats.GaugeInt("journal.sent_lines", sentCount, tags)
	l.cfg.Stats.GaugeInt("journal.status", int64(health(running, connected).gauge), tags)
}

// rawString renders a cursor string as raw JSON bytes for the
// state-file BufferState (Python writes the cursor as a JSON string).
func rawString(s string) json.RawMessage {
	b, _ := json.Marshal(s)
	return b
}
