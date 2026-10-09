// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package sender

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Aiven-Open/journalpump/internal/statsd"
)

// fakeTransport records every call and routes behavior through
// configurable per-attempt functions (nil = always succeed).
type fakeTransport struct {
	mu            sync.Mutex
	sendBehavior  func(attempt int) error
	maintBehavior func(attempt int) error
	sendCalls     int
	maintCalls    int
	calls         []fakeCall
}

type fakeCall struct {
	batch  [][]byte
	cursor string
	at     time.Time
}

func (f *fakeTransport) SendMessages(_ context.Context, batch [][]byte, cursor string) error {
	f.mu.Lock()
	f.sendCalls++
	attempt := f.sendCalls
	f.calls = append(f.calls, fakeCall{batch: batch, cursor: cursor, at: time.Now()})
	behavior := f.sendBehavior
	f.mu.Unlock()
	if behavior == nil {
		return nil
	}
	return behavior(attempt)
}

func (f *fakeTransport) MaintenanceOperations(_ context.Context) error {
	f.mu.Lock()
	f.maintCalls++
	attempt := f.maintCalls
	behavior := f.maintBehavior
	f.mu.Unlock()
	if behavior == nil {
		return nil
	}
	return behavior(attempt)
}

func (f *fakeTransport) attemptCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sendCalls
}

func (f *fakeTransport) maintenanceCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.maintCalls
}

func (f *fakeTransport) lastCall() fakeCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[len(f.calls)-1]
}

func (f *fakeTransport) callAt(index int) fakeCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[index]
}

// newTestLoop builds a loop around a fake transport with the given
// idle-flush interval and optional classifier; stats are disabled
// (nil client would panic — tests that need stats pass them).
func newTestLoop(tr *fakeTransport, classifier Classifier, in chan Item, interval time.Duration) *Loop {
	return NewLoop(
		LoopConfig{Name: "out", Tags: map[string]string{"sender": "out"}},
		Instance{
			Transport:       tr,
			Classifier:      classifier,
			MaxSendInterval: interval,
			Type:            "FakeSender",
		},
		in,
	)
}

// startLoop runs the loop in a goroutine; the returned channel closes
// when Run returns.
func startLoop(ctx context.Context, loop *Loop) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = loop.Run(ctx)
	}()
	return done
}

func items(n int) []Item {
	out := make([]Item, n)
	for i := range out {
		out[i] = Item{Payload: []byte("x"), Cursor: fmt.Sprintf("c%d", i)}
	}
	return out
}

func TestSliceBatch(t *testing.T) {
	t.Parallel()
	mk := func(sizes ...int) []Item {
		out := make([]Item, len(sizes))
		for i, size := range sizes {
			out[i] = Item{Payload: make([]byte, size), Cursor: fmt.Sprintf("c%d", i)}
		}
		return out
	}
	tests := []struct {
		name         string
		items        []Item
		max          int
		overhead     int
		wantBatchLen int
		wantRestLen  int
		wantCursor   string
	}{
		{
			name:         "single item forms a batch even when oversized",
			items:        mk(2000),
			max:          100,
			overhead:     30,
			wantBatchLen: 1,
			wantRestLen:  0,
			wantCursor:   "c0",
		},
		{
			name:         "second item reaching the limit breaks the batch",
			items:        mk(30, 10),
			max:          100,
			overhead:     30,
			wantBatchLen: 1,
			wantRestLen:  1,
			wantCursor:   "c0",
		},
		{
			name:         "second item fitting under the limit joins",
			items:        mk(30, 0),
			max:          100,
			overhead:     30,
			wantBatchLen: 2,
			wantRestLen:  0,
			wantCursor:   "c1",
		},
		{
			name:         "websocket 1-byte overhead override",
			items:        mk(50, 47, 0),
			max:          100,
			overhead:     1,
			wantBatchLen: 2,
			wantRestLen:  1,
			wantCursor:   "c1",
		},
		{
			name:         "zero-overhead exact boundary",
			items:        mk(100, 1),
			max:          100,
			overhead:     0,
			wantBatchLen: 1,
			wantRestLen:  1,
			wantCursor:   "c0",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			batch, rest := sliceBatch(test.items, test.max, test.overhead)
			if len(batch) != test.wantBatchLen || len(rest) != test.wantRestLen {
				t.Fatalf("sliceBatch(%d items, max=%d, overhead=%d) = batch %d + rest %d, want %d + %d",
					len(test.items), test.max, test.overhead, len(batch), len(rest), test.wantBatchLen, test.wantRestLen)
			}
			if got := batch[len(batch)-1].Cursor; got != test.wantCursor {
				t.Fatalf("batch last cursor = %q, want %q", got, test.wantCursor)
			}
		})
	}
}

// levelLogger records lines per severity so tests can assert the routing
// of Python's log.error / log.warning call sites.
type levelLogger struct {
	mu       sync.Mutex
	errors   []string
	warnings []string
}

func (*levelLogger) Printf(string, ...any) {}

func (l *levelLogger) Errorf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.errors = append(l.errors, fmt.Sprintf(format, args...))
}

func (l *levelLogger) Warningf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.warnings = append(l.warnings, fmt.Sprintf(format, args...))
}

func (l *levelLogger) hasError(substr string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, line := range l.errors {
		if strings.Contains(line, substr) {
			return true
		}
	}
	return false
}

// TestSendFailureLoggedAsError locks the severity routing of a REAL send
// failure: base.py:279 logs "Problem sending ... messages" at ERROR, so
// the loop's failBatch must emit through Errorf (not the INFO Printf).
func TestSendFailureLoggedAsError(t *testing.T) {
	t.Parallel()
	tr := &fakeTransport{sendBehavior: func(int) error { return errors.New("send boom") }}
	log := &levelLogger{}
	in := make(chan Item, 4)
	loop := NewLoop(
		LoopConfig{Name: "out", Tags: map[string]string{}, Log: log},
		Instance{Transport: tr, MaxSendInterval: time.Hour, Type: "FakeSender"},
		in,
	)
	in <- Item{Payload: []byte("x"), Cursor: "c0"}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		loop.drain(ctx)
	}()
	deadline := time.Now().Add(2 * time.Second)
	for tr.attemptCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel() // release the backoff sleep
	<-done

	if !log.hasError("during send") {
		t.Errorf("send failure not logged at ERROR; errors = %v", log.errors)
	}
}

func TestBackoffBaseDuration(t *testing.T) {
	t.Parallel()
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: 0, want: 250 * time.Millisecond}, // 2**0 case: base/2
		{attempt: 1, want: 500 * time.Millisecond},
		{attempt: 2, want: 1 * time.Second},
		{attempt: 3, want: 2 * time.Second},
		{attempt: 11, want: 512 * time.Second},
		{attempt: 12, want: 900 * time.Second}, // saturated: cap/2
		{attempt: 13, want: 900 * time.Second},
		{attempt: 100, want: 900 * time.Second},
		{attempt: 200, want: 900 * time.Second}, // exponent clamped at 100
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("attempt %d", test.attempt), func(t *testing.T) {
			t.Parallel()
			if got := backoffBaseDuration(test.attempt); got != test.want {
				t.Fatalf("backoffBaseDuration(%d) = %s, want %s", test.attempt, got, test.want)
			}
		})
	}
}

func TestFlushTriggers(t *testing.T) {
	t.Parallel()

	t.Run("count trigger strictly above 1000 buffered", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			tr := &fakeTransport{}
			in := make(chan Item, 2000)
			loop := newTestLoop(tr, nil, in, time.Hour) // idle trigger disabled
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := startLoop(ctx, loop)

			for _, item := range items(1000) {
				in <- item
			}
			time.Sleep(300 * time.Millisecond) // several ticks
			synctest.Wait()
			if got := tr.attemptCount(); got != 0 {
				t.Fatalf("sent %d times with exactly 1000 buffered, want 0", got)
			}

			in <- Item{Payload: []byte("x"), Cursor: "c1000"}
			time.Sleep(200 * time.Millisecond) // the next tick flushes
			synctest.Wait()
			if got := tr.attemptCount(); got != 1 {
				t.Fatalf("sent %d times after crossing the threshold, want 1", got)
			}
			call := tr.lastCall()
			if len(call.batch) != 1001 {
				t.Fatalf("batch size = %d, want 1001", len(call.batch))
			}
			if call.cursor != "c1000" {
				t.Fatalf("batch cursor = %q, want c1000 (the last item's)", call.cursor)
			}
			cancel()
			<-done
		})
	})

	t.Run("idle trigger after max_send_interval", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			tr := &fakeTransport{}
			in := make(chan Item, 10)
			loop := newTestLoop(tr, nil, in, 300*time.Millisecond)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := startLoop(ctx, loop)

			in <- Item{Payload: []byte("x"), Cursor: "c0"}
			time.Sleep(200 * time.Millisecond)
			synctest.Wait()
			if got := tr.attemptCount(); got != 0 {
				t.Fatalf("sent before max_send_interval elapsed, want 0, got %d", got)
			}
			time.Sleep(200 * time.Millisecond) // 400ms total > 300ms
			synctest.Wait()
			if got := tr.attemptCount(); got != 1 {
				t.Fatalf("sent %d times after idle interval, want 1", got)
			}
			cancel()
			<-done
		})
	})

	t.Run("held retry tail triggers the next flush", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			tr := &fakeTransport{sendBehavior: func(attempt int) error {
				if attempt == 1 {
					return errors.New("retriable")
				}
				return nil
			}}
			in := make(chan Item, 10)
			loop := newTestLoop(tr, nil, in, 50*time.Millisecond)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := startLoop(ctx, loop)

			in <- Item{Payload: []byte("x"), Cursor: "c0"}
			time.Sleep(150 * time.Millisecond) // attempt 1 fails
			synctest.Wait()
			if got := tr.attemptCount(); got != 1 {
				t.Fatalf("sent %d times before the first flush, want 1", got)
			}
			// The backoff (≥0.5s) passes with NO new items arriving;
			// the held tail alone triggers the retry.
			time.Sleep(1400 * time.Millisecond)
			synctest.Wait()
			if got := tr.attemptCount(); got != 2 {
				t.Fatalf("sent %d times, want 2 (failure + held retry)", got)
			}
			if got := tr.lastCall().cursor; got != "c0" {
				t.Fatalf("retry cursor = %q, want c0 (same batch from its head)", got)
			}
			cancel()
			<-done
		})
	})
}

func TestBackoffJitterBounds(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		tr := &fakeTransport{sendBehavior: func(int) error { return errors.New("boom") }}
		in := make(chan Item, 1)
		loop := newTestLoop(tr, nil, in, 50*time.Millisecond)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := startLoop(ctx, loop)

		in <- Item{Payload: []byte("x"), Cursor: "c0"}
		time.Sleep(150 * time.Millisecond) // first attempt on a tick
		synctest.Wait()
		if got := tr.attemptCount(); got != 1 {
			t.Fatalf("sent %d times before the first flush, want 1", got)
		}

		// attempt 1 sleeps in [0.5s, 1.0s); the retry follows on the
		// next tick (≤100ms later). attempt 3 needs attempt 2's own
		// backoff (≥1s) on top, so at +1400ms exactly attempt 2 ran.
		time.Sleep(1400 * time.Millisecond)
		synctest.Wait()
		if got := tr.attemptCount(); got != 2 {
			t.Fatalf("sent %d times after the backoff window, want 2", got)
		}
		gap := tr.callAt(1).at.Sub(tr.callAt(0).at)
		if gap < 500*time.Millisecond || gap >= 1100*time.Millisecond {
			t.Fatalf("first retry gap = %s, want within [500ms, 1100ms)", gap)
		}
		cancel()
		<-done
	})
}

func TestHealthRing(t *testing.T) {
	t.Parallel()
	newLoop := func() *Loop {
		return newTestLoop(&fakeTransport{}, nil, make(chan Item, 1), time.Minute)
	}

	t.Run("dedup keeps one entry per distinct message", func(t *testing.T) {
		t.Parallel()
		loop := newLoop()
		loop.MarkDisconnected(errors.New("boom"))
		loop.MarkDisconnected(errors.New("boom")) // same message, no second entry
		loop.MarkDisconnected(errors.New("other"))
		state := loop.State()
		if got := strings.Join(state.Health.Errors, ","); got != "boom,other" {
			t.Fatalf("errors = %q, want boom,other", got)
		}
	})

	t.Run("messages truncated to 128 characters", func(t *testing.T) {
		t.Parallel()
		loop := newLoop()
		loop.MarkDisconnected(errors.New(strings.Repeat("x", 200)))
		if got := loop.State().Health.Errors[0]; len(got) != 128 {
			t.Fatalf("truncated error length = %d, want 128", len(got))
		}
	})

	t.Run("truncation counts codepoints not bytes", func(t *testing.T) {
		t.Parallel()
		loop := newLoop()
		// 100 'é' codepoints = 200 UTF-8 bytes: NOT truncated.
		multibyte := strings.Repeat("é", 100)
		loop.MarkDisconnected(errors.New(multibyte))
		if got := loop.State().Health.Errors[0]; got != multibyte {
			t.Fatalf("multibyte error was truncated: %d runes", len([]rune(got)))
		}
		// 200 codepoints → exactly 128 kept.
		long := strings.Repeat("é", 200)
		loop.MarkDisconnected(errors.New(long))
		errors := loop.State().Health.Errors
		if len(errors) != 2 || len([]rune(errors[1])) != 128 {
			t.Fatalf("long error not truncated to 128 codepoints")
		}
	})

	t.Run("overflow keeps the first error and the newest seven", func(t *testing.T) {
		t.Parallel()
		loop := newLoop()
		for i := range 10 {
			loop.MarkDisconnected(fmt.Errorf("e%d", i))
		}
		want := []string{"e0", "e3", "e4", "e5", "e6", "e7", "e8", "e9"}
		if got := loop.State().Health.Errors; strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("errors = %q, want %q", got, want)
		}
	})

	t.Run("nil and empty errors only flip the state", func(t *testing.T) {
		t.Parallel()
		loop := newLoop()
		loop.MarkDisconnected(nil)
		loop.MarkDisconnected(errors.New(""))
		if got := loop.State().Health.Errors; len(got) != 0 {
			t.Fatalf("errors = %q, want empty", got)
		}
		if got := loop.State().Health.Status; got != "disconnected" {
			t.Fatalf("status = %q, want disconnected", got)
		}
	})

	t.Run("mark connected clears the ring and resets backoff", func(t *testing.T) {
		t.Parallel()
		loop := newLoop()
		loop.MarkDisconnected(errors.New("boom"))
		loop.MarkConnected()
		state := loop.State()
		if len(state.Health.Errors) != 0 || state.Health.Status != "connected" {
			t.Fatalf("state after reconnect = %+v", state.Health)
		}
	})
}

func TestHealthStatusMapping(t *testing.T) {
	t.Parallel()
	tests := []struct {
		running   bool
		connected bool
		name      string
		gauge     int
	}{
		{running: true, connected: true, name: "connected", gauge: 3},
		{running: true, connected: false, name: "disconnected", gauge: 2},
		{running: false, connected: true, name: "stale", gauge: 2},
		{running: false, connected: false, name: "stopped", gauge: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got := health(test.running, test.connected)
			if got.name != test.name || got.gauge != test.gauge {
				t.Fatalf("health(%v, %v) = %q/%d, want %q/%d",
					test.running, test.connected, got.name, got.gauge, test.name, test.gauge)
			}
		})
	}
}

func TestClassifierRouting(t *testing.T) {
	t.Parallel()

	t.Run("default classifier routes InitializationError permanent", func(t *testing.T) {
		t.Parallel()
		if !(defaultClassifier{}).Retriable(errors.New("anything")) {
			t.Fatal("generic error must be retriable by default")
		}
		if (defaultClassifier{}).Retriable(&InitializationError{Err: errors.New("denied")}) {
			t.Fatal("InitializationError must be permanent by default")
		}
		if (defaultClassifier{}).Retriable(fmt.Errorf("wrapped: %w", &InitializationError{Err: errors.New("denied")})) {
			t.Fatal("InitializationError must be permanent through wrapping (errors.As)")
		}
	})

	t.Run("permanent send errors emit unexpected_exception", func(t *testing.T) {
		t.Parallel()
		capture := newUDPCapture(t)
		client := newStatsClient(capture)
		tr := &fakeTransport{sendBehavior: func(int) error { return errors.New("permanent") }}
		in := make(chan Item, 1)
		loop := NewLoop(
			LoopConfig{Name: "out", Stats: client, Tags: map[string]string{"sender": "out"}},
			Instance{
				Transport:       tr,
				Classifier:      ClassifierFunc(func(error) bool { return false }), // everything permanent
				MaxSendInterval: 10 * time.Millisecond,
				Type:            "FakeSender",
			},
			in,
		)
		loop.tickInterval = 5 * time.Millisecond
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := startLoop(ctx, loop)
		in <- Item{Payload: []byte("x"), Cursor: "c0"}
		time.Sleep(50 * time.Millisecond)
		cancel()
		<-done
		datagram := capture.datagram()
		if !strings.Contains(datagram, "exception") || !strings.Contains(datagram, "where=sender") ||
			!strings.Contains(datagram, "app=journalpump") {
			t.Fatalf("datagram = %q, want the unexpected_exception counter with where=sender and app=journalpump", datagram)
		}
	})

	t.Run("retriable send errors do not emit unexpected_exception", func(t *testing.T) {
		t.Parallel()
		capture := newUDPCapture(t)
		client := newStatsClient(capture)
		tr := &fakeTransport{sendBehavior: func(int) error { return errors.New("retriable") }}
		in := make(chan Item, 1)
		loop := NewLoop(
			LoopConfig{Name: "out", Stats: client, Tags: map[string]string{"sender": "out"}},
			Instance{
				Transport:       tr,
				Classifier:      ClassifierFunc(func(error) bool { return true }),
				MaxSendInterval: time.Hour,
				Type:            "FakeSender",
			},
			in,
		)
		loop.tickInterval = 5 * time.Millisecond
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := startLoop(ctx, loop)
		in <- Item{Payload: []byte("x"), Cursor: "c0"}
		time.Sleep(50 * time.Millisecond)
		cancel()
		<-done
		capture.assertNoDatagram()
	})
}

func TestCursorAdvanceOnAckOnly(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		tr := &fakeTransport{sendBehavior: func(attempt int) error {
			if attempt == 1 {
				return errors.New("first attempt fails")
			}
			return nil
		}}
		in := make(chan Item, 10)
		loop := NewLoop(
			LoopConfig{Name: "out"},
			Instance{
				Transport:       tr,
				MaxSendInterval: 50 * time.Millisecond,
				// one item per batch: payload+overhead reaches the limit
				MaxBatchSize:    1 + DefaultMessageOverhead,
				MessageOverhead: DefaultMessageOverhead,
				Type:            "FakeSender",
			},
			in,
		)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := startLoop(ctx, loop)

		for _, item := range []Item{
			{Payload: []byte("a"), Cursor: "c1"},
			{Payload: []byte("b"), Cursor: "c2"},
			{Payload: []byte("c"), Cursor: "c3"},
		} {
			loop.TrackEnqueue(item)
			in <- item
		}

		// After the failed first batch nothing is acked: no sent
		// cursor, no count.
		time.Sleep(150 * time.Millisecond)
		synctest.Wait()
		if got := tr.attemptCount(); got != 1 {
			t.Fatalf("sent %d times, want 1", got)
		}
		if state := loop.State(); state.Sent.Cursor != nil || state.Sent.Count != 0 {
			t.Fatalf("sent state advanced on a failed batch: %+v", state.Sent)
		}

		// The retries ack every batch in order; the sent cursor lands
		// on the LAST acked batch's cursor.
		time.Sleep(1 * time.Second)
		synctest.Wait()
		if got := tr.attemptCount(); got != 4 {
			t.Fatalf("sent %d times, want 4 (fail + 3 batches)", got)
		}
		state := loop.State()
		if state.Sent.Cursor == nil || *state.Sent.Cursor != "c3" {
			t.Fatalf("sent cursor = %v, want c3", state.Sent.Cursor)
		}
		if state.Sent.Count != 3 || state.Sent.Bytes != 3 {
			t.Fatalf("sent count/bytes = %d/%d, want 3/3", state.Sent.Count, state.Sent.Bytes)
		}
		// Health is connected after the successful sends.
		if state.Health.Status != "connected" {
			t.Fatalf("status = %q, want connected", state.Health.Status)
		}
		cancel()
		<-done
	})
}

// durableFakeTransport mimics the file sender's delivery checkpoint at
// the loop layer: it acks batches (logical send() completion), reports a
// controllable flushed watermark, and exposes the logical payload total.
type durableFakeTransport struct {
	mu      sync.Mutex
	logical int64
	flushed int64
}

func (d *durableFakeTransport) SendMessages(_ context.Context, batch [][]byte, _ string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, m := range batch {
		d.logical += int64(len(m))
	}
	return nil
}

func (d *durableFakeTransport) MaintenanceOperations(context.Context) error { return nil }

func (d *durableFakeTransport) FlushedPayloadBytes() int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.flushed
}

func (d *durableFakeTransport) setFlushed(n int64) {
	d.mu.Lock()
	d.flushed = n
	d.mu.Unlock()
}

// TestDurableCursorTracksFlushedWatermark locks the delivery-checkpoint
// mapping (Task 17): for a DurableReporter transport, the sender's
// reported sent cursor is the cursor of the last acked item whose payload
// bytes have actually flushed, lagging the logical mark_sent position
// until the bytes reach the sink.
func TestDurableCursorTracksFlushedWatermark(t *testing.T) {
	t.Parallel()
	tr := &durableFakeTransport{}
	loop := NewLoop(
		LoopConfig{Name: "out"},
		Instance{Transport: tr, MaxSendInterval: time.Hour, Type: "DurableSender"},
		make(chan Item, 8),
	)

	// "a" then "bb": logical payload ends are 1 and 3.
	loop.markSent([]Item{
		{Payload: []byte("a"), Cursor: "c0"},
		{Payload: []byte("bb"), Cursor: "c1"},
	})

	// Nothing flushed: no durable cursor yet (no resume baseline either),
	// even though mark_sent already moved the logical cursor to c1.
	if cur := loop.State().Sent.Cursor; cur != nil {
		t.Fatalf("durable cursor = %v, want nil before any flush", *cur)
	}

	// The first line's bytes are durable, the second's are not: the cursor
	// stops at c0 rather than skipping ahead to the logical c1.
	tr.setFlushed(1)
	if cur := loop.State().Sent.Cursor; cur == nil || *cur != "c0" {
		t.Fatalf("durable cursor = %v, want c0", cur)
	}

	// The full payload is durable: the cursor catches up to c1.
	tr.setFlushed(3)
	if cur := loop.State().Sent.Cursor; cur == nil || *cur != "c1" {
		t.Fatalf("durable cursor = %v, want c1", cur)
	}
}

// TestDurableCursorNeverRegressesBelowBaseline locks the resume-cursor
// baseline: before ANY new item is durable, the persisted cursor stays at
// the position the run resumed from (never nil/older), so a save during
// the unflushed window cannot force a full resync on restart.
func TestDurableCursorNeverRegressesBelowBaseline(t *testing.T) {
	t.Parallel()
	tr := &durableFakeTransport{}
	loop := NewLoop(
		LoopConfig{Name: "out"},
		Instance{Transport: tr, MaxSendInterval: time.Hour, Type: "DurableSender"},
		make(chan Item, 8),
	)
	baseline := "resume"
	loop.SetResumeCursor(&baseline)

	loop.markSent([]Item{{Payload: []byte("a"), Cursor: "c0"}})
	if cur := loop.State().Sent.Cursor; cur == nil || *cur != "resume" {
		t.Fatalf("durable cursor = %v, want resume (baseline)", cur)
	}

	// Once the new item is durable the cursor advances past the baseline.
	tr.setFlushed(1)
	if cur := loop.State().Sent.Cursor; cur == nil || *cur != "c0" {
		t.Fatalf("durable cursor = %v, want c0", cur)
	}
}

func TestLoopState(t *testing.T) {
	t.Parallel()

	t.Run("initial state mirrors a fresh Python sender", func(t *testing.T) {
		t.Parallel()
		loop := newTestLoop(&fakeTransport{}, nil, make(chan Item, 4), time.Minute)
		state := loop.State()
		if state.Type != "FakeSender" {
			t.Fatalf("type = %q, want FakeSender", state.Type)
		}
		if state.Buffer.Cursor != nil {
			t.Fatalf("initial buffer cursor = %s, want null", state.Buffer.Cursor)
		}
		if state.Buffer.TotalSize != 0 || state.Buffer.EntryNum != 0 || state.Buffer.CurrentQueue != 0 {
			t.Fatalf("initial buffer = %+v", state.Buffer)
		}
		if state.Sent.Cursor != nil || state.Sent.Count != 0 || state.Sent.Bytes != 0 {
			t.Fatalf("initial sent = %+v", state.Sent)
		}
		if state.Health.Status != "disconnected" {
			t.Fatalf("initial status = %q, want disconnected", state.Health.Status)
		}
	})

	t.Run("buffer accounting on enqueue", func(t *testing.T) {
		t.Parallel()
		in := make(chan Item, 4)
		loop := newTestLoop(&fakeTransport{}, nil, in, time.Minute)
		// The buffer layer (Task 14) enqueues the item into the channel
		// and reports it via TrackEnqueue — mirror both halves.
		for _, item := range []Item{
			{Payload: []byte("12345"), Cursor: "c1"},
			{Payload: []byte("678"), Cursor: "c2"},
		} {
			loop.TrackEnqueue(item)
			in <- item
		}
		state := loop.State()
		if state.Buffer.EntryNum != 2 || state.Buffer.TotalSize != 8 {
			t.Fatalf("buffer counters = %+v, want entry 2 bytes 8", state.Buffer)
		}
		if state.Buffer.Cursor == nil || string(state.Buffer.Cursor) != `"c2"` {
			t.Fatalf("buffer cursor = %s, want the last enqueued cursor c2", state.Buffer.Cursor)
		}
		if state.Buffer.CurrentQueue != 2 {
			t.Fatalf("current queue = %d, want 2 (the buffered channel length)", state.Buffer.CurrentQueue)
		}
	})

	t.Run("running flag flips to stale on shutdown", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			loop := newTestLoop(&fakeTransport{}, nil, make(chan Item, 1), time.Minute)
			loop.MarkConnected() // connected while running
			ctx, cancel := context.WithCancel(context.Background())
			done := startLoop(ctx, loop)
			cancel()
			<-done
			if got := loop.State().Health.Status; got != "stale" {
				t.Fatalf("status after shutdown = %q, want stale (connected but not running)", got)
			}
		})
	})
}

func TestMaintenanceGate(t *testing.T) {
	t.Parallel()

	t.Run("failed maintenance is retried only after the gate", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			tr := &fakeTransport{maintBehavior: func(attempt int) error {
				if attempt == 1 {
					return errors.New("maintenance failed")
				}
				return nil
			}}
			loop := newTestLoop(tr, nil, make(chan Item, 1), time.Hour)
			loop.tickInterval = 10 * time.Millisecond
			loop.maintenanceRetry = 60 * time.Millisecond
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := startLoop(ctx, loop)

			time.Sleep(30 * time.Millisecond) // tick 1: fails; ticks 2-3 gated
			synctest.Wait()
			if got := tr.maintenanceCount(); got != 1 {
				t.Fatalf("maintenance called %d times within the gate, want 1", got)
			}
			time.Sleep(80 * time.Millisecond) // past the 60ms gate
			synctest.Wait()
			if got := tr.maintenanceCount(); got < 2 {
				t.Fatalf("maintenance called %d times after the gate, want ≥2", got)
			}
			cancel()
			<-done
		})
	})

	t.Run("successful maintenance runs every tick", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			tr := &fakeTransport{}
			loop := newTestLoop(tr, nil, make(chan Item, 1), time.Hour)
			loop.tickInterval = 10 * time.Millisecond
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := startLoop(ctx, loop)
			time.Sleep(100 * time.Millisecond)
			synctest.Wait()
			if got := tr.maintenanceCount(); got < 5 {
				t.Fatalf("maintenance called %d times in 100ms of 10ms ticks, want ≥5", got)
			}
			cancel()
			<-done
		})
	})

	t.Run("maintenance failure counter carries no app tag", func(t *testing.T) {
		// Real UDP I/O, like the classifier routing tests — not inside
		// the synctest bubble.
		t.Parallel()
		capture := newUDPCapture(t)
		client := newStatsClient(capture)
		tr := &fakeTransport{maintBehavior: func(int) error { return errors.New("maintenance failed") }}
		in := make(chan Item, 1)
		loop := NewLoop(
			LoopConfig{Name: "out", Stats: client, Tags: map[string]string{"sender": "out"}},
			Instance{Transport: tr, MaxSendInterval: time.Hour, Type: "FakeSender"},
			in,
		)
		loop.tickInterval = 10 * time.Millisecond
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := startLoop(ctx, loop)
		time.Sleep(30 * time.Millisecond)
		cancel()
		<-done
		datagram := capture.datagram()
		if !strings.Contains(datagram, "where=maintenance_operation") {
			t.Fatalf("datagram = %q, want where=maintenance_operation", datagram)
		}
		if strings.Contains(datagram, "app=journalpump") {
			t.Fatalf("datagram = %q, want no app tag on the maintenance route (base.py:245)", datagram)
		}
	})
}

// udpCapture binds a loopback UDP socket the client sends into.
type udpCapture struct {
	t    *testing.T
	conn *net.UDPConn
	mu   sync.Mutex
	seen []string
}

func newUDPCapture(t *testing.T) *udpCapture {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("ListenUDP: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	c := &udpCapture{t: t, conn: conn}
	go func() {
		buf := make([]byte, 65535)
		for {
			n, _, err := conn.ReadFromUDP(buf)
			if err != nil {
				return
			}
			c.mu.Lock()
			c.seen = append(c.seen, string(buf[:n]))
			c.mu.Unlock()
		}
	}()
	return c
}

func (c *udpCapture) addr() (string, *int64) {
	port := int64(c.conn.LocalAddr().(*net.UDPAddr).Port)
	return "127.0.0.1", &port
}

func (c *udpCapture) datagram() string {
	c.t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		if len(c.seen) > 0 {
			defer c.mu.Unlock()
			return c.seen[0]
		}
		c.mu.Unlock()
		time.Sleep(time.Millisecond)
	}
	c.t.Fatal("no datagram arrived within 2s")
	return ""
}

// datagrams waits for n datagrams and returns them in arrival order.
func (c *udpCapture) datagrams(n int) []string {
	c.t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		enough := len(c.seen) >= n
		var out []string
		if enough {
			out = append([]string(nil), c.seen[:n]...)
		}
		c.mu.Unlock()
		if enough {
			return out
		}
		time.Sleep(time.Millisecond)
	}
	c.t.Fatalf("only %d of %d datagrams arrived within 2s", len(c.seen), n)
	return nil
}

func (c *udpCapture) assertNoDatagram() {
	c.t.Helper()
	time.Sleep(50 * time.Millisecond)
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.seen) != 0 {
		c.t.Fatalf("expected no datagrams, got %q", c.seen)
	}
}

// newStatsClient builds a disabled-until-needed statsd client pointed
// at the capture socket.
func newStatsClient(capture *udpCapture) *statsd.Client {
	host, port := capture.addr()
	return statsd.New(&host, port, nil, nil, "", nil)
}
