// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package buffer_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Aiven-Open/journalpump/internal/buffer"
	"github.com/Aiven-Open/journalpump/internal/sender"
)

// fakeTransport optionally blocks in SendMessages until its gate is
// closed, so tests control exactly when the loop drains.
type fakeTransport struct {
	mu       sync.Mutex
	calls    int
	messages int
	gate     chan struct{}
}

func (f *fakeTransport) SendMessages(_ context.Context, batch [][]byte, _ string) error {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	if f.gate != nil {
		<-f.gate
	}
	// Count only COMPLETED sends: increment after the optional gate
	// releases, so messageCount really means delivered.
	f.mu.Lock()
	f.messages += len(batch)
	f.mu.Unlock()
	return nil
}

func (f *fakeTransport) MaintenanceOperations(_ context.Context) error { return nil }

func (f *fakeTransport) release() { close(f.gate) }

func (f *fakeTransport) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// messageCount is the number of messages whose SendMessages COMPLETED —
// the delivery acknowledgment the drain assertions must wait on.
func (f *fakeTransport) messageCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.messages
}

// newWired builds the composed pair in the production order:
// buffer → loop consuming the buffer's channel → Attach. interval is
// the loop's max_send_interval (idle-flush trigger).
func newWired(tr *fakeTransport, maxLength int, interval time.Duration) (*sender.Loop, *buffer.SenderBuffer) {
	buf := buffer.NewSenderBuffer("out", maxLength)
	loop := sender.NewLoop(
		sender.LoopConfig{Name: "out"},
		sender.Instance{Transport: tr, MaxSendInterval: interval, Type: "FakeSender"},
		buf.Chan(),
	)
	buf.Attach(loop)
	return loop, buf
}

func item(n int) sender.Item {
	return sender.Item{Payload: []byte("payload-" + strconv.Itoa(n)), Cursor: "c" + strconv.Itoa(n)}
}

func TestEnqueueAccounting(t *testing.T) {
	t.Parallel()
	tr := &fakeTransport{}
	loop, buf := newWired(tr, buffer.DefaultMsgBufferMaxLength, time.Hour)
	ctx := context.Background()
	for n := range 5 {
		if err := buf.Enqueue(ctx, item(n)); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}
	if got := buf.Count(); got != 5 {
		t.Fatalf("Count() = %d, want 5", got)
	}
	var wantBytes int
	for n := range 5 {
		wantBytes += len(item(n).Payload)
	}
	if got := buf.ByteCount(); got != wantBytes {
		t.Fatalf("ByteCount() = %d, want %d", got, wantBytes)
	}
	st := loop.State()
	if st.Buffer.EntryNum != 5 {
		t.Fatalf("EntryNum = %d, want 5", st.Buffer.EntryNum)
	}
	if st.Buffer.TotalSize != int64(wantBytes) {
		t.Fatalf("TotalSize = %d, want %d", st.Buffer.TotalSize, wantBytes)
	}
	if st.Buffer.Cursor == nil || string(st.Buffer.Cursor) != `"c4"` {
		t.Fatalf("Buffer cursor = %s, want \"c4\"", st.Buffer.Cursor)
	}
	if st.Buffer.CurrentQueue != 5 {
		t.Fatalf("CurrentQueue = %d, want 5", st.Buffer.CurrentQueue)
	}
}

func TestEnqueueCancelWhileFull(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		// Everything the bubble blocks on (transport, buffer, context)
		// is created inside the bubble — out-of-bubble channels make
		// blocks non-durable for synctest.Wait.
		tr := &fakeTransport{}
		_, buf := newWired(tr, 2, time.Hour)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// Fill the channel to capacity through the accounting path.
		for buf.Count() < cap(buf.Chan()) {
			if err := buf.Enqueue(ctx, item(0)); err != nil {
				t.Fatalf("Enqueue while filling: %v", err)
			}
		}
		if buf.Count() != cap(buf.Chan()) {
			t.Fatalf("buffer not full: %d of %d", buf.Count(), cap(buf.Chan()))
		}

		enqErr := make(chan error, 1)
		go func() {
			enqErr <- buf.Enqueue(ctx, item(1)) // blocks: channel full, no consumer
		}()
		synctest.Wait()
		select {
		case err := <-enqErr:
			t.Fatalf("Enqueue returned early: %v", err)
		default:
		}
		cancel()
		synctest.Wait()
		if err := <-enqErr; !errors.Is(err, context.Canceled) {
			t.Fatalf("Enqueue after cancel = %v, want context.Canceled", err)
		}
		if buf.Count() != cap(buf.Chan()) {
			t.Fatalf("canceled Enqueue must neither drop nor add: Count = %d, want %d",
				buf.Count(), cap(buf.Chan()))
		}
	})
}

// TestBackpressureStopAndResume is the plan's reader-backpressure
// test: one slow sender fills its buffer, the reader parks at the
// worst-sender gate, and resumes when the sender drains.
//
// Slow-sender shape: the transport blocks in the FIRST SendMessages
// forever, so after the first flush the loop goroutine is stuck
// inside the send while the reader keeps filling the channel until
// the gate closes it. The dynamics are deterministic in the synctest
// bubble: the reader's channel sends consume no virtual time, so the
// channel is at capacity before the first 100 ms tick.
func TestBackpressureStopAndResume(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		// Everything the bubble blocks on is created inside the
		// bubble — out-of-bubble channels make blocks non-durable for
		// synctest.Wait.
		const (
			maxLength = 1500
			total     = 4000 // large enough to park after any early drain
		)
		tr := &fakeTransport{gate: make(chan struct{})} // blocks every send
		loop, buf := newWired(tr, maxLength, 100*time.Millisecond)
		gate := buffer.NewGate(maxLength, maxLength, buf) // 1-byte payloads: both budgets close together
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() { _ = loop.Run(ctx) }()

		readDone := make(chan int, 1)
		go func() {
			// The simulated reader: per-message admission gate, like
			// Python's read_messages loop.
			n := 0
			one := sender.Item{Payload: []byte("x")}
			for n < total {
				if gate.WriteLimitCount() <= 0 || gate.WriteLimitBytes() <= 0 {
					time.Sleep(time.Millisecond) // reader paused by the gate
					continue
				}
				one.Cursor = "c" + strconv.Itoa(n)
				if err := buf.Enqueue(ctx, one); err != nil {
					return
				}
				n++
			}
			readDone <- n
		}()

		// The reader parks with a full buffer and stays parked while
		// the loop is stuck in its blocked send.
		synctest.Wait()
		select {
		case n := <-readDone:
			t.Fatalf("reader finished despite the full buffer: %d", n)
		default:
		}
		if got := buf.Count(); got != maxLength {
			t.Fatalf("Count() = %d, want the reader parked at maxLength %d", got, maxLength)
		}
		if got := buf.ByteCount(); got != maxLength {
			t.Fatalf("ByteCount() = %d, want %d", got, maxLength)
		}
		if got := gate.WriteLimitCount(); got != 0 {
			t.Fatalf("WriteLimitCount() = %d, want 0", got)
		}
		if got := gate.WriteLimitBytes(); got != 0 {
			t.Fatalf("WriteLimitBytes() = %d, want 0", got)
		}

		// Resume: release the transport — the blocked send completes,
		// the loop resumes ticking, drains refill, and the reader
		// finishes all remaining items.
		tr.release()
		for range 1000 {
			if buf.Count() == 0 && buf.ByteCount() == 0 {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		synctest.Wait()
		if got := buf.Count(); got != 0 {
			t.Fatalf("Count() = %d after release, want 0", got)
		}
		if got := buf.ByteCount(); got != 0 {
			t.Fatalf("ByteCount() = %d after release, want 0", got)
		}
		select {
		case n := <-readDone:
			if n != total {
				t.Fatalf("reader finished with %d, want %d", n, total)
			}
		default:
			t.Fatal("reader did not finish after release")
		}
	})
}

// TestDrainOpensGate pins the drain side of the accounting: the byte
// budget is released exactly when the loop's collect takes the items
// out of the channel (Python get_items' buffer_size reset), not by a
// raw receive.
func TestDrainOpensGate(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		tr := &fakeTransport{}
		buf := buffer.NewSenderBuffer("out", 100)
		loop := sender.NewLoop(
			sender.LoopConfig{Name: "out"},
			sender.Instance{Transport: tr, MaxSendInterval: 20 * time.Millisecond, Type: "FakeSender"},
			buf.Chan(),
		)
		buf.Attach(loop)
		gate := buffer.NewGate(50, 1000, buf)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() { _ = loop.Run(ctx) }()

		for n := range 3 {
			if err := buf.Enqueue(ctx, item(n)); err != nil {
				t.Fatalf("Enqueue: %v", err)
			}
		}
		if got := gate.WriteLimitCount(); got != 50-3 {
			t.Fatalf("WriteLimitCount() = %d, want %d", got, 50-3)
		}
		// The idle trigger (20ms) drains on the first 100ms tick.
		for range 100 {
			if buf.Count() == 0 && buf.ByteCount() == 0 {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		synctest.Wait()
		if got := buf.Count(); got != 0 {
			t.Fatalf("Count() = %d after the drain, want 0", got)
		}
		if got := buf.ByteCount(); got != 0 {
			t.Fatalf("ByteCount() = %d after the drain, want 0", got)
		}
		if got := gate.WriteLimitCount(); got != 50 {
			t.Fatalf("WriteLimitCount() = %d after the drain, want 50", got)
		}
		if got := gate.WriteLimitBytes(); got != 1000 {
			t.Fatalf("WriteLimitBytes() = %d after the drain, want 1000", got)
		}
	})
}

// TestConcurrentEnqueueDrain runs producers against a draining loop
// under -race. The drain assertion waits for the transport's DELIVERY
// acknowledgment (SendMessages completion), not merely the empty
// counters — collection without delivery was the reproduced race.
func TestConcurrentEnqueueDrain(t *testing.T) {
	t.Parallel()
	tr := &fakeTransport{}
	loop, buf := newWired(tr, buffer.DefaultMsgBufferMaxLength, 50*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		_ = loop.Run(ctx)
	}()
	// Cancel and join the loop before returning (no goroutine leak).
	defer func() {
		cancel()
		<-loopDone
	}()

	const (
		producers = 4
		perProd   = 300 // 1200 total: trips the >1000 flush trigger
		total     = producers * perProd
	)
	var wg sync.WaitGroup
	for p := range producers {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for n := range perProd {
				if err := buf.Enqueue(ctx, item(base*perProd+n)); err != nil {
					t.Errorf("Enqueue: %v", err)
					return
				}
			}
		}(p)
	}
	wg.Wait()

	// Wait for every enqueued message to be DELIVERED (SendMessages
	// completed), then the buffer accounting must be released.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if tr.messageCount() == total {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if got := tr.messageCount(); got != total {
		t.Fatalf("delivered %d messages, want %d (empty counters prove collection, not delivery)", got, total)
	}
	if got := tr.callCount(); got == 0 {
		t.Fatal("transport was never called")
	}
	if got := buf.Count(); got != 0 {
		t.Fatalf("Count() = %d after drain, want 0", got)
	}
	if got := buf.ByteCount(); got != 0 {
		t.Fatalf("ByteCount() = %d after drain, want 0", got)
	}
}

func TestGateLimits(t *testing.T) {
	t.Parallel()
	tr := &fakeTransport{}
	_, slow := newWired(tr, 100, time.Hour)
	_, fast := newWired(tr, 100, time.Hour)
	ctx := context.Background()
	// The slow sender's buffer holds 3 items; the gate must gate on
	// the WORST sender.
	for n := range 3 {
		if err := slow.Enqueue(ctx, item(n)); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}
	if err := fast.Enqueue(ctx, item(0)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	gate := buffer.NewGate(50, 1000, slow, fast)
	if got := gate.WriteLimitCount(); got != 50-3 {
		t.Fatalf("WriteLimitCount() = %d, want %d (worst sender)", got, 50-3)
	}
	wantBytes := 3 * len(item(0).Payload) // the WORST sender's bytes, not the sum
	if got := gate.WriteLimitBytes(); got != 1000-wantBytes {
		t.Fatalf("WriteLimitBytes() = %d, want %d", got, 1000-wantBytes)
	}

	t.Run("no senders gives the Python no-sender limits", func(t *testing.T) {
		t.Parallel()
		empty := buffer.NewGate(50, 1000)
		if got := empty.WriteLimitCount(); got != buffer.ChunkSize {
			t.Fatalf("WriteLimitCount() = %d, want CHUNK_SIZE %d", got, buffer.ChunkSize)
		}
		if got := empty.WriteLimitBytes(); got != buffer.DefaultMsgBufferMaxBytes {
			t.Fatalf("WriteLimitBytes() = %d, want _5_MB", got)
		}
	})
}

func TestAttachEnforced(t *testing.T) {
	t.Parallel()
	buf := buffer.NewSenderBuffer("out", 10)
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("Enqueue without Attach must panic")
		}
	}()
	_ = buf.Enqueue(context.Background(), item(0))
}
