// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/IBM/sarama"

	"github.com/Aiven-Open/journalpump/internal/sender"
)

// recordingProducer is a producer fake driven through the REAL
// sender.Loop. It records every message it is handed (value bytes, key,
// topic) in call order and replays a scripted error per SendMessages
// call, so tests can assert the exact ordered bytes the loop delivers and
// the classification/retry behavior with realistic sarama result types.
type recordingProducer struct {
	mu     sync.Mutex
	values [][]byte
	keys   []string
	topics []string
	errs   []error
	delay  time.Duration
	// calls counts SendMessages invocations; when holdAt is non-zero the
	// call with that 1-based index blocks on gate before delivering, so a
	// test can inspect the loop's cursor/count/health WHILE an ack is
	// pending (review item 9).
	calls  int
	holdAt int
	gate   chan struct{}
}

func (r *recordingProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	r.mu.Lock()
	r.calls++
	hold := r.holdAt > 0 && r.calls == r.holdAt
	gate := r.gate
	delay := r.delay
	r.mu.Unlock()
	if hold && gate != nil {
		<-gate
	}
	if delay > 0 {
		time.Sleep(delay)
	}
	r.mu.Lock()
	for _, m := range msgs {
		v, _ := m.Value.Encode()
		r.values = append(r.values, v)
		r.topics = append(r.topics, m.Topic)
		if m.Key == nil {
			r.keys = append(r.keys, "")
		} else {
			k, _ := m.Key.Encode()
			r.keys = append(r.keys, string(k))
		}
	}
	var err error
	if len(r.errs) > 0 {
		err = r.errs[0]
		r.errs = r.errs[1:]
	}
	r.mu.Unlock()
	return err
}

func (r *recordingProducer) Close() error { return nil }

func (r *recordingProducer) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

func (r *recordingProducer) recorded() ([][]byte, []string, []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([][]byte(nil), r.values...), append([]string(nil), r.keys...), append([]string(nil), r.topics...)
}

// startKafkaLoop wires a KafkaSender onto a fake producer factory and runs
// the real sender.Loop, returning the loop and the channel to enqueue on.
func startKafkaLoop(t *testing.T, raw map[string]any) (*sender.Loop, chan<- sender.Item, *recordingProducer, func()) {
	t.Helper()
	inst, err := New(sender.ConstructorArgs{Name: "kafka-loop", Config: raw})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	s := inst.Transport.(*Sender)
	rp := &recordingProducer{}
	s.newProducer = func(producerConfig) (producer, error) { return rp, nil }
	in := make(chan sender.Item, 16)
	loop := sender.NewLoop(sender.LoopConfig{Name: "kafka-loop"}, inst, in)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = loop.Run(ctx)
		close(done)
	}()
	stop := func() {
		cancel()
		<-done
	}
	return loop, in, rp, stop
}

// TestLoopSendsExactOrderedBytes asserts, through the real loop, the exact
// payload bytes, the fixed key, the topic, the per-flush batch cursor and
// the loop-level sent count/cursor (review item E14).
func TestLoopSendsExactOrderedBytes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		raw := baseConfig()
		raw["kafka_msg_key"] = "journalpump"
		loop, in, rp, stop := startKafkaLoop(t, raw)

		in <- sender.Item{Payload: []byte("line-0"), Cursor: "c0"}
		in <- sender.Item{Payload: []byte("line-1"), Cursor: "c1"}
		time.Sleep(time.Second)

		in <- sender.Item{Payload: []byte("line-2"), Cursor: "c2"}
		in <- sender.Item{Payload: []byte("line-3"), Cursor: "c3"}
		time.Sleep(time.Second)
		synctest.Wait()

		st := loop.State()
		stop()

		values, keys, topics := rp.recorded()
		want := []string{"line-0", "line-1", "line-2", "line-3"}
		if len(values) != len(want) {
			t.Fatalf("delivered %d messages, want %d", len(values), len(want))
		}
		for i, w := range want {
			if string(values[i]) != w {
				t.Errorf("message %d = %q, want %q", i, values[i], w)
			}
			if keys[i] != "journalpump" {
				t.Errorf("message %d key = %q, want journalpump", i, keys[i])
			}
			if topics[i] != "test-topic" {
				t.Errorf("message %d topic = %q, want test-topic", i, topics[i])
			}
		}
		if st.Sent.Count != 4 {
			t.Errorf("sent count = %d, want 4", st.Sent.Count)
		}
		if st.Sent.Cursor == nil || *st.Sent.Cursor != "c3" {
			t.Errorf("sent cursor = %v, want c3", st.Sent.Cursor)
		}
	})
}

// TestLoopDelayedAckAdvancesCursor: the producer is held on an explicit gate
// (a delayed broker ack). The loop must deliver nothing and advance no cursor
// WHILE the ack is pending, and only advance it once the ack arrives
// (review items E14/9).
func TestLoopDelayedAckAdvancesCursor(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		loop, in, rp, stop := startKafkaLoop(t, baseConfig())
		rp.gate = make(chan struct{})
		rp.holdAt = 1
		in <- sender.Item{Payload: []byte("delayed"), Cursor: "c0"}
		// Let the loop pick the item up and block inside SendMessages.
		time.Sleep(time.Second)
		synctest.Wait()

		if rp.callCount() != 1 {
			t.Fatalf("SendMessages calls = %d, want 1 (the in-flight ack)", rp.callCount())
		}
		if v, _, _ := rp.recorded(); len(v) != 0 {
			t.Fatalf("delivered %q while the ack was pending; want nothing", v)
		}
		mid := loop.State()
		if mid.Sent.Count != 0 {
			t.Errorf("sent count during a pending ack = %d, want 0", mid.Sent.Count)
		}
		if mid.Sent.Cursor != nil {
			t.Errorf("cursor advanced to %v during a pending ack; want nil", *mid.Sent.Cursor)
		}
		if mid.Buffer.CurrentQueue != 0 {
			t.Errorf("queued items during a pending ack = %d, want 0 (the batch is in flight)", mid.Buffer.CurrentQueue)
		}

		rp.gate <- struct{}{}
		time.Sleep(time.Second)
		synctest.Wait()
		st := loop.State()
		stop()
		if st.Sent.Count != 1 {
			t.Errorf("sent count = %d, want 1", st.Sent.Count)
		}
		if st.Sent.Cursor == nil || *st.Sent.Cursor != "c0" {
			t.Errorf("sent cursor = %v, want c0", st.Sent.Cursor)
		}
		if v, _, _ := rp.recorded(); len(v) != 1 || string(v[0]) != "delayed" {
			t.Errorf("delivered = %q, want [delayed]", v)
		}
	})
}

// TestLoopRetriesRetriableBatch: a batch that fails with sarama's
// ProducerErrors (all causes retriable) is dropped, the producer torn
// down, and the SAME batch reshaped and resent after the backoff; the
// cursor advances only on the successful attempt (review item E14).
func TestLoopRetriesRetriableBatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		inst, err := New(sender.ConstructorArgs{Name: "kafka-loop", Config: baseConfig()})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		s := inst.Transport.(*Sender)
		rp := &recordingProducer{}
		connects := 0
		// Hold the retry's RECONNECT so the test can inspect the state after
		// the failure but before the successful resend clears it.
		connectEntered := make(chan struct{})
		connectGate := make(chan struct{})
		s.newProducer = func(producerConfig) (producer, error) {
			connects++
			if connects == 2 {
				close(connectEntered)
				<-connectGate
			}
			return rp, nil
		}
		rp.errs = []error{producerErrors(sarama.ErrNotLeaderForPartition), nil}
		in := make(chan sender.Item, 4)
		loop := sender.NewLoop(sender.LoopConfig{Name: "kafka-loop"}, inst, in)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { _ = loop.Run(ctx); close(done) }()

		in <- sender.Item{Payload: []byte("retry-me"), Cursor: "c0"}
		time.Sleep(5 * time.Second)
		synctest.Wait()

		// Mid-retry: the first attempt has failed (its delivery recorded, the
		// producer torn down) and the retry is reconnecting; nothing has been
		// acked yet and the failure is still in the health ring.
		select {
		case <-connectEntered:
		default:
			t.Fatal("the loop did not attempt to reconnect after the retriable failure")
		}
		mid := loop.State()
		if mid.Sent.Count != 0 || mid.Sent.Cursor != nil {
			t.Errorf("mid-retry sent = %d cursor = %v; want 0/nil (no ack before the retry succeeds)", mid.Sent.Count, mid.Sent.Cursor)
		}
		if mid.Health.Status != "disconnected" {
			t.Errorf("mid-retry health status = %q, want disconnected", mid.Health.Status)
		}
		if len(mid.Health.Errors) == 0 {
			t.Fatal("mid-retry health errors empty; the failed attempt's cause was not recorded")
		}
		if !strings.Contains(mid.Health.Errors[0], "failed to deliver") {
			t.Errorf("mid-retry health error = %q, want the aggregate send error text", mid.Health.Errors[0])
		}

		close(connectGate)
		time.Sleep(time.Second)
		synctest.Wait()
		st := loop.State()
		cancel()
		<-done

		values, _, _ := rp.recorded()
		if len(values) != 2 || string(values[0]) != "retry-me" || string(values[1]) != "retry-me" {
			t.Fatalf("delivered = %q, want [retry-me retry-me]", values)
		}
		if connects != 2 {
			t.Errorf("producer connections = %d, want 2 (teardown + re-init on retry)", connects)
		}
		if st.Sent.Count != 1 {
			t.Errorf("sent count = %d, want 1 (only the successful attempt is acked)", st.Sent.Count)
		}
		if st.Sent.Cursor == nil || *st.Sent.Cursor != "c0" {
			t.Errorf("sent cursor = %v, want c0", st.Sent.Cursor)
		}
		if st.Health.Status != "connected" {
			t.Errorf("post-retry health status = %q, want connected", st.Health.Status)
		}
	})
}

// TestLoopPartialBatchFailureResendsWholeBatch: SendMessages reports
// per-message ProducerErrors with mixed causes. sarama's SyncProducer has
// no partial-ack semantics, so the whole batch fails (Retriable = false,
// the permanent path), is held, and is resent wholesale after the
// reconnect — the topic create re-init tolerating TopicAlreadyExists
// (review item E14).
func TestLoopPartialBatchFailureResendsWholeBatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		raw := baseConfig()
		raw["kafka_topic_config"] = map[string]any{"num_partitions": 2, "replication_factor": 1}
		inst, err := New(sender.ConstructorArgs{Name: "kafka-loop", Config: raw})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		s := inst.Transport.(*Sender)
		rp := &recordingProducer{}
		// One cause retriable, one permanent: not all-retriable, so the
		// batch is classified permanent.
		mixed := sarama.ProducerErrors{
			&sarama.ProducerError{Err: sarama.ErrNotLeaderForPartition},
			&sarama.ProducerError{Err: sarama.ErrInvalidMessage},
		}
		rp.errs = []error{mixed, nil}
		connects, adminCalls := 0, 0
		s.newProducer = func(producerConfig) (producer, error) {
			connects++
			return rp, nil
		}
		s.newAdmin = func(producerConfig) (admin, error) {
			adminCalls++
			return &fakeAdmin{err: sarama.ErrTopicAlreadyExists}, nil
		}
		in := make(chan sender.Item, 4)
		loop := sender.NewLoop(sender.LoopConfig{Name: "kafka-loop"}, inst, in)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { _ = loop.Run(ctx); close(done) }()

		in <- sender.Item{Payload: []byte("p0"), Cursor: "c0"}
		in <- sender.Item{Payload: []byte("p1"), Cursor: "c1"}
		time.Sleep(5 * time.Second)
		synctest.Wait()
		st := loop.State()
		cancel()
		<-done

		values, _, _ := rp.recorded()
		want := []string{"p0", "p1", "p0", "p1"}
		if len(values) != len(want) {
			t.Fatalf("delivered = %q, want %q (whole batch resent)", values, want)
		}
		for i, w := range want {
			if string(values[i]) != w {
				t.Errorf("message %d = %q, want %q", i, values[i], w)
			}
		}
		if connects != 2 {
			t.Errorf("producer connections = %d, want 2 (reconnect after failure)", connects)
		}
		if adminCalls != 2 {
			t.Errorf("admin initialization attempts = %d, want 2 (initial connect + reconnect after the failure)", adminCalls)
		}
		if st.Sent.Count != 2 {
			t.Errorf("sent count = %d, want 2", st.Sent.Count)
		}
		if st.Sent.Cursor == nil || *st.Sent.Cursor != "c1" {
			t.Errorf("sent cursor = %v, want c1", st.Sent.Cursor)
		}
		if st.Health.Status == "" {
			t.Error("health status empty")
		}
	})
}

// producerErrors builds sarama's batch error type with one cause per
// message, the shape SyncProducer.SendMessages returns.
func producerErrors(cause error) sarama.ProducerErrors {
	return sarama.ProducerErrors{&sarama.ProducerError{Msg: &sarama.ProducerMessage{}, Err: cause}}
}

// TestRecordingProducerShape guards the fake itself: it records message
// bytes in order and replays one scripted error per call.
func TestRecordingProducerShape(t *testing.T) {
	rp := &recordingProducer{errs: []error{errors.New("boom")}}
	msgs := []*sarama.ProducerMessage{
		{Value: sarama.ByteEncoder([]byte("a"))},
		{Value: sarama.ByteEncoder([]byte("b"))},
	}
	if err := rp.SendMessages(msgs); err == nil {
		t.Fatal("first call: want scripted error")
	}
	if err := rp.SendMessages(msgs); err != nil {
		t.Fatalf("second call: %v", err)
	}
	values, _, _ := rp.recorded()
	if fmt.Sprint(values) != "[[97] [98] [97] [98]]" {
		t.Fatalf("values = %v", values)
	}
}
