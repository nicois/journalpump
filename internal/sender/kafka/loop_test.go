// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package kafka

import (
	"context"
	"errors"
	"fmt"
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
}

func (r *recordingProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	r.mu.Lock()
	if r.delay > 0 {
		d := r.delay
		r.mu.Unlock()
		time.Sleep(d)
		r.mu.Lock()
	}
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

// TestLoopDelayedAckAdvancesCursor: the producer blocks for a while (a
// delayed broker ack) before succeeding; the loop must deliver the batch
// and only then advance the cursor (review item E14).
func TestLoopDelayedAckAdvancesCursor(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		loop, in, rp, stop := startKafkaLoop(t, baseConfig())
		rp.delay = 2 * time.Second
		before := loop.State()
		if before.Sent.Cursor != nil {
			t.Fatalf("cursor before first ack = %v, want nil", before.Sent.Cursor)
		}
		in <- sender.Item{Payload: []byte("delayed"), Cursor: "c0"}
		time.Sleep(10 * time.Second)
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
		s.newProducer = func(producerConfig) (producer, error) {
			connects++
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
		if adminCalls == 0 {
			t.Error("topic create was not re-attempted after reconnect")
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
