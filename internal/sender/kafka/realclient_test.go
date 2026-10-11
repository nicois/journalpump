// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package kafka

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/Aiven-Open/journalpump/internal/sender"
)

// fakeBroker is a minimal Kafka broker stub for the REAL-client cancellation
// tests (item 4). It completes negotiation (ApiVersions + Metadata) so a real
// franz-go client finishes initialization, then OBSERVES Produce/CreateTopics
// requests and WITHHOLDS their responses. That is the only way to exercise the
// production liveProducer/liveAdmin cancellation against an in-flight request
// rather than a stalled initialization (round3's real-client test stalls during
// metadata; the produce/admin paths there use fakes).
type fakeBroker struct {
	ln    net.Listener
	addr  string
	topic string

	produceSeen chan struct{}
	createSeen  chan struct{}
	onceProduce sync.Once
	onceCreate  sync.Once
}

func startFakeBroker(t *testing.T, topic string) *fakeBroker {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	fb := &fakeBroker{
		ln:          ln,
		addr:        ln.Addr().String(),
		topic:       topic,
		produceSeen: make(chan struct{}),
		createSeen:  make(chan struct{}),
	}
	go fb.serve()
	t.Cleanup(func() { _ = ln.Close() })
	return fb
}

func (f *fakeBroker) serve() {
	for {
		conn, err := f.ln.Accept()
		if err != nil {
			return
		}
		go f.handle(conn)
	}
}

// handle reads length-prefixed requests and answers only the negotiation
// requests; Produce (0) and CreateTopics (19) are observed then left
// unanswered, and the connection is held open until the client closes it.
func (f *fakeBroker) handle(conn net.Conn) {
	defer func() { _ = conn.Close() }()
	br := bufio.NewReader(conn)
	for {
		var size int32
		if err := binary.Read(br, binary.BigEndian, &size); err != nil {
			return
		}
		if size < 8 || size > 1<<20 {
			return
		}
		buf := make([]byte, size)
		if _, err := io.ReadFull(br, buf); err != nil {
			return
		}
		apiKey := int16(binary.BigEndian.Uint16(buf[0:2]))
		apiVersion := int16(binary.BigEndian.Uint16(buf[2:4]))
		corrID := int32(binary.BigEndian.Uint32(buf[4:8]))
		switch apiKey {
		case 18: // ApiVersions
			f.writeResponse(conn, corrID, f.apiVersions(apiVersion))
		case 3: // Metadata
			f.writeResponse(conn, corrID, f.metadata(apiVersion))
		case 0: // Produce — observe and withhold
			f.onceProduce.Do(func() { close(f.produceSeen) })
		case 19: // CreateTopics — observe and withhold
			f.onceCreate.Do(func() { close(f.createSeen) })
		default:
			// Withhold anything else.
		}
	}
}

// apiVersions advertises non-flexible caps so the client's subsequent requests
// (and our hand-encoded response headers) stay simple.
func (f *fakeBroker) apiVersions(ver int16) kmsg.Response {
	r := kmsg.NewPtrApiVersionsResponse()
	r.Version = ver
	r.ApiKeys = []kmsg.ApiVersionsResponseApiKey{
		{ApiKey: 0, MinVersion: 0, MaxVersion: 8},  // Produce (flexible >= 9)
		{ApiKey: 3, MinVersion: 0, MaxVersion: 8},  // Metadata (flexible >= 9)
		{ApiKey: 18, MinVersion: 0, MaxVersion: 2}, // ApiVersions (flexible >= 3)
		{ApiKey: 19, MinVersion: 0, MaxVersion: 4}, // CreateTopics (flexible >= 5)
	}
	return r
}

func (f *fakeBroker) metadata(ver int16) kmsg.Response {
	host, portStr, _ := net.SplitHostPort(f.addr)
	port, _ := strconv.Atoi(portStr)
	topic := f.topic
	r := kmsg.NewPtrMetadataResponse()
	r.Version = ver
	r.ControllerID = 0
	r.Brokers = []kmsg.MetadataResponseBroker{{NodeID: 0, Host: host, Port: int32(port)}}
	r.Topics = []kmsg.MetadataResponseTopic{{
		Topic: &topic,
		Partitions: []kmsg.MetadataResponseTopicPartition{{
			Partition: 0,
			Leader:    0,
			Replicas:  []int32{0},
			ISR:       []int32{0},
		}},
	}}
	return r
}

// writeResponse frames a response: size, correlation id, and (only for
// flexible requests other than ApiVersions, matching franz-go) an empty tag
// buffer before the body.
func (f *fakeBroker) writeResponse(conn net.Conn, corrID int32, resp kmsg.Response) {
	req := resp.RequestKind()
	req.SetVersion(resp.GetVersion())
	flexible := req.IsFlexible() && resp.Key() != 18
	body := resp.AppendTo(nil)
	payload := make([]byte, 4, 4+1+len(body))
	binary.BigEndian.PutUint32(payload, uint32(corrID))
	if flexible {
		payload = append(payload, 0)
	}
	payload = append(payload, body...)
	out := make([]byte, 4)
	binary.BigEndian.PutUint32(out, uint32(len(payload)))
	_, _ = conn.Write(out)
	_, _ = conn.Write(payload)
}

// TestRealProducerCancellationAbortsStalledProduce (item 4): a REAL franz-go
// client completes initialization, the Produce request is observed and its
// response withheld, and cancelling the loop's context aborts the in-flight
// produce promptly and stops the loop WITHOUT advancing the sent cursor.
func TestRealProducerCancellationAbortsStalledProduce(t *testing.T) {
	fb := startFakeBroker(t, "test-topic")
	inst, err := New(sender.ConstructorArgs{Name: "kafka-real", Config: map[string]any{
		"kafka_address": fb.addr,
		"kafka_topic":   "test-topic",
	}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	s := inst.Transport.(*Sender)
	// A large deadline so only the ctx cancellation can end this send.
	s.deliveryDeadline = time.Hour

	in := make(chan sender.Item, 4)
	loop := sender.NewLoop(sender.LoopConfig{Name: "kafka-real"}, inst, in)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { _ = loop.Run(ctx); close(done) }()

	in <- sender.Item{Payload: []byte("stalled"), Cursor: "c0"}

	select {
	case <-fb.produceSeen:
	case <-time.After(15 * time.Second):
		t.Fatal("the real client never issued a Produce request after initialization")
	}

	start := time.Now()
	cancel()
	select {
	case <-done:
		if elapsed := time.Since(start); elapsed > 5*time.Second {
			t.Fatalf("loop stopped %v after cancellation; the real client did not abort the in-flight Produce", elapsed)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("loop did not stop after cancellation; the real client did not abort the in-flight Produce")
	}

	st := loop.State()
	if st.Sent.Count != 0 {
		t.Errorf("sent count = %d, want 0 (a cancelled produce must not be acked)", st.Sent.Count)
	}
	if st.Sent.Cursor != nil {
		t.Errorf("sent cursor = %v, want nil (no cursor advancement on cancellation)", *st.Sent.Cursor)
	}
}

// TestRealAdminCancellationAbortsCreateTopic (item 4): a REAL franz-go admin
// client issues CreateTopics (after completing negotiation) and the response is
// withheld; cancelling the caller's context aborts the call via doWithClose.
func TestRealAdminCancellationAbortsCreateTopic(t *testing.T) {
	fb := startFakeBroker(t, "auto-topic")
	raw := map[string]any{
		"kafka_address":      fb.addr,
		"kafka_topic":        "auto-topic",
		"kafka_topic_config": map[string]any{"num_partitions": 2, "replication_factor": 1},
	}
	inst, err := New(sender.ConstructorArgs{Name: "kafka-real", Config: raw})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	s := inst.Transport.(*Sender)
	pc, err := resolveProducerConfig(raw)
	if err != nil {
		t.Fatalf("resolveProducerConfig: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.maybeCreateTopic(ctx, pc) }()

	select {
	case <-fb.createSeen:
	case <-time.After(15 * time.Second):
		t.Fatal("the real admin client never issued a CreateTopics request")
	}

	start := time.Now()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("maybeCreateTopic error = %v, want context.Canceled", err)
		}
		if elapsed := time.Since(start); elapsed > 5*time.Second {
			t.Fatalf("maybeCreateTopic took %v after cancellation; the real admin call was not aborted", elapsed)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("maybeCreateTopic did not return after cancellation; the real admin call was not aborted")
	}
}
