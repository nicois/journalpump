// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

// Package buffer ports the per-sender message buffer and the reader
// backpressure of journalpump (plan Task 14, spec §4.3): a buffered
// channel of (payload, cursor) per sender plus the worst-sender
// admission gate the reader consults before reading more journal
// entries.
//
// Python shape, for reference:
//
//   - senders/base.py:MsgBuffer — a locked list drained by
//     get_items (snapshot + clear, buffer_size reset); add_item keeps
//     the cumulative entry_num/total_size/cursor the state file
//     serializes.
//   - journalpump.py:210-244 — get_write_limit_bytes /
//     get_write_limit_message_count subtract the WORST sender's
//     occupancy from msg_buffer_max_bytes / msg_buffer_max_length;
//     read_messages (journalpump.py:1019-1043) stops reading as soon
//     as a limit reaches zero — one full sender pauses the whole
//     reader, and nothing is ever dropped.
//
// The Go handoff is a channel instead of a locked list: the sender
// loop (internal/sender) consumes Chan() and reads len(Chan()) as its
// flush trigger — the same signal Python's loop takes from
// len(msg_buffer). Admission never overfills the channel because the
// reader re-checks the gate per message, so enqueue blocks only in
// fault scenarios and retries with the context — it never drops.
package buffer

import (
	"context"
	"sync"

	"github.com/Aiven-Open/journalpump/internal/sender"
)

// Framework constants ported from journalpump.py.
const (
	// DefaultMsgBufferMaxLength is journalpump.py:148's
	// msg_buffer_max_length default.
	DefaultMsgBufferMaxLength = 50000
	// DefaultMsgBufferMaxBytes is journalpump.py:149's
	// msg_buffer_max_bytes default (_5_MB).
	DefaultMsgBufferMaxBytes = 5 * 1024 * 1024
	// ChunkSize is journalpump.py:36 CHUNK_SIZE (the per-chunk read
	// budget and the no-senders message limit).
	ChunkSize = 5000
	// admissionHeadroom is the small capacity margin on top of the
	// message budget (spec §4.3) so an item already past the gate is
	// never blocked behind a momentarily full channel.
	admissionHeadroom = 16
)

// SenderBuffer is one sender's buffer: the channel the sender loop
// consumes plus the in-flight byte accounting the admission gate
// reads. Safe for concurrent use.
type SenderBuffer struct {
	name string
	ch   chan sender.Item
	loop *sender.Loop

	mu    sync.Mutex
	bytes int // payload bytes currently in the channel
}

// NewSenderBuffer builds a buffer whose channel capacity is maxLength
// (+ a small headroom). Wiring order in the composed system:
// NewSenderBuffer → sender.NewLoop with Chan() → Attach.
func NewSenderBuffer(name string, maxLength int) *SenderBuffer {
	if maxLength < 1 {
		// The config layer enforces positive values; defensively fall
		// back to the Python default instead of an unbuffered channel.
		maxLength = DefaultMsgBufferMaxLength
	}
	return &SenderBuffer{
		name: name,
		ch:   make(chan sender.Item, maxLength+admissionHeadroom),
	}
}

// Attach wires the buffer to its sender loop: the loop's dequeue
// observer maintains the byte accounting exactly where Python's
// get_items resets buffer_size (senders/base.py:86), and Enqueue
// advances the loop's cumulative buffer counters on admission — the
// same moment Python's add_item does. Must be called before Enqueue
// and before the loop runs.
func (b *SenderBuffer) Attach(loop *sender.Loop) {
	b.loop = loop
	loop.SetDequeueObserver(b.drained)
}

// Chan is the receive side the sender loop consumes. Its length is
// the loop's flush trigger; the sender package must be handed this
// channel (via NewLoop), not drain anything else.
func (b *SenderBuffer) Chan() <-chan sender.Item { return b.ch }

// Enqueue admits one item, blocking while the channel is full and
// retrying until it fits or ctx is done — the spec §4.3 "never drops"
// rule. On admission it updates the in-flight byte accounting and the
// loop's cumulative buffer counters (Python add_item's accounting
// half).
func (b *SenderBuffer) Enqueue(ctx context.Context, item sender.Item) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for {
		select {
		case b.ch <- item:
			b.mu.Lock()
			b.bytes += len(item.Payload)
			b.mu.Unlock()
			if b.loop != nil {
				b.loop.TrackEnqueue(item)
			}
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// drained is the loop's dequeue observer: items leaving the channel
// release their byte budget, exactly where Python's get_items resets
// buffer_size. Called on the loop goroutine.
func (b *SenderBuffer) drained(items []sender.Item) {
	b.mu.Lock()
	for _, item := range items {
		b.bytes -= len(item.Payload)
	}
	b.mu.Unlock()
}

// Count is the current message occupancy (Python len(msg_buffer)).
func (b *SenderBuffer) Count() int { return len(b.ch) }

// ByteCount is the current in-flight payload bytes (Python
// msg_buffer.buffer_size). Note: Python measures with
// sys.getsizeof(item), i.e. with the CPython bytes-object overhead
// (~33 B/item); Go counts plain payload lengths — a CPython memory
// measurement detail, not intent (the budget only feeds the transient
// admission gate and appears nowhere serialized).
func (b *SenderBuffer) ByteCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.bytes
}

// Name is the sender name the buffer serves.
func (b *SenderBuffer) Name() string { return b.name }
