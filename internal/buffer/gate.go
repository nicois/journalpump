// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package buffer

// Gate is the reader-side admission gate: the worst-sender pre-read
// check of journalpump.py:210-227. The reader computes the write
// limits once per chunk and stops reading as soon as one reaches zero
// (journalpump.py:1019-1043), so one full sender pauses the whole
// reader without head-of-line blocking between senders — each sender
// has its own buffer.
type Gate struct {
	maxLength int
	maxBytes  int // 0 disables the byte budget for the caller (Python's falsy check)
	buffers   []*SenderBuffer
}

// NewGate builds the gate over the reader's sender buffers. maxLength
// and maxBytes are the reader's configured budgets (journalpump.py
// defaults 50000 / 5 MiB).
func NewGate(maxLength, maxBytes int, buffers ...*SenderBuffer) *Gate {
	return &Gate{maxLength: maxLength, maxBytes: maxBytes, buffers: buffers}
}

// WriteLimitCount ports journalpump.py:221-231: how many more
// messages the reader may read before the worst sender's buffer is
// full. With no senders the limit is Python's CHUNK_SIZE (readers
// that only collect search statistics never gate on senders). The
// "failed senders and no live senders → 0" variant lives with the
// reader wiring (it owns the failed-sender bookkeeping).
func (g *Gate) WriteLimitCount() int {
	if len(g.buffers) == 0 {
		return ChunkSize
	}
	worst := 0
	for _, b := range g.buffers {
		if count := b.Count(); count > worst {
			worst = count
		}
	}
	return g.maxLength - worst
}

// WriteLimitBytes ports journalpump.py:210-219: how many more payload
// bytes the reader may read before the worst sender's byte budget is
// exhausted. With no senders the limit is Python's literal _5_MB.
// Callers that honor the msg_buffer_max_bytes=0 disable-switch (the
// falsy check in update_status) must consult maxBytes themselves; the
// gate reproduces the arithmetic for any configured value.
func (g *Gate) WriteLimitBytes() int {
	if len(g.buffers) == 0 {
		return DefaultMsgBufferMaxBytes
	}
	worst := 0
	for _, b := range g.buffers {
		if size := b.ByteCount(); size > worst {
			worst = size
		}
	}
	return g.maxBytes - worst
}
