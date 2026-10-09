// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package syslogframing

import "strconv"

// DefaultMaxMsg is Python's max_msg default (rsyslog.py:118): `max_msg or
// 2048`, so ONLY the falsy 0 is replaced — negative values are preserved
// and used as Python negative slice bounds.
const DefaultMaxMsg = 2048

// Framing builds the bytes a single send puts on the wire, porting the
// two RFC 6587 methods in SyslogTcpClient.send (rsyslog.py:184-202).
type Framing struct {
	// MaxMsg bounds the message body; == 0 means DefaultMaxMsg. Negative
	// values are preserved (Python's `max_msg or 2048` keeps them) and
	// applied as Python negative slice indices.
	MaxMsg int
	// OctetCounted selects RFC 6587 section 3.4.1 (<LEN> SP body, no
	// trailing newline) over non-transparent framing (section 3.4.2,
	// newline-delimited).
	OctetCounted bool
}

// NewFraming normalizes max_msg exactly like Python: only 0 falls back to
// the default (`max_msg or 2048`); negatives pass through.
func NewFraming(maxMsg int, octetCounted bool) Framing {
	if maxMsg == 0 {
		maxMsg = DefaultMaxMsg
	}
	return Framing{MaxMsg: maxMsg, OctetCounted: octetCounted}
}

// pySliceEnd ports Python's b[:end], including the negative-index form:
// b[:-n] drops the last n bytes (empty when n exceeds the length).
func pySliceEnd(b []byte, end int) []byte {
	if end < 0 {
		end += len(b)
		if end < 0 {
			end = 0
		}
	}
	if end > len(b) {
		end = len(b)
	}
	return b[:end]
}

// Frame returns the wire bytes for message.
//
// Octet-counted: "<len(body)> <body>" where body = message[:MaxMsg]
// (Python negative-index slicing when MaxMsg < 0); the advertised length
// always equals the body sent, and there is no trailing newline (a stray
// newline would desync the next frame).
//
// Non-transparent: message[:MaxMsg-1], plus an explicit "\n" when
// len(message) >= MaxMsg (the formatter's own newline is truncated away).
func (fr Framing) Frame(message []byte) []byte {
	if fr.OctetCounted {
		body := pySliceEnd(message, fr.MaxMsg)
		out := make([]byte, 0, len(body)+4)
		out = strconv.AppendInt(out, int64(len(body)), 10)
		out = append(out, ' ')
		out = append(out, body...)
		return out
	}
	body := pySliceEnd(message, fr.MaxMsg-1)
	out := append([]byte(nil), body...)
	// The truncation test uses the ORIGINAL length (rsyslog.py:200-202):
	// a body at or beyond max_msg loses its formatter newline and gets an
	// explicit one. With a negative max_msg this is always true.
	if len(message) >= fr.MaxMsg {
		out = append(out, '\n')
	}
	return out
}
