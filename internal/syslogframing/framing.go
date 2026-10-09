// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package syslogframing

import "strconv"

// DefaultMaxMsg is Python's max_msg default (rsyslog.py:118); config
// values are falsy-normalized to it (max_msg or 2048).
const DefaultMaxMsg = 2048

// Framing builds the bytes a single send puts on the wire, porting the
// two RFC 6587 methods in SyslogTcpClient.send (rsyslog.py:184-202).
type Framing struct {
	// MaxMsg bounds the message body; <= 0 means DefaultMaxMsg.
	MaxMsg int
	// OctetCounted selects RFC 6587 section 3.4.1 (<LEN> SP body, no
	// trailing newline) over non-transparent framing (section 3.4.2,
	// newline-delimited).
	OctetCounted bool
}

// NewFraming normalizes max_msg (Python's `max_msg or 2048`).
func NewFraming(maxMsg int, octetCounted bool) Framing {
	if maxMsg == 0 {
		maxMsg = DefaultMaxMsg
	}
	return Framing{MaxMsg: maxMsg, OctetCounted: octetCounted}
}

// Frame returns the wire bytes for message.
//
// Octet-counted: "<len(body)> <body>" where body = message[:MaxMsg]; the
// advertised length always equals the body sent, and there is no
// trailing newline (a stray newline would desync the next frame).
//
// Non-transparent: message[:MaxMsg-1], plus an explicit "\n" when
// len(message) >= MaxMsg (the formatter's own newline is truncated away).
func (fr Framing) Frame(message []byte) []byte {
	if fr.OctetCounted {
		body := message
		if len(body) > fr.MaxMsg {
			body = body[:fr.MaxMsg]
		}
		out := make([]byte, 0, len(body)+4)
		out = strconv.AppendInt(out, int64(len(body)), 10)
		out = append(out, ' ')
		out = append(out, body...)
		return out
	}
	limit := fr.MaxMsg - 1
	if limit < 0 {
		limit = 0
	}
	body := message
	if len(body) > limit {
		body = body[:limit]
	}
	out := append([]byte(nil), body...)
	// The truncation test uses the ORIGINAL length (rsyslog.py:200-202):
	// a body at or beyond max_msg loses its formatter newline and gets an
	// explicit one.
	if len(message) >= fr.MaxMsg {
		out = append(out, '\n')
	}
	return out
}
