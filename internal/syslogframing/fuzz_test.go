// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package syslogframing

import (
	"bytes"
	"math"
	"strconv"
	"testing"
)

// oracleFrame independently reimplements Python's rsyslog.py:184-202
// framing so the fuzz expectation does NOT call production code (calling
// pySliceEnd would validate the implementation against itself). It
// includes the `max_msg or 2048` zero normalization and Python
// arbitrary-precision slice bounds, saturated safely at the int64
// extremes where `max_msg - 1` would overflow Go int.
func oracleFrame(body []byte, maxMsgRaw int, octet bool) []byte {
	maxMsg := maxMsgRaw
	if maxMsg == 0 {
		maxMsg = 2048
	}
	if octet {
		// <len> SP body, body = message[:max_msg].
		b := pythonSlice(body, maxMsg)
		out := append([]byte(strconv.Itoa(len(b))), ' ')
		return append(out, b...)
	}
	// message[:max_msg - 1], plus "\n" when len(message) >= max_msg.
	bound := maxMsg
	if maxMsg != math.MinInt {
		bound = maxMsg - 1
	}
	out := append([]byte(nil), pythonSlice(body, bound)...)
	if len(body) >= maxMsg {
		out = append(out, '\n')
	}
	return out
}

// pythonSlice reimplements Python's b[:end] for any end, including the
// negative-index and out-of-range forms, without touching production code.
func pythonSlice(b []byte, end int) []byte {
	switch {
	case end < 0:
		n := len(b) + end
		if n < 0 {
			n = 0
		}
		return b[:n]
	case end > len(b):
		return b
	default:
		return b[:end]
	}
}

// FuzzFramingSelfConsistent fuzzes the framer (plan Task 19): whatever
// the body and max_message_size, an octet-counted frame's advertised
// length must equal the body actually framed (never desyncing the
// stream), and the framed bytes must equal the independently computed
// oracle. The int64 extremes (math.MinInt/MaxInt max_msg) are seeds.
func FuzzFramingSelfConsistent(f *testing.F) {
	f.Add([]byte("hello\n"), 8, true)
	f.Add([]byte("<13>1 2024-01-01T00:00:00Z h a - - m\n"), 4096, false)
	f.Add([]byte(""), 1, true)
	f.Add(bytes.Repeat([]byte("x"), 300), 50, false)
	f.Add([]byte("abcdef"), -1, true)
	f.Add([]byte("abcdef"), -3, false)
	f.Add([]byte("abcdef"), 0, true)
	f.Add([]byte("abcdef"), math.MinInt, true)
	f.Add([]byte("abcdef"), math.MinInt, false)
	f.Add([]byte("abcdef"), math.MaxInt, false)
	f.Fuzz(func(t *testing.T, body []byte, maxMsg int, octet bool) {
		wire := NewFraming(maxMsg, octet).Frame(body)

		if octet {
			sp := bytes.IndexByte(wire, ' ')
			if sp < 0 {
				t.Fatalf("octet frame %q has no length prefix", wire)
			}
			n, err := strconv.Atoi(string(wire[:sp]))
			if err != nil {
				t.Fatalf("bad length prefix %q: %v", wire[:sp], err)
			}
			sent := wire[sp+1:]
			if n != len(sent) {
				t.Fatalf("advertised %d != sent %d (stream desync)", n, len(sent))
			}
		}

		if want := oracleFrame(body, maxMsg, octet); !bytes.Equal(wire, want) {
			t.Fatalf("Frame(%q, %d, octet=%v) = %q, want %q", body, maxMsg, octet, wire, want)
		}
	})
}
