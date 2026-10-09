// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package syslogframing

import (
	"bytes"
	"strconv"
	"testing"
)

// FuzzFramingSelfConsistent fuzzes the framer (plan Task 19): whatever
// the body and max_message_size, an octet-counted frame's advertised
// length must equal the body actually framed (never desyncing the
// stream), and a non-transparent frame must be body-verbatim when it
// fits or body[:max-1]+"\n" when truncated.
func FuzzFramingSelfConsistent(f *testing.F) {
	f.Add([]byte("hello\n"), 8, true)
	f.Add([]byte("<13>1 2024-01-01T00:00:00Z h a - - m\n"), 4096, false)
	f.Add([]byte(""), 1, true)
	f.Add(bytes.Repeat([]byte("x"), 300), 50, false)
	f.Add([]byte("abcdef"), -1, true)
	f.Add([]byte("abcdef"), -3, false)
	f.Add([]byte("abcdef"), 0, true)
	f.Fuzz(func(t *testing.T, body []byte, maxMsg int, octet bool) {
		fr := NewFraming(maxMsg, octet)
		wire := fr.Frame(body)

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
			want := pySliceEnd(body, fr.MaxMsg)
			if !bytes.Equal(sent, want) {
				t.Fatalf("octet body = %q, want %q", sent, want)
			}
			return
		}

		// Non-transparent: Python message[:max_msg-1], plus an explicit
		// newline when len(message) >= max_msg.
		want := pySliceEnd(body, fr.MaxMsg-1)
		if len(body) >= fr.MaxMsg {
			want = append(append([]byte(nil), want...), '\n')
		}
		if !bytes.Equal(wire, want) {
			t.Fatalf("non-transparent wire = %q, want %q", wire, want)
		}
	})
}
