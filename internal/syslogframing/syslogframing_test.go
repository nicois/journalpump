// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package syslogframing

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"testing"
)

// stubConn records the bytes handed to sendall (the Python _StubSocket).
type stubConn struct {
	sent []byte
}

func (s *stubConn) Write(p []byte) (int, error) {
	s.sent = append(s.sent, p...)
	return len(p), nil
}

func (s *stubConn) Close() error { return nil }

// makeClient mirrors _make_client: a SyslogTcpClient with a patched
// connect whose socket is a recording stub.
func makeClient(t *testing.T, maxMsg int, octetCounted, escapeNewlines bool) (*Client, *stubConn) {
	t.Helper()
	return makeClientCFG(t, Config{
		MaxMsg:              maxMsg,
		OctetCountedFraming: octetCounted,
		EscapeNewlines:      escapeNewlines,
	})
}

func makeClientCFG(t *testing.T, cfg Config) (*Client, *stubConn) {
	t.Helper()
	sock := &stubConn{}
	cfg.Server = "127.0.0.1"
	cfg.Port = 1
	if cfg.RFC == "" {
		cfg.RFC = "RFC5424"
	}
	cfg.Dial = func(context.Context) (Conn, error) { return sock, nil }
	c, err := NewClient(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return c, sock
}

// head is Python's lenient slice b[:n] (clamped to len).
func head(b []byte, n int) []byte {
	if n > len(b) {
		n = len(b)
	}
	return b[:n]
}

// parseOctetCountedFrames ports _parse_octet_counted_frames.
func parseOctetCountedFrames(t *testing.T, stream []byte) [][]byte {
	t.Helper()
	var frames [][]byte
	for i := 0; i < len(stream); {
		sp := strings.IndexByte(string(stream[i:]), ' ')
		if sp < 0 {
			t.Fatalf("stream desync at offset %d: no space", i)
		}
		sp += i
		n, err := strconv.Atoi(string(stream[i:sp]))
		if err != nil {
			t.Fatalf("stream desync at offset %d: %v", i, err)
		}
		body := stream[sp+1 : sp+1+n]
		if len(body) != n {
			t.Fatalf("stream desync at offset %d: advertised %d bytes, got %d", i, n, len(body))
		}
		frames = append(frames, body)
		i = sp + 1 + n
	}
	return frames
}

// TestNonTransparentFraming ports TestNonTransparentFraming (the pre-fix
// newline-delimited behavior must stay byte-for-byte identical).
func TestNonTransparentFraming(t *testing.T) {
	small := []byte("<13>1 2024-01-01T00:00:00Z host app - - hello\n")

	t.Run("small_body_sent_verbatim", func(t *testing.T) {
		c, sock := makeClient(t, len(small)+10, false, false)
		if err := c.Send(context.Background(), small); err != nil {
			t.Fatal(err)
		}
		if string(sock.sent) != string(small) {
			t.Fatalf("wire = %q, want %q", sock.sent, small)
		}
	})
	t.Run("small_body_no_extra_newline", func(t *testing.T) {
		c, sock := makeClient(t, len(small)+10, false, false)
		if err := c.Send(context.Background(), small); err != nil {
			t.Fatal(err)
		}
		if got := strings.Count(string(sock.sent), "\n"); got != 1 {
			t.Fatalf("newline count = %d, want 1", got)
		}
	})
	t.Run("oversize_truncated_and_newline_appended", func(t *testing.T) {
		body := append([]byte(strings.Repeat("A", 200)), '\n')
		c, sock := makeClient(t, 50, false, false)
		if err := c.Send(context.Background(), body); err != nil {
			t.Fatal(err)
		}
		want := append(append([]byte(nil), body[:49]...), '\n')
		if string(sock.sent) != string(want) {
			t.Fatalf("wire = %q, want %q", sock.sent, want)
		}
	})
	t.Run("body_exactly_max_msg_triggers_truncation", func(t *testing.T) {
		body := []byte(strings.Repeat("B", 50))
		c, sock := makeClient(t, 50, false, false)
		if err := c.Send(context.Background(), body); err != nil {
			t.Fatal(err)
		}
		want := append(append([]byte(nil), body[:49]...), '\n')
		if string(sock.sent) != string(want) {
			t.Fatalf("wire = %q, want %q", sock.sent, want)
		}
	})
}

// TestOctetCountedFraming ports TestOctetCountedFraming.
func TestOctetCountedFraming(t *testing.T) {
	t.Run("small_multiline_body_framed_correctly", func(t *testing.T) {
		body := []byte("<13>1 2024-01-01T00:00:00Z host app - - line1\nline2\nline3\n")
		c, sock := makeClient(t, len(body)+50, true, false)
		if err := c.Send(context.Background(), body); err != nil {
			t.Fatal(err)
		}
		wire := sock.sent
		sp := strings.IndexByte(string(wire), ' ')
		advertised, _ := strconv.Atoi(string(wire[:sp]))
		if advertised != len(body) {
			t.Fatalf("advertised = %d, want %d", advertised, len(body))
		}
		if string(wire[sp+1:]) != string(body) {
			t.Fatalf("body = %q, want %q", wire[sp+1:], body)
		}
	})
	t.Run("small_body_no_trailing_newline_outside_frame", func(t *testing.T) {
		body := []byte("<13>1 2024-01-01T00:00:00Z host app - - msg\n")
		c, sock := makeClient(t, len(body)+50, true, false)
		if err := c.Send(context.Background(), body); err != nil {
			t.Fatal(err)
		}
		wire := sock.sent
		sp := strings.IndexByte(string(wire), ' ')
		n, _ := strconv.Atoi(string(wire[:sp]))
		if len(wire) != sp+1+n {
			t.Fatalf("len(wire)=%d, want %d (no stray newline)", len(wire), sp+1+n)
		}
	})
	t.Run("oversize_truncated_and_prefix_matches", func(t *testing.T) {
		body := []byte(strings.Repeat("X", 300))
		c, sock := makeClient(t, 100, true, false)
		if err := c.Send(context.Background(), body); err != nil {
			t.Fatal(err)
		}
		wire := sock.sent
		sp := strings.IndexByte(string(wire), ' ')
		advertised, _ := strconv.Atoi(string(wire[:sp]))
		sentBody := wire[sp+1:]
		if advertised != len(sentBody) {
			t.Fatalf("advertised = %d, body = %d", advertised, len(sentBody))
		}
		if len(sentBody) > 100 {
			t.Fatalf("body = %d bytes, want <= 100", len(sentBody))
		}
		if string(sentBody) != string(body[:100]) {
			t.Fatalf("body mismatch")
		}
	})
	t.Run("back_to_back_oversize_messages_no_desync", func(t *testing.T) {
		body1 := append([]byte("<13>1 2024-01-01T00:00:00Z h a - - "), append([]byte(strings.Repeat("M", 300)), '\n')...)
		body2 := []byte("<14>1 2024-01-01T00:00:01Z h b - - short\n")
		c, sock := makeClient(t, 60, true, false)
		if err := c.Send(context.Background(), body1); err != nil {
			t.Fatal(err)
		}
		if err := c.Send(context.Background(), body2); err != nil {
			t.Fatal(err)
		}
		frames := parseOctetCountedFrames(t, sock.sent)
		if len(frames) != 2 {
			t.Fatalf("frames = %d, want 2", len(frames))
		}
		if string(frames[0]) != string(body1[:60]) || string(frames[1]) != string(head(body2, 60)) {
			t.Fatalf("frame mismatch: %q %q", frames[0], frames[1])
		}
	})
	t.Run("very_small_max_msg_produces_self_consistent_frame", func(t *testing.T) {
		body := []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ\n")
		c, sock := makeClient(t, 8, true, false)
		if err := c.Send(context.Background(), body); err != nil {
			t.Fatal(err)
		}
		frames := parseOctetCountedFrames(t, sock.sent)
		if len(frames) != 1 || len(frames[0]) > 8 {
			t.Fatalf("frames = %d (len %d), want 1 (<= 8)", len(frames), len(frames[0]))
		}
	})
	t.Run("various_small_max_msg_always_self_consistent", func(t *testing.T) {
		for _, maxMsg := range []int{1, 4, 8, 16, 32} {
			t.Run(strconv.Itoa(maxMsg), func(t *testing.T) {
				body := []byte(strings.Repeat("Hello, multi\nline\nbody\n", 5))
				c, sock := makeClient(t, maxMsg, true, false)
				if err := c.Send(context.Background(), body); err != nil {
					t.Fatal(err)
				}
				frames := parseOctetCountedFrames(t, sock.sent)
				if len(frames) != 1 || len(frames[0]) > maxMsg {
					t.Fatalf("frames = %d (len %d), want 1 (<= %d)", len(frames), len(frames[0]), maxMsg)
				}
			})
		}
	})
}

// TestEscapeNewlines ports TestEscapeNewlines.
func TestEscapeNewlines(t *testing.T) {
	logMultiline := func(t *testing.T, escape bool) []byte {
		t.Helper()
		c, sock := makeClient(t, 4096, false, escape)
		host, program := "host", "app"
		msg := "line1\nline2\r\nline3"
		if err := c.Log(context.Background(), LogInput{
			Facility: 1, Severity: 6,
			Timestamp: "2024-01-01T00:00:00.000000Z",
			Hostname:  &host, Program: &program, Msg: &msg,
		}); err != nil {
			t.Fatal(err)
		}
		return sock.sent
	}

	t.Run("disabled_keeps_raw_newlines", func(t *testing.T) {
		got := logMultiline(t, false)
		want := "<14>1 2024-01-01T00:00:00.000000Z host app - - line1\nline2\r\nline3\n"
		if string(got) != want {
			t.Fatalf("wire = %q, want %q", got, want)
		}
	})
	t.Run("enabled_escapes_newlines", func(t *testing.T) {
		got := logMultiline(t, true)
		want := "<14>1 2024-01-01T00:00:00.000000Z host app - - line1\\nline2\\r\\nline3\n"
		if string(got) != want {
			t.Fatalf("wire = %q, want %q", got, want)
		}
	})
	t.Run("enabled_single_trailing_real_newline", func(t *testing.T) {
		if got := strings.Count(string(logMultiline(t, true)), "\n"); got != 1 {
			t.Fatalf("newline count = %d, want 1", got)
		}
	})
}

// oracleCase is one entry of the Python-generated fixture below.
type oracleCase struct {
	RFC                 string  `json:"rfc"`
	LogFormat           *string `json:"log_format"`
	Facility            int     `json:"facility"`
	Severity            int     `json:"severity"`
	Timestamp           string  `json:"timestamp"`
	Hostname            *string `json:"hostname"`
	Program             *string `json:"program"`
	PID                 *int    `json:"pid"`
	MsgID               *string `json:"msgid"`
	Msg                 *string `json:"msg"`
	SD                  *string `json:"sd"`
	OctetCountedFraming bool    `json:"octet_counted_framing"`
	EscapeNewlines      bool    `json:"escape_newlines"`
	MaxMsg              int     `json:"max_msg"`
	WireHex             string  `json:"wire_hex"`
}

// TestLogMatchesPythonGroundtruth replays testdata/logline_groundtruth.json
// — wire bytes captured from the REAL journalpump/rsyslog.py
// SyslogTcpClient (generated by the harness, not hand-written) — covering
// RFC5424/RFC3164/CUSTOM formatting, PRI in/out of range, the RFC3164
// date reformat, NILVALUE defaults, structured data and escape_newlines.
func TestLogMatchesPythonGroundtruth(t *testing.T) {
	raw, err := os.ReadFile("testdata/logline_groundtruth.json")
	if err != nil {
		t.Fatalf("read groundtruth: %v", err)
	}
	var cases []oracleCase
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatalf("parse groundtruth: %v", err)
	}
	if len(cases) == 0 {
		t.Fatal("groundtruth fixture is empty")
	}
	for i, tc := range cases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			logFormat := ""
			if tc.LogFormat != nil {
				logFormat = *tc.LogFormat
			}
			c, sock := makeClientCFG(t, Config{
				RFC:                 tc.RFC,
				LogFormat:           logFormat,
				MaxMsg:              tc.MaxMsg,
				OctetCountedFraming: tc.OctetCountedFraming,
				EscapeNewlines:      tc.EscapeNewlines,
			})
			var pid any
			if tc.PID != nil {
				pid = *tc.PID
			}
			if err := c.Log(context.Background(), LogInput{
				Facility:  tc.Facility,
				Severity:  tc.Severity,
				Timestamp: tc.Timestamp,
				Hostname:  tc.Hostname,
				Program:   tc.Program,
				PID:       pid,
				MsgID:     tc.MsgID,
				Msg:       tc.Msg,
				SD:        tc.SD,
			}); err != nil {
				t.Fatalf("Log: %v", err)
			}
			want, err := hex.DecodeString(tc.WireHex)
			if err != nil {
				t.Fatalf("decode wire_hex: %v", err)
			}
			if string(sock.sent) != string(want) {
				t.Fatalf("wire = %q, want %q", sock.sent, want)
			}
		})
	}
}
