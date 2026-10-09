// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package rsyslog

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"

	"github.com/Aiven-Open/journalpump/internal/sender"
	"github.com/Aiven-Open/journalpump/internal/syslogframing"
)

// stubConn records the wire bytes (the syslogframing.Conn seam).
type stubConn struct {
	sent []byte
}

func (s *stubConn) Write(p []byte) (int, error) {
	s.sent = append(s.sent, p...)
	return len(p), nil
}
func (s *stubConn) Close() error { return nil }

// wireCase is one fixture entry captured from the real Python
// RsyslogSender (see test/test_rsyslog_sender_groundtruth.py).
type wireCase struct {
	Name     string           `json:"name"`
	Config   map[string]any   `json:"config"`
	Messages []map[string]any `json:"messages"`
	WireHex  string           `json:"wire_hex"`
}

// TestWireMatchesPythonGroundtruth replays the committed Python capture:
// the SAME sender config and journal payloads must produce the SAME wire
// bytes through the Go sender. Every case carries an explicit timestamp,
// so the capture is deterministic (the missing-timestamp fallback is
// wall-clock and covered by the unit tests).
func TestWireMatchesPythonGroundtruth(t *testing.T) {
	raw, err := os.ReadFile("testdata/wire_groundtruth.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var cases []wireCase
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatalf("parse fixture: %v", err)
	}
	if len(cases) == 0 {
		t.Fatal("fixture is empty")
	}
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			cfg := make(map[string]any, len(tc.Config)+2)
			for k, v := range tc.Config {
				cfg[k] = v
			}
			cfg["rsyslog_server"] = "127.0.0.1"
			cfg["rsyslog_port"] = 514

			inst, err := New(sender.ConstructorArgs{Name: "t", Config: cfg})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			s := inst.Transport.(*Sender)
			stub := &stubConn{}
			s.newClient = func(ctx context.Context) (logClient, error) {
				resolved, err := s.resolvedConfig()
				if err != nil {
					return nil, err
				}
				clientCfg := resolved.client
				clientCfg.Dial = func(context.Context) (syslogframing.Conn, error) { return stub, nil }
				return syslogframing.NewClient(ctx, clientCfg)
			}

			payloads := make([][]byte, len(tc.Messages))
			for i, m := range tc.Messages {
				b, err := json.Marshal(m)
				if err != nil {
					t.Fatalf("marshal message %d: %v", i, err)
				}
				payloads[i] = b
			}
			if err := s.SendMessages(context.Background(), payloads, "c"); err != nil {
				t.Fatalf("SendMessages: %v", err)
			}
			want, err := hex.DecodeString(tc.WireHex)
			if err != nil {
				t.Fatalf("decode wire_hex: %v", err)
			}
			if string(stub.sent) != string(want) {
				t.Fatalf("wire = %q, want %q", stub.sent, want)
			}
		})
	}
}
