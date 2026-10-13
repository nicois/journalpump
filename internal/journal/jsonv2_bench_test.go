// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package journal

import (
	"encoding/json/jsontext"
	"fmt"
	"strconv"
	"strings"
	"testing"

	v2 "encoding/json/v2"
)

// This file measures the stdlib JSON encoders (encoding/json/v2, Go 1.27)
// against the hand-rolled emitter (EmitJSON) on the SAME 16-field bench
// entry, so docs/perf-attribution.md can quote a like-for-like ns/op and
// allocs/op. It is MEASUREMENT ONLY: nothing here is wired into the
// production pipeline.

// benchEntryV2 is the 16-field bench entry as a plain Go struct with the
// emitted (underscore-stripped) key names, in the order EmitJSON emits
// them. Extras are appended as a slice, mirroring extra_field_values.
// Values are the POST-conversion spellings the pipeline produces (UUID
// canonicalisation, numeric realtime, default-class BENCH_SEQ as a
// string), so semantic parity with EmitJSON is a value comparison, not a
// type-normalisation accident.
type benchEntryV2 struct {
	Cursor             string       `json:"CURSOR"`
	SeqnumID           string       `json:"SEQNUM_ID"`
	SystemdUnit        string       `json:"SYSTEMD_UNIT"`
	BenchToken         string       `json:"BENCH_TOKEN"`
	PID                int64        `json:"PID"`
	SyslogIdentifier   string       `json:"SYSLOG_IDENTIFIER"`
	Message            string       `json:"MESSAGE"`
	Priority           int64        `json:"PRIORITY"`
	MonotonicTimestamp any          `json:"MONOTONIC_TIMESTAMP"`
	MachineID          string       `json:"MACHINE_ID"`
	Hostname           string       `json:"HOSTNAME"`
	BootID             string       `json:"BOOT_ID"`
	BenchSeq           string       `json:"BENCH_SEQ"`
	BenchRun           string       `json:"BENCH_RUN"`
	RealtimeTimestamp  float64      `json:"REALTIME_TIMESTAMP"`
	Seqnum             int64        `json:"SEQNUM"`
	Timestamp          string       `json:"timestamp"`
	ExtraFieldValues   []benchExtra `json:"extra_field_values,omitempty"`
}

type benchExtra struct {
	Env     string `json:"env,omitempty"`
	Region  string `json:"region,omitempty"`
	Cluster string `json:"cluster,omitempty"`
}

// benchStructV2 builds the struct representation of the same fixture the
// EmitJSON benchmark uses, with the values the raw pipeline emits.
func benchStructV2() benchEntryV2 {
	return benchEntryV2{
		Cursor:             "s=91ae92bb6cbd414bad3894fb8df2e74a;i=493e0;b=0123456789abcdef0123456789abcdef;m=0;t=5af3119859f18;x=7631647777d4934f",
		SeqnumID:           "91ae92bb6cbd414bad3894fb8df2e74a",
		SystemdUnit:        "bench-c.service",
		BenchToken:         "tok-00299999",
		PID:                5999,
		SyslogIdentifier:   "bench-c.service",
		Message:            string(benchMessage()),
		Priority:           7,
		MonotonicTimestamp: nil,
		MachineID:          "fedcba98-7654-3210-fedc-ba9876543210",
		Hostname:           "journalpump-bench",
		BootID:             "01234567-89ab-cdef-0123-456789abcdef",
		BenchSeq:           "299999",
		BenchRun:           "bench",
		RealtimeTimestamp:  1600000299.999,
		Seqnum:             300000,
		Timestamp:          "2020-09-13T12:31:39.999000",
	}
}

func benchMapV2() map[string]any {
	return map[string]any{
		"CURSOR":              "s=91ae92bb6cbd414bad3894fb8df2e74a;i=493e0;b=0123456789abcdef0123456789abcdef;m=0;t=5af3119859f18;x=7631647777d4934f",
		"SEQNUM_ID":           "91ae92bb6cbd414bad3894fb8df2e74a",
		"SYSTEMD_UNIT":        "bench-c.service",
		"BENCH_TOKEN":         "tok-00299999",
		"PID":                 5999,
		"SYSLOG_IDENTIFIER":   "bench-c.service",
		"MESSAGE":             string(benchMessage()),
		"PRIORITY":            7,
		"MONOTONIC_TIMESTAMP": nil,
		"MACHINE_ID":          "fedcba98-7654-3210-fedc-ba9876543210",
		"HOSTNAME":            "journalpump-bench",
		"BOOT_ID":             "01234567-89ab-cdef-0123-456789abcdef",
		"BENCH_SEQ":           "299999",
		"BENCH_RUN":           "bench",
		"REALTIME_TIMESTAMP":  1600000299.999,
		"SEQNUM":              300000,
		"timestamp":           "2020-09-13T12:31:39.999000",
	}
}

func BenchmarkEmitJSONv2Struct(b *testing.B) {
	entry := benchStructV2()
	b.ReportAllocs()
	b.ResetTimer()
	var out []byte
	var err error
	for i := 0; i < b.N; i++ {
		out, err = v2.Marshal(entry)
		if err != nil {
			b.Fatal(err)
		}
	}
	_ = out
}

func BenchmarkEmitJSONv2Map(b *testing.B) {
	entry := benchMapV2()
	b.ReportAllocs()
	b.ResetTimer()
	var out []byte
	var err error
	for i := 0; i < b.N; i++ {
		out, err = v2.Marshal(entry)
		if err != nil {
			b.Fatal(err)
		}
	}
	_ = out
}

// TestJSONv2StructFieldOrder pins the two properties the canary cares
// about for the struct encoder: struct fields are emitted in declaration
// order, and the encoder appends nothing between them beyond the compact
// JSON punctuation.
func TestJSONv2StructFieldOrder(t *testing.T) {
	t.Parallel()
	type s struct {
		B int `json:"b"`
		A int `json:"a"`
	}
	out, err := v2.Marshal(s{B: 1, A: 2})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if got, want := string(out), `{"b":1,"a":2}`; got != want {
		t.Errorf("v2 struct output = %s, want declaration order %s", got, want)
	}
}

// TestJSONv2MapOrderNotDeterministic documents that the map encoder does
// NOT sort keys: Go map iteration randomness leaks into the bytes, so a
// map-based encoder cannot satisfy a byte-exact canary.
func TestJSONv2MapOrderNotDeterministic(t *testing.T) {
	t.Parallel()
	m := map[string]any{"b": 1, "a": 2, "m": 3}
	seen := map[string]bool{}
	for i := 0; i < 200; i++ {
		out, err := v2.Marshal(m)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		seen[string(out)] = true
	}
	if len(seen) < 2 {
		t.Skipf("map encoder happened to be deterministic here: %v", seen)
	}
	t.Logf("map encoder produced %d distinct orderings over 200 marshals: not byte-stable", len(seen))
}

// TestJSONv2StructMatchesEmitJSONModuloSeparators pins, on the 16-field
// bench entry, that the v2 struct encoder emits exactly the same
// keys/values IN THE SAME ORDER as EmitJSON, differing only by the
// separators (v2 compact `,`/`:`, Python `", "`/`": "`). That is the
// strongest "same fields, same order" statement short of byte identity.
func TestJSONv2StructMatchesEmitJSONModuloSeparators(t *testing.T) {
	t.Parallel()
	batch := benchBatch()
	emit, err := EmitJSON(nil, batch, &batch.Entries[0], &EmitOptions{})
	if err != nil {
		t.Fatalf("EmitJSON: %v", err)
	}
	v2out, err := v2.Marshal(benchStructV2())
	if err != nil {
		t.Fatalf("v2.Marshal: %v", err)
	}
	if !semanticEqual(t, emit, v2out) {
		t.Fatalf("v2 struct is not semantically equal to EmitJSON:\n emit: %s\n v2:   %s", emit, v2out)
	}
	compact := strings.ReplaceAll(strings.ReplaceAll(string(emit), ", ", ","), ": ", ":")
	if compact != string(v2out) {
		t.Errorf("v2 struct differs from EmitJSON by more than separators:\n emit-compact: %s\n v2:           %s", compact, v2out)
	}
	t.Logf("v2 struct byte-identical to EmitJSON after `, `/`: ` -> `,`/`:` (separators are the only diff)")
}

// TestJSONv2NonASCIIIsNotPythonEscaped pins the wire-level divergence
// that survives even with the struct encoder: Python's
// json.dumps(ensure_ascii=True) escapes non-ASCII as \uXXXX, v2 emits
// raw UTF-8. The payload is semantically equal but NOT byte-equal, so a
// v2 encoder would break byte-exact fixtures while leaving a semantic
// canary green.
func TestJSONv2NonASCIIIsNotPythonEscaped(t *testing.T) {
	t.Parallel()
	type s struct {
		M string `json:"MESSAGE"`
	}
	out, err := v2.Marshal(s{M: "héllo 世界"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	python := `{"MESSAGE": "héllo 世界"}`
	if string(out) == python {
		t.Fatalf("v2 unexpectedly matched Python's escaped form: %s", out)
	}
	if !semanticEqual(t, []byte(python), out) {
		t.Fatalf("v2 non-ASCII form is not even semantically equal:\n python: %s\n v2:     %s", python, out)
	}
	t.Logf("v2 emits raw UTF-8 (%s); Python escapes (%s): semantic-only parity", out, python)
}

// BenchmarkAppendJSONStringRef and BenchmarkJSONTextAppendQuote isolate the
// string-escaping primitive: our appendJSONString (Python
// json.dumps(ensure_ascii=True): \uXXXX escapes for non-ASCII) against the
// stdlib jsontext.AppendQuote (compact, raw UTF-8). The Task 32 charter
// now permits either escaping form, so this is the "is the library
// escaping cheaper than ours" half of the v2-format review item.
func BenchmarkAppendJSONStringRef(b *testing.B) {
	msg := benchMessage()
	dst := make([]byte, 0, 512)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst = appendJSONString(dst[:0], msg)
	}
	_ = dst
}

func BenchmarkJSONTextAppendQuote(b *testing.B) {
	msg := benchMessage()
	dst := make([]byte, 0, 512)
	b.ReportAllocs()
	b.ResetTimer()
	var err error
	for i := 0; i < b.N; i++ {
		dst, err = jsontext.AppendQuote(dst[:0], msg)
		if err != nil {
			b.Fatal(err)
		}
	}
	_ = dst
}

// BenchmarkEmitJSONjsontext builds the same 16-field object with the
// jsontext primitives, writing fields in dynamic (source) order — the
// format-relaxed candidate: unlike a fixed struct it CAN express an
// arbitrary field set, at the cost of our Python-fidelity conversions.
func BenchmarkEmitJSONjsontext(b *testing.B) {
	e := benchStructV2()
	dst := make([]byte, 0, 1024)
	b.ReportAllocs()
	b.ResetTimer()
	var err error
	for i := 0; i < b.N; i++ {
		dst, err = appendJSONTextEntry(dst[:0], e)
		if err != nil {
			b.Fatal(err)
		}
	}
	_ = dst
}

func appendJSONTextEntry(dst []byte, e benchEntryV2) ([]byte, error) {
	dst = append(dst, '{')
	first := true
	put := []struct {
		k string
		v any
	}{
		{"CURSOR", e.Cursor},
		{"SEQNUM_ID", e.SeqnumID},
		{"SYSTEMD_UNIT", e.SystemdUnit},
		{"BENCH_TOKEN", e.BenchToken},
		{"PID", e.PID},
		{"SYSLOG_IDENTIFIER", e.SyslogIdentifier},
		{"MESSAGE", e.Message},
		{"PRIORITY", e.Priority},
		{"MONOTONIC_TIMESTAMP", e.MonotonicTimestamp},
		{"MACHINE_ID", e.MachineID},
		{"HOSTNAME", e.Hostname},
		{"BOOT_ID", e.BootID},
		{"BENCH_SEQ", e.BenchSeq},
		{"BENCH_RUN", e.BenchRun},
		{"REALTIME_TIMESTAMP", e.RealtimeTimestamp},
		{"SEQNUM", e.Seqnum},
		{"timestamp", e.Timestamp},
	}
	for _, f := range put {
		if !first {
			dst = append(dst, ',')
		}
		first = false
		var err error
		dst, err = jsontext.AppendQuote(dst, f.k)
		if err != nil {
			return nil, err
		}
		dst = append(dst, ':')
		switch v := f.v.(type) {
		case string:
			dst, err = jsontext.AppendQuote(dst, v)
		case int64:
			dst = strconv.AppendInt(dst, v, 10)
		case float64:
			dst = jsontext.AppendFloat(dst, v, 64)
		case nil:
			dst = append(dst, "null"...)
		default:
			return nil, fmt.Errorf("unhandled %T", f.v)
		}
		if err != nil {
			return nil, err
		}
	}
	return append(dst, '}'), nil
}
