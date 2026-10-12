// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package journal

import (
	"bytes"
	"math/rand"
	"strconv"
	"testing"
	"time"
	"unicode/utf8"
)

// The transform micro-benchmark isolates the pure-Go part of the read
// path — journal fields -> converted values -> processed keys ->
// serialized JSON bytes — from the poll pacing and the cgo read. It is
// the "our transform" half of the per-entry budget in
// docs/perf-root-cause.md; the cgo half is measured from the whole-run
// profile because it needs a live sd-journal handle.
//
// The field set mirrors bench/gen_journal.py's 16 fields (as they come
// back from journalctl), with the same converter hits: _PID/PRIORITY
// (int), __REALTIME_TIMESTAMP/_BOOT_ID/_MACHINE_ID/__MONOTONIC_TIMESTAMP
// (converter table), MESSAGE (80-400 B ASCII, every 1000th entry carrying
// one invalid UTF-8 byte).

func benchRawFields(message []byte) []Field {
	return []Field{
		{Key: "__CURSOR", Value: []byte("s=91ae92bb6cbd414bad3894fb8df2e74a;i=493e0;b=0123456789abcdef0123456789abcdef;m=0;t=5af3119859f18;x=7631647777d4934f")},
		{Key: "__SEQNUM", Value: []byte("299999")},
		{Key: "__SEQNUM_ID", Value: []byte("91ae92bb6cbd414bad3894fb8df2e74a")},
		{Key: "_SYSTEMD_UNIT", Value: []byte("bench-c.service")},
		{Key: "BENCH_TOKEN", Value: []byte("tok-00299999")},
		{Key: "_PID", Value: []byte("5999")},
		{Key: "SYSLOG_IDENTIFIER", Value: []byte("bench-c.service")},
		{Key: "MESSAGE", Value: message},
		{Key: "PRIORITY", Value: []byte("7")},
		{Key: "__MONOTONIC_TIMESTAMP", Value: []byte("0")},
		{Key: "_MACHINE_ID", Value: []byte("fedcba9876543210fedcba9876543210")},
		{Key: "_HOSTNAME", Value: []byte("journalpump-bench")},
		{Key: "_BOOT_ID", Value: []byte("0123456789abcdef0123456789abcdef")},
		{Key: "BENCH_SEQ", Value: []byte("299999")},
		{Key: "__REALTIME_TIMESTAMP", Value: []byte("1600000299999000")},
		{Key: "BENCH_RUN", Value: []byte("bench")},
	}
}

func benchMessage() []byte {
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 .,:;-_"
	rng := rand.New(rand.NewSource(20261012))
	out := make([]byte, 240)
	for i := range out {
		out[i] = alphabet[rng.Intn(len(alphabet))]
	}
	return out
}

func benchTimestamp() time.Time {
	return time.Unix(1600000299, 999000000).UTC()
}

// transformOne is the pure-Go per-entry pipeline: reader conversion,
// process() key rewriting, the timestamp the handler injects, and the
// final json.dumps-equivalent serialization.
func transformOne(raw []Field) ([]byte, error) {
	converted := ConvertEntry(raw)
	processed := ProcessEntry(converted)
	processed = SetField(processed, "timestamp", benchTimestamp())
	return Serialize(processed)
}

func BenchmarkTransform(b *testing.B) {
	raw := benchRawFields(benchMessage())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := transformOne(raw); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkTransformInvalidUTF8 exercises the repr() fallback MESSAGE
// (every 1000th fixture entry).
func BenchmarkTransformInvalidUTF8(b *testing.B) {
	message := benchMessage()
	message[1] = 0xff
	raw := benchRawFields(message)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := transformOne(raw); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSerializeOnly isolates the JSON emission, so the reader-side
// conversion cost can be subtracted.
func BenchmarkSerializeOnly(b *testing.B) {
	fields := ProcessEntry(ConvertEntry(benchRawFields(benchMessage())))
	fields = SetField(fields, "timestamp", benchTimestamp())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Serialize(fields); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkConvertEntry(b *testing.B) {
	raw := benchRawFields(benchMessage())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ConvertEntry(raw)
	}
}

func BenchmarkProcessEntry(b *testing.B) {
	fields := ConvertEntry(benchRawFields(benchMessage()))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ProcessEntry(fields)
	}
}

// --- reference implementation (measurement only, NOT production code) ------
//
// appendJSONStringRef appends PythonJSONString(s) to dst without the
// per-call bytes.Buffer: an ASCII fast path copies the whole run at once,
// and anything needing an escape falls back to PythonJSONString, so the
// bytes are identical by construction. BenchmarkSerializeRef measures the
// serializer's headroom with a reused output buffer — the number the
// ranking in docs/perf-root-cause.md is calibrated against.

func appendJSONStringRef(dst []byte, s string) []byte {
	clean := true
	for i := 0; i < len(s); i++ {
		if c := s[i]; c < 0x20 || c >= 0x7f || c == '"' || c == '\\' {
			clean = false
			break
		}
	}
	if !clean {
		return append(dst, PythonJSONString(s)...)
	}
	dst = append(dst, '"')
	dst = append(dst, s...)
	return append(dst, '"')
}

func appendPythonJSONRef(dst []byte, value any) []byte {
	switch v := value.(type) {
	case string:
		return appendJSONStringRef(dst, v)
	case []byte:
		if utf8.Valid(v) {
			return appendJSONStringRef(dst, string(v))
		}
		return dst
	case int64:
		return strconv.AppendInt(dst, v, 10)
	case float64:
		return append(dst, PythonFloatRepr(v)...)
	case time.Time:
		return appendJSONStringRef(dst, pythonISOFormat(v))
	default:
		return append(dst, PythonJSONString("")...)
	}
}

func serializeRef(fields []Field, buf []byte) []byte {
	buf = append(buf, '{')
	for i, field := range fields {
		if i > 0 {
			buf = append(buf, ", "...)
		}
		buf = appendJSONStringRef(buf, field.Key)
		buf = append(buf, ": "...)
		buf = appendPythonJSONRef(buf, field.Value)
	}
	return append(buf, '}')
}

func BenchmarkSerializeRef(b *testing.B) {
	fields := ProcessEntry(ConvertEntry(benchRawFields(benchMessage())))
	fields = SetField(fields, "timestamp", benchTimestamp())
	buf := make([]byte, 0, 2048)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = serializeRef(fields, buf[:0])
	}
	if len(buf) == 0 {
		b.Fatal("empty output")
	}
}

// BenchmarkMaterializeAlike replicates the per-field work materialize()
// does around the cgo read (key slice-to-string, value copy, duplicate
// scan, slice append) over one entry's worth of raw tuples, so the
// allocation count of that Go-side code can be attributed without a live
// journal handle. It is NOT the production function: materialize reads
// each tuple from libsystemd, which cannot run in a benchmark.
func BenchmarkMaterializeAlike(b *testing.B) {
	tuples := [][]byte{}
	for _, f := range benchRawFields(benchMessage()) {
		tuple := append([]byte(f.Key), '=')
		tuple = append(tuple, f.Value.([]byte)...)
		tuples = append(tuples, tuple)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var fields []Field
		for _, data := range tuples {
			idx := bytes.IndexByte(data, '=')
			key := string(data[:idx])
			value := append([]byte(nil), data[idx+1:]...)
			replaced := false
			for j := range fields {
				if fields[j].Key == key {
					replaced = true
					break
				}
			}
			if !replaced {
				fields = append(fields, Field{Key: key, Value: value})
			}
		}
		if len(fields) != len(tuples) {
			b.Fatalf("fields=%d tuples=%d", len(fields), len(tuples))
		}
	}
}
