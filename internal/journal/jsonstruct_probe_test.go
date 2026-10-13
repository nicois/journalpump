// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package journal

import (
	"bytes"
	"encoding/json/jsontext"
	"testing"

	v2 "encoding/json/v2"
)

// This file records the "JSON -> struct with the remaining keys in a
// rawjson object" question (user, 2026-10-13) as compile-and-run evidence
// on THIS toolchain (Go 1.27), and measures the entry-representation
// alternative it maps onto. MEASUREMENT / PROBE ONLY: nothing here is
// wired into the production path.

// probeTyped is the modelled part of a journal entry: the ~16 common fields.
type probeTyped struct {
	Message  string `json:"MESSAGE"`
	Priority int64  `json:"PRIORITY"`
}

// probeInline is the ",inline" catch-all spelling one might expect to work.
type probeInline struct {
	Message   string                    `json:"MESSAGE"`
	Priority  int64                     `json:"PRIORITY"`
	Remainder map[string]jsontext.Value `json:",inline"`
}

// probeEmbedded is an anonymous-style embedded map with json:"-".
type probeEmbedded struct {
	Message   string                    `json:"MESSAGE"`
	Priority  int64                     `json:"PRIORITY"`
	Remainder map[string]jsontext.Value `json:"-"`
}

const probeInput = `{"MESSAGE":"hello","PRIORITY":7,"ZED":"z","ALPHA":"a","_BOOT_ID":"0123","N":42}`

// TestJSONv2HasNoInlineCatchAll pins the probe result: encoding/json/v2
// (Go 1.27) has NO tag or embedded-map form that captures unmodelled
// members. Extra keys are silently dropped (they are only rejected when
// the RejectUnknownMembers option is set). Both the ",inline" tag and an
// embedded json:"-" map compile and run but receive an EMPTY remainder.
func TestJSONv2HasNoInlineCatchAll(t *testing.T) {
	t.Parallel()

	var typed probeTyped
	if err := v2.Unmarshal([]byte(probeInput), &typed); err != nil {
		t.Fatalf("typed unmarshal: %v", err)
	}
	if typed.Message != "hello" || typed.Priority != 7 {
		t.Fatalf("typed fields not decoded: %+v", typed)
	}

	var inline probeInline
	if err := v2.Unmarshal([]byte(probeInput), &inline); err != nil {
		t.Fatalf(`",inline" unmarshal: %v`, err)
	}
	if len(inline.Remainder) != 0 {
		t.Errorf(`",inline" tag captured %d extras, probe expected 0 (v2 has no catch-all): %v`,
			len(inline.Remainder), inline.Remainder)
	}

	var embedded probeEmbedded
	if err := v2.Unmarshal([]byte(probeInput), &embedded); err != nil {
		t.Fatalf(`embedded-map unmarshal: %v`, err)
	}
	if len(embedded.Remainder) != 0 {
		t.Errorf("embedded json:\"-\" map captured %d extras, probe expected 0: %v",
			len(embedded.Remainder), embedded.Remainder)
	}
	t.Logf("v2 drops unmodelled members: typed %+v, inline remainder %v, embedded remainder %v",
		typed, inline.Remainder, embedded.Remainder)
}

// TestJSONv2RejectUnknownMembersIsTheOnlyControl pins the one knob v2
// offers for unknown members: rejection (an error), never capture.
func TestJSONv2RejectUnknownMembersIsTheOnlyControl(t *testing.T) {
	t.Parallel()
	var typed probeTyped
	if err := v2.Unmarshal([]byte(probeInput), &typed, v2.RejectUnknownMembers(true)); err == nil {
		t.Fatal("RejectUnknownMembers(true) accepted unmodelled ZED/ALPHA keys")
	} else {
		t.Logf("RejectUnknownMembers(true) errors as expected: %v", err)
	}
}

// TestJSONTextDecoderPreservesOrder pins the ONLY mechanism that captures
// unmodelled keys with their ORDER intact on this toolchain: a manual
// jsontext.Decoder loop. A map (v1 or v2) loses order, which the canary's
// journald field order requires.
func TestJSONTextDecoderPreservesOrder(t *testing.T) {
	t.Parallel()
	dec := jsontext.NewDecoder(bytes.NewReader([]byte(probeInput)))
	tok, err := dec.ReadToken()
	if err != nil || tok.Kind() != '{' {
		t.Fatalf("read begin: %v %v", err, tok)
	}
	var order []string
	remainder := map[string]jsontext.Value{}
	for dec.PeekKind() != '}' {
		keyTok, err := dec.ReadToken()
		if err != nil {
			t.Fatalf("read key: %v", err)
		}
		key := keyTok.String()
		order = append(order, key)
		raw, err := dec.ReadValue()
		if err != nil {
			t.Fatalf("read value: %v", err)
		}
		switch key {
		case "MESSAGE", "PRIORITY":
		default:
			remainder[key] = raw
		}
	}
	want := []string{"MESSAGE", "PRIORITY", "ZED", "ALPHA", "_BOOT_ID", "N"}
	if len(order) != len(want) {
		t.Fatalf("order = %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order = %v, want %v", order, want)
		}
	}
	if len(remainder) != 4 {
		t.Errorf("remainder = %v, want 4 extras", remainder)
	}
	t.Logf("jsontext preserves order %v; remainder %v", order, remainder)
}

// entryIndex is the in-process "typed struct of the common fields + opaque
// remainder" representation the question maps onto. The hot path produces
// JSON from journald BYTES (it does not decode JSON), so the win being
// tested is not decoding: it is resolving the common fields once per entry
// instead of the gate's per-key linear scan, with no string strip, no
// value copy and no `any` boxing.
type entryIndex struct {
	systemdUnit int // -1 when absent
	priority    int
	remainder   []int
}

func indexEntry(b *Batch, e *RawEntry) entryIndex {
	ix := entryIndex{systemdUnit: -1, priority: -1}
	for i := range e.Fields {
		field := &e.Fields[i]
		if field.Duplicate {
			continue
		}
		key := stripUnderscore(b.Key(field))
		switch {
		case bytes.Equal(key, []byte("SYSTEMD_UNIT")):
			ix.systemdUnit = i
		case bytes.Equal(key, []byte("PRIORITY")):
			ix.priority = i
		default:
			ix.remainder = append(ix.remainder, i)
		}
	}
	return ix
}

// BenchmarkGateCurrent measures the current effectiveUnitPriority shape:
// one rawGroupValue scan per key, each allocating string(stripUnderscore)
// per field and boxing the converted value through processedRawValue.
func BenchmarkGateCurrent(b *testing.B) {
	batch := benchBatch()
	entry := &batch.Entries[0]
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = rawGroupValue(batch, entry, "SYSTEMD_UNIT", processedRawValue)
		_, _ = rawGroupValue(batch, entry, "PRIORITY", processedRawValue)
	}
}

// BenchmarkGateStructIndex measures the typed-index shape: one field walk
// per entry, no strip allocation, no boxing; the values stay []byte views.
func BenchmarkGateStructIndex(b *testing.B) {
	batch := benchBatch()
	entry := &batch.Entries[0]
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ix := indexEntry(batch, entry)
		if ix.systemdUnit >= 0 {
			_ = batch.ValueBytes(&entry.Fields[ix.systemdUnit])
		}
		if ix.priority >= 0 {
			_ = batch.ValueBytes(&entry.Fields[ix.priority])
		}
	}
}
