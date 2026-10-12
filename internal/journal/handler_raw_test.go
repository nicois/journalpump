// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package journal

import (
	"encoding/json"
	"testing"

	"github.com/Aiven-Open/journalpump/internal/config"
	"github.com/Aiven-Open/journalpump/internal/filter"
)

// collectTargets gives each sender an AddItem that records its payload.
func collectTargets(targets []SenderTarget, got *[][]byte) []SenderTarget {
	out := make([]SenderTarget, len(targets))
	for i := range targets {
		target := targets[i]
		idx := i
		target.AddItem = func(item []byte, _ string) {
			(*got)[idx] = append([]byte(nil), item...)
		}
		out[i] = target
	}
	return out
}

func runBoth(t *testing.T, fields []string, targets []SenderTarget) (raw, old [][]byte) {
	t.Helper()
	cursor := "s=x;i=1a2b;b=1;m=2;t=3;x=4"
	b := buildBatch(t, 1700000000500000, cursor, fields...)
	entry := &b.Entries[0]

	rawGot := make([][]byte, len(targets))
	rawHandler := NewHandler(HandlerConfig{})
	if _, _, _, err := rawHandler.ProcessRaw(b, entry, collectTargets(targets, &rawGot)); err != nil {
		t.Fatalf("ProcessRaw: %v", err)
	}

	oldGot := make([][]byte, len(targets))
	oldHandler := NewHandler(HandlerConfig{})
	if _, _, _, err := oldHandler.Process(rawToEntry(t, b, entry), collectTargets(targets, &oldGot)); err != nil {
		t.Fatalf("Process: %v", err)
	}
	return rawGot, oldGot
}

func assertSamePayloads(t *testing.T, raw, old [][]byte) {
	t.Helper()
	for i := range old {
		switch {
		case raw[i] == nil && old[i] == nil:
		case raw[i] == nil || old[i] == nil:
			t.Fatalf("sender %d: raw %v old %v", i, raw[i], old[i])
		case !semanticEqual(t, raw[i], old[i]):
			t.Fatalf("sender %d semantic mismatch:\n raw %s\n old %s", i, raw[i], old[i])
		}
	}
}

// TestProcessRawMatchesProcess is the pipeline oracle: for a matrix of
// senders the streaming handler and the old handler must produce
// semantically equal payloads per sender.
func TestProcessRawMatchesProcess(t *testing.T) {
	fields := []string{"MESSAGE=hello", "PRIORITY=6", "_SYSTEMD_UNIT=foo.service", "_PID=42"}

	infoLevel, err := filter.NewUnitLogLevel("info", []config.UnitLogLevelEntry{{ServiceGlob: "*.service", LogLevel: "INFO"}})
	if err != nil {
		t.Fatal(err)
	}
	warnLevel, err := filter.NewUnitLogLevel("warn", []config.UnitLogLevelEntry{{ServiceGlob: "*.service", LogLevel: "WARNING"}})
	if err != nil {
		t.Fatal(err)
	}
	keepMessage := filter.NewFieldFilter("msg", config.FieldFilterConfig{Whitelist: true, Fields: []string{"MESSAGE", "SYSTEMD_UNIT"}})
	dropPriority := filter.NewFieldFilter("nopri", config.FieldFilterConfig{Whitelist: false, Fields: []string{"PRIORITY"}})

	targets := []SenderTarget{
		{Name: "plain"},
		{Name: "filter", FieldFilter: keepMessage},
		{Name: "extras", ExtraFieldValues: []Field{{Key: "env", Value: "prod"}, {Key: "n", Value: int64(3)}}},
		{Name: "override", ExtraFieldValues: []Field{{Key: "PRIORITY", Value: int64(2)}}},
		{Name: "unit-info", UnitLogLevels: infoLevel},
		{Name: "unit-warn", UnitLogLevels: warnLevel},
		{Name: "same-key-hit", FieldFilter: keepMessage, UnitLogLevels: warnLevel},
		{Name: "same-key-nopri", FieldFilter: dropPriority, UnitLogLevels: warnLevel},
		{Name: "connector", FieldFilter: keepMessage, ExtraFieldValues: []Field{{Key: "connector", Value: "kafka"}}},
	}
	raw, old := runBoth(t, fields, targets)
	assertSamePayloads(t, raw, old)
}

// TestProcessRawRuling1ExtrasContamination locks ruling 1: a cache-miss
// sender with a distinct filter key receives the extras of every earlier
// cache-miss sender, in config order (Python's shared-dict mutation).
func TestProcessRawRuling1ExtrasContamination(t *testing.T) {
	fields := []string{"MESSAGE=hello"}
	f := func(name string) *filter.FieldFilter {
		return filter.NewFieldFilter(name, config.FieldFilterConfig{Whitelist: false, Fields: []string{"_IRRELEVANT"}})
	}
	targets := []SenderTarget{
		{Name: "a", FieldFilter: f("f1"), ExtraFieldValues: []Field{{Key: "a", Value: "1"}}},
		{Name: "b", FieldFilter: f("f2"), ExtraFieldValues: []Field{{Key: "b", Value: "2"}}},
		{Name: "c", FieldFilter: f("f3"), ExtraFieldValues: []Field{{Key: "c", Value: "3"}}},
	}
	raw, old := runBoth(t, fields, targets)
	assertSamePayloads(t, raw, old)

	expect := map[int][]string{0: {"a"}, 1: {"a", "b"}, 2: {"a", "b", "c"}}
	for i, keys := range expect {
		for _, key := range keys {
			if !containsKey(t, raw[i], key) {
				t.Errorf("sender %d payload missing inherited extra %q: %s", i, key, raw[i])
			}
		}
	}
}

// TestProcessRawUnitLevelCacheHitRuling2 locks the ruling-2 emulation: a
// cache hit whose emitting filter kept both keys is decided from the
// record; one whose filter removed a key passes.
func TestProcessRawUnitLevelCacheHitRuling2(t *testing.T) {
	fields := []string{"MESSAGE=hello", "PRIORITY=6", "_SYSTEMD_UNIT=foo.service"}
	infoLevel, err := filter.NewUnitLogLevel("info", []config.UnitLogLevelEntry{{ServiceGlob: "*.service", LogLevel: "INFO"}})
	if err != nil {
		t.Fatal(err)
	}
	warnLevel, err := filter.NewUnitLogLevel("warn", []config.UnitLogLevelEntry{{ServiceGlob: "*.service", LogLevel: "WARNING"}})
	if err != nil {
		t.Fatal(err)
	}
	dropPriority := filter.NewFieldFilter("nopri", config.FieldFilterConfig{Whitelist: false, Fields: []string{"PRIORITY"}})
	keepAll := filter.NewFieldFilter("all", config.FieldFilterConfig{Whitelist: true, Fields: []string{"MESSAGE", "PRIORITY", "SYSTEMD_UNIT"}})

	// (a) keep-all first miss passes at INFO; the second sender reuses the
	// cached payload but its stricter WARNING level drops it.
	raw, old := runBoth(t, fields, []SenderTarget{
		{Name: "a", FieldFilter: keepAll, UnitLogLevels: infoLevel},
		{Name: "b", FieldFilter: keepAll, UnitLogLevels: warnLevel},
	})
	assertSamePayloads(t, raw, old)
	if raw[0] == nil {
		t.Error("sender a should emit")
	}
	if raw[1] != nil {
		t.Errorf("sender b should be dropped by the cache-hit decision, got %s", raw[1])
	}

	// (b) the emitting filter removed PRIORITY: the cache-hit sender
	// passes even with a level that would drop the live entry.
	raw, old = runBoth(t, fields, []SenderTarget{
		{Name: "a", FieldFilter: dropPriority, UnitLogLevels: infoLevel},
		{Name: "b", FieldFilter: dropPriority, UnitLogLevels: warnLevel},
	})
	assertSamePayloads(t, raw, old)
	if raw[1] == nil {
		t.Error("sender b should pass on the cache hit (PRIORITY filtered out)")
	}
}

// containsKey reports whether payload has the given top-level JSON key.
func containsKey(t *testing.T, payload []byte, key string) bool {
	t.Helper()
	var parsed map[string]any
	if err := json.Unmarshal(payload, &parsed); err != nil {
		t.Fatalf("payload not JSON: %v (%s)", err, payload)
	}
	_, ok := parsed[key]
	return ok
}
