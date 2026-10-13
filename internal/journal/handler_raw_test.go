// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package journal

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Aiven-Open/journalpump/internal/config"
	"github.com/Aiven-Open/journalpump/internal/filter"
)

func fieldFilter(name string) *filter.FieldFilter {
	return filter.NewFieldFilter(name, config.FieldFilterConfig{Whitelist: false, Fields: []string{"_IRRELEVANT"}})
}

func parsePayload(t *testing.T, payload []byte) map[string]any {
	t.Helper()
	var parsed map[string]any
	if err := json.Unmarshal(payload, &parsed); err != nil {
		t.Fatalf("payload not JSON: %v (%s)", err, payload)
	}
	return parsed
}

// collectTargets gives each sender an AddItem that records its payload.
func collectTargets(targets []SenderTarget, got *[][]byte) []SenderTarget {
	out := make([]SenderTarget, len(targets))
	for i := range targets {
		target := targets[i]
		idx := i
		target.AddItem = func(item []byte, _ string, _ [10]byte) {
			(*got)[idx] = append([]byte(nil), item...)
		}
		out[i] = target
	}
	return out
}

// TestProcessRawIndexDateSidecar pins that the raw pipeline threads the
// Elasticsearch index date from the emitter to AddItem (design §2.4): the
// synthesised timestamp's first 10 bytes when the payload carries it, and
// zero when the sender's filter removes the timestamp key.
func TestProcessRawIndexDateSidecar(t *testing.T) {
	cursor := "s=x;i=1a2b;b=1;m=2;t=3;x=4"
	realtime := uint64(1700000000500000)
	timestamp, err := TimestampFromRealtime(float64(realtime) / 1e6)
	if err != nil {
		t.Fatalf("TimestampFromRealtime: %v", err)
	}
	want := pythonISOFormat(timestamp)[:10]

	capture := func(t *testing.T, filter *filter.FieldFilter) [10]byte {
		t.Helper()
		b := buildBatch(t, realtime, cursor, "MESSAGE=hello", "PRIORITY=6")
		var got [10]byte
		calls := 0
		tgt := SenderTarget{Name: "s", FieldFilter: filter, AddItem: func(_ []byte, _ string, id [10]byte) {
			got = id
			calls++
		}}
		if _, _, _, err := NewHandler(HandlerConfig{}).ProcessRaw(b, &b.Entries[0], []SenderTarget{tgt}); err != nil {
			t.Fatalf("ProcessRaw: %v", err)
		}
		if calls != 1 {
			t.Fatalf("AddItem calls = %d, want 1", calls)
		}
		return got
	}

	if got := capture(t, nil); string(got[:]) != want {
		t.Fatalf("index date = %q, want %q", got[:], want)
	}
	if got := capture(t, blacklistFilter(t, "timestamp")); got != ([10]byte{}) {
		t.Fatalf("index date = %q, want zero when the filter drops timestamp", got[:])
	}
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

// runBothSecret is runBoth with secret filters wired into both handlers;
// it returns the raw-side secret-match count.
func runBothSecret(t *testing.T, fields []string, targets []SenderTarget, filters []*filter.SecretFilter, metrics bool) (raw, old [][]byte, rawMatches int) {
	t.Helper()
	cursor := "s=x;i=1a2b;b=1;m=2;t=3;x=4"
	b := buildBatch(t, 1700000000500000, cursor, fields...)
	entry := &b.Entries[0]

	rawGot := make([][]byte, len(targets))
	rawHandler := NewHandler(HandlerConfig{
		SecretFilters:         filters,
		SecretFilterMetrics:   metrics,
		NoteSecretFilterMatch: func() { rawMatches++ },
	})
	if _, _, _, err := rawHandler.ProcessRaw(b, entry, collectTargets(targets, &rawGot)); err != nil {
		t.Fatalf("ProcessRaw: %v", err)
	}

	oldGot := make([][]byte, len(targets))
	oldHandler := NewHandler(HandlerConfig{
		SecretFilters:         filters,
		SecretFilterMetrics:   metrics,
		NoteSecretFilterMatch: func() {},
	})
	if _, _, _, err := oldHandler.Process(rawToEntry(t, b, entry), collectTargets(targets, &oldGot)); err != nil {
		t.Fatalf("Process: %v", err)
	}
	return rawGot, oldGot, rawMatches
}

// TestProcessRawExtrasLastWins locks item 2: overlapping extra keys
// across distinct filter keys follow Python's successive data.update —
// the LAST assignment wins the value, the FIRST insertion keeps the
// position, and the key appears once.
func TestProcessRawExtrasLastWins(t *testing.T) {
	fields := []string{"MESSAGE=hello", "dup=raw"}
	raw, old := runBoth(t, fields, []SenderTarget{
		{Name: "a", FieldFilter: fieldFilter("f1"), ExtraFieldValues: []Field{{Key: "dup", Value: "a"}, {Key: "only_a", Value: "1"}}},
		{Name: "b", FieldFilter: fieldFilter("f2"), ExtraFieldValues: []Field{{Key: "dup", Value: "b"}}},
	})
	assertSamePayloads(t, raw, old)
	parsed := parsePayload(t, raw[1])
	if parsed["dup"] != "b" {
		t.Errorf("sender b dup = %#v, want \"b\" (last update wins)", parsed["dup"])
	}
	if parsed["only_a"] != "1" {
		t.Errorf("sender b only_a = %#v, want \"1\" (contaminated)", parsed["only_a"])
	}
	if bytes.Count(raw[1], []byte(`"dup"`)) != 1 {
		t.Errorf("sender b payload carries dup %d times: %s", bytes.Count(raw[1], []byte(`"dup"`)), raw[1])
	}
}

// TestProcessRawTimestampResetAcrossMisses locks item 3: Python assigns
// data["timestamp"] on EVERY cache miss, so an earlier sender's timestamp
// extra is overwritten for a later miss, and an earlier REALTIME_TIMESTAMP
// extra feeds the later miss's timestamp computation.
func TestProcessRawTimestampResetAcrossMisses(t *testing.T) {
	fields := []string{"MESSAGE=hello", "PRIORITY=6", "_SYSTEMD_UNIT=foo.service"}

	t.Run("timestamp extra reset", func(t *testing.T) {
		raw, old := runBoth(t, fields, []SenderTarget{
			{Name: "a", FieldFilter: fieldFilter("f1"), ExtraFieldValues: []Field{{Key: "timestamp", Value: "1999-01-01T00:00:00"}}},
			{Name: "b", FieldFilter: fieldFilter("f2")},
		})
		assertSamePayloads(t, raw, old)
		if got := parsePayload(t, raw[0])["timestamp"]; got != "1999-01-01T00:00:00" {
			t.Errorf("sender a timestamp = %#v, want its own extra", got)
		}
		got, _ := parsePayload(t, raw[1])["timestamp"].(string)
		if strings.HasPrefix(got, "1999-") {
			t.Errorf("sender b timestamp = %q, want the recomputed realtime timestamp", got)
		}
		if !strings.HasPrefix(got, "2023-11-14") {
			t.Errorf("sender b timestamp = %q, want 2023-11-14T...", got)
		}
	})

	t.Run("realtime override feeds later timestamp", func(t *testing.T) {
		raw, old := runBoth(t, fields, []SenderTarget{
			{Name: "a", FieldFilter: fieldFilter("g1"), ExtraFieldValues: []Field{{Key: "REALTIME_TIMESTAMP", Value: float64(0)}}},
			{Name: "b", FieldFilter: fieldFilter("g2")},
		})
		assertSamePayloads(t, raw, old)
		if got := parsePayload(t, raw[1])["REALTIME_TIMESTAMP"]; got != float64(0) {
			t.Errorf("sender b REALTIME_TIMESTAMP = %#v, want 0", got)
		}
		got, _ := parsePayload(t, raw[1])["timestamp"].(string)
		if !strings.HasPrefix(got, "1970-01-01") {
			t.Errorf("sender b timestamp = %q, want 1970-01-01T... (from the overridden realtime)", got)
		}
	})
}

// TestProcessRawTruncationDecisionRecord locks item 4: a cache-hit sender
// whose stricter unit level would drop the live entry must PASS on a
// truncated payload, because Python parses the cached truncation record
// (which carries no PRIORITY/SYSTEMD_UNIT).
func TestProcessRawTruncationDecisionRecord(t *testing.T) {
	big := strings.Repeat("x", MaxKafkaMessageSize+100)
	fields := []string{"MESSAGE=" + big, "PRIORITY=7", "_SYSTEMD_UNIT=foo.service"}
	keepAll := filter.NewFieldFilter("all", config.FieldFilterConfig{Whitelist: true, Fields: []string{"MESSAGE", "PRIORITY", "SYSTEMD_UNIT", "timestamp"}})
	warnLevel, err := filter.NewUnitLogLevel("warn", []config.UnitLogLevelEntry{{ServiceGlob: "*.service", LogLevel: "WARNING"}})
	if err != nil {
		t.Fatal(err)
	}
	raw, old := runBoth(t, fields, []SenderTarget{
		{Name: "a", FieldFilter: keepAll},
		{Name: "b", FieldFilter: keepAll, UnitLogLevels: warnLevel},
	})
	assertSamePayloads(t, raw, old)
	if raw[1] == nil {
		t.Error("sender b should pass on the truncated cache hit (no PRIORITY in the payload)")
	}
}

// TestSecretFilterEmptyRedaction locks the cat-1 secret leak: a secret
// filter that redacts the WHOLE MESSAGE to an empty string must emit
// "MESSAGE": "" — never fall through to the unredacted original.
func TestSecretFilterEmptyRedaction(t *testing.T) {
	filters, warnings, err := filter.NewSecretFilters([]config.SecretFilterConfig{
		{Pattern: `^.*$`, Replacement: ``},
	})
	if err != nil || len(warnings) != 0 {
		t.Fatalf("NewSecretFilters: %v %v", err, warnings)
	}
	raw, old, note := runBothSecret(t, []string{"MESSAGE=top-secret", "PRIORITY=6"}, []SenderTarget{{Name: "a"}}, filters, true)
	assertSamePayloads(t, raw, old)
	if raw[0] == nil {
		t.Fatal("raw payload is nil")
	}
	var parsed map[string]any
	if err := json.Unmarshal(raw[0], &parsed); err != nil {
		t.Fatalf("payload not JSON: %v (%s)", err, raw[0])
	}
	if got, present := parsed["MESSAGE"]; !present || got != "" {
		t.Fatalf("MESSAGE = %#v (present %v), want the empty redaction", got, present)
	}
	if bytes.Contains(raw[0], []byte("top-secret")) {
		t.Fatalf("payload leaks the secret: %s", raw[0])
	}
	if note != 1 {
		t.Fatalf("secret_filter_matches = %d, want 1", note)
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
