// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package journal

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Aiven-Open/journalpump/internal/config"
	"github.com/Aiven-Open/journalpump/internal/filter"
	"github.com/Aiven-Open/journalpump/internal/tagged"
)

// recordingStats captures the Increase calls the handler makes (the
// too_long read_error counter and anything a search emits).
type recordingStats struct {
	incs []metric
}

type metric struct {
	name  string
	value int64
	tags  map[string]string
}

func (r *recordingStats) Increase(name string, value int64, tags map[string]string) {
	r.incs = append(r.incs, metric{name: name, value: value, tags: tags})
}

// collector is one SenderTarget's AddItem sink.
type collector struct {
	items      [][]byte
	cursors    []string
	indexDates [][10]byte
}

func (c *collector) add(item []byte, cursor string, indexDate [10]byte) {
	c.items = append(c.items, item)
	c.cursors = append(c.cursors, cursor)
	c.indexDates = append(c.indexDates, indexDate)
}

func target(ff *filter.FieldFilter, ull *filter.UnitLogLevel, extras []Field, c *collector) SenderTarget {
	return SenderTarget{
		Name:             "s",
		FieldFilter:      ff,
		UnitLogLevels:    ull,
		ExtraFieldValues: extras,
		AddItem:          c.add,
	}
}

// newTestSearcher compiles a single-field search; stats nil (the search
// fast path tolerates it).
func newTestSearcher(t *testing.T, name, field, pattern string) *filter.Searcher {
	t.Helper()
	searches, err := filter.NewSearches([]any{map[string]any{
		"name":   name,
		"fields": map[string]any{field: pattern},
	}})
	if err != nil {
		t.Fatalf("NewSearches: %v", err)
	}
	searcher, err := filter.NewSearcher(searches, 0, nil, tagged.New(nil), nil, nil)
	if err != nil {
		t.Fatalf("NewSearcher: %v", err)
	}
	return searcher
}

// TestProcessBasicPipeline locks the happy path: a cursor-carrying entry
// serializes once per sender, gains an ISO timestamp, and reports its
// bytes as the max serialized size.
func TestProcessBasicPipeline(t *testing.T) {
	c := &collector{}
	h := NewHandler(HandlerConfig{
		ReaderName: "r",
		Stats:      &recordingStats{},
		PumpTags:   tagged.New(nil),
	})
	entry := Entry{
		Cursor: "c1",
		Fields: []Field{
			{Key: "MESSAGE", Value: "hello"},
			{Key: "PRIORITY", Value: int64(6)},
			{Key: "REALTIME_TIMESTAMP", Value: float64(1700000000.5)},
		},
	}
	hasMore, bytesRead, hasBytes, err := h.Process(entry, []SenderTarget{target(nil, nil, nil, c)})
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if !hasMore || !hasBytes {
		t.Fatalf("hasMore=%v hasBytes=%v, want true true", hasMore, hasBytes)
	}
	if len(c.items) != 1 {
		t.Fatalf("items = %d, want 1", len(c.items))
	}
	if c.cursors[0] != "c1" {
		t.Errorf("cursor = %q, want c1", c.cursors[0])
	}
	if len(c.items[0]) != bytesRead {
		t.Errorf("bytesRead = %d, want serialized len %d", bytesRead, len(c.items[0]))
	}
	var parsed map[string]any
	if err := json.Unmarshal(c.items[0], &parsed); err != nil {
		t.Fatalf("payload is not JSON: %v (%s)", err, c.items[0])
	}
	if parsed["MESSAGE"] != "hello" {
		t.Errorf("MESSAGE = %v, want hello", parsed["MESSAGE"])
	}
	if _, ok := parsed["timestamp"]; !ok {
		t.Errorf("payload has no timestamp: %s", c.items[0])
	}
}

// TestProcessCursorlessEntrySkipped locks the "No more journal entries"
// arm: a missing cursor is a (false, None) result with no enqueue.
func TestProcessCursorlessEntrySkipped(t *testing.T) {
	c := &collector{}
	h := NewHandler(HandlerConfig{ReaderName: "r"})
	hasMore, bytesRead, hasBytes, err := h.Process(
		Entry{Fields: []Field{{Key: "MESSAGE", Value: "x"}}},
		[]SenderTarget{target(nil, nil, nil, c)},
	)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if hasMore || hasBytes || bytesRead != 0 {
		t.Errorf("got (%v,%d,%v), want (false,0,false)", hasMore, bytesRead, hasBytes)
	}
	if len(c.items) != 0 {
		t.Errorf("items = %d, want 0", len(c.items))
	}
}

// TestProcessSearchGate runs the searches against the RAW entry: a match
// enqueues, a miss drops with hasMore true (the reader keeps going).
func TestProcessSearchGate(t *testing.T) {
	searcher := newTestSearcher(t, "keep", "MESSAGE", "^ok$")
	h := NewHandler(HandlerConfig{ReaderName: "r", Searcher: searcher})

	c := &collector{}
	hit := Entry{Cursor: "c1", Fields: []Field{{Key: "MESSAGE", Value: "ok"}}}
	hasMore, _, hasBytes, err := h.Process(hit, []SenderTarget{target(nil, nil, nil, c)})
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if !hasMore || !hasBytes || len(c.items) != 1 {
		t.Fatalf("hit: hasMore=%v hasBytes=%v items=%d", hasMore, hasBytes, len(c.items))
	}

	miss := Entry{Cursor: "c2", Fields: []Field{{Key: "MESSAGE", Value: "nope"}}}
	hasMore, bytesRead, hasBytes, err := h.Process(miss, []SenderTarget{target(nil, nil, nil, c)})
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if !hasMore || hasBytes || bytesRead != 0 {
		t.Errorf("miss: got (%v,%d,%v), want (true,0,false)", hasMore, bytesRead, hasBytes)
	}
	if len(c.items) != 1 {
		t.Errorf("miss enqueued: items = %d, want 1", len(c.items))
	}
}

// TestProcessMatchGuards locks check_match at both levels, including the
// pinned unset-match_value-never-matches guard.
func TestProcessMatchGuards(t *testing.T) {
	pumpC := &collector{}
	h := NewHandler(HandlerConfig{
		ReaderName:     "r",
		PumpMatchKey:   "SYSLOG_IDENTIFIER",
		PumpMatchValue: nil,
	})
	entry := Entry{Cursor: "c", Fields: []Field{{Key: "SYSLOG_IDENTIFIER", Value: "svc"}}}
	hasMore, _, hasBytes, err := h.Process(entry, []SenderTarget{target(nil, nil, nil, pumpC)})
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if !hasMore || hasBytes || len(pumpC.items) != 0 {
		t.Errorf("nil match_value: got hasMore=%v hasBytes=%v items=%d, want true false 0",
			hasMore, hasBytes, len(pumpC.items))
	}

	readerC := &collector{}
	h = NewHandler(HandlerConfig{
		ReaderName:       "r",
		ReaderMatchKey:   "SYSLOG_IDENTIFIER",
		ReaderMatchValue: "other",
	})
	hasMore, _, hasBytes, err = h.Process(entry, []SenderTarget{target(nil, nil, nil, readerC)})
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if !hasMore || hasBytes || len(readerC.items) != 0 {
		t.Errorf("mismatched reader value: got hasMore=%v hasBytes=%v items=%d", hasMore, hasBytes, len(readerC.items))
	}
}

// TestProcessUnitLogLevelDrop locks the drop path: a too-verbose entry
// yields nothing (bytes_read 0, not None) and is NOT cached.
func TestProcessUnitLogLevelDrop(t *testing.T) {
	ull, err := filter.NewUnitLogLevel("lvl", []config.UnitLogLevelEntry{{ServiceGlob: "*", LogLevel: "INFO"}})
	if err != nil {
		t.Fatalf("NewUnitLogLevel: %v", err)
	}
	c := &collector{}
	h := NewHandler(HandlerConfig{ReaderName: "r"})
	entry := Entry{Cursor: "c", Fields: []Field{
		{Key: "SYSTEMD_UNIT", Value: "svc.service"},
		{Key: "PRIORITY", Value: int64(7)}, // debug > info → dropped
		{Key: "MESSAGE", Value: "noisy"},
	}}
	hasMore, bytesRead, hasBytes, err := h.Process(entry, []SenderTarget{target(nil, ull, nil, c)})
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	// Python returns (True, 0) here: the drop is not the bytes_read=None
	// arm (that is reserved for skipped and cursorless entries).
	if !hasMore || !hasBytes || bytesRead != 0 {
		t.Errorf("got (%v,%d,%v), want (true,0,true)", hasMore, bytesRead, hasBytes)
	}
	if len(c.items) != 0 {
		t.Errorf("items = %d, want 0", len(c.items))
	}
	if len(h.jsonObjects) != 0 {
		t.Errorf("dropped entry was cached: %v", h.jsonObjects)
	}
}

// TestProcessCacheHitUnitLogLevel locks the cache-hit branch: two
// senders share one field filter (one serialization); the second's
// stricter unit log level is re-checked against the cached bytes and
// drops the entry.
func TestProcessCacheHitUnitLogLevel(t *testing.T) {
	ff := filter.NewFieldFilter("ff", config.FieldFilterConfig{
		Whitelist: true,
		Fields:    []string{"MESSAGE", "SYSTEMD_UNIT", "PRIORITY"},
	})
	loose, err := filter.NewUnitLogLevel("loose", []config.UnitLogLevelEntry{{ServiceGlob: "*", LogLevel: "INFO"}})
	if err != nil {
		t.Fatal(err)
	}
	strict, err := filter.NewUnitLogLevel("strict", []config.UnitLogLevelEntry{{ServiceGlob: "*", LogLevel: "WARNING"}})
	if err != nil {
		t.Fatal(err)
	}
	a, b := &collector{}, &collector{}
	h := NewHandler(HandlerConfig{ReaderName: "r"})
	entry := Entry{Cursor: "c", Fields: []Field{
		{Key: "MESSAGE", Value: "x"},
		{Key: "SYSTEMD_UNIT", Value: "svc.service"},
		{Key: "PRIORITY", Value: int64(6)}, // info: passes warning(4) but fails... 6 > 4 → strict drops
	}}
	hasMore, _, _, err := h.Process(entry, []SenderTarget{
		target(ff, loose, nil, a),
		target(ff, strict, nil, b),
	})
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if !hasMore {
		t.Fatal("hasMore = false")
	}
	if len(a.items) != 1 {
		t.Errorf("loose sender items = %d, want 1", len(a.items))
	}
	if len(b.items) != 0 {
		t.Errorf("strict sender items = %d, want 0 (cache-hit re-check)", len(b.items))
	}
}

// TestProcessExtraFieldContamination locks the cross-sender quirk: the
// processed entry is shared and mutated, so a second sender without
// extras still serializes the first sender's extras.
func TestProcessExtraFieldContamination(t *testing.T) {
	ffA := filter.NewFieldFilter("a", config.FieldFilterConfig{Whitelist: true, Fields: []string{"MESSAGE"}})
	ffB := filter.NewFieldFilter("b", config.FieldFilterConfig{Whitelist: true, Fields: []string{"MESSAGE", "EXTRA"}})
	a, b := &collector{}, &collector{}
	h := NewHandler(HandlerConfig{ReaderName: "r"})
	entry := Entry{Cursor: "c", Fields: []Field{{Key: "MESSAGE", Value: "x"}}}
	if _, _, _, err := h.Process(entry, []SenderTarget{
		target(ffA, nil, []Field{{Key: "EXTRA", Value: "from-a"}}, a),
		target(ffB, nil, nil, b),
	}); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(a.items) != 1 || len(b.items) != 1 {
		t.Fatalf("items = %d %d, want 1 1", len(a.items), len(b.items))
	}
	var parsedB map[string]any
	if err := json.Unmarshal(b.items[0], &parsedB); err != nil {
		t.Fatalf("B payload: %v", err)
	}
	if parsedB["EXTRA"] != "from-a" {
		t.Errorf("B EXTRA = %v, want from-a (shared-entry contamination)", parsedB["EXTRA"])
	}
}

// TestProcessSharedFilterCacheLockedToFirst locks the serialization-cache
// quirk: two senders sharing one field-filter NAME reuse the first's
// cached bytes verbatim, so the second's own extra_field_values never
// reach its payload.
func TestProcessSharedFilterCacheLockedToFirst(t *testing.T) {
	ff := filter.NewFieldFilter("shared", config.FieldFilterConfig{
		Whitelist: true,
		Fields:    []string{"MESSAGE", "EXTRA"},
	})
	a, b := &collector{}, &collector{}
	h := NewHandler(HandlerConfig{ReaderName: "r"})
	entry := Entry{Cursor: "c", Fields: []Field{{Key: "MESSAGE", Value: "x"}}}
	if _, _, _, err := h.Process(entry, []SenderTarget{
		target(ff, nil, []Field{{Key: "EXTRA", Value: "from-a"}}, a),
		target(ff, nil, []Field{{Key: "EXTRA", Value: "from-b"}}, b),
	}); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(a.items) != 1 || len(b.items) != 1 {
		t.Fatalf("items = %d %d, want 1 1", len(a.items), len(b.items))
	}
	for name, items := range map[string][][]byte{"a": a.items, "b": b.items} {
		var parsed map[string]any
		if err := json.Unmarshal(items[0], &parsed); err != nil {
			t.Fatalf("%s payload: %v", name, err)
		}
		if parsed["EXTRA"] != "from-a" {
			t.Errorf("%s EXTRA = %v, want from-a (first serialization cached)", name, parsed["EXTRA"])
		}
	}
}

// TestProcessTruncation locks the too-large path: the entry is replaced
// by the error record, the counter fires once per handler, and the
// record IS cached for the next sender.
func TestProcessTruncation(t *testing.T) {
	stats := &recordingStats{}
	c := &collector{}
	h := NewHandler(HandlerConfig{
		ReaderName: "r",
		Stats:      stats,
		PumpTags:   tagged.New(map[string]string{"base": "v"}),
	})
	big := strings.Repeat("a", MaxKafkaMessageSize+10)
	entry := Entry{Cursor: "c", Fields: []Field{{Key: "MESSAGE", Value: big}}}
	hasMore, bytesRead, hasBytes, err := h.Process(entry, []SenderTarget{target(nil, nil, nil, c)})
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if !hasMore || !hasBytes || len(c.items) != 1 {
		t.Fatalf("got hasMore=%v hasBytes=%v items=%d", hasMore, hasBytes, len(c.items))
	}
	var record map[string]any
	if err := json.Unmarshal(c.items[0], &record); err != nil {
		t.Fatalf("truncation record: %v", err)
	}
	if record["error"] == nil || record["partial_data"] == nil {
		t.Errorf("record = %v, want error + partial_data", record)
	}
	if _, ok := record["timestamp"]; !ok {
		t.Errorf("record has no timestamp: %s", c.items[0])
	}
	if len(stats.incs) != 1 {
		t.Fatalf("stats increases = %d, want 1", len(stats.incs))
	}
	got := stats.incs[0]
	if got.name != "journal.read_error" || got.value != 1 {
		t.Errorf("metric = %+v, want journal.read_error=1", got)
	}
	if got.tags["error"] != "too_long" || got.tags["reader"] != "r" || got.tags["base"] != "v" {
		t.Errorf("tags = %v, want error=too_long reader=r base=v", got.tags)
	}
	if bytesRead != len(c.items[0]) {
		t.Errorf("bytesRead = %d, want %d (the truncated record)", bytesRead, len(c.items[0]))
	}
}

// TestProcessTruncationReportsOnce locks error_reported across senders:
// a second target reuses the cache and never re-counters.
func TestProcessTruncationReportsOnce(t *testing.T) {
	stats := &recordingStats{}
	a, b := &collector{}, &collector{}
	h := NewHandler(HandlerConfig{ReaderName: "r", Stats: stats, PumpTags: tagged.New(nil)})
	big := strings.Repeat("a", MaxKafkaMessageSize+10)
	entry := Entry{Cursor: "c", Fields: []Field{{Key: "MESSAGE", Value: big}}}
	if _, _, _, err := h.Process(entry, []SenderTarget{
		target(nil, nil, nil, a),
		target(nil, nil, nil, b),
	}); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(stats.incs) != 1 {
		t.Errorf("stats increases = %d, want 1 (once per handler)", len(stats.incs))
	}
}

// TestCheckMatch guards the direct helper contract.
func TestCheckMatch(t *testing.T) {
	fields := []Field{{Key: "PRIORITY", Value: int64(6)}, {Key: "SYSLOG_IDENTIFIER", Value: "svc"}}
	cases := []struct {
		name  string
		key   string
		value any
		want  bool
	}{
		{"unset key passes", "", nil, true},
		{"nil value never matches", "PRIORITY", nil, false},
		{"numeric equality", "PRIORITY", int64(6), true},
		{"numeric inequality", "PRIORITY", int64(7), false},
		{"string equality", "SYSLOG_IDENTIFIER", "svc", true},
		{"string inequality", "SYSLOG_IDENTIFIER", "other", false},
		{"missing field", "NOPE", "x", false},
		{"nil value matches missing field", "NOPE", nil, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := checkMatch(fields, tc.key, tc.value); got != tc.want {
				t.Errorf("checkMatch(%q, %v) = %v, want %v", tc.key, tc.value, got, tc.want)
			}
		})
	}
}

// TestPythonEqual locks the Python == semantics the match guard and the
// secret/extra plumbing depend on.
func TestPythonEqual(t *testing.T) {
	cases := []struct {
		a, b any
		want bool
	}{
		{int64(1), int64(1), true},
		{int64(1), float64(1), true},
		{true, int64(1), true},
		{false, int64(0), true},
		{"x", "x", true},
		{"x", "y", false},
		{int64(1), "1", false},
		{nil, nil, true},
		{nil, "x", false},
		{"x", nil, false},
		{[]any{"a", int64(1)}, []any{"a", int64(1)}, true},
		{[]any{"a"}, []any{"a", "b"}, false},
		{[]Field{{Key: "k", Value: "v"}}, []Field{{Key: "k", Value: "v"}}, true},
		{[]Field{{Key: "k", Value: "v"}}, []Field{{Key: "k", Value: "w"}}, false},
		{[]byte("x"), []byte("x"), true},
	}
	for _, tc := range cases {
		if got := PythonEqual(tc.a, tc.b); got != tc.want {
			t.Errorf("PythonEqual(%#v, %#v) = %v, want %v", tc.a, tc.b, got, tc.want)
		}
	}
}

// TestProcessFallbackTimestamp locks the absent-REALTIME_TIMESTAMP arm:
// the injected clock's now() becomes the entry timestamp.
func TestProcessFallbackTimestamp(t *testing.T) {
	fixed := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	c := &collector{}
	h := NewHandler(HandlerConfig{ReaderName: "r", Now: func() time.Time { return fixed }})
	entry := Entry{Cursor: "c", Fields: []Field{{Key: "MESSAGE", Value: "x"}}}
	if _, _, _, err := h.Process(entry, []SenderTarget{target(nil, nil, nil, c)}); err != nil {
		t.Fatalf("Process: %v", err)
	}
	var parsed map[string]any
	if err := json.Unmarshal(c.items[0], &parsed); err != nil {
		t.Fatal(err)
	}
	if got := parsed["timestamp"]; got != "2026-01-02T03:04:05" {
		t.Errorf("timestamp = %v, want 2026-01-02T03:04:05", got)
	}
}

// TestProcessWhitelistEmptiesToBraces locks the ordering that the review
// corrected: Python checks emptiness BEFORE the field filter, then
// serializes and enqueues {} when a whitelist removes every field
// (journalpump.py:777-784).
func TestProcessWhitelistEmptiesToBraces(t *testing.T) {
	ff := filter.NewFieldFilter("ff", config.FieldFilterConfig{
		Whitelist: true,
		Fields:    []string{"NOT_PRESENT"},
	})
	c := &collector{}
	h := NewHandler(HandlerConfig{ReaderName: "r"})
	entry := Entry{Cursor: "c", Fields: []Field{
		{Key: "MESSAGE", Value: "x"},
		{Key: "PRIORITY", Value: int64(6)},
	}}
	hasMore, bytesRead, hasBytes, err := h.Process(entry, []SenderTarget{target(ff, nil, nil, c)})
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if !hasMore || !hasBytes {
		t.Fatalf("got hasMore=%v hasBytes=%v, want true true", hasMore, hasBytes)
	}
	if len(c.items) != 1 {
		t.Fatalf("items = %d, want 1 ({} is enqueued, not dropped)", len(c.items))
	}
	if got := string(c.items[0]); got != "{}" {
		t.Errorf("payload = %s, want {}", got)
	}
	if c.cursors[0] != "c" {
		t.Errorf("cursor = %q, want c (the sender advances)", c.cursors[0])
	}
	if bytesRead != 2 {
		t.Errorf("bytesRead = %d, want 2 (len(\"{}\"))", bytesRead)
	}
}

// TestProcessMatchValueNilMatchesAbsent locks the corrected check_match:
// a nil match_value matches an entry MISSING the key (Python's
// entry.get(match_key) == None), at both the pump and reader levels.
func TestProcessMatchValueNilMatchesAbsent(t *testing.T) {
	pumpC := &collector{}
	h := NewHandler(HandlerConfig{
		ReaderName:     "r",
		PumpMatchKey:   "SYSLOG_IDENTIFIER",
		PumpMatchValue: nil,
	})
	// Entry has NO SYSLOG_IDENTIFIER field.
	entry := Entry{Cursor: "c", Fields: []Field{{Key: "MESSAGE", Value: "x"}}}
	hasMore, _, hasBytes, err := h.Process(entry, []SenderTarget{target(nil, nil, nil, pumpC)})
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if !hasMore || !hasBytes || len(pumpC.items) != 1 {
		t.Errorf("pump nil match_value absent key: got hasMore=%v hasBytes=%v items=%d, want true true 1",
			hasMore, hasBytes, len(pumpC.items))
	}

	readerC := &collector{}
	h = NewHandler(HandlerConfig{
		ReaderName:       "r",
		ReaderMatchKey:   "SYSLOG_IDENTIFIER",
		ReaderMatchValue: nil,
	})
	hasMore, _, hasBytes, err = h.Process(entry, []SenderTarget{target(nil, nil, nil, readerC)})
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if !hasMore || !hasBytes || len(readerC.items) != 1 {
		t.Errorf("reader nil match_value absent key: got hasMore=%v hasBytes=%v items=%d, want true true 1",
			hasMore, hasBytes, len(readerC.items))
	}

	// A present non-null value still never equals nil.
	readerC2 := &collector{}
	h = NewHandler(HandlerConfig{
		ReaderName:       "r",
		ReaderMatchKey:   "SYSLOG_IDENTIFIER",
		ReaderMatchValue: nil,
	})
	present := Entry{Cursor: "c", Fields: []Field{{Key: "SYSLOG_IDENTIFIER", Value: "svc"}}}
	if _, _, _, err := h.Process(present, []SenderTarget{target(nil, nil, nil, readerC2)}); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(readerC2.items) != 0 {
		t.Errorf("present value vs nil match_value: items = %d, want 0", len(readerC2.items))
	}
}

// TestPythonEqualLargeIntegers locks exact integer matching: adjacent
// int64 values above 2^53 stay distinct (no float64 collapse), and int
// vs float equality never rounds.
func TestPythonEqualLargeIntegers(t *testing.T) {
	const a = int64(1) << 53 // 9007199254740992
	const b = a + 1          // 9007199254740993
	if PythonEqual(a, b) {
		t.Errorf("PythonEqual(%d, %d) = true, want false (distinct ints above 2^53)", a, b)
	}
	if PythonEqual(a, float64(a)) != true {
		t.Errorf("PythonEqual(%d, float64(%d)) = false, want true", a, a)
	}
	// float64(b) rounds to float64(a) == 2^53; Python's int.__eq__ must
	// not treat b as equal to that float.
	if PythonEqual(b, float64(b)) {
		t.Errorf("PythonEqual(%d, float64(%d)) = true, want false (float rounds)", b, b)
	}
	// json.Number integer literals compare exactly too.
	if PythonEqual(json.Number("9007199254740993"), int64(9007199254740993)) != true {
		t.Error("json.Number int64 literal should equal the same int64")
	}
	if PythonEqual(json.Number("9007199254740993"), int64(9007199254740992)) {
		t.Error("json.Number literal collapsed two distinct large ints")
	}
}
