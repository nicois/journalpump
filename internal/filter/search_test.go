// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package filter_test

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Aiven-Open/journalpump/internal/filter"
	"github.com/Aiven-Open/journalpump/internal/tagged"
)

// recordingStats captures the metrics calls PerformSearches makes.
type recordingStats struct {
	increases []metricCall
	gauges    []metricCall
}

type metricCall struct {
	metric string
	value  float64
	tags   map[string]string
}

func (r *recordingStats) Increase(metric string, _ int64, tags map[string]string) {
	r.increases = append(r.increases, metricCall{metric: metric, tags: tags})
}

func (r *recordingStats) Gauge(metric string, value float64, tags map[string]string) {
	r.gauges = append(r.gauges, metricCall{metric: metric, value: value, tags: tags})
}

func newTestSearcher(t *testing.T, raw []any, stats filter.Stats) *filter.Searcher {
	t.Helper()
	searches, err := filter.NewSearches(raw)
	if err != nil {
		t.Fatalf("NewSearches: %v", err)
	}
	searcher, err := filter.NewSearcher(searches, 0.001, stats, tagged.New(map[string]string{"reader": "r"}), nil, nil)
	if err != nil {
		t.Fatalf("NewSearcher: %v", err)
	}
	return searcher
}

func mustDecodeUTF8(t *testing.T, s string) []byte {
	t.Helper()
	return []byte(s)
}

// TestPerformSearches ports the perform_searches behaviors exercised by
// test/test_journalpump.py plus the field-iteration and value quirks of
// journalpump.py:579-640.
func TestPerformSearches(t *testing.T) {
	t.Parallel()
	t.Run("match and no match", func(t *testing.T) {
		t.Parallel()
		stats := &recordingStats{}
		searcher := newTestSearcher(t, []any{
			map[string]any{
				"name": "service_stop",
				"fields": map[string]any{
					"MESSAGE": `Stopped target (?P<target>.+)\.`,
				},
			},
		}, stats)
		if !searcher.PerformSearches(map[string]any{"MESSAGE": "Stopped target myunit.service."}) {
			t.Fatal("want match")
		}
		if searcher.PerformSearches(map[string]any{"MESSAGE": "unrelated"}) {
			t.Error("want no match")
		}
		if len(stats.increases) != 1 {
			t.Fatalf("increases = %v want one", stats.increases)
		}
		if stats.increases[0].metric != "service_stop" {
			t.Errorf("increase metric = %q", stats.increases[0].metric)
		}
		if stats.increases[0].tags["target"] != "myunit.service" {
			t.Errorf("target tag = %q", stats.increases[0].tags["target"])
		}
		if stats.increases[0].tags["reader"] != "r" {
			t.Errorf("reader base tag missing: %v", stats.increases[0].tags)
		}
		search := searcher.Searches()[0]
		if search.Hits() != 1 {
			t.Errorf("hits = %d want 1", search.Hits())
		}
	})

	t.Run("missing and empty fields do not match", func(t *testing.T) {
		t.Parallel()
		stats := &recordingStats{}
		searcher := newTestSearcher(t, []any{
			map[string]any{"name": "s", "fields": map[string]any{"MESSAGE": "x"}},
		}, stats)
		if searcher.PerformSearches(map[string]any{}) {
			t.Error("missing field must not match")
		}
		if searcher.PerformSearches(map[string]any{"MESSAGE": ""}) {
			t.Error("empty field must not match (Python: `if not line`)")
		}
		if searcher.PerformSearches(map[string]any{"MESSAGE": nil}) {
			t.Error("nil field must not match")
		}
		if len(stats.increases) != 0 {
			t.Errorf("unexpected increases: %v", stats.increases)
		}
	})

	t.Run("int priority renders via str", func(t *testing.T) {
		t.Parallel()
		stats := &recordingStats{}
		searcher := newTestSearcher(t, []any{
			map[string]any{"name": "s", "fields": map[string]any{"PRIORITY": `^6$`}},
		}, stats)
		if !searcher.PerformSearches(map[string]any{"PRIORITY": int64(6)}) {
			t.Error("int64 6 must match ^6$")
		}
		if searcher.PerformSearches(map[string]any{"PRIORITY": int64(0)}) {
			t.Error("int64 0 renders '0' and must not match ^6$")
		}
	})

	t.Run("bool renders via str", func(t *testing.T) {
		t.Parallel()
		stats := &recordingStats{}
		searcher := newTestSearcher(t, []any{
			map[string]any{"name": "s", "fields": map[string]any{"F": `^True$`}},
		}, stats)
		if !searcher.PerformSearches(map[string]any{"F": true}) {
			t.Error("bool true must render 'True' (Python str(True))")
		}
	})

	t.Run("bytes decode with per-call cache and failure", func(t *testing.T) {
		t.Parallel()
		stats := &recordingStats{}
		searcher := newTestSearcher(t, []any{
			map[string]any{"name": "s", "fields": map[string]any{
				"MESSAGE": `hello`,
				"OTHER":   `world`,
			}},
		}, stats)
		// Both fields must match after best-effort decode.
		if !searcher.PerformSearches(map[string]any{
			"MESSAGE": mustDecodeUTF8(t, "hello"),
			"OTHER":   mustDecodeUTF8(t, "world"),
		}) {
			t.Error("decoded bytes fields must match")
		}
		// Undecodable bytes: no match (Python UnicodeDecodeError branch).
		if searcher.PerformSearches(map[string]any{"MESSAGE": []byte{0xff, 0xfe}}) {
			t.Error("invalid utf-8 bytes must not match")
		}
		// Empty bytes are falsy in Python (`if not line`).
		if searcher.PerformSearches(map[string]any{"MESSAGE": []byte{}}) {
			t.Error("empty bytes must not match")
		}
	})

	t.Run("all fields must match", func(t *testing.T) {
		t.Parallel()
		stats := &recordingStats{}
		searcher := newTestSearcher(t, []any{
			map[string]any{"name": "s", "fields": map[string]any{
				"MESSAGE": `a`,
				"OTHER":   `b`,
			}},
		}, stats)
		if searcher.PerformSearches(map[string]any{"MESSAGE": "a", "OTHER": "x"}) {
			t.Error("partial field match must not count")
		}
	})

	t.Run("field order sorted non-MESSAGE then MESSAGE", func(t *testing.T) {
		t.Parallel()
		// The iteration order is observable through the slow-regex gauge
		// emission order (Python rebuilds the fields dict sorted with
		// MESSAGE last, journalpump.py:520-524): make every field slow and
		// assert the gauges arrive alpha, zeta, MESSAGE.
		stats := &recordingStats{}
		searcher := newTestSearcher(t, []any{
			map[string]any{
				"name": "s",
				"fields": map[string]any{
					"MESSAGE": `m`,
					"zeta":    `z`,
					"alpha":   `a`,
				},
			},
		}, stats)
		current := time.Now()
		searcher.Now = func() time.Time {
			current = current.Add(10 * time.Millisecond)
			return current
		}
		if !searcher.PerformSearches(map[string]any{"MESSAGE": "m", "zeta": "z", "alpha": "a"}) {
			t.Fatal("want match")
		}
		if len(stats.gauges) != 3 {
			t.Fatalf("gauges = %v want three (one per field)", stats.gauges)
		}
		// The gauge's regex tag carries the pattern; alpha/zeta/MESSAGE
		// order with distinct patterns proves the field order.
		wantOrder := []string{"a", "z", "m"}
		for i, want := range wantOrder {
			if got := stats.gauges[i].tags["regex"]; got != want {
				t.Errorf("gauge %d regex tag = %q want %q", i, got, want)
			}
		}
	})

	t.Run("static tags override groupdict", func(t *testing.T) {
		t.Parallel()
		stats := &recordingStats{}
		searcher := newTestSearcher(t, []any{
			map[string]any{
				"name":   "s",
				"fields": map[string]any{"MESSAGE": `(?P<who>.+)`},
				"tags":   map[string]any{"who": "static", "extra": "v"},
			},
		}, stats)
		if !searcher.PerformSearches(map[string]any{"MESSAGE": "bob"}) {
			t.Fatal("want match")
		}
		if stats.increases[0].tags["who"] != "static" {
			t.Errorf("static tag must overwrite groupdict: %v", stats.increases[0].tags)
		}
		if stats.increases[0].tags["extra"] != "v" {
			t.Errorf("missing static tag: %v", stats.increases[0].tags)
		}
	})

	t.Run("tag values are sanitized for metrics", func(t *testing.T) {
		t.Parallel()
		stats := &recordingStats{}
		searcher := newTestSearcher(t, []any{
			map[string]any{
				"name":   "s",
				"fields": map[string]any{"MESSAGE": `(?P<host>.+)`},
			},
		}, stats)
		if !searcher.PerformSearches(map[string]any{"MESSAGE": "localhost:1234"}) {
			t.Fatal("want match")
		}
		if stats.increases[0].tags["host"] != "localhost_1234" {
			t.Errorf("tag value not sanitized: %v", stats.increases[0].tags)
		}
	})
}

// TestTagCallablesRunInConfigOrder locks the Python visibility rule:
// perform_searches evaluates the tags dict in config order
// (journalpump.py:629-634), so a callable whose args[0] names an earlier
// static tag sees that value — and when the same search's tags run in
// the reverse order, the earlier tag is not yet set (Python KeyError →
// sanctioned crash-free "").
func TestTagCallablesRunInConfigOrder(t *testing.T) {
	t.Parallel()
	newSearcher := func(t *testing.T, tagOrder []string) (*recordingStats, *filter.Searcher) {
		t.Helper()
		searches, err := filter.NewSearches([]any{
			map[string]any{
				"name":   "s",
				"fields": map[string]any{"MESSAGE": `ip`},
				"tags": map[string]any{
					"gateway": "10.0.0.1",
					"geo":     "ip_to_geohash(gateway,5)",
				},
			},
		}, tagOrder)
		if err != nil {
			t.Fatalf("NewSearches: %v", err)
		}
		stats := &recordingStats{}
		searcher, err := filter.NewSearcher(searches, 0.001, stats, tagged.New(map[string]string{"reader": "r"}), nil,
			map[string]filter.TagFunc{
				"ip_to_geohash": func(tags map[string]string, args []string) string {
					return "geo:" + tags[args[0]]
				},
			})
		if err != nil {
			t.Fatalf("NewSearcher: %v", err)
		}
		return stats, searcher
	}

	t.Run("document order sees the earlier tag", func(t *testing.T) {
		t.Parallel()
		stats, searcher := newSearcher(t, []string{"gateway", "geo"})
		if !searcher.PerformSearches(map[string]any{"MESSAGE": "ip"}) {
			t.Fatal("want match")
		}
		// The callable produced "geo:10.0.0.1"; the metrics path
		// sanitizes ":" (unsafe value class) to "_".
		if got := stats.increases[0].tags["geo"]; got != "geo_10.0.0.1" {
			t.Errorf("geo tag = %q want geo_10.0.0.1", got)
		}
	})

	t.Run("reverse order loses visibility like Python", func(t *testing.T) {
		t.Parallel()
		stats, searcher := newSearcher(t, []string{"geo", "gateway"})
		if !searcher.PerformSearches(map[string]any{"MESSAGE": "ip"}) {
			t.Fatal("want match")
		}
		// The callable produced "geo:"; sanitization drops the trailing
		// run of unsafe characters (Python: strip of the same class).
		if got := stats.increases[0].tags["geo"]; got != "geo" {
			t.Errorf("geo tag = %q want geo (gateway not set yet)", got)
		}
	})
}

// TestPerformSearchesSlowRegex locks the slow-regex gauge: a search
// slower than threshold_for_metric_emit emits
// journal.perform_search_regex_duration with the regex pattern tag.
func TestPerformSearchesSlowRegex(t *testing.T) {
	t.Parallel()
	stats := &recordingStats{}
	searcher := newTestSearcher(t, []any{
		map[string]any{"name": "s", "fields": map[string]any{"MESSAGE": `needle`}},
	}, stats)
	// Fake clock: each Now call advances 10ms, so the measured duration
	// is 10ms > threshold 1ms.
	current := time.Now()
	searcher.Now = func() time.Time {
		current = current.Add(10 * time.Millisecond)
		return current
	}
	if !searcher.PerformSearches(map[string]any{"MESSAGE": "needle"}) {
		t.Fatal("want match")
	}
	if len(stats.gauges) != 1 {
		t.Fatalf("gauges = %v want one", stats.gauges)
	}
	if stats.gauges[0].metric != "journal.perform_search_regex_duration" {
		t.Errorf("gauge metric = %q", stats.gauges[0].metric)
	}
	if stats.gauges[0].value <= 0 {
		t.Errorf("gauge value = %v", stats.gauges[0].value)
	}
	if stats.gauges[0].tags["regex"] != "needle" {
		t.Errorf("gauge regex tag = %q", stats.gauges[0].tags["regex"])
	}
	if stats.gauges[0].tags["reader"] != "r" {
		t.Errorf("gauge base tags missing: %v", stats.gauges[0].tags)
	}

	// Below the threshold: no gauge.
	stats = &recordingStats{}
	searcher = newTestSearcher(t, []any{
		map[string]any{"name": "s", "fields": map[string]any{"MESSAGE": `needle`}},
	}, stats)
	searcher.Now = func() time.Time { return time.Time{} } // zero duration
	if !searcher.PerformSearches(map[string]any{"MESSAGE": "needle"}) {
		t.Fatal("want match")
	}
	if len(stats.gauges) != 0 {
		t.Errorf("fast regex emitted gauge: %v", stats.gauges)
	}
}

// TestPerformSearchesNoStats verifies the `if self.stats:` guards.
func TestPerformSearchesNoStats(t *testing.T) {
	t.Parallel()
	searcher := newTestSearcher(t, []any{
		map[string]any{"name": "s", "fields": map[string]any{"MESSAGE": `x`}},
	}, nil)
	if !searcher.PerformSearches(map[string]any{"MESSAGE": "x"}) {
		t.Fatal("want match without stats")
	}
}

// TestNewSearchesErrors locks construction-time validation (Python
// raises/crashes at reader construction; strict-at-load mirrors it).
func TestNewSearchesErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		raw      []any
		fragment string
	}{
		{name: "not an object", raw: []any{"x"}, fragment: "must be an object"},
		{name: "missing name", raw: []any{map[string]any{"fields": map[string]any{}}}, fragment: "missing field 'name'"},
		{name: "non-string name", raw: []any{map[string]any{"name": 5}}, fragment: "must be a string"},
		{name: "fields not object", raw: []any{map[string]any{"name": "s", "fields": "x"}}, fragment: "fields must be an object"},
		{name: "non-string pattern", raw: []any{map[string]any{"name": "s", "fields": map[string]any{"MESSAGE": 5}}}, fragment: "must be a string"},
		{name: "invalid regex", raw: []any{map[string]any{"name": "s", "fields": map[string]any{"MESSAGE": "["}}}, fragment: "invalid regex"},
		{name: "tags not object", raw: []any{map[string]any{"name": "s", "tags": "x"}}, fragment: "tags must be an object"},
		{name: "non-string tag", raw: []any{map[string]any{"name": "s", "tags": map[string]any{"t": 5}}}, fragment: "must be a string"},
		{name: "invalid tag function", raw: []any{map[string]any{"name": "s", "tags": map[string]any{"t": "not_a_call()"}}}, fragment: "Invalid tag function tag value"},
		{name: "unknown tag function", raw: []any{map[string]any{"name": "s", "tags": map[string]any{"t": "nope(a,5)"}}}, fragment: "Unknown tag function"},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := filter.NewSearches(tc.raw)
			if err == nil || !strings.Contains(err.Error(), tc.fragment) {
				t.Errorf("want error containing %q, got %v", tc.fragment, err)
			}
		})
	}
}

// TestNewSearchesTagFunctionAccepted locks that a well-formed
// ip_to_geohash call passes construction (Task 6 supplies the
// implementation).
func TestNewSearchesTagFunctionAccepted(t *testing.T) {
	t.Parallel()
	searches, err := filter.NewSearches([]any{
		map[string]any{
			"name":   "s",
			"fields": map[string]any{"MESSAGE": `(?P<ip_address>\d+\.\d+\.\d+\.\d+)`},
			"tags":   map[string]any{"geohash": "ip_to_geohash(ip_address,5)"},
		},
	})
	if err != nil {
		t.Fatalf("NewSearches: %v", err)
	}
	if searches[0].Name() != "s" {
		t.Errorf("name = %q", searches[0].Name())
	}
}

// TestHitsConcurrentWithPerformSearches is the gate-integrity regression for
// the data race between the reader goroutine's PerformSearches and the
// state/stats path's Hits() read (a plain int64 counter failed under -race;
// the counter is atomic). Run with -race.
func TestHitsConcurrentWithPerformSearches(t *testing.T) {
	searcher := newTestSearcher(t, []any{
		map[string]any{"name": "s", "fields": map[string]any{"MESSAGE": "hit"}},
	}, nil)
	entry := map[string]any{"MESSAGE": "hit"}
	var wg sync.WaitGroup
	stop := make(chan struct{})
	firstDone := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Guarantee at least one increment happens-before the reader loop
		// starts (the scheduler may otherwise never run this goroutine
		// before the 200k reads finish).
		searcher.PerformSearches(entry)
		close(firstDone)
		for {
			select {
			case <-stop:
				return
			default:
				searcher.PerformSearches(entry)
			}
		}
	}()
	<-firstDone
	var sink int64
	for i := 0; i < 200000; i++ {
		sink += searcher.Searches()[0].Hits()
	}
	close(stop)
	wg.Wait()
	if sink < 1 {
		t.Fatalf("Hits() never observed an increment (sink=%d)", sink)
	}
}
