// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package filter

import (
	"testing"

	"github.com/Aiven-Open/journalpump/internal/tagged"
)

// TestMatchRawValuesEquivalence pins the byte-view search runner to
// PerformSearches over the same converted entry: same match outcome,
// same hit counters and same metric calls.
func TestMatchRawValuesEquivalence(t *testing.T) {
	t.Parallel()
	raw := []any{
		map[string]any{
			"name":   "message",
			"fields": map[string]any{"MESSAGE": `(?P<word>hello)`},
		},
		map[string]any{
			"name":   "priority",
			"fields": map[string]any{"PRIORITY": `^6$`},
		},
		map[string]any{
			"name":   "unit",
			"fields": map[string]any{"_SYSTEMD_UNIT": `foo\.service`},
		},
	}
	entries := []map[string]any{
		{"MESSAGE": "hello world"},
		{"MESSAGE": "bye"},
		{"PRIORITY": int64(6)},
		{"PRIORITY": int64(5)},
		{"_SYSTEMD_UNIT": "foo.service"},
		{"_SYSTEMD_UNIT": "bar.service"},
		{"MESSAGE": ""},
		{"MESSAGE": "hello", "PRIORITY": int64(6)},
	}
	for i, entry := range entries {
		searches, err := NewSearches(raw)
		if err != nil {
			t.Fatalf("NewSearches: %v", err)
		}
		wantSearcher, err := NewSearcher(searches, 0.001, nil, tagged.New(nil), nil, nil)
		if err != nil {
			t.Fatalf("NewSearcher: %v", err)
		}
		want := wantSearcher.PerformSearches(entry)

		searches, err = NewSearches(raw)
		if err != nil {
			t.Fatalf("NewSearches: %v", err)
		}
		gotSearcher, err := NewSearcher(searches, 0.001, nil, tagged.New(nil), nil, nil)
		if err != nil {
			t.Fatalf("NewSearcher: %v", err)
		}
		byteFields := map[string]string{}
		value := func(field string) (string, bool) {
			return searchFieldValue(field, entry, byteFields)
		}
		got := gotSearcher.MatchRawValues(value)

		if got != want {
			t.Errorf("entry %d: MatchRawValues = %v want %v", i, got, want)
		}
		for j, search := range gotSearcher.Searches() {
			if search.Hits() != wantSearcher.Searches()[j].Hits() {
				t.Errorf("entry %d search %d: hits = %d want %d", i, j, search.Hits(), wantSearcher.Searches()[j].Hits())
			}
		}
	}
}
