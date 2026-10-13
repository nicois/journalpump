// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package filter_test

import (
	"testing"

	"github.com/Aiven-Open/journalpump/internal/config"
	"github.com/Aiven-Open/journalpump/internal/filter"
)

// TestFieldFilterKeepBytes pins KeepBytes to Keep over the raw-path key
// shapes (leading underscores, mixed case, non-ASCII).
func TestFieldFilterKeepBytes(t *testing.T) {
	t.Parallel()
	filts := []*filter.FieldFilter{
		filter.NewFieldFilter("wl", config.FieldFilterConfig{Whitelist: true, Fields: []string{"_foo", "BAR"}}),
		filter.NewFieldFilter("bl", config.FieldFilterConfig{Whitelist: false, Fields: []string{"_foo"}}),
		// Non-ASCII whitelist: Python lowercases Unicode, so "Ä" matches.
		filter.NewFieldFilter("uni", config.FieldFilterConfig{Whitelist: true, Fields: []string{"ä"}}),
	}
	keys := []string{"Foo", "_foo", "BAR", "_bar", "_zob", "__REALTIME_TIMESTAMP", "message", "", "_", "Å", "Ä", "ä", "PRIORITY", "MESSAGE"}
	for _, ff := range filts {
		for _, key := range keys {
			want := ff.Keep(key)
			if got := ff.KeepBytes([]byte(key)); got != want {
				t.Errorf("%s KeepBytes(%q) = %v want %v", ff.Name(), key, got, want)
			}
		}
	}
}

// TestFieldFilter ports test/test_journalpump.py:TestFieldFilter.
func TestFieldFilter(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		cfg   config.FieldFilterConfig
		data  map[string]any
		want  map[string]any
		input map[string]any // expected input map after the call (not mutated)
	}{
		{
			name:  "whitelist",
			cfg:   config.FieldFilterConfig{Name: "test", Whitelist: true, Fields: []string{"_foo", "BAR"}},
			data:  map[string]any{"Foo": "a", "_bar": "b", "_zob": "c"},
			want:  map[string]any{"Foo": "a", "_bar": "b"},
			input: map[string]any{"Foo": "a", "_bar": "b", "_zob": "c"},
		},
		{
			name:  "blacklist",
			cfg:   config.FieldFilterConfig{Name: "test", Whitelist: false, Fields: []string{"_foo"}},
			data:  map[string]any{"Foo": "a", "_bar": "b", "_zob": "c"},
			want:  map[string]any{"_bar": "b", "_zob": "c"},
			input: map[string]any{"Foo": "a", "_bar": "b", "_zob": "c"},
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ff := filter.NewFieldFilter(tc.cfg.Name, tc.cfg)
			if ff.Name() != tc.cfg.Name {
				t.Errorf("Name() = %q want %q", ff.Name(), tc.cfg.Name)
			}
			got := ff.FilterFields(tc.data)
			if len(got) != len(tc.want) {
				t.Fatalf("FilterFields size = %d want %d (%v)", len(got), len(tc.want), got)
			}
			for key, value := range tc.want {
				if got[key] != value {
					t.Errorf("FilterFields[%q] = %v want %v", key, got[key], value)
				}
			}
			if len(tc.data) != len(tc.input) {
				t.Fatalf("input map was mutated: %v", tc.data)
			}
			for key, value := range tc.input {
				if tc.data[key] != value {
					t.Errorf("input map mutated at %q: %v", key, tc.data[key])
				}
			}
		})
	}
}
