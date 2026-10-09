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

// TestUnitLogLevels ports test/test_journalpump.py:TestUnitLogLevels
// (9 methods) plus the most-restrictive-wins quirk lock: an entry is
// dropped when ANY matching glob is more restrictive, regardless of
// order (journalpump.py:691-696).
func TestUnitLogLevels(t *testing.T) {
	t.Parallel()
	type entry map[string]any
	tests := []struct {
		name   string
		levels []config.UnitLogLevelEntry
		data   entry
		want   bool // Filter result: true keeps the entry, false drops it
	}{
		{
			// test_empty: glob does not match the unit -> as-is.
			name:   "empty/no match",
			levels: []config.UnitLogLevelEntry{{ServiceGlob: "unit-a", LogLevel: "INFO"}},
			data:   entry{"Foo": "bar", "PRIORITY": config.SeverityLevels["INFO"], "SYSTEMD_UNIT": "unit-b"},
			want:   true,
		},
		{
			// test_empty_log_levels: no configured levels -> as-is.
			name:   "empty log levels",
			levels: nil,
			data:   entry{"Foo": "bar", "PRIORITY": config.SeverityLevels["INFO"], "SYSTEMD_UNIT": "unit-b"},
			want:   true,
		},
		{
			// test_matching_level: priority equal to level -> kept.
			name:   "matching level",
			levels: []config.UnitLogLevelEntry{{ServiceGlob: "unit-a", LogLevel: "INFO"}},
			data:   entry{"Foo": "bar", "PRIORITY": config.SeverityLevels["INFO"], "SYSTEMD_UNIT": "unit-a"},
			want:   true,
		},
		{
			// test_higher_level: priority above level -> dropped.
			name:   "higher level",
			levels: []config.UnitLogLevelEntry{{ServiceGlob: "unit-a", LogLevel: "WARNING"}},
			data:   entry{"Foo": "bar", "PRIORITY": config.SeverityLevels["WARNING"] + 1, "SYSTEMD_UNIT": "unit-a"},
			want:   false,
		},
		{
			// test_lower_level: priority below level -> kept.
			name:   "lower level",
			levels: []config.UnitLogLevelEntry{{ServiceGlob: "unit-a", LogLevel: "WARNING"}},
			data:   entry{"Foo": "bar", "PRIORITY": config.SeverityLevels["WARNING"] - 1, "SYSTEMD_UNIT": "unit-a"},
			want:   true,
		},
		{
			// test_no_systemd_unit: no unit -> kept even though priority
			// is too low.
			name:   "no systemd unit",
			levels: []config.UnitLogLevelEntry{{ServiceGlob: "unit-a", LogLevel: "CRITICAL"}},
			data:   entry{"Foo": "bar", "PRIORITY": config.SeverityLevels["INFO"] - 1},
			want:   true,
		},
		{
			// test_no_priority: no PRIORITY -> kept.
			name:   "no priority",
			levels: []config.UnitLogLevelEntry{{ServiceGlob: "unit-a", LogLevel: "WARNING"}},
			data:   entry{"Foo": "bar", "SYSTEMD_UNIT": "unit-a"},
			want:   true,
		},
		{
			// test_no_priority_or_unit: neither -> kept.
			name:   "no priority or unit",
			levels: []config.UnitLogLevelEntry{{ServiceGlob: "unit-a", LogLevel: "WARNING"}},
			data:   entry{"Foo": "bar"},
			want:   true,
		},
		{
			// test_unit_glob_matching: glob wildcard keeps at exactly the
			// configured level, drops above it, and does not match
			// without the dash.
			name:   "unit glob matching/keep at level",
			levels: []config.UnitLogLevelEntry{{ServiceGlob: "unit-a*", LogLevel: "WARNING"}},
			data:   entry{"Foo": "bar", "PRIORITY": config.SeverityLevels["WARNING"], "SYSTEMD_UNIT": "unit-a-something"},
			want:   true,
		},
		{
			name:   "unit glob matching/drop above level",
			levels: []config.UnitLogLevelEntry{{ServiceGlob: "unit-a*", LogLevel: "WARNING"}},
			data:   entry{"Foo": "bar", "PRIORITY": config.SeverityLevels["INFO"], "SYSTEMD_UNIT": "unit-a-something-else"},
			want:   false,
		},
		{
			name:   "unit glob matching/glob must match",
			levels: []config.UnitLogLevelEntry{{ServiceGlob: "unit-a*", LogLevel: "WARNING"}},
			data:   entry{"Foo": "bar", "PRIORITY": config.SeverityLevels["WARNING"], "SYSTEMD_UNIT": "unita"},
			want:   true,
		},
		{
			// Quirk lock: most-restrictive-wins. The FIRST matching glob
			// is permissive, the SECOND restrictive -> dropped.
			name: "most restrictive wins/second rule drops",
			levels: []config.UnitLogLevelEntry{
				{ServiceGlob: "unit-a*", LogLevel: "DEBUG"},
				{ServiceGlob: "unit-a-specific*", LogLevel: "ERROR"},
			},
			data: entry{"PRIORITY": config.SeverityLevels["WARNING"], "SYSTEMD_UNIT": "unit-a-specific-1"},
			want: false,
		},
		{
			// Same rules in the opposite order — still dropped.
			name: "most restrictive wins/order independent",
			levels: []config.UnitLogLevelEntry{
				{ServiceGlob: "unit-a-specific*", LogLevel: "ERROR"},
				{ServiceGlob: "unit-a*", LogLevel: "DEBUG"},
			},
			data: entry{"PRIORITY": config.SeverityLevels["WARNING"], "SYSTEMD_UNIT": "unit-a-specific-1"},
			want: false,
		},
		{
			// A matching permissive glob does not keep an entry another
			// matching glob drops; a non-matching restrictive glob does
			// not drop anything.
			name: "most restrictive wins/non-matching rule ignored",
			levels: []config.UnitLogLevelEntry{
				{ServiceGlob: "other-unit*", LogLevel: "EMERGENCY"},
				{ServiceGlob: "unit-a*", LogLevel: "WARNING"},
			},
			data: entry{"PRIORITY": config.SeverityLevels["WARNING"], "SYSTEMD_UNIT": "unit-a"},
			want: true,
		},
		{
			// PRIORITY 0 is falsy in Python (`not priority`) -> kept
			// as-is even though 0 is the most severe severity.
			name:   "falsy priority 0 kept",
			levels: []config.UnitLogLevelEntry{{ServiceGlob: "unit-a*", LogLevel: "EMERGENCY"}},
			data:   entry{"PRIORITY": int64(0), "SYSTEMD_UNIT": "unit-a"},
			want:   true,
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ll, err := filter.NewUnitLogLevel("test", tc.levels)
			if err != nil {
				t.Fatalf("NewUnitLogLevel: %v", err)
			}
			if got := ll.Filter(tc.data); got != tc.want {
				t.Errorf("Filter = %v want %v (entry %v)", got, tc.want, tc.data)
			}
		})
	}
}

// TestUnitLogLevelSeveritySweep ports the loop in
// test/test_journalpump.py:test_log_level_filtering: every configured
// severity is tried against a fixed level; entries at or above the
// configured severity pass, more severe (numerically higher) priorities
// drop.
func TestUnitLogLevelSeveritySweep(t *testing.T) {
	t.Parallel()
	configured := config.SeverityLevels["INFO"]
	ll, err := filter.NewUnitLogLevel("unit_log_levels_d", []config.UnitLogLevelEntry{
		{ServiceGlob: "test-unit", LogLevel: "INFO"},
	})
	if err != nil {
		t.Fatalf("NewUnitLogLevel: %v", err)
	}
	expected := 0
	for _, priority := range config.SeverityLevels {
		want := priority <= configured
		if want {
			expected++
		}
		entry := map[string]any{"SYSTEMD_UNIT": "test-unit", "PRIORITY": priority}
		if got := ll.Filter(entry); got != want {
			t.Errorf("priority %d: Filter = %v want %v", priority, got, want)
		}
	}
	if expected == 0 {
		t.Fatal("sweep did not exercise any passing priority")
	}
}

// TestNewUnitLogLevelErrors locks the strict-at-load validation of
// unknown log level names (Python crashes with KeyError at filter time;
// sanctioned deviation, spec §6.1a).
func TestNewUnitLogLevelErrors(t *testing.T) {
	t.Parallel()
	if _, err := filter.NewUnitLogLevel("test", []config.UnitLogLevelEntry{{ServiceGlob: "a*", LogLevel: "NOT_A_LEVEL"}}); err == nil {
		t.Error("unknown log level: want error, got nil")
	}
}
