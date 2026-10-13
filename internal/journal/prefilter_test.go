// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package journal

import (
	"math/rand"
	"testing"

	"github.com/Aiven-Open/journalpump/internal/config"
	"github.com/Aiven-Open/journalpump/internal/filter"
)

// unitLevel builds a UnitLogLevel from "glob:LEVEL" pairs, in order.
func unitLevel(tb testing.TB, name string, globs ...string) *filter.UnitLogLevel {
	tb.Helper()
	if len(globs) == 0 {
		return nil
	}
	entries := make([]config.UnitLogLevelEntry, 0, len(globs))
	for _, g := range globs {
		glob, level := g, "INFO"
		for i := len(g) - 1; i >= 0; i-- {
			if g[i] == ':' {
				glob, level = g[:i], g[i+1:]
				break
			}
		}
		entries = append(entries, config.UnitLogLevelEntry{ServiceGlob: glob, LogLevel: level})
	}
	levels, err := filter.NewUnitLogLevel(name, entries)
	if err != nil {
		tb.Fatalf("NewUnitLogLevel(%s): %v", name, err)
	}
	return levels
}

// TestSeverityPrefilterPreservedRules is the §2.6.1 preserved-rules table:
// absent/zero/non-numeric/repeated PRIORITY, absent or empty SYSTEMD_UNIT,
// a sender without a catch-all glob, a sender without unit_log_level, and
// an extras override of either key must all reproduce Python's keep/drop
// decision (asserted against the same per-sender path).
func TestSeverityPrefilterPreservedRules(t *testing.T) {
	const cursor = "s=x;i=1a2b;b=1;m=2;t=3;x=4"
	realtime := uint64(1700000000500000)

	for _, tc := range []struct {
		name    string
		fields  []string
		targets []SenderTarget
		want    bool
	}{
		{
			name:    "priority above floor prunes",
			fields:  []string{"MESSAGE=hi", "PRIORITY=7", "_SYSTEMD_UNIT=foo.service"},
			targets: []SenderTarget{{Name: "a", UnitLogLevels: unitLevel(t, "l", "*:INFO")}},
			want:    true,
		},
		{
			name:    "priority at floor passes",
			fields:  []string{"MESSAGE=hi", "PRIORITY=6", "_SYSTEMD_UNIT=foo.service"},
			targets: []SenderTarget{{Name: "a", UnitLogLevels: unitLevel(t, "l", "*:INFO")}},
			want:    false,
		},
		{
			name:    "priority zero passes (falsy)",
			fields:  []string{"MESSAGE=hi", "PRIORITY=0", "_SYSTEMD_UNIT=foo.service"},
			targets: []SenderTarget{{Name: "a", UnitLogLevels: unitLevel(t, "l", "*:INFO")}},
			want:    false,
		},
		{
			name:    "leading zeros are pruned via tier B",
			fields:  []string{"MESSAGE=hi", "PRIORITY=0007", "_SYSTEMD_UNIT=foo.service"},
			targets: []SenderTarget{{Name: "a", UnitLogLevels: unitLevel(t, "l", "*:INFO")}},
			want:    true,
		},
		{
			name:    "non-numeric priority passes",
			fields:  []string{"MESSAGE=hi", "PRIORITY=abc", "_SYSTEMD_UNIT=foo.service"},
			targets: []SenderTarget{{Name: "a", UnitLogLevels: unitLevel(t, "l", "*:INFO")}},
			want:    false,
		},
		{
			name:    "repeated priority is a list and passes",
			fields:  []string{"MESSAGE=hi", "PRIORITY=7", "PRIORITY=7", "_SYSTEMD_UNIT=foo.service"},
			targets: []SenderTarget{{Name: "a", UnitLogLevels: unitLevel(t, "l", "*:INFO")}},
			want:    false,
		},
		{
			name:    "absent priority passes",
			fields:  []string{"MESSAGE=hi", "_SYSTEMD_UNIT=foo.service"},
			targets: []SenderTarget{{Name: "a", UnitLogLevels: unitLevel(t, "l", "*:INFO")}},
			want:    false,
		},
		{
			name:    "absent unit passes (kernel entry)",
			fields:  []string{"MESSAGE=hi", "PRIORITY=7"},
			targets: []SenderTarget{{Name: "a", UnitLogLevels: unitLevel(t, "l", "*:INFO")}},
			want:    false,
		},
		{
			name:    "empty unit passes",
			fields:  []string{"MESSAGE=hi", "PRIORITY=7", "SYSTEMD_UNIT="},
			targets: []SenderTarget{{Name: "a", UnitLogLevels: unitLevel(t, "l", "*:INFO")}},
			want:    false,
		},
		{
			name:    "underscore unit counts",
			fields:  []string{"MESSAGE=hi", "PRIORITY=7", "_SYSTEMD_UNIT=foo.service"},
			targets: []SenderTarget{{Name: "a", UnitLogLevels: unitLevel(t, "l", "*:INFO")}},
			want:    true,
		},
		{
			name:    "distinct raw keys stripping alike are a scalar collision",
			fields:  []string{"MESSAGE=hi", "PRIORITY=7", "_SYSTEMD_UNIT=foo.service", "SYSTEMD_UNIT=bar.service"},
			targets: []SenderTarget{{Name: "a", UnitLogLevels: unitLevel(t, "l", "*:INFO")}},
			// ProcessEntry keeps ONE scalar key (last value wins), so the
			// unit filter sees a real unit and drops at WARNING+.
			want: true,
		},
		{
			name:    "no catch-all glob: matching unit prunes",
			fields:  []string{"MESSAGE=hi", "PRIORITY=7", "_SYSTEMD_UNIT=foo.service"},
			targets: []SenderTarget{{Name: "a", UnitLogLevels: unitLevel(t, "l", "*.service:INFO")}},
			want:    true,
		},
		{
			name:    "no catch-all glob: non-matching unit passes",
			fields:  []string{"MESSAGE=hi", "PRIORITY=7", "_SYSTEMD_UNIT=other"},
			targets: []SenderTarget{{Name: "a", UnitLogLevels: unitLevel(t, "l", "*.service:INFO")}},
			want:    false,
		},
		{
			name:    "sender without unit_log_level never prunes",
			fields:  []string{"MESSAGE=hi", "PRIORITY=7", "_SYSTEMD_UNIT=foo.service"},
			targets: []SenderTarget{{Name: "a", UnitLogLevels: unitLevel(t, "l", "*:INFO")}, {Name: "b"}},
			want:    false,
		},
		{
			name:    "most permissive sender keeps the entry",
			fields:  []string{"MESSAGE=hi", "PRIORITY=5", "_SYSTEMD_UNIT=foo.service"},
			targets: []SenderTarget{{Name: "a", UnitLogLevels: unitLevel(t, "l", "*:INFO")}, {Name: "b", UnitLogLevels: unitLevel(t, "m", "*:WARNING")}},
			want:    false,
		},
		{
			name:    "all senders drop at their own floor",
			fields:  []string{"MESSAGE=hi", "PRIORITY=7", "_SYSTEMD_UNIT=foo.service"},
			targets: []SenderTarget{{Name: "a", UnitLogLevels: unitLevel(t, "l", "*:INFO")}, {Name: "b", UnitLogLevels: unitLevel(t, "m", "*:WARNING")}},
			want:    true,
		},
		{
			name:    "extras override of PRIORITY below floor keeps",
			fields:  []string{"MESSAGE=hi", "PRIORITY=7", "_SYSTEMD_UNIT=foo.service"},
			targets: []SenderTarget{{Name: "a", UnitLogLevels: unitLevel(t, "l", "*:INFO"), ExtraFieldValues: []Field{{Key: "PRIORITY", Value: int64(3)}}}},
			want:    false,
		},
		{
			name:    "extras override of PRIORITY above floor prunes",
			fields:  []string{"MESSAGE=hi", "PRIORITY=1", "_SYSTEMD_UNIT=foo.service"},
			targets: []SenderTarget{{Name: "a", UnitLogLevels: unitLevel(t, "l", "*:INFO"), ExtraFieldValues: []Field{{Key: "PRIORITY", Value: int64(7)}}}},
			want:    true,
		},
		{
			name:    "extras override of SYSTEMD_UNIT to empty keeps",
			fields:  []string{"MESSAGE=hi", "PRIORITY=7", "_SYSTEMD_UNIT=foo.service"},
			targets: []SenderTarget{{Name: "a", UnitLogLevels: unitLevel(t, "l", "*:INFO"), ExtraFieldValues: []Field{{Key: "SYSTEMD_UNIT", Value: ""}}}},
			want:    false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := buildBatch(t, realtime, cursor, tc.fields...)
			entry := &b.Entries[0]
			pre := NewSeverityPrefilter(tc.targets)

			if got := pre.ShouldPrune(b, entry); got != tc.want {
				t.Fatalf("ShouldPrune = %v, want %v", got, tc.want)
			}
			// Cross-check: the pre-filter must agree with the real
			// per-sender path — pruned iff no sender emits a payload.
			got := make([][]byte, len(tc.targets))
			handler := NewHandler(HandlerConfig{}) // no pre-filter: the oracle
			if _, _, _, err := handler.ProcessRaw(b, entry, collectTargets(tc.targets, &got)); err != nil {
				t.Fatalf("ProcessRaw: %v", err)
			}
			emitted := false
			for _, payload := range got {
				if payload != nil {
					emitted = true
				}
			}
			if tc.want == emitted {
				t.Fatalf("prune=%v but per-sender path emitted=%v", tc.want, emitted)
			}
		})
	}
}

// TestSeverityPrefilterNeverChangesOutput is the property test: over
// random sender configs and entries, running the raw path WITH the
// pre-filter must produce exactly the same per-sender payloads and
// return tuple as the RETAINED converted-entry Process oracle — the
// pre-filter may only short-circuit an outcome that was already empty
// (design §2.6.1). Comparing the pre-filtered raw path against the
// unfiltered raw path (the previous shape) shared every raw-path bug, so
// it could not catch a wrong ShouldPrune.
func TestSeverityPrefilterNeverChangesOutput(t *testing.T) {
	rng := rand.New(rand.NewSource(20261013))
	units := []string{"foo.service", "bar.socket", "bench-c.service", "other", ""}
	priorities := []string{"0", "1", "3", "6", "7", "0007", "abc", ""}
	fieldFilters := []*filter.FieldFilter{
		nil,
		filter.NewFieldFilter("keep-msg", config.FieldFilterConfig{Whitelist: true, Fields: []string{"MESSAGE", "PRIORITY", "SYSTEMD_UNIT"}}),
		filter.NewFieldFilter("drop-pri", config.FieldFilterConfig{Whitelist: false, Fields: []string{"PRIORITY"}}),
	}
	extraSets := [][]Field{
		nil,
		{{Key: "env", Value: "prod"}},
		{{Key: "PRIORITY", Value: int64(2)}},
		{{Key: "SYSTEMD_UNIT", Value: "override.service"}},
		// Overlapping extras across senders (same key, different value) so
		// the ruling-1 contamination is exercised on both paths.
		{{Key: "env", Value: "stage"}, {Key: "PRIORITY", Value: int64(7)}},
	}
	levelOptions := []func(name string) *filter.UnitLogLevel{
		func(string) *filter.UnitLogLevel { return nil },
		func(name string) *filter.UnitLogLevel { return unitLevel(t, name, "*:INFO") },
		func(name string) *filter.UnitLogLevel { return unitLevel(t, name, "*:WARNING") },
		func(name string) *filter.UnitLogLevel { return unitLevel(t, name, "*.service:INFO") },
	}

	// run compares the pre-filtered raw path against the retained Process
	// oracle for one sender list; both handlers get independent state.
	run := func(iter int, targets []SenderTarget, fields []string) {
		t.Helper()
		b := buildBatch(t, 1700000000500000, "s=x;i=1a2b;b=1;m=2;t=3;x=4", fields...)
		entry := &b.Entries[0]

		gotWith := make([][]byte, len(targets))
		hWith := NewHandler(HandlerConfig{Prefilter: NewSeverityPrefilter(targets)})
		hm, brm, bsm, err := hWith.ProcessRaw(b, entry, collectTargets(targets, &gotWith))
		if err != nil {
			t.Fatalf("iter %d with: %v", iter, err)
		}
		oracle := make([][]byte, len(targets))
		hOracle := NewHandler(HandlerConfig{})
		hm2, brm2, bsm2, err := hOracle.Process(rawToEntry(t, b, entry), collectTargets(targets, &oracle))
		if err != nil {
			t.Fatalf("iter %d oracle: %v", iter, err)
		}
		if hm != hm2 || brm != brm2 || bsm != bsm2 {
			t.Fatalf("iter %d: return (%v,%d,%v) != oracle (%v,%d,%v)", iter, hm, brm, bsm, hm2, brm2, bsm2)
		}
		for i := range oracle {
			switch {
			case gotWith[i] == nil && oracle[i] == nil:
			case gotWith[i] == nil || oracle[i] == nil:
				t.Fatalf("iter %d sender %d: prefiltered=%v oracle=%v", iter, i, gotWith[i], oracle[i])
			case !semanticEqual(t, gotWith[i], oracle[i]):
				t.Fatalf("iter %d sender %d semantic mismatch:\n with   %s\noracle %s", iter, i, gotWith[i], oracle[i])
			}
		}
	}

	for iter := 0; iter < 300; iter++ {
		nsenders := 1 + rng.Intn(4)
		targets := make([]SenderTarget, 0, nsenders)
		for s := 0; s < nsenders; s++ {
			targets = append(targets, SenderTarget{
				Name:             "s" + string(rune('a'+s)),
				FieldFilter:      fieldFilters[rng.Intn(len(fieldFilters))],
				UnitLogLevels:    levelOptions[rng.Intn(len(levelOptions))]("l" + string(rune('a'+s))),
				ExtraFieldValues: extraSets[rng.Intn(len(extraSets))],
			})
		}
		fields := []string{"MESSAGE=hello"}
		if p := priorities[rng.Intn(len(priorities))]; p != "" {
			fields = append(fields, "PRIORITY="+p)
		}
		if u := units[rng.Intn(len(units))]; u != "" {
			fields = append(fields, "_SYSTEMD_UNIT="+u)
			// A distinct raw key that strips to the same processed name: a
			// scalar collision on the oracle, a repeat on the raw path.
			if rng.Intn(2) == 0 {
				fields = append(fields, "SYSTEMD_UNIT="+u)
			}
		}
		run(iter, targets, fields)
		// The contamination quirk makes the sender ORDER observable; the
		// pre-filtered path and the oracle must agree for both orders.
		if nsenders > 1 && rng.Intn(2) == 0 {
			rev := append([]SenderTarget(nil), targets...)
			for i, j := 0, len(rev)-1; i < j; i, j = i+1, j-1 {
				rev[i], rev[j] = rev[j], rev[i]
			}
			run(iter, rev, fields)
		}
	}
}
