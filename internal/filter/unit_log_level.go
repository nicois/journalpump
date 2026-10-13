// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package filter

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"

	"github.com/Aiven-Open/journalpump/internal/config"
)

// UnitLogLevel ports journalpump.py:UnitLogLevel. Like FieldFilter, it
// is a construction-time object captured by senders (spec §3 reload
// corollary) and immutable afterwards.
//
// Semantics (journalpump.py:671-696): an entry is dropped if ANY glob
// matching the unit has a level more restrictive than the entry's
// priority — most-restrictive-wins across ALL matches, not first-match.
// Missing/falsy priority or unit, or an empty level list, keeps the
// entry as-is. Python's lru_cache over (levels, unit, priority) is a
// pure memoization with no observable behavior and is not reproduced.
type UnitLogLevel struct {
	name   string
	levels []unitLevel
}

type unitLevel struct {
	glob  string
	regex *regexp.Regexp
	level int64
}

// NewUnitLogLevel compiles the service globs and resolves the severity
// levels. Python resolves log_level names lazily at filter time
// (KeyError crash on unknown names, journalpump.py:694); per the
// sanctioned strict-at-load policy (spec §6.1a) unknown names are
// rejected here instead.
func NewUnitLogLevel(name string, entries []config.UnitLogLevelEntry) (*UnitLogLevel, error) {
	levels := make([]unitLevel, 0, len(entries))
	for _, entry := range entries {
		matcher, err := TranslateGlobPattern(entry.ServiceGlob)
		if err != nil {
			return nil, fmt.Errorf("unit_log_levels.%s: invalid service_glob %q: %w", name, entry.ServiceGlob, err)
		}
		level, ok := config.SeverityLevels[entry.LogLevel]
		if !ok {
			return nil, fmt.Errorf("unit_log_levels.%s.log_level: unknown log level %q", name, entry.LogLevel)
		}
		levels = append(levels, unitLevel{glob: entry.ServiceGlob, regex: matcher, level: level})
	}
	return &UnitLogLevel{name: name, levels: levels}, nil
}

// Name returns the configured name (Python: self.name).
func (u *UnitLogLevel) Name() string {
	return u.name
}

// CatchAllLevel returns the severity level of the catch-all glob
// (`service_glob` exactly "*"), if the config has one. A `*` glob makes
// condition (iii) of Decide hold for every non-empty unit, so its level is
// the most permissive threshold any entry must clear for this filter to
// drop it; the caller of this accessor (the §2.6 tier-A floor, design
// docs/design-journal-pipeline.md §2.6.1) takes the maximum across
// senders. ok is false when there is no `*` glob (the filter can only
// drop units matching a specific glob, so no unit-agnostic floor exists).
func (u *UnitLogLevel) CatchAllLevel() (int64, bool) {
	for i := range u.levels {
		if u.levels[i].glob == "*" {
			return u.levels[i].level, true
		}
	}
	return 0, false
}

// Filter reports whether the entry passes the unit log level rules:
// true keeps the entry as-is, false means the caller drops it (Python
// returns {} for a dropped entry, which the caller renders as nothing).
// The entry is the converted per-sender entry (the caller json.loads
// cached serialized entries first, as Python's filter_by_level does).
func (u *UnitLogLevel) Filter(entry map[string]any) bool {
	if len(entry) == 0 {
		// Python: `if not entry: return {}`.
		return false
	}
	return u.Decide(entryValueString(entry["SYSTEMD_UNIT"]), entry["PRIORITY"])
}

// Decide is Filter's decision over the two views the streaming handler
// has: the unit name (the converted SYSTEMD_UNIT) and the priority
// value (the converted PRIORITY, nil when the field is absent). It is
// exported so the raw pipeline can reach the decision without
// materialising a map.
func (u *UnitLogLevel) Decide(unit string, priority any) bool {
	if !truthy(priority) || unit == "" {
		// If the entry does not contain a unit or priority, return it
		// as-is (Python: `if (not priority) or (not unit)`).
		return true
	}
	if len(u.levels) == 0 {
		return true
	}
	priorityNum, numeric := asPythonNumber(priority)
	if !numeric {
		// Python: `priority > LOG_SEVERITY_MAPPING[log_level]` raises
		// TypeError for non-numeric priorities. Sanctioned deviation
		// (crash-free runtime): keep the entry.
		return true
	}
	for _, level := range u.levels {
		if level.regex.MatchString(unit) && priorityNum.greaterThan(level.level) {
			return false
		}
	}
	return true
}

// entryValueString reports the value if it is a string usable as a unit
// name; any other type would make Python's fnmatch raise TypeError, so
// it is reported as absent (sanctioned crash-free deviation).
func entryValueString(value any) string {
	if s, ok := value.(string); ok {
		return s
	}
	return ""
}

// truthy mirrors Python truthiness for the values that can appear in a
// converted entry.
func truthy(v any) bool {
	switch typed := v.(type) {
	case nil:
		return false
	case bool:
		return typed
	case int64:
		return typed != 0
	case float64:
		return typed != 0
	case json.Number:
		return typed.String() != "" && typed.String() != "0"
	case string:
		return typed != ""
	}
	return true
}

// pythonNumber is the numeric view of an entry value for the
// `priority > level` comparison (Python compares int/float/bool
// numerically).
type pythonNumber struct {
	isFloat bool
	i       int64
	f       float64
}

func asPythonNumber(v any) (pythonNumber, bool) {
	switch typed := v.(type) {
	case bool:
		if typed {
			return pythonNumber{i: 1}, true
		}
		return pythonNumber{i: 0}, true
	case int64:
		return pythonNumber{i: typed}, true
	case float64:
		return pythonNumber{isFloat: true, f: typed}, true
	case json.Number:
		if i, err := strconv.ParseInt(typed.String(), 10, 64); err == nil {
			return pythonNumber{i: i}, true
		}
		if f, err := typed.Float64(); err == nil {
			return pythonNumber{isFloat: true, f: f}, true
		}
	}
	return pythonNumber{}, false
}

func (n pythonNumber) greaterThan(level int64) bool {
	if n.isFloat {
		return n.f > float64(level)
	}
	return n.i > level
}
