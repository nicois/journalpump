// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package reader

import (
	"fmt"
	"strings"
	"testing"
)

// Python logging level numbers (logging.INFO/WARNING), used by the
// threshold-aware test logger below.
const (
	testInfoLevel    = 20
	testWarningLevel = 30
)

// warnLevelLogger models a level-aware sink: Printf emits at INFO,
// Warningf at WARNING, and anything below the configured minimum is
// dropped — exactly how journaldlog.Log filters.
type warnLevelLogger struct {
	min   int
	lines []string
}

func (l *warnLevelLogger) Printf(format string, args ...any) {
	if l.min <= testInfoLevel {
		l.lines = append(l.lines, fmt.Sprintf(format, args...))
	}
}

func (l *warnLevelLogger) Warningf(format string, args ...any) {
	if l.min <= testWarningLevel {
		l.lines = append(l.lines, fmt.Sprintf(format, args...))
	}
}

// TestCorruptedEntryWarnsAtWarningThreshold locks the severity routing of
// the "Bad message" arm (journalpump.py:392-397, ported as CorruptedEntry):
// Python logs it at WARNING, so it must survive a WARNING threshold. The
// old Printf path emitted at INFO and vanished at that threshold; the INFO
// probe below confirms the test logger really drops INFO.
func TestCorruptedEntryWarnsAtWarningThreshold(t *testing.T) {
	log := &warnLevelLogger{min: testWarningLevel}
	s := NewStats("r", map[string]string{"reader": "r"}, nil, log, nil)

	s.CorruptedEntry()
	if len(log.lines) != 1 || !strings.Contains(log.lines[0], "Corrupted log entry in r") {
		t.Fatalf("warning lines = %q, want the corruption warning", log.lines)
	}

	// Sanity: an INFO line is genuinely suppressed at this threshold, so
	// the assertion above would have failed under the old Printf path.
	log.Printf("info probe")
	if len(log.lines) != 1 {
		t.Fatalf("INFO line surfaced at the WARNING threshold: %q", log.lines)
	}
}
