// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package journal

import (
	"bytes"
	"math"

	"github.com/Aiven-Open/journalpump/internal/filter"
)

// SeverityPrefilter implements the design §2.6 global severity
// pre-filter (docs/design-journal-pipeline.md §2.6.1): before emitting
// anything, an entry is skipped entirely when EVERY sender's
// unit_log_level would drop it. It is behaviour-preserving — it only
// short-circuits an all-drop outcome — and it calls the same
// filter.UnitLogLevel.Decide and the same effective-entry resolution the
// per-sender path uses, so it cannot diverge from them.
//
// Construction folds ruling 1: pruning only happens when every sender is a
// cache miss (a drop caches nothing), so sender s's effective entry is the
// raw entry overlaid with the union, in config order, of the extras of
// every sender up to and including s's own (Python's shared-dict
// contamination). Those per-sender prefixes are precomputed here.
type SeverityPrefilter struct {
	// tierA is the unit-agnostic floor: enabled when every sender has a
	// unit_log_level with a catch-all `*` glob AND no sender's effective
	// extras override PRIORITY or SYSTEMD_UNIT. floor is the maximum
	// (most permissive) of those levels.
	tierA bool
	floor int64
	// tierB is the exact all-senders decision: enabled when every sender
	// has a unit_log_level (otherwise no entry can be dropped by all).
	tierB   bool
	senders []prefilterSender
}

// prefilterSender is one sender's pre-filter inputs: its unit_log_level
// and the effective extras prefix for that sender.
type prefilterSender struct {
	levels *filter.UnitLogLevel
	extras []Field
}

var (
	priorityKeyName    = []byte("PRIORITY")
	systemdUnitKeyName = []byte("SYSTEMD_UNIT")
)

// NewSeverityPrefilter builds the pre-filter for one reader's ordered
// sender targets, or nil when no entry can ever be pruned (an empty
// reader, or a sender without unit_log_level).
func NewSeverityPrefilter(targets []SenderTarget) *SeverityPrefilter {
	if len(targets) == 0 {
		return nil
	}
	p := &SeverityPrefilter{tierA: true, tierB: true, floor: math.MinInt64}
	var acc []Field
	for i := range targets {
		t := &targets[i]
		acc = concatFields(acc, t.ExtraFieldValues)
		for _, extra := range t.ExtraFieldValues {
			// An extras override of either key makes the value
			// entry-independent; the tier-A raw-byte scan cannot see it,
			// so disable tier A (tier B resolves it exactly).
			if extra.Key == "PRIORITY" || extra.Key == "SYSTEMD_UNIT" {
				p.tierA = false
			}
		}
		if t.UnitLogLevels == nil {
			// This sender never drops: no entry can be pruned by all.
			p.tierB = false
			p.tierA = false
		} else if level, ok := t.UnitLogLevels.CatchAllLevel(); ok {
			if level > p.floor {
				p.floor = level
			}
		} else {
			p.tierA = false
		}
		p.senders = append(p.senders, prefilterSender{levels: t.UnitLogLevels, extras: acc})
	}
	p.tierA = p.tierA && p.tierB
	if !p.tierA && !p.tierB {
		return nil
	}
	return p
}

// ShouldPrune reports whether every sender's unit_log_level would drop the
// entry. A nil receiver (no pre-filter configured) never prunes. It is
// only ever true when no sender would emit, so a caller may return the
// all-dropped result without running emission.
func (p *SeverityPrefilter) ShouldPrune(b *Batch, e *RawEntry) bool {
	if p == nil {
		return false
	}
	if p.tierA && p.tierAPrunes(b, e) {
		return true
	}
	if !p.tierB {
		return false
	}
	for i := range p.senders {
		s := &p.senders[i]
		unit, priority := effectiveUnitPriority(b, e, s.extras)
		if s.levels.Decide(unit, priority) {
			return false
		}
	}
	return true
}

// tierAPrunes is the fast path (~20-50 ns): a single raw-byte scan that
// prunes iff the entry carries exactly one SYSTEMD_UNIT with a non-empty
// value and exactly one PRIORITY whose bytes are a canonical decimal
// strictly greater than the floor. Non-canonical priorities (0, "0006",
// "+6", whitespace, huge values) and repeated fields fall through to
// tier B, which resolves them exactly as Python does; re-running the byte
// scan per entry is bounded by the 16-field index.
func (p *SeverityPrefilter) tierAPrunes(b *Batch, e *RawEntry) bool {
	var priority []byte
	priorityCount := 0
	unitCount := 0
	unitNonEmpty := false
	for i := range e.Fields {
		key := stripUnderscore(b.Key(&e.Fields[i]))
		if len(key) == 0 {
			continue
		}
		// First-byte dispatch keeps the (long) equality check off the
		// scan for the ~14 other keys in the fixture.
		switch key[0] {
		case 'P':
			if bytes.Equal(key, priorityKeyName) {
				priorityCount++
				if priorityCount == 1 {
					priority = b.ValueBytes(&e.Fields[i])
				}
			}
		case 'S':
			if bytes.Equal(key, systemdUnitKeyName) {
				unitCount++
				if unitCount == 1 && len(b.ValueBytes(&e.Fields[i])) > 0 {
					unitNonEmpty = true
				}
			}
		}
	}
	// A repeated field is a list in Python (truthy but non-numeric for
	// PRIORITY, non-string for SYSTEMD_UNIT): never tier-A prunable.
	if priorityCount != 1 || unitCount != 1 || !unitNonEmpty {
		return false
	}
	return canonicalDecimalAbove(priority, p.floor)
}

// canonicalDecimalAbove reports whether v is a canonical decimal integer
// ([1-9][0-9]*, the form verifiably above the truthy-non-zero floor)
// strictly greater than floor. Anything else — empty, leading zero, sign,
// whitespace, non-digit, or an int64 overflow — is not the fast path and
// falls through to tier B.
func canonicalDecimalAbove(v []byte, floor int64) bool {
	if len(v) == 0 || v[0] < '1' || v[0] > '9' {
		return false
	}
	var n int64
	for _, c := range v {
		if c < '0' || c > '9' {
			return false
		}
		digit := int64(c - '0')
		if n > (math.MaxInt64-digit)/10 {
			return false
		}
		n = n*10 + digit
	}
	return n > floor
}
