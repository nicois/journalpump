// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package journal

import (
	"bytes"
	"strconv"

	"github.com/Aiven-Open/journalpump/internal/filter"
)

// This file is the streaming counterpart of handler.go: ProcessRaw runs
// the same per-message pipeline (process + _get_or_generate_json)
// directly over a raw batch entry, without materialising []Field maps
// and without re-parsing cached JSON. It implements rulings 1 and 2 of
// docs/design-journal-pipeline.md §7:
//
//   - ruling 1: a cache-miss sender's effective extras are the union, in
//     config order, of the extras of every earlier cache-miss sender for
//     this entry plus its own (Python's shared-dict contamination).
//   - ruling 2: on a cache hit, the sender's unit_log_level decision is
//     taken from the decision record captured when the payload was
//     emitted — from the live entry only when the emitting sender's field
//     filter kept BOTH PRIORITY and SYSTEMD_UNIT.

// rawCacheEntry is the per-field-filter cache record: the emitted
// payload plus the ruling-2 decision record.
type rawCacheEntry struct {
	payload []byte
	// empty is true when the payload is the empty object "{}" (a field
	// filter that kept no field): Python's cache-hit json.loads then sees
	// {} and the unit-log-level filter drops it.
	empty bool
	// keptPriority / keptUnit report whether the emitting sender's field
	// filter kept PRIORITY / SYSTEMD_UNIT (ruling 2).
	keptPriority bool
	keptUnit     bool
	// unit and priority are the emitting sender's effective entry views
	// (after the ruling-1 effective extras), used by the cache-hit
	// decision.
	unit     string
	priority any
	// indexDate is the Elasticsearch index date (timestamp[:10]) of the
	// payload, or zero when it must be recovered by parsing (design §2.4).
	indexDate [10]byte
}

// ProcessRaw is Process over a raw batch entry. Semantics are exactly
// Process's; see handler.go for the pipeline contract.
func (h *Handler) ProcessRaw(b *Batch, e *RawEntry, targets []SenderTarget) (hasMore bool, bytesRead int, hasBytes bool, err error) {
	if len(b.Cursor(e)) == 0 {
		return false, 0, false, nil
	}
	msgOverride, keep := h.gateRaw(b, e)
	if !keep {
		return true, 0, false, nil
	}
	// §2.6 global severity pre-filter (design §2.6.1): after the
	// searches/match/secret gates (whose counters must still see the
	// entry) and before any emission — skip the entry when every sender
	// would drop it. The result equals the all-dropped outcome below, and
	// a pruned entry allocates nothing (the cursor string is built only
	// once emission is known to happen).
	if h.cfg.Prefilter.ShouldPrune(b, e) {
		return true, 0, true, nil
	}
	cursor := string(b.Cursor(e))
	cache, err := h.emitRaw(b, e, targets, cursor, msgOverride)
	if err != nil {
		return false, 0, false, err
	}
	maxBytes := 0
	for _, ce := range cache {
		if len(ce.payload) > maxBytes {
			maxBytes = len(ce.payload)
		}
	}
	if len(cache) > 0 && h.cfg.IncLineStats != nil {
		h.cfg.IncLineStats(1, maxBytes)
	}
	return true, maxBytes, true, nil
}

// gateRaw runs the searches, the pump/reader match and the secret
// redaction — the entry-level gates that run before any sender. keep is
// false when the entry is dropped; otherwise msgOverride holds the
// redacted MESSAGE bytes (nil when nothing changed).
func (h *Handler) gateRaw(b *Batch, e *RawEntry) (msgOverride []byte, keep bool) {
	if h.cfg.Searcher != nil && len(h.cfg.Searcher.Searches()) > 0 {
		if !h.cfg.Searcher.MatchRawValues(func(field string) (string, bool) {
			return rawSearchValue(b, e, field)
		}) {
			return nil, false
		}
	}
	if !h.checkMatchRaw(b, e, h.cfg.PumpMatchKey, h.cfg.PumpMatchValue) {
		return nil, false
	}
	if !h.checkMatchRaw(b, e, h.cfg.ReaderMatchKey, h.cfg.ReaderMatchValue) {
		return nil, false
	}
	// Secret redaction mutates the shared entry before any sender runs, so
	// the redacted MESSAGE applies to every sender (Python data["MESSAGE"]).
	if len(h.cfg.SecretFilters) == 0 {
		return nil, true
	}
	v, ok := rawGroupValue(b, e, "MESSAGE", processedRawValue)
	if !ok {
		return nil, true
	}
	text, ok := v.(string)
	if !ok {
		return nil, true
	}
	out, changed := filter.ApplySecretFiltersRaw([]byte(text), h.cfg.SecretFilters, nil)
	if !changed {
		return nil, true
	}
	if h.cfg.SecretFilterMetrics && h.cfg.NoteSecretFilterMatch != nil {
		h.cfg.NoteSecretFilterMatch()
	}
	return append([]byte(nil), out...), true
}

// emitRaw runs the per-sender loop, returning the per-filter-key cache.
func (h *Handler) emitRaw(b *Batch, e *RawEntry, targets []SenderTarget, cursor string, msgOverride []byte) (map[string]rawCacheEntry, error) {
	cache := map[string]rawCacheEntry{}
	var accFragments []ExtraFragment
	var accFields []Field

	for _, target := range targets {
		ffName := ""
		if target.FieldFilter != nil {
			ffName = target.FieldFilter.Name()
		}
		if ce, ok := cache[ffName]; ok {
			if h.cacheHitPasses(target.UnitLogLevels, ce) && len(ce.payload) > 0 && target.AddItem != nil {
				target.AddItem(ce.payload, cursor, ce.indexDate)
			}
			continue
		}

		effFragments := concatFragments(accFragments, targetExtraFragments(target))
		effFields := concatFields(accFields, target.ExtraFieldValues)

		unit, priority := effectiveUnitPriority(b, e, effFields)
		if target.UnitLogLevels != nil && !target.UnitLogLevels.Decide(unit, priority) {
			// Dropped: Python merged the extras into the shared dict
			// BEFORE the unit check, so this sender still contaminates
			// later ones (ruling 1).
			accFragments = effFragments
			accFields = effFields
			continue
		}

		var indexDate [10]byte
		payload, err := EmitJSON(nil, b, e, &EmitOptions{
			Filter:          target.FieldFilter,
			Extras:          effFragments,
			MessageOverride: msgOverride,
			IndexDate:       &indexDate,
		})
		if err != nil {
			return nil, err
		}
		if len(payload) > MaxKafkaMessageSize {
			payload, err = h.truncateLongMessage(payload, rawEntryTimestamp(e))
			if err != nil {
				return nil, err
			}
		}

		cache[ffName] = rawCacheEntry{
			payload:      payload,
			empty:        len(payload) == 2,
			keptPriority: target.FieldFilter == nil || target.FieldFilter.Keep("PRIORITY"),
			keptUnit:     target.FieldFilter == nil || target.FieldFilter.Keep("SYSTEMD_UNIT"),
			unit:         unit,
			priority:     priority,
			indexDate:    indexDate,
		}
		accFragments = effFragments
		accFields = effFields
		if target.AddItem != nil {
			target.AddItem(payload, cursor, indexDate)
		}
	}
	return cache, nil
}

// cacheHitPasses applies ruling 2's cheap emulation of Python's
// filter_by_log_level over a cached payload: an empty payload is dropped
// (json.loads gives {}); a payload whose emitting filter removed either
// PRIORITY or SYSTEMD_UNIT passes (the key is absent in the parsed
// payload); otherwise the recorded views decide.
func (h *Handler) cacheHitPasses(unitLogLevels *filter.UnitLogLevel, ce rawCacheEntry) bool {
	if unitLogLevels == nil {
		return true
	}
	if ce.empty {
		return false
	}
	if !ce.keptPriority || !ce.keptUnit {
		return true
	}
	return unitLogLevels.Decide(ce.unit, ce.priority)
}

// checkMatchRaw is checkMatch (handler.go) over the raw entry's processed
// view.
func (h *Handler) checkMatchRaw(b *Batch, e *RawEntry, matchKey string, matchValue any) bool {
	if matchKey == "" {
		return true
	}
	if value, ok := rawGroupValue(b, e, matchKey, processedRawValue); ok {
		return PythonEqual(value, matchValue)
	}
	return matchValue == nil
}

// effectiveUnitPriority resolves the unit and priority views of a
// sender's effective entry (ruling 1): the entry's converted values,
// overridden by the effective extras in config order (an extra with the
// key wins, exactly as Python's data.update).
func effectiveUnitPriority(b *Batch, e *RawEntry, extras []Field) (string, any) {
	unit := ""
	if v, ok := rawGroupValue(b, e, "SYSTEMD_UNIT", processedRawValue); ok {
		if s, ok := v.(string); ok {
			unit = s
		}
	}
	var priority any
	if v, ok := rawGroupValue(b, e, "PRIORITY", processedRawValue); ok {
		priority = v
	}
	for _, extra := range extras {
		switch extra.Key {
		case "SYSTEMD_UNIT":
			if s, ok := extra.Value.(string); ok {
				unit = s
			} else {
				unit = ""
			}
		case "PRIORITY":
			priority = extra.Value
		}
	}
	return unit, priority
}

// rawSearchValue yields the search string for one raw field, mirroring
// searchFieldValue over the CONVERTED entry (the view PerformSearches
// receives: keys with underscores, invalid bytes still []byte, duplicate
// keys as a []any list — which never matches).
func rawSearchValue(b *Batch, e *RawEntry, field string) (string, bool) {
	v, ok := rawGroupValue(b, e, field, convertedRawValue)
	if !ok {
		return "", false
	}
	return searchStringValue(v)
}

// searchStringValue mirrors filter.searchFieldValue's value handling
// (which is unexported there) for a converted entry value.
func searchStringValue(v any) (string, bool) {
	switch typed := v.(type) {
	case nil:
		return "", false
	case string:
		if typed == "" {
			return "", false
		}
		return typed, true
	case bool:
		if typed {
			return "True", true
		}
		return "False", true
	case int64:
		return strconv.FormatInt(typed, 10), true
	case []byte:
		if len(typed) == 0 {
			return "", false
		}
		decoded, valid := decodeUTF8(typed)
		if !valid {
			return "", false
		}
		return decoded, true
	default:
		return "", false
	}
}

// rawGroupValue resolves the value of the field whose stripped key is
// name, reproducing the reader's conversion: a single occurrence is
// mapped through toValue; repeated occurrences become a []any list of the
// raw byte values (the reader merges duplicate keys before ConvertEntry,
// so the list elements are never converted). ok is false when absent.
func rawGroupValue(b *Batch, e *RawEntry, name string, toValue func(*Batch, *RawField) any) (any, bool) {
	first := -1
	count := 0
	for i := range e.Fields {
		if string(stripUnderscore(b.Key(&e.Fields[i]))) == name {
			if first < 0 {
				first = i
			}
			count++
		}
	}
	if first < 0 {
		return nil, false
	}
	if count == 1 {
		return toValue(b, &e.Fields[first]), true
	}
	list := make([]any, 0, count)
	for i := range e.Fields {
		field := &e.Fields[i]
		if string(stripUnderscore(b.Key(field))) != name {
			continue
		}
		list = append(list, append([]byte(nil), b.ValueBytes(field)...))
	}
	return list, true
}

// convertedRawValue converts one raw field value like ConvertEntry (the
// converter class of the raw key, then best-effort UTF-8 decoding),
// leaving invalid bytes as []byte.
func convertedRawValue(b *Batch, field *RawField) any {
	value := append([]byte(nil), b.ValueBytes(field)...)
	return convertValue(string(b.Key(field)), value)
}

// processedRawValue is convertedRawValue plus ProcessEntry's repr
// rewriting of undecodable byte values — the view check_match, the unit
// log level and the secret filters operate on.
func processedRawValue(b *Batch, field *RawField) any {
	value := convertedRawValue(b, field)
	if raw, ok := value.([]byte); ok {
		return pythonReprBytes(raw)
	}
	return value
}

// targetExtraFragments returns the sender's pre-rendered extras, falling
// back to rendering ExtraFieldValues for callers that only set those
// (the tests).
func targetExtraFragments(t SenderTarget) []ExtraFragment {
	if t.ExtraFragments != nil {
		return t.ExtraFragments
	}
	if len(t.ExtraFieldValues) == 0 {
		return nil
	}
	out := make([]ExtraFragment, 0, len(t.ExtraFieldValues))
	for _, field := range t.ExtraFieldValues {
		var buf bytes.Buffer
		if err := appendPythonJSON(&buf, field.Key, field.Value); err != nil {
			continue
		}
		out = append(out, ExtraFragment{Key: field.Key, JSON: buf.Bytes()})
	}
	return out
}

// concatFragments concatenates two fragment lists without sharing
// backing arrays with later appends.
func concatFragments(a, b []ExtraFragment) []ExtraFragment {
	if len(b) == 0 {
		return a
	}
	if len(a) == 0 {
		return b
	}
	out := make([]ExtraFragment, 0, len(a)+len(b))
	out = append(out, a...)
	out = append(out, b...)
	return out
}

// concatFields is concatFragments over []Field.
func concatFields(a, b []Field) []Field {
	if len(b) == 0 {
		return a
	}
	if len(a) == 0 {
		return b
	}
	out := make([]Field, 0, len(a)+len(b))
	out = append(out, a...)
	out = append(out, b...)
	return out
}

// rawEntryTimestamp is the truncation path's timestamp value, derived
// from the raw realtime exactly like the synthesised timestamp field.
func rawEntryTimestamp(e *RawEntry) any {
	timestamp, err := TimestampFromRealtime(float64(e.Realtime) / 1e6)
	if err != nil {
		return nil
	}
	return timestamp
}
