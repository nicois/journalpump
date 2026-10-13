// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package journal

import (
	"bytes"
	"strconv"
	"time"

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
	msgOverride, overrideSet, keep := h.gateRaw(b, e)
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
	cache, err := h.emitRaw(b, e, targets, cursor, msgOverride, overrideSet)
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
// false when the entry is dropped; otherwise overrideSet reports whether
// the secret filter changed MESSAGE (in which case msgOverride holds the
// redacted bytes, possibly EMPTY — Python writes data["MESSAGE"] = "").
func (h *Handler) gateRaw(b *Batch, e *RawEntry) (msgOverride []byte, overrideSet, keep bool) {
	if h.cfg.Searcher != nil && len(h.cfg.Searcher.Searches()) > 0 {
		if !h.cfg.Searcher.MatchRawValues(func(field string) (string, bool) {
			return rawSearchValue(b, e, field)
		}) {
			return nil, false, false
		}
	}
	if !h.checkMatchRaw(b, e, h.cfg.PumpMatchKey, h.cfg.PumpMatchValue) {
		return nil, false, false
	}
	if !h.checkMatchRaw(b, e, h.cfg.ReaderMatchKey, h.cfg.ReaderMatchValue) {
		return nil, false, false
	}
	// Secret redaction mutates the shared entry before any sender runs, so
	// the redacted MESSAGE applies to every sender (Python data["MESSAGE"]).
	if len(h.cfg.SecretFilters) == 0 {
		return nil, false, true
	}
	v, ok := rawGroupValue(b, e, "MESSAGE", processedRawValue)
	if !ok {
		return nil, false, true
	}
	text, ok := v.(string)
	if !ok {
		return nil, false, true
	}
	out, changed := filter.ApplySecretFiltersRaw([]byte(text), h.cfg.SecretFilters, nil)
	if !changed {
		return nil, false, true
	}
	if h.cfg.SecretFilterMetrics && h.cfg.NoteSecretFilterMatch != nil {
		h.cfg.NoteSecretFilterMatch()
	}
	return append([]byte(nil), out...), true, true
}

// emitRaw runs the per-sender loop, returning the per-filter-key cache.
func (h *Handler) emitRaw(b *Batch, e *RawEntry, targets []SenderTarget, cursor string, msgOverride []byte, overrideSet bool) (map[string]rawCacheEntry, error) {
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

		ownFragments := targetExtraFragments(target)
		effFragments := concatFragments(accFragments, ownFragments)
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

		// Python reassigns data["timestamp"] on EVERY cache miss
		// (journalpump.py:762-766), masking any earlier sender's timestamp
		// extra; an earlier REALTIME_TIMESTAMP extra feeds this miss's
		// computation. The sender's OWN timestamp extra is applied
		// afterwards and wins the emission (see emitFragments below).
		timestamp, err := h.effectiveTimestamp(e, accFields)
		if err != nil {
			return nil, err
		}
		tsISO := pythonISOFormat(timestamp)
		emitFragments := maskEarlierTimestamps(effFragments, len(accFragments))

		var indexDate [10]byte
		payload, err := EmitJSON(nil, b, e, &EmitOptions{
			Filter:             target.FieldFilter,
			Extras:             emitFragments,
			MessageOverride:    msgOverride,
			MessageOverrideSet: overrideSet,
			TimestampISO:       tsISO,
			IndexDate:          &indexDate,
		})
		if err != nil {
			return nil, err
		}
		keptPriority := target.FieldFilter == nil || target.FieldFilter.Keep("PRIORITY")
		keptUnit := target.FieldFilter == nil || target.FieldFilter.Keep("SYSTEMD_UNIT")
		if len(payload) > MaxKafkaMessageSize {
			payload, err = h.truncateLongMessage(payload, truncationTimestamp(target.FieldFilter, target.ExtraFieldValues, tsISO))
			if err != nil {
				return nil, err
			}
			// The truncation record {"error", "partial_data", "timestamp"}
			// carries no PRIORITY/SYSTEMD_UNIT, so Python's cache-hit
			// filter_by_level sees them absent and keeps the entry
			// (journalpump.py:791-794).
			keptPriority, keptUnit = false, false
		}

		cache[ffName] = rawCacheEntry{
			payload:      payload,
			empty:        len(payload) == 2,
			keptPriority: keptPriority,
			keptUnit:     keptUnit,
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
// searchFieldValue over the CONVERTED entry PerformSearches receives: keys
// WITH underscores (so the lookup is an EXACT raw-key match, never the
// stripped-key grouping), invalid bytes still []byte, duplicate keys as a
// []any list — which never matches. The reader's synthesised
// __REALTIME_TIMESTAMP / __SEQNUM keys are resolved too.
func rawSearchValue(b *Batch, e *RawEntry, field string) (string, bool) {
	switch field {
	case "__REALTIME_TIMESTAMP":
		return searchStringValue(convertValue("__REALTIME_TIMESTAMP", int64(e.Realtime)))
	case "__SEQNUM":
		seqnum, ok := ParseSeqnumFromCursor(string(b.Cursor(e)))
		if !ok {
			return "", false
		}
		return searchStringValue(seqnum)
	}
	var result any
	found := false
	for i := range e.Fields {
		if e.Fields[i].Duplicate {
			continue
		}
		if string(b.Key(&e.Fields[i])) != field {
			continue
		}
		result = groupFieldValue(b, e, i, convertedRawValue)
		found = true
	}
	if !found {
		return "", false
	}
	return searchStringValue(result)
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

// rawGroupValue resolves the PROCESSED entry value for the stripped key
// name, reproducing ProcessEntry: keys are stripped of leading
// underscores, repeated IDENTICAL raw keys were already merged into a
// list by the reader, and where two DIFFERENT raw keys strip to the same
// name the LAST one wins (dict assignment keeps the first position, the
// last value). ok is false when absent.
func rawGroupValue(b *Batch, e *RawEntry, name string, toValue func(*Batch, *RawField) any) (any, bool) {
	var result any
	found := false
	for i := range e.Fields {
		field := &e.Fields[i]
		if field.Duplicate {
			continue
		}
		if string(stripUnderscore(b.Key(field))) != name {
			continue
		}
		result = groupFieldValue(b, e, i, toValue)
		found = true
	}
	return result, found
}

// groupFieldValue is one distinct exact raw key's converted value: toValue
// for a single occurrence, or a []any list of the raw byte values when the
// reader merged repeated identical keys (python-systemd _get_all) — the
// list elements are never converted.
func groupFieldValue(b *Batch, e *RawEntry, index int, toValue func(*Batch, *RawField) any) any {
	dups := duplicateIndexes(b, e, index)
	if len(dups) == 0 {
		return toValue(b, &e.Fields[index])
	}
	list := make([]any, 0, len(dups)+1)
	list = append(list, append([]byte(nil), b.ValueBytes(&e.Fields[index])...))
	for _, d := range dups {
		list = append(list, append([]byte(nil), b.ValueBytes(&e.Fields[d])...))
	}
	return list
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

// effectiveTimestamp is Python's data["timestamp"] for a cache miss
// (journalpump.py:762-766): fromtimestamp(data["REALTIME_TIMESTAMP"]),
// where the value is the entry's converted realtime unless an EARLIER
// sender's extra (priorExtras) overrode it. The sender's own extras are
// applied by Python AFTER this assignment, so they do not feed it.
func (h *Handler) effectiveTimestamp(e *RawEntry, priorExtras []Field) (time.Time, error) {
	var realtime any
	found := false
	for _, extra := range priorExtras {
		if extra.Key == "REALTIME_TIMESTAMP" {
			realtime = extra.Value
			found = true
		}
	}
	if found {
		seconds, ok := realtime.(float64)
		if !ok {
			// Mirrors getOrGenerateJSON: a non-float REALTIME_TIMESTAMP
			// override falls back to now() (Python would raise; the oracle
			// keeps running).
			return h.now(), nil
		}
		return TimestampFromRealtime(seconds)
	}
	return TimestampFromRealtime(float64(e.Realtime) / 1e6)
}

// maskEarlierTimestamps drops the "timestamp" fragments contributed by
// EARLIER senders (the first priorCount entries) so the synth timestamp
// (this miss's reassignment) wins; the sender's own timestamp extra is
// kept and overrides it, exactly like Python's data["timestamp"] = ...
// followed by data.update(own_extras).
func maskEarlierTimestamps(fragments []ExtraFragment, priorCount int) []ExtraFragment {
	mask := false
	for i := 0; i < priorCount && i < len(fragments); i++ {
		if fragments[i].Key == "timestamp" {
			mask = true
			break
		}
	}
	if !mask {
		return fragments
	}
	out := make([]ExtraFragment, 0, len(fragments))
	for i := range fragments {
		if i < priorCount && fragments[i].Key == "timestamp" {
			continue
		}
		out = append(out, fragments[i])
	}
	return out
}

// truncationTimestamp is Python's _truncate_long_message timestamp
// argument, data.get("timestamp") over the FIELD-FILTERED data: nil when
// the filter drops the timestamp key, otherwise the sender's own
// timestamp extra (the last one) or the synthesised ISO timestamp.
func truncationTimestamp(ff *filter.FieldFilter, ownExtras []Field, tsISO string) any {
	if ff != nil && !ff.Keep("timestamp") {
		return nil
	}
	for i := len(ownExtras) - 1; i >= 0; i-- {
		if ownExtras[i].Key == "timestamp" {
			return ownExtras[i].Value
		}
	}
	return tsISO
}
