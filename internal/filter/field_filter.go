// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

// Package filter ports the Python filter objects: field filters,
// unit log levels, secret filters and searches
// (journalpump/journalpump.py). All of them are construction-time
// objects captured by senders at sender construction (spec §3 reload
// corollary) and immutable afterwards.
package filter

import (
	"strings"

	"github.com/Aiven-Open/journalpump/internal/config"
)

// FieldFilter ports journalpump.py:FieldFilter. Filter objects are
// constructed once from the configuration and then CAPTURED by senders
// at sender-construction time (spec §3 reload corollary): when the
// readers blob is unchanged on reload, rebuilt filter objects never
// reach existing readers. FieldFilter is therefore immutable after
// construction.
type FieldFilter struct {
	name      string
	whitelist bool
	fields    map[string]struct{}
}

// NewFieldFilter builds the filter; the configured fields are
// trimmed of leading underscores and lowercased exactly like
// journalpump.py:660 (`f.lstrip("_").lower()`).
func NewFieldFilter(name string, cfg config.FieldFilterConfig) *FieldFilter {
	fields := make(map[string]struct{}, len(cfg.Fields))
	for _, field := range cfg.Fields {
		fields[strings.ToLower(strings.TrimLeft(field, "_"))] = struct{}{}
	}
	return &FieldFilter{
		name:      name,
		whitelist: cfg.Whitelist,
		fields:    fields,
	}
}

// Name returns the filter name (Python: self.name), used as the
// per-field-filter serialization cache key.
func (f *FieldFilter) Name() string {
	return f.name
}

// FilterFields returns a new map with the fields kept per the
// whitelist/blacklist mode: an entry key is kept when
// (key trimmed of leading underscores, lowercased, in fields) equals the
// whitelist flag. The input map is not modified.
func (f *FieldFilter) FilterFields(data map[string]any) map[string]any {
	result := make(map[string]any, len(data))
	for name, value := range data {
		_, member := f.fields[strings.ToLower(strings.TrimLeft(name, "_"))]
		if member == f.whitelist {
			result[name] = value
		}
	}
	return result
}
