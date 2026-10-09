// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

// Package tagged ports journalpump/senders/base.py:Tagged: the tag
// sanitization shared by senders and search metrics.
package tagged

import (
	"regexp"
	"strings"
)

// unsafeTagNameChars mirrors base.py:unsafe_tag_name_chars
// (`[^\w_\-]+`). Python's \w is Unicode-aware and matches exactly the
// letters, numbers and underscore (verified by a full-codepoint
// differential test), so the Go equivalent is [\pL\pN_].
var unsafeTagNameChars = regexp.MustCompile(`[^\pL\pN_\-]+`)

// unsafeTagValueChars mirrors base.py:unsafe_tag_value_chars
// (`[\s\|=:]+`). Python's \s matches exactly [\t-\r\x1c-\x1f\x85] plus
// the Unicode space categories (verified by a full-codepoint
// differential test), so the Go equivalent is the explicit class below —
// not Go's ASCII-only \s.
var unsafeTagValueChars = regexp.MustCompile(`[\t-\r\x1c-\x1f\x85\pZ|=:]`)

// Tagged holds the base tags of a sender/reader and sanitizes tag
// name/value pairs the way base.py:make_tags does.
type Tagged struct {
	tags map[string]string
}

// New copies tags as the base tag set (Python: Tagged.__init__ copies
// `tags or {}`).
func New(tags map[string]string) *Tagged {
	copied := make(map[string]string, len(tags))
	for key, value := range tags {
		copied[key] = value
	}
	return &Tagged{tags: copied}
}

// ReplaceTags replaces the base tag set (base.py:replace_tags).
func (t *Tagged) ReplaceTags(tags map[string]string) {
	copied := make(map[string]string, len(tags))
	for key, value := range tags {
		copied[key] = value
	}
	t.tags = copied
}

// Tags returns a copy of the base tag set.
func (t *Tagged) Tags() map[string]string {
	return t.MakeTags(nil)
}

// MakeTags merges tags over the base tags, sanitizing every incoming
// name/value. Python skips pairs whose name or value is None; Go string
// maps cannot hold nil, and callers of the Python code only pass None
// for absent regex groups (which internal/filter omits), so the check
// has no Go equivalent.
func (t *Tagged) MakeTags(tags map[string]string) map[string]string {
	output := make(map[string]string, len(t.tags)+len(tags))
	for key, value := range t.tags {
		output[key] = value
	}
	for name, value := range tags {
		output[SanitizeName(name)] = SanitizeValue(value)
	}
	return output
}

// SanitizeName substitutes unsafe tag-name characters with "_" and trims
// leading/trailing underscores (base.py:unsafe_tag_name_chars +
// .strip("_")).
func SanitizeName(name string) string {
	return strings.Trim(unsafeTagNameChars.ReplaceAllString(name, "_"), "_")
}

// SanitizeValue substitutes unsafe tag-value characters with "_" and
// trims leading/trailing underscores (base.py:unsafe_tag_value_chars +
// .strip("_")).
func SanitizeValue(value string) string {
	return strings.Trim(unsafeTagValueChars.ReplaceAllString(value, "_"), "_")
}
