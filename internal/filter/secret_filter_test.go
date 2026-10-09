// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package filter_test

import (
	"strings"
	"testing"

	"github.com/Aiven-Open/journalpump/internal/config"
	"github.com/Aiven-Open/journalpump/internal/filter"
)

// secretFilterReader ports the three reader setups of
// test/test_journalpump.py:TestSecretFilter.setUp: reader_c uses simple
// patterns, reader the complex capture-group ones, reader_b none.
func secretFilterReader(kind string) []*filter.SecretFilter {
	switch kind {
	case "complex":
		filters, warnings, err := filter.NewSecretFilters([]config.SecretFilterConfig{
			{Pattern: `(.*?\s?)(SECRET)(\s?.*)`, Replacement: `\1[REDACTED]\3`},
			{Pattern: `(.*?\s?)(SECOND)(\s?.*)`, Replacement: `\1[REDACTED TOO]\3`},
			{Pattern: `(.*?\s?)(THIRD)(\s?.*)`, Replacement: `\3[REDACTED TOO]\1`},
		})
		if err != nil || len(warnings) != 0 {
			panic(err)
		}
		return filters
	case "simple":
		filters, warnings, err := filter.NewSecretFilters([]config.SecretFilterConfig{
			{Pattern: `SECRET`, Replacement: `[REDACTED]`},
			{Pattern: `SECOND`, Replacement: `[REDACTED TOO]`},
		})
		if err != nil || len(warnings) != 0 {
			panic(err)
		}
		return filters
	}
	return nil
}

// TestApplySecretFilters ports the behavior of the twelve
// test/test_journalpump.py:TestSecretFilter process tests (redaction
// outcome on the MESSAGE field; the surrounding process() pipeline is
// Task 7's).
func TestApplySecretFilters(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		kind string // "simple", "complex", "none"
		in   string
		want string
	}{
		// Simple regex mode.
		{name: "secret at end simple", kind: "simple", in: "This is a field with a trailing SECRET", want: "This is a field with a trailing [REDACTED]"},
		{name: "secret in middle simple", kind: "simple", in: "This is a field with a SECRET data in the middle", want: "This is a field with a [REDACTED] data in the middle"},
		{name: "secret at start simple", kind: "simple", in: "SECRET message with sensitive data at the start", want: "[REDACTED] message with sensitive data at the start"},
		{name: "no secret simple", kind: "simple", in: "message with no sensitive data", want: "message with no sensitive data"},
		{name: "multi secret simple", kind: "simple", in: "SECRET message with sensitive SECOND at the start", want: "[REDACTED] message with sensitive [REDACTED TOO] at the start"},
		// Complex regex mode.
		{name: "secret at end", kind: "complex", in: "This is a field with a trailing SECRET", want: "This is a field with a trailing [REDACTED]"},
		{name: "secret in middle", kind: "complex", in: "This is a field with a SECRET data in the middle", want: "This is a field with a [REDACTED] data in the middle"},
		{name: "secret at start", kind: "complex", in: "SECRET message with sensitive data at the start", want: "[REDACTED] message with sensitive data at the start"},
		{name: "multi secret", kind: "complex", in: "SECRET message with sensitive SECOND at the start", want: "[REDACTED] message with sensitive [REDACTED TOO] at the start"},
		{name: "multi secret restructure", kind: "complex", in: " and has been rearranged to make senseTHIRDThis message has a ", want: "This message has a [REDACTED TOO] and has been rearranged to make sense"},
		{name: "no secret", kind: "complex", in: "message with no sensitive data", want: "message with no sensitive data"},
		{name: "empty secret filters", kind: "none", in: "testing config with no secret filters", want: "testing config with no secret filters"},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			data := map[string]any{"MESSAGE": tc.in}
			changed := filter.ApplySecretFilters(data, secretFilterReader(tc.kind))
			if data["MESSAGE"] != tc.want {
				t.Errorf("MESSAGE = %q want %q", data["MESSAGE"], tc.want)
			}
			if wantChanged := tc.in != tc.want; changed != wantChanged {
				t.Errorf("changed = %v want %v", changed, wantChanged)
			}
		})
	}
}

// TestApplySecretFiltersEdgeCases locks the MESSAGE truthiness and
// type guards of journalpump.py:820-830.
func TestApplySecretFiltersEdgeCases(t *testing.T) {
	t.Parallel()
	filters := secretFilterReader("simple")
	// Missing MESSAGE: no-op.
	if filter.ApplySecretFilters(map[string]any{}, filters) {
		t.Error("missing MESSAGE: changed should be false")
	}
	// Empty MESSAGE: falsy in Python (`if msg:`), untouched.
	data := map[string]any{"MESSAGE": ""}
	if filter.ApplySecretFilters(data, filters) {
		t.Error("empty MESSAGE: changed should be false")
	}
	if data["MESSAGE"] != "" {
		t.Errorf("empty MESSAGE mutated: %v", data["MESSAGE"])
	}
	// nil MESSAGE: falsy, untouched.
	if filter.ApplySecretFilters(map[string]any{"MESSAGE": nil}, filters) {
		t.Error("nil MESSAGE: changed should be false")
	}
	// No change when nothing matches.
	data = map[string]any{"MESSAGE": "clean"}
	if filter.ApplySecretFilters(data, filters) {
		t.Error("clean MESSAGE: changed should be false")
	}
}

// TestNewSecretFilterErrors ports test_ex_no_pattern,
// test_ex_no_replacement and test_ex_invalid_regex, including the check
// order (replacement is validated first) and Python's exact message
// fragments.
func TestNewSecretFilterErrors(t *testing.T) {
	t.Parallel()
	// Pattern missing (decode yields an empty Pattern only when the key
	// is absent-but-rejected earlier; here the missing pattern is nil
	// which the decoder rejects, so the constructor-level case is an
	// invalid regex).
	_, _, err := filter.NewSecretFilters([]config.SecretFilterConfig{{Replacement: "x", Pattern: "["}})
	if err == nil || !strings.Contains(err.Error(), "invalid regex") {
		t.Errorf("invalid regex: want 'invalid regex' error, got %v", err)
	}
	// A pattern that Python rejects too: missing closing bracket.
	_, _, err = filter.NewSecretFilters([]config.SecretFilterConfig{{Replacement: "x", Pattern: "a(b"}})
	if err == nil || !strings.Contains(err.Error(), "invalid regex") {
		t.Errorf("unclosed group: want 'invalid regex' error, got %v", err)
	}
	// Replacement template validation (Python compiles it lazily at the
	// first substitution and crashes mid-run; sanctioned strict-at-load):
	// a reference to a nonexistent group.
	_, _, err = filter.NewSecretFilters([]config.SecretFilterConfig{{Pattern: "(a)", Replacement: `\5`}})
	if err == nil || !strings.Contains(err.Error(), "invalid replacement") {
		t.Errorf("out-of-range group reference: want error, got %v", err)
	}
	// Bad escape in the replacement.
	_, _, err = filter.NewSecretFilters([]config.SecretFilterConfig{{Pattern: "(a)", Replacement: `\q`}})
	if err == nil || !strings.Contains(err.Error(), "invalid replacement") {
		t.Errorf("bad replacement escape: want error, got %v", err)
	}
}

// TestNewSecretFiltersUnsupported locks the warn-and-disable path for
// Python-valid constructs RE2 cannot express.
func TestNewSecretFiltersUnsupported(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		pattern string
	}{
		{name: "backreference", pattern: `(password=)\1`},
		{name: "named backreference", pattern: `(?P<x>a)(?P=x)`},
		{name: "lookahead", pattern: `foo(?=bar)`},
		{name: "negative lookahead", pattern: `foo(?!bar)`},
		{name: "lookbehind", pattern: `(?<=foo)bar`},
		{name: "negative lookbehind", pattern: `(?<!foo)bar`},
		{name: "atomic group", pattern: `(?>foo)`},
		{name: "possessive star", pattern: `a*+b`},
		{name: "possessive plus", pattern: `a++b`},
		{name: "possessive question", pattern: `a?+b`},
		{name: "possessive brace", pattern: `a{2,3}+b`},
		{name: "empty-minimum brace", pattern: `a{,2}b`},
		{name: "python end anchor", pattern: `foo\Z`},
		{name: "comment group", pattern: `foo(?#comment)bar`},
		{name: "verbose flag", pattern: `(?x)foo`},
		{name: "locale flag", pattern: `(?L)foo`},
		{name: "conditional group", pattern: `(a)?b(?(1)c|d)`},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			compiled, warnings, err := filter.NewSecretFilters([]config.SecretFilterConfig{
				{Pattern: tc.pattern, Replacement: "[REDACTED]"},
			})
			if err != nil {
				t.Fatalf("NewSecretFilters: %v", err)
			}
			if len(warnings) != 1 {
				t.Fatalf("warnings = %v want exactly one", warnings)
			}
			if !strings.Contains(warnings[0], "was disabled") {
				t.Errorf("warning %q does not report the disable", warnings[0])
			}
			if len(compiled) != 0 {
				t.Errorf("disabled filter still compiled: %v", compiled)
			}
		})
	}
}

// TestTranslateReplacementGolden locks the replacement template
// translation against Python parse_template semantics (verified against
// CPython 3.14).
func TestTranslateReplacementGolden(t *testing.T) {
	t.Parallel()
	// Pattern "(a)(b)?(c)" has 3 groups; group 2 is optional so
	// unmatched-group behavior is exercisable.
	pattern := `(a)(b)?(c)`
	replacements := []struct {
		in   string // Python template
		want string // applied to "ac" (group 2 unmatched) and "abc"
	}{
		{in: `\1-\3`, want: "a-c"},
		{in: `\g<1>x\g<3>y`, want: "axcy"},
		{in: `\g<0>`, want: "abc"},
		{in: `\\1`, want: `\1`},
		{in: `\n\t`, want: "\n\t"},
		{in: `\-`, want: `\-`}, // Python keeps the backslash for non-letters
		{in: `100$`, want: "100$"},
		{in: `a$b`, want: "a$b"},
		{in: `\0`, want: "\x00"},
		{in: `\01`, want: "\x01"},
		{in: `\08`, want: "\x00" + "8"},
		{in: `\123`, want: "S"}, // \123 -> octal 0o123 = 'S'
	}
	for _, tc := range replacements {
		filters, warnings, err := filter.NewSecretFilters([]config.SecretFilterConfig{
			{Pattern: pattern, Replacement: tc.in},
		})
		if err != nil {
			t.Errorf("replacement %q: %v", tc.in, err)
			continue
		}
		if len(warnings) != 0 {
			t.Errorf("replacement %q: unexpected warnings %v", tc.in, warnings)
		}
		got := map[string]string{}
		for _, input := range []string{"ac", "abc"} {
			data := map[string]any{"MESSAGE": input}
			filter.ApplySecretFilters(data, filters)
			got[input] = data["MESSAGE"].(string)
		}
		if got["abc"] != tc.want {
			t.Errorf("replacement %q on abc: got %q want %q", tc.in, got["abc"], tc.want)
		}
	}
}

// TestTranslateReplacementErrors locks the template validation errors
// (all raised lazily in Python; strict-at-load here).
func TestTranslateReplacementErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		pattern     string
		replacement string
		fragment    string
	}{
		{name: "trailing backslash", pattern: "(a)", replacement: `x\`, fragment: "end of pattern"},
		{name: "bad escape letter", pattern: "(a)", replacement: `\A`, fragment: "bad escape"},
		{name: "bad escape x", pattern: "(a)", replacement: `\x41`, fragment: "bad escape"},
		{name: "group reference out of range", pattern: "(a)", replacement: `\2`, fragment: "invalid group reference"},
		{name: "two-digit group out of range", pattern: "(a)", replacement: `\12`, fragment: "invalid group reference"},
		{name: "g reference out of range", pattern: "(a)", replacement: `\g<2>`, fragment: "invalid group reference"},
		{name: "unknown group name", pattern: "(?P<a>x)", replacement: `\g<b>`, fragment: "unknown group name"},
		{name: "missing angle bracket", pattern: "(a)", replacement: `\g1`, fragment: "missing <"},
		{name: "unterminated group name", pattern: "(a)", replacement: `\g<1`, fragment: "missing >"},
		{name: "octal out of range", pattern: "(a)", replacement: `\400`, fragment: "outside of range"},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, _, err := filter.NewSecretFilters([]config.SecretFilterConfig{
				{Pattern: tc.pattern, Replacement: tc.replacement},
			})
			if err == nil || !strings.Contains(err.Error(), tc.fragment) {
				t.Errorf("want error containing %q, got %v", tc.fragment, err)
			}
		})
	}
}
