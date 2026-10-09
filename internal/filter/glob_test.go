// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package filter_test

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/Aiven-Open/journalpump/internal/filter"
)

// TestTranslateGlobPatternMatchesPython verifies the fnmatch.translate
// port against a Python-generated matrix: every (glob, string) pair's
// expected match outcome was produced by fnmatch.fnmatchcase (the exact
// call journalpump.py:694 makes) by
// testdata/generate_golden.py.
func TestTranslateGlobPatternMatchesPython(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile("testdata/glob_match_matrix.json")
	if err != nil {
		t.Fatalf("read golden matrix: %v", err)
	}
	var matrix map[string]map[string]bool
	if err := json.Unmarshal(data, &matrix); err != nil {
		t.Fatalf("decode golden matrix: %v", err)
	}
	if len(matrix) == 0 {
		t.Fatal("empty golden matrix")
	}
	for glob, candidates := range matrix {
		re, err := filter.TranslateGlobPattern(glob)
		if err != nil {
			t.Errorf("glob %q: compile: %v", glob, err)
			continue
		}
		for candidate, want := range candidates {
			if got := re.MatchString(candidate); got != want {
				t.Errorf("glob %q vs %q: got match=%v want %v", glob, candidate, got, want)
			}
		}
	}
}

// TestTranslateGlobPatternStructure locks the translated regex structure
// for a few representative globs (the Python translation differs only in
// escaping style and the RE2-incompatible atomic groups, both covered by
// the comment on joinTranslatedGlob).
func TestTranslateGlobPatternStructure(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ glob, want string }{
		{"unit-a*", `(?s:^unit-a.*)\z`},
		{"*.service", `(?s:^.*\.service)\z`},
		{"*a*ab", `(?s:^.*?a.*ab)\z`},
		{"a*b*c", `(?s:^a.*?b.*c)\z`},
		{"[]]x", `(?s:^[]]x)\z`},
		{"[x", `(?s:^\[x)\z`},
		{"[a-c][!x]z", `(?s:^[a-c][^x]z)\z`},
		{"café*", `(?s:^café.*)\z`},
		{"[]", `(?s:^\[\])\z`},
		{"with space", `(?s:^with space)\z`},
	} {
		got, err := filter.TranslateGlobPattern(tc.glob)
		if err != nil {
			t.Errorf("glob %q: compile: %v", tc.glob, err)
			continue
		}
		if got.String() != tc.want {
			t.Errorf("glob %q: got %q want %q", tc.glob, got.String(), tc.want)
		}
	}
}
