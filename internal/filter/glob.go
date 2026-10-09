// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package filter

import (
	"regexp"
	"strings"
	"unicode/utf8"
)

// glob.go ports Python's fnmatch.translate (Lib/fnmatch.py, CPython
// 3.14) for the unit_log_levels service globs, which Python matches with
// fnmatch.fnmatch (case-sensitive on POSIX, like fnmatchcase).
//
// The generated regexes are Go RE2 regexes with two adjustments, both
// match-equivalent for full-string matching:
//   - Python wraps the translation as (?s:...)\z; Go supports both
//     constructs verbatim.
//   - Python 3.12+ emits atomic groups (?>.*?) around interior `*`
//     wildcards to bound backtracking. Go RE2 does not backtrack, and
//     for a fully anchored boolean match the atomic lazy form is
//     equivalent to plain .*?: the atomic lazy group locks the EARLIEST
//     occurrence of the following fixed piece, which leaves the longest
//     possible remainder, and matching a suffix pattern against a longer
//     remainder implies matching it against every suffix of it — so no
//     string is matched by one form and not the other.

// translateGlob ports fnmatch._translate(pat, '*', '.').
func translateGlob(pat string) ([]string, []int) {
	var parts []string
	var starIndices []int

	i, n := 0, len(pat)
	for i < n {
		c := pat[i]
		i++
		switch c {
		case '*':
			starIndices = append(starIndices, len(parts))
			parts = append(parts, "") // placeholder for the star
			// Compress consecutive `*` into one.
			for i < n && pat[i] == '*' {
				i++
			}
		case '?':
			parts = append(parts, ".")
		case '[':
			j := i
			if j < n && pat[j] == '!' {
				j++
			}
			if j < n && pat[j] == ']' {
				j++
			}
			for j < n && pat[j] != ']' {
				j++
			}
			if j >= n {
				parts = append(parts, `\[`)
			} else {
				stuff := pat[i:j]
				if !strings.Contains(stuff, "-") {
					stuff = strings.ReplaceAll(stuff, `\`, `\\`)
				} else {
					// Split on hyphens that start ranges, keeping the
					// chunk boundaries the Python algorithm uses.
					var chunks []string
					k := i + 1
					if i < len(pat) && pat[i] == '!' {
						k = i + 2
					}
					for {
						idx := strings.Index(pat[min(k, j):j], "-")
						if idx < 0 {
							break
						}
						k = min(k, j) + idx
						chunks = append(chunks, pat[i:k])
						i = k + 1
						k += 3
					}
					chunk := pat[i:j]
					if chunk != "" {
						chunks = append(chunks, chunk)
					} else {
						chunks[len(chunks)-1] += "-"
					}
					// Remove empty ranges — invalid in RE. Python compares
					// characters, so compare runes, not bytes.
					for k := len(chunks) - 1; k > 0; k-- {
						last, _ := utf8.DecodeLastRuneInString(chunks[k-1])
						first, _ := utf8.DecodeRuneInString(chunks[k])
						if last > first {
							chunks[k-1] = chunks[k-1][:len(chunks[k-1])-utf8.RuneLen(last)] + chunks[k][utf8.RuneLen(first):]
							chunks = append(chunks[:k], chunks[k+1:]...)
						}
					}
					// Escape backslashes and hyphens for set difference (--).
					// Hyphens that create ranges shouldn't be escaped.
					for k := range chunks {
						chunks[k] = strings.ReplaceAll(chunks[k], `\`, `\\`)
						chunks[k] = strings.ReplaceAll(chunks[k], "-", `\-`)
					}
					stuff = strings.Join(chunks, "-")
				}
				i = j + 1
				switch stuff {
				case "":
					// Empty range: never match. Python emits (?!), which RE2
					// cannot compile; this class matches nothing.
					parts = append(parts, `[^\s\S]`)
				case "!":
					// Negated empty range: match any character.
					parts = append(parts, ".")
				default:
					// Escape set operations (&&, ~~ and ||) so RE2 treats
					// them as literals, like Python does.
					for _, op := range []string{"&&", "~~", "||"} {
						escaped := `\` + op[:1] + `\` + op[1:]
						stuff = strings.ReplaceAll(stuff, op, escaped)
					}
					if strings.HasPrefix(stuff, "!") {
						stuff = "^" + stuff[1:]
					} else if strings.HasPrefix(stuff, "^") || strings.HasPrefix(stuff, "[") {
						stuff = `\` + stuff
					}
					parts = append(parts, "["+stuff+"]")
				}
			}
		default:
			parts = append(parts, quoteMetaRune(pat[i-1:i]))
		}
	}
	return parts, starIndices
}

// joinTranslatedGlob ports fnmatch._join_translated_parts without the
// atomic groups (see the file comment). Python applies the translation
// with re.match (start-anchored) plus the trailing \z; the Go regex
// carries the equivalent ^ anchor so plain MatchString has full-match
// semantics.
func joinTranslatedGlob(parts []string, starIndices []int) string {
	if len(starIndices) == 0 {
		return `(?s:^` + strings.Join(parts, "") + `)\z`
	}
	var buffer []string
	buffer = append(buffer, parts[:starIndices[0]]...)
	i := starIndices[0] + 1
	for _, j := range starIndices[1:] {
		buffer = append(buffer, ".*?")
		buffer = append(buffer, parts[i:j]...)
		i = j + 1
	}
	buffer = append(buffer, ".*")
	buffer = append(buffer, parts[i:]...)
	return `(?s:^` + strings.Join(buffer, "") + `)\z`
}

// TranslateGlobPattern compiles a shell glob into an equivalent Go
// regexp with Python fnmatch.fnmatch semantics (full-string, case-
// sensitive, `*` crosses nothing specially — with (?s) it matches
// everything including newlines).
func TranslateGlobPattern(pat string) (*regexp.Regexp, error) {
	parts, starIndices := translateGlob(pat)
	return regexp.Compile(joinTranslatedGlob(parts, starIndices))
}

// quoteMetaRune escapes RE2 metacharacters in a literal rune. Python's
// re.escape escapes a wider set (space, #, &, ...), but escaping more
// than needed is not valid RE2 syntax for those characters; escaping
// exactly the metacharacters produces the same matches.
func quoteMetaRune(s string) string {
	return regexp.QuoteMeta(s)
}
