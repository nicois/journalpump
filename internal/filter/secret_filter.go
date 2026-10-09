// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package filter

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/Aiven-Open/journalpump/internal/config"
)

// SecretFilter ports the compiled form of one reader "secret_filters"
// entry (journalpump.py:549-574, 820-830). Python compiles each pattern
// with `re` at reader construction and applies `pattern.sub(replacement,
// MESSAGE)`; the Go port compiles with RE2.
//
// Two RE2 gaps are handled at construction:
//   - RE2 does not support backreferences, lookarounds, atomic groups,
//     possessive quantifiers and a few other Python-valid constructs.
//     Python accepts such patterns, so rejecting them would deviate from
//     Python's acceptance: instead the filter is DISABLED and a startup
//     warning is returned (plan Task 5, "startup warning for
//     unsupported constructs").
//   - The Python replacement template (\1, \g<name>, octal escapes,
//     \n-style escapes) is translated to Go's $-template syntax at load
//     time, with Python's template compile rules (which Python applies
//     lazily at the first substitution; a failing template crashes
//     mid-run there, so rejecting it at load is the sanctioned
//     strict-at-load deviation, spec §6.1a).
type SecretFilter struct {
	pattern string
	// replacement is the Go replacement template, already translated
	// from the Python template.
	replacement string
	regex       *regexp.Regexp
}

// NewSecretFilters validates and compiles the configured secret filters
// in configuration order. Warnings are returned for filters disabled
// because of RE2-unsupported constructs; they are startup warnings, not
// errors.
func NewSecretFilters(filters []config.SecretFilterConfig) (compiled []*SecretFilter, warnings []string, err error) {
	compiled = make([]*SecretFilter, 0, len(filters))
	for _, cfg := range filters {
		if desc := findUnsupportedConstruct(cfg.Pattern); desc != "" {
			warnings = append(warnings, fmt.Sprintf(
				"secret filter pattern %q uses %s, which the Go regexp engine does not support; the filter was disabled",
				cfg.Pattern, desc,
			))
			continue
		}
		regex, err := regexp.Compile(cfg.Pattern)
		if err != nil {
			// Python: re.compile raises re.error -> ValueError("invalid
			// secret_filters configuration - invalid regex").
			return nil, nil, fmt.Errorf("invalid secret_filters configuration - invalid regex: %w", err)
		}
		replacement, err := translateReplacement(cfg.Replacement, regex.NumSubexp(), subexpNames(regex))
		if err != nil {
			return nil, nil, fmt.Errorf("invalid secret_filters configuration - invalid replacement: %w", err)
		}
		compiled = append(compiled, &SecretFilter{
			pattern:     cfg.Pattern,
			replacement: replacement,
			regex:       regex,
		})
	}
	return compiled, warnings, nil
}

// Pattern returns the configured Python-syntax pattern (for logs and
// metrics tags).
func (f *SecretFilter) Pattern() string {
	return f.pattern
}

// subexpNames returns the named capture groups of the pattern (group
// number -> name), for \g<name> validation.
func subexpNames(regex *regexp.Regexp) map[string]int {
	names := make(map[string]int)
	for i, name := range regex.SubexpNames() {
		if name != "" {
			names[name] = i
		}
	}
	return names
}

// findUnsupportedConstruct reports the first Python-valid construct in
// the pattern that RE2 cannot compile, or "" when the pattern is fully
// expressible. The scan tracks character-class context because inside
// [...] Python's \1 is an octal escape, not a backreference, and class
// bodies cannot contain the group constructs.
func findUnsupportedConstruct(pattern string) string {
	i, n := 0, len(pattern)
	inClass := false
	for i < n {
		c := pattern[i]
		switch {
		case c == '\\':
			if i+1 >= n {
				// Trailing backslash: invalid in Python too; let the
				// compiler produce the real error.
				return ""
			}
			if !inClass {
				if desc := unsupportedEscape(pattern[i+1]); desc != "" {
					return desc
				}
			}
			i += 2
		case !inClass && c == '(' && i+1 < n && pattern[i+1] == '?':
			desc, advance := unsupportedGroupConstruct(pattern[i+2:])
			if desc != "" {
				return desc
			}
			i += 2 + advance
		case c == '[':
			inClass = true
			i++
			if i < n && pattern[i] == '^' {
				i++
			}
			// A ']' first in the class is a literal in Python and Go.
			if i < n && pattern[i] == ']' {
				i++
			}
		case inClass && c == ']':
			inClass = false
			i++
		case !inClass && (c == '*' || c == '+' || c == '?') && i+1 < n && pattern[i+1] == '+':
			return "a possessive quantifier"
		case !inClass && c == '{':
			// Brace repeats: Python accepts {,n} (empty minimum,
			// RE2-invalid) and {n,m}+ (possessive, RE2-invalid). Braces
			// that are not a Python quantifier are literal characters in
			// both engines and need no check.
			desc, advance := unsupportedBraceConstruct(pattern, i)
			if desc != "" {
				return desc
			}
			i += advance
		default:
			i++
		}
	}
	return ""
}

// unsupportedBraceConstruct reports RE2-incompatible brace quantifiers
// at pattern[i:] ("{,n}" with its empty minimum, possessive "{n,m}+")
// and returns how far the scanner must advance past the brace.
func unsupportedBraceConstruct(pattern string, i int) (string, int) {
	end := strings.IndexByte(pattern[i:], '}')
	if end < 0 || !isQuantifierContent(pattern[i+1:i+end]) {
		return "", 1
	}
	content := pattern[i+1 : i+end]
	if content[0] == ',' {
		return "a {,n} quantifier", end + 1
	}
	if i+end+1 < len(pattern) && pattern[i+end+1] == '+' {
		return "a possessive quantifier", end + 1
	}
	return "", end + 1
}

// unsupportedEscape reports constructs behind an out-of-class backslash.
func unsupportedEscape(next byte) string {
	if next >= '1' && next <= '9' {
		return "a backreference (\"\\" + string(next) + "\")"
	}
	if next == 'Z' {
		return "the \\Z anchor"
	}
	if next == 'N' {
		return "the \\N escape"
	}
	return ""
}

// unsupportedGroupConstruct reports RE2-incompatible constructs behind
// "(?" (rest starts after those two characters) and returns how many
// extra characters the scanner must skip.
func unsupportedGroupConstruct(rest string) (string, int) {
	switch {
	case strings.HasPrefix(rest, "="), strings.HasPrefix(rest, "!"):
		return "a lookahead assertion", 0
	case strings.HasPrefix(rest, "<="), strings.HasPrefix(rest, "<!"):
		return "a lookbehind assertion", 0
	case strings.HasPrefix(rest, ">"):
		return "an atomic group", 0
	case strings.HasPrefix(rest, "("):
		return "a conditional group", 0
	case strings.HasPrefix(rest, "P="):
		return "a backreference", 0
	case strings.HasPrefix(rest, "#"):
		return "a comment group", 0
	case len(rest) > 0 && strings.IndexByte("aiLmsux", rest[0]) >= 0:
		// (?flags) or (?flags:...). Flags invalid for RE2: L (locale),
		// u (Python's Unicode flag; Go's U means ungreedy), x (verbose).
		// i, m, s are fine in both.
		flags := rest[:flagsEnd(rest)]
		for _, flag := range flags {
			if flag == 'L' || flag == 'u' || flag == 'x' {
				return "the (?" + string(flag) + ") flag", 0
			}
		}
		return "", len(flags)
	default:
		return "", 0
	}
}

// isQuantifierContent reports whether the text between { and } is a
// Python quantifier: {n}, {n,}, {n,m}, {,m} or {,} (digits and at most
// one comma, with at least one digit or exactly one bare comma).
func isQuantifierContent(content string) bool {
	if content == "" {
		return false
	}
	commas := 0
	digits := 0
	for i := 0; i < len(content); i++ {
		switch {
		case isDecimalDigit(content[i]):
			digits++
		case content[i] == ',':
			commas++
			if commas > 1 {
				return false
			}
		default:
			return false
		}
	}
	return digits > 0 || content == ","
}

// flagsEnd returns the length of the leading flag letters of a (?flags)
// body (letters a-i-L-m-s-u-x).
func flagsEnd(rest string) int {
	i := 0
	for i < len(rest) && strings.IndexByte("aiLmsux", rest[i]) >= 0 {
		i++
	}
	return i
}

// translateReplacement ports re._parser.parse_template: it validates the
// Python replacement template against the compiled pattern's groups and
// renders the equivalent Go replacement template.
//
// Python rules (verified against CPython 3.14):
//   - \\ -> literal backslash; the next char is then literal
//   - \g<name> / \g<N> -> group, validated (\g<0> is the whole match;
//     unknown names and out-of-range numbers are errors)
//   - \0 plus up to two more octal digits -> that character (masked to
//     0xff)
//   - \1..\9: one or two decimal digits; three octal digits form an
//     octal escape (<= 0o377), otherwise the digits are a group reference
//   - \a \b \f \n \r \t \v -> control characters
//   - other ASCII letters -> "bad escape" error
//   - any other character after backslash (including non-letters) ->
//     literal backslash + character
//   - a trailing lone backslash -> "bad escape (end of pattern)"
//
// Group references render as Go $N / ${name}; literal dollar signs are
// escaped as $$ so Go never treats them as group references.
func translateReplacement(repl string, numGroups int, groupNames map[string]int) (string, error) {
	var out strings.Builder
	i, n := 0, len(repl)
	for i < n {
		c := repl[i]
		if c != '\\' {
			if c == '$' {
				out.WriteString("$$")
			} else {
				out.WriteByte(c)
			}
			i++
			continue
		}
		if i+1 >= n {
			return "", errors.New("bad escape (end of pattern)")
		}
		next := repl[i+1]
		switch {
		case next == '\\':
			out.WriteByte('\\')
			i += 2
		case next == 'g':
			name, err := parseTemplateName(repl, i)
			if err != nil {
				return "", err
			}
			if isASCIIDecimal(name) {
				index, err := strconv.Atoi(name)
				if err != nil {
					return "", fmt.Errorf("invalid group reference %s", name)
				}
				// Python MAXGROUPS = 100.
				if index >= 100 {
					return "", fmt.Errorf("invalid group reference %d", index)
				}
				if index > numGroups {
					return "", fmt.Errorf("invalid group reference %d", index)
				}
			} else if _, ok := groupNames[name]; !ok {
				return "", fmt.Errorf("unknown group name %s", pythonRepr(name))
			}
			out.WriteString("${" + name + "}")
			i += 3 + len(name) + 1
		case next == '0':
			j := i + 1
			for count, k := 0, j; count < 2 && k < n && isOctalDigit(repl[k]); count, k = count+1, k+1 {
				j++
			}
			value, _ := strconv.ParseUint(repl[i+1:j], 8, 32)
			out.WriteString(string(rune(value & 0xff)))
			i = j
		case next >= '1' && next <= '9':
			if i+2 < n && isDecimalDigit(repl[i+2]) {
				if isOctalDigit(next) && isOctalDigit(repl[i+2]) && i+3 < n && isOctalDigit(repl[i+3]) {
					// Three octal digits: an octal escape.
					value, _ := strconv.ParseUint(repl[i+1:i+4], 8, 32)
					if value > 0o377 {
						return "", fmt.Errorf("octal escape value \\%s outside of range 0-0o377", repl[i+1:i+4])
					}
					out.WriteString(string(rune(value)))
					i += 4
					continue
				}
				index, _ := strconv.Atoi(repl[i+1 : i+3])
				if index > numGroups {
					return "", fmt.Errorf("invalid group reference %d", index)
				}
				out.WriteString("$" + strconv.Itoa(index))
				i += 3
				continue
			}
			index, _ := strconv.Atoi(repl[i+1 : i+2])
			if index > numGroups {
				return "", fmt.Errorf("invalid group reference %d", index)
			}
			out.WriteString("$" + strconv.Itoa(index))
			i += 2
		case next == 'a', next == 'b', next == 'f', next == 'n', next == 'r', next == 't', next == 'v':
			out.WriteString(map[byte]string{
				'a': "\a", 'b': "\b", 'f': "\f", 'n': "\n", 'r': "\r", 't': "\t", 'v': "\v",
			}[next])
			i += 2
		case isASCIILetter(next):
			return "", fmt.Errorf("bad escape \\%c", next)
		default:
			// Python keeps the backslash for any non-ASCII-letter escape
			// (e.g. "\-" stays "\-").
			out.WriteString(repl[i : i+2])
			i += 2
		}
	}
	return out.String(), nil
}

// parseTemplateName parses "\g<...>" at repl[i:] and returns the name.
func parseTemplateName(repl string, i int) (string, error) {
	if i+2 >= len(repl) || repl[i+2] != '<' {
		return "", errors.New("missing <")
	}
	rest := repl[i+3:]
	end := strings.IndexByte(rest, '>')
	if end < 0 {
		return "", errors.New("missing >, unterminated name")
	}
	name := rest[:end]
	if name == "" {
		return "", errors.New("missing group name")
	}
	return name, nil
}

func isOctalDigit(c byte) bool { return c >= '0' && c <= '7' }

func isDecimalDigit(c byte) bool { return c >= '0' && c <= '9' }

func isASCIILetter(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

// isASCIIDecimal reports whether s consists only of ASCII digits
// (Python: name.isdecimal() and name.isascii()).
func isASCIIDecimal(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if !isDecimalDigit(s[i]) {
			return false
		}
	}
	return true
}

// pythonRepr renders a string with Python str repr() quoting, for error
// messages that quote configuration values the way Python does (%r).
func pythonRepr(s string) string {
	quote := byte('\'')
	if strings.ContainsRune(s, '\'') && !strings.ContainsRune(s, '"') {
		quote = '"'
	}
	var out strings.Builder
	out.WriteByte(quote)
	for _, r := range s {
		switch r {
		case '\\', rune(quote):
			out.WriteByte('\\')
			out.WriteRune(r)
		case '\n':
			out.WriteString(`\n`)
		case '\r':
			out.WriteString(`\r`)
		case '\t':
			out.WriteString(`\t`)
		default:
			out.WriteRune(r)
		}
	}
	out.WriteByte(quote)
	return out.String()
}

// ApplySecretFilters applies every compiled secret filter to the entry's
// MESSAGE field (journalpump.py:820-830). A falsy MESSAGE is left
// untouched, like Python's `if msg:` guard. The returned bool reports
// whether the MESSAGE changed; the caller increments the reader's
// secret_filter_matches counter when the change reporting is enabled
// (Python increments once per entry regardless of how many filters
// matched).
func ApplySecretFilters(data map[string]any, filters []*SecretFilter) bool {
	msg := data["MESSAGE"] // Python: data.get("MESSAGE")
	original := msg
	if truthy(msg) {
		if text, ok := msg.(string); ok {
			for _, f := range filters {
				text = f.regex.ReplaceAllString(text, f.replacement)
			}
			msg = text
		}
		// A truthy non-string MESSAGE makes Python's re.sub raise
		// TypeError mid-run; sanctioned crash-free deviation: leave the
		// value untouched.
	}
	if msg != original {
		data["MESSAGE"] = msg
		return true
	}
	return false
}
