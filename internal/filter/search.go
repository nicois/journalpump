// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package filter

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Aiven-Open/journalpump/internal/tagged"
)

// search.go ports journalpump.py:_build_searches (506-544) and
// perform_searches (579-640).
//
// Searches are ENTRY filters: process() drops an entry when searches are
// configured and none of them matches (`if self.reader.searches and not
// self.reader.perform_searches(jobject)`). They also feed per-search hit
// counters (persisted in the state file) and metrics tags. Search
// objects are constructed at reader construction and captured by senders
// (spec §3 reload corollary) — they are immutable after construction,
// except for the hit counter.

// TagFunc is a search tag callable: Python builds `lambda tags:
// f(tags, args)` for tag values like "ip_to_geohash(ip_address,5)"
// (journalpump.py:526-542). The registry is provided by the wiring
// (ip_to_geohash arrives with Task 6); construction-time validation
// happens against the fixed function name set of journalpump.py:513.
type TagFunc func(tags map[string]string, args []string) string

// knownTagFuncs mirrors journalpump.py:513-515 funcs dict keys.
var knownTagFuncs = map[string]bool{
	"ip_to_geohash": true,
}

// tagFuncRe mirrors journalpump.py:512.
var tagFuncRe = regexp.MustCompile(`(?P<func>[a-z_]+)\((?P<args>[a-z0-9_,]+)\)`)

// Search is one compiled search entry.
type Search struct {
	name string
	// fields holds the compiled field regexes in iteration order: sorted
	// non-MESSAGE fields first, then MESSAGE last (Python's dict
	// insertion order after the sorted rebuild, journalpump.py:520-524).
	fields []searchField
	// tags holds static tag values and tag function calls.
	tags map[string]searchTag
	// hits mirrors search["hits"] (journalpump.py:639): a mutable
	// counter used by the state file. Only the reader's goroutine
	// touches it.
	hits int64
}

type searchField struct {
	name  string
	regex *regexp.Regexp
}

// searchTag is either a static string value or a parsed tag function
// call.
type searchTag struct {
	static   string
	funcName string
	args     []string
}

// NewSearches compiles the raw searches configuration. Errors mirror the
// exceptions Python raises at reader construction (a broken search
// config crashes the pump there): structural problems are reported with
// Python's messages where Python has one ("Invalid tag function tag
// value %r", "Unknown tag function %r in %r") and with a strict
// key-qualified error for the crashes Python produces lazily.
//
// Quirk note: Python's `search.setdefault("tags", {})"` mutates the
// parsed config, so Python's running-config dump includes "tags" and
// "fields" keys the config never had; the Go port does not mutate the
// parsed document (sanctioned deviation, recorded in the parity doc).
func NewSearches(searches []any) ([]*Search, error) {
	result := make([]*Search, 0, len(searches))
	for _, raw := range searches {
		search, err := newSearch(raw)
		if err != nil {
			return nil, err
		}
		result = append(result, search)
	}
	return result, nil
}

func newSearch(raw any) (*Search, error) {
	search, ok := raw.(map[string]any)
	if !ok {
		return nil, errors.New("searches: each search must be an object")
	}
	nameValue, present := search["name"]
	if !present || nameValue == nil {
		return nil, errors.New("search: missing field 'name'")
	}
	name, ok := nameValue.(string)
	if !ok {
		return nil, errors.New("search.name: must be a string")
	}

	// fields: pop MESSAGE, compile the rest sorted, MESSAGE last.
	rawFields := map[string]any{}
	if fieldsValue, present := search["fields"]; present && fieldsValue != nil {
		rawFields, ok = fieldsValue.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("search %s: fields must be an object", pythonRepr(name))
		}
	}
	messagePattern := ""
	if value, present := rawFields["MESSAGE"]; present && !pythonFalsy(value) {
		// Python: `if message_pattern:` — a falsy non-string MESSAGE
		// pattern (0/false/null/"") is popped and silently dropped, not
		// compiled.
		pattern, err := asPatternString(value, "search "+pythonRepr(name)+" fields.MESSAGE")
		if err != nil {
			return nil, err
		}
		messagePattern = pattern
	}
	nonMessage := make([]string, 0, len(rawFields))
	for field := range rawFields {
		if field != "MESSAGE" {
			nonMessage = append(nonMessage, field)
		}
	}
	sort.Strings(nonMessage)
	fields := make([]searchField, 0, len(rawFields))
	for _, field := range nonMessage {
		regex, err := compileSearchField(rawFields[field], "search "+pythonRepr(name)+" fields."+field)
		if err != nil {
			return nil, err
		}
		fields = append(fields, searchField{name: field, regex: regex})
	}
	if messagePattern != "" {
		regex, err := regexp.Compile(messagePattern)
		if err != nil {
			return nil, fmt.Errorf("search %s fields.MESSAGE: invalid regex: %w", pythonRepr(name), err)
		}
		fields = append(fields, searchField{name: "MESSAGE", regex: regex})
	}

	// tags: static values and function calls.
	rawTags := map[string]any{}
	if tagsValue, present := search["tags"]; present && tagsValue != nil {
		rawTags, ok = tagsValue.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("search %s: tags must be an object", pythonRepr(name))
		}
	}
	tags := make(map[string]searchTag, len(rawTags))
	for tag, value := range rawTags {
		text, ok := value.(string)
		if !ok {
			// Python: `"(" in value` raises TypeError for non-strings at
			// reader construction; strict-at-load deviation mirrors it.
			return nil, fmt.Errorf("search %s tags.%s: must be a string", pythonRepr(name), pythonRepr(tag))
		}
		if strings.ContainsAny(text, "()") {
			match := tagFuncRe.FindStringSubmatch(text)
			if match == nil {
				return nil, fmt.Errorf("Invalid tag function tag value %s", pythonRepr(text)) //nolint:staticcheck // ST1005: mirrors Python's exact ValueError text
			}
			funcName := match[tagFuncRe.SubexpIndex("func")]
			if !knownTagFuncs[funcName] {
				return nil, fmt.Errorf("Unknown tag function %s in %s", pythonRepr(funcName), pythonRepr(text)) //nolint:staticcheck // ST1005: mirrors Python's exact ValueError text
			}
			argsText := match[tagFuncRe.SubexpIndex("args")]
			args := strings.Split(argsText, ",")
			tags[tag] = searchTag{funcName: funcName, args: args}
			continue
		}
		tags[tag] = searchTag{static: text}
	}

	return &Search{name: name, fields: fields, tags: tags}, nil
}

func asPatternString(value any, where string) (string, error) {
	text, ok := value.(string)
	if !ok {
		// Python: re.compile(non-string) raises TypeError at reader
		// construction; strict-at-load deviation mirrors it.
		return "", fmt.Errorf("%s: must be a string", where)
	}
	return text, nil
}

func compileSearchField(value any, where string) (*regexp.Regexp, error) {
	pattern, err := asPatternString(value, where)
	if err != nil {
		return nil, err
	}
	regex, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("%s: invalid regex: %w", where, err)
	}
	return regex, nil
}

// Name returns the search name (Python: search["name"]).
func (s *Search) Name() string {
	return s.name
}

// Hits returns the accumulated match count (Python: search["hits"],
// persisted in the state file as {search name: hits}).
func (s *Search) Hits() int64 {
	return s.hits
}

// Stats is the subset of the statsd client perform_searches uses
// (journalpump.py:612-617, 637-638).
type Stats interface {
	// Increase mirrors statsd.increase(metric, tags=...).
	Increase(metric string, tags map[string]string)
	// Gauge mirrors statsd.gauge(metric, value=..., tags=...).
	Gauge(metric string, value float64, tags map[string]string)
}

// Logger is the logging subset used for slow regex warnings; the
// standard library *log.Logger satisfies it.
type Logger interface {
	Printf(format string, args ...any)
}

// Searcher holds the compiled searches of one reader plus the reader
// context perform_searches needs (stats, base tags, slow-regex
// threshold).
type Searcher struct {
	searches []*Search
	// thresholdSeconds mirrors threshold_for_metric_emit (Python
	// compares a perf_counter duration in seconds against this int).
	thresholdSeconds float64
	stats            Stats
	tagged           *tagged.Tagged
	log              Logger
	// Now is the monotonic clock used for slow-regex measurement;
	// replaceable in tests.
	Now func() time.Time
}

// NewSearcher builds the reader-side search runner. Python's reader is
// Tagged(tags, reader=name); the base tags feed the slow-regex gauge.
func NewSearcher(searches []*Search, thresholdSeconds float64, stats Stats, tags *tagged.Tagged, log Logger) *Searcher {
	return &Searcher{
		searches:         searches,
		thresholdSeconds: thresholdSeconds,
		stats:            stats,
		tagged:           tags,
		log:              log,
		Now:              time.Now,
	}
}

// Searches exposes the compiled searches (reader get_state persists
// each search's hit count).
func (s *Searcher) Searches() []*Search {
	return s.searches
}

// PerformSearches runs all searches against a raw (pre-conversion)
// entry and reports whether at least one matched; the caller drops the
// entry when searches are configured and this returns false. Named
// groups only feed the per-search hit metrics: Python's results dict is
// used solely for its truthiness in process() and for stats tags, and
// make_tags drops None values (unmatched groups), so unmatched groups
// are omitted here. Group values that are empty are dropped too: RE2
// cannot distinguish a matched-empty group from an unmatched one, while
// Python emits `tag=` (an empty value) into the datagram for a
// matched-empty group and drops only the unmatched one — that delta is
// the documented deviation.
func (s *Searcher) PerformSearches(entry map[string]any) bool {
	byteFields := make(map[string]string)
	matched := false
	for _, search := range s.searches {
		allMatch := true
		tags := make(map[string]string)
		for _, field := range search.fields {
			line, ok := searchFieldValue(field.name, entry, byteFields)
			if !ok {
				allMatch = false
				break
			}
			match := measureSearch(s, field.regex, line)
			if match == nil {
				allMatch = false
				break
			}
			for i, name := range field.regex.SubexpNames() {
				if name != "" && match[i] != "" {
					tags[name] = match[i]
				}
			}
		}
		if !allMatch {
			continue
		}
		// Add static tags + possible callables (journalpump.py:629-634).
		for tag, spec := range search.tags {
			if spec.funcName != "" {
				if fn := tagFuncRegistry[spec.funcName]; fn != nil {
					tags[tag] = fn(tags, spec.args)
				}
				// Unregistered callables (ip_to_geohash until Task 6) are
				// skipped; construction already validated the name.
				continue
			}
			tags[tag] = spec.static
		}
		if s.stats != nil {
			s.stats.Increase(search.name, s.tagged.MakeTags(tags))
		}
		search.hits++
		matched = true
	}
	return matched
}

// measureSearch runs one regex.search with slow-regex metric emission
// (journalpump.py:608-618).
func measureSearch(s *Searcher, regex *regexp.Regexp, line string) []string {
	start := s.Now()
	match := regex.FindStringSubmatch(line)
	duration := s.Now().Sub(start)
	if duration.Seconds() > s.thresholdSeconds {
		if s.stats != nil {
			s.stats.Gauge("journal.perform_search_regex_duration", duration.Seconds(),
				s.tagged.MakeTags(map[string]string{"regex": regex.String()}))
		}
		if s.log != nil {
			// Python logs "for duration %s seconds" with the float
			// perf_counter seconds, not a Duration rendering.
			s.log.Printf("Slow regex search: %s for duration %v seconds", regex.String(), duration.Seconds())
		}
	}
	return match
}

// tagFuncRegistry holds the callable implementations; journalpump.py
// builds them from JournalPump methods (ip_to_geohash, Task 6).
//
// TODO(Task 6): when ip_to_geohash is wired, move this package global
// into a Searcher field populated at construction — the charter prefers
// explicit wiring over package-level mutable state.
var tagFuncRegistry = map[string]TagFunc{}

// RegisterTagFunc registers a tag callable implementation.
func RegisterTagFunc(name string, fn TagFunc) {
	tagFuncRegistry[name] = fn
}

// searchFieldValue mirrors journalpump.py:587-606: missing or empty
// field -> no match (Python `if not line`); int/bool render via str();
// bytes decode best-effort with a per-call cache (byte_fields).
func searchFieldValue(field string, entry map[string]any, byteFields map[string]string) (string, bool) {
	switch typed := entry[field].(type) {
	case nil:
		// Python: entry.get(field, "") — missing or None.
		return "", false
	case string:
		if typed == "" {
			return "", false
		}
		return typed, true
	case bool:
		// Python str(True) == "True".
		if typed {
			return "True", true
		}
		return "False", true
	case int64:
		return strconv.FormatInt(typed, 10), true
	case []byte:
		if cached, ok := byteFields[field]; ok {
			return cached, true
		}
		if len(typed) == 0 {
			return "", false
		}
		decoded, err := decodeUTF8BestEffort(typed)
		if err != nil {
			// best-effort decode failed
			return "", false
		}
		byteFields[field] = decoded
		return decoded, true
	case float64:
		// Python: re.search on a float raises TypeError mid-run;
		// sanctioned crash-free deviation: no match.
		return "", false
	default:
		// json.Number and other exotic values: Python raises TypeError;
		// sanctioned crash-free deviation: no match.
		return "", false
	}
}

// pythonFalsy mirrors Python truthiness for the config values that can
// appear in a search document (Python drops falsy MESSAGE patterns with
// `if message_pattern:` and tolerates a missing fields/tags key because
// `_build_searches` setdefaults them into the document).
func pythonFalsy(v any) bool {
	switch typed := v.(type) {
	case nil:
		return true
	case bool:
		return !typed
	case int64:
		return typed == 0
	case float64:
		return typed == 0
	case json.Number:
		f, err := typed.Float64()
		return err == nil && f == 0
	case string:
		return typed == ""
	case map[string]any:
		return len(typed) == 0
	case []any:
		return len(typed) == 0
	}
	return false
}

// decodeUTF8BestEffort mirrors Python bytes.decode("utf-8") rejection of
// invalid sequences.
func decodeUTF8BestEffort(b []byte) (string, error) {
	if !utf8.Valid(b) {
		return "", errors.New("invalid utf-8")
	}
	return string(b), nil
}
