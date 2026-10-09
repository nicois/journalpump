// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package config

import (
	"bytes"
	"encoding/json"
)

// tagOrders carries the document-order tag keys the token walk captured
// from the raw config JSON.
type tagOrders struct {
	// readers maps reader name -> per-search tag key lists, aligned with
	// the search list index.
	readers map[string][][]string
	// statsdTags holds the top-level statsd.tags keys in document order.
	statsdTags []string
}

// captureTagOrders walks the raw config JSON and returns the
// document-order key lists of (a) each search's "tags" object, per
// reader, and (b) the top-level statsd.tags object.
//
// Why: encoding/json decodes objects into maps, which discards key
// order — but Python's json.load preserves it, and the order is
// observable downstream: perform_searches evaluates the tag callables
// in config order (journalpump.py:629-634), so a tag callable whose
// args name an earlier static tag must see the value Python sees, and
// statsd.py renders the global tags in dict order onto the wire. The
// typed decode cannot recover the order from the parsed map, hence
// this targeted token walk.
//
// Reader rows are aligned with the search list index; searches without
// a "tags" object (or with a non-object "tags" value) leave a nil row.
// A malformed document yields partial results — the main decode reports
// the error.
func captureTagOrders(data []byte) tagOrders {
	w := &tagOrderWalker{
		dec: json.NewDecoder(bytes.NewReader(data)),
		out: tagOrders{readers: map[string][][]string{}},
	}
	_ = w.walkDocument() // malformed JSON is reported by the main decode
	return w.out
}

type tagOrderWalker struct {
	dec *json.Decoder
	out tagOrders
	// reader and searchIndex track the readers.<name>.searches[i] path.
	reader      string
	searchIndex int
}

// openContainer consumes the first token of a value. It returns the open
// delimiter for `{`/`[` containers (now consumed), or ok=false for
// scalars (fully consumed).
func (w *tagOrderWalker) openContainer() (json.Delim, bool, error) {
	tok, err := w.dec.Token()
	if err != nil {
		return 0, false, err
	}
	d, ok := tok.(json.Delim)
	if !ok || (d != '{' && d != '[') {
		return 0, false, nil
	}
	return d, true, nil
}

// objectEntries consumes a `{`-delimited object body (the `{` must be
// consumed already) and invokes visit for every key; the visitor must
// consume the key's value. Duplicate keys visit twice, matching the
// last-wins map decode.
func (w *tagOrderWalker) objectEntries(visit func(key string) error) error {
	for w.dec.More() {
		keyTok, err := w.dec.Token()
		if err != nil {
			return err
		}
		key, ok := keyTok.(string)
		if !ok {
			return w.skipValue()
		}
		if err := visit(key); err != nil {
			return err
		}
	}
	_, err := w.dec.Token() // closing }
	return err
}

// skipValue consumes one complete value.
func (w *tagOrderWalker) skipValue() error {
	tok, err := w.dec.Token()
	if err != nil {
		return err
	}
	return w.skipRest(tok)
}

// skipRest finishes a value whose FIRST token was already consumed
// (scalars are complete; containers run to their closing delimiter).
func (w *tagOrderWalker) skipRest(first json.Token) error {
	d, ok := first.(json.Delim)
	if !ok {
		return nil
	}
	switch d {
	case '{':
		return w.objectEntries(func(_ string) error { return w.skipValue() })
	case '[':
		for w.dec.More() {
			if err := w.skipValue(); err != nil {
				return err
			}
		}
	}
	_, err := w.dec.Token() // closing delimiter
	return err
}

func (w *tagOrderWalker) walkDocument() error {
	d, ok, err := w.openContainer()
	if err != nil || !ok || d != '{' {
		return err // non-object document: the main decode errors later
	}
	return w.objectEntries(func(key string) error {
		switch key {
		case "readers":
			d, ok, err := w.openContainer()
			if err != nil || !ok || d != '{' {
				return err
			}
			return w.objectEntries(w.walkReader)
		case "statsd":
			return w.walkStatsd()
		default:
			return w.skipValue()
		}
	})
}

// walkStatsd visits the statsd section for its tags object. A duplicate
// "statsd" (or "tags") key overwrites the capture — last wins, matching
// the map decode.
func (w *tagOrderWalker) walkStatsd() error {
	d, ok, err := w.openContainer()
	if err != nil || !ok || d != '{' {
		return err // non-object statsd: the main decode errors later
	}
	return w.objectEntries(func(key string) error {
		if key != "tags" {
			return w.skipValue()
		}
		d, ok, err := w.openContainer()
		if err != nil || !ok || d != '{' {
			return err
		}
		keys := []string{}
		if err := w.objectEntries(func(key string) error {
			keys = append(keys, key)
			return w.skipValue()
		}); err != nil {
			return err
		}
		w.out.statsdTags = keys
		return nil
	})
}

func (w *tagOrderWalker) walkReader(name string) error {
	previousReader, previousIndex := w.reader, w.searchIndex
	w.reader, w.searchIndex = name, 0
	// A duplicate reader name visits this function twice while the map
	// decode keeps only the LAST occurrence; truncate the rows captured
	// for a previous occurrence so they do not leak into the kept one.
	w.out.readers[name] = nil
	defer func() { w.reader, w.searchIndex = previousReader, previousIndex }()
	d, ok, err := w.openContainer()
	if err != nil || !ok || d != '{' {
		return err // non-object reader entry: the main decode errors later
	}
	return w.objectEntries(func(key string) error {
		if key != "searches" {
			return w.skipValue()
		}
		d, ok, err := w.openContainer()
		if err != nil || !ok || d != '[' {
			return err // non-array searches: the main decode errors later
		}
		// A duplicate "searches" key visits this branch twice while the
		// map decode keeps only the LAST array; restart the index AND
		// truncate the rows captured so far, so the rows align with
		// whichever array the decode kept.
		w.searchIndex = 0
		w.out.readers[w.reader] = nil
		for w.dec.More() {
			if err := w.walkSearch(); err != nil {
				return err
			}
			w.searchIndex++
		}
		_, err = w.dec.Token() // closing ]
		return err
	})
}

// walkSearch consumes one element of a searches array.
func (w *tagOrderWalker) walkSearch() error {
	d, ok, err := w.openContainer()
	if err != nil || !ok || d != '{' {
		return err // non-object search element: the main decode errors later
	}
	return w.objectEntries(func(key string) error {
		if key != "tags" {
			return w.skipValue()
		}
		d, ok, err := w.openContainer()
		if err != nil || !ok || d != '{' {
			return err
		}
		keys := []string{}
		if err := w.objectEntries(func(key string) error {
			keys = append(keys, key)
			return w.skipValue()
		}); err != nil {
			return err
		}
		for len(w.out.readers[w.reader]) <= w.searchIndex {
			w.out.readers[w.reader] = append(w.out.readers[w.reader], nil)
		}
		w.out.readers[w.reader][w.searchIndex] = keys
		return nil
	})
}
