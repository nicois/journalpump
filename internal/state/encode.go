// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package state

import (
	"bytes"
	"encoding/json"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"fmt"
	"io"
)

// The state-file WRITER encodes with Go 1.27's new encoding/json/v2
// encoder (user decision 2026-10-09, charter §2). The output is
// Go-native JSON, a sanctioned deviation from Python's
// json.dump(state, fp, indent=4, sort_keys=True):
//
//   - object key order is Go-native — struct fields in declaration
//     order, map keys in Go map iteration order, which is
//     NONDETERMINISTIC across writes. Nothing (in Go or Python) may
//     depend on a Go-written file's key order; Python's json.load and
//     Go's Read both parse order-independently, and the resume quirk
//     walks the file's key order whatever it is.
//   - strings are not ensure_ascii-escaped: non-ASCII characters are
//     raw UTF-8 (v2 does not escape HTML either, like the old writer).
//   - floats render Go-native (health.elapsed 3.5 → 3.5, 0 → 0; no
//     Python float repr).
//   - non-finite floats are an encoding ERROR in v2 (probed:
//     "unsupported value: NaN"), where Python's json.dump default
//     allow_nan=True emits NaN/Infinity literals. Unreachable from
//     journalpump's own paths — the decoded state only carries RFC-valid
//     JSON values, which cannot contain non-finite literals — but it
//     narrows the "any Python-acceptable state file re-writes" guarantee
//     at the writer: a state value that is not valid RFC JSON cannot be
//     re-encoded. (A Python-written file containing NaN already fails
//     Go's strict-JSON Read, so the read-side boundary is unchanged.)
//     Deviation boundary recorded in docs/go-test-parity.md (Task 8).
//
// What is preserved from Python: the parsed document is equal
// (json.load-compatible structure and values), the 4-space
// json.dump-style indentation (items on their own lines, empty
// containers as {} and []), no trailing newline, and the 0600 file
// mode. The WIRE-path serializer (internal/journal) keeps Python
// byte-faithful output — that path is byte-parity-critical and
// unaffected by this decision.

// stateJSON mirrors State for encoding: the same data, with the
// name-keyed levels as maps (order Go-native, see above) and the
// lenient write-only fields as json.RawMessage, which the encoder
// inlines (whitespace-reformatted under indentation, values preserved).
type stateJSON struct {
	Readers   map[string]readerJSON `json:"readers"`
	StartTime json.RawMessage       `json:"start_time"`
}

// readerJSON mirrors ReaderState for encoding. The senders slice
// becomes a map keyed by sender name: Python's senders value is a
// dict, duplicate names collapse to the LAST value (the previous
// writer's semantics, kept), and only the decode side preserves file
// order for the resume quirk.
type readerJSON struct {
	Cursor     *string                    `json:"cursor"`
	Searches   map[string]json.RawMessage `json:"searches"`
	Senders    map[string]SenderState     `json:"senders"`
	TotalLines json.RawMessage            `json:"total_lines"`
	TotalBytes json.RawMessage            `json:"total_bytes"`
}

// marshalable builds the encoder view of a decoded state.
func marshalable(state *State) stateJSON {
	out := stateJSON{Readers: make(map[string]readerJSON, len(state.Readers)), StartTime: state.StartTime}
	for name, reader := range state.Readers {
		rj := readerJSON{
			Cursor:     reader.Cursor,
			Searches:   reader.Searches,
			TotalLines: reader.TotalLines,
			TotalBytes: reader.TotalBytes,
			Senders:    make(map[string]SenderState, len(reader.Senders)),
		}
		if rj.Searches == nil {
			rj.Searches = map[string]json.RawMessage{}
		}
		for _, sender := range reader.Senders {
			rj.Senders[sender.Name] = sender.State // duplicate name: last value wins
		}
		out.Readers[name] = rj
	}
	return out
}

// writeStateJSON encodes state with the 4-space json.dump layout and no
// trailing newline (the jsontext encoder terminates the top-level value
// with one; Python's json.dump does not append it).
func writeStateJSON(w io.Writer, state *State) error {
	var buf bytes.Buffer
	enc := jsontext.NewEncoder(&buf, jsontext.WithIndent("    "), jsontext.Multiline(true))
	if err := jsonv2.MarshalEncode(enc, marshalable(state)); err != nil {
		return fmt.Errorf("encoding state file: %w", err)
	}
	_, err := w.Write(bytes.TrimSuffix(buf.Bytes(), []byte("\n")))
	return err
}
