// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package state

import (
	"encoding/json"
	"sort"
	"strconv"
	"strings"

	"github.com/Aiven-Open/journalpump/internal/journal"
)

// rawString renders a Go string as raw JSON bytes (for constructing
// states in Go; the decoder stores decoded values verbatim).
func rawString(s string) json.RawMessage {
	return json.RawMessage(journal.PythonJSONString(s))
}

// rawInt renders an int64 as raw JSON bytes.
func rawInt(n int64) json.RawMessage {
	return json.RawMessage(strconv.FormatInt(n, 10))
}

// render produces the exact bytes of Python's
// json.dump(state, fp, indent=4, sort_keys=True): every OBJECT's keys
// sorted alphabetically (recursively), 4-space indentation, items
// separated by ",\n" and keys by ": ", empty containers as "{}"/"[]",
// ensure_ascii string escaping, Python float repr, and no trailing
// newline (json.dump does not append one).
func render(state *State) []byte {
	out := &lineBuffer{indent: 4}
	out.beginContainer('{')
	out.key("readers")
	renderReaders(out, state.Readers)
	out.key("start_time")
	out.raw(state.StartTime)
	out.endContainer('}')
	return out.bytes()
}

func renderReaders(out *lineBuffer, readers map[string]*ReaderState) {
	// Readers is a map; sort_keys sorts every level, so the object is
	// written in sorted name order regardless of the decode order.
	names := make([]string, 0, len(readers))
	for name := range readers {
		names = append(names, name)
	}
	sort.Strings(names)
	out.beginContainer('{')
	for _, name := range names {
		out.key(name)
		renderReader(out, readers[name])
	}
	out.endContainer('}')
}

func renderReader(out *lineBuffer, reader *ReaderState) {
	out.beginContainer('{')
	out.key("cursor")
	renderNullableString(out, reader.Cursor)
	out.key("searches")
	searchNames := make([]string, 0, len(reader.Searches))
	for name := range reader.Searches {
		searchNames = append(searchNames, name)
	}
	sort.Strings(searchNames)
	out.beginContainer('{')
	for _, name := range searchNames {
		out.key(name)
		// Write-only field: emitted verbatim from the lenient decode (or
		// rawInt for states constructed in Go).
		out.raw(reader.Searches[name])
	}
	out.endContainer('}')
	out.key("senders")
	senderNames := make([]string, 0, len(reader.Senders))
	byName := make(map[string]SenderState, len(reader.Senders))
	for _, sender := range reader.Senders {
		if _, ok := byName[sender.Name]; !ok {
			senderNames = append(senderNames, sender.Name)
		}
		byName[sender.Name] = sender.State // duplicate name: last value wins
	}
	sort.Strings(senderNames)
	out.beginContainer('{')
	for _, name := range senderNames {
		out.key(name)
		renderSender(out, byName[name])
	}
	out.endContainer('}')
	out.key("total_bytes")
	out.raw(reader.TotalBytes)
	out.key("total_lines")
	out.raw(reader.TotalLines)
	out.endContainer('}')
}

func renderSender(out *lineBuffer, sender SenderState) {
	out.beginContainer('{')
	out.key("buffer")
	out.beginContainer('{')
	// Sorted keys (sort_keys=True): current_queue < cursor < entry_num
	// < total_size.
	out.key("current_queue")
	out.literal(strconv.FormatInt(sender.Buffer.CurrentQueue, 10))
	out.key("cursor")
	// Write-only field: emitted verbatim from the lenient decode (or
	// rawNullableString for states constructed in Go).
	out.raw(sender.Buffer.Cursor)
	out.key("entry_num")
	out.literal(strconv.FormatInt(sender.Buffer.EntryNum, 10))
	out.key("total_size")
	out.literal(strconv.FormatInt(sender.Buffer.TotalSize, 10))
	out.endContainer('}')
	out.key("health")
	out.beginContainer('{')
	out.key("elapsed")
	out.literal(journal.PythonFloatRepr(sender.Health.Elapsed))
	out.key("errors")
	if len(sender.Health.Errors) == 0 {
		out.literal("[]")
	} else {
		out.beginContainer('[')
		for _, err := range sender.Health.Errors {
			out.item()
			out.string(err)
		}
		out.endContainer(']')
	}
	out.key("status")
	out.string(sender.Health.Status)
	out.endContainer('}')
	out.key("sent")
	out.beginContainer('{')
	out.key("bytes")
	out.literal(strconv.FormatInt(sender.Sent.Bytes, 10))
	out.key("count")
	out.literal(strconv.FormatInt(sender.Sent.Count, 10))
	out.key("cursor")
	renderNullableString(out, sender.Sent.Cursor)
	out.endContainer('}')
	out.key("type")
	out.string(sender.Type)
	out.endContainer('}')
}

func renderNullableString(out *lineBuffer, s *string) {
	if s == nil {
		out.literal("null")
	} else {
		out.string(*s)
	}
}

// lineBuffer builds indent-formatted JSON the way json.dump does: every
// container item starts on a fresh line indented depth*indent spaces,
// with ",\n" separators and ": " after keys; a container without items
// renders as "{}"/"[]".
type lineBuffer struct {
	out    []byte
	indent int
	depth  int
	// pending, top of the stack, records whether the innermost open
	// container already received an item (and therefore needs a comma
	// before the next one).
	pending []bool
}

func (b *lineBuffer) bytes() []byte { return b.out }

// spaces returns n indent spaces (json.dump's indentation).
func spaces(n int) []byte {
	return []byte(strings.Repeat(" ", n))
}

func (b *lineBuffer) push() {
	b.depth++
	b.pending = append(b.pending, false)
}

func (b *lineBuffer) pop() {
	b.depth--
	b.pending = b.pending[:len(b.pending)-1]
}

// beginContainer opens a container; the caller passes '{' or '['.
func (b *lineBuffer) beginContainer(open byte) {
	b.out = append(b.out, open)
	b.push()
}

// endContainer closes a container: if it received items, the closing
// delimiter sits on its own line at the container's own indent.
func (b *lineBuffer) endContainer(closer byte) {
	if b.pending[len(b.pending)-1] {
		b.out = append(b.out, '\n')
		b.out = append(b.out, spaces(b.depth*b.indent-b.indent)...)
	}
	b.out = append(b.out, closer)
	b.pop()
}

// itemStart starts one item of the innermost container: newline plus
// one indent per depth, and a comma when this is not the first item.
func (b *lineBuffer) itemStart() {
	top := len(b.pending) - 1
	if b.pending[top] {
		b.out = append(b.out, ',')
	}
	b.out = append(b.out, '\n')
	b.out = append(b.out, spaces(b.depth*b.indent)...)
	b.pending[top] = true
}

func (b *lineBuffer) key(name string) {
	b.itemStart()
	b.out = append(b.out, journal.PythonJSONString(name)...)
	b.out = append(b.out, ": "...)
}

func (b *lineBuffer) item() {
	b.itemStart()
}

func (b *lineBuffer) literal(s string) {
	b.out = append(b.out, s...)
}

// raw emits pre-rendered JSON bytes: the lenient decode stores
// write-only values verbatim, so re-emission is byte-faithful. An
// empty value renders as null — only reachable for hand-constructed
// states that left the field unset.
func (b *lineBuffer) raw(raw json.RawMessage) {
	if len(raw) == 0 {
		b.literal("null")
		return
	}
	b.literal(string(raw))
}

func (b *lineBuffer) string(s string) {
	b.out = append(b.out, journal.PythonJSONString(s)...)
}
