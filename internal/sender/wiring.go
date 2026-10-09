// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

// The explicit sender wiring map (plan Task 13, thread A11): one
// greppable output_type → constructor map, no blank imports and no
// init() registration. Adding a sender (Tasks 19–26) means adding one
// entry here — the Go counterpart of
// journalpump/senders/__init__.py:output_type_to_sender_class_path.

package sender

import (
	"fmt"

	"github.com/Aiven-Open/journalpump/internal/filter"
	"github.com/Aiven-Open/journalpump/internal/journal"
	"github.com/Aiven-Open/journalpump/internal/statsd"
	"github.com/Aiven-Open/journalpump/internal/tagged"
)

// ConstructorArgs carries the construction inputs every sender
// receives (the kwargs of Python's sender_class call,
// journalpump.py:286-299), including the reader-side extras
// (field_filter, unit_log_levels, extra_field_values).
type ConstructorArgs struct {
	// Name is the sender name (the config key under "senders").
	Name string
	// Config is the sender's raw config block; each sender package
	// decodes its own key set and applies its own defaults, as in
	// Python.
	Config map[string]any
	// Stats is the pump-level statsd client (may be a disabled client).
	Stats *statsd.Client
	// Tags are the pump-level base tags (Python self.make_tags());
	// the sender merges its own identity (sender=<name>) over them
	// via tagged.
	Tags *tagged.Tagged
	// FieldFilter is the reader-resolved field_filter object
	// (journalpump.py:269-271; nil when the config key is absent or
	// empty). Senders that need it keep the reference — the reload
	// corollary: filter objects are captured at sender construction,
	// not looked up per entry.
	FieldFilter *filter.FieldFilter
	// UnitLogLevels is the reader-resolved unit_log_level object
	// (journalpump.py:273-275; nil when unset).
	UnitLogLevels *filter.UnitLogLevel
	// ExtraFieldValues carries the sender's extra_field_values in
	// config document order (journalpump.py:277-284; empty when the
	// key is absent, empty, or warned about as a non-object).
	ExtraFieldValues []journal.Field
}

// Constructor builds one sender transport from its config block.
// Returned errors map to Python's "sender failed to initialize" path:
// the sender is not started and counted in the sender.failed_to_start
// persistent gauge. A constructor that reports an unstartable sender
// should wrap InitializationError.
type Constructor func(args ConstructorArgs) (Instance, error)

// constructors maps the config output_type to the sender constructor.
// Populated by Register from the process wiring (cmd/journalpump), the
// greppable counterpart of
// journalpump/senders/__init__.py:output_type_to_sender_class_path.
var constructors = map[string]Constructor{}

// Register wires one output_type to its constructor. Called from the
// process wiring (cmd/journalpump) — no blank imports and no init()
// registration; each implemented sender package appears there as one
// explicit import plus one Register call.
func Register(outputType string, ctor Constructor) {
	constructors[outputType] = ctor
}

// Lookup mirrors senders/__init__.py:get_sender_class: unknown
// output_type is an error (Python raises ValueError "Unknown sender
// type ..."), which fails the reader's configuration like Python's.
func Lookup(outputType string) (Constructor, error) {
	ctor, ok := constructors[outputType]
	if !ok {
		return nil, fmt.Errorf("Unknown sender type '%s'", outputType) //nolint:staticcheck // error-text parity (including capitalized first word) with senders/__init__.py:get_sender_class's ValueError %r
	}
	return ctor, nil
}
