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

	"github.com/Aiven-Open/journalpump/internal/statsd"
	"github.com/Aiven-Open/journalpump/internal/tagged"
)

// ConstructorArgs carries the construction inputs every sender
// receives (the kwargs of Python's sender_class call,
// journalpump.py:286-299). Reader-side extras (field_filter,
// extra_field_values, unit_log_levels) are added as fields when the
// first sender that needs them lands; the struct is additive.
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
}

// Constructor builds one sender transport from its config block.
// Returned errors map to Python's "sender failed to initialize" path:
// the sender is not started and counted in the sender.failed_to_start
// persistent gauge. A constructor that reports an unstartable sender
// should wrap InitializationError.
type Constructor func(args ConstructorArgs) (Instance, error)

// constructors maps the config output_type to the sender constructor.
// Empty until the first sender task lands (Task 19 file sender); the
// framework, map type and lookup ship in Task 13 so every sender
// plugs into one reviewed seam.
var constructors = map[string]Constructor{}

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
