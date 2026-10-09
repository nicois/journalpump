// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package sender

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestLookup(t *testing.T) {
	// Not parallel: the package-global wiring map is mutated below.
	t.Run("unknown output type errors like Python's ValueError", func(t *testing.T) {
		_, err := Lookup("nonexistent")
		// Python's %r on a string renders single quotes.
		if err == nil || err.Error() != "Unknown sender type 'nonexistent'" {
			t.Fatalf("Lookup error = %v, want Unknown sender type 'nonexistent'", err)
		}
	})

	t.Run("registered constructor is returned", func(t *testing.T) {
		ctor := func(ConstructorArgs) (Instance, error) { return Instance{}, nil }
		constructors["test-fake"] = ctor
		defer delete(constructors, "test-fake")
		got, err := Lookup("test-fake")
		if err != nil {
			t.Fatalf("Lookup: %v", err)
		}
		if got == nil {
			t.Fatal("Lookup returned a nil constructor")
		}
	})
}

func TestRefreshStats(t *testing.T) {
	t.Parallel()

	t.Run("four gauges with the sender's base tags", func(t *testing.T) {
		t.Parallel()
		capture := newUDPCapture(t)
		loop := NewLoop(
			LoopConfig{Name: "out", Stats: newStatsClient(capture), Tags: map[string]string{"sender": "out"}},
			Instance{Transport: &fakeTransport{}, MaxSendInterval: time.Minute, Type: "FakeSender"},
			make(chan Item, 1),
		)
		loop.markSent([]Item{
			{Payload: []byte("ab"), Cursor: "c1"},
			{Payload: []byte("cde"), Cursor: "c2"},
		})
		loop.RefreshStats()
		datagrams := capture.datagrams(4)
		joined := strings.Join(datagrams, "\n")
		for _, want := range []string{
			"journal.last_sent_ago,sender=out:",
			"journal.sent_bytes,sender=out:5|g",
			"journal.sent_lines,sender=out:2|g",
			"journal.status,sender=out:3|g",
		} {
			if !strings.Contains(joined, want) {
				t.Fatalf("datagrams %q missing %q", joined, want)
			}
		}
	})

	t.Run("nil stats is a no-op", func(t *testing.T) {
		t.Parallel()
		loop := NewLoop(
			LoopConfig{Name: "out"},
			Instance{Transport: &fakeTransport{}, MaxSendInterval: time.Minute, Type: "FakeSender"},
			make(chan Item, 1),
		)
		loop.RefreshStats() // must not panic
	})
}

func TestInitializationError(t *testing.T) {
	t.Parallel()
	wrapped := &InitializationError{Err: context.DeadlineExceeded}
	if got := wrapped.Error(); got != "sender initialization failed: context deadline exceeded" {
		t.Fatalf("Error() = %q", got)
	}
	if !errors.Is(wrapped, context.DeadlineExceeded) {
		t.Fatal("Unwrap must expose the wrapped cause")
	}
	var target *InitializationError
	if !errors.As(wrapped, &target) {
		t.Fatal("errors.As must match InitializationError")
	}
}
