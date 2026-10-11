// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package kafka

import (
	"errors"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
)

// --- Item 7: the aggregate send error exposes its causes -------------------
//
// franz-go's ProduceSync returns one ProduceResult per record; liveProducer
// aggregates the individual causes into a *sendErrors. These tests pin that
// the aggregate exposes each cause to errors.Is/As, renders their text, and
// classifies all-retriable vs mixed batches.

func TestSendErrorsExposeCauses(t *testing.T) {
	auth := errors.New("SASL authentication failed")
	err := sendErr(kerr.ErrorForCode(6), auth) // 6 = NotLeaderForPartition
	if !errors.Is(err, auth) {
		t.Error("individual cause not exposed to errors.Is")
	}
	var ke *kerr.Error
	if !errors.As(err, &ke) || ke.Code != 6 {
		t.Errorf("KError cause not exposed to errors.As (got %v)", ke)
	}
	if !strings.Contains(err.Error(), "SASL authentication failed") {
		t.Errorf("Error() = %q; the cause text must be rendered, not just 'failed to deliver N messages'", err.Error())
	}
	var se *sendErrors
	if !errors.As(err, &se) {
		t.Fatal("aggregate error is not a *sendErrors")
	}
	if se.retriable() {
		t.Error("a batch with a permanent cause must not be all-retriable")
	}
	if Retriable(err) {
		t.Error("Retriable(mixed causes) = true, want false")
	}
}

func TestSendErrorsAllRetriable(t *testing.T) {
	err := sendErr(kerr.ErrorForCode(6), kerr.ErrorForCode(7))
	if !Retriable(err) {
		t.Error("Retriable(all-retriable) = false, want true")
	}
	var se *sendErrors
	if !errors.As(err, &se) {
		t.Fatal("aggregate error is not a *sendErrors")
	}
	if !se.retriable() {
		t.Error("sendErrors.retriable() = false, want true")
	}
}

// TestSendErrorsRendersCount: the aggregate renders the cause count and every
// cause text, so the loop's health ring shows the real cause.
func TestSendErrorsRendersCount(t *testing.T) {
	err := sendErr(errors.New("boom-a"), errors.New("boom-b"))
	msg := err.Error()
	if !strings.Contains(msg, "failed to deliver 2 messages") {
		t.Errorf("Error() = %q, want the delivered-count prefix", msg)
	}
	for _, want := range []string{"boom-a", "boom-b"} {
		if !strings.Contains(msg, want) {
			t.Errorf("Error() = %q, must contain %q", msg, want)
		}
	}
}
