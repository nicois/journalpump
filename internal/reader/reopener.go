// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package reader

import (
	"time"
)

// RecreateInterval is the minimum spacing between reader-creation
// attempts (journalpump.py:202: time.monotonic() -
// self.last_journald_create_attempt > 2).
const RecreateInterval = 2 * time.Second

// Reopener ports create_journald_reader_if_missing
// (journalpump.py:201-206): when the pump has no reader, retry opening
// at most once per RecreateInterval, recording the attempt timestamp
// BEFORE the attempt so a slow/failed open does not back-to-back
// retry. The open function is injectable so tests (and the Task 16
// pump, if it needs to intercept) can substitute fakes.
type Reopener struct {
	// cfg supplies the OpenConfig per attempt; recreations must carry
	// the pump's current resume cursor.
	cfg func() OpenConfig
	// open builds the reader; nil defaults to Open.
	open     func(cfg OpenConfig) (Reader, error)
	interval time.Duration

	log Logger

	reader      Reader
	lastAttempt time.Time
}

// NewReopener builds a reopener; cfg is called once per attempt.
func NewReopener(cfg func() OpenConfig, log Logger) *Reopener {
	if log == nil {
		log = discardLogger{}
	}
	return &Reopener{
		cfg:      cfg,
		open:     func(o OpenConfig) (Reader, error) { return Open(o) },
		interval: RecreateInterval,
		log:      log,
	}
}

// Ensure returns the current reader, or attempts a (rate-limited)
// recreation. A nil reader with a nil error means "not time yet" — the
// pump keeps running without a reader, exactly like Python's
// get_reader returning None.
func (r *Reopener) Ensure() (Reader, error) {
	if r.reader != nil {
		return r.reader, nil
	}
	if time.Since(r.lastAttempt) <= r.interval {
		return nil, nil
	}
	r.lastAttempt = time.Now()
	reader, err := r.open(r.cfg())
	if err != nil {
		return nil, err
	}
	r.reader = reader
	return reader, nil
}

// Invalidate closes and forgets the current reader (Python
// invalidate(), journalpump.py:191-197): called when the reader's view
// of the files is stale (e.g. files were deleted) so the next Ensure
// recreates it.
func (r *Reopener) Invalidate() {
	if r.reader != nil {
		_ = r.reader.Close()
		r.reader = nil
	}
}
