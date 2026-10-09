// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package reader

import (
	"sync"
	"time"

	"github.com/Aiven-Open/journalpump/internal/statsd"
)

// Stats ports the reader-side observability of JournalReader
// (journalpump.py:314-382): inc_line_stats with its 10 s per-reader
// throttle and gauges, and the corrupted-entry counter. One instance
// serves one reader across journal-file recreations — Python keeps
// this state on JournalReader while PumpReader instances come and go.
//
// Python shape, for reference:
//
//   - inc_line_stats (journalpump.py:350-383): accumulate
//     read_lines/read_bytes; at most every 10 s emit the gauges
//     journal.last_read_ago / journal.read_lines / journal.read_bytes,
//     log "Processed %r journal lines (%r bytes)" every 120 s with the
//     since-last-log diffs, and refresh every sender's stats.
//   - set_persistent_gauge (journalpump.py:314-324) is ported as
//     statsd.PersistentGauges (with the emit-only-on-zero-transition
//     contract covered by statsd's TestPersistentGauge, the parity
//     twin of test/unit/reader/test_stats.py); the reader owns one via
//     Gauges.
//
// All times are monotonic-clock times, like Python's time.monotonic().
type Stats struct {
	name    string
	client  *statsd.Client // may be nil (Python self.stats None)
	Gauges  *statsd.PersistentGauges
	tags    map[string]string
	log     Logger
	refresh func() // sender refresh_stats for each sender; may be nil

	mu                sync.Mutex
	readLines         int64
	readBytes         int64
	lastStatsSend     time.Time
	lastJournalMsg    time.Time
	lastStatsPrint    time.Time
	sentLinesDiff     int64
	sentBytesDiff     int64
	lastSentReadLines int64
	lastSentReadBytes int64
	now               func() time.Time // test seam; defaults to time.Now
}

// Throttle intervals ported from journalpump.py:355 and :368.
const (
	statsThrottleInterval = 10 * time.Second
	statsPrintInterval    = 120 * time.Second
)

// NewStats builds the reader stats. client may be nil (Python
// self.stats None skips gauge emission but keeps the throttle and
// logging); refresh may be nil (Python's per-sender refresh_stats —
// the Task 16 pump wires the sender loops in).
func NewStats(name string, tags map[string]string, client *statsd.Client, log Logger, refresh func()) *Stats {
	if log == nil {
		log = discardLogger{}
	}
	now := time.Now
	return &Stats{
		name:           name,
		client:         client,
		Gauges:         statsd.NewPersistentGauges(client),
		tags:           tags,
		log:            log,
		refresh:        refresh,
		lastStatsSend:  now(), // journalpump.py:178
		lastJournalMsg: now(), // journalpump.py:179
		lastStatsPrint: now(), // journalpump.py:165
		now:            now,
	}
}

// Client returns the construction-time stats client the reader captured
// (Python JournalReader.self.stats, journalpump.py:170). Sender
// initialization and secret-filter metrics emit through THIS client
// (Python passes stats=self.stats at journalpump.py:294 and reports via
// self.stats at :346); only the pump-level gauge refresh uses the live
// client. May be nil when statsd is unconfigured.
func (s *Stats) Client() *statsd.Client { return s.client }

// IncLineStats ports inc_line_stats: journalLines/journalBytes join the
// cumulative counters; at most once per 10 s the gauges are emitted,
// the 120 s summary line logged, and the senders refreshed.
func (s *Stats) IncLineStats(journalLines, journalBytes int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.readBytes += int64(journalBytes)
	s.readLines += int64(journalLines)

	now := s.now()
	if now.Sub(s.lastStatsSend) < statsThrottleInterval {
		// Do not send stats too often
		return
	}

	if s.client != nil {
		s.client.Gauge("journal.last_read_ago", now.Sub(s.lastJournalMsg).Seconds(), s.tags)
		s.client.GaugeInt("journal.read_lines", s.readLines, s.tags)
		s.client.GaugeInt("journal.read_bytes", s.readBytes, s.tags)
	}

	s.lastStatsSend = now
	s.sentLinesDiff += s.readLines - s.lastSentReadLines
	s.sentBytesDiff += s.readBytes - s.lastSentReadBytes
	if now.Sub(s.lastStatsPrint) > statsPrintInterval {
		// Python logs %r of the diff ints — plain decimal digits.
		s.log.Printf("Processed %d journal lines (%d bytes)", s.sentLinesDiff, s.sentBytesDiff)
		s.sentLinesDiff = 0
		s.sentBytesDiff = 0
		s.lastStatsPrint = now
	}

	s.lastSentReadLines = s.readLines
	s.lastSentReadBytes = s.readBytes

	if s.refresh != nil {
		s.refresh()
	}
}

// SecretFilterStatsDue reports whether the 10 s stats-send window has
// elapsed — the gate of report_secret_filter_metrics
// (journalpump.py:338-341), which shares the reader's
// last_stats_send_time with inc_line_stats but does NOT advance it
// (only inc_line_stats does). Callers combine this with their own
// match-count check; the counter is NOT reset by this method.
func (s *Stats) SecretFilterStatsDue() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.now().Sub(s.lastStatsSend) >= statsThrottleInterval
}

// CorruptedEntry ports the "Bad message" arm of read_next
// (journalpump.py:392-397): journal.corrupted_log_entry increases
// immediately (not throttled) and the corruption is warned.
func (s *Stats) CorruptedEntry() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.client != nil {
		s.client.Increase("journal.corrupted_log_entry", 1, s.tags)
	}
	s.log.Printf("Corrupted log entry in %s", s.name)
}

// NoteCursor updates the last-message-seen time (journalpump.py:403-405:
// every entry with a cursor refreshes last_journal_msg_time). The
// cursor value itself stays pump-owned (state file).
func (s *Stats) NoteCursor() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastJournalMsg = s.now()
}

// Totals returns the cumulative read counters (Python read_lines /
// read_bytes, serialized in get_state).
func (s *Stats) Totals() (lines, bytes int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.readLines, s.readBytes
}

// LastJournalMsgAgo reports the seconds since the last cursor-carrying
// entry was read (Python now - last_journal_msg_time).
func (s *Stats) LastJournalMsgAgo() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.now().Sub(s.lastJournalMsg)
}
