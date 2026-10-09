// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

// Package daemon is the pump supervisor and reader run loop — the port of
// JournalPump.run
// and JournalReader's runtime half (journalpump.py:136-655, 1005-1208)
// plus the handle_new_config construction (950-970). Spec §4.3
// concurrency model: one goroutine per reader drives its journal
// (Python's single-threaded poll loop is decomposed per reader — the
// recorded shape change), one goroutine per sender flush loop, and the
// supervisor ticks the per-second housekeeping (watchdog ping, search
// hit stats, state saves) and owns the D9 shutdown sequence.
package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Aiven-Open/journalpump/internal/buffer"
	"github.com/Aiven-Open/journalpump/internal/config"
	"github.com/Aiven-Open/journalpump/internal/filter"
	"github.com/Aiven-Open/journalpump/internal/geoip"
	"github.com/Aiven-Open/journalpump/internal/journal"
	"github.com/Aiven-Open/journalpump/internal/journaldlog"
	"github.com/Aiven-Open/journalpump/internal/reader"
	"github.com/Aiven-Open/journalpump/internal/sender"
	"github.com/Aiven-Open/journalpump/internal/state"
	"github.com/Aiven-Open/journalpump/internal/statsd"
	"github.com/Aiven-Open/journalpump/internal/tagged"
	"golang.org/x/sync/errgroup"
)

// pollInterval mirrors poll_interval_ms (journalpump.py:863): the
// per-iteration housekeeping cadence and the idle wait when no journal
// reader exists. It is a var (not a const) so the systest build can
// honor JOURNALPUMP_POLL_INTERVAL_MS via poll_interval_systest.go —
// the Go analogue of the Python systest harness setting
// pump.poll_interval_ms = 100. The production default is unchanged.
var pollInterval = time.Second

// Logger is the pump's logging seam; the standard library *log.Logger
// satisfies it (same seam as internal/sender).
type Logger interface {
	Printf(format string, args ...any)
}

// ErrorLogger is the OPTIONAL Logger capability for ERROR-level lines
// (Python's log.error/log.exception); the pump falls back to Printf when
// absent, so plain test loggers keep working.
type ErrorLogger interface {
	Errorf(format string, args ...any)
}

// WarningLogger is the OPTIONAL Logger capability for WARNING-level lines
// (Python's log.warning).
type WarningLogger interface {
	Warningf(format string, args ...any)
}

// LevelSetter is the OPTIONAL Logger capability that applies the config
// log_level (daemon.py's reload_config sets it then reconfigures logging).
type LevelSetter interface {
	SetLevel(level int)
}

func (p *Pump) logError(format string, args ...any) {
	logErrorTo(p.log, format, args...)
}

func (p *Pump) logWarning(format string, args ...any) {
	logWarningTo(p.log, format, args...)
}

// logWarningTo emits a WARNING-level line through the given logger when it
// supports the capability (the level-aware journald sink does; plain test
// loggers fall back to Printf). Python's log.warning call sites use it.
func logWarningTo(log Logger, format string, args ...any) {
	if l, ok := log.(WarningLogger); ok {
		l.Warningf(format, args...)
		return
	}
	log.Printf(format, args...)
}

// logErrorTo emits an ERROR-level line through the given logger when it
// supports the capability, else falls back to Printf.
func logErrorTo(log Logger, format string, args ...any) {
	if l, ok := log.(ErrorLogger); ok {
		l.Errorf(format, args...)
		return
	}
	log.Printf(format, args...)
}

// namedLogger derives a child logger carrying the given component name —
// the per-component names Python emits (JournalReader:<name>,
// LogSender:<reader>, JournalObjectHandler, MsgBuffer, ...). A logger
// without the Namer capability (test capture loggers) is returned
// unchanged.
func namedLogger(log Logger, name string) Logger {
	if n, ok := log.(journaldlog.Namer); ok {
		return n.Named(name)
	}
	return log
}

type discardLogger struct{}

func (discardLogger) Printf(_ string, _ ...any) {}

// PumpDeps carries the injectable seams of NewPump; nils select the
// production defaults.
type PumpDeps struct {
	// Log receives the pump logs; nil discards.
	Log Logger
	// Notifier is the sd_notify client; nil selects SocketNotifier.
	Notifier Notifier
	// Now is the clock seam for tests; nil = time.Now.
	Now func() time.Time
	// ReadState replaces state.Read (tests); nil = state.Read.
	ReadState func(path string) (*state.State, error)
	// ConfigPath is the config file the SIGHUP reload handler re-stats and
	// re-reads (daemon.py's config_path). Empty disables reload.
	ConfigPath string
	// LoadConfig re-reads the config file on reload (tests); nil =
	// config.Load. Every reload re-parses, so key/order-insensitive
	// comparisons stay in one place (config.ReadersChanged).
	LoadConfig func(path string) (*config.Config, error)
	// StatConfig reads the config file's ctime for the reload guard
	// (tests); nil = configCTime (st_ctime, daemon.py:105).
	StatConfig func(path string) (time.Time, error)
}

// senderEntry is one live sender the pump supervises: its loop, the
// buffer it drains, and the context its goroutine runs under.
type senderEntry struct {
	name   string
	loop   *sender.Loop
	buffer *buffer.SenderBuffer
	inst   sender.Instance
	cancel context.CancelFunc
	grace  time.Duration // inst.MaxSendInterval, captured for the D9 window
	// done closes when the sender's loop goroutine returns; a reload
	// retires a reader's senders by cancelling and joining them here.
	done chan struct{}
}

// ReaderSource is the reader-opening seam (ensure/invalidate) —
// *reader.Reopener in production, a fake in tests.
type ReaderSource interface {
	Ensure() (reader.Reader, error)
	Invalidate()
}

// Pump is the supervisor: the port of JournalPump's construction
// (handle_new_config) plus the per-second housekeeping loop and the
// shutdown sequence.
type Pump struct {
	log       Logger
	notifier  Notifier
	now       func() time.Time
	readState func(path string) (*state.State, error)

	// reload wiring (daemon.py:reload_config).
	configPath     string
	loadConfig     func(path string) (*config.Config, error)
	statConfig     func(path string) (time.Time, error)
	configCTime    time.Time
	configCTimeSet bool
	// readersActiveBlob is the currently-applied parsed `readers` value
	// (Python: readers_active_config); the reload diff compares against it.
	readersActiveBlob map[string]any

	// Reader goroutine lifecycle, set by Run. The base context and group
	// are shared so a reader failure cancels every reader (errgroup.WithCancel
	// semantics) while a reload can cancel and add INDIVIDUAL readers.
	readerGroup    *errgroup.Group
	readerGroupCtx context.Context
	readerErrCh    chan error

	cfg   *config.Config
	stats *statsd.Client
	tags  *tagged.Tagged // pump-level base tags (Python self._tags)
	geoip *geoip.DB
	// settings is the atomically-swapped snapshot of the pump-level values
	// a running reader consults PER ENTRY. Python's readers read the
	// CURRENT pump (self.pump.stats, self.pump.make_tags(),
	// self.pump.check_match), so a reload must change what an unchanged
	// reader sees; the atomic swap keeps a reload from racing a running
	// reader goroutine.
	settings      atomic.Pointer[pumpSettings]
	fieldFilters  map[string]*filter.FieldFilter
	unitLogLevels map[string]*filter.UnitLogLevel

	readers []*ReaderRuntime

	startTimeStr string

	// Sender lifecycle: senders initialize lazily inside reader
	// goroutines, so the registry is mutex-guarded. The root context is
	// deliberately NOT the run context: senders must outlive the readers
	// for the D9 grace window (spec §4.3).
	senderMu   sync.Mutex
	senderRoot context.Context
	senders    map[string][]*senderEntry // reader name -> entries
	// senderGroup joins the sender goroutines and surfaces the first
	// sender-loop error (errogroup composition, mirroring the reader
	// group). The group has no context of its own: senders are cancelled
	// SEPARATELY (entry.cancel, after the D9 grace window), never by a
	// reader failure.
	senderGroup errgroup.Group
	// senderErrCh reports a sender-loop failure to Run's select so the
	// supervisor can fail fast (a returned loop error is a bug-class
	// failure: transport errors are handled inside the loop). Capacity is
	// the number of configured senders, so a report is never dropped.
	senderErrCh chan error

	// linesRead accumulates entries processed since the last supervisor
	// tick — the global `lines` counter of journalpump.py:1124/1195 (the
	// idle-refresh gate stays GLOBAL, matching Python).
	linesRead atomic.Int64

	// hits holds the merged per-search hit counters (Python's `hits`
	// dict, journalpump.py:1114/1184-1187).
	hitsMu     sync.Mutex
	hits       map[string]int64
	lastHitsAt time.Time

	// state bookkeeping (journalpump.py:1057-1072, 1190-1193).
	previousState *state.State
	statePath     string
	lastStateSave time.Time
}

// pumpSettings is the immutable snapshot of the pump-level settings a
// running reader consults per entry: the stats client, the base tags, and
// the pump-level match rules. handleNewConfig installs a fresh snapshot on
// every reload, so an unchanged reader picks up new global settings
// without being rebuilt (Python consults the live pump).
type pumpSettings struct {
	stats      *statsd.Client
	tags       *tagged.Tagged
	matchKey   string
	matchValue any
}

// installSettings snapshots the current pump-level settings. Call after
// p.stats/p.tags are (re)built.
func (p *Pump) installSettings(cfg *config.Config) {
	p.settings.Store(&pumpSettings{
		stats:      p.stats,
		tags:       p.tags,
		matchKey:   cfg.MatchKey,
		matchValue: cfg.MatchValue,
	})
}

// NewPump builds the supervisor from a loaded config — the port of
// JournalPump.__init__ + handle_new_config (journalpump.py:845-970).
// Construction errors (unknown sender type, unknown field filter or
// unit log level name, bad search/tag configuration, unreadable state
// file) correspond to Python's startup crashes: the caller reports and
// exits 1.
func NewPump(cfg *config.Config, deps PumpDeps) (*Pump, error) {
	log := deps.Log
	if log == nil {
		log = discardLogger{}
	}
	notifier := deps.Notifier
	if notifier == nil {
		notifier = SocketNotifier{}
	}
	now := deps.Now
	if now == nil {
		now = time.Now
	}
	readState := deps.ReadState
	if readState == nil {
		readState = state.Read
	}
	loadConfig := deps.LoadConfig
	if loadConfig == nil {
		loadConfig = config.Load
	}
	statConfig := deps.StatConfig
	if statConfig == nil {
		statConfig = configCTime
	}

	// Python handle_new_config: the stats client and the base tags.
	stats := statsd.New(cfg.Statsd.Host, cfg.Statsd.Port, cfg.Statsd.Tags, cfg.Statsd.TagOrder, cfg.Statsd.Prefix, log)
	pump := &Pump{
		log:           log,
		notifier:      notifier,
		now:           now,
		readState:     readState,
		configPath:    deps.ConfigPath,
		loadConfig:    loadConfig,
		statConfig:    statConfig,
		cfg:           cfg,
		stats:         stats,
		tags:          tagged.New(cfg.Tags),
		senderRoot:    context.Background(),
		senders:       map[string][]*senderEntry{},
		hits:          map[string]int64{},
		statePath:     cfg.JSONStateFilePath,
		startTimeStr:  startTimeString(now()),
		lastStateSave: now(),
	}
	pump.installSettings(cfg)

	// The initial load records the config file's ctime (Python's
	// reload_config at construction) so the first SIGHUP with an unchanged
	// file is a no-op.
	if pump.configPath != "" {
		if ctime, err := statConfig(pump.configPath); err == nil {
			pump.configCTime = ctime
			pump.configCTimeSet = true
		}
	}

	// Python loads geoip AFTER the stats client; a failure crashes the
	// daemon (the import guard or the geoip2 open error) — propagate as
	// a construction error.
	if cfg.GeoipDatabase != "" {
		log.Printf("Loading GeoIP data from %q", cfg.GeoipDatabase)
		db, err := geoip.Open(cfg.GeoipDatabase)
		if err != nil {
			return nil, err
		}
		pump.geoip = db
	}

	// configure_field_filters / configure_unit_log_levels: construction
	// errors (bad glob, unknown severity) are Python startup crashes.
	pump.fieldFilters = make(map[string]*filter.FieldFilter, len(cfg.FieldFilters))
	for name, fcfg := range cfg.FieldFilters {
		pump.fieldFilters[name] = filter.NewFieldFilter(name, fcfg)
	}
	pump.unitLogLevels = make(map[string]*filter.UnitLogLevel, len(cfg.UnitLogLevels))
	for name, entries := range cfg.UnitLogLevels {
		ull, err := filter.NewUnitLogLevel(name, entries)
		if err != nil {
			return nil, err
		}
		pump.unitLogLevels[name] = ull
	}

	// configure_readers (first load): the resume cursors come from the
	// state file. Python reads it with json.load — a parse error crashes
	// at startup; state.Read propagates non-missing errors alike.
	var loaded *state.State
	if cfg.JSONStateFilePath != "" {
		var err error
		loaded, err = readState(cfg.JSONStateFilePath)
		if err != nil {
			return nil, err
		}
	} else {
		loaded = &state.State{Readers: map[string]*state.ReaderState{}}
	}

	// Python iterates new_config.items() (document order); Go's map
	// decode loses it, so readers construct in sorted-name order — a
	// cosmetic deviation recorded in the parity doc.
	names := make([]string, 0, len(cfg.Readers))
	for name := range cfg.Readers {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		rt, err := pump.newReaderRuntime(name, cfg.Readers[name], loaded.Readers[name], log)
		if err != nil {
			return nil, err
		}
		pump.readers = append(pump.readers, rt)
	}

	// Record the active readers blob so a subsequent reload with identical
	// readers is a no-op (Python: readers_active_config set at the end of
	// configure_readers).
	pump.readersActiveBlob = cfg.ReadersBlob()

	// The initial handle_new_config tail: write_running_config
	// (journalpump.py:970). A write failure is logged, never fatal.
	pump.writeRunningConfig(cfg)

	// One report slot per configured sender: a sender goroutine returns
	// at most once, so the fail-fast channel never drops a report.
	reportSlots := 0
	for _, rt := range pump.readers {
		reportSlots += len(rt.resolved)
	}
	if reportSlots < 1 {
		reportSlots = 1
	}
	pump.senderErrCh = make(chan error, reportSlots)
	return pump, nil
}

// reportSenderError forwards a sender-loop failure to Run (fail-fast).
// A returned loop error is a bug-class failure: transport errors are
// handled inside the loop (retriable/backoff), so the loop itself broke.
func (p *Pump) reportSenderError(err error) {
	if err == nil {
		return
	}
	select {
	case p.senderErrCh <- err:
	default:
	}
}

// startTimeString ports start_time_str (journalpump.py:859):
// datetime.now(UTC).replace(tzinfo=None).isoformat().
func startTimeString(t time.Time) string {
	t = t.UTC().Truncate(time.Microsecond)
	if t.Nanosecond()/1000 == 0 {
		return t.Format("2006-01-02T15:04:05")
	}
	return t.Format("2006-01-02T15:04:05.000000")
}

// resolvedSender is one sender config resolved against the pump-level
// filter registries at construction time — the lookups Python performs
// at initialize_senders inside get_reader, where a KeyError/ValueError
// crashes the daemon (output_type, field_filter and unit_log_level are
// all unguarded there). Resolving up front keeps the crash at startup;
// CONSTRUCTOR failures (open errors and the like) stay lazy and
// retried, exactly like Python's initialize_senders try/except.
type resolvedSender struct {
	name             string
	config           map[string]any
	ctor             sender.Constructor
	fieldFilter      *filter.FieldFilter
	unitLogLevels    *filter.UnitLogLevel
	extraFieldValues []journal.Field
}

// ReaderRuntime is the runtime half of JournalReader (journalpump.py
// 136-655): reader recreation, the readiness gate, sender construction,
// and the per-entry pipeline. One goroutine drives it; the fields the
// supervisor or the stats refresh closure touch (senders, buffers) are
// mutex-guarded.
type ReaderRuntime struct {
	pump *Pump
	name string
	log  Logger

	cfg  config.ReaderConfig
	tags *tagged.Tagged // reader-level base tags (Python reader.make_tags())

	stats *reader.Stats

	searcher *filter.Searcher

	secretFilters       []*filter.SecretFilter
	secretFilterMetrics bool
	secretFilterMatches int64

	matchKey   string
	matchValue any

	msgBufferMaxLength int
	msgBufferMaxBytes  int

	// resolved senders: construction-time lookup results (see
	// resolvedSender). The live set, buffers and gate are guarded by mu.
	mu            sync.Mutex
	resolved      []resolvedSender
	initialised   map[string]struct{}
	failedSenders int
	senders       map[string]*senderEntry
	// senderOrder is the SUCCESSFUL-initialization order (Python's
	// self.senders dict order): process iterates it, so a sender that
	// failed at first open and succeeded later lands after the ones
	// initialized before it.
	senderOrder []string
	buffers     []*buffer.SenderBuffer
	gate        *buffer.Gate

	// reopener is the ensure/invalidate seam (*reader.Reopener or a
	// test fake); openFlags and initialPosition feed its OpenConfig.
	reopener   ReaderSource
	openFlags  int
	initial    reader.InitialPosition
	initialSet bool

	// run state (single goroutine).
	journaldReader reader.Reader
	pending        reader.Event
	readyFlag      bool // Python _is_ready

	// goroutine lifecycle, set by startReader: cancelRun cancels this
	// reader's context (a reload retire) and done closes when run returns.
	cancelRun context.CancelFunc
	done      chan struct{}

	muCursor sync.Mutex
	cursor   string // "" = Python None
	// resumeCursor is the cursor the reader was constructed with (the
	// state-file resume position); it seeds each sender loop's durable
	// baseline so the delivery-checkpoint cursor never regresses below it.
	resumeCursor string

	// hitsDirty marks the read_messages recording points for the
	// supervisor's merge (Python's `hits` dict, journalpump.py:1046-1047).
	hitsDirty atomic.Bool
}

// settings returns the CURRENT pump-level settings snapshot (Python:
// self.pump.stats / self.pump.make_tags() / self.pump.check_match read
// the live pump, so a reload's global changes reach unchanged readers).
func (rt *ReaderRuntime) settings() *pumpSettings {
	return rt.pump.settings.Load()
}

// newReaderRuntime builds one reader runtime: the port of
// JournalReader.__init__ plus the construction-time sender resolution.
func (p *Pump) newReaderRuntime(name string, cfg config.ReaderConfig, st *state.ReaderState, log Logger) (*ReaderRuntime, error) {
	if st == nil {
		st = &state.ReaderState{}
	}

	// Resume cursor (journalpump.py:889-909): ResumeCursor performs the
	// sender walk and emits the per-reader resuming line for EVERY
	// configured reader, exactly where Python logs it.
	resume, err := state.ResumeCursor(st, name, log)
	if err != nil {
		return nil, err
	}
	cursor := ""
	if resume != nil {
		cursor = *resume
	}

	// Reader-level logger: Python logging.getLogger(f"JournalReader:{name}")
	// (journalpump.py:154). The resume line above is pump-level
	// (journalpump.py:909), so it keeps the daemon logger name.
	readerLog := namedLogger(log, "JournalReader:"+name)

	// Reader-level tags: JournalReader(Tagged) receives the pump's
	// make_tags() and adds reader=<name> RAW via the Tagged kwargs
	// (base.py:42-63) — no sanitization until make_tags.
	readerTags := p.tags.Tags()
	readerTags["reader"] = name
	taggedReader := tagged.New(readerTags)

	// Searches (Task 5): compile with the reader's tag-order rows and
	// the reader-tagged searcher context.
	searches, err := filter.NewSearches(cfg.Searches, cfg.SearchTagOrder...)
	if err != nil {
		return nil, err
	}
	searcher, err := filter.NewSearcher(
		searches, float64(cfg.ThresholdForMetricEmit), p.stats, taggedReader, readerLog,
		map[string]filter.TagFunc{"ip_to_geohash": p.geoip.IPToGeohash},
	)
	if err != nil {
		return nil, err
	}

	secretFilters, warnings, err := filter.NewSecretFilters(cfg.SecretFilters)
	if err != nil {
		return nil, err
	}
	for _, warning := range warnings {
		readerLog.Printf("%s", warning)
	}

	rt := &ReaderRuntime{
		pump:                p,
		name:                name,
		log:                 readerLog,
		cfg:                 cfg,
		tags:                taggedReader,
		searcher:            searcher,
		secretFilters:       secretFilters,
		secretFilterMetrics: cfg.SecretFilterMetrics,
		matchKey:            cfg.MatchKey,
		matchValue:          cfg.MatchValue,
		msgBufferMaxLength:  int(p.cfg.MsgBufferMaxLength),
		msgBufferMaxBytes:   int(p.cfg.MsgBufferMaxBytes),
		initialised:         map[string]struct{}{},
		senders:             map[string]*senderEntry{},
		resolved:            make([]resolvedSender, 0, len(cfg.Senders)),
		readyFlag:           true, // journalpump.py:186
		cursor:              cursor,
		resumeCursor:        cursor,
	}

	rt.stats = reader.NewStats(name, taggedReader.MakeTags(nil), p.stats, readerLog, func() {
		// inc_line_stats' refresh_stats per sender (journalpump.py:381).
		rt.mu.Lock()
		entries := make([]*senderEntry, 0, len(rt.senders))
		for _, entry := range rt.senders {
			entries = append(entries, entry)
		}
		rt.mu.Unlock()
		for _, entry := range entries {
			entry.loop.RefreshStats()
		}
	})

	// journal_flags: names resolve against the journal constants here —
	// Python's getattr inside get_reader raises AttributeError on an
	// unknown name (a crash); construction fails instead.
	flags := 0
	if cfg.JournalFlagNames != nil {
		flags, err = reader.FlagsFromNames(cfg.JournalFlagNames)
		if err != nil {
			return nil, err
		}
	} else if cfg.JournalFlagsInt != nil {
		flags = int(*cfg.JournalFlagsInt)
	}
	rt.openFlags = flags

	// initial_position: "head"/"tail", int seconds-from-boot; any other
	// string falls through Python's elif chain untouched (PositionNone).
	switch v := cfg.InitialPosition.(type) {
	case nil:
	case string:
		switch v {
		case "tail":
			rt.initial = reader.InitialPosition{Mode: reader.PositionTail}
		case "head":
			rt.initial = reader.InitialPosition{Mode: reader.PositionHead}
		}
	case int64:
		rt.initial = reader.InitialPosition{Mode: reader.PositionSeconds, Seconds: int(v)}
	}
	rt.initialSet = true

	// add_match constraints: units_to_match become _SYSTEMD_UNIT matches
	// (journalpump.py:480-481). Any configured journald_filters dict
	// makes Python's add_match raise TypeError mid-run (probed in Task
	// 15); Go rejects it at construction (crash-free parity, parity doc).
	if len(cfg.JournaldFilters) > 0 {
		return nil, fmt.Errorf("readers.%s.journald_filters: journald_filters is not supported by the Go reader (Python crashed with TypeError at add_match)", name)
	}
	matches := make([]reader.Match, 0, len(cfg.UnitsToMatch))
	for _, unit := range cfg.UnitsToMatch {
		matches = append(matches, reader.Match{Field: "_SYSTEMD_UNIT", Value: unit})
	}

	// The reopener: recreation rate-limited like
	// create_journald_reader_if_missing (journalpump.py:201-206); the
	// OpenConfig closure carries the CURRENT cursor (recreations resume
	// from the last seen entry) and the once-only initial position
	// (journalpump.py:478).
	rt.reopener = newReopener(func() reader.OpenConfig {
		return reader.OpenConfig{
			Flags:        rt.openFlags,
			Path:         cfg.JournalPath,
			Files:        cfg.JournalFiles,
			Namespace:    cfg.JournalNamespace,
			Matches:      matches,
			ResumeCursor: rt.currentCursor(),
			Initial:      rt.takeInitial(),
			Stats:        rt.stats,
		}
	}, readerLog)

	// Resolve the senders now (see resolvedSender): unknown output
	// types, field filters, or unit log levels are Python crashes
	// (KeyError/ValueError inside get_reader) — fail construction.
	// CONFIG DOCUMENT ORDER, matching configured_senders.items(): the
	// order decides which sender first populates the per-message
	// serialization cache and therefore whose extra_field_values reach
	// the other senders sharing a field filter (the contamination quirk).
	for _, senderName := range senderNamesInOrder(cfg.Senders, cfg.SenderOrder) {
		resolved, err := rt.resolveSender(senderName, cfg.Senders[senderName])
		if err != nil {
			return nil, err
		}
		rt.resolved = append(rt.resolved, resolved)
	}
	return rt, nil
}

// senderNamesInOrder returns the sender config keys in document order,
// falling back to sorted order for synthetic configs (tests) and
// appending any key the captured order missed.
func senderNamesInOrder(senders map[string]any, order []string) []string {
	out := make([]string, 0, len(senders))
	seen := make(map[string]bool, len(senders))
	for _, name := range order {
		if _, ok := senders[name]; ok && !seen[name] {
			out = append(out, name)
			seen[name] = true
		}
	}
	if len(out) != len(senders) {
		rest := make([]string, 0, len(senders)-len(out))
		for name := range senders {
			if !seen[name] {
				rest = append(rest, name)
			}
		}
		sort.Strings(rest)
		out = append(out, rest...)
	}
	return out
}

// currentCursor reads the cursor (thread-safe: the supervisor's state
// builder and the openConfig closure read while the reader goroutine
// writes).
func (rt *ReaderRuntime) currentCursor() string {
	rt.muCursor.Lock()
	defer rt.muCursor.Unlock()
	return rt.cursor
}

func (rt *ReaderRuntime) setCursor(cursor string) {
	rt.muCursor.Lock()
	rt.cursor = cursor
	rt.muCursor.Unlock()
}

// takeInitial returns the initial position for an open attempt: the
// configured position while it is set, none afterwards. The CLEARING
// happens only after a successful open (updateStatus) — Python clears
// self.initial_position after ANY successful get_reader
// (journalpump.py:476-478) and retries failed opens with it intact.
// resumeCursorPtr returns the reader's resume cursor as a pointer, or nil
// for the empty (Python None) position.
func (rt *ReaderRuntime) resumeCursorPtr() *string {
	if rt.resumeCursor == "" {
		return nil
	}
	cursor := rt.resumeCursor
	return &cursor
}

func (rt *ReaderRuntime) takeInitial() reader.InitialPosition {
	if !rt.initialSet {
		return reader.InitialPosition{}
	}
	return rt.initial
}

// resolveSender performs the construction-time lookups of
// initialize_senders (journalpump.py:267-284): unknown output_type /
// field_filter / unit_log_level are Python KeyError/ValueError crashes
// → construction errors; a non-object extra_field_values warns and
// degrades to none, like Python.
func (rt *ReaderRuntime) resolveSender(senderName string, raw any) (resolvedSender, error) {
	resolved := resolvedSender{name: senderName}
	config, ok := raw.(map[string]any)
	if !ok {
		return resolved, fmt.Errorf("readers.%s.senders.%s: sender config must be an object", rt.name, senderName)
	}
	resolved.config = config

	outputType, ok := config["output_type"]
	if !ok {
		// Python sender_config["output_type"] KeyError → daemon crash.
		return resolved, fmt.Errorf("readers.%s.senders.%s.output_type: key is required", rt.name, senderName)
	}
	typeName, ok := outputType.(string)
	if !ok {
		return resolved, fmt.Errorf("readers.%s.senders.%s.output_type: must be a string", rt.name, senderName)
	}
	ctor, err := sender.Lookup(typeName)
	if err != nil {
		return resolved, fmt.Errorf("readers.%s.senders.%s: %w", rt.name, senderName, err)
	}
	resolved.ctor = ctor

	// field_filter: truthy name → registry lookup (Python
	// self.field_filters[name] KeyError crash).
	if value, present := config["field_filter"]; present && !isFalsyAny(value) {
		name, ok := value.(string)
		if !ok {
			return resolved, fmt.Errorf("readers.%s.senders.%s.field_filter: must be a string", rt.name, senderName)
		}
		ff, ok := rt.pump.fieldFilters[name]
		if !ok {
			return resolved, fmt.Errorf("readers.%s.senders.%s.field_filter: unknown field filter %q", rt.name, senderName, name)
		}
		resolved.fieldFilter = ff
	}

	// unit_log_level: truthy name → registry lookup (Python
	// self.unit_log_levels[name] KeyError crash; the pump-level dict is
	// always present, so the `is not None` guard is always true).
	if value, present := config["unit_log_level"]; present && !isFalsyAny(value) {
		name, ok := value.(string)
		if !ok {
			return resolved, fmt.Errorf("readers.%s.senders.%s.unit_log_level: must be a string", rt.name, senderName)
		}
		ull, ok := rt.pump.unitLogLevels[name]
		if !ok {
			return resolved, fmt.Errorf("readers.%s.senders.%s.unit_log_level: unknown unit log level %q", rt.name, senderName, name)
		}
		resolved.unitLogLevels = ull
	}

	// extra_field_values: decoded from the order-captured raw subtree
	// (config.ReaderConfig.ExtraFieldValuesRaw). Absent → none; a
	// non-object warns and degrades to none, like Python
	// (journalpump.py:279-284); the %r rendering of the value is the
	// raw JSON text (rendering deviation, parity doc).
	rawExtras, present := rt.cfg.ExtraFieldValuesRaw[senderName]
	if present && len(rawExtras) > 0 && rawExtras[0] != '{' {
		logWarningTo(rt.log, "Extra_field_values: %s not a dictionary object, ignoring", rawExtras)
	} else if present {
		extras, err := journal.DecodeOrderedFields(rawExtras)
		if err != nil {
			return resolved, fmt.Errorf("readers.%s.senders.%s.extra_field_values: %w", rt.name, senderName, err)
		}
		resolved.extraFieldValues = extras
	}
	return resolved, nil
}

// isFalsyAny mirrors Python truthiness for the sender-config lookup
// guards (`if sender_config.get("field_filter", None)`).
func isFalsyAny(v any) bool {
	switch typed := v.(type) {
	case nil:
		return true
	case bool:
		return !typed
	case int64:
		return typed == 0
	case float64:
		return typed == 0
	case string:
		return typed == ""
	case map[string]any:
		return len(typed) == 0
	case []any:
		return len(typed) == 0
	}
	return false
}

// initSenders ports initialize_senders (journalpump.py:261-312),
// called at the tail of every successful reader open. Failed
// constructors log, skip and count — retried on every recreation;
// _failed_senders feeds the sender.failed_to_start persistent gauge
// with the reader's base tags.
func (rt *ReaderRuntime) initSenders() {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	for _, resolved := range rt.resolved {
		if _, done := rt.initialised[resolved.name]; done {
			continue
		}
		args := sender.ConstructorArgs{
			Name:             resolved.name,
			Config:           resolved.config,
			Stats:            rt.stats.Client(), // reader-CAPTURED stats (Python stats=self.stats)
			Tags:             rt.tags,           // reader-level Tagged (base + reader=name)
			FieldFilter:      resolved.fieldFilter,
			UnitLogLevels:    resolved.unitLogLevels,
			ExtraFieldValues: resolved.extraFieldValues,
		}
		inst, err := resolved.ctor(args)
		if err != nil {
			// Python log.exception("Sender %r failed to initialize") —
			// the traceback renders as the error text in Go.
			rt.pump.logError("Sender %q failed to initialize: %v", resolved.name, err)
			continue
		}
		buf := buffer.NewSenderBuffer(resolved.name, rt.msgBufferMaxLength)
		loop := sender.NewLoop(sender.LoopConfig{
			Name:  resolved.name,
			Stats: rt.stats.Client(), // reader-CAPTURED stats (Python stats=self.stats)
			Tags:  rt.senderTags(resolved.name),
			// Python logging.getLogger(f"LogSender:{reader.name}") (base.py:119).
			Log: namedLogger(rt.pump.log, "LogSender:"+rt.name),
		}, inst, buf.Chan())
		// Delivery-checkpoint baseline: a durable sender's persisted
		// cursor must not regress below the position this run resumed from.
		loop.SetResumeCursor(rt.resumeCursorPtr())
		buf.Attach(loop)
		if inst.ConnectOnInit {
			// The file sender opens its output and marks itself
			// connected at construction (file.py:8-11).
			loop.MarkConnected()
		}
		sctx, cancel := context.WithCancel(rt.pump.senderRoot)
		entry := &senderEntry{name: resolved.name, loop: loop, buffer: buf, inst: inst, cancel: cancel, grace: inst.MaxSendInterval, done: make(chan struct{})}
		rt.pump.senderMu.Lock()
		rt.pump.senders[rt.name] = append(rt.pump.senders[rt.name], entry)
		rt.pump.senderMu.Unlock()
		rt.pump.senderGroup.Go(func() error {
			defer close(entry.done)
			err := runSenderLoop(entry.loop, sctx)
			rt.pump.reportSenderError(err)
			return err
		})
		rt.initialised[resolved.name] = struct{}{}
		rt.senders[resolved.name] = entry
		rt.senderOrder = append(rt.senderOrder, resolved.name)
		rt.buffers = append(rt.buffers, buf)
	}
	rt.failedSenders = len(rt.resolved) - len(rt.initialised)
	rt.gate = buffer.NewGate(rt.msgBufferMaxLength, rt.msgBufferMaxBytes, rt.buffers...)
	rt.stats.Gauges.Set("sender.failed_to_start", int64(rt.failedSenders), rt.tags.MakeTags(nil))
}

// senderTags builds the sender's final tag set: Python passes the
// READER's make_tags() to the constructor and base.py's Tagged adds
// sender=<name> RAW; the final sanitization happens at make_tags time.
func (rt *ReaderRuntime) senderTags(senderName string) map[string]string {
	tags := rt.tags.Tags()
	tags["sender"] = senderName
	return tagged.New(tags).MakeTags(nil)
}

// writeLimitCount ports get_write_limit_message_count
// (journalpump.py:221-230), including the "failed senders and no live
// senders → 0" variant.
func (rt *ReaderRuntime) writeLimitCount() int {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.failedSenders > 0 && len(rt.senders) == 0 {
		return 0
	}
	if rt.gate == nil {
		return buffer.ChunkSize
	}
	return rt.gate.WriteLimitCount()
}

// writeLimitBytes ports get_write_limit_bytes (journalpump.py:210-219)
// with the same failed-senders variant. The caller honors the falsy
// msg_buffer_max_bytes toggle (update_status, journalpump.py:236-237).
func (rt *ReaderRuntime) writeLimitBytes() int {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.failedSenders > 0 && len(rt.senders) == 0 {
		return 0
	}
	if rt.gate == nil {
		return buffer.DefaultMsgBufferMaxBytes
	}
	return rt.gate.WriteLimitBytes()
}

// ready mirrors the is_ready property (journalpump.py:207-208):
// bool(journald_reader and _is_ready).
func (rt *ReaderRuntime) ready() bool {
	return rt.journaldReader != nil && rt.readyFlag
}

// run drives one reader — the per-goroutine decomposition of the
// Python poll loop body for one reader (journalpump.py:1116-1206):
// wait for journal activity, drain pending events up to the write
// limits, then update_status (recreation + readiness gate + sender
// initialization) and the per-iteration secret-filter metrics report.
func (rt *ReaderRuntime) run(ctx context.Context) error {
	defer func() {
		if rt.journaldReader != nil {
			_ = rt.journaldReader.Close()
			rt.journaldReader = nil
		}
	}()
	for {
		// Event wait phase. With a live reader, honor journal events
		// (Python's poller on the inotify fd with poll_timeout_ms);
		// without one, idle-wait one poll interval.
		if rt.journaldReader != nil {
			evt, err := rt.journaldReader.Wait(ctx, pollInterval)
			if err != nil {
				if ctx.Err() != nil {
					return nil // shutdown: the normal exit path
				}
				return err
			}
			rt.pending = maxEvent(rt.pending, evt)
		} else {
			if err := sleepCtx(ctx, pollInterval); err != nil {
				return nil
			}
		}

		// Process pending events (journalpump.py:1153-1173): only ready
		// readers drain; INVALIDATE recreates after the drain.
		if rt.ready() && (rt.pending == reader.EventAppend || rt.pending == reader.EventInvalidate) {
			exhausted, lines, err := rt.readMessages(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return nil // shutdown
				}
				return err
			}
			rt.pump.linesRead.Add(int64(lines))
			if rt.pending == reader.EventInvalidate {
				// If a file was deleted some unread messages may be lost
				// here; releasing the descriptors is the point
				// (journalpump.py:1165-1169).
				rt.reopener.Invalidate()
				rt.journaldReader = nil
			}
			// Only drop the event when everything was drained
			// (journalpump.py:1171-1173).
			if exhausted {
				rt.pending = reader.EventNOP
			}
		}

		// update_status + register_for_poll parity: a freshly opened
		// reader schedules an APPEND ("Check if there is something to
		// read in newly created reader", journalpump.py:1177-1179).
		if err := rt.updateStatus(ctx); err != nil {
			if ctx.Err() != nil {
				return nil // shutdown
			}
			return err
		}

		if rt.secretFilterMetrics {
			rt.reportSecretFilterMetrics()
		}
	}
}

// updateStatus ports update_status (journalpump.py:232-250): reader
// recreation (rate-limited), then the readiness transitions with their
// two log lines.
func (rt *ReaderRuntime) updateStatus(ctx context.Context) error {
	if rt.journaldReader == nil {
		if ctx.Err() != nil {
			// Shutdown: do not open a new reader (or initialize its
			// senders) past the cancellation point.
			return ctx.Err()
		}
		opened, err := rt.reopener.Ensure()
		switch {
		case err != nil && errors.Is(err, reader.ErrFilesNotAvailable):
			logWarningTo(rt.log, "Journal files for %q are not yet available", rt.name)
		case err != nil && errors.Is(err, os.ErrNotExist):
			// Python renders "FileNotFoundError: [Errno 2] ..."; Go
			// renders the wrapped error text (rendering deviation).
			logWarningTo(rt.log, "Journal for %q not available yet: FileNotFoundError: %v", rt.name, err)
		case err != nil:
			// Python: any other open error propagates out of the run
			// loop and crashes the daemon.
			return err
		case opened != nil:
			rt.journaldReader = opened
			rt.initialSet = false // journalpump.py:476-478
			if ctx.Err() == nil {
				rt.initSenders() // get_reader tail: initialize_senders LAST
			}
			rt.pending = maxEvent(rt.pending, reader.EventAppend)
		}
	}

	senderOverLimit := rt.writeLimitCount() <= 0
	if rt.msgBufferMaxBytes != 0 {
		senderOverLimit = senderOverLimit || rt.writeLimitBytes() <= 0
	}
	if !rt.readyFlag && !senderOverLimit {
		rt.log.Printf("Message buffer size under threshold for all senders, ready to process journal for %q", rt.name)
		rt.readyFlag = true
	} else if rt.readyFlag && senderOverLimit {
		rt.log.Printf("Message buffer size for at least one sender over threshold, pause processing journal for %q", rt.name)
		rt.readyFlag = false
	}
	return nil
}

// readMessages ports read_messages (journalpump.py:1019-1049): the
// budgets are sampled once per chunk; each iteration reads ONE message
// (Python read_single_message); skipped messages (bytes_read None) do
// not decrement the budgets but still consume loop iterations; the
// search-hit recording happens only on the exhaustion and full-chunk
// paths, never on the early limit returns.
func (rt *ReaderRuntime) readMessages(ctx context.Context) (exhausted bool, lines int, err error) {
	limitBytes := rt.writeLimitBytes()
	limitCount := rt.writeLimitCount()
	for i := 0; i < buffer.ChunkSize; i++ {
		// Shutdown check before every read: stats-only readers and
		// search/match-dropped entries never reach Enqueue's ctx check,
		// so without this SIGTERM could consume a whole chunk (and
		// advance the saved cursor) after cancellation.
		if ctx.Err() != nil {
			return false, lines, nil
		}
		if limitBytes <= 0 {
			return false, lines, nil
		}
		if limitCount <= 0 {
			return false, lines, nil
		}
		hasMore, bytesRead, hasBytes, err := rt.readSingleMessage(ctx)
		if err != nil {
			return false, lines, err
		}
		if hasBytes {
			limitBytes -= bytesRead
			limitCount--
		}
		if !hasMore {
			rt.recordHits()
			return true, lines, nil
		}
		lines++
	}
	rt.recordHits()
	return false, lines, nil
}

// readSingleMessage ports read_single_message (journalpump.py
// 1005-1017): ONE entry per call; the broad exception handler wraps
// BOTH the read and the processing.
func (rt *ReaderRuntime) readSingleMessage(ctx context.Context) (hasMore bool, bytesRead int, hasBytes bool, err error) {
	entries, batchErr := rt.journaldReader.ReadBatch(1)
	if errors.Is(batchErr, reader.ErrBadMessage) {
		// Python read_next: corrupted_log_entry + None → (False, None)
		// — the chunk ends and stalls until the next poll cycle.
		rt.stats.CorruptedEntry()
		return false, 0, false, nil
	}
	if batchErr != nil {
		// Python: any other read error escapes read_next into
		// read_single_message's broad handler.
		rt.broadHandler(batchErr)
		return false, 0, false, nil
	}
	if len(entries) == 0 {
		// Journal exhausted (Python StopIteration → None → (False, None)).
		return false, 0, false, nil
	}
	entry := entries[0]

	// read_next (journalpump.py:403-405): EVERY cursor-carrying entry
	// updates the cursor and last_journal_msg_time — including entries
	// later dropped by searches/match.
	if entry.Cursor != "" {
		rt.setCursor(entry.Cursor)
		rt.stats.NoteCursor()
	}

	hasMore, bytesRead, hasBytes, processErr := rt.process(ctx, entry)
	if processErr != nil {
		rt.broadHandler(processErr)
		return false, 0, false, nil
	}
	return hasMore, bytesRead, hasBytes, nil
}

// broadHandler ports read_single_message's except-arm
// (journalpump.py:1012-1017): log (Python's log.exception renders the
// traceback; Go renders the error), the unexpected_exception counter,
// a 0.5 s sleep, and (has_more=False, bytes_read=None).
func (rt *ReaderRuntime) broadHandler(err error) {
	rt.pump.logError("Unexpected exception while handling entry for %s: %v", rt.name, err)
	if s := rt.settings(); s.stats != nil {
		s.stats.UnexpectedException(err, "mainloop", s.tags.MakeTags(map[string]string{"app": "journalpump"}))
	}
	time.Sleep(500 * time.Millisecond)
}

// process runs the per-entry pipeline (Python JournalObjectHandler
// constructed per message): the live senders become the targets, in
// initialization order (Python reader.senders insertion order).
func (rt *ReaderRuntime) process(ctx context.Context, entry journal.Entry) (bool, int, bool, error) {
	var enqueueErr error
	// Read the CURRENT pump-level settings (Python's handler consults
	// self.pump per entry), so a reload's global match/stats/tag changes
	// take effect even when this reader was not rebuilt.
	settings := rt.settings()
	rt.mu.Lock()
	// Iterate the SUCCESSFUL-initialization order (Python's
	// self.senders.items()); each resolved entry carries the
	// construction-time filter/extras lookups.
	resolvedByName := make(map[string]resolvedSender, len(rt.resolved))
	for _, resolved := range rt.resolved {
		resolvedByName[resolved.name] = resolved
	}
	targets := make([]journal.SenderTarget, 0, len(rt.senders))
	for _, name := range rt.senderOrder {
		resolved, ok := resolvedByName[name]
		if !ok {
			continue
		}
		live, ok := rt.senders[name]
		if !ok {
			continue
		}
		sbuf := live.buffer
		targets = append(targets, journal.SenderTarget{
			Name:             resolved.name,
			FieldFilter:      resolved.fieldFilter,
			UnitLogLevels:    resolved.unitLogLevels,
			ExtraFieldValues: resolved.extraFieldValues,
			AddItem: func(item []byte, cursor string) {
				// Python msg_buffer.add_item never fails; Enqueue blocks
				// only past the gate and returns on ctx cancellation
				// (shutdown) — recorded, not dropped.
				if enqueueErr != nil {
					return
				}
				enqueueErr = sbuf.Enqueue(ctx, sender.Item{Payload: item, Cursor: cursor})
			},
		})
	}
	rt.mu.Unlock()

	handler := journal.NewHandler(journal.HandlerConfig{
		ReaderName:            rt.name,
		Searcher:              rt.searcher,
		ReaderMatchKey:        rt.matchKey,
		ReaderMatchValue:      rt.matchValue,
		SecretFilters:         rt.secretFilters,
		SecretFilterMetrics:   rt.secretFilterMetrics,
		PumpMatchKey:          settings.matchKey,
		PumpMatchValue:        settings.matchValue,
		Stats:                 settings.stats,
		PumpTags:              settings.tags,
		IncLineStats:          rt.stats.IncLineStats,
		NoteSecretFilterMatch: func() { rt.secretFilterMatches++ },
		// Python logging.getLogger(self.__class__.__name__) == "JournalObjectHandler".
		Log: namedLogger(rt.pump.log, "JournalObjectHandler"),
	})
	hasMore, bytesRead, hasBytes, err := handler.Process(entry, targets)
	if err == nil && enqueueErr != nil {
		// Enqueue only fails on context cancellation (shutdown): stop
		// reading, exactly like a ctx-done Wait would.
		return false, 0, false, nil
	}
	return hasMore, bytesRead, hasBytes, err
}

// recordHits ports the read_messages tail (journalpump.py:1046-1047):
// the search hit counters are cumulative; the supervisor merges them.
func (rt *ReaderRuntime) recordHits() {
	rt.hitsDirty.Store(true)
}

// reportSecretFilterMetrics ports report_secret_filter_metrics
// (journalpump.py:337-348): the 10 s gate shares inc_line_stats'
// last-send time WITHOUT advancing it; an absent stats client leaves
// the counter unreset (journalpump.py:344-346).
func (rt *ReaderRuntime) reportSecretFilterMetrics() {
	if !rt.stats.SecretFilterStatsDue() {
		return
	}
	if rt.secretFilterMatches == 0 {
		return
	}
	// Python reports through the reader's CAPTURED self.stats
	// (journalpump.py:343-346), not the pump's live client.
	stats := rt.stats.Client()
	if stats == nil {
		return
	}
	stats.Increase("journal.secret_filter_match", rt.secretFilterMatches, rt.tags.MakeTags(nil))
	rt.secretFilterMatches = 0
}

// signalNotify / signalStop are seams around os/signal so tests can run
// Run under synctest (the runtime signal-management goroutine selects on
// a channel created in the synctest bubble otherwise).
var (
	signalNotify = signal.Notify
	signalStop   = signal.Stop
)

// newReopener builds a reader's ensure/invalidate source. It is a
// package seam so tests can substitute a fake (the production factory
// opens libsystemd, which unit tests must not require); a reload builds
// the new readers through the same factory.
var newReopener = func(cfg func() reader.OpenConfig, log Logger) ReaderSource {
	return reader.NewReopener(cfg, log)
}

// runSenderLoop is a seam around (*sender.Loop).Run. Loop.Run always
// returns nil in production (cancellation is the normal exit path) and
// whatever it returns is the sender errgroup's result, so a sender
// failure propagates out of shutdown instead of being discarded; tests
// inject a failure here to exercise that path.
var runSenderLoop = func(l *sender.Loop, ctx context.Context) error { return l.Run(ctx) }

// graceWaitHook is a test seam invoked at the start of shutdown's grace
// wait, after the readers are joined and BEFORE the grace timer blocks
// and the senders are cancelled. nil in production. It lets a test
// observe "shutdown is parked in its grace window" deterministically.
var graceWaitHook func()

// Run drives the pump until ctx is done or a component fails: reader
// goroutines, the per-second housekeeping tick, and the D9 shutdown
// sequence (spec §4.3): STOPPING notify, reader drain, the sender
// grace window, sender cancellation, the final state save.
func (p *Pump) Run(ctx context.Context, hup <-chan os.Signal) error {
	// The TERM/INT log line needs the signal NAME (Python's sigterm
	// handler renders SIGTERM vs SIGINT); the run context already
	// consumed the signal, so observe it a second time.
	sigCh := make(chan os.Signal, 1)
	signalNotify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	defer signalStop(sigCh)

	// Readers run under an errgroup: a reader error cancels the group
	// (and thus the other readers) and is surfaced for the supervisor.
	// Sender cancellation is deliberately SEPARATE (senderRoot-derived,
	// entry.cancel): senders must stay alive through the D9 grace window
	// even after the readers are gone.
	readerCtx, cancelReaders := context.WithCancel(ctx)
	defer cancelReaders()
	group, groupCtx := errgroup.WithContext(readerCtx)
	// At least one slot, always: a reader added by a later reload can fail
	// while the supervisor is busy with housekeeping/reload, and the
	// non-blocking report in startReader must not drop it (with zero
	// initial readers an unbuffered channel would).
	readerErrSlots := len(p.readers)
	if readerErrSlots < 1 {
		readerErrSlots = 1
	}
	readerErr := make(chan error, readerErrSlots)
	// The reader lifecycle state lives on the Pump so a reload can retire
	// and add individual readers (startReader/retireReader); the base
	// context and errgroup are shared so a reader failure still cancels
	// every reader.
	p.readerGroup = group
	p.readerGroupCtx = groupCtx
	p.readerErrCh = readerErr
	for _, rt := range p.readers {
		p.startReader(rt)
	}
	// waitReaders joins the reader errgroup exactly once; both the
	// supervisor loop and the shutdown sequence may need the join.
	var (
		readersOnce sync.Once
		readersErr  error
	)
	waitReaders := func() error {
		readersOnce.Do(func() { readersErr = group.Wait() })
		return readersErr
	}

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			name := "TERM"
			select {
			case sig := <-sigCh:
				if sig == syscall.SIGINT {
					name = "INT"
				}
			default:
			}
			p.log.Printf("Received SIG%s, stopping daemon...", name)
			// A clean signal-driven shutdown always ends the loop: the
			// run context stays cancelled, so returning shutdown's
			// result (nil on success, the STOPPING notify error
			// otherwise) is the exit.
			return p.shutdown(cancelReaders, waitReaders)
		case err := <-readerErr:
			// A reader goroutine failed: Python's unhandled exception in
			// the single-threaded loop would crash the daemon — shut down
			// (with the grace window) and propagate the error.
			p.logError("Reader failed, stopping daemon: %v", err)
			p.log.Printf("Received SIGTERM, stopping daemon...")
			shutErr := p.shutdown(cancelReaders, waitReaders)
			if err != nil {
				return err
			}
			return shutErr
		case err := <-p.senderErrCh:
			// A sender LOOP failed (bug-class: transport errors are
			// handled inside the loop via retriable/backoff). Fail fast:
			// full shutdown with the grace window honored for the other
			// senders; shutdown's sender-group join re-propagates this
			// error as Run's result.
			p.logError("Sender failed, stopping daemon: %v", err)
			p.log.Printf("Received SIGTERM, stopping daemon...")
			return p.shutdown(cancelReaders, waitReaders)
		case <-ticker.C:
			if err := p.tick(); err != nil {
				p.logError("Housekeeping failed, stopping daemon: %v", err)
				p.log.Printf("Received SIGTERM, stopping daemon...")
				shutErr := p.shutdown(cancelReaders, waitReaders)
				if shutErr != nil {
					return shutErr
				}
				return err
			}
		case <-hup:
			// SIGHUP is a reload EVENT (daemon.py:72-74): log, then run
			// the ctime-guarded reload. A failure is logged and the run
			// loop continues (reload_if_requested's catch).
			p.log.Printf("Received SIGHUP")
			if err := p.reloadConfig(); err != nil {
				p.logError("Reloading configuration failed: %v", err)
			}
		}
	}
}

// shutdown ports the exit sequence: STOPPING=1 notify (Python's
// sigterm handler — an OSError there crashes the daemon), the reader
// drain, the D9 sender grace window (max_send_interval + TickInterval
// over the LIVE senders), sender cancellation, the buffered-transport
// finalization, the final state save, and transport closure.
//
// Every failure is preserved on the result: the STOPPING notify error
// (Python's sigterm handler would crash there), the joined reader
// errors (a cancellation must not mask a reader failure), the joined
// sender errors, and a failed final flush. A failed final delivery is
// surfaced as an error; the persisted sent cursor of a checkpointed
// (durable) sender already lags to the flushed boundary, while a
// non-checkpointed buffered sender is rolled back to the last persisted
// state (see the flush ordering below) — so the state file never claims
// undelivered bytes were delivered.
func (p *Pump) shutdown(cancelReaders context.CancelFunc, waitReaders func() error) error {
	// The STOPPING notification error is preserved as Run's result but
	// must NOT skip cleanup: Python's sigterm handler would crash the
	// daemon there, which is exactly what the port avoids.
	var notifyErr error
	if _, err := p.notifier.Notify(StateStopping); err != nil {
		notifyErr = err
	}

	cancelReaders()
	// Preserve the joined reader errors: a cancellation racing a reader
	// failure must not report success.
	readerErr := waitReaders()

	grace := p.maxSenderGrace() + sender.TickInterval
	timer := time.NewTimer(grace)
	defer timer.Stop()
	if graceWaitHook != nil {
		graceWaitHook()
	}
	<-timer.C

	// liveSender remembers which reader owns each entry so a flush
	// rollback can address the persisted state.
	type liveSender struct {
		reader string
		entry  *senderEntry
	}
	p.senderMu.Lock()
	live := make([]liveSender, 0, len(p.senders))
	for readerName, readerEntries := range p.senders {
		for _, entry := range readerEntries {
			live = append(live, liveSender{reader: readerName, entry: entry})
		}
	}
	p.senderMu.Unlock()

	// Cancellation of the senders is SEPARATE from the readers and
	// happens only after the grace window.
	for _, l := range live {
		l.entry.cancel()
	}
	senderErr := p.senderGroup.Wait()

	// Finalize buffered transports BEFORE the final state save: the
	// bytes a sender buffered (e.g. the file sender's BufferedWriter)
	// are only durable once flushed here. A failed flush is surfaced
	// and the affected sender's sent cursor is rolled back below.
	var flushErr error
	type flushFailure struct {
		reader string
		name   string
	}
	var flushFailures []flushFailure
	for _, l := range live {
		finalizer, ok := l.entry.inst.Transport.(sender.Finalizer)
		if !ok {
			continue
		}
		if err := finalizer.Finalize(); err != nil {
			p.logError("Final delivery failed for sender %q: %v", l.entry.name, err)
			l.entry.loop.MarkDisconnected(err)
			flushErr = errors.Join(flushErr, fmt.Errorf("sender %q final flush: %w", l.entry.name, err))
			// A checkpointed (durable) sender's cursor ALREADY lags to
			// the flushed boundary, so the failed flush is reflected in
			// its State() without any rollback — the interpreter's second
			// contract must not double-correct it. Non-checkpointed
			// buffered senders keep the interim rollback to the last
			// persisted state.
			if _, durable := l.entry.inst.Transport.(sender.DurableReporter); !durable {
				flushFailures = append(flushFailures, flushFailure{reader: l.reader, name: l.entry.name})
			}
		}
	}

	// Python shutdown: save_state in a broad try ("Saving state at
	// shutdown failed"). A failed final delivery must not be recorded as
	// successfully completed, so the rolled-back state is what gets
	// written.
	st := p.buildState()
	for _, f := range flushFailures {
		rollbackSent(st, p.previousState, f.reader, f.name)
	}
	if err := p.persistState(st); err != nil {
		p.logError("Saving state at shutdown failed: %v", err)
	}
	for _, l := range live {
		if closer, ok := l.entry.inst.Transport.(io.Closer); ok {
			_ = closer.Close()
		}
	}
	return errors.Join(notifyErr, readerErr, senderErr, flushErr)
}

// rollbackSent restores the persisted state's sent cursor/count/bytes
// for one sender to the last successfully saved value (zero when there
// is no previous state), so a failed final flush does not record
// undelivered messages as sent.
func rollbackSent(st, prev *state.State, reader, senderName string) {
	rs := st.Readers[reader]
	if rs == nil {
		return
	}
	var prevSent *state.SentState
	if prev != nil {
		if prs := prev.Readers[reader]; prs != nil {
			for i := range prs.Senders {
				if prs.Senders[i].Name == senderName {
					prevSent = &prs.Senders[i].State.Sent
					break
				}
			}
		}
	}
	for i := range rs.Senders {
		if rs.Senders[i].Name != senderName {
			continue
		}
		if prevSent != nil {
			rs.Senders[i].State.Sent = *prevSent
		} else {
			rs.Senders[i].State.Sent = state.SentState{}
		}
		return
	}
}

// maxSenderGrace returns the max over the LIVE senders'
// max_send_interval (the D9 window: every sender gets one final flush
// tick). No senders → zero grace.
func (p *Pump) maxSenderGrace() time.Duration {
	p.senderMu.Lock()
	defer p.senderMu.Unlock()
	maxGrace := time.Duration(0)
	for _, readerEntries := range p.senders {
		for _, entry := range readerEntries {
			if entry.grace > maxGrace {
				maxGrace = entry.grace
			}
		}
	}
	return maxGrace
}

// sleepCtx sleeps d or returns ctx.Err() — the ctx-aware idle wait.
func sleepCtx(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func maxEvent(a, b reader.Event) reader.Event {
	if b > a {
		return b
	}
	return a
}

// tick is one supervisor iteration (journalpump.py:1184-1206): the
// watchdog ping, the 60 s search-hits line, the 10 s state save +
// persistent-gauge refresh, and the GLOBAL idle refresh (Python gates
// it on no reader having produced lines this iteration).
func (p *Pump) tick() error {
	// ping_watchdog (Python unguarded notify: an error crashes).
	if _, err := p.notifier.Notify(StateWatchdog); err != nil {
		return err
	}

	// Merge newly recorded search hits; log at most every 60 s
	// (journalpump.py:1184-1187). Python renders the dict with %s —
	// reproduced with sorted names (cosmetic order deviation).
	p.mergeHits()
	p.hitsMu.Lock()
	hits := p.hits
	last := p.lastHitsAt
	p.hitsMu.Unlock()
	if len(hits) > 0 && p.now().Sub(last) > 60*time.Second {
		p.log.Printf("search hits stats: %s", pythonDictString(hits))
		p.hitsMu.Lock()
		p.hits = map[string]int64{}
		p.lastHitsAt = p.now()
		p.hitsMu.Unlock()
	}

	// save_state + refresh_gauges every 10 s (journalpump.py:1189-1193).
	if p.now().Sub(p.lastStateSave) > 10*time.Second {
		if err := p.saveState(); err != nil {
			return err
		}
		p.lastStateSave = p.now()
		for _, rt := range p.readers {
			// Python's refresh_gauges re-emits each reader's gauge
			// values/tags through the LIVE pump client (self.stats,
			// journalpump.py:1102-1108), so a reload's new statsd
			// client takes over the resend loop.
			rt.stats.Gauges.RefreshNonZeroTo(p.stats)
		}
	}

	// Idle refresh, GLOBAL gate (journalpump.py:1195-1198): refresh the
	// readers so they flush buffered stats when NO reader produced
	// lines since the last tick.
	if p.linesRead.Swap(0) == 0 {
		for _, rt := range p.readers {
			rt.stats.IncLineStats(0, 0)
		}
	}
	return nil
}

// mergeHits folds the readers' cumulative hit counters into the pump's
// hits dict when newly recorded (last-writer-wins across readers, like
// Python's shared dict; Go merges in sorted reader order — cosmetic).
func (p *Pump) mergeHits() {
	for _, rt := range p.readers {
		if !rt.hitsDirty.CompareAndSwap(true, false) {
			continue
		}
		p.hitsMu.Lock()
		for _, search := range rt.searcher.Searches() {
			p.hits[search.Name()] = search.Hits()
		}
		p.hitsMu.Unlock()
	}
}

// pythonDictString renders a name->int map like Python's %s of a dict:
// {'a': 1, 'b': 2} — keys in sorted order (Python renders insertion
// order; the cosmetic deviation is recorded).
func pythonDictString(hits map[string]int64) string {
	names := make([]string, 0, len(hits))
	for name := range hits {
		names = append(names, name)
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, name := range names {
		parts = append(parts, fmt.Sprintf("'%s': %d", name, hits[name]))
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

// saveState ports save_state (journalpump.py:1057-1072): an absent
// path skips entirely; the write happens only when the state CHANGED
// (Python compares the parsed dicts — Go compares the typed states
// with reflect.DeepEqual, the sanctioned equivalent).
func (p *Pump) saveState() error {
	return p.persistState(p.buildState())
}

// persistState writes st when it differs from the last persisted state.
func (p *Pump) persistState(st *state.State) error {
	if p.statePath == "" {
		return nil
	}
	if p.previousState != nil && reflect.DeepEqual(st, p.previousState) {
		return nil
	}
	if err := state.Write(p.statePath, st); err != nil {
		return err
	}
	p.previousState = st
	return nil
}

// buildState ports the state_to_save construction: per-reader
// get_state (journalpump.py:326-335) plus start_time.
func (p *Pump) buildState() *state.State {
	st := &state.State{
		Readers:   map[string]*state.ReaderState{},
		StartTime: json.RawMessage(`"` + p.startTimeStr + `"`),
	}
	for _, rt := range p.readers {
		cursor := rt.currentCursor()
		var cursorPtr *string
		if cursor != "" {
			cursorPtr = &cursor
		}
		searches := map[string]json.RawMessage{}
		for _, search := range rt.searcher.Searches() {
			searches[search.Name()] = json.RawMessage(strconv.FormatInt(search.Hits(), 10))
		}
		rt.mu.Lock()
		senders := make([]state.NamedSenderState, 0, len(rt.senders))
		for name, entry := range rt.senders {
			senders = append(senders, state.NamedSenderState{Name: name, State: entry.loop.State()})
		}
		rt.mu.Unlock()
		sort.Slice(senders, func(i, j int) bool { return senders[i].Name < senders[j].Name })
		lines, bytes := rt.stats.Totals()
		st.Readers[rt.name] = &state.ReaderState{
			Cursor:     cursorPtr,
			Searches:   searches,
			Senders:    senders,
			TotalLines: json.RawMessage(strconv.FormatInt(lines, 10)),
			TotalBytes: json.RawMessage(strconv.FormatInt(bytes, 10)),
		}
	}
	return st
}
