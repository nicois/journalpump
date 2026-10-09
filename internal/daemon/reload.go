// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package daemon

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"syscall"
	"time"

	"github.com/Aiven-Open/journalpump/internal/config"
	"github.com/Aiven-Open/journalpump/internal/filter"
	"github.com/Aiven-Open/journalpump/internal/geoip"
	"github.com/Aiven-Open/journalpump/internal/sender"
	"github.com/Aiven-Open/journalpump/internal/state"
	"github.com/Aiven-Open/journalpump/internal/statsd"
	"github.com/Aiven-Open/journalpump/internal/tagged"
)

// configCTime reads the config file's inode-change time — the Go port of
// daemon.py's `self.config_path.stat().st_ctime` (journalpump.py:100 and
// the reload comparison at daemon.py:105). Linux exposes st_ctime as
// Stat_t.Ctim; the syscall is used directly so no new dependency is
// pulled in.
func configCTime(path string) (time.Time, error) {
	var st syscall.Stat_t
	if err := syscall.Stat(path, &st); err != nil {
		return time.Time{}, err
	}
	return time.Unix(st.Ctim.Sec, st.Ctim.Nsec), nil
}

// reloadConfig ports daemon.py:reload_config + reload_if_requested
// (daemon.py:76-114): stat the config file, and when its ctime differs
// from the last observed one, notify RELOADING=1, log the load line,
// re-read the config, apply handle_new_config, and notify READY=1. Any
// failure is returned for Run's SIGHUP case to log as "Reloading
// configuration failed" — like Python's reload_if_requested catch, a
// failed reload never takes the daemon down.
func (p *Pump) reloadConfig() error {
	if p.configPath == "" {
		return nil
	}
	fileCTime, err := p.statConfig(p.configPath)
	if err != nil {
		// Python: ServiceDaemonError("Cannot start without json config
		// file ...") when require_config, caught by reload_if_requested.
		return fmt.Errorf("stat config %q: %w", p.configPath, err)
	}
	if p.configCTimeSet && fileCTime.Equal(p.configCTime) {
		// No change: daemon.py's ctime guard — reload is a no-op.
		return nil
	}

	if _, err := p.notifier.Notify(StateReloading); err != nil {
		return err
	}
	// daemon.py:107 "%soading configuration" → "Loading configuration"
	// on the first ever load, "Reloading configuration" afterwards.
	if p.configCTimeSet {
		p.log.Printf("Reloading configuration")
	} else {
		p.log.Printf("Loading configuration")
	}
	p.configCTime = fileCTime
	p.configCTimeSet = true

	cfg, err := p.loadConfig(p.configPath)
	if err != nil {
		return err
	}
	// Python replaces self.config BEFORE handle_new_config; a failure
	// below therefore leaves the freshly-parsed config in place (a
	// documented residual, spec §4.5's "failed reload keeps the previous
	// running config" applies to the readers, which configureReaders
	// leaves untouched on error).
	p.cfg = cfg
	if err := p.handleNewConfig(cfg); err != nil {
		return err
	}
	if _, err := p.notifier.Notify(StateReady); err != nil {
		return err
	}
	return nil
}

// handleNewConfig ports journalpump.py:handle_new_config (950-970): the
// stats client and base tags are rebuilt on EVERY reload, the GeoIP
// database is (re)opened only when configured — Python leaves an
// open database in place when the key is absent — the field-filter and
// unit-log-level registries are rebuilt, the readers are reconfigured,
// and the running config is (re)written.
func (p *Pump) handleNewConfig(cfg *config.Config) error {
	// daemon.py:reload_config sets self.log_level from the config and
	// reconfigures logging BEFORE handle_new_config.
	if setter, ok := p.log.(LevelSetter); ok {
		setter.SetLevel(int(cfg.LogLevel))
	}
	p.stats = statsd.New(cfg.Statsd.Host, cfg.Statsd.Port, cfg.Statsd.Tags, cfg.Statsd.TagOrder, cfg.Statsd.Prefix, p.log)
	p.tags = tagged.New(cfg.Tags)
	// Install the new global snapshot so running readers consult it per
	// entry (Python's readers read the live pump).
	p.installSettings(cfg)
	// journalpump.py:get_state_file_path reads the CURRENT config, so a
	// reload that changes/enables/disables json_state_file_path moves the
	// resume reads and the periodic saves to the new path.
	p.statePath = cfg.JSONStateFilePath

	if cfg.GeoipDatabase != "" {
		p.log.Printf("Loading GeoIP data from %q", cfg.GeoipDatabase)
		db, err := geoip.Open(cfg.GeoipDatabase)
		if err != nil {
			return err
		}
		p.geoip = db
	}

	fieldFilters := make(map[string]*filter.FieldFilter, len(cfg.FieldFilters))
	for name, fcfg := range cfg.FieldFilters {
		fieldFilters[name] = filter.NewFieldFilter(name, fcfg)
	}
	p.fieldFilters = fieldFilters

	unitLogLevels := make(map[string]*filter.UnitLogLevel, len(cfg.UnitLogLevels))
	for name, entries := range cfg.UnitLogLevels {
		ull, err := filter.NewUnitLogLevel(name, entries)
		if err != nil {
			return err
		}
		unitLogLevels[name] = ull
	}
	p.unitLogLevels = unitLogLevels

	if err := p.configureReaders(cfg); err != nil {
		return err
	}
	p.writeRunningConfig(cfg)
	return nil
}

// configureReaders ports journalpump.py:configure_readers (873-927): the
// readers are replaced wholesale (any change inside the parsed `readers`
// blob replaces ALL readers — the granularity quirk) only when the parsed
// blob differs from the active one; otherwise the running readers are
// kept. The resume cursors come from the state FILE, not memory, with
// the ordered last-sender-wins / any-missing⇒replay semantics of
// state.ResumeCursor.
//
// Unlike Python, the new readers are CONSTRUCTED before the old ones are
// retired: a construction error aborts the reconfigure with the previous
// readers and the active blob intact (spec §4.5 "a failed reload keeps
// the previous running config"). Python instead resets self.readers first
// and leaves a partially-built set on the error path.
func (p *Pump) configureReaders(cfg *config.Config) error {
	newBlob := cfg.ReadersBlob()
	if !config.ReadersChanged(p.readersActiveBlob, newBlob) {
		return nil
	}

	var loaded *state.State
	if p.statePath != "" {
		st, err := p.readState(p.statePath)
		if err != nil {
			// Python's load_state only swallows FileNotFoundError; any
			// other error crashes the daemon. Surface it as a reload
			// failure (the previous readers stay live).
			return fmt.Errorf("loading state file: %w", err)
		}
		loaded = st
	}
	if loaded == nil {
		loaded = &state.State{Readers: map[string]*state.ReaderState{}}
	}

	// Document order in Python; Go's map decode lost it, so readers build
	// in sorted-name order (cosmetic deviation recorded in the parity
	// doc), matching NewPump's first-load construction.
	names := make([]string, 0, len(cfg.Readers))
	for name := range cfg.Readers {
		names = append(names, name)
	}
	sort.Strings(names)

	runtimes := make([]*ReaderRuntime, 0, len(cfg.Readers))
	for _, name := range names {
		rt, err := p.newReaderRuntime(name, cfg.Readers[name], loaded.Readers[name], p.log)
		if err != nil {
			return err
		}
		runtimes = append(runtimes, rt)
	}

	// Commit: retire the previous readers (and their senders), swap in
	// the new runtimes, start them, and record the active blob.
	for _, rt := range p.readers {
		p.retireReader(rt)
	}
	p.readers = runtimes
	for _, rt := range p.readers {
		p.startReader(rt)
	}
	p.readersActiveBlob = newBlob
	return nil
}

// startReader launches one reader goroutine under the shared reader
// errgroup. The per-reader context derives from the group context, so a
// reader failure (which cancels the group) stops this reader too, while
// retireReader can cancel just this reader. A reader error is reported
// on readerErrCh (best-effort, non-blocking) for Run's fail-fast select;
// the goroutine's return value also lands in the errgroup so shutdown's
// join preserves it.
func (p *Pump) startReader(rt *ReaderRuntime) {
	ctx, cancel := context.WithCancel(p.readerGroupCtx)
	rt.cancelRun = cancel
	rt.done = make(chan struct{})
	p.readerGroup.Go(func() error {
		defer close(rt.done)
		err := rt.run(ctx)
		if err != nil {
			select {
			case p.readerErrCh <- err:
			default:
			}
		}
		return err
	})
}

// retireReader stops one running reader and its senders — the Go port of
// JournalReader.request_stop (journalpump.py:252-255) as invoked by
// configure_readers: the reader goroutine is cancelled and joined, its
// sender loops are cancelled (buffered messages are DROPPED, matching
// Python's request_stop-then-exit rather than the D9 shutdown grace), and
// the transports are closed to release descriptors (Go resource hygiene;
// Python relies on process-lifetime daemon threads).
func (p *Pump) retireReader(rt *ReaderRuntime) {
	// A reader built but never started (a unit test, or a reload before
	// Run wired the group) has no goroutine to join.
	if rt.cancelRun != nil {
		rt.cancelRun()
		<-rt.done
	}

	p.senderMu.Lock()
	entries := p.senders[rt.name]
	delete(p.senders, rt.name)
	p.senderMu.Unlock()
	for _, entry := range entries {
		entry.cancel()
		if entry.done != nil {
			<-entry.done
		}
		switch transport := entry.inst.Transport.(type) {
		case sender.Discarder:
			// Drop the buffered-but-unpersisted tail (the checkpoint
			// never claimed it): a reload retire must not deliver lines
			// the persisted cursor does not cover, or replay would
			// duplicate them.
			_ = transport.CloseDiscarding()
		case io.Closer:
			_ = transport.Close()
		}
	}
}

// writeRunningConfig ports write_running_config (journalpump.py:929-948):
// a warning when the feature is disabled or undestinationed, otherwise
// the atomic dump; a write failure is logged and never fatal (Python's
// broad except).
func (p *Pump) writeRunningConfig(cfg *config.Config) {
	reason, err := cfg.WriteRunningConfigFile()
	if reason != "" {
		p.logWarning("%s", reason)
	}
	if err != nil {
		path := config.RunningConfigPath(cfg.JSONRunningConfigPath, os.Getenv("RUNTIME_DIRECTORY"))
		p.log.Printf("Writing running config to %q failed: %v", path, err)
	}
}
