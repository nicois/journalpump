// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package daemon

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Aiven-Open/journalpump/internal/config"
	"github.com/Aiven-Open/journalpump/internal/reader"
	"github.com/Aiven-Open/journalpump/internal/sender"
	"github.com/Aiven-Open/journalpump/internal/state"
	"golang.org/x/sync/errgroup"
)

// decodeConfig parses a config document the way the daemon does, so the
// readers blob used for reload diffing is populated.
func decodeConfig(t *testing.T, text string) *config.Config {
	t.Helper()
	cfg, err := config.Decode([]byte(text))
	if err != nil {
		t.Fatalf("Decode(%s): %v", text, err)
	}
	return cfg
}

// overrideReopener stubs the reader source factory so reload/run tests do
// not open libsystemd; each call returns a fresh idle fake reader.
func overrideReopener(t *testing.T) {
	t.Helper()
	previous := newReopener
	newReopener = func(func() reader.OpenConfig, Logger) ReaderSource {
		return &fakeSource{reader: &fakeReader{}}
	}
	t.Cleanup(func() { newReopener = previous })
}

// startReaderGroup wires the reader-lifecycle fields Run normally sets,
// so configureReaders' start/retire paths can be driven without Run.
func startReaderGroup(p *Pump) context.CancelFunc {
	ctx, cancel := context.WithCancel(context.Background())
	p.readerGroup = &errgroup.Group{}
	p.readerGroupCtx = ctx
	p.readerErrCh = make(chan error, 16)
	return cancel
}

// retireAll stops every live reader and sender of the pump (test cleanup).
func retireAll(p *Pump) {
	for _, rt := range p.readers {
		_ = p.retireReader(rt)
	}
	if p.readerGroup != nil {
		_ = p.readerGroup.Wait()
	}
}

// stateWithCursor builds a state file value whose one reader resumes from
// the given sender's sent cursor.
func stateWithCursor(readerName, senderName, cursor string) *state.State {
	c := cursor
	return &state.State{Readers: map[string]*state.ReaderState{
		readerName: {Senders: []state.NamedSenderState{{
			Name:  senderName,
			State: state.SenderState{Sent: state.SentState{Cursor: &c}},
		}}},
	}}
}

// ---------------------------------------------------------------------------
// configureReaders

// TestConfigureReadersKeepsReadersWhenBlobUnchanged locks the reload diff
// granularity: an identical parsed `readers` blob (the tags here differ)
// keeps the running readers — Python's `self.readers_active_config ==
// new_config` guard.
func TestConfigureReadersKeepsReadersWhenBlobUnchanged(t *testing.T) {
	registerFakeSenders()
	initial := decodeConfig(t, `{"readers":{"r1":{"senders":{"s":{"output_type":"fake"}}}}}`)
	updated := decodeConfig(t, `{"tags":{"env":"prod"},"readers":{"r1":{"senders":{"s":{"output_type":"fake"}}}}}`)
	p, err := NewPump(initial, PumpDeps{Log: &captureLog{}})
	if err != nil {
		t.Fatal(err)
	}
	before := p.readers[0]
	if err := p.handleNewConfig(updated); err != nil {
		t.Fatalf("handleNewConfig: %v", err)
	}
	if len(p.readers) != 1 || p.readers[0] != before {
		t.Errorf("readers were replaced despite an unchanged readers blob")
	}
	if got := p.tags.MakeTags(nil)["env"]; got != "prod" {
		t.Errorf("tags were not rebuilt on reload: env = %q", got)
	}
}

// TestConfigureReadersReplacesAndResumesCursor locks the whole-blob
// replace: a changed `readers` blob retires the old reader and builds the
// new one, resuming from the state file's sender cursor
// (last-sender-wins).
func TestConfigureReadersReplacesAndResumesCursor(t *testing.T) {
	registerFakeSenders()
	overrideReopener(t)
	initial := decodeConfig(t, `{"json_state_file_path":"/tmp/state.json","readers":{"r1":{"senders":{"s":{"output_type":"fake"}}}}}`)
	updated := decodeConfig(t, `{"json_state_file_path":"/tmp/state.json","readers":{"r2":{"senders":{"s":{"output_type":"fake"}}}}}`)
	p, err := NewPump(initial, PumpDeps{
		Log:       &captureLog{},
		ReadState: func(string) (*state.State, error) { return stateWithCursor("r2", "s", "cur9"), nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	cancel := startReaderGroup(p)
	defer cancel()
	t.Cleanup(func() { retireAll(p) })

	if err := p.handleNewConfig(updated); err != nil {
		t.Fatalf("handleNewConfig: %v", err)
	}
	if len(p.readers) != 1 {
		t.Fatalf("readers = %d, want 1", len(p.readers))
	}
	if p.readers[0].name != "r2" {
		t.Errorf("reader name = %q, want r2", p.readers[0].name)
	}
	if got := p.readers[0].currentCursor(); got != "cur9" {
		t.Errorf("resume cursor = %q, want cur9", got)
	}
	if config.ReadersChanged(p.readersActiveBlob, updated.ReadersBlob()) {
		t.Error("active readers blob was not updated to the new one")
	}
}

// failDiscardTransport is a sender.Discarder whose CloseDiscarding fails
// (an output whose complete-line boundary could not be restored).
type failDiscardTransport struct {
	*fakeTransport
}

func (t *failDiscardTransport) CloseDiscarding() error { return errors.New("truncate boom") }

// flakyDiscardTransport fails the first CloseDiscarding and succeeds on
// the retry, driving the cleanup-repair path.
type flakyDiscardTransport struct {
	*fakeTransport
	mu    sync.Mutex
	calls int
}

func (t *flakyDiscardTransport) CloseDiscarding() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.calls++
	if t.calls == 1 {
		return errors.New("truncate boom")
	}
	return nil
}

// TestConfigureReadersRefusesReplacementOnDiscardFailure locks the
// cleanup-error propagation AND its persistence across reloads: when
// retiring a reader's sender fails to discard (restore its complete-line
// boundary), the reload aborts BEFORE starting the replacement reader, so
// no replacement writer can append replay onto a corrupt prefix. The
// failed sender stays registered, so a SECOND reload with a changed config
// ctime retries the cleanup and — still failing — refuses again instead of
// treating the output as already retired.
func TestConfigureReadersRefusesReplacementOnDiscardFailure(t *testing.T) {
	registerFakeSenders()
	overrideReopener(t)
	initial := decodeConfig(t, `{"readers":{"r1":{"senders":{"s":{"output_type":"fake"}}}}}`)
	updated := decodeConfig(t, `{"readers":{"r2":{"senders":{"s":{"output_type":"fake"}}}}}`)

	var statCalls int
	t0 := time.Now()
	p, err := NewPump(initial, PumpDeps{
		Log:        &captureLog{},
		Notifier:   &fakeNotifier{},
		ConfigPath: filepath.Join(t.TempDir(), "config.json"),
		StatConfig: func(string) (time.Time, error) {
			statCalls++
			return t0.Add(time.Duration(statCalls) * time.Hour), nil
		},
		LoadConfig: func(string) (*config.Config, error) { return updated, nil },
	})
	if err != nil {
		t.Fatal(err)
	}

	canceled := 0
	entry := &senderEntry{
		name:   "s",
		cancel: func() { canceled++ },
		inst:   sender.Instance{Transport: &failDiscardTransport{}},
	}
	p.senderMu.Lock()
	p.senders["r1"] = []*senderEntry{entry}
	p.senderMu.Unlock()

	// First reload: cleanup fails, abort before the replacement starts.
	if err := p.reloadConfig(); err == nil || !strings.Contains(err.Error(), "truncate boom") {
		t.Fatalf("first reload err = %v, want the discard failure", err)
	}
	if canceled == 0 {
		t.Error("old sender was not cancelled")
	}
	if len(p.readers) != 1 || p.readers[0].name != "r1" {
		t.Errorf("replacement reader started despite the discard failure (readers=%d)", len(p.readers))
	}
	if config.ReadersChanged(p.readersActiveBlob, initial.ReadersBlob()) {
		t.Error("active readers blob changed despite the aborted reload")
	}
	// The failed entry is RETAINED — the persistent cleanup-failure state.
	p.senderMu.Lock()
	retained := len(p.senders["r1"])
	p.senderMu.Unlock()
	if retained != 1 {
		t.Fatalf("failed sender retained = %d, want 1 (persistent cleanup-failure state)", retained)
	}

	// Second reload with a CHANGED ctime must still refuse: the retained
	// entry is retried, fails again, and the reload aborts.
	if err := p.reloadConfig(); err == nil || !strings.Contains(err.Error(), "truncate boom") {
		t.Fatalf("second reload err = %v, want the discard failure", err)
	}
	if len(p.readers) != 1 || p.readers[0].name != "r1" {
		t.Errorf("second reload started a replacement reader (readers=%d)", len(p.readers))
	}
	if config.ReadersChanged(p.readersActiveBlob, initial.ReadersBlob()) {
		t.Error("active readers blob changed after the second aborted reload")
	}
}

// TestConfigureReadersProceedsAfterCleanupRepair locks the other half of
// the persistent cleanup-failure state: once a retried retire finally
// restores the output's complete-line boundary, the reload proceeds and
// swaps in the replacement reader.
func TestConfigureReadersProceedsAfterCleanupRepair(t *testing.T) {
	registerFakeSenders()
	overrideReopener(t)
	initial := decodeConfig(t, `{"readers":{"r1":{"senders":{"s":{"output_type":"fake"}}}}}`)
	updated := decodeConfig(t, `{"readers":{"r2":{"senders":{"s":{"output_type":"fake"}}}}}`)
	p, err := NewPump(initial, PumpDeps{Log: &captureLog{}})
	if err != nil {
		t.Fatal(err)
	}
	cancel := startReaderGroup(p)
	defer cancel()
	t.Cleanup(func() { retireAll(p) })

	entry := &senderEntry{
		name:   "s",
		cancel: func() {},
		inst:   sender.Instance{Transport: &flakyDiscardTransport{}},
	}
	p.senderMu.Lock()
	p.senders["r1"] = []*senderEntry{entry}
	p.senderMu.Unlock()

	if err := p.handleNewConfig(updated); err == nil || !strings.Contains(err.Error(), "truncate boom") {
		t.Fatalf("first reload err = %v, want the discard failure", err)
	}
	if err := p.handleNewConfig(updated); err != nil {
		t.Fatalf("second reload err = %v, want success once the cleanup is repaired", err)
	}
	if len(p.readers) != 1 || p.readers[0].name != "r2" {
		t.Fatalf("readers = %d (first %q), want the r2 replacement after repair", len(p.readers), p.readers[0].name)
	}
}

// TestConfigureReadersAbortsOnConstructionError locks the safe failure
// path: a bad new reader config aborts the reconfigure with the previous
// reader and active blob intact (spec §4.5 "a failed reload keeps the
// previous running config").
func TestConfigureReadersAbortsOnConstructionError(t *testing.T) {
	registerFakeSenders()
	initial := decodeConfig(t, `{"readers":{"r1":{"senders":{"s":{"output_type":"fake"}}}}}`)
	updated := decodeConfig(t, `{"readers":{"r2":{"senders":{"s":{"output_type":"nope"}}}}}`)
	p, err := NewPump(initial, PumpDeps{Log: &captureLog{}})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.handleNewConfig(updated); err == nil {
		t.Fatal("handleNewConfig = nil, want a construction error")
	}
	if len(p.readers) != 1 || p.readers[0].name != "r1" {
		t.Errorf("previous reader was not preserved on reload failure")
	}
	if config.ReadersChanged(p.readersActiveBlob, initial.ReadersBlob()) {
		t.Error("active readers blob changed despite the aborted reload")
	}
}

// ---------------------------------------------------------------------------
// write_running_config

// TestPumpWritesRunningConfigAtStartup locks the handle_new_config tail:
// the running config is written on first construction, parsed-equal to
// the loaded document.
func TestPumpWritesRunningConfigAtStartup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sub", "running_config.json")
	initial := decodeConfig(t, `{"write_running_config":true,"json_running_config_path":`+quote(path)+`,"readers":{}}`)
	if _, err := NewPump(initial, PumpDeps{Log: &captureLog{}}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("running config not written: %v", err)
	}
	written, err := config.Decode(data)
	if err != nil {
		t.Fatalf("Decode(running config): %v", err)
	}
	if !reflect.DeepEqual(written.Raw(), initial.Raw()) {
		t.Errorf("running config not parsed-equal:\n got %v\nwant %v", written.Raw(), initial.Raw())
	}
	if info, err := os.Stat(path); err == nil && info.Mode().Perm() != 0o600 {
		t.Errorf("running config mode = %o, want 600", info.Mode().Perm())
	}
}

// TestPumpWarnsRunningConfigWithoutEnable locks the warning Python logs
// when json_running_config_path is set but write_running_config is not
// true (journalpump.py:930-934).
func TestPumpWarnsRunningConfigWithoutEnable(t *testing.T) {
	log := &captureLog{}
	cfg := decodeConfig(t, `{"json_running_config_path":"/tmp/whatever.json","readers":{}}`)
	if _, err := NewPump(cfg, PumpDeps{Log: log}); err != nil {
		t.Fatal(err)
	}
	if !log.contains("write_running_config is not true") {
		t.Errorf("missing warning; lines = %v", log.lines)
	}
}

// levelLog records SetLevel calls (the OPTIONAL LevelSetter capability).
type levelLog struct {
	captureLog
	levels []int
}

func (l *levelLog) SetLevel(level int) { l.levels = append(l.levels, level) }

// TestReloadAppliesLogLevel locks the logging wiring: a reload applies the
// config log_level through SetLevel (daemon.py sets log_level then
// reconfigures logging on every reload).
func TestReloadAppliesLogLevel(t *testing.T) {
	registerFakeSenders()
	initial := decodeConfig(t, `{"log_level":"INFO","readers":{}}`)
	updated := decodeConfig(t, `{"log_level":"WARNING","readers":{}}`)
	log := &levelLog{}
	p, err := NewPump(initial, PumpDeps{Log: log})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.handleNewConfig(updated); err != nil {
		t.Fatalf("handleNewConfig: %v", err)
	}
	if len(log.levels) == 0 || log.levels[len(log.levels)-1] != 30 {
		t.Fatalf("SetLevel calls = %v, want the last to be 30 (WARNING)", log.levels)
	}
}

// TestReloadUpdatesStatePath locks get_state_file_path being re-read on
// reload: a changed json_state_file_path moves the resume reads and saves,
// for both an unchanged and a replaced readers blob.
func TestReloadUpdatesStatePath(t *testing.T) {
	registerFakeSenders()
	overrideReopener(t)
	empty := func(string) (*state.State, error) {
		return &state.State{Readers: map[string]*state.ReaderState{}}, nil
	}
	initial := decodeConfig(t, `{"json_state_file_path":"/old/state.json","readers":{"r1":{"senders":{"s":{"output_type":"fake"}}}}}`)
	p, err := NewPump(initial, PumpDeps{Log: &captureLog{}, ReadState: empty})
	if err != nil {
		t.Fatal(err)
	}
	if p.statePath != "/old/state.json" {
		t.Fatalf("statePath = %q, want /old/state.json", p.statePath)
	}

	// Unchanged readers blob, new state path.
	same := decodeConfig(t, `{"json_state_file_path":"/new/state.json","readers":{"r1":{"senders":{"s":{"output_type":"fake"}}}}}`)
	if err := p.handleNewConfig(same); err != nil {
		t.Fatalf("handleNewConfig (unchanged readers): %v", err)
	}
	if p.statePath != "/new/state.json" {
		t.Errorf("statePath after unchanged-blob reload = %q, want /new/state.json", p.statePath)
	}

	// Replaced readers resume from the NEW path.
	cancel := startReaderGroup(p)
	defer cancel()
	t.Cleanup(func() { retireAll(p) })
	var readPaths []string
	p.readState = func(path string) (*state.State, error) {
		readPaths = append(readPaths, path)
		return &state.State{Readers: map[string]*state.ReaderState{}}, nil
	}
	replaced := decodeConfig(t, `{"json_state_file_path":"/new2/state.json","readers":{"r2":{"senders":{"s":{"output_type":"fake"}}}}}`)
	if err := p.handleNewConfig(replaced); err != nil {
		t.Fatalf("handleNewConfig (replaced readers): %v", err)
	}
	if p.statePath != "/new2/state.json" {
		t.Errorf("statePath after replaced-blob reload = %q, want /new2/state.json", p.statePath)
	}
	if len(readPaths) == 0 || readPaths[len(readPaths)-1] != "/new2/state.json" {
		t.Errorf("state reads = %v, want the new path last", readPaths)
	}
}

// failingSource is a ReaderSource whose Ensure always fails, so a reloaded
// reader goroutine reports an error immediately.
type failingSource struct{ err error }

func (f failingSource) Ensure() (reader.Reader, error) { return nil, f.err }
func (failingSource) Invalidate()                      {}

// TestZeroReaderStartDeliversLaterReaderFailure locks the reader-error
// channel capacity: starting with ZERO readers and reloading in one that
// then fails must still surface the failure to Run (an unbuffered channel
// would drop startReader's non-blocking report).
func TestZeroReaderStartDeliversLaterReaderFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		disableSignals(t)
		registerFakeSenders()
		boom := errors.New("reader boom")
		previous := newReopener
		newReopener = func(func() reader.OpenConfig, Logger) ReaderSource { return failingSource{err: boom} }
		t.Cleanup(func() { newReopener = previous })

		initial := decodeConfig(t, `{"readers":{}}`)
		updated := decodeConfig(t, `{"readers":{"r1":{"senders":{"s":{"output_type":"fake"}}}}}`)
		var statCalls int
		t0 := time.Now()
		p, err := NewPump(initial, PumpDeps{
			Log:        &captureLog{},
			Notifier:   &fakeNotifier{},
			ConfigPath: filepath.Join(t.TempDir(), "config.json"),
			StatConfig: func(string) (time.Time, error) {
				statCalls++
				if statCalls <= 1 {
					return t0, nil
				}
				return t0.Add(time.Hour), nil
			},
			LoadConfig: func(string) (*config.Config, error) { return updated, nil },
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(p.readers) != 0 {
			t.Fatalf("readers = %d, want 0", len(p.readers))
		}

		hup := make(chan os.Signal, 1)
		done := make(chan error, 1)
		go func() { done <- p.Run(context.Background(), hup) }()
		synctest.Wait()
		hup <- syscall.SIGHUP
		synctest.Wait()

		select {
		case err := <-done:
			if err == nil || !strings.Contains(err.Error(), "reader boom") {
				t.Fatalf("Run = %v, want the reader failure", err)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("reader failure dropped after a zero-reader start")
		}
	})
}

// ---------------------------------------------------------------------------
// SIGHUP reload through Run

// TestRunSIGHUPReload drives a real reload through the Run loop: a SIGHUP
// with a changed config ctime notifies RELOADING=1 then READY=1 and swaps
// in the readers of the new config, resuming from the state file.
func TestRunSIGHUPReload(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		disableSignals(t)
		registerFakeSenders()
		overrideReopener(t)

		initial := decodeConfig(t, `{"json_state_file_path":"/tmp/state.json","readers":{"r1":{"senders":{"s":{"output_type":"fake"}}}}}`)
		updated := decodeConfig(t, `{"json_state_file_path":"/tmp/state.json","readers":{"r2":{"senders":{"s":{"output_type":"fake"}}}}}`)

		var statCalls int
		t0 := time.Now()
		notifier := &fakeNotifier{}
		log := &captureLog{}
		p, err := NewPump(initial, PumpDeps{
			Log:        log,
			Notifier:   notifier,
			ConfigPath: filepath.Join(t.TempDir(), "config.json"),
			StatConfig: func(string) (time.Time, error) {
				statCalls++
				if statCalls <= 1 {
					return t0, nil
				}
				return t0.Add(time.Hour), nil
			},
			LoadConfig: func(string) (*config.Config, error) { return updated, nil },
			ReadState:  func(string) (*state.State, error) { return stateWithCursor("r2", "s", "cur9"), nil },
		})
		if err != nil {
			t.Fatal(err)
		}
		if p.readers[0].name != "r1" {
			t.Fatalf("initial reader = %q, want r1", p.readers[0].name)
		}

		hup := make(chan os.Signal, 1)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- p.Run(ctx, hup) }()

		synctest.Wait()
		hup <- syscall.SIGHUP
		synctest.Wait()

		if len(p.readers) != 1 || p.readers[0].name != "r2" {
			t.Fatalf("readers after reload = %v, want [r2]", readerNames(p.readers))
		}
		if got := p.readers[0].currentCursor(); got != "cur9" {
			t.Errorf("resume cursor after reload = %q, want cur9", got)
		}
		if !log.contains("Received SIGHUP") {
			t.Errorf("missing SIGHUP log; lines = %v", log.lines)
		}
		if !notifier.notified(StateReloading) || !notifier.notified(StateReady) {
			t.Errorf("missing RELOADING/READY notify; states = %v", notifier.states)
		}

		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("Run did not return after cancellation")
		}
	})
}

// TestRunSIGHUPNoChangeIsNoop locks the ctime guard: an unchanged config
// ctime makes SIGHUP a no-op (no reader swap, no RELOADING notify).
func TestRunSIGHUPNoChangeIsNoop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		disableSignals(t)
		registerFakeSenders()
		overrideReopener(t)

		initial := decodeConfig(t, `{"readers":{"r1":{"senders":{"s":{"output_type":"fake"}}}}}`)
		notifier := &fakeNotifier{}
		loaded := false
		p, err := NewPump(initial, PumpDeps{
			Log:        &captureLog{},
			Notifier:   notifier,
			ConfigPath: filepath.Join(t.TempDir(), "config.json"),
			StatConfig: func(string) (time.Time, error) { return time.Unix(100, 0), nil },
			LoadConfig: func(string) (*config.Config, error) { loaded = true; return initial, nil },
		})
		if err != nil {
			t.Fatal(err)
		}
		before := p.readers[0]

		hup := make(chan os.Signal, 1)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- p.Run(ctx, hup) }()
		synctest.Wait()
		hup <- syscall.SIGHUP
		synctest.Wait()

		if loaded {
			t.Error("config was re-read despite an unchanged ctime")
		}
		if p.readers[0] != before {
			t.Error("readers were replaced despite an unchanged config")
		}
		if notifier.notified(StateReloading) {
			t.Error("RELOADING=1 notified for a no-op reload")
		}

		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("Run did not return")
		}
	})
}

// TestRunSIGHUPReloadFailureKeepsRunning locks reload_if_requested's
// catch: a failed load is logged as "Reloading configuration failed" and
// the daemon keeps running with its previous readers.
func TestRunSIGHUPReloadFailureKeepsRunning(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		disableSignals(t)
		registerFakeSenders()
		overrideReopener(t)

		initial := decodeConfig(t, `{"readers":{"r1":{"senders":{"s":{"output_type":"fake"}}}}}`)
		var statCalls int
		log := &captureLog{}
		p, err := NewPump(initial, PumpDeps{
			Log:        log,
			Notifier:   &fakeNotifier{},
			ConfigPath: filepath.Join(t.TempDir(), "config.json"),
			StatConfig: func(string) (time.Time, error) {
				statCalls++
				return time.Unix(int64(statCalls), 0), nil
			},
			LoadConfig: func(string) (*config.Config, error) { return nil, errReloadBoom },
		})
		if err != nil {
			t.Fatal(err)
		}

		hup := make(chan os.Signal, 1)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- p.Run(ctx, hup) }()
		synctest.Wait()
		hup <- syscall.SIGHUP
		synctest.Wait()

		if !log.contains("Reloading configuration failed") {
			t.Errorf("missing failure log; lines = %v", log.lines)
		}
		if len(p.readers) != 1 || p.readers[0].name != "r1" {
			t.Errorf("readers changed on a failed reload")
		}

		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("Run did not return")
		}
	})
}

var errReloadBoom = &reloadBoomError{}

type reloadBoomError struct{}

func (*reloadBoomError) Error() string { return "config load boom" }

// workerJoined reports whether a worker lifecycle channel is closed — the
// "is this goroutine actually joined?" check used by the reload leak tests.
// A closed channel means the worker finished; an open one means it is still
// running (a leak, or an unjoined retire).
func workerJoined(done <-chan struct{}) bool {
	select {
	case <-done:
		return true
	default:
		return false
	}
}

// TestJoinedWorkerCheckDetectsUnjoined verifies the leak assertion itself is
// not vacuous: workerJoined must report false for a still-open lifecycle
// channel and true once it is closed. Without this negative control a test
// that merely asserts workerJoined twice would pass even if the helper always
// returned true.
func TestJoinedWorkerCheckDetectsUnjoined(t *testing.T) {
	open := make(chan struct{})
	if workerJoined(open) {
		t.Fatal("workerJoined reported a running worker as joined")
	}
	close(open)
	if !workerJoined(open) {
		t.Fatal("workerJoined reported a finished worker as unjoined")
	}
}

// TestReloadJoinsRetiredWorkers is the lifecycle-leak assertion the binary
// systest cannot make (os.Exit abandons goroutines regardless of exit status).
// It drives a CHANGED-reader SIGHUP reload through the real Run loop with an
// already-initialized reader+sender, then asserts the retired reader's run
// goroutine and every one of its sender loops have been joined (their done
// channels closed) before Run reports the reload complete.
func TestReloadJoinsRetiredWorkers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		disableSignals(t)
		registerFakeSenders()
		overrideReopener(t)

		initial := decodeConfig(t, `{"readers":{"r1":{"senders":{"s":{"output_type":"fake"}}}}}`)
		updated := decodeConfig(t, `{"readers":{"r2":{"senders":{"s":{"output_type":"fake"}}}}}`)

		var statCalls int
		t0 := time.Now()
		p, err := NewPump(initial, PumpDeps{
			Log:        &captureLog{},
			Notifier:   &fakeNotifier{},
			ConfigPath: filepath.Join(t.TempDir(), "config.json"),
			StatConfig: func(string) (time.Time, error) {
				statCalls++
				if statCalls <= 1 {
					return t0, nil
				}
				return t0.Add(time.Hour), nil
			},
			LoadConfig: func(string) (*config.Config, error) { return updated, nil },
			ReadState:  func(string) (*state.State, error) { return stateWithCursor("r2", "s", "cur9"), nil },
		})
		if err != nil {
			t.Fatal(err)
		}

		hup := make(chan os.Signal, 1)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- p.Run(ctx, hup) }()

		// Let the r1 reader open and initialize its sender: without this the
		// changed-reader reload has no running worker to join and the test
		// would pass vacuously.
		time.Sleep(2 * pollInterval)
		synctest.Wait()

		old := p.readers[0]
		if old.name != "r1" {
			t.Fatalf("initial reader = %q, want r1", old.name)
		}
		if workerJoined(old.done) {
			t.Fatal("r1 reader goroutine already finished before reload; test would not exercise joining")
		}
		old.mu.Lock()
		oldEntries := make([]*senderEntry, 0, len(old.senders))
		for _, e := range old.senders {
			oldEntries = append(oldEntries, e)
		}
		old.mu.Unlock()
		if len(oldEntries) == 0 {
			t.Fatal("r1 sender was never initialized; cannot test worker joining")
		}

		hup <- syscall.SIGHUP
		synctest.Wait()

		if len(p.readers) != 1 || p.readers[0].name != "r2" {
			t.Fatalf("readers after reload = %v, want [r2]", readerNames(p.readers))
		}
		if !workerJoined(old.done) {
			t.Errorf("retired r1 reader goroutine not joined after reload")
		}
		for _, e := range oldEntries {
			if !workerJoined(e.done) {
				t.Errorf("retired r1 sender loop %q not joined after reload", e.name)
			}
		}

		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("Run did not return")
		}
	})
}

// readerNames lists the reader names for diagnostics.
func readerNames(readers []*ReaderRuntime) []string {
	names := make([]string, 0, len(readers))
	for _, rt := range readers {
		names = append(names, rt.name)
	}
	return names
}

// quote renders a path as a JSON string literal.
func quote(s string) string {
	return `"` + strings.ReplaceAll(s, `\`, `\\`) + `"`
}

// closeOnlyTransport is an io.Closer that is NOT a Discarder, so retireReader
// takes its io.Closer branch (the ES sender's shape: no buffered output to
// discard, but idle HTTP connections to release).
type closeOnlyTransport struct {
	*fakeTransport
	closes int32
}

func (t *closeOnlyTransport) Close() error {
	atomic.AddInt32(&t.closes, 1)
	return nil
}

// TestRetireReaderClosesCloserTransport locks that retiring a sender on reload
// releases io.Closer transports (the ES sender closes its idle connections).
func TestRetireReaderClosesCloserTransport(t *testing.T) {
	registerFakeSenders()
	p, err := NewPump(decodeConfig(t, `{"readers":{"r1":{"senders":{"s":{"output_type":"fake"}}}}}`), PumpDeps{Log: &captureLog{}})
	if err != nil {
		t.Fatal(err)
	}
	tr := &closeOnlyTransport{fakeTransport: &fakeTransport{}}
	p.senderMu.Lock()
	p.senders["r1"] = []*senderEntry{{name: "s", cancel: func() {}, inst: sender.Instance{Transport: tr}}}
	p.senderMu.Unlock()

	if err := p.retireReader(&ReaderRuntime{name: "r1"}); err != nil {
		t.Fatalf("retireReader: %v", err)
	}
	if got := atomic.LoadInt32(&tr.closes); got != 1 {
		t.Fatalf("Close calls = %d, want 1", got)
	}
}
