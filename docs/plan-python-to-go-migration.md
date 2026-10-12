# Plan: journalpump Python → Go migration (implementation tasks)

Source spec: `docs/spec-python-to-go-migration.md` (final, rev 8). Decisions
D1–D12 referenced below are §9 of the spec (D5 revised 2026-10-11: kafka
client is twmb/franz-go, not IBM/sarama — see Task 22). Every task's diff
goes through the dual review gate (parity + Go idiom, charter §5).

## Conventions for every task (D1, D10, thread A)

- **Branch**: all work on a dedicated branch of THIS repo (e.g.
  `python-to-go`); the Go tree (`cmd/`, `internal/`, `go.mod`) is added
  alongside the untouched Python tree. No changes to main's release
  process.
- **Version**: Go binary version comes from `git describe --tags` with
  `VERSION` env override, mirroring the Makefile's `short_ver`/`long_ver`
  logic.
- **Tests**: unit tests live next to the code (`*_test.go`, run by
  `go test -race ./...`); system tests under `systest-go/` with a `systest`
  build tag; `make unittest-go` / `make systest-go` targets mirror the
  Python ones. Table-driven tests with `t.Run`, `t.Parallel` where safe,
  and `testing/synctest` (stable/default from Go 1.25; Go 1.24 requires
  `GOEXPERIMENT=synctest` — D10 mandates the latest stable toolchain, so
  the default-on path applies) for timing-dependent logic instead of
  sleeps (see Tasks 13/17/19); go-cmp / `reflect.DeepEqual` for golden and
  state comparisons. A task is not done until its named tests exist and
  pass.
- **Errors**: wrap with `%w`; typed/sentinel errors for the
  `SenderInitializationError` equivalent; no panic in library code;
  goroutine errors surfaced via errgroup or logged-and-continue exactly
  where Python's broad-except was.
- **Parity checklist**: `docs/go-test-parity.md` tracks every Python unit
  test function (≈127; `test/test_journalpump.py` alone has 64) as ported /
  Go-idiomatic / dropped-with-reason, and Go-native tests with no Python
  counterpart as `go-native` (with recorded reason) so coverage deltas
  stay explicit. Tasks 4, 5, 6, 8, 13, 15, and every sender task update
  it; Task 29 verifies completeness.
- Every preserved quirk from spec §3/§5 gets a named quirk-lock test that
  fails if the behavior drifts (spec risk 5).

---

## Phase 0 — Scaffold and frontloaded e2e

### Task 1. Branch scaffold and Go module skeleton
Files: `go.mod`, `cmd/journalpump/main.go` (usage + config-path arg + exit
codes), `internal/` package dirs (spec §4.1), `Makefile` (new targets
`build-go`, `unittest-go`, `systest-go`, `lint-go`; version from
`git describe`), `internal/version` package.
Acceptance: `make build-go` builds a binary that prints usage on no args
and exits 1 with a missing-config error, mirroring `daemon.py:main`.
Tests: `cmd/journalpump` test asserting exit codes for missing/invalid
config path (parity: daemon.py main behavior).

### Task 2. CI workflow: lint + unit test (D10)
Files: `.github/workflows/build-go.yml` (or extend `build.yml` in the
branch): latest stable Go; `golangci-lint` with gocyclo, ineffassign,
revive, staticcheck, errcheck, govet, gocritic; `go vet`;
`go test -race ./...`; systest job added in Task 30.
Acceptance: workflow runs on PRs touching Go files and is green on the
skeleton; lint config (`golangci.yml`) checked in.
Tests: CI itself; a trivial intentional lint failure must block (verified
once on the branch).

### Task 3. Config loader and running-config writer
Files: `internal/config/`. Full `journalpump.json` schema with defaults
(spec §4.5, §5 key sets; code is source of truth), validation errors
(typed decoding rejects wrong-typed values at load — sanctioned deviation,
NEWS in Task 28), `write_running_config` (RUNTIME_DIRECTORY, dir 0700 /
file 0600, warning when path set without opt-in; writer emits
alphabetically sorted keys, matching Python's
`json.dump(..., sort_keys=True)`, `journalpump.py:946`). **Reload diffing
captures the parsed `readers` as `map[string]any`** — compared with
`reflect.DeepEqual`/go-cmp, NOT raw JSON text (Python compares parsed
dicts, order/whitespace-insensitive, `journalpump.py:875`).
Acceptance: `journalpump.json` and every documented/implemented key parse
with identical defaults; unknown-key handling matches Python (ignore, as
today); a whitespace/order change in the JSON produces NO reload, a value
change does.
Tests: golden config tests per sender key set; reload-diff equivalence
tests (order-insensitivity, whitespace-insensitivity); running-config
tests (parity: `test/test_running_config.py` 5 tests +
`systest/test_running_config.py` 7 cases — file modes asserted by
spawning a binary in Task 10/18; unit level here).

### Task 4. Journal entry model, converters, JSON serialization
Files: `internal/journal/`. Entry struct with byte field values; Python
converter table; `i=<hex>` cursor seqnum parse (`__SEQNUM`); **repr-style
fallback for undecodable bytes** (D8): a value that is not valid UTF-8 is
replaced by its Python-`repr`-equivalent string before serialization
(`journalpump.py:711-714`), so raw invalid bytes never reach the wire;
deterministic JSON key order matching Python construction order; HTML
escaping off; `extra_field_values` merge.
Acceptance: serialized output for a fixture entry (including an entry
with invalid UTF-8) is byte-identical to Python's `json.dumps` output.
Tests: golden-file parity test with fixture journal entries incl. an
invalid-UTF-8 fixture (spec risk 2); cursor-seqnum tests (parity: seqnum
extraction tests in `test/test_journalpump.py`); fuzz test for the cursor
parser; update parity checklist.

### Task 5. Filters
Files: `internal/filter/`. FieldFilter whitelist/blacklist (case-
insensitive, leading-underscore trim); SecretFilter regex with `\1`→`$1`
translation at load + startup warning for unsupported constructs;
`secret_filter_metrics`; Search regex with hit counters and
`journal.perform_search_regex_duration`; UnitLogLevel semantics exactly as
implemented (`journalpump.py:691-696`): **an entry is dropped if ANY
glob matching the unit has a more restrictive log level than the
configured level (most-restrictive-wins within the matched config)** —
NOT first-match-only.
Acceptance: all filter semantics match Python; filters are constructed
objects handed to senders at construction time (spec §3 reload corollary).
Tests: parity ports of the 12 SecretFilter tests, 10 UnitLogLevel tests
(most-restrictive-wins case included), FieldFilter tests, search tests
from `test/test_journalpump.py`; update parity checklist.

### Task 6. GeoIP / geohash enrichment
Files: `internal/geohash/` (pure encoder port of `journalpump/geohash.py`:
base32 encoding, longitude wrap into [-180,180), lat>=90 raise) +
GeoIP database loading (`geoip_database` config, .mmdb via
oschwald/geoip2-golang) and the search tag callables registry —
`ip_to_geohash(ip_tag_name, precision)` (`journalpump.py:488-504`,
callables 512-544) — placed with the reader/search handling, NOT in
internal/geohash (spec §4.1/§4.4).
Acceptance: a `searches` config using `ip_to_geohash` produces identical
tag values to Python (spec risk 9: empty-tag regression guarded).
Tests: geohash encoder golden vectors; tag-callable unit tests with a
test .mmdb fixture; update parity checklist.

### Task 7. statsd client and tag sanitization
Files: `internal/statsd/`. Raw UDP, telegraf tag syntax
(`metric;tag=val:value|type`), gauge/counter/timing, prefix, global tags;
`Tagged.make_tags` port — unsafe name/value character substitution
(`base.py:42-60`).
Acceptance: wire format identical to `journalpump/statsd.py`; sanitized
tags identical to Python.
Tests: UDP socket capture test asserting exact datagram bytes for every
metric type; `make_tags` sanitization cases (parity:
`test/unit/senders/test_base.py` 6 parametrized cases); update parity
checklist.

### Task 8. State file I/O (D3)
Files: `internal/state/`. Schema parity with Python's `get_state`:
`readers.{name}.cursor`, `readers.{name}.searches.{name}.hits`
(`journalpump.py:328-335`), `senders.{name}.cursor/sent_count/sent_bytes/
health/errors`, totals, `start_time`; atomic tmp+rename; 10 s cadence;
read+write. **Ordered decode**: the per-reader `senders` object is parsed
with `json.Decoder` token streaming into ordered `(name, cursor)` pairs —
resume semantics depend on state-file KEY ORDER (`journalpump.py:890`)
and `encoding/json` into a map would discard it. **Encode (amended,
user decision 2026-10-09 — charter §2)**: originally the writer emitted
alphabetically sorted keys byte-faithful to Python's
`json.dump(..., sort_keys=True)` (`journalpump.py:1070`); per the
governing principle sort_keys is over-prescription, so the writer now
uses Go 1.27's `encoding/json/v2` with Go-native key order — the
compatibility contract is the PARSED document, no test asserts a
Go-written file's key ordering, and the ordered decode above is
unchanged (the resume quirk reads the file's key order whatever it
is).
Acceptance: reads a Python-written state file; a Go-written state file is
readable by Python — locked permanently, not one-off.
Tests: round-trip tests against a committed Python-generated fixture; a
committed Go-written fixture plus a **permanent Python-side pytest**
(`test/test_state_file_interop.py`, added on the branch) that reads the
Go fixture and asserts identical resume-cursor selection — both
directions pinned in CI; resume-cursor selection helpers tested here:
last-sender-wins in **key order** (quirk-lock test where last-listed
sender ≠ alphabetically-first) and any-missing⇒None (spec §6.3, risk 8).
The last-listed-≠-alphabetically-first fixture must be **hand-crafted**:
Python's `sort_keys=True` means captured Python-written files always
order senders alphabetically and cannot exhibit the quirk.

### Task 9. sd_notify, watchdog, signals
Files: `internal/daemon/` (notify + signal part). `READY=1`,
`RELOADING=1`→`READY=1`, `STOPPING=1`, `WATCHDOG=1` ping each loop
iteration; **SIGTERM/SIGINT via `signal.NotifyContext`** (the run context,
spec §4.3); **SIGHUP via a separate buffered `signal.Notify` channel
(`hupCh`), consumed by the supervisor's select — SIGHUP is a reload
event, not cancellation** (thread A3).
Acceptance: same notify datagrams as `journalpump_process.py` expects;
signals compose per spec §4.3.
Tests: harness test spawning the stub binary with an abstract-socket
`NOTIFY_SOCKET`; signal-composition unit tests (SIGHUP does not cancel
runCtx; SIGTERM does).

### Task 10. Implementation-agnostic systest harness + first black-box suites (thread C1)
Files: `systest-go/journalpump_process_test.go` (harness) + suites. The harness
spawns *a binary*: env `JOURNALPUMP_BIN` selects the implementation —
unset/`go` runs the built Go binary, `python` runs
`python3 -m journalpump` with the same env (`RUNTIME_DIRECTORY`, abstract
`NOTIFY_SOCKET`); harness asserts sd_notify (`READY=1`, `RELOADING=1`→
`READY=1`, `WATCHDOG=1`), handles SIGHUP reload and SIGTERM with exit-code
capture. Immediately port `systest/test_running_config.py` (7 cases) and
`systest/test_reload.py` against it and run them against BOTH
implementations.
Acceptance: the same suites pass against the Python binary today and are
the standing harness every later systest uses.
Re-scope note (Task 10 review, 2026-10-09): the Go-side harness
(systest-go/journalpump_process_test.go) is deferred to Task 18, where the
Go systest suites land; the Python-side harness rework
(JOURNALPUMP_BIN selection) was delivered in Task 10 and is the
mechanism both implementations will use.
Tests: the two ported suites (both implementations); harness unit tests
(notify socket, exit-code capture).
Depends on: Tasks 1, 9.

### Task 11. Python ground-truth e2e capture: file/logplex/ES/websocket/state/replay (thread C2)
Files: `systest-go/groundtruth_*_test.go` + committed fixtures under
`systest-go/fixtures/`. Run the **Python binary** through the Task 10
harness with local capture servers, and commit the captured wire bytes as
golden fixtures that later Go tasks must match:
- **file sender**: filtered output file bytes for a fixture journal.
- **logplex**: POST bodies + headers captured by a local HTTP server
  (Python's actual bytes-repr wire framing is Python ground truth only —
  sanctioned deviation, charter §2/D6: Go sends the clean RFC frame and
  content parity is asserted via the repr-stripped frame list);
  quirk-lock evidence: 500 response does NOT trigger retry/disconnect;
  hostname/pid literal "localhost".
- **ES/OpenSearch**: fake HTTP server capturing index-create + `_bulk`
  NDJSON bytes incl. daily rollover and mapping-from-first-message.
- **websocket**: local websocket server capturing frames (NUL-joined
  batch, snappy bytes, `Journalpump-Batch-Size`/`Journalpump-Compression`
  headers).
- **state files**: Python-written state fixtures committed; Python
  `get_state` assertions recorded (the Python→Go→Python round trip's
  outer legs; the Go middle leg lands in Tasks 8/18).
- **missing-cursor replay**: capture that Python replays from head when
  any sender cursor is missing (quirk-lock ground truth).
Acceptance: fixtures committed; each capture test asserts wire bytes, not
library call shape (thread B rule).
Tests: the capture tests themselves (run against the Python binary in CI
until Task 29 retires that mode; then they flip to Go-binary parity
assertions).

Re-scope note (Task 11, 2026-10-09): the Go-side files named here
(systest-go/groundtruth_*_test.go) are deferred to Task 18 together
with the deferred Go harness; Task 11 delivers the committed fixtures
under systest-go/fixtures/ plus the Python capture tests
(systest/test_groundtruth_*.py) that produce and verify them, so a Go
test can consume the same manifest files unchanged.
Depends on: Task 10.

### Task 12. Kafka ground truth via real broker or recording proxy (thread C2)
Files: `systest-go/groundtruth_kafka_test.go` + fixtures. Against a real
broker (CI service) or a recording proxy: run the **Python binary**
producing a fixture journal (fixed `kafka_msg_key`, snappy compression),
consume with kafka-python, and record: delivered byte payloads, key/
partition placement, topic auto-create behavior. This is the ground truth
for the Go kafka sender (Task 22 — client-agnostic: it validated the
sarama implementation and is re-run unchanged against the franz-go
rewrite) and the xerial-framing interop question (spec §5 kafka, risk
10).
Acceptance: recorded broker fixtures committed; auto-create and
key-partition behavior documented in the task output.
Tests: capture tests; consumer assertions include decompression and
byte-level payload comparison.
Depends on: Task 10.

## Phase 1 — Core pipeline + file sender

### Task 13. Sender framework (shared flush/backoff loop)
Files: `internal/sender/` (interface, wiring map, shared loop). Sender
interface per spec §4.2: `SendMessages(ctx, batch, cursor) error` +
`retriable(error) bool` classifier seam per sender + typed
`SenderInitializationError` equivalent. 100 ms tick; flush when >1000
buffered or idle > `max_send_interval`; 1 MiB batch slicing (30 B
overhead, per-sender overridable); jittered exponential backoff 0.5 s→
1800 s reset on connect; health ring ≤8 deduped 128-char errors;
`mark_connected/mark_disconnected` driven by the classified error;
sent-cursor bookkeeping (last message's cursor, advance on ack only);
stats gauges (`journal.last_sent_ago/sent_bytes/sent_lines/status`);
`maintenance_operations` with 60 s failure retry; `sender.failed_to_start`
gauge every 10 s. **Explicit wiring map** in `internal/sender` (one
greppable map `output_type → constructor`, no blank imports/init()
registration — thread A11).
Acceptance: loop behavior indistinguishable from `senders/base.py`;
error classification routes retriable vs permanent exactly like Python's
per-sender except-clauses.
Depends on: Task 7 (statsd), Task 8 (state).
Tests: unit tests for flush triggers, batch slicing boundaries, backoff
curve (jitter bounds), health ring dedup/truncation, classifier routing,
cursor-advance-on-ack only; `testing/synctest` bubble for the 100 ms tick
and backoff timing (thread A9); `-race` clean; update parity checklist
(`test/unit/senders/test_base.py`, kafka config tests land in Task 22).

### Task 14. Per-sender buffer and reader backpressure
Files: `internal/buffer/`. Channel-per-sender `(payload, cursor)` with
message-count and byte budget per spec §4.3: capacity =
`msg_buffer_max_length` (+ headroom); **admission gate = pre-read
worst-sender check** (50000 messages / 5 MiB defaults); failed enqueue
retries with `select { case ch <- item: case <-ctx.Done(): }` — never
drops; flush trigger reads `len(chan)`.
Acceptance: reader pauses when any sender's buffer hits a limit; nothing
is dropped; no head-of-line blocking across senders.
Tests: backpressure unit tests (fill one slow sender, assert reader stop
and resume); enqueue-retry/ctx-cancel tests; `testing/synctest` for
capacity/admission timing; `-race` under concurrent producer/consumer.

### Task 15. sdjournal reader (incl. reader-side stats)
Files: `internal/reader/`. **Consumer-side seam** (thread A12):
`Wait(ctx, timeout) (Event, error)`, `ReadBatch(n) ([]Entry, error)`,
`Seek(cursor) error`, `Close()` — plus the sdjournal implementation:
open with `journal_flags`, `journal_path`, `journal_files`,
`journal_namespace`, `journald_filters`, `units_to_match`;
`initial_position` head/tail/seconds-from-boot (`seek_monotonic`); chunk
reads of 5000 with 1 s poll budget; APPEND/INVALIDATE handling with
reader recreation ≤1 per 2 s; missing-journal-files close-and-retry;
"Bad message" tolerance counter; cursor seek + one-entry skip on resume.
**`match_key`/`match_value` are entry-level equality checks applied in
the handler** (`journalpump.py:642-653, 723-727`), NOT sd_journal
`add_match` filters — implement there. Reader-side observability lives
here too: `inc_line_stats` / `set_persistent_gauge` emit-on-change, 10 s
per-reader throttle, gauges `journal.read_lines`, `journal.read_bytes`,
`journal.last_read_ago` (`journalpump.py:350-383`).
Acceptance: reads the binary `.journal` fixtures from `systest/data/`
identically to Python (same 40 messages — probed twice and confirmed
by review; the original "30" figure was wrong).
Tests: unit tests with the fake reader seam; fixture-driven tests for
initial-position modes and bad-message data (`systest/data/bad_msg/`);
reader-stats tests (parity: `test/unit/reader/test_stats.py`); `testing/
synctest` for the 2 s rotation-retry cadence; spike note (spec risk 3)
filed after first run against fixtures; update parity checklist.

### Task 16. Run loop and per-sender serialization pipeline + file sender
Files: `internal/daemon/` (run loop), `internal/journal` handler glue,
`internal/sender/file/`. Per-reader goroutine driving the poll loop;
per-sender serialization applying field filter, unit log level, secret
filters, searches (with tag callables from Task 6), `extra_field_values`,
`match_key`/`match_value`; senders constructed from the wiring map with
filter objects captured at construction (reload corollary); file sender
(append-binary, one JSON line + `\n`, never fails) — **parity-asserted
against the Task 11 file fixture**.
Acceptance: a running Go binary tails a journal to a file with output
byte-identical to the captured Python fixture.
**Value-demonstration checkpoint (user decision, 2026-10-09 — charter §6
last-value-demonstration metric)**: before Task 17 begins, the Go binary
must demonstrably run end-to-end — reading real journald entries through
the Task 15 reader, applying filters and serialization, and delivering
via the file sender to a real output file, driven by a config in the
production `journalpump.json` format. Record how the run was executed
and what was observed in the ledger's "last value demonstration" line
(charter §6; ledger edits are builder-only charter commits per
charter §5). This is a mid-course checkpoint, NOT a replacement for
acceptance: the Task 18 systest sweep remains the full gate (Milestone A).
Tests: unit tests for the serialization pipeline (filter/unit-log-level/
search combinations); file sender golden test vs Task 11 fixture;
quirk-lock test for filter capture at construction.
Depends on: Tasks 3, 4, 5, 6, 9, 13, 14, 15, 11 (file fixture).

### Task 17. Reload, resume, state save, shutdown (D9)
Files: `internal/daemon/` (reload + lifecycle). Lifecycle per spec §4.3:
errgroup.WithCancel reader/sender groups, runCtx from
`signal.NotifyContext`; on SIGTERM: cancel readers → readerGroup.Wait() →
sender cancel with grace timer `max(max_send_interval)` → senderGroup.
Wait() → save state → exit 0. **The grace-flush window is a sanctioned
deviation (D9): Python's sender threads are daemon threads
(`senders/base.py:117`) and `shutdown()` (`journalpump.py:972-981`) loses
buffered messages; the user chose to keep the flush — record it in NEWS
(Task 28), do not "fix" it back to Python's behavior.** SIGHUP channel
event + ctime check; whole-blob `readers` diff (parsed-structure compare
from Task 3) with reader stop/recreate; stale reader cleanup; resume from
state file with ordered last-sender-wins / any-missing⇒replay semantics;
`save_state` every 10 s + on shutdown; log-to-journald via slog datagram
writer when stdout is not a tty; statsd/tags/geoip/filters rebuilt on
every reload even when readers are not.
Acceptance: cursors survive restart, SIGHUP reload, and Python↔Go
state-file handover; shutdown matches D9 (sanctioned grace-flush
deviation); no leaked goroutines on exit (errgroup wait verified under
`-race` and a leak check).
Tests: unit tests for reload diff granularity, resume-cursor selection,
shutdown flush window; `testing/synctest` bubble for the 10 s state
cadence and the shutdown grace timer (thread A9); integration with the
Task 10 harness.
Depends on: Task 16.

Review additions (2026-10-10, cross-review changes-required fix batch;
full records in charter §2):
- **Delivery-checkpoint contract**: the durable sender cursor only claims
  bytes durably on disk — the watermark never advances past a complete
  output line, and a failed flush or reload retire truncates back to the
  last complete-line boundary before state is saved. Required failure
  tests: restart after a line-splitting flush (valid JSONL, no duplicate
  or corrupt partial object), discard-truncation to the last complete
  line, failed flush leaving a clean edge.
- **Construct-before-retire safe abort** (sanctioned deviation): the
  replacement reader is built before the old one is retired; a
  construction failure keeps the old reader running, where Python tears
  the old reader down first.
- **Logging architecture** (sanctioned deviation from spec §4.5's slog
  mention): an io.Writer sink into the Printf seam with a level-aware
  journald/TTY logger — Python PRIORITY mapping, `log_level` honored at
  startup and on SIGHUP, logger-name context emitted. The io.Writer sink
  was ruled acceptable separately; the fixed bug was missing
  level-awareness.
- **Global vs reader-owned settings on reload**: readers consult a
  synchronized current pump-settings snapshot for GLOBAL settings
  (match key/value, statsd client, tags) so an unchanged readers blob
  still picks up new global match rules; only genuinely reader-owned
  settings stay captured at construction.
- Also fixed: `statePath` updated from the parsed config on reload;
  journald multi-line field values encoded with the native binary
  field form (8-byte little-endian length prefix); the readerErr channel
  buffered with at least one slot so the first reader failure is
  delivered even from a zero-reader start.

Round-2 additions (2026-10-10, second changes-required verdict):
- The file-sender byte counters are RUN-relative, so an absolute truncate
  is initialAppendOffset + run-relative boundary: reopening an existing
  output and retiring before any new flush preserves every previously
  delivered line (previously the run-relative boundary was applied as an
  absolute offset and could delete historical lines).
- A reload whose reader retirement fails to restore a complete-line
  boundary is refused before starting the replacement reader (propagated
  CloseDiscarding errors), so no replacement writer appends onto a
  corrupt prefix.
- The statsd client split is now correct in BOTH directions: sender
  init + secret-filter metrics use the reader's CAPTURED client; the
  periodic persistent-gauge refresh uses the LIVE pump client.
- Every Python log.error/log.warning/log.exception call site routes
  through the severity methods (sender maintenance/send failures, broad
  handler, running-config/state-save failures), not Printf.
- Per-component logger context (`JournalReader:<name>`,
  `LogSender:<reader>`, `JournalObjectHandler`) and the multi-thread
  journal format (LOG_FORMAT_JOURNAL_MULTI_THREAD, since JournalPump is
  constructed multi_threaded=True); the thread-name VALUE is adjudicated
  in the parity doc (Go emits "MainThread").

Round-3 additions (2026-10-10, third changes-required verdict — closing):
- The failed retirement is a PERSISTENT cleanup-failure state: the failed
  sender entries stay registered so a later changed-ctime reload retries
  the cleanup and still refuses (previously the entries were removed
  before cleanup succeeded, so the next reload found nothing to retire
  and started a replacement writer on the unrepaired partial line). A
  successful retry releases the entries and lets the reload proceed; the
  file sender keeps its handle open across a failed discard so the retry
  can truncate.
- The stats-split parity test exercises BOTH captured-client paths AFTER
  the statsd-only reload (lazy sender init + secret-filter matches on the
  old endpoint; live gauge refresh on the new one).
- `CorruptedEntry` (the reader's "Bad message" arm) routes through
  Warningf, so it survives a WARNING threshold instead of emitting at
  INFO and vanishing.

Round-4 additions (2026-10-10, fourth changes-required verdict):
- The file sender records a pending boundary repair explicitly. A failed
  discard leaves the partial line on disk; Finalize (SIGTERM/shutdown)
  and Close now retry the truncation, and Close refuses to release an
  unrepaired handle. This closes the shutdown-with-no-following-reload
  hole where an empty buffer let Finalize flush nothing and Close release
  a corrupt handle.
- The repair tests write real data (a complete line plus a partial next
  line), inject the truncation failure, and assert the retry/Finalize
  removes exactly the partial bytes, preserves complete lines, and allows
  a valid-JSONL replay.

### Task 18. Core systest suites (Milestone A)
Files: `systest-go/`. Rotation suite as a **Go-native test** (thread C3:
`systest/test_rotation.py` is in-process/monkeypatched in Python — port
the behavior, not the mechanism): 30 messages across rotation, buffer-
limit batching boundaries, corrupt-entry skip, lsof fd release — driven
against the binary via the Task 10 harness with the `.journal` fixtures.
Sigterm suite as a **Go-native-only test** (thread C3: the sitecustomize
trick is Python-only — replace with an injected slow-flush sender or
timing control): exit 0 mid-iteration with the grace flush observed. It
has NO Python-side counterpart run, because it asserts the grace-flush
window Python does not exhibit (sanctioned deviation D9); record it as
`go-native` with that reason in the parity checklist (outside the ~127
Python parity set — Task 29 counts it separately). Running-config and
reload suites already run dual-implementation from Task 10. State-file
interop suite (D3, Task 8 fixtures) asserting the full Python→Go→Python
round trip.
Acceptance: `make systest-go` green; **Milestone A**: journal→file with
cursors surviving restart/reload/Python-state handover.
Depends on: Tasks 17, 10, 11.

Evidence note (review round, 2026-10-10): the Python suite count is NOT
Go-binary evidence wholesale. Of the Python sweep, only 9 cases spawn the
Go binary (test_reload 1, test_running_config 7, test_sigterm 1); the 6
test_harness cases use a stub binary and the 10 test_rotation cases run
Python journalpump in-process and ignore `JOURNALPUMP_BIN`. The Go-native
`make systest-go` run (20 top-level tests, `-race`) is the Go delivery
evidence. See `docs/go-test-parity.md` Task 18.

## Phase 2 — Syslog + logplex

### Task 19. Syslog framing package
Files: `internal/syslogframing/`. RFC5424/RFC3164/custom `logline`
templates (`%token%`, unknown→`-`, auto newline), RFC6587 octet-counted vs
non-transparent framing, truncation rules (`max_message_size` 2048),
`escape_newlines`, PRI rules (facility*8+severity, out-of-range→13,
list-FACILITY guard, list-PRIORITY raises), timestamp handling (26-char+Z,
fallback UTC, RFC3164 date from first 19 chars), app-name chain, NILVALUE,
utf-8 `replace` encode, structured_data, TCP + TLS client (SNI, IPv4/6,
one send-retry on EPIPE/ECONNRESET/ETIMEDOUT/SSLEOF). Where the shared
loop injects a clock, keep the `func() time.Time` seam for the socket
deadline paths synctest can't cover.
Acceptance: byte-identical wire output to `journalpump/rsyslog.py`.
Tests: port all 17 framing tests byte-exact (`test/test_rsyslog.py`,
stub TCP server per thread B custom-fake rule — no maintained wire fake
exists for this); fuzz the framer; update parity checklist.

### Task 20. Rsyslog sender
Files: `internal/sender/rsyslog/`. Config keys (spec §5), init loop
(backoff + 5 s extra sleep), `PRIORITY` list → re-init.
Tests: parity ports of `test/unit/senders/test_rsyslog_sender.py` (6 test
functions, no parametrization: timestamp fallback, warning emission,
PRIORITY→severity mapping); live rsyslogd systest — driver ported to
process-spawn + journald injection (thread C4), wire assertions
unchanged: 5 parametrized cases (priorities, multiline octet-counted vs
escape_newlines, exact logline endings, oversize truncation without
stream desync — parity: `systest/test_rsyslog.py`).
**Milestone B**: journal→rsyslogd byte parity.
Depends on: Tasks 19, 13.

### Task 21. Logplex sender (D6)
Files: `internal/sender/logplex/`. Fire-and-forget POST, PRI 190 frame,
format-time timestamp, hostname/pid always literal `"localhost"`
(underscore-strip quirk — named quirk-lock test), `verify=False`, 2 s
timeout.
Tests: httptest asserting headers and the repr-stripped frame LIST
**vs the Task 11 logplex fixture** — the fixture's bytes-repr wire
framing is Python ground truth only (sanctioned deviation, charter
§2/D6: Go sends the clean RFC frame, so body BYTES are not compared,
content is) — and that a 500 response does NOT trigger retry or
disconnect (parity quirk lock; custom fake per thread B — no
maintained wire fake exists).
Depends on: Tasks 13, 11.

## Phase 3 — Kafka, ES/OpenSearch, websocket

### Task 22. Kafka sender (franz-go, D4, D5 — client revised 2026-10-11)
Files: `internal/sender/kafka/`. Config mapping for
PLAINTEXT/SSL/SASL_PLAINTEXT/SASL_SSL (`ca` for SSL/SASL_SSL,
certfile/keyfile SSL), SASL PLAIN/SCRAM-SHA-256/512 (franz-go in-tree
`pkg/sasl/plain`, `pkg/sasl/scram`), socks5/socks5h dialer via
`kgo.Dialer` wrapping the existing `socks5.go`, `kafka_api_version`
(unset = auto via franz-go's per-connection ApiVersions negotiation; set
= `kgo.MaxVersions(kversion.V<x>())`; values below 0.10.0 rejected at
load — spec §6.1(c)), producer settings (linger 500 ms via
`ProducerLinger`; `ProduceRequestTimeout` 30 s, `RecordDeliveryTimeout`
120 s, `RecordRetries` 20; `RetryBackoffFn` pinned to a fixed 100 ms so
the documented bound holds; reconnect backoff 1 s/10 s in our loop), fixed
`kafka_msg_key`, our murmur2 `partitioner.go` kept behind
`kgo.RecordPartitioner` (Python nil-key placement is random; franz-go's
default is sticky — Task 12 run 2 asserts partition numbers), topic
auto-create via `kadm.CreateTopic(ctx, ...)` only when both
`kafka_topic_config` values present (`kerr.TopicAlreadyExists`
tolerated). **Send path uses `kgo.Client.ProduceSync(ctx, records...)`**
— one call per batch, `ProduceResults.FirstErr()` fails the whole batch,
ack-then-advance; acks=1 is `RequiredAcks(LeaderAck())` paired with the
mandatory `DisableIdempotentWrite()` (thread A7: do not reimplement
kafka-python's produce-per-message/future/flush dance; NEWS-note the
timeout mapping if user-visible). **Compression:
`ProducerBatchCompression(SnappyCompression())`** (thread A8 — raw snappy
block, not a manual github.com/golang/snappy dependency; that module is
websocket-only). Retriable error → teardown + re-init loop, classified
via `kerr.Error.Retriable` plus a short override list; silenced client
logging. **Cancellation**: on `ctx.Done()` during a stalled produce the
sender calls `cl.Close()`, which fails buffered records with
`ErrClientClosed` so `ProduceSync` returns within one
`ProduceRequestTimeout` (typically sub-second) — a documented library
path, not a socket closed under the library.
Acceptance: config validation outcomes identical to Python (except the
§6.1(c) version floor); messages land on a real broker matching the Task
12 ground truth (payload bytes, key/partition, auto-create);
kafka-python-consumer-vs-Go-producer interop test on a real broker proves
the snappy framing story or yields a NEWS-recorded deviation (spec risk
10); ctx cancel during a stalled produce returns within one request
timeout; `Close()` returns promptly.
Tests: parity ports of the ~20 config tests
(`test/unit/senders/test_kafka_sender.py`); unit tests against a narrow
`producer` interface fake (connect / produce batch / create topic / close
— the `loop_test.go` `recordingProducer` pattern) and franz-go's in-tree
`pkg/kfake` in-process cluster for the stall/cancel suite (thread B:
franz-go ships no SyncProducer-style mock; `sarama/mocks` retired);
`partitioner_test.go`, `socks5_test.go`, config tests and
`kafka_e2e_test.go` carry over; systest against a real broker asserting
the Task 12 fixtures (both runs) with the kafka-python consumer re-run;
update parity checklist.
Depends on: Tasks 13, 14, 12.

Client revision (user decision 2026-10-11; rationale in
`docs/assess-kafka-client.md`, sarama limitation in
`docs/assess-sender-cancellation.md`; the builder records the §2 charter
deviation — not restated here):
- Rounds 1–4 of this task were delivered on IBM/sarama (ledger row 22).
  Sarama has no context support, so cancellation was a socket-force-close
  workaround with a ~2-3 s floor. The franz-go rewrite **replaces the
  sarama implementation behind the unchanged `SendMessages(ctx)` / Loop
  contract** (spec §4.2, Task 13) — no change to the shared loop, the
  wiring map, config parsing/validation, `topicConfig`,
  `normalizeKafkaVersion`, the TLS builder, `socks5.go`, or
  `partitioner.go` (interface rename only).
- Sub-steps, each one builder dispatch, in order: (a) throwaway spike —
  `ProduceSync` with `DisableIdempotentWrite` + `LeaderAck` against the
  systest broker; confirm ctx cancel during a stalled produce returns
  within one `ProduceRequestTimeout`, `Close()` returns promptly, and the
  kafka-python consumer reads the records (go/no-go gate, not merged);
  (b) introduce the narrow `producer` interface + fake and port
  `loop_test.go`'s `recordingProducer`; (c) replace
  `buildSaramaConfig`/connect/`sendBatch`/`maybeCreateTopic` with the
  kgo/kadm implementation and wire socks5, partitioner, SASL,
  `kafka_api_version`; (d) delete `trackingDialer`, `runCancellable`,
  version inference + legacy probe, `scram.go`, the KError table; replace
  classification with the `kerr` retriable flag; drop the sarama, xdg-go/
  scram, eapache/*, gokrb5, go-metrics, go-resiliency module deps;
  (e) rewrite the round2/round3 cancellation tests against kfake or the
  interface fake — keep the ≤5 s assertion and add the tighter
  one-request-timeout bound; (f) parity re-verification: re-run
  `kafka_e2e_test.go` against `fixtures/kafka.json` (both runs) with the
  kafka-python consumer; update `docs/go-test-parity.md` (retire the
  "Sarama limitation" subsection, record the new shutdown bound and the
  deviations below) and the charter ledger row 22 via builder-only docs
  commits.
- **Carries over unchanged**: the bounded supervisor join from round 4
  (`pump.go`, defence in depth independent of client choice — keep it);
  the Task 12 fixture and the dual-run parity assertion (payload bytes,
  key, partition number, contiguous offsets, partition count, sent
  cursor).
- Deviations the builder records (charter §2 / parity doc): pre-0.10
  broker version inference dropped (spec §6.1(c), operator-visible —
  user-approved); retry backoff pinned to fixed 100 ms instead of
  franz-go's default jittered exponential (cosmetic); shutdown latency
  bound improves from "~2-3 s floor, ≤5 s asserted" to "≤ one request
  timeout, typically sub-second".

### Task 23. Elasticsearch/OpenSearch sender
Files: `internal/sender/elasticsearch/`. Raw HTTP; version probe;
`_aliases`; index create with `?include_type_name=true` + `_type` for ES≤7;
mapping-from-first-message (bool/float/int/str/dict recursion,
`SYSTEMD_SESSION`/`SESSION_ID`, unmappable skipped); daily rollover
(`prefix-YYYY-MM-DD` from first 10 chars of timestamp, missing→UTC now +
warning); `_bulk` NDJSON (success = 200/201 only, item errors ignored,
already-exists tolerated — quirk-lock test); hourly retention
`maintenance_operations`; error-class dedup logging; timeouts (session,
bulk = 2×).
Acceptance: HTTP request/response sequences identical to the Task 11 ES
fixtures.
Tests: httptest suite porting the 21 parametrized cases
(`test/test_elasticsearch_opensearch_sender.py` + ES cases in
`test/test_journalpump.py`) — custom fakes here are sanctioned by thread
B only because rollover/mapping assertions exceed what official Elastic
tooling covers; fakes assert wire bytes; update parity checklist.
Depends on: Tasks 13, 11.

#### Shared HTTP helper (coordinator decision 2026-10-11)
The raw-HTTP senders — logplex (Task 21), elasticsearch/opensearch
(Task 23) and Google Cloud Logging (Task 26) — share ONE internal HTTP
client helper instead of each carrying its own client code.
- **Why**: logplex and ES landed with independent HTTP plumbing and
  diverged on proxy selection — logplex hand-ports `requests`' proxy
  semantics, ES uses Go's default `http.ProxyFromEnvironment`. That is an
  operator-visible difference in proxy routing, not a cosmetic one, so it
  is fixed by sharing rather than by a third copy at Task 26.
- **Shape**: the helper owns the common plumbing — `http.Client`
  construction (TLS/`verify` options, transport), per-request context
  deadline and cancellation, and retry hooks the sender's `retriable`
  classifier plugs into. **Proxy SELECTION is a per-sender parameter**,
  not a helper default: each sender passes the proxy policy that matches
  its Python HTTP client — `requests` semantics for logplex (incl. its
  env-var and `no_proxy` handling), the SAME `requests` semantics for ES
  (verified: ES goes through `journalpump.util.get_requests_session` →
  `requests.Session`, NOT elasticsearch-py/urllib3), and the google-auth
  transport's behaviour for GCL at Task 26. A quirk-lock test per sender
  pins its proxy choice against a proxy-env fixture.
- **When**: lands as part of CLOSING Task 23 — one builder dispatch
  extracts the helper and refactors logplex + elasticsearch onto it with
  no wire-byte change (the Task 11 logplex/ES fixtures and existing
  httptest suites are the regression gate; the parity doc records the
  ES proxy-selection fix as a bug fix toward Python, not a deviation).
  Task 26 then builds on the helper rather than on a fresh client. No
  task renumbering; cross-review of the refactor diff as usual.

### Task 24. Websocket sender
Files: `internal/sender/websocket/`. Single goroutine replacing the
asyncio-in-thread design; app-level snappy (github.com/golang/snappy) +
permessage-deflate options; batch NUL-join with `Journalpump-Batch-Size`
= configured max; batch slicing at the LogSender base defaults (30 B
overhead, 1 MiB limit — see the correction note below); headers
(`User-Agent: journalpump/{version}`, `Journalpump-Compression`); TLS
triple ca/keyfile/certfile; SOCKS5; inbound drain consumer; reconnect
backoff 20 s×1.8→90 s reset after 60 s; 2 MiB inbound cap; close timeout
20 s; graceful close.
Acceptance: frames byte-match the Task 11 websocket fixture (NUL-joined
batch, snappy bytes, headers).
Tests: in-process Go websocket server (httptest, per thread B) — non-
batched and snappy-compressed batched delivery (parity:
`test/test_websocket.py` 2 tests, extended to cover header values and the
slicing overhead/slicing-limit independence); `testing/synctest` for
reconnect-backoff timing; update parity checklist.

CORRECTION (2026-10-12): an earlier draft of this task said websocket
overrides the shared batch overhead to 1 byte, citing
`websocket.py:73,168-169`. That is wrong. `websocket.py:73` sets
`self.batch_message_overhead = 1` on the `WebsocketRunner` Thread, an
object that never slices and whose attribute is never read; the
`WebsocketSender` LogSender that slices keeps the base defaults set at
`base.py:130-131` (overhead 30, limit 1 MiB) and slices with
`self.batch_message_overhead` / `self.max_batch_size` at `base.py:262,266`.
Runtime probe (instantiate `WebsocketSender` with `max_batch_size=2048`)
reports slicing overhead 30 and slicing max_batch_size 1048576. The
configured `max_batch_size` therefore affects only batching (the NUL-join)
and the `Journalpump-Batch-Size` header, never the slicing limit. Go
implements 30 / 1 MiB accordingly.
Depends on: Tasks 13, 11.

## Phase 4 — CloudWatch + Google Cloud Logging

### Task 25. CloudWatch sender
Files: `internal/sender/cloudwatch/` (aws-sdk-go-v2 behind a small client
interface). Init: 3 create attempts ignoring already-exists, paginator for
sequence token, AccessDenied→backoff→init failure; send with
first-send-no-token then `nextSequenceToken`; timestamp ms; batch guard
≤1 MB and ≤10k events.
Tests: **aws-sdk-go-v2 ships no generated mocks**, so per thread B this
is a sanctioned custom-fake case: a small client-interface seam with
hand-written or mockgen-generated fakes, porting the Stubber-based cases
inside `test/test_journalpump.py:1263-1520` (`test_awscloudwatch_sender`,
`_sender_init`, `_sender_init_paginate`) — throttle/denied/token flows —
recorded as **ported** in the parity checklist with the corrected
citation; fakes assert request wire shape, not call shape; optional
localstack integration (excluded from default CI); update parity
checklist.
Depends on: Task 13.

### Task 26. Google Cloud Logging sender
Files: `internal/sender/gcl/` (logging/v2 with injectable HTTP transport;
the transport is built on the shared HTTP helper from Task 23's closing
block, with GCL's own proxy-selection parameter).
Quota logic (250 KiB / 200 KiB truncation, replacement entries), severity
map, timestamp 26-char+Z, MESSAGE JSON-parse→jsonPayload, `generic_node`,
logName, optimistic reconnect.
Tests: official Google Cloud Logging fakes first (thread B), httptest
against the injected transport where they fall short, porting
`test/test_google_cloud_logging.py` cases (dummy SA key fixture reused
from `test/data.py`); update parity checklist.
Depends on: Task 13, Task 23 (shared HTTP helper).

## Phase 4b — Performance and resource baseline (inserted 2026-10-12)

Ordering note: this phase runs AFTER the sender phases (so the baseline is
taken on the feature-complete binary) and BEFORE Phase 5 (so Task 28's
NEWS can cite measured numbers and Task 30 can wire the optional CI job).
The task is numbered 31 — the plan's numbering is strictly sequential and
ordering is carried by the "Dependency order and milestones" section, the
same convention the Task 23 shared-HTTP-helper block used ("no task
renumbering"). Tasks 27–30 keep their numbers. Task 32 (streaming
pipeline, added 2026-10-13) joins this phase under the same convention:
it consumes the Task 31 baseline and precedes Phase 5.

Rationale (audit, 2026-10-12): the repo has NO performance or resource
comparison of any kind — no Go benchmarks, no pytest-benchmark, no
throughput/CPU/RSS assertion, no perf task in this plan, no CI perf job.
Every existing test is correctness/parity. The migration's value claim
includes speed and footprint, so the user directed that it be measured,
not assumed.

Revision notes (2026-10-12, three cross-review rounds): round 1 removed
the Python-only storage-probe shim from the fixture, the gauge-based
timing, and the "bench-check must pass" contradiction. Round 2 simplified
the method itself: the first drafts tried to isolate "when processing
finished" with file-growth heuristics and mid-run windows, and every
remaining finding came from that. The PRIMARY comparison is now the WHOLE
RUN (spawn → clean exit) and steady-state figures are optional,
clearly-labelled secondaries that never gate anything. Round 3 corrected
the framing of that whole-run number: the SIGTERM trigger is observed
through a 10 s reporting throttle, so wall is an INSTRUMENTED measurement
carrying up to one interval of observer-induced wait that is NOT equal
between the two runs; verdicts must respect that uncertainty band. It
also fixed the baseline invocation (`make bench-baseline`, not an
unsupported `make` option).

### Task 31. Go-vs-Python performance and resource benchmark harness + baseline
Files: `bench/journalpump_bench.py` (driver), `bench/gen_journal.py`
(fixture generator), `bench/report.py` (table + ratios + comparator +
canonical-config hashing), `bench/config.json.tmpl` (the ONE config both
binaries run), `bench/results/baseline-<YYYY-MM-DD>-<host>.json`
(committed baseline), `docs/perf-baseline.md` (rendered table +
environment block), `Makefile` targets `bench`, `bench-go`,
`bench-python`, `bench-baseline`, `bench-check`, `bench-fixture`,
`bench-test`; a small reviewed extension of
`systest/journalpump_process.py` (see "Process ownership").
- **Fixture — must open in BOTH unmodified production binaries**. Both
  implementations refuse to start unless libsystemd reports
  `sd_journal_has_persistent_files || sd_journal_has_runtime_files`
  (`journalpump.py:432`; `internal/reader/sdjournal.go:184-194`), and
  libsystemd derives those flags from the path prefix of the files it
  opened (`/var/...` → persistent, `/run/...` → runtime). The Task 11
  fixtures dodge this with a Python-only `sitecustomize` shim and a Go
  `-tags systest` override (`files_available_systest.go`) — NEITHER is
  admissible here: the bench runs the production `make build-go` binary
  and an unshimmed `python3 -m journalpump`. The generated journal
  therefore lives under a probe-accepted prefix: default
  `BENCH_JOURNAL_DIR=/run/user/$UID/journalpump-bench/<run-id>/` (tmpfs,
  user-writable, runtime flag), fallback
  `/var/tmp/journalpump-bench/<run-id>/` (persistent flag), selected via
  `journal_path` in the shared config. The prefix rule is confirmed
  empirically by a **preflight**: the driver starts each production
  binary unshimmed against the fixture, requires `READY=1` and non-zero
  growth of the output file within 10 s, and aborts the whole run with a
  diagnostic if either implementation rejects the layout.
  `gen_journal.py` emits a deterministic journal export stream (seeded
  PRNG; `MESSAGE` 80–400 B, `_SYSTEMD_UNIT`, `PRIORITY`, `_PID`,
  `SYSLOG_IDENTIFIER`, a handful of custom fields, every 1000th entry
  invalid-UTF-8 to exercise the D8 repr path) and builds `.journal`
  files with `systemd-journal-remote -o` (root-free). Default
  `BENCH_ENTRIES=200000`; the journal is not committed (size), the
  generator + seed are, and every result records the export stream's
  SHA-256. No live-journald generation, no shim fallback of any kind.
- **Workload and fairness**: identical `journalpump.json` rendered from
  the template for both — one reader (`journal_path` → fixture dir,
  `initial_position: head`), one **file sender** with `file_output` on
  tmpfs (`/dev/shm` or `$BENCH_TMPFS`), `statsd` pointed at the bench's
  own UDP listener, `log_level: WARNING`. The file sender is the lightest
  sink in both implementations, so the number measures reader +
  filtering + JSON serialization + the shared flush loop — the parts the
  port changed — not a sink. Its buffering is recorded in the report, not
  altered: neither implementation fsyncs; Python's `open(..., "ab")`
  default `BufferedWriter`, Go's 128 KiB `bufio.Writer`
  (`file.go:33,329`); both flush on buffer fill and at shutdown. Go
  binary = production `make build-go` output, NOT the `-race -tags
  systest` systest build; no `JOURNALPUMP_POLL_INTERVAL_MS` /
  `JOURNALPUMP_FILES_AVAILABLE` shims (the driver asserts they are
  unset). Same env, same cgroup, cold start each run; the driver records
  the CPU governor and refuses to run unless it is `performance` or
  `BENCH_ALLOW_ANY_GOVERNOR=1`.
- **Process ownership and rusage**: `systest/journalpump_process.py`
  reaps its child via `Popen.poll()/wait()`, after which `os.wait4` can
  no longer recover rusage. The bench reuses ONLY the harness's command
  selection and environment construction (`JOURNALPUMP_BIN` → argv;
  `RUNTIME_DIRECTORY`, abstract `NOTIFY_SOCKET` listener,
  `PYTHONUNBUFFERED`) and OWNS spawning and the single reap: one
  `os.wait4(pid, 0)` call, never `poll()`/`wait()`/`communicate()` on
  that pid. A reviewed harness extension is authorized: factor the
  argv/env/notify-listener construction out of `JournalpumpProcess.start`
  into importable helpers with no behavior change for the systests
  (their suites are the regression gate). `python3 -m journalpump` is the
  direct child in both cases, so rusage covers the whole implementation.
- **Run protocol — the whole run is the unit of measurement**. Per run:
  `t_spawn` → wait for `READY=1` (`t_ready`) → wait until the
  implementation's own terminal gauge `journal.sent_lines`, received on
  the bench's statsd listener, equals `BENCH_ENTRIES` (`t_confirm`;
  this is a CORRECTNESS confirmation that everything was processed, and
  the SIGTERM trigger — it is not a timing boundary) → SIGTERM
  (`t_sigterm`) → `wait4` returns (`t_exit`, rusage). Wall deliberately
  INCLUDES shutdown: Go's D9 grace-flush and final file flush, Python's
  daemon-thread exit — that is the operator-visible cost of a complete
  run. Post-exit correctness (a FAILED run, never a silently reported
  one): exit code 0; output line count == `BENCH_ENTRIES` for Go, and
  for Python ≥ `BENCH_ENTRIES` minus one buffer's worth of entries
  (Python drops its unflushed tail on SIGTERM, D9) — the exact Python
  count is recorded. Hard timeout `BENCH_TIMEOUT` (default 600 s) → run
  FAILED, not hung.
- **Measurement uncertainty — stated, not assumed away**. The gauge that
  triggers SIGTERM is emitted through a 10 s per-reader throttle in both
  implementations (`inc_line_stats`), so SIGTERM lands anywhere from ~0 s
  to ~one reporting interval after processing actually finished,
  depending on where the finish falls in the throttle phase. That wait is
  OBSERVER-INDUCED, it is part of the measured wall, and it is NOT equal
  between the two implementations or between two runs of the same one;
  `t_confirm − t_ready` is recorded but does not isolate it, and the
  phase effect need not show up in min/max ranges. Consequently wall and
  every figure derived from it (entries/s, MB/s) are labelled
  **INSTRUMENTED whole-run measurements** in the table and the JSON, with
  the uncertainty bound printed beside them: ≈ one reporting interval
  (10 s), which at the baseline's 60 s minimum wall is ≤ 1/6 of wall.
  CPU and peak RSS are NOT subject to this bound in the same way (an
  idle wait burns negligible CPU and does not raise peak RSS); they carry
  only the min/max noise band.
- **PRIMARY metrics (per run, from the driver, no code change to either
  implementation)**: total wall = `t_exit − t_spawn` (instrumented, see
  above); startup = `t_ready − t_spawn` (reported, informational); total
  CPU user+sys and peak RSS (`ru_maxrss`) from `wait4` rusage over the
  whole lifetime; end-to-end throughput = `BENCH_ENTRIES / wall`
  (entries/s) and output bytes / wall (MB/s) (instrumented). These need
  no completion heuristic and directly answer "is Go faster and lighter
  than Python on the same work" within the stated uncertainty. Runs: 1
  warm-up (discarded) then `BENCH_RUNS=5` measured per implementation;
  report median plus min/max per metric.
- **SECONDARY metrics (optional, labelled, never gating)**: the driver
  samples output-file size and `/proc/<pid>/status` `VmRSS` every 100 ms
  for the run's lifetime. From the OBSERVED growth samples it reports
  steady-state throughput = least-squares slope over the middle 50 % of
  growth samples (between first and last observed growth), and
  steady-state RSS = median `VmRSS` over the same sample range. If fewer
  than 20 growth samples exist (short runs, smoke test) both are reported
  as `unavailable` with the sample count — never a failure, never a
  comparison input. Their caveat (file grows only on buffer fill, so the
  slope is quantised by the 128 KiB / `BufferedWriter` flush size) is
  printed with them. `bench-check` ignores them entirely.
- **Ratio direction (explicit)**: every ratio is `Go / Python` of
  medians. For wall, startup, CPU and RSS a ratio < 1 means Go is better;
  for entries/s and MB/s a ratio > 1 means Go is better. The table carries
  a per-row "better when" column (`<1` / `>1`) and a derived
  "improvement" column computed with the correct direction, so no reader
  has to invert. `bench/report.py` tests cover both directions,
  ratio = 1, and a Go-worse case.
- **Two separate comparisons, two separate outcomes**:
  (1) **Go-vs-Python result** — always REPORTED (table + ratios +
  per-metric verdict), NEVER a failure. The verdict for each metric is
  one of "Go better" / "Python better" / "within measurement uncertainty
  — no clear difference", and it MUST account for the uncertainty above:
  for the instrumented metrics (wall, entries/s, MB/s) a difference of
  medians is only called a win when it exceeds the reporting-interval
  bound (|Go wall − Python wall| > one interval, and correspondingly for
  the derived rates) AND the two min/max ranges do not overlap; for CPU
  and RSS the non-overlap test alone applies. Anything else is reported
  as "within measurement uncertainty — no clear difference", never as a
  Go or Python win. If Go loses on any metric the task still completes;
  the finding goes in the report and the charter ledger row for a user
  decision. (2) **Baseline regression tripwire** — Go-current vs
  Go-baseline, PRIMARY metrics only; exits non-zero ONLY when a Go
  primary metric regresses by more than 50 % in the worse direction
  relative to the baseline median (one threshold constant in
  `bench/report.py`; at the 60 s minimum wall the 10 s bound is well
  inside that margin), and only when the two results are **compatible**.
  Compatibility key = (host label, fixture SHA-256, `BENCH_ENTRIES`,
  **canonical config hash**, CPU model + online count, cgroup `cpu.max`
  and `memory.max` of the bench's own cgroup, governor, kernel release).
  Any mismatch downgrades the comparison to INFORMATIONAL with the
  differing keys listed — never a failure.
- **Canonical config hash**: the hash is NOT of the rendered file (which
  carries run-local paths and ports) but of a canonical form produced by
  `bench/report.py`: parse the rendered config, replace `journal_path`,
  `file_output`, the statsd `host`/`port`, and any `RUNTIME_DIRECTORY`-
  derived path with fixed placeholders, keep every behavior-affecting
  key (filters, `max_send_interval`, `msg_buffer_max_length`,
  `initial_position`, `log_level`, …), then `json.dumps(sort_keys=True,
  separators=(",", ":"))` → SHA-256. Two independently rendered runs with
  different run-ids/ports MUST hash equal; changing any behavior key MUST
  change the hash — both are unit-tested, so `bench-check`'s
  self-comparison against the committed baseline can actually succeed.
- **Baseline**: `make bench-baseline` (a dedicated target that invokes
  the driver with `--baseline`; `make bench` never does) writes
  `bench/results/baseline-<date>-<host>.json` plus `docs/perf-baseline.md`
  and refuses unless `BENCH_ENTRIES ≥ 200000` (constant `BASELINE_MIN_
  ENTRIES`) AND the measured Go median wall is ≥ 60 s (constant
  `BASELINE_MIN_WALL_S`), so the 10 s reporting-interval bound is ≤ 1/6
  of wall and the secondaries are available; the result records the full
  compatibility key plus Go/Python/python-systemd/systemd versions and
  `git describe`.
- **Non-goals**: not a correctness gate; not a PR blocker; `bench-check`
  is not in the required CI set (Task 30 may add it as an informational
  `workflow_dispatch`/nightly job); no `testing.B` micro-benchmarks (a
  later task may add them where this harness points at a hot spot); no
  tuning work; no change to either implementation; no attempt to
  isolate "processing finished" more finely than the whole run, and no
  attempt to subtract the observation wait from wall.
Acceptance: `make bench-baseline` on the defined machine produces
`docs/perf-baseline.md` with the Go-vs-Python PRIMARY table (total wall,
startup, CPU user+sys, peak RSS, entries/s, MB/s, output bytes, exit
code, `t_confirm − t_ready`) in which wall and the derived rates are
labelled INSTRUMENTED with the ≈ one-interval uncertainty bound printed
beside them, correctly directed ratios, and a per-metric verdict that
follows the uncertainty rule above; the SECONDARY table (steady-state
entries/s, steady-state RSS, or `unavailable` with sample count) under
its caveat; and the environment/compatibility block filled; the baseline
JSON is committed; `make bench-check` re-run against the committed
baseline on the same host reports a COMPATIBLE comparison with no
regression (tripwire self-consistency) and reports the Go-vs-Python
outcome whatever it is — a Go loss or a "no clear difference" is a
recorded result, not an acceptance failure; `BENCH_ENTRIES=5000
BENCH_RUNS=1 BENCH_WARMUP=0 make bench` completes without timeout (no
fixed wall-clock acceptance number).
Tests (all collected by the new `make bench-test` target, which runs
`make build-go` first so the production binary exists, then `python3 -m
pytest bench/ systest/test_bench_smoke.py`; neither `make unittest`
(targets `test/`) nor `make systest` collects `bench/`, and no perf
threshold is asserted anywhere in this target):
`systest/test_bench_smoke.py` — runs the driver with `BENCH_ENTRIES=2000`,
`BENCH_RUNS=1`, `BENCH_WARMUP=0` against BOTH binaries (`JOURNALPUMP_BIN`
parametrized like the other dual-implementation suites; the preflight is
exercised as part of it) and asserts per-field validity — exit code 0,
timestamps monotone `t_spawn ≤ t_ready ≤ t_confirm ≤ t_sigterm ≤ t_exit`
(lifecycle ordering via timestamps, not "one phase outlasts another"),
durations finite and ≥ 0, entries/s and MB/s > 0, peak RSS > 0, CPU ≥ 0,
output line-count rule above, the instrumented label and uncertainty
bound present on wall/rates, secondaries either numeric or `unavailable`
with a sample count; NO threshold on any timing. `bench/test_report.py` —
median/min/max, ratio arithmetic in both directions, per-metric verdict
including the uncertainty rule (a wall difference smaller than one
interval → "within measurement uncertainty" even with non-overlapping
ranges; a difference larger than one interval with overlapping ranges →
also "within measurement uncertainty"; both conditions met → a win in the
correct direction; CPU/RSS verdicts use the non-overlap test only),
comparator on regressed / improved / compatible-no-change /
incompatible-informational inputs, secondaries ignored by the comparator,
environment-mismatch listing, canonical config hash (equal across two
renders with different run-local values; changed by a behavior key;
placeholder set covers every run-local key the template emits).
`bench/test_gen_journal.py` — deterministic for a seed (SHA-256 stable
across two generations), exactly N entries, the invalid-UTF-8 cadence
present.
Parity checklist: `docs/go-test-parity.md` gains a `## Task 31` section
recording all three as `go-native` (reason: performance harness, no
Python counterpart; outside the ~127 Python parity set Task 29 counts).
Charter ledger row 31 via the builder-only docs commit as usual.
Depends on: Task 18 (Milestone A — working Go binary, file sender,
`make build-go`), Task 10 (`JOURNALPUMP_BIN` selection + notify listener
helpers), Task 13 (`journal.sent_lines` gauge from the shared sender
loop); ordered after Tasks 25/26 so the baseline is taken on the
feature-complete binary.

### Task 32. Streaming journal pipeline — idiomatic Go path (design approved 2026-10-13)
Files: `internal/reader/ffi.go` (`jp_api` function-pointer struct +
`jp_read_batch` C batch loop), `internal/reader/sdjournal.go` (`ReadRaw`,
`Batch`/`RawEntry`/`RawField`; `ReadBatch` kept as an adapter until step
(g)), `internal/journal/stream.go` (`EmitJSON`, field index,
`appendJSONString`; replaces `serialize.go`/`process.go`/`converters.go`),
`internal/journal/handler.go` (rewrite over `RawEntry`; the §2.6 global
severity pre-filter; delete `fieldMap` and the `filterByLogLevel` JSON
re-parse), `internal/filter/` (byte-slice
entry points), `internal/sender/` (`Item.IndexDate` sidecar; file sender
write-then-`\n`; ES uses the sidecar), `systest/groundtruth_support.py`
(`semantic_equal`), `internal/journal/transform_bench_test.go`,
`bench/results/`, `docs/perf-baseline.md`, `docs/perf-root-cause.md`
(ledger). Design: **`docs/design-journal-pipeline.md`** (approved; it is
the spec for this task and is not restated here). Motivation: the Task 31
baseline and `docs/perf-root-cause.md` show the Go port at ~28.7 µs
CPU/entry serial and worse than Python at default GOMAXPROCS; the design
targets ≈5-6 µs, ~4 allocs/entry.
Scope: one cgo crossing per batch (C loop fills a Go-owned buffer), a
16 B/field offset index instead of `[]Field`/maps, every filter over byte
views, JSON appended straight from raw field bytes into one exactly-sized
payload per filter key shared across senders as today. Rulings carried
from design §7 (user, 2026-10-13):
- **Ruling 1 (emulate, no deviation)**: cross-sender `extra_field_values`
  contamination is reproduced via precomputed per-sender "effective
  extras" — the union, in config order, of the fragments of earlier
  cache-miss senders plus the sender's own. Zero runtime cost. No NEWS
  entry.
- **Ruling 2 (cheap emulation, no deviation)**: on a cache hit,
  `unit_log_level` is decided from the live entry only if the emitting
  sender's field filter kept BOTH `PRIORITY` and `SYSTEMD_UNIT`, taking
  presence/value from that sender's effective entry (incl. its effective
  extras); otherwise the entry passes. Python-identical outcome, no JSON
  re-parse. No NEWS entry.
- **Ruling 3 (charter amendment)**: the charter's "byte-identical"
  serialization requirement becomes "semantically-equivalent,
  byte-identical where free". The builder records the amendment in the
  charter via its dedicated docs commit; Task 28 carries the NEWS entry.
- **Global severity pre-filter (design §2.6, behaviour-preserving, no
  ruling)**: before any emission, skip the entry entirely when EVERY
  sender's `unit_log_level` would drop it — tier A: a unit-agnostic
  floor from the senders' catch-all `*` globs (max level across senders;
  no floor if any sender lacks a `*` glob or has no `unit_log_level`),
  checked by a byte scan of the raw `PRIORITY` view (~20-50 ns, zero
  alloc); tier B: the exact `Decide` of every sender over the effective
  entry (~0.3-1 µs). Both use the ruling-1 effective entry (all-drop
  extras union; an extras override of `PRIORITY`/`SYSTEMD_UNIT` is
  folded at construction). Python rules preserved and pinned: absent,
  `0`, non-numeric or repeated `PRIORITY` passes; absent/empty
  `SYSTEMD_UNIT` passes (kernel entries never pruned); searches hit
  counters and the secret-filter metric still count pruned entries;
  `Process` returns today's all-dropped result. The journald-side
  `PRIORITY` add_match variant is ASSESSED AND REJECTED (design §2.6.2:
  libsystemd cannot express "field absent", so it would drop entries
  Python passes, plus unit/value-form/metric differences) — not
  implemented; any future opt-in is a charter §2 deviation needing a
  user ruling. The refined **two-reader partition** (a reader for
  records with both unit and `PRIORITY`, one for the complement) is
  likewise ASSESSED AND REJECTED (design §2.6.5): `sd_journal_add_match`
  has no existence/wildcard/negation form — empirically, field-only and
  `!=` are `-EINVAL`, `PRIORITY=` matches the literal empty value and
  `PRIORITY=*` matches nothing — so the complement cannot be expressed
  and the second reader could only ever be the unmatched main reader.
**Sink PROTOCOLS stay exact; message-content JSON becomes
semantically-equivalent.** Kafka framing/compression/key→partition,
logplex octet-counted frames and headers, rsyslog framing, ES `_bulk`
action lines and mapping, websocket NUL-join/snappy frames, file
`payload + "\n"`, CloudWatch/GCL request shapes are untouched and remain
byte-asserted; only the JSON object they carry is compared semantically
(same key set, equal decoded strings, numbers equal as float64 or as
decimal strings beyond 2^53, structural equality for null/arrays).
Python formatting (separators, first-occurrence key order, synthesised
keys last, `ensure_ascii=True`, `PythonFloatRepr`, repr fallback) is kept
where free, so the expectation is zero fixture regeneration.
Sub-steps, each one builder dispatch, in order (design §5.2; (a) and (b)
may run in parallel): (a) reader C batch loop + `ReadRaw` with the
`ReadBatch` adapter — existing `sdjournal_test.go` unchanged plus an
adapter-equals-`readNext` test on the systest `.journal` fixtures;
(b) `stream.go` `EmitJSON` + index — the old
`ConvertEntry/ProcessEntry/Serialize` chain becomes the ORACLE in a
semantic-equivalence test and a fuzz test (random keys/values/duplicates/
invalid UTF-8 → both paths → parse → compare), golden file kept as byte
canary; (c) filter byte-slice entry points (`Searcher.MatchRaw`,
`UnitLogLevel.Decide(unit, prio []byte)`, `FieldFilter.KeepKey`,
`SecretFilters.ApplyRaw`; a literal `*` glob compiles to a constant-true
matcher) with table tests mirroring the string ones and a pin test for
repeated-field behaviour BEFORE changing it; (d) handler rewrite over
`RawEntry` with the decision record (ruling 2), effective-extras list
(ruling 1), the §2.6 pre-filter (tier A floor + tier B exact, computed at
construction, calling the SAME `Decide` and key-resolution helper as the
per-sender path — no second implementation of the rule), `Item.IndexDate`;
`handler_test.go` ported to semantic assertions plus quirk-lock tests
with the OLD handler as oracle — (i) three senders, distinct filter keys
and extras, sender C's payload carries A's and B's extras; (ii)
first-of-key sender dropped by unit level then a same-key sender emits
with its own extras appended, and a cache-hit sender after a filter that
removed `PRIORITY` passes; (iii) the §2.6.1 preserved-rules table and a
property test over random configs/entries asserting prune ⇒ the old
handler enqueued nothing to any sender, and no prune ⇒ per-sender
outputs semantically equal; `pump_test.go` unchanged; (e) senders — file
write-then-`\n`, ES `IndexDate`; (f) tests + bench — `semantic_equal` for
the JSON body in `test_groundtruth_{file,kafka}.py` keeping raw comparison
for framing/headers/partition/key/counts, `TestSerializeGolden` becomes
the canary (`t.Logf` on byte diff, fail only on semantic diff), micro-benchmark
rewired to `EmitJSON` plus a debug-heavy pruned-path variant (`*`/`INFO`
config, `PRIORITY=7` entries — records the ~2.5-3 µs pruned figure;
the committed bench fixture has no `unit_log_level`, so the pre-filter is
inactive in the baseline and the main gate is measured without it),
`make bench-baseline` re-recorded and `bench-check` re-armed; (g) cleanup
— delete the adapter, old chain,
`enumBufPool`; `-tags poison` build fills the batch buffer after each
batch; `perf-root-cause.md` ledger closed.
Gates (all must hold before (g) closes the task):
- **Bench performance gate** (Task 31 harness, same host and fixture as
  the committed baseline, COMPATIBLE comparison): Go CPU/entry serial
  (`GOMAXPROCS=1`) ≤ 8 µs (from 28.7; design target 5-6); allocs/entry
  ≤ 8 (from 158; target ~4); default-GOMAXPROCS whole-run CPU below
  Python's with non-overlapping ranges; `bench-check` tripwire shows no
  regression on any PRIMARY metric; wall is expected unchanged (poll-
  paced) and is not gated beyond the tripwire.
- **NEW semantic-equivalence tests**: the (b) oracle + fuzz tests, the
  (d) handler semantic port and two quirk-lock tests, the (f)
  `semantic_equal` systest comparisons — all green against BOTH binaries
  where dual-implementation suites exist.
- **Byte-exact fixtures as canary**: `serialization_golden.jsonl` and
  the Task 11/12 file and kafka fixtures are retained unmodified; a byte
  diff is logged, never fails, and a semantic diff fails. Expected
  outcome: zero byte diffs (recorded in the ledger).
- **Existing suites green**: `go test -race ./...`, `make systest-go`,
  `make systest` dual-implementation suites, `make bench-test`; the
  json-cache quirk systest (`test_groundtruth_json_cache.py`), the
  replay and logplex fixtures, and `poll_interval_systest.go` (no added
  latency) pass unchanged.
- **No deviations** beyond ruling 3; rulings 1 and 2 and the §2.6
  pre-filter are behaviour-preserving and the parity doc records them as
  emulated quirks / short-circuit pinned by the (d) tests. The
  pre-filter's wrong direction (pruning an entry a sender would keep) is
  data loss; the (d) property test asserts that direction specifically.
Non-goals: pure-Go journal reader and `journalctl -o export` subprocess
(design §1.2/§1.3, fallbacks only); journald-side `PRIORITY` add_match
pre-filter (design §2.6.2, rejected — would need an operator opt-in and a
user ruling); turning `ensure_ascii` off (exists as
an option, default true, Python form); sidecars for logplex/rsyslog
(later phase); any sender wire change.
Parity checklist: `docs/go-test-parity.md` gains a `## Task 32` section
listing the oracle/fuzz/quirk-lock tests as `go-native` (reason: the
oracle is the old Go chain, no Python counterpart) and the semantic
re-expression of `TestSerializeGolden` and the file/kafka systests
(ported → semantic, with the canary note). Charter ledger row 32 and the
ruling-3 charter amendment via the builder-only docs commit as usual.
Depends on: Task 15 (reader seam, `sdjournal.go`/`ffi.go`), Task 16
(run loop, handler, file sender), Task 31 (harness, committed baseline
and `bench-check` for the performance gate); ordered after 31 and BEFORE
27 so Task 28's NEWS cites the post-pipeline numbers and the ruling-3
amendment. Numbered 32 to avoid renumbering 27–30.

## Phase 5 — Packaging, docs, cutover

### Task 27. Packaging on the branch (D1, D7)
Files: `journalpump.spec` (branch version: arch-specific, BuildRequires
golang, builds the Go binary, `Release` gains a golang variation — e.g.
`%{minor_version}.golang%{?dist}` — and `BuildArch` drops `noarch`),
`debian/control` + `debian/rules` (golang build, arch-specific,
`adduser` postinst unchanged), `Makefile` deb/rpm targets, systemd unit
file unchanged.
Acceptance: `make deb` and `make rpm` produce installable, lintian-clean
(respectively rpmlint-clean) packages whose `ExecStart` runs the Go binary
with `Type=notify` passing; release ID visibly marks the golang build.
Tests: package build smoke test (script or Makefile check target);
install-and-start check in the podman systest environment.
Depends on: Task 18 (working binary).

### Task 28. Docs and NEWS
Files: branch `README.rst` section (Go build/cutover instructions incl.
state-file compatibility and rollback: reinstall Python package, state
file interoperates both ways — D3), `NEWS` entry listing intentional
behavior notes: golang release ID; kafka client is franz-go — compression
via `SnappyCompression()` (raw snappy block) and the xerial-framing
interop verdict from Task 22; **sanctioned deviation: Kafka brokers
older than 0.10.0 are no longer supported — the pre-0.10 version probe
is dropped, `kafka_api_version` unset negotiates automatically, a set
value pins the maximum protocol version** (user decision, 2026-10-11, D5
revision); franz-go send-path timeout mapping NEWS note if user-visible
(thread A7); **sanctioned deviation: SIGTERM grace-flush window — the Go
daemon gives senders up to `max_send_interval` to flush buffered messages
where Python's daemon sender threads lose them** (D9); **sanctioned
deviation: wrong-typed config values are rejected at load where Python
crashed mid-run — extended (user decision, Task 3 review) to values
Python silently coerced or truthiness-tested: numeric strings/floats for
integer keys, non-string values for string keys, boolean/float tag
values, readers: null** (thread A17); **sanctioned deviation: the
state-file and running-config writers use Go 1.27's `encoding/json/v2`
encoder — the parsed document stays equal to Python's
`json.dump(indent=4, sort_keys=True)` output, but the bytes are
Go-native: object key order follows Go map iteration (NOT sorted and NOT
deterministic across writes; nothing may assume an ordering in Go-written
files), floats render Go-style (60.0 → "60", 1e-05 → "0.00001",
1e+20 → "100000000000000000000"), and non-ASCII is raw UTF-8 instead of
ensure_ascii \uXXXX; the resume-cursor quirk is unaffected because
reading walks the file's key order in both implementations** (user
decision, 2026-10-09, extending the Task 3 review decision); **sanctioned
deviation: the logplex sender emits the clean RFC frame — Python's
accidental f-string bytes-repr wire framing (`len b'...'`) is NOT
reproduced; the Task 11 logplex fixture pins Python's actual wire shape
and Go's content parity is asserted via the repr-stripped frame list**
(user decision, 2026-10-09, Task 11 review); **sanctioned deviation:
runtime tolerance of malformed entry values — values that crash Python
mid-run (truthy non-string MESSAGE in the secret-filter/apply paths,
float/exotic search field values, non-numeric PRIORITY, non-string unit)
are tolerated: the value is left untouched or the field does not match,
and processing continues** (user decision, Task 5 review); **sanctioned
deviation: the regex warn-and-disable policy — secret-filter patterns
that are Python-valid but use constructs RE2 cannot compile are disabled
with a startup warning instead of being rejected** (user decision, Task 5
review); no other behavior changes.
Acceptance: a user can follow the README to build, install, cut over, and
roll back; every sanctioned deviation has a NEWS entry.
Depends on: Task 27.

### Task 29. Final parity sweep
Files: `docs/go-test-parity.md` completed; full test run.
Acceptance: every one of the ~127 Python unit tests is
ported/Go-idiomatic/dropped-with-reason; `make unittest-go systest-go`
green locally and in CI; quirk-lock tests enumerated (logplex localhost +
fire-and-forget, last-sender-wins resume, missing-cursor replay, `_bulk`
item-ignore, websocket batch header + slicing at the 30 B / 1 MiB base defaults, kafka snappy wire
format + fixed msg key, geohash tag values, state-file key order) all
present; the Task 10/11/12 capture suites now run against the Go binary
as parity assertions; `go-native` entries (rotation and sigterm suites,
grace-flush unit tests) accounted for separately from the Python parity
set.
Depends on: Tasks 18–26.

### Task 30. CI systest job
Files: `.github/workflows/build-go.yml` systest job on ubuntu-latest (real
journald + rsyslogd + broker service as needed — the kafka gate runs the
Task 22 franz-go systest against the Task 12 fixtures plus the
kafka-python consumer interop check; `CGO_ENABLED=0` static build is
preserved, which is one reason the cgo confluent client was rejected),
plus `scripts/systest-go-on-podman` (privileged Fedora container with
systemd-journald, mirroring `scripts/systest-on-podman`).
Acceptance: `make systest-go` runs in CI green; podman script works
locally.
Depends on: Task 18.

---

## Dependency order and milestones

- Tasks 1–9 (Phase 0) are mostly independent; sequence 1→2→3→9, and
  4/5/6/7/8 in parallel with those if multiple builders are available.
- Task 10 depends on 1 and 9; Task 11 depends on 10; Task 12 depends on
  10 (these three frontload all ground-truth fixtures).
- Task 13 depends on 7, 8; Task 15 depends on 4; 16 depends on 3, 4, 5,
  6, 9, 13, 14, 15, 11; 17 depends on 16; 18 depends on 17, 10, 11.
  Task 16 carries the first end-to-end value-demonstration checkpoint
  (charter §6). **Milestone A** after 18: journal→file with cursors
  surviving restart/reload/Python-state handover — the full acceptance
  gate; the Task 16 checkpoint is a mid-course demonstration only.
- 19→20→21 (Phase 2; 21 also needs 11). **Milestone B** after 20:
  journal→rsyslogd byte parity.
- 22 (needs 12, 13, 14; sub-steps a→f in order, 2026-10-11 client
  revision) / 23 (needs 11) / 24 (needs 11) parallelizable. Task 23
  closes with the shared HTTP helper extraction (logplex + ES refactor).
- 25/26 (Phase 4, parallelizable, need 13; 26 also needs the Task 23
  shared HTTP helper).
- **31 (Phase 4b, inserted 2026-10-12) runs after 25/26 and BEFORE 27** —
  hard prerequisites are 18, 10 and 13; the after-25/26 ordering is so
  the committed baseline measures the feature-complete binary. Task 28
  cites its numbers in NEWS; Task 30 may add `make bench-check` as an
  informational `workflow_dispatch`/nightly job and `make bench-test` to
  the correctness systest job, never a perf threshold as a required
  check. Numbered 31 to avoid renumbering 27–30.
- **32 (Phase 4b, added 2026-10-13) runs after 31 and BEFORE 27** — hard
  prerequisites are 15, 16 and 31; sub-steps (a)/(b) parallel, (d) needs
  both, (f) before (g). Its gate is the Task 31 harness against the
  committed baseline; it re-records the baseline on completion. Task 28
  carries the ruling-3 NEWS entry (charter amendment to
  "semantically-equivalent, byte-identical where free") and cites the
  post-pipeline numbers. Numbered 32 to avoid renumbering 27–30.
- 27→28→29→30 (Phase 5). **Milestone C** after 30: installable
  golang-marked packages + full CI parity.