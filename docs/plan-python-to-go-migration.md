# Plan: journalpump Python → Go migration (implementation tasks)

Source spec: `docs/spec-python-to-go-migration.md` (final, rev 7). Decisions
D1–D12 referenced below are §9 of the spec. Every task's diff goes through
the dual review gate (parity + Go idiom, charter §5).

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
and `encoding/json` into a map would discard it. **Sorted encode**: the
Go writer emits alphabetically sorted keys, matching Python's
`json.dump(..., sort_keys=True)` (`journalpump.py:1070`), so the file is
byte-faithful in both directions.
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
Files: `systest-go/journalpump_process.go` (harness) + suites. The harness
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
Tests: the two ported suites (both implementations); harness unit tests
(notify socket, exit-code capture).
Depends on: Tasks 1, 9.

### Task 11. Python ground-truth e2e capture: file/logplex/ES/websocket/state/replay (thread C2)
Files: `systest-go/groundtruth_*_test.go` + committed fixtures under
`systest-go/fixtures/`. Run the **Python binary** through the Task 10
harness with local capture servers, and commit the captured wire bytes as
golden fixtures that later Go tasks must match:
- **file sender**: filtered output file bytes for a fixture journal.
- **logplex**: exact POST body + headers captured by a local HTTP server;
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
Depends on: Task 10.

### Task 12. Kafka ground truth via real broker or recording proxy (thread C2)
Files: `systest-go/groundtruth_kafka_test.go` + fixtures. Against a real
broker (CI service) or a recording proxy: run the **Python binary**
producing a fixture journal (fixed `kafka_msg_key`, snappy compression),
consume with kafka-python, and record: delivered byte payloads, key/
partition placement, topic auto-create behavior. This is the ground truth
for the sarama sender (Task 22) and the xerial-framing interop question
(spec §5 kafka, risk 10).
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
identically to Python (same 30 messages).
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
Tests: httptest asserting exact body/headers **vs the Task 11 logplex
fixture** and that a 500 response does NOT trigger retry or disconnect
(parity quirk lock; custom fake per thread B — no maintained wire fake
exists).
Depends on: Tasks 13, 11.

## Phase 3 — Kafka, ES/OpenSearch, websocket

### Task 22. Kafka sender (sarama, D4, D5)
Files: `internal/sender/kafka/`. Config mapping for
PLAINTEXT/SSL/SASL_PLAINTEXT/SASL_SSL (`ca` for SSL/SASL_SSL,
certfile/keyfile SSL), SASL PLAIN/SCRAM-SHA-256/512, socks5 dialer,
`kafka_api_version`, producer settings (linger 500 ms, reconnect backoff
1 s/10 s), fixed `kafka_msg_key`, topic auto-create via cluster admin only
when both `kafka_topic_config` values present (already-exists tolerated).
**Send path uses `sarama.SyncProducer.SendMessages(batch)`** — one call
per batch, per-message `*ProduceError`, ack-then-advance (thread A7: do
not reimplement kafka-python's produce-per-message/future/flush dance;
map the 1 s per-future timeout to `Producer.WaitForAcks`/Net.ReadTimeout
and NEWS-note if user-visible). **Compression: `sarama.CompressionSnappy`**
(thread A8 — not a manual github.com/golang/snappy dependency; that
module is websocket-only). Retriable error → teardown + re-init loop,
silenced client logging.
Acceptance: config validation outcomes identical to Python; messages land
on a real broker matching the Task 12 ground truth (payload bytes,
key/partition, auto-create); kafka-python-consumer-vs-sarama-producer
interop test on a real broker proves the snappy framing story or yields a
NEWS-recorded deviation (spec risk 10).
Tests: parity ports of the ~20 config tests
(`test/unit/senders/test_kafka_sender.py`); unit tests with **sarama
mocks** (thread B: maintained mock tooling first); systest against a real
broker asserting the Task 12 fixtures; update parity checklist.
Depends on: Tasks 13, 14, 12.

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

### Task 24. Websocket sender
Files: `internal/sender/websocket/`. Single goroutine replacing the
asyncio-in-thread design; app-level snappy (github.com/golang/snappy) +
permessage-deflate options; batch NUL-join with `Journalpump-Batch-Size`
= configured max; batch overhead 1 (quirk-lock test); headers
(`User-Agent: journalpump/{version}`, `Journalpump-Compression`); TLS
triple ca/keyfile/certfile; SOCKS5; inbound drain consumer; reconnect
backoff 20 s×1.8→90 s reset after 60 s; 2 MiB inbound cap; close timeout
20 s; graceful close.
Acceptance: frames byte-match the Task 11 websocket fixture (NUL-joined
batch, snappy bytes, headers).
Tests: in-process Go websocket server (httptest, per thread B) — non-
batched and snappy-compressed batched delivery (parity:
`test/test_websocket.py` 2 tests, extended to cover header values and the
1-byte overhead); `testing/synctest` for reconnect-backoff timing; update
parity checklist.
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
Files: `internal/sender/gcl/` (logging/v2 with injectable HTTP transport).
Quota logic (250 KiB / 200 KiB truncation, replacement entries), severity
map, timestamp 26-char+Z, MESSAGE JSON-parse→jsonPayload, `generic_node`,
logName, optimistic reconnect.
Tests: official Google Cloud Logging fakes first (thread B), httptest
against the injected transport where they fall short, porting
`test/test_google_cloud_logging.py` cases (dummy SA key fixture reused
from `test/data.py`); update parity checklist.
Depends on: Task 13.

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
behavior notes: golang release ID; kafka compression via sarama
(`CompressionSnappy`) and the xerial-framing interop verdict from Task 22;
sarama send-path timeout mapping NEWS note if user-visible (thread A7);
**sanctioned deviation: SIGTERM grace-flush window — the Go daemon gives
senders up to `max_send_interval` to flush buffered messages where
Python's daemon sender threads lose them** (D9); **sanctioned deviation:
wrong-typed config values are rejected at load where Python crashed
mid-run — extended (user decision, Task 3 review) to values Python
silently coerced or truthiness-tested: numeric strings/floats for integer
keys, non-string values for string keys, boolean/float tag values,
readers: null** (thread A17); **sanctioned deviation: the running-config
dump is parsed-JSON-equal to Python's json.dump(sort_keys=True) but not
byte-identical — float rendering (60.0 → "60", 1e-05, 1e+20) and
non-ASCII emitted as raw UTF-8 instead of ensure_ascii \uXXXX** (user
decision, Task 3 review); no other behavior changes.
Acceptance: a user can follow the README to build, install, cut over, and
roll back; every sanctioned deviation has a NEWS entry.
Depends on: Task 27.

### Task 29. Final parity sweep
Files: `docs/go-test-parity.md` completed; full test run.
Acceptance: every one of the ~127 Python unit tests is
ported/Go-idiomatic/dropped-with-reason; `make unittest-go systest-go`
green locally and in CI; quirk-lock tests enumerated (logplex localhost +
fire-and-forget, last-sender-wins resume, missing-cursor replay, `_bulk`
item-ignore, websocket batch header + 1-byte overhead, kafka snappy wire
format + fixed msg key, geohash tag values, state-file key order) all
present; the Task 10/11/12 capture suites now run against the Go binary
as parity assertions; `go-native` entries (rotation and sigterm suites,
grace-flush unit tests) accounted for separately from the Python parity
set.
Depends on: Tasks 18–26.

### Task 30. CI systest job
Files: `.github/workflows/build-go.yml` systest job on ubuntu-latest (real
journald + rsyslogd + broker service as needed), plus
`scripts/systest-go-on-podman` (privileged Fedora container with
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
  **Milestone A** after 18: journal→file with cursors surviving
  restart/reload/Python-state handover.
- 19→20→21 (Phase 2; 21 also needs 11). **Milestone B** after 20:
  journal→rsyslogd byte parity.
- 22 (needs 12, 13, 14) / 23 (needs 11) / 24 (needs 11) parallelizable.
- 25/26 (Phase 4, parallelizable, need 13).
- 27→28→29→30 (Phase 5). **Milestone C** after 30: installable
  golang-marked packages + full CI parity.
