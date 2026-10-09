# journalpump Python → Go migration charter

Status: active. This is the single north-star document for the migration.
Every role agent validates its own progress and decisions against this
file — never against conversation memory.

Authoritative artifacts (source of truth, in this order):

1. `docs/migration-charter.md` — this file: requirements, protocol, ledger.
2. `docs/spec-python-to-go-migration.md` — the design spec (rev 7): full
   architecture, sender inventory, quirks, compatibility requirements,
   resolved decisions (§9).
3. `docs/plan-python-to-go-migration.md` — the 30-task implementation plan
   with per-task files, acceptance criteria, tests, and dependencies.

If this charter and the spec disagree, the spec wins on design detail and
this charter wins on requirements/protocol; flag the conflict to the
coordinator instead of silently picking one.

---

## 1. Purpose

journalpump is an Aiven-maintained daemon that reads systemd-journald and
forwards filtered entries as JSON to Kafka, Elasticsearch/OpenSearch, AWS
CloudWatch Logs, Google Cloud Logging, logplex, rsyslog, websockets, and
files. The migration is a **full rewrite in Go** (not a port-in-place) to
replace the Python runtime (~3,700 implementation lines, Python 3.13/3.14,
python-systemd private C bindings) with a single deployable Go binary.

"Done" means (spec §1): all eight senders pass byte- or behavior-level
parity tests against Python; the systemd notify lifecycle, SIGHUP reload,
SIGTERM shutdown, statsd metric names, state-file schema, deb/rpm
packaging and unit-file semantics are preserved; CI runs Go lint + unit +
system tests at coverage equivalent to today's `make unittest` /
`make systest`; the Python tree stays untouched on the branch until
cutover.

## 2. Non-negotiable requirements

These are hard requirements. A diff that violates any of them is rejected
in review regardless of passing tests elsewhere.

1. **Behavior identical to Python** (D2), including the deliberately
   preserved quirks (spec §3, §5). "Fixing" a quirk during the rewrite is
   a behavior change and needs an explicit user decision, not a builder
   choice. Every quirk has a named quirk-lock test.
2. **`journalpump.json` loads unchanged** (D2): every key accepted by the
   current Python code (complete per-sender key sets in spec §5) parses
   with identical defaults and validation outcomes. The code — not
   README.rst — is the source of truth for keys.
3. **Bidirectional state-file compatibility** (D3): a state file written
   by Go is readable by Python's `get_state`/`configure_readers` and vice
   versa, including per-reader `searches` hit counters, so operators can
   switch implementations freely without losing cursors or replaying the
   journal. Locked by permanent tests on BOTH sides (Go test + a
   branch-added pytest reading a committed Go-written fixture). Key order
   is part of the contract: Python writes with
   `json.dump(..., sort_keys=True)` (`journalpump.py:1070`), so the Go
   writer must emit alphabetically sorted keys.
4. **Resume-cursor quirk replicated exactly** (spec §6.3): resume cursor =
   last sender's persisted cursor **in state-file key order**; if any
   sender's cursor is missing, resume from journal head (full replay). Do
   not "fix" to oldest-cursor. This requires ordered decoding of the
   senders object (`json.Decoder` token streaming) — map decoding would
   silently destroy the quirk. Note: in Python-written files the order is
   alphabetical, so last-sender-wins always resolves to the
   alphabetically-last sender; quirk-lock fixtures exhibiting a different
   order must be hand-crafted.
5. **socks5-kafka retained** (D4) via sarama (D5) with a custom SOCKS5
   dialer.
6. **Logplex quirks preserved** (D6): fire-and-forget (response status
   ignored, no retry, always mark_sent), PRI 190 frame, and the
   underscore-strip quirk making hostname/pid always literal
   `"localhost"`.
7. **repr()-style UTF-8 fallback** (D8): Python's `process()`
   (`journalpump.py:711-714`) converts undecodable byte field values to
   their `repr()` string before JSON serialization — raw invalid bytes
   never reach the wire. The Go serializer must do the same; this is what
   makes byte-identical golden-file parity achievable.
8. **Kafka compression** (D5 + thread A8): `sarama.CompressionSnappy` —
   not a manual github.com/golang/snappy dependency (that module is for
   websocket app-level compression only). kafka-python used **xerial-
   framed** snappy, which is not automatically byte-identical to a
   sarama producer's framing; the kafka task must prove interop with a
   kafka-python consumer against the sarama producer on a real broker,
   and any mismatch affecting a supported consumer is a NEWS-recorded
   deviation.
9. **SIGTERM** (D9, **sanctioned deviation**): stop reading immediately,
   give senders up to their `max_send_interval` to flush, save state,
   exit 0. This deliberately differs from Python, whose daemon sender
   threads lose buffered messages on SIGTERM — see the sanctioned-
   deviations list below.
10. **No silent scope creep**: no new senders, no config format v2, no new
    metrics backends, no parallel-run machinery, no release-process
    change.

**Sanctioned deviations** (user-approved behavior changes, each with a
NEWS entry — plan Task 28):
- Wrong-typed config values are rejected at load by typed decoding,
  where Python crashed mid-run when the value was used (thread A17).
  User decision (2026-10-09, Task 3 review): the strict rejection also
  covers values Python silently coerced or truthiness-tested — numeric
  strings/floats for integer keys (e.g. threshold_for_metric_emit "10"
  or 10.5, msg_buffer_max_length 50000.0), non-string values for string
  keys (initial_position 1.5, write_running_config "yes", statsd host as
  number), boolean/float statsd and global-tag values, and readers: null
  — all rejected at load.
- Running-config dump formatting: the Go dump is parsed-JSON-equal to
  Python's json.dump(..., sort_keys=True) but not byte-identical (Go
  renders floats like 60.0 as "60", 1e-05 as "0.00001", and 1e+20 as
  "100000000000000000000" (Python's repr renders "60.0", "1e-05",
  "1e+20"), and emits
  non-ASCII as raw UTF-8 where Python's ensure_ascii=True writes \uXXXX).
  User decision (2026-10-09, Task 3 review): recorded as a deviation
  rather than reimplementing Python's float repr.
- Kafka: sarama send path replaces kafka-python's produce-per-message/
  future/flush dance with one `SyncProducer.SendMessages(batch)` call;
  per-future 1 s timeout maps to `WaitForAcks`/Net.ReadTimeout, NEWS note
  if user-visible (thread A7). Snappy framing verdict pending the Task 22
  interop test.
- SIGTERM grace-flush window: the Go daemon stops reading and gives
  senders up to `max_send_interval` to flush buffered messages before
  saving state and exiting 0. Python's sender threads are `daemon=True`
  (`senders/base.py:117`) and `shutdown()` (`journalpump.py:972-981`)
  only sets running flags + saves state, so buffered messages ARE lost on
  SIGTERM. User decision: keep the flush window (D9). NEWS-recorded.
- Runtime tolerance of malformed entry values: values that crash Python
  mid-run (truthy non-string MESSAGE in the secret-filter/apply paths,
  float/exotic search field values, non-numeric PRIORITY, non-string
  unit) are tolerated by Go — the value is left untouched or the field
  does not match, and processing continues. User decision (2026-10-09,
  Task 5 review): sanctioned crash-free deviation. NEWS-recorded
  (Task 28).
- Regex warn-and-disable policy: secret-filter patterns that are
  Python-valid but use constructs RE2 cannot compile (backreferences,
  lookarounds, atomic groups, possessive quantifiers, `{,n}`, `\Z`,
  `(?#)`, `(?u)`/`(?x)`, conditionals) are DISABLED with a startup
  warning instead of crashing or rejecting (Python would accept them at
  load). Python-INVALID patterns still hard-error at load with Python's
  message. User decision (2026-10-09, Task 5 review): sanctioned.
  NEWS-recorded (Task 28).

## 3. Resolved decisions (spec §9, user-confirmed 2026-10-09)

- **D1 Cutover/repo**: all work on a branch of this repo; the Go tree
  lives alongside the untouched Python tree. The RPM spec stays in the
  branch; users wanting the Go version build rpm/deb from the branch
  instead of main. The builder varies the release ID to mark golang builds
  (plan Task 27 proposes e.g. `%{minor_version}.golang%{?dist}`). No
  parallel-run machinery, no release-process change.
- **D2 Behavior**: identical to Python; config loads unchanged.
- **D3 State files**: bidirectionally Python-compatible (including
  `sort_keys=True` alphabetical key order).
- **D4 socks5-kafka**: retained.
- **D5 Kafka library**: IBM/sarama.
- **D6 Logplex**: identical behavior incl. the localhost quirk.
- **D7 Packaging**: rpm/deb still produced; arch-specific variants
  acceptable (rpm leaves `noarch`, deb leaves `Architecture: all`).
- **D8 Invalid UTF-8**: repr()-style fallback matching Python exactly.
- **D9 SIGTERM**: stop reading → flush ≤ `max_send_interval` → exit.
  Sanctioned deviation from Python, which loses buffered messages.
- **D10 Go policy**: latest stable Go toolchain; modern lint CI
  (golangci-lint with gocyclo, ineffassign, revive, staticcheck, errcheck,
  govet, gocritic; `go vet`; `go test -race ./...`).
- **D11 Mock strategy** (thread B): third-party mock tooling first
  (sarama mocks, official Elastic tooling, Google Cloud Logging fakes);
  for CloudWatch, aws-sdk-go-v2 ships no generated mocks, so the
  sanctioned approach is a small client-interface seam with hand-written
  or mockgen-generated fakes; custom httptest/net.Pipe/localhost fakes
  only where no maintained one exists; every fake asserts wire bytes,
  not library call shape.
- **D12 E2e frontloading** (thread C): implementation-agnostic systest
  harness (env var selects Python vs Go binary); ground-truth wire
  fixtures captured from the Python binary before Go sender work.

## 4. Validation checklist

Concrete, checkable statements. A role marks a task done only when every
statement relevant to that task's files can be answered "yes" from the
diff and test output.

**Any code task (builder self-check before requesting review):**
- [ ] Behavior for every touched path matches spec §3/§5, including quirks.
- [ ] Every test named in the plan task exists and passes
      (`go test -race ./...`; systests for systest tasks).
- [ ] No config key accepted by Python is dropped or re-defaulted (spec §5
      key sets).
- [ ] Python tree (`journalpump/`, `test/`, `systest/`, packaging for
      Python) untouched except where the plan task explicitly says
      otherwise (e.g. Task 8's interop pytest).
- [ ] Parity checklist (`docs/go-test-parity.md`) updated with ported /
      Go-idiomatic / dropped-with-reason / go-native-with-reason entries
      for any test covered.
- [ ] Lint clean (golangci-lint per D10) and no behavior comment left
      unjustified in the diff.

**Serialization / entries (Tasks 4, 6):**
- [ ] Golden serialization output byte-identical to Python `json.dumps`
      for the fixture entries, including an invalid-UTF-8 fixture whose
      field values appear as repr()-style strings, not raw bytes.
- [ ] Cursor `i=<hex>` seqnum parse and `__SEQNUM` enrichment match Python.
- [ ] `ip_to_geohash` tag values match Python for a test .mmdb (no silent
      empty tags).

**State and resume (Tasks 8, 17, 18):**
- [ ] State file written by Go can be read by Python's `get_state` and
      vice versa (both directions have a permanent, committed test); the
      Go writer emits alphabetically sorted keys matching Python's
      `json.dump(..., sort_keys=True)`.
- [ ] Resume selection: last-sender-wins in state-file key order
      (quirk-lock test with last-listed ≠ alphabetically-first —
      hand-crafted fixture, since captured Python files are always
      alphabetical); any-missing ⇒ replay from head.
- [ ] Ordered decode of the senders object; atomic write (tmp + rename),
      10 s cadence, saved on shutdown.

**Senders (Tasks 20–26):**
- [ ] `SendMessages` returns `error`; the shared loop classifies retriable
      vs permanent via the sender's `retriable(error) bool` seam and owns
      `mark_disconnected`/backoff (spec §4.2).
- [ ] Wire output byte-identical or behavior-identical per spec §5 row
      (framing bytes, headers, index/mapping logic, truncation) — asserted
      against the Task 11/12 ground-truth fixtures.
- [ ] Every quirk in the spec's quirk-lock list has a named Go test:
      logplex localhost + fire-and-forget; rsyslog octet-counted vs
      non-transparent truncation and PRI 13; ES `_bulk` item-errors-ignored
      + already-exists tolerated; websocket `Journalpump-Batch-Size` =
      configured max + 1-byte batch overhead; kafka fixed `kafka_msg_key`
      + snappy compression + one-SendMessages-per-batch ack-then-advance;
      CloudWatch sequence-token protocol; GCL 250 KiB quota replacement
      entries.
- [ ] Backoff (0.5 s→1800 s jittered, reset on connect), 1 MiB batching
      (+30 B, websocket +1), ≤8-deduped-128-char health ring, and
      `max_send_interval` defaults per sender match `senders/base.py`.
- [ ] Fakes follow the mock-strategy rule (D11): maintained third-party
      mocks first; custom fakes only where none exist (CloudWatch:
      client-interface seam with hand-written or mockgen-generated fakes —
      aws-sdk-go-v2 ships no generated mocks); all fakes assert wire
      bytes, not call shape.

**Daemon lifecycle (Tasks 9, 16, 17, 18):**
- [ ] sd_notify datagrams exactly: `READY=1`, `RELOADING=1`→`READY=1`,
      `STOPPING=1`, `WATCHDOG=1` each iteration.
- [ ] Concurrency per spec §4.3: `signal.NotifyContext` run context;
      errgroup.WithCancel reader/sender groups; SIGHUP via a separate
      buffered `signal.Notify` channel consumed by the supervisor select;
      no hand-rolled done-channels or atomic flags; leak-free shutdown
      verified.
- [ ] SIGHUP: ctime check at handler time; statsd/tags/geoip/filters
      rebuilt always; readers rebuilt only when the parsed `readers`
      differs (parsed-structure compare, order/whitespace-insensitive);
      filters captured at sender construction (no flush-time resolution).
- [ ] SIGTERM: cancel readers → wait → sender grace ≤ `max_send_interval`
      (sanctioned deviation D9 — Python's daemon sender threads lose
      buffered messages; the flush window is deliberate and NEWS-recorded)
      → state save → exit 0; exit codes match Python (`daemon.py:main`).

**Backpressure (Tasks 14):**
- [ ] Reader stops when the worst sender's buffer hits 50000 messages /
      5 MiB (`msg_buffer_max_length` / `msg_buffer_max_bytes`); channel
      capacity = limit + headroom; failed enqueue retries via select on
      send vs ctx.Done — nothing is dropped; flush when >1000 buffered or
      idle > `max_send_interval`.
- [ ] `-race` clean on buffer and shutdown paths.

**Go idiom (every task — see the dual review gate in §5):**
- [ ] A professional Go developer could navigate the codebase without
      wondering why it is structured unusually; no Python-shaped
      structures exist except where wire parity requires them. Structural
      imitation that does not affect wire behavior is flagged in review
      and replaced.
- [ ] Small consumer-side interfaces (defined at the consumer, not the
      producer); `context.Context` threading and `signal.NotifyContext`
      for cancellation/shutdown; goroutine lifecycle owned and leak-free
      (errgroup/WaitGroup, no orphaned goroutines on exit).
- [ ] Errors wrapped with `%w` and never swallowed — goroutine errors are
      surfaced to the caller, not lost.
- [ ] Table-driven tests (`t.Run`, `t.Parallel` where safe);
      `testing/synctest` (stable/default from Go 1.25; Go 1.24 requires
      `GOEXPERIMENT=synctest`) for timing-dependent logic instead of
      sleeps.
- [ ] Std-lib preferred over dependencies; sensible `internal/` layout
      with `cmd/journalpump` for the binary (spec §4.1); explicit wiring
      map instead of init()-registration.

**Packaging (Task 27):**
- [ ] `make deb` / `make rpm` from the branch produce arch-specific
      packages whose release ID visibly marks the golang build.
- [ ] Unit-file semantics unchanged (`Type=notify`, root, `Restart=always`,
      runtime dir 0700); deb `adduser` postinst unchanged.
- [ ] `ExecStart` runs the Go binary and reaches `READY=1` under systemd.

**Cross-reviewer gate (every diff — DUAL gate):**
- [ ] Parity gate: verified the diff against this checklist's relevant
      items and the spec sections the task cites.
- [ ] Go-idiom gate: verified the diff against the Go-idiom items above.
      Behavioral parity (wire formats, state files, quirks) always wins
      over idiom when the two conflict — but structural imitation of
      Python that does not affect wire behavior must be flagged and
      replaced. The verdict names which gate items were checked.
- [ ] Verdict recorded in the ledger (§6).

## 5. Working protocol

- **Loop**: architect designs (spec/plan) → cross-reviewer reviews each
  artifact and each builder diff → builder implements one plan task at a
  time → cross-reviewer reviews the diff → coordinator records the verdict
  and dispatches the next task. No task starts before its dependencies
  (plan "Dependency order and milestones") are reviewed-accepted.
- **Dual review gate**: every cross-reviewer pass on a builder diff
  checks **Go best practices in addition to parity**: small consumer-side
  interfaces; `context.Context` / `signal.NotifyContext` for cancellation
  and signals; goroutine lifecycle and leak-free shutdown (errgroup /
  WaitGroup); error wrapping (`%w`) with goroutine errors surfaced, not
  swallowed; table-driven tests with `t.Run` / `t.Parallel` and
  `testing/synctest` (stable/default from Go 1.25; Go 1.24 requires
  `GOEXPERIMENT=synctest`) for timing-dependent code; sensible `internal/`
  layout with `cmd/` for the binary; std-lib preference; no structural
  imitation of Python where it does not affect wire behavior.
  **Behavioral parity (wire formats, state files, quirks) always wins over
  idiom when they conflict** — but structural imitation must be flagged
  and replaced where parity is unaffected.
- **Role boundaries**: only the builder edits code (including tests,
  packaging, and the branch-added Python interop pytest). The architect
  writes only under `docs/`. The cross-reviewer is read-only.
  bulk-reader is used for any question spanning several files instead of
  raw file dumps; boilerplate-writer for files that mirror an existing
  one.
- **Source of truth**: this charter, the spec, and the plan — NOT
  conversation memory. An agent that cannot validate a decision from
  these three files must ask the coordinator rather than guess. Handover
  rule: before starting a task, read the task entry in the plan, the spec
  sections it cites, and the ledger row of its dependencies.
- **Quirk policy**: any deviation from Python behavior — even an
  improvement — requires a user decision via the coordinator and a NEWS
  entry (plan Task 28). The builder does not get to decide. The
  sanctioned-deviations list in §2 is the complete set of pre-approved
  deviations.
- **Docs updates**: spec/plan changes go through architect + cross-review,
  same as code.
- **Review-round record**: the pre-implementation Go-idiom review of
  spec+plan (thread A), the mock-strategy decision (thread B, D11), and
  the e2e-frontloading decision (thread C, D12) are folded into the spec
  and the 30-task plan; the validation round (SIGTERM grace-flush
  sanctioned deviation, sort_keys state-file ordering, synctest toolchain
  note, CloudWatch mock rewording) is folded into spec rev 7. No open
  placeholder remains; future review rounds amend these artifacts through
  the same loop.

## 6. Progress ledger

The coordinator (or builder, after receiving a review verdict) updates
this table after every reviewed task — one row per plan task, status one
of: `not-started`, `in-progress`, `review`, `done`, `blocked`. The commit
column holds the branch commit hash (or PR link) of the accepted diff;
verdict is the cross-reviewer's last verdict (`approved` / `changes-
required: <n>` / `-`). Any agent or future session reads this table
first to locate the migration's current state; do not re-derive progress
from git log alone.

| Task | Description (short) | Status | Commit | Review verdict |
|---|---|---|---|---|
| 1 | Branch scaffold, Go module skeleton, Makefile targets | done | 1421f47 + 353ea17 | approved (6 minors fixed)¹ |
| 2 | CI workflow: golangci-lint, go vet, go test -race | done | 17d1c3b + 9cb194f | approved (6 minors fixed)² |
| 3 | Config loader + running-config writer (parsed-blob diff) | done | c9fbab9 + 7ab4a89 | approved (3 doc minors fixed)³ |
| 4 | Journal entry model, repr UTF-8 fallback, JSON serialization | done | 3fbfe92 + 45d003c + 1d678cb | approved (1 major + 6 minors fixed across two review rounds; golden fixture Python-generated, 8 cases, byte-identical; 2.2M-exec fuzz clean)⁴ |
| 5 | Filters (field/unit-log-level/secret/search) | not-started | - | - |
| 6 | Geohash encoder + GeoIP + tag callables | not-started | - | - |
| 7 | statsd client + Tagged sanitization | not-started | - | - |
| 8 | State file I/O (ordered decode, sorted encode, interop tests) | not-started | - | - |
| 9 | sd_notify, watchdog, signals (NotifyContext + SIGHUP channel) | not-started | - | - |
| 10 | Implementation-agnostic systest harness + running-config/reload suites | not-started | - | - |
| 11 | Python ground-truth e2e: file/logplex/ES/websocket/state/replay fixtures | not-started | - | - |
| 12 | Kafka ground truth via real broker / recording proxy | not-started | - | - |
| 13 | Sender framework (error contract, wiring map, synctest) | not-started | - | - |
| 14 | Per-sender buffer + reader backpressure (admission gate) | not-started | - | - |
| 15 | sdjournal reader (consumer-side seam, stats, match_key/value in handler) | not-started | - | - |
| 16 | Run loop + per-sender pipeline + file sender | not-started | - | - |
| 17 | Reload, resume, state save, shutdown (errgroup lifecycle) | not-started | - | - |
| 18 | Core systest suites (rotation/sigterm Go-native, interop) | not-started | - | - |
| 19 | Syslog framing package | not-started | - | - |
| 20 | Rsyslog sender (+ ported rsyslog systest driver) | not-started | - | - |
| 21 | Logplex sender | not-started | - | - |
| 22 | Kafka sender (sarama SyncProducer, CompressionSnappy, interop proof) | not-started | - | - |
| 23 | Elasticsearch/OpenSearch sender | not-started | - | - |
| 24 | Websocket sender | not-started | - | - |
| 25 | CloudWatch sender (aws-sdk-go-v2 client-interface fakes) | not-started | - | - |
| 26 | Google Cloud Logging sender | not-started | - | - |
| 27 | Packaging on branch (golang release ID, arch-specific) | not-started | - | - |
| 28 | Docs and NEWS (sanctioned deviations) | not-started | - | - |
| 29 | Final parity sweep (~127 tests, fixtures flip to Go parity) | not-started | - | - |
| 30 | CI systest job + podman script | not-started | - | - |

**Update rule**: after the cross-reviewer approves a task's diff and the
coordinator accepts it, the builder (or coordinator) sets the row to
`done` with the commit hash and verdict, and sets the next dispatched task
to `in-progress`. If a task is returned with changes required, keep it
`review` (or `in-progress` if rework was dispatched) and update the
verdict. Milestones (plan: A after Task 18, B after Task 20, C after Task
30) are noted in the verdict column of the completing task.

¹ Task 1 evidence: `make build-go` / `unittest-go` (exit-code parity
subtests, `-race`) / `systest-go` / `lint-go` all green on 353ea17;
missing-config message renders Python-faithfully as `PosixPath(<path>)`.
**Carried note (resolved in Task 2)**: the preinstalled golangci-lint
2.12.2 is built with go1.26 and refuses `go.mod` targeting 1.27; CI now
installs v2.14.0 from source with the go.mod toolchain (Go 1.27).

² Task 2 evidence: `golangci-lint config verify` OK (.golangci.yml v2,
D10 linter set + gofumpt); trivial intentional lint failures verified to
block once on the branch (errcheck exit 2; gofumpt and gocyclo reported);
`make lint-go` / `unittest-go` green on 9cb194f; workflow YAML parsed
(actual GitHub Actions run pending the first Go-touching PR).

³ Task 3 evidence: `make unittest-go` / `make lint-go` green on 7ab4a89;
golden full-config decode (all 9 senders' §5 key sets), 34+ rejection
rows (strict typed decoding incl. trailing data and invalid UTF-8),
numeric-aware blobEqual quirk-lock (10 vs 10.0, 1 vs true), byte-exact
running-config write (0600/0700 modes), WARN/FATAL level aliases.

⁴ Task 4 evidence: `make unittest-go` / `make lint-go` green on 1d678cb;
golden fixture Python-generated by driving the real pipeline
(`internal/journal/testdata/generate_golden.py` → 8 cases, byte-identical,
incl. the CPython repr quote-selection lock `b"don't\xff"` →
`b"don't\xff"` with `"` delimiter); `FuzzParseSeqnumFromCursor` ~2.2M
execs clean with a hand-rolled reference oracle; committed fuzz seeds
79f77c8e (bare `i=`) and b77f1dbf (`i=++0` double-sign regression).