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

> **Governing principle (user, 2026-10-09)** — the top-level rule from
> which every deviation decision in this charter derives:
> **Aspire to consistency with Python, EXCEPT where the Python
> implementation is over-prescriptive (e.g. JSON key ordering) or
> incorrect (e.g. the logplex f-string bytes-repr wire bug).**
> Named exemplars and the resulting decisions:
> - *JSON key ordering*: Python's `sort_keys=True` is an implementation
>   detail, not intent — it is not imitated anywhere. The state-file and
>   running-config writers emit Go-native key order (user decision,
>   2026-10-09); parsed-document equality with Python is the contract.
> - *Logplex (user decision, 2026-10-09)*: the Go sender emits the clean
>   RFC frame format; Python's accidental f-string bytes-repr wire
>   framing is NOT reproduced — a sanctioned deviation.
> Everything else defaults to reproduce-exactly (items 1–10 below).

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
   branch-added pytest reading a committed Go-written fixture). The
   compatibility contract is the PARSED document: Python writes with
   `json.dump(..., sort_keys=True)` (`journalpump.py:1070`), while the
   Go writers use Go 1.27's `encoding/json/v2` with Go-native key order
   — a sanctioned deviation (user decision, 2026-10-09); no test may
   assert a Go-written file's key ordering. The resume-cursor quirk
   (item 4) is unaffected: reading walks the file's key order in both
   implementations.
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
   `"localhost"` — except the framing itself, which follows the governing
   principle above (clean RFC frame, bytes-repr bug not reproduced).
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
  rather than reimplementing Python's float repr. Extended (user
  decision, 2026-10-09, encoder switch — see the v2-writers entry
  below): the running-config writer AND the state-file writer now use
  Go 1.27's encoding/json/v2.
- State-file and running-config writers use Go 1.27's
  `encoding/json/v2` encoder: object key order is Go-native — map
  iteration order, NOT sorted and (probe-verified) NONDETERMINISTIC
  across writes — string escaping is raw UTF-8 (no ensure_ascii), and
  floats render Go-native. Python's json.dump 4-space layout, the
  no-trailing-newline shape and the 0600 atomic replace are kept, and
  the PARSED document stays equal to Python's
  json.dump(indent=4, sort_keys=True) output; the resume-cursor quirk
  is unaffected because reading walks the file's key order in both
  implementations. No test may assert a Go-written file's key
  ordering. User decision (2026-10-09, encoder switch). NEWS-recorded
  (Task 28).
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
  (Task 28). EXTENDED (user decision, 2026-10-09, Task 6 review) to the
  ip_to_geohash failure paths (`int(precision)` ValueError, missing tag
  KeyError, geoip2 AddressValueError/AddressNotFoundError, AttributeError
  on a record without location, geohash's ValueError): Go degrades to an
  empty geohash tag and FORWARDS the entry, where Python's
  read_single_message (journalpump.py:1011-1018) catches, logs
  "Unexpected exception while handling entry for %s", emits
  stats.unexpected_exception, sleeps 0.5 s and DROPS the entry —
  forwarded-with-empty-tag vs dropped-with-unexpected-exception is the
  sanctioned delta.
- Statsd per-call tag ordering: the GLOBAL `statsd.tags` reach the statsd
  wire in exact Python order (the config layer captures the document order
  from the raw JSON; `dict.update` override-keeps-position semantics are
  ported exactly). Per-call tag keys (the `Tagged.make_tags` output) arrive
  as Go maps whose insertion order is unrecoverable, so NEW per-call keys are
  appended after the globals sorted among themselves. Coordinator decision
  (2026-10-09, Task 7 review): sanctioned — metric values identical; telegraf
  treats the tag list as a set; ordering unobservable in stored data.
  NEWS-recorded (Task 28).
- Regex warn-and-disable policy: secret-filter patterns that are
  Python-valid but use constructs RE2 cannot compile (backreferences,
  lookarounds, atomic groups, possessive quantifiers, `{,n}`, `\Z`,
  `(?#)`, `(?u)`/`(?x)`, conditionals) are DISABLED with a startup
  warning instead of crashing or rejecting (Python would accept them at
  load). Python-INVALID patterns still hard-error at load with Python's
  message. User decision (2026-10-09, Task 5 review): sanctioned.
  NEWS-recorded (Task 28).
- Native sd_notify client: the sd_notify/watchdog client is a
  hand-written std-lib implementation (~60 lines, `internal/daemon`)
  instead of the spec §4.6 coreos/go-systemd (sdnotify) library row.
  Coordinator decision (2026-10-09, Task 9 review): sanctioned —
  std-lib preference (D10, no new dependency) and Python-exact failure
  shapes (the empty NOTIFY_SOCKET must fail with Python's OSError
  EPROTO shape and a garbage address must fail at a prefix check,
  where go-systemd would attempt a relative-path connect; go-systemd
  v22.7.0 `daemon.SdNotify` returns (false, nil) for an empty socket
  value and uses the same abstract-socket mechanism — the deviation is
  the failure-shape parity and the Notifier seam, not the wire
  mechanics). NEWS-recorded (Task 28).
- Native libsystemd journal binding: the sd-journal interop is a
  hand-written dlopen binding (`internal/reader/ffi.go`, cgo with only
  dlfcn/stdlib/stdint/string; all symbols resolved at runtime) instead
  of the spec §4.4 coreos/go-systemd/v22 (sdjournal) library row.
  Reviewer-verified 2026-10-09 (Task 15 review): go-systemd's sdjournal
  needs the systemd development headers at build time (absent on the
  build host; no package install available) and lacks
  `sd_journal_open_namespace` entirely, while the binding needs only
  libdl and probes libsystemd.so.0 at runtime — the identical runtime
  dependency python-systemd has today (precedent: the D10 native
  sd_notify client). NEWS-recorded (Task 28).
- Logplex framing: Go emits the clean RFC frame format; Python's
  accidental f-string bytes-repr wire framing bug is not reproduced
  (user decision, 2026-10-09; see the governing principle above).
- Fail-fast on sender panic: a panic inside a sender's transport call
  is NOT recovered — it crashes the process, where Python's blind
  per-thread except (senders/base.py:117 daemon threads) silently
  froze a crashed sender thread while the process stayed "healthy"
  (no is_alive monitoring, reader wedged on backpressure).
  Coordinator-ruled 2026-10-09 (Task 13 review) under the §2 "Python
  is incorrect" clause, independently confirmed by review.
  Trade-off recorded: a deterministic panic (poison entry)
  crash-loops the whole daemon and stalls all senders, where Python
  degraded to one wedged sender — accepted because senders must
  return errors rather than panic and the systemd restart is
  self-resetting with state-file resume. NEWS-recorded (Task 28).
- Construct-before-retire safe abort: on reload, the replacement reader
  is fully constructed BEFORE the old one is retired; if construction
  fails, the old reader stays running with its settings and cursors
  intact (a safe abort) rather than tearing down the old reader and
  leaving the pump with a missing reader. Python destroys the old
  reader first and constructs the replacement second
  (`configure_readers`), so a construction failure leaves it with
  neither — under the §2 "Python is incorrect" clause this ordering is
  not reproduced. Coordinator-ruled 2026-10-10 (Task 17 review);
  NEWS-recorded (Task 28).
- Delivery-checkpoint contract (Task 17): the persisted sender cursor
  only ever claims bytes that are durably on disk — the durable
  watermark never advances past a complete output line. A failed flush
  or a reload retire truncates the output back to the last complete-line
  boundary before the cursor is saved, so a restart/replay can never
  re-append onto a corrupt partial JSON object and can never duplicate a
  line the cursor already claimed. The sender's byte counters are
  RUN-relative, so an absolute truncate is the output's initial append
  offset plus the run-relative boundary: reopening an existing output and
  retiring before any new flush preserves every previously delivered
  line. A reload whose retirement cannot restore the boundary (a failed
  CloseDiscarding) is refused before the replacement reader starts, so no
  writer is allowed to append onto a corrupt prefix; the failed entries
  stay registered as a persistent cleanup-failure state, so a later reload
  retries the cleanup and refuses again until it succeeds (the file sender
  keeps its handle open across a failed discard to make that retry able to
  truncate). If no reload follows, shutdown's Finalize and Close still
  retry the pending truncation — Close reports the error and refuses to
  release an unrepaired handle, so a SIGTERM after a failed cleanup cannot
  leave a corrupt prefix. This is a
  correctness contract, not a Python-parity claim: Python's cursor is the
  last SENT (not flushed) position, which under its daemon-thread shutdown
  can already point past data it never wrote. Required failure tests:
  restart after a flush that cuts through a line (assert valid JSONL, no
  duplicate/corrupt partial object), discard truncation to the last
  complete line, a failed flush leaving a clean edge, a reopen-retire
  on a pre-populated output, a repeated-reload refusal (a first
  failed cleanup makes the next changed reload refuse too) plus the
  repair path once a retried cleanup succeeds, and a shutdown repair
  (a failed cleanup followed by Finalize/Close with no reload removes the
  partial line or surfaces an error, leaving valid JSONL for the next
  restart). NEWS-recorded (Task 28).
- Logging architecture (Task 17): the daemon logs through an io.Writer
  sink wired into the Printf seam (level-aware `journaldlog.Log`), rather
  than the spec §4.5 slog-to-journald datagram mention. The sink selects
  the journald datagram writer when stdout is not a tty and a TTY
  formatter otherwise, maps Python log levels to journald PRIORITY,
  honors `log_level` at startup and on SIGHUP, and emits the logger-name
  context Python includes — reproducing daemon.py's configure_logging
  behavior (the Printf seam was the pre-existing wiring; the bug fixed
  here was that it ignored level/severity). Coordinator-sanctioned
  2026-10-10 (Task 17 review) as a deviation from the spec's slog
  mention; NEWS-recorded (Task 28).

## 3. Resolved decisions (spec §9, user-confirmed 2026-10-09)

- **D1 Cutover/repo**: all work on a branch of this repo; the Go tree
  lives alongside the untouched Python tree. The RPM spec stays in the
  branch; users wanting the Go version build rpm/deb from the branch
  instead of main. The builder varies the release ID to mark golang builds
  (plan Task 27 proposes e.g. `%{minor_version}.golang%{?dist}`). No
  parallel-run machinery, no release-process change.
- **D2 Behavior**: identical to Python; config loads unchanged.
- **D3 State files**: bidirectionally Python-compatible (parsed-document
  equality; Go writers use Go-native key order — user decision,
  2026-10-09).
- **D4 socks5-kafka**: retained.
- **D5 Kafka library**: IBM/sarama.
- **D6 Logplex**: identical behavior incl. the localhost quirk; framing
  follows the governing principle (clean RFC frame, not the bytes-repr
  bug).
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
      parsed document matches Python's `json.dump(..., sort_keys=True)`
      output while the Go writer's key order is Go-native (user
      decision, 2026-10-09 — no ordering asserted for Go-written files).
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
  deviations. When classifying a finding, reviewers and builders apply
  the §2 governing principle as the test: does the Python behavior
  reflect **intent** (reproduce it, with a quirk-lock test) or
  **accident/over-prescription** (propose a deviation — user or
  coordinator decision per the same granularity rule that separates
  quirk-locks from sanctioned deviations)?
- **Docs updates**: spec/plan changes go through architect + cross-review,
  same as code.
- **Charter sections change only via dedicated docs commits**: the §2
  sanctioned-deviations list and the §6 progress ledger never ride along
  inside a code-task commit. Motivation (real incident, 2026-10-09):
  commit 1773ebb folded an uncommitted §2 restructure into a code
  commit and silently wiped the §6 ledger (Tasks 1–10 done rows) and
  five §2 bullets; both were restored from git history in f3e1f24 and
  the restoration was confirmed by both reviewers.
- **Review-round record**: the pre-implementation Go-idiom review of
  spec+plan (thread A), the mock-strategy decision (thread B, D11), and
  the e2e-frontloading decision (thread C, D12) are folded into the spec
  and the 30-task plan; the validation round (SIGTERM grace-flush
  sanctioned deviation, sort_keys state-file ordering, synctest toolchain
  note, CloudWatch mock rewording) is folded into spec rev 7. No open
  placeholder remains; future review rounds amend these artifacts through
  the same loop.

**Drift controls** (added 2026-10-09 after a sibling project's
remediation drifted into infrastructure tasks while the user's actual
objective receded and local approvals became the progress metric):

- **Review-finding taxonomy** (cross-reviewer prompts label each
  finding; coordinator adjudicates): (1) defect in an accepted
  requirement, (2) unmet requirement needed for the current milestone,
  (3) proposed new requirement or threat-model extension, (4) optional
  robustness improvement. Only 1–2 block the milestone; 3–4 get
  coordinator scope adjudication — accept, defer to a named task, or
  reject — rather than automatic fix rounds.
- **Replan triggers** — pause the task cadence and reassess priorities
  with the user when: two unsuccessful correction rounds on one task;
  three scope escalations within one task; scope growth >25% against
  the plan task's stated deliverables; or five implementation tasks
  completed without any end-to-end value demonstration (the Go binary
  actually running).
- **Ledger metrics** (§6): each task row records its review-round count
  and minor-finding count (majors already implied by the verdict
  history), and the ledger keeps a running "last value demonstration"
  line — the most recent commit where the Go binary demonstrably ran
  end-to-end (none yet; update when Task 16/18 produces the first
  run).

## 6. Progress ledger

The coordinator (or builder, after receiving a review verdict) updates
this table after every reviewed task — one row per plan task, status one
of: `not-started`, `in-progress`, `review`, `done`, `blocked`. The commit
column holds the branch commit hash (or PR link) of the accepted diff;
verdict is the cross-reviewer's last verdict (`approved` / `changes-
required: <n>` / `-`). Any agent or future session reads this table
first to locate the migration's current state; do not re-derive progress
from git log alone.

Last value demonstration: 2026-10-10 — the Milestone A acceptance gate
(Task 18): `make systest-go` runs 20 Go-native systest tests under `-race`
(the child daemon itself built with `-race`), the real Go binary is started
from production-format config against real libsystemd and fixture journals
for the full journal→file pipeline (rotation boundaries, reload with the
persisted resume cursor, SIGTERM grace flush, restart, and a real
Python→Go→Python state round trip); plus 9 Python-harness cases that spawn
the Go binary (test_reload 1, test_running_config 7, test_sigterm 1).
Supersedes the Task 16 file-sender e2e shim demonstration below.

Earlier value demonstration: 2026-10-09 — `systest-go/file_sender_e2e_test.go`
(`TestFileSenderEndToEnd`, run via `make systest-go` or
`go test -race -tags systest ./systest-go/`): real Go binary +
production-format config + real libsystemd reader + fixture journals →
40 output lines byte-identical to the Task 11 ground truth, sent
cursor/count parity, exit 0 on SIGTERM. The evidence is the TEST-SHIM
path (`JOURNALPUMP_FILES_AVAILABLE=1`, the analog of the Python
ground-truth sitecustomize patch); production-path equivalence is the
Task 18 harness's job.

**Milestone A complete (2026-10-10).** Core-pipeline parity is done: branch
scaffold + CI, the run loop and per-sender pipeline, the state file, filters
and serialization, the file sender, and reload/resume/shutdown, with the
systest gate above passed. Milestone B (journal→rsyslogd byte parity) starts
with Tasks 19–20.

| Task | Description (short) | Status | Commit | Review verdict | Rounds | Minors |
|---|---|---|---|---|---|---|
| 1 | Branch scaffold, Go module skeleton, Makefile targets | done | 1421f47 + 353ea17 | approved (6 minors fixed)¹ | – | – |
| 2 | CI workflow: golangci-lint, go vet, go test -race | done | 17d1c3b + 9cb194f | approved (6 minors fixed)² | – | – |
| 3 | Config loader + running-config writer (parsed-blob diff) | done | c9fbab9 + 7ab4a89 | approved (3 doc minors fixed)³ | – | – |
| 4 | Journal entry model, repr UTF-8 fallback, JSON serialization | done | 3fbfe92 + 45d003c + 1d678cb | approved (1 major + 6 minors fixed across two review rounds; golden fixture Python-generated, 8 cases, byte-identical; 2.2M-exec fuzz clean)⁴ | – | – |
| 5 | Filters (field/unit-log-level/secret/search) | done | ef53aac + c999ebb | approved (3 majors + 9 minors fixed; one finding rebutted with evidence and upheld — Python accepts a missing search fields key via setdefault; two new sanctioned deviations: runtime entry-value tolerance, regex warn-and-disable) | – | – |
| 6 | Geohash encoder + GeoIP + tag callables | done | 5122ff2 + 283e87e | approved (3 majors + 5 minors fixed; NaN guard, document-order tag visibility via JSON token walk, deviation extension recorded per coordinator ruling; 225-case golden battery) | – | – |
| 7 | statsd client + Tagged sanitization | done | 5b343e2 + 10ffc40 | approved (1 major + 5 minors fixed; global statsd tag order exact via capture, per-call appended sorted = sanctioned deviation; empty-host/udp4 parity locked) | – | – |
| 8 | State file I/O (ordered decode, Go-native encode, interop tests) | done | b445b53 + 9ec632a + 1773ebb | approved (1 major + 4 minors fixed; sent.cursor error scope matches Python's walk position incl. null-cursor abort; file-order decode; lenient write-only fields per coordinator ruling; permanent Python interop test in test/test_state_file_interop.py; two non-blocking residuals noted: pythonReprString \xNN escapes unreachable, other write-only fields strictly typed but unreachable from Python-written files). RE-RECORDED against the amended contract (1773ebb, user decision 2026-10-09, encoder switch — charter §2): the original byte-identical sorted-key writer was replaced by Go 1.27's encoding/json/v2 writer — parsed-equality contract, Go-native key order (nondeterministic across writes per probe), order-independent tests; verdict re-affirmed by the 1773ebb review round (2 doc restoration majors + 2 minors fixed) | – | – |
| 9 | sd_notify, watchdog, signals (NotifyContext + SIGHUP channel) | done | 079c897 + 861d57d | approved per the reviewer's line: "sd_notify client Python-parity probed (unset no-op / empty+garbage EPROTO / ECONNREFUSED / ENOENT / '@' abstract), byte-exact state constants, NotifyContext + buffered(1) SIGHUP channel with drop-when-full coalescing pinned; native-client library deviation coordinator-sanctioned (charter §2) with corrected rationale; unittest-go/lint-go green" | – | – |
| 10 | Implementation-agnostic systest harness + running-config/reload suites | done | d773674 + 0d7c1ba | approved (Go harness deferral recorded: systest-go/journalpump_process.go deferred to Task 18; Python-side harness rework (JOURNALPUMP_BIN selection) delivered in Task 10 and is the mechanism both implementations use; running-config/reload suites pass against the Python binary unchanged; harness unit tests for notify socket + exit-code capture) | – | – |
| 11 | Python ground-truth e2e: file/logplex/ES/websocket/state/replay fixtures | done | 4e9985b + 0d4bac5 | changes-required (2 majors + 4 minors) FIXED in 0d4bac5: logplex bytes-repr framing reclassified as a sanctioned deviation (charter §2/D6) — Python ground truth only, content parity via the repr-stripped frame list (parity doc, support docstring, plan Task 21/11/28 aligned); ES content-length assertion moved into the per-run loop; ordered non-alias request sequence persisted in the fixture; per-entry JSON-cache quirk now CAPTURED with two concurrent filterless senders; replay count literal derived. Confirmation pass added: request_sequence fixture value built as lists (GROUNDTRUTH_UPDATE=1 worked end-to-end) | – | – |
| 12 | Kafka ground truth via real broker / recording proxy | done | 6e0c765 + eaee3cf | approved after one review round (5 minors fixed in eaee3cf; explicit skips: _free_port TOCTOU, base64 payload pinning — ASCII fail-loud guard instead): podman Kafka 4.0 KRaft captures (auto-create + manual-create/2-partitions), 40 payloads per run byte-reproducible (fixture sha1 48d65e4b), payload/key/partition/offsets/sent-cursor asserted, snappy pinned via zstandard-blocking shim (deployment-faithful; zstd-preference fact recorded in fixture notes for Task 22) | 1 | 5 |
| 13 | Sender framework (error contract, wiring map, synctest) | done | 8b3e164 + 6e32971 | approved after one review round (5 minors fixed in 6e32971: post-jitter backoff log, per-route app-tag routing, %r-style quote parity, parity-doc forward notes). Fail-fast divergence independently verified by review: Python = silent unrecoverable sender wedge (base.py:117 daemon thread, no is_alive monitoring) while watchdog keeps passing — now a sanctioned deviation (§2) | 1 | 5 |
| 14 | Per-sender buffer + reader backpressure (admission gate) | done | 08ad213 + 3ed7609 | approved after one review round (4 minors fixed in 3ed7609: budget passthrough for the msg_buffer_max_length=0 corner, runtime Attach guard, Task 16 forward note on the bytes_read measure; getsizeof byte budget ruled cosmetic with coordinator+reviewer concurrence) | 1 | 4 |
| 15 | sdjournal reader (consumer-side seam, stats, match_key/value in handler) | done | 0fabf21 + 56f217a | changes-required → FIXED in 56f217a (one review round: 3 majors + 4 minors): FFI step ABI (sd_journal_next/previous return int — a uint64 wrapper zero-extended -errno into "entry present"; stepFromRc + test pin), match-add moved after the initial seek (Python order — match-then-seek silently lost the first matching entry after the resume cursor), LOCAL_ONLY default for plain/namespace opens restored (python-systemd journal.py:168-174; earlier false "no masking" probe claim corrected in the parity doc); minors: Wait timeout semantics (<0 indefinite, 0 non-blocking poll), symbol-resolution errors surfaced as named errors, match_value=None wiring guard + mid-open-failure divergence pinned in the parity doc. Native libsystemd dlopen binding deviation sanctioned (§2). Fixture acceptance: rotated_logs reproduces Python's probed ground truth (40 entries, entry-0 fields/types/key order); bad_msg corruption taxonomy probed (field-without-= vs Bad message layering). Metrics backfilled: 2 rounds (1 major round: 3M+4m, then confirm: 1 minor residual + accepted scope call) | 2 | 5 |
| 16 | Run loop + per-sender pipeline + file sender | done | 6403ff0 + d51bb7d + 2846641 + 050a2bd | approved after 5 review rounds (round 1: 9 majors + 2 minors; round 2: 3 majors + 3 minors; round 3: 3 majors; round 4: 2 majors; round 5: approve), all fixed. Round-1 finds included four real parity bugs — sender document order, empty-filter {}, match_value None, SIGTERM chunk over-read — plus the buffered-flush ordering major. Value demonstration validated as the committed e2e test (systest-tagged shim path: all 40 lines + state + exit 0 independently confirmed) | 5 | 5 |
| 17 | Reload, resume, state save, shutdown (errgroup lifecycle) | done | 4734fe2 + 3e46a03 + 02f784f + 79758bc + 0be3b65 + aedfff0 + 8405ea7 + e69e897 + 6b2c8f2 | approved after 4 review rounds (6 majors + 1 minor → 5 majors + 2 minors → 1 major + 2 minors → 1 major + 2 minors, all fixed). Round 1 FIXED in 02f784f: partial-line corruption on auto-flush/discard now truncates to the last complete-line boundary (durable cursor never claims bytes past a complete line); global pump settings (match/stats/tags) read from a synchronized snapshot so an unchanged readers blob picks up new GLOBAL match rules; `statePath` updated on reload; journald multi-line values use the native binary field form (8-byte LE length prefix); logging made level-aware (Python PRIORITY mapping, `log_level` at startup and on SIGHUP, logger-name context); readerErr buffered ≥1 so a zero-reader start still delivers the first reader failure. Construct-before-retire safe abort, the delivery-checkpoint contract and the io.Writer-sink logging architecture recorded in §2. Round 2 FIXED: file-sender truncation uses initialAppendOffset + run-relative boundary (reopen-retire preserves pre-existing lines) with valid-JSONL discard/reopen/replay and partial-write-then-fail tests; reload refuses to start a replacement writer when CloseDiscarding fails to restore the boundary (errors propagated); statsd split corrected both ways (sender init + secret-filter → reader-captured client, gauge refresh → live pump client); all Python error/warning call sites routed through severity methods (real send-failure and state-save-failure tests); per-component logger names (JournalReader:<name>, LogSender:<reader>, JournalObjectHandler) and the multi-thread journal format, with the thread-name value adjudicated in the parity doc. Round 3 FIXED in 8405ea7: failed retirement is a PERSISTENT cleanup-failure state (failed entries stay registered, so a later changed-ctime reload retries and still refuses instead of starting a replacement writer on the unrepaired partial line; a successful retry releases them and the file sender keeps its handle open across a failed discard so the retry can truncate), with second-reload-refusal and repair-path tests; the stats-split test exercises BOTH captured-client paths after the statsd-only reload; CorruptedEntry routes through Warningf so the "Bad message" warning survives a WARNING threshold. Round 4 FIXED in e69e897: explicit pending-boundary-repair state on the file sender — Finalize (SIGTERM/shutdown) and Close retry the truncation, and Close refuses to release an unrepaired handle, so a failed reload cleanup followed by shutdown cannot leave a corrupt prefix for the next restart; the repair tests now write a complete line plus a partial next line and assert the partial bytes are removed, complete lines preserved, and replay valid JSONL. Final verdict: approved | 4 | 2 |
| 18 | Core systest suites (rotation/sigterm Go-native, interop) | done | 12778c1 + 06ffa43 + 3e1e3f4 + d2fbc4b + 29abaf0 + 3e854f0 | approved after 2 review rounds (round 1: evidence-integrity correction + category-1 reader-recreation INVALIDATE defect; round 2: root-cause reader-init fix via RegisterForPoll, non-vacuous reload-cursor test, doc fixes; Milestone A gate). Go delivery evidence = 20 Go-native systest-go tests under -race with a race-instrumented child daemon + 9 Python-harness cases that spawn the Go binary (test_reload 1, test_running_config 7, test_sigterm 1); the 6 test_harness cases are harness/unit and the 10 test_rotation cases run Python in-process. Deferred Go-side harness landed (systest-go/journalpump_process_test.go: TestMain builds the systest binary once and shares it, now WITH -race and propagating child data-race reports through each test's shutdown check; append-only notify queue so WATCHDOG floods cannot block the daemon) plus rotation/running-config/sigterm/reload/state-interop suites. Go-native green: 20 top-level systest-go tests under -race (make systest-go, 137 s, exit 0). EVIDENCE CORRECTED: the previous row implied the Python suites "run against the Go binary". Exact per-case reality — 9 Python cases spawn the Go binary (test_reload.py 1, test_running_config.py 7, test_sigterm.py 1); 6 harness/unit cases (test_harness.py) do not drive Go — they exercise command selection/source discovery, exit-code capture and several different stub binaries; the 10 test_rotation.py cases (7 functions/10 collected) run Python journalpump IN-PROCESS and ignore JOURNALPUMP_BIN, so they are Python-implementation evidence only and are NOT cited as Go delivery. Python sweep with JOURNALPUMP_BIN=build/journalpump: test_harness+test_reload+test_sigterm+test_running_config 15 passed (9 Go-binary + 6 stub); test_rotation 10 passed (in-process Python). Depth added this round: rotation byte boundaries use controlled non-oversized payloads (50→1, 1000→2 messages) and ordered-delivery assertions, observing delivery via the state file's sent.count because the file sender has no mid-run flush; TestReadMessagesHonorsBufferBoundaries (in-process) pins the exact per-drain count and byte boundary; TestGoReloadPreservesDeliveryCursors (journal→file across a changed-reader SIGHUP, no missing/duplicate, correct saved cursor); TestReloadJoinsRetiredWorkers asserts retired reader+sender goroutines join, with negative control TestJoinedWorkerCheckDetectsUnjoined; TestPythonToGoToPythonRoundTrip does a real Python→Go→Python round trip with an intermediate cursor and persisted search counters; TestGoReloadPreservesDeliveryCursors is non-vacuous (keeps initial_position head, forces the changed-reader rebuild by adding a search; blanking the reload cursor makes it fail at 20 vs 10 lines — verified). Two defects fixed: (1) real defect cat 1 — a freshly opened reader's first sd_journal_wait manufactures an INVALIDATE for the open-time file snapshot (probe-confirmed) that Python never observes, and the recreate path preserved it through maxEvent, so stable journals were closed/recreated repeatedly; ROOT-CAUSE fix — SDJournal.Open now calls RegisterForPoll (sd_journal_get_fd), which internally runs sd_journal_process and absorbs the open-time INVALIDATE exactly like python-systemd Reader.fileno() at register_for_poll (journalpump.py:1090-1100,1177-1179), and no first-wait masking remains (a blanket mask was rejected: it also swallowed a GENUINE pre-first-wait rotation, which then never recreated). Recreate still REPLACES pending with APPEND. Regressions: reader TestOpenAbsorbsInitialNotification (first Wait(0) is NOP; fails with event 2 when RegisterForPoll is disabled) and TestPreFirstWaitDeletionReportsInvalidate (deletion before the first wait still reports INVALIDATE); daemon TestRunLoopFirstWaitInvalidateRecreates (fails with invalidated==0 on the blanket-mask code). (2) cosmetic — SIGHUP/SIGTERM handlers were installed only after both startup READY=1 notifications, so a SIGHUP arriving right after the first READY (as the harness sends) raced ahead of handler installation and was dropped — moved before the READY loop, matching Python's ServiceDaemon.__init__ ordering (daemon.py:52-54); no §2 entry. Test-only shim (not operator-visible): pollInterval is a var so the systest build honors JOURNALPUMP_POLL_INTERVAL_MS (production default 1000 ms unchanged; file compiled only under -tags systest) | – | – |
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