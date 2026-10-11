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
5. **socks5-kafka retained** (D4) via franz-go (D5) with a custom SOCKS5
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
8. **Kafka compression** (D5 + thread A8): `kgo.SnappyCompression` —
   not a manual github.com/golang/snappy dependency (that module is for
   websocket app-level compression only). kafka-python used **xerial-
   framed** snappy, which is not automatically byte-identical to a
   client's framing; the kafka task must prove interop with a
   kafka-python consumer against the client on a real broker,
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
  — all rejected at load. Extended (2026-10-10, Task 21 review): a negative
  `logplex_request_timeout` is rejected at load; Python's TimeoutAdapter
  treats a negative number as truthy, passes it through, and requests then
  raises "the timeout cannot be set to a value less than or equal to 0" at
  send time.
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
- Kafka: franz-go send path replaces kafka-python's produce-per-message/
  future/flush dance with one synchronous produce per shared-loop flush
  (`ProduceSync`); the per-future 1 s timeout maps to a client-side
  delivery deadline, NEWS note if user-visible (thread A7). Snappy
  framing verdict RESOLVED (no deviation) and pinned by the Task 22
  kafka-python interop test (`TestKafkaPythonConsumerInterop`).
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
- Logplex transport errors: on a dial/read/timeout failure Python's
  uncaught `requests` exception aborts `get_and_send_messages`, silently
  DROPS the drained batch, and leaves the sender marked connected —
  silent data loss. Go RETURNS the error, so the shared sender loop
  retains the batch, retries it, and marks the sender disconnected,
  following the framework-wide retry semantics the spec mandates.
  Coordinator-ruled 2026-10-10 (Task 21 review) under the §2 "Python is
  incorrect" clause. The HTTP-status behavior is unchanged (a 4xx/5xx is
  still ignored, no retry, batch marked sent). NEWS-recorded (Task 28).
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
- Bounded supervisor join (Task 22, kafka): after cancelling the senders,
  shutdown waits on the sender group for at most `senderJoinHardStop`
  (30 s = the larger of 10 s and one kafka `request_timeout_ms`), then logs
  a WARNING naming any sender still draining and proceeds to
  finalize/state-save/exit rather than blocking indefinitely. This differs
  from the spec §4.3 contract as written (an unbounded `senderGroup.Wait()`)
  and is operator-visible: shutdown may complete with a sender still
  draining, and that sender's persisted cursor stays at the flushed
  boundary — consistent with the delivery-checkpoint contract above (no
  acknowledged batch is advanced past what was flushed). With the
  franz-go client (D5, 2026-10-11) cancellation is genuinely ctx-driven
  and aborts in-flight work, so this bound is retained as DEFENCE IN
  DEPTH rather than a workaround for a library latency floor: it exists
  so a future regression cannot wedge process shutdown. Review round 6
  tightened the bound to hold end to end: a sender still draining at the
  bound is EXCLUDED from the synchronous Finalize/Close cleanup, because
  either can block and would defeat the bound — shutdown logs the
  exclusion, saves state (leaving that sender's cursor at its flushed
  boundary) and lets the process exit (pinned by
  TestShutdownSkipsCleanupForDrainingSender). User-directed
  2026-10-11 (Task 22; basis docs/assess-sender-cancellation.md, then
  docs/assess-kafka-client.md). NEWS-recorded (Task 28).
- Kafka client switch and broker-version floor (Task 22): the kafka sender
  is built on `github.com/twmb/franz-go` (`kgo` + `kadm`) instead of
  IBM/sarama, and drops the reference client's pre-0.10 broker
  version-inference probes. The sender supports brokers 0.10.0+: the API
  version is auto-negotiated by franz-go, or pinned explicitly when
  `kafka_api_version` is set (mapped to `kgo.MaxVersions`; SASL
  PLAIN/SCRAM come from `pkg/sasl`). Pre-0.10 brokers (which the
  sarama-era probe could infer via ListGroups/FindCoordinator fallbacks)
  are no longer probed. Review round 6 makes the floor STRICT AT LOAD: a
  `kafka_api_version` below 0.10.0 is rejected when the sender
  configuration is loaded (constructor error → "sender failed to
  initialize", counted in sender.failed_to_start), and again on every
  reader recreation/reload, rather than being accepted and failing later
  at connect; 0.10.0 itself is accepted. The round-6 fix also restores
  kafka-python's per-broker reconnect backoff (1s→10s, ±20% jitter,
  measured from the attempt start, reset after a complete connection) on
  the kgo dialer, and — because franz-go refuses both `Dialer` and
  `DialTLSConfig` — folds the TLS handshake into that dialer when
  `socks5_proxy` is combined with SSL/SASL_SSL, verifying against the
  BROKER hostname (never the proxy or a resolved IP), so SASL
  credentials can never be sent in plaintext. Review round 7 tightened
  three details: the reconnect history is reset/grown from franz-go's
  COMPLETE-connection outcome (`HookBrokerConnect`, after ApiVersions +
  SASL) rather than a TCP/TLS-only success; each connection attempt —
  connect AND TLS handshake — runs under the dialer's own 10s per-attempt
  deadline, so a broker that accepts TCP but never completes TLS cannot
  stall initialization; and the reconnect interval is scheduled BEFORE it
  grows, so the waits are 1s, 2s, 4s, 8s, 10s. This is operator-visible
  only for a deployment on a pre-0.10 broker; the intended deployment
  (spec §5 / D5: apache/kafka 4.0) is unaffected. User-directed
  2026-10-11 (Task 22; basis docs/assess-kafka-client.md). NEWS-recorded
  (Task 28).
- Elasticsearch/OpenSearch HTTP client (Task 23): the timeout SEMANTICS are
  parity; the section records ONE sanctioned §2 deviation (the default trust
  set) and one fail-closed restriction (a CA directory is refused), both
  below. The timeout is requests' per-request INACTIVITY timeout applied at
  each REQUEST boundary and PHASE (`httpx.InactivityClient`), not a total
  context deadline and not fixed at dial time: a reused keep-alive connection
  carries the CURRENT request's timeout, so a bulk POST's 2× timeout never
  leaks onto a reused ordinary connection (nor an ordinary timeout onto a
  reused bulk connection); the read deadline is armed while CONNECTION
  ESTABLISHMENT is in progress (dial plus TLS handshake) so a peer that
  accepts TCP but never completes the handshake is bounded even though
  `WroteRequest` cannot fire before the handshake finishes; the
  response-read deadline is armed (via an httptrace `WroteRequest` hook) only
  after the request is written, so a progressing upload whose total outlasts
  the timeout is not aborted; the phase is reset for EVERY request attempt
  (via the per-attempt `GotConn`/`WroteRequest` hooks) so a redirected upload
  re-enters the upload phase rather than inheriting the previous attempt's
  read deadline; and timeout/deadline updates are serialized under one mutex
  so a pending background read cannot overwrite a newly installed boundary
  deadline. A slow-but-progressing response is not aborted. A configured non-positive
  `*_timeout` (zero or negative) is REJECTED at load, matching requests
  (`journalpump.util.get_requests_session` installs a `TimeoutAdapter` whose
  fallback IS the configured value, and requests raises for a non-positive
  connect timeout) — this is deliberately NOT logplex's 60 s session default.
  The connect/index-create/delete `stats.unexpected_exception` emissions are
  reproduced (the sender carries the pump statsd client). The raw-HTTP
  plumbing was extracted into the shared `internal/httpx` helper (logplex +
  ES), parameterized by proxy-selection strategy; both senders use the
  requests-compatible selector (`httpx.RequestsProxy`) because both Python
  senders use `requests` — ES through
  `journalpump.util.get_requests_session`, NOT elasticsearch-py/urllib3
  (correcting the initial port's use of `http.ProxyFromEnvironment`). CA
  resolution reproduces requests' `merge_environment_settings` +
  `merge_setting` precedence: REQUESTS_CA_BUNDLE (then CURL_CA_BUNDLE) WINS
  over `session.verify` whenever non-empty — even when `ca` is false, empty,
  or an explicit path — because the ES sender passes NO per-request verify;
  otherwise `ca` applies (absent/true → system roots, empty/false → no
  verification, a path → CA FILE). A CA DIRECTORY is REFUSED at load (fail
  closed, user decision 2026-10-12): requests passes a directory verify to
  OpenSSL as a capath whose entries are selected by the SUBJECT HASH of the
  chain being verified, and reproducing that faithfully is not cheap, so the
  config is rejected with a clear error rather than silently trusting every
  hashed-looking file. The sender implements `io.Closer` so the retire path
  releases its idle connections.
  SANCTIONED §2 DEVIATION (user decision 2026-10-12) — DEFAULT TRUST SET: with
  `ca` absent and no `*_CA_BUNDLE`, Go uses `x509.SystemCertPool`, which loads
  certificate DIRECTORIES and honors `SSL_CERT_FILE`/`SSL_CERT_DIR`, whereas
  requests uses certifi's single bundle. The two trust sets coincide in the
  reference RPM deployment ONLY in the absence of additional trust inputs —
  no `SSL_CERT_FILE`/`SSL_CERT_DIR` and no extra system certificate
  directories: the RPM `python3-certifi`'s `certifi.where()` is
  `/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem`, which Go's system pool
  also reads via `/etc/pki/tls/certs/ca-bundle.crt`, a symlink to it. Any
  `SSL_CERT_FILE`/`SSL_CERT_DIR` override, or the extra system certificate
  directories Go's pool loads, contributes roots the certifi bundle lacks;
  and other deployments (deb, pip) can differ even without those. The user
  approved keeping Go's system-store behavior and documenting the difference
  rather than restricting it.
- Websocket sender (Task 24): batch SLICING uses the LogSender base defaults
  (30-byte overhead, 1 MiB limit) for ALL configurations; the configured
  `max_batch_size` changes only batching (the NUL-join) and the
  `Journalpump-Batch-Size` header, never the slicing limit. This corrects an
  earlier plan/spec claim that the sender overrides the overhead to 1 byte:
  `websocket.py:73` sets `batch_message_overhead = 1` on the
  `WebsocketRunner` Thread, which never slices and whose attribute is never
  read, while the `WebsocketSender` LogSender that slices
  (`base.py:262,266`) keeps `base.py:130-131` (30 / 1 MiB). A runtime probe
  (instantiate `WebsocketSender` with `max_batch_size=2048`) reports slicing
  overhead 30 and slicing max_batch_size 1048576. Coordinator ruling
  2026-10-12: match Python, so Go uses 30 / 1 MiB and the plan/spec are
  corrected. The websocket config is also validated STRICTLY AT LOAD (missing
  `websocket_uri`, unknown `compression`/`websocket_compression` enum, `ssl`
  without the ca/keyfile/certfile triple → one constructor error), whereas
  Python reads the keys lazily inside `send_messages` and retries forever on a
  bad value; same class as the Task 22/23 strict-at-load decisions, kept per
  the coordinator ruling 2026-10-12 (recorded here, not as a new escalation).
  A review round (2026-10-12) restored further Python behaviors that the
  first cut missed, all now tested: reject `ssl: true` with a `ws://` URI
  (gorilla ignores `TLSClientConfig` there and would send PLAINTEXT; Python
  raises); send HTTP Basic for `websocket_uri` userinfo and strip it from
  the dial URL (gorilla rejects userinfo); follow bounded handshake
  redirects with Python's TLS-downgrade + preconnected-socket restrictions;
  select environment proxies like websockets `proxy=True` (explicit
  `socks5_proxy` still wins); resolve SOCKS5 destinations at the PROXY
  (remote DNS); run an active 20 s ping / 20 s pong heartbeat (gorilla only
  auto-answers); bound the DECOMPRESSED inbound stream at 2 MiB (a
  permessage-deflate frame can expand arbitrarily past `SetReadLimit`);
  await the peer close response within the close deadline; keep the inner
  10 s opening timeout separate from the outer 30 s connection/SOCKS budget;
  reject explicit null compression enums; classify ordinary transport write
  failures so they no longer emit `stats.unexpected_exception`. gorilla's
  `Conn.SetWriteDeadline` cannot interrupt an in-flight write (it only
  stores a field), so a shutdown/reload with a peer that stops reading is
  unblocked by an IRREVERSIBLE abort that closes the raw socket; this is an
  in-library fix, NOT a library-swap escalation.
  A second review round (2026-10-12) hardened the above and fixed remaining
  gaps: the raw-socket abort is now irreversible (a one-shot deadline could
  be re-armed by a concurrent gorilla write, so abort closes the socket;
  a normal stop still runs the closing handshake first and force-closes
  after); the closing handshake is coordinated with the live inbound reader
  so the peer's close response is awaited; the proxy is re-resolved per
  redirect destination (scheme + `no_proxy` can change); urllib's env
  precedence is reproduced (a present-but-empty lowercase var removes the
  uppercase setting; `no_proxy` bypasses only when the whole value is `*`);
  an `https://` proxy is tunneled with its own verified TLS hop (gorilla
  supports only `http://` proxies); the heartbeat requires a MATCHING pong
  within 20 s, so a peer that streams data but never pongs still times out;
  credentials are stripped from EVERY dial URL (a redirect `Location` may
  carry userinfo); and the dial is phased — preconnect under the outer 30 s
  budget, then the opening handshake under the inner 10 s `open_timeout`.
  A third review round (2026-10-12) closed the remaining races and gaps:
  shutdown is now bounded INDEPENDENTLY of `writeFrames` (a watchdog
  goroutine irreversibly aborts the connection after the close deadline even
  while a frame write is blocked, since gorilla re-arms a one-shot deadline);
  the ENTIRE CONNECT tunnel exchange is bounded (explicit socket deadline
  `min(now+connectTimeout, ctx deadline)` plus close-on-context-cancellation,
  and `HandshakeContext` for the TLS hop), so a proxy withholding its CONNECT
  response cannot block past the opening deadline or a runner stop; CONNECT
  carries a Python-compatible Basic `Proxy-Authorization` from the proxy URL
  userinfo, sent only to the proxy; the phase separation now matches Python
  (ONE `open_timeout` deadline across ordinary establishment AND the whole
  redirect sequence, with only an EXPLICIT `socks5_proxy` preconnection under
  the outer `connectTimeout`); and the `no_proxy` wildcard is compared as the
  COMPLETE value (a padded `" * "` does not bypass an ordinary host). These
  are all parity restorations, not deviations.
  A closing review round (2026-10-12) closed the stop/dial race: connection
  publication is now synchronized with stopping (a `stopped` flag set and
  `stopCh` closed under the runner mutex alongside reading the live
  connection), so a dial that COMPLETES after `stop()` is rejected and its
  socket closed instead of being served without the shutdown watchdog;
  otherwise `serve`/`submit` could select a ready send under a closed
  `stopCh` and block `Close`/reload unbounded. The explicit-SOCKS phase test
  was also rewritten to drive `runner.dial` (the code that chooses each
  phase's budget) through a forwarding SOCKS5 proxy, so it fails if
  `openTimeout` were ever applied to explicit-SOCKS establishment.

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
- **D5 Kafka library**: github.com/twmb/franz-go (`kgo` + `kadm`) —
  revised 2026-10-11 from IBM/sarama by user decision (basis
  docs/assess-kafka-client.md).
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
  (official Elastic tooling, Google Cloud Logging fakes; for kafka,
  franz-go ships no mocks package, so a small `producer`/`admin` seam
  with hand-written fakes);
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
      configured max + 30 B / 1 MiB slicing base defaults; kafka fixed `kafka_msg_key`
      + snappy compression + one-SendMessages-per-batch ack-then-advance;
      CloudWatch sequence-token protocol; GCL 250 KiB quota replacement
      entries.
- [ ] Backoff (0.5 s→1800 s jittered, reset on connect), 1 MiB batching
      (+30 B for all senders incl. websocket), ≤8-deduped-128-char health
      ring, and `max_send_interval` defaults per sender match
      `senders/base.py`.
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
| 19 | Syslog framing package | done | f4620b5 + a7de16a + 1b7c62c + bd1fc4d | approved after 2 review rounds. Round 1 (4 majors + 3 minors) fixed in 1b7c62c: retry classification restored to Python-exact `_should_retry` (EPIPE/ECONNRESET/ETIMEDOUT only; SSLEOFError is an OSError subclass whose errno is not in the tuple, so Python does not retry it — the suspected Python bug is reproduced, not "fixed", per the coordinator ruling); CUSTOM %structured-data% sd=None renders "None"; max_msg negatives preserved as Python negative slice indices (only 0 → 2048); UTF-8 encoding matches s.encode("utf-8","replace") so each invalid byte becomes one '?' BEFORE framing; TLS handshake completed inside NewClient; context.Context threaded through NewClient/Send/Log. Round 2 (2 majors + 3 minors) fixed in bd1fc4d: AfterFunc callback synchronized with write completion and the connection whose deadline the callback forced on a completed write is retired (deterministic cancellation-inside-Write test, no sleep); max_msg == math.MinInt overflow fixed via saturating maxMsgMinusOne (+ int64-extreme cases and a -(2**63) oracle case); the fuzz oracle now computes the expectation INDEPENDENTLY of production pySliceEnd (own slice logic with zero normalization and int64 extremes) while keeping the octet-length desync check; the cancellable-worker test signals Write entry instead of sleeping. GATE-INTEGRITY finding: systest/test_groundtruth_replay.py (a Task 11 consumer of the file_sender fixture) went red when d51bb7d (Task 16) expanded that fixture 10→40 lines but left a literal [:10] comparison — undetected because no consumer suite of the changed fixture was in the per-task green runs; recorded in the parity doc with the corrective that milestone/full-suite runs must include ALL consumer suites of a changed fixture. Oracle fixture 52 cases; test/test_rsyslog_groundtruth.py is the committed reproducible capture harness (GROUNDTRUTH_UPDATE=1). Non-vacuity proven for the retirement, overflow, UTF-8, custom-SD and negative-slicing fixes. Gate: make unittest-go, make lint-go (0 issues), make systest-go green; JOURNALPUMP_BIN=go pytest systest/ 38 passed; pytest test/ 159 passed; fuzzer 15 s clean | 2 | 0 |
| 20 | Rsyslog sender (+ ported rsyslog systest driver) | done | 7fe5764 + d666fb2 + e611cde + 726a878 + b7231b2 + 9bc493a | Internal/sender/rsyslog ports journalpump/senders/rsyslog.py on the Task 19 syslogframing client: payload mapping (facility list-first-element fallback, unguarded PRIORITY int(), raw timestamp[:26]+"Z" with strftime fallback + warning, app-name key chain, PID truthiness), client drop on send failure. Journal payloads decode with json UseNumber so integer fields keep integer identity (numeric PID 100 renders "100", not "100.0"; a float keeps its fraction). Framework seam: per-sender classifier installed (RSYSLOG_CONN_ERRORS retriable, else Python's generic-except path); connection transitions and backoff are owned by the shared Task 13 loop (retriable connect failure → warning + report the original error to the loop's mark_disconnected + flat 5 s sleep + RETURN the error, so health-errors and the ONE shared backoff counter are the loop's — no sender-private exponent; round 2 moved the error report before the sleep, see below). ca_certs presence is preserved (CACertsPresent): an explicitly empty ca_certs engages TLS like Python's `cacerts is not None`, never silently plaintext. ConstructorArgs gains the sender logger (base.py:119 LogSender:<reader>) populated by initSenders; rsyslog.New registered in the wiring map. Ground truth: test/test_rsyslog_sender_groundtruth.py drives the real Python RsyslogSender against a recording TCP server and commits wire bytes to testdata/wire_groundtruth.json (8 cases incl. numeric PID/fields, GROUNDTRUTH_UPDATE=1; explicit timestamps); wire_groundtruth_test.go replays them byte-exact; the 6 Python unit tests are ported plus field mapping/structured-data/numeric-identity/CA-presence/reconnect-cycle/classifier. systest/test_rsyslog.py ported off in-process JournalPump onto the Task 10 harness (process-spawn + journald injection, both implementations). CORRECTION: Python does NOT open the journal before READY (JournalReader.__init__ leaves journald_reader None; update_status opens it inside run after both READY notifications); READY precedes reader-open in both implementations. The real Go divergence was an extra full poll-interval delay before the first open (Python's poll_timeout_ms starts at 0.0); fixed in ReaderRuntime.run (first iteration polls immediately), guarded by TestRunLoopOpensReaderWithoutInitialPollDelay. A strict open-before-READY guarantee would be a separate lifecycle improvement requiring adjudication, NOT reproduction of Python, and was not done. The driver's bounded readiness probe replaces journalpump_initialized() and would also mask a returning delay, so the daemon regression guard is the authoritative check. Round 2 (2 majors + 1 minor) fixed in b7231b2: (a) a successful connect now reports connected through a new OPTIONAL transport seam sender.ConnectionReporter (SetConnectionObserver, detected in NewLoop like DurableReporter) so the loop's MarkConnected resets the shared backoff exponent — previously the reset waited for a whole successful batch, so repeated payload failures separated by successful reconnects escalated toward the 1800s cap where Python's mark_connected resets on connect; a REAL sender.Loop integration test (TestLoopReconnectResetsSharedBackoff, synctest fake clock) requires >=8 attempts in 10s and drops to ~4 without the wiring. (b) the retriable connect failure is reported to the shared health ring BEFORE the flat 5s wait (Python marks disconnected then sleeps), so the original error is recorded immediately and a cancellation during the wait no longer replaces it with context.Canceled; the loop's shared backoff is then applied on top of the flat sleep. (c) a truthy non-string ca_certs (e.g. 1) is now REJECTED instead of being silently mapped to an empty cafile (system roots) while selecting TLS, matching Python's ssl.create_default_context(cafile=<non-string>) raising (the sanctioned wrong-type deviation permits earlier rejection, not silent acceptance). Gate: make unittest-go, make lint-go (0 issues), make systest-go green; JOURNALPUMP_BIN=go pytest systest/ 38 passed; pytest test/ 160 passed; pytest systest/test_rsyslog.py 5 passed for both binaries. APPROVED after 2 review rounds; integration business: the ConnectionReporter seam + rebooted backoff reset on connect, ca_certs presence semantics, numeric-PID identity. | – | – |
| 21 | Logplex sender | done | d951dd6 + 2d2cf90 + 0ba179b + 84fe918 + a3b7edf + d539a59 + efd197e + e130ea8 + 0d7256a + 639eba4 + d6aa501 + c2bada0 + 031491a + f8cc866 | Internal/sender/logplex ports journalpump/senders/logplex.py and is registered as output_type logplex. Fire-and-forget HTTP POST: format the batch into length-prefixed frames, POST once with Basic auth ("token"/token), Content-Type application/logplex-1 and Logplex-Msg-Count = batch length, and IGNORE the response status (no raise_for_status; the batch is always marked sent; a 500 must not retry or disconnect). Frame: "<190>1 <format-now> localhost <token> localhost - -<MESSAGE>", length-prefixed; PRI fixed 190; msgid/structured-data "-"; timestamp is the FORMAT time (now(UTC), not the journal timestamp) in %Y-%m-%dT%H:%M:%S+00:00 shape; the named quirk-lock: _HOSTNAME/_PID lookups never match because the journal processor strips leading underscores (journalpump.py:708-714), so hostname/pid are the literal "localhost". New requires logplex_log_input_url and logplex_token (Python config[...] KeyErrors), defaults max_send_interval 5.0 and logplex_request_timeout 2s, sets ConnectOnInit (mark_connected in __init__), and disables TLS verification (verify=False, deliberate per spec §2). SANCTIONED DEVIATION (charter §2/D6, user decision 2026-10-09): Python's format_msg f-strings the ENCODED packet so the Python wire carries the bytes repr (b'...') after each length prefix (len+3 chars); it is a real Python bug and Go sends the CLEAN RFC frame, so content parity is asserted STRUCTURALLY against the Task 11 fixture's repr-stripped frame list (systest-go/fixtures/logplex.json, GROUNDTRUTH_UPDATE=1), never byte-for-byte against the broken bytes. Tests: TestFormatMessageCleanFrame (no b', length = frame byte length), TestHostnamePIDAlwaysLocalhost, TestPRIAndFieldConstants, TestFormatTimestampIsFormatTime, TestMissingMessageIsAnError, TestSendPostsHeadersAndBody, TestHTTPServerEndToEnd (httptest), Test500DoesNotRetryOrDisconnect (REAL sender.Loop: one request, sent.count 1, health connected), TestNewValidatesRequiredConfig, TestFixtureStructuralParity. Non-vacuous: reproducing the bytes-repr fails the deviation tests. REVIEW ROUND 1 (changes-required: 5 majors + 2 minors, fixed in 84fe918 incl. an unrelated gate-integrity race fix in 0ba179b): (1) transport errors now RETRY — new sanctioned §2 deviation (Go returns the error so the shared loop retains+retries+disconnects; Python's uncaught exception drops the batch and stays green), real sender.Loop regression test TestTransportFailureRetainsAndRetries; (2) SendMessages CONSUMES the response body and propagates truncated/stalled read failures (the HTTP status stays ignored) — TestTruncatedBodyIsAnError + TestStalledBodyTimesOut; (3) the timeout is an INACTIVITY timeout (idleTimeoutConn dial/per-read deadlines, no http.Client.Timeout) with 0 -> 60 s session default (TimeoutAdapter) — TestResolvedTimeoutUsesSessionDefault + TestSlowResponseWithinTimeout + TestIncrementalDeliveryExceedsTotalTimeout; (4) the custom Transport now honors the proxy environment incl. no_proxy (round 2 corrected the selector; see below); (5) pyStr now renders Python str() for extra_field_values-supplied _HOSTNAME/_PID (capitalized bools, Python float repr, integer identity, list/object repr) and the "always localhost" claim is QUALIFIED (journal fields only) — TestHostnamePIDFromExtras; (6) TestFixtureStructuralParity now asserts at the HTTP capture boundary (every captured length-prefixed frame parsed and compared field-by-field against the fixture, rejecting bytes-repr). No new dependency (round 2 removed the transient golang.org/x/net/httpproxy use added in round 1). Separately: internal/filter Search.hits was a plain int64 racing under -race (masked by cached runs; same class as d51bb7d) — now atomic, with a deterministic non-vacuous regression test (0ba179b), recorded as a gate-integrity finding in the parity doc. Gate: make unittest-go, make lint-go (0 issues), make build-go green; go test -race -count=1 -tags systest ./systest-go/... green (138.1s); JOURNALPUMP_BIN=go pytest systest/ 38 passed; pytest test/ 160 passed; pytest systest/test_groundtruth_logplex.py both modes green, no fixture drift. REVIEW ROUND 2 (changes-required: 2 majors + 2 minors, final round): (1) ONLY a zero logplex_request_timeout now maps to the 60 s session default; a negative value is truthy in Python (TimeoutAdapter passes it to requests, which raises "cannot be set to a value less than or equal to 0") and is now REJECTED at load (strict-at-load deviation extended) — TestNegativeTimeoutRejected, TestResolvedTimeoutUsesSessionDefault corrected; (2) the proxy selector was rewritten to requests' own rules, replacing net/http.ProxyFromEnvironment/httpproxy (which gives uppercase precedence, ignores ALL_PROXY, and unconditionally bypasses loopback): lowercase *_proxy wins (empty lowercase removes), lowercase-first no_proxy with uppercase fallback, ALL_PROXY scheme fallback, and loopback NOT special-cased — proxyForRequest/shouldBypassProxies/proxyEnvironment; TestProxySelectionMatchesRequests + TestNoProxySelection (conflicting-case precedence, ALL_PROXY fallback), and the vacuous loopback NO_PROXY test replaced by TestProxyUsedForLoopbackTarget (non-vacuous loopback control) + TestNoProxyBypass (non-loopback target, proxy unused) — no new dependency; (3) pyRepr now matches Python repr exactly (double-quote selection when the string contains ' and not ", backslash escaping of the chosen quote/backslash, \t \n \r and \xNN for other control chars incl. NUL/DEL) — TestPyStrMatchesPython with apostrophe/control-character extras cases; (4) TestFixtureStructuralParity now compares hostname/PID/token/msgid/sd directly against the fixture's parsed fields and asserts the captured method/path/fixture.Request.Headers, with the constant-field quirk-locks kept separate. Gate: make unittest-go, make lint-go (0 issues), make build-go green; go test -race -count=1 -tags systest ./systest-go/... green; JOURNALPUMP_BIN=go pytest systest/ 38 passed; pytest test/ 160 passed; pytest systest/test_groundtruth_logplex.py both modes green. REVIEW ROUND 3 (changes-required: 2 majors + 1 minor, final round): (1) the no_proxy matching was completed to the FULL requests/urllib semantics — the Linux proxy_bypass fallback is NOT a no-op (requests' set_environ(..., None) leaves the environment untouched), so shouldBypassProxies now OR-s requests' own loop (plain suffix on host and host:port for hostnames; CIDR/plain-IP for IP hosts) with urllib's proxy_bypass_environment (NO_PROXY=* wildcard, case-insensitive, leading-dot tolerance and .name label-boundary suffixes) — TestNoProxySelection covers wildcard/mixed-case/leading-dot/label-boundary/port/CIDR; NOTE: requests' own loop is a plain suffix with NO label boundary, so no_proxy=example.com also bypasses notexample.com (probe-verified against requests 2.33.1) and Python parity is kept rather than silently adopting the stricter rule the review asked for (that would be an unsanctioned, operator-visible deviation); (2) scheme-less proxy addresses (HTTP_PROXY=proxy.example:8080) are normalized with requests' prepend_scheme_if_needed (http:// default) plus host validation (parseProxyURL) — TestParseProxyURL + TestProxyConfiguredWithoutScheme; (3) pyRepr completed Python's Unicode printability + escape-width rules (nonprintable per Cc/Cf/Co/Cs/Zl/Zp/Zs-except-space, agreeing with unicode.IsPrint; \xNN <=U+00FF, \uNNNN <=U+FFFF, \UXXXXXXXX above; printable non-ASCII literal) — TestPyStrMatchesPython latin-1/BMP/astral cases. Gate: make unittest-go, make lint-go (0 issues), make build-go green; go test -race -count=1 -tags systest ./systest-go/... green; JOURNALPUMP_BIN=go pytest systest/ 38 passed; pytest test/ 160 passed; pytest systest/test_groundtruth_logplex.py both modes green. REVIEW ROUND 4 (changes-required: 1 major, fixed in e130ea8's successor): the two no_proxy layers were conflated — requests' first pass is CASE-SENSITIVE plain suffix (raw entry, no label boundary; CIDR/plain-IP for IP hosts) only when get_proxy("no_proxy") is non-empty, while the urllib fallback is case-insensitive WITH label boundaries and draws its list from getproxies_environment()["no"] (empty lowercase removes the entry). Fixed shouldBypassProxies to keep the layers separate with their own sources; negative controls added — no_proxy=Example.Com does NOT bypass notexample.com (case-sensitive first pass misses; label-boundary fallback misses), and no_proxy="" + NO_PROXY="*" does NOT bypass-all (uppercase removed), while the lowercase plain-suffix quirk (no_proxy=example.com DOES bypass notexample.com) stays pinned. Gate: make unittest-go, make lint-go (0 issues), make build-go green; go test -race -count=1 -tags systest ./systest-go/... green; JOURNALPUMP_BIN=go pytest systest/ 38 passed; pytest test/ 160 passed; pytest systest/test_groundtruth_logplex.py both modes green. REVIEW ROUND 5 (changes-required: 1 minor): the urllib fallback used requests-layer entry preprocessing — Python strips leading/trailing whitespace (space, \t, \n, \r, \f, \v) from each comma-split fallback entry, while requests' first layer removes only ASCII spaces; splitNoProxyEntries therefore missed no_proxy="\tExample.Com\t" (bypasses example.com in Python). Fixed with an independent splitFallbackEntries using TrimSpace for layer 2, first layer unchanged; tab-padded mixed-case regression added. Gate-INTEGRITY (separate commit 031491a): TestGoRotationDeletedFileFdRelease was flaky (~1 in 5 runs) — it checked fds for a just-deleted file immediately, racing the asynchronous close; waitFdTargetGone now polls (bounded 10 s); recorded in the parity doc, unrelated to Task 21. Gate: make unittest-go, make lint-go (0 issues), make build-go green; go test -race -count=1 -tags systest ./systest-go/... green (138.4s); JOURNALPUMP_BIN=go pytest systest/ 38 passed; pytest test/ 160 passed; pytest systest/test_groundtruth_logplex.py both modes green. APPROVED after 5 review rounds; logplex HTTP-client parity across retry/body/timeout/proxy/repr plus the transport-error §2 deviation; gate-integrity fixes: 0ba179b (search-race), 031491a (flaky fd). | – | – |
| 22 | Kafka sender (franz-go kgo SyncProducer, CompressionSnappy, interop proof) | done | 37ab063 + ce6e90f + 8be8586 + da1620e + 9a64eb6 + aba4360 + 537cc33 + 0a3f7ed + 68ac82b + c4e7973 + 7ff7773 + fcb0802 + 4e2ab6a + 9cade74 + 0d5e4a0 + 58fc844 + 6f9cb46 + 51b56a3 + 0b73164 + e2e6769 + 8e66a9f + 3f59c0d + 643f6ac + a2a7e96 | internal/sender/kafka ports journalpump/senders/kafka_sender.py on IBM/sarama and is registered as output_type kafka. Config mapping parity (kafka_topic required; security_protocol validation + ssl derivation; SSL/SASL material; SASL mechanism PLAIN/SCRAM-SHA-256/512 with the username+password requirement; linger_ms 500; snappy pin; socks5_proxy via golang.org/x/net/proxy — D4 retained). Send path: ONE sarama SyncProducer.SendMessages per shared-loop flush (thread A7; ack-then-advance) replacing kafka-python's produce-per-message + flush + future.get(timeout=1); RequiredAcks WaitForLocal (kafka-python default acks=1 — coordinator's "all/-1" note was wrong, confirmed by the coordinator); connect-inside-send reports via the ConnectionReporter seam; dropProducer on failure re-inits next send; topic auto-create via ClusterAdmin only when BOTH kafka_topic_config values present, TopicAlreadyExists tolerated. NOT a DurableReporter (cursor advances on broker ack — the file sender alone is durable). Compression pinned CompressionSnappy to the deployed codec (zstd-preference fact recorded, not reproduced). Review round 1 (verdict changes-required: 14 majors + 2 minors) all fixed: (A1) partitioner is a port of kafka-python DefaultPartitioner's murmur2 (vector-pinned), replacing sarama ReferenceHashPartitioner whose algorithm/seed routes a fixed key differently; (A2) sarama.ProducerErrors classified per-cause, retriable only when EVERY cause is, causes preserved; (A3) retriable set derived from kafka-python 2.3.2 — BrokerNotAvailable/RebalanceInProgress NOT retriable; ReplicaNotAvailable/NotController/ListenerNotFound/ThrottlingQuotaExceeded and numeric codes 100/103/106/122/123 ARE; (B4) ssl truthiness (1/"true"/"yes" enable SSL) and malformed ca/certfile/keyfile/sasl_plain_* rejected, never silently downgraded; (B5) SCRAM-SHA-256/512 wired to working xdg-go/scram generators (validation + full auth exchange tested); (B6) kafka_address accepts string/list, host-without-port (9092) and IPv6 forms; (C7) connect, topic-create and send are cancellation-aware (runCtx + drain-close, no leak) with stalled-broker tests; (C8) socks5 resolves locally / socks5h remotely (asserted at the proxy) with a bounded connect+handshake; (C9) no sarama global Logger write (default is already discarding), -race concurrent-construction test; (D10) kafka_api_version accepts "auto"/"2.5"/"0.10.2"; (D11) unset version auto-detected from the broker ApiVersions response via kafka-python's decision list (sarama's ApiVersionsRequest is fire-and-forget and would keep the 2.1.0 default); (D12) reconnect_backoff_ms 1s / reconnect_backoff_max_ms 10s (BackoffFunc), delivery_timeout_ms 120s, retry_backoff_ms 100ms, request_timeout_ms 30s; (E13) present-but-invalid kafka_topic_config values rejected, not skipped/truncated/narrowed; (E14) send path tested through the REAL sender.Loop (exact ordered bytes, delayed ack, retriable batch retry + teardown, mixed-cause whole-batch resend with TopicAlreadyExists re-init); (E15) committed kafka-python-consumer interop regression (systest-go/kafka_python_consumer.py, exact fixture bytes) + hardened e2e (bounded SIGTERM success, race-detector-output rejection, exact fixture partition count). Review round 2 (verdict changes-required: 1 blocker + 7 majors + 2 minors) all fixed: (1, blocker) Producer.Retry.Max bounded to 20 (producerRetryMax) — sarama allocates Retry.Max+1 partitionRetryState entries per partition on the first message, so the previous 2147483647 would allocate ~2^31 entries (~tens of GiB/partition) before delivery; the unbounded behavior comes from the shared loop's outer retry plus (2) a real client-side delivery_timeout_ms deadline in sendBatch (Producer.Timeout is the broker-side acks timeout and bounds neither retries nor SendMessages); (3) cancellation now aborts the in-flight network op and JOINS the worker, and Close is bounded by closeTimeout (5s); (4) the version probe runs with SASL disabled and a detected/configured pre-1.0 version falls back to SASLHandshake v0 (sarama authenticates before the explicit ApiVersions call, so PLAIN and both SCRAM failed whenever kafka_api_version was unset); (5) kafka-python's pre-0.10 legacy probes implemented and an error-carrying ApiVersions response is not inferred from; (6) per-broker reconnect backoff implemented at the connection layer (backoffDialer: exponential 1s->10s, ±20% jitter, reset on success, per-address) instead of only pacing metadata retries; (7) failed batches return a sendErrors aggregate that renders and unwraps every cause so auth/timeout causes reach the loop health ring; (8, minor) one cancellable deadline spans SOCKS5 local resolution + proxy connect + negotiation; (9, minor) loop tests inspect state WHILE an ack is pending and DURING retry (held-ack gate + parked reconnect) and require exactly two admin init attempts; (10, minor) reproducible broker provisioning via scripts/kafka-e2e-go / make systest-go-kafka, and the docs no longer claim CI runs the interop (that CI gate is plan Task 30) — Review round 3 (verdict changes-required: 4 majors + 2 minors; theme: the cancellation/deadline machinery must be genuinely ABORTIVE) all addressed: (1) sendBatch terminates the in-flight produce AND its retries (closing every tracked connection) BEFORE joining the worker and closing the producer; the live producer/admin expose abort() (sendErrors unchanged); pinned against a REAL sarama SyncProducer whose broker answers metadata but never Produce (TestSendDeliveryDeadlineAbortsRealStalledProduce, TestSendCancellationAbortsRealStalledProduce) — the round-2 stallProducer whose Close released the send did not exercise this and is removed; (2) the producer/admin factories take a context and construction (version probes, metadata, DNS, SOCKS) runs under a tracking dialer force-closed when it ends, so cancellation no longer waits for each inner timeout to accumulate — TestConnectProducerCancellationTerminatesMetadata cancels a blocked metadata handshake WITHOUT releasing the broker by hand; (3) Close aborts outstanding work and JOINS the worker it owns before returning (the join is bounded by the sarama retry floor, not a deadline); the closeWait/closeTimeout 5s timer and its abandoned closing goroutine are removed (TestCloseAbortsAndJoinsWithoutTimer); (4) reconnect timing matches kafka-python — per-address state stores the next allowed dial measured from the ATTEMPT START (kafka-python sets last_attempt at the top of connect()), so a slow dial that already exceeded the interval leaves no wait and a later attempt waits only the REMAINING interval, the history resets only after a COMPLETE connection succeeds (markConnected, after NewClient: TCP/TLS/SASL/metadata) rather than on a TCP-only success, and post-connect failures are recorded (markFailed); the double-backoff Metadata.Retry.BackoffFunc is removed and Metadata.Retry.Backoff is the 100ms retry_backoff_ms (TestBackoffDialerMeasuresFromAttemptStart, TestBackoffDialerWaitsOnlyRemaining, TestBackoffDialerTCPSuccessDoesNotReset, TestBackoffDialerMarkConnectedResetsAndMarkFailedGrows); (5, minor) Producer.Timeout is the 30s request_timeout_ms that sarama transmits in Produce requests, not the 120s delivery_timeout_ms, which stays a separate client-side deadline (TestSaramaConfigRetryTimeouts asserts both independently, TestDeliveryDeadlineIsClientSideAndDistinct); (6, minor, evidence accuracy) the original contract description in docs/go-test-parity.md was corrected (metadata backoff vs reconnect backoff; Producer.Timeout vs the delivery deadline; Producer.Retry.Max bounded — producerRetryMax) and the unsupported completion claims about cancellation/graceful-close/uncancellable-construction were removed and replaced by the production-path regression tests above. Recorded execution: make systest-go-kafka green against a podman apache/kafka:4.0.0 KRaft broker (TestKafkaSenderEndToEnd auto-create + manual-create-keyed, TestKafkaPythonConsumerInterop PASS); make unittest-go / make lint-go (0 issues) / make systest-go green; JOURNALPUMP_BIN=go pytest systest/ 38 passed; pytest test/ 160 passed. Ledger row stays review pending a verdict. Review round 4 (direction changed by the user: STOP the escalation; basis docs/assess-sender-cancellation.md — the residual sarama sleep/retry tail is a LIBRARY limitation, not a defect to engineer around): (1) the supervisor join is BOUNDED — pump.go's unbounded senderGroup.Wait() after cancel is replaced by waitSendersBounded, which waits at most senderJoinHardStop (30s = max(10s, one request_timeout_ms)), then logs a WARNING naming the still-draining senders and proceeds to finalize/state-save/exit; recorded as a sanctioned §2 deviation (operator-visible; the draining sender's persisted cursor stays at the flushed boundary), pinned by TestWaitSendersBoundedProceedsAfterHardStop + TestWaitSendersBoundedJoinsCleanlyWhenSendersFinish (both under -race); the unbounded join was the real self-inflicted contributor; (2) the kafka machinery was TRIMMED not extended — watchCancel is now context.AfterFunc (stdlib idiom, as in rsyslog), the tracking dialer owns a cancellable lifetime so closeAll also cancels an in-flight DIAL, and the Net.Proxy.Enable dialer-hook abuse is documented at the call site (bypasses Net.DialTimeout/KeepAlive/LocalAddr, logs "using proxy"); producerRetryMax stays 20 (lowering to 3-5 would shrink the ~3s floor but changes leader-election behavior — recorded as an open operator decision, not slipped in); no further abort layers added; (3) the kafka shutdown/cancellation latency floor is documented in docs/go-test-parity.md as a SARAMA LIMITATION (no context support; uninterruptible retry sleeps; ~Retry.Max×Backoff ≈ 3s; tests assert ≤ 5s; the bounded supervisor join is what keeps shutdown bounded) and the unsupported "fails at once" claim was removed; (4) the round-3 retryMax value is corrected to 20 everywhere and the Close/join lifecycle claims are qualified, and the reconnect interval is now measured from attempt START per kafka-python's last_attempt (TestBackoffDialerMeasuresFromAttemptStart). Task 22 stays review for the confirmation review. recorded execution: the full systest-go suite passed with TestKafkaSenderEndToEnd + TestKafkaPythonConsumerInterop PASS against a podman apache/kafka:4.0.0 KRaft broker. Tests: kafka package config/send-path cases porting test/unit/senders/test_kafka_sender.py + sarama-mocks send tests behind a producer/admin seam (thread B) + loop_test.go through the shared loop; opt-in systest-go tests (skip without JOURNALPUMP_KAFKA_BROKER) assert the Task 12 fixture payload/key/partition/sent ground truth and the kafka-python interop. snappy xerial-framing interop (spec risk 10): RESOLVED no-deviation, now pinned by TestKafkaPythonConsumerInterop. Review round 5 (user-directed client switch 2026-10-11; basis docs/assess-kafka-client.md): IBM/sarama REPLACED by github.com/twmb/franz-go (kgo + kadm) — a direct rewrite, no spike; D5 revised. Framework contract unchanged (sender.Sender / SendMessages(ctx, …) / sender.Loop untouched; kafka specifics stay in internal/sender/kafka). DELETED sarama workarounds: the tracking dialer + force-close machinery (dialer.go, abort()), the version-inference probe and the SASL-off bootstrap probe, scram.go (SASL PLAIN/SCRAM now from pkg/sasl), and the sarama KError retriable table. franz-go handles NATIVELY: context-aware dials/produce (ProduceSync)/kadm admin; API-version negotiation (kgo.MaxVersions; auto by default) plus SASL; structured kerr errors (kadm.CreateTopic TopicAlreadyExists; Retriable maps kerr Code through an explicit numeric table keeping kafka-python KAFKA_CONN_ERRORS parity — BrokerNotAvailable=8 NOT retriable). Still added: doWithClose (the documented franz-go lever — cl.Ping(ctx) does NOT abort an in-flight request to a broker that accepted the TCP conn but never answers; doWithClose calls cl.Close() on ctx.Done then joins, used for metadata + admin), the client-side delivery deadline distinct from ProduceRequestTimeout, the murmur2 partitioner on the kgo Partitioner interfaces, the custom socks5h dialer (kgo.Dialer), and RetryBackoffFn (reconnect_backoff_ms; the per-address jitter tracker is not carried over since franz-go re-dials internally). kgo opts: LeaderAck + DisableIdempotentWrite (acks=1), ProducerLinger 500ms, SnappyCompression, AllowAutoTopicCreation (franz-go default OFF, sarama default ON — needed by the Task 12 auto-create fixture), ProduceRequestTimeout 30s, RecordDeliveryTimeout 120s, RecordRetries 20. The bounded supervisor join (waitSendersBounded) is KEPT as defence in depth, not a latency-floor workaround. Operator-visible deviation recorded in §2: dropping pre-0.10 broker version inference — supported brokers 0.10.0+ (auto-negotiated, or pinned via kafka_api_version → kgo.MaxVersions); intended deployment apache/kafka 4.0 unaffected. Recorded execution (all UNCACHED): go test -race -count=1 ./... PASS; golangci-lint run 0 issues; JOURNALPUMP_BIN=go pytest systest/ 38 passed; pytest test/ 160 passed; make systest-go-kafka (podman apache/kafka:4.0.0 KRaft, -race -count=1 -tags systest -v) PASS — TestKafkaSenderEndToEnd (auto-create + manual-create-keyed) + TestKafkaPythonConsumerInterop. Review round 6 (review of the round-5 rewrite; verdict changes-required, 5 majors) all fixed: (1, SECURITY) socks5_proxy + SSL/SASL_SSL folded the TLS handshake INTO the custom kgo dialer (dial.go), because franz-go refuses both Dialer and DialTLSConfig — TLS is no longer bypassed and SASL credentials cannot go plaintext; ServerName is the BROKER host (never the proxy or a resolved IP), pinned by dial_test.go (handshake happens; a DNS-only leaf verifies for broker.test and FAILS for an IP ServerName); (2) kafka-python per-broker reconnect backoff RESTORED on the kgo dialer (reconnect.go: 1s->10s, +-20% jitter, measured from the attempt START, reset after a complete connection), pinned by reconnect_test.go; (3) the 0.10.0 floor is STRICT AT LOAD — New rejects kafka_api_version < 0.10.0 (and non-string) at configuration load and the daemon rejects it again on reader recreation/reload (TestNewRejectsPreKafka010AtLoad, TestSenderKafkaPre010RejectedAtLoadAndReload); (4) real-client cancellation proven against a fake broker that completes ApiVersions + Metadata negotiation then WITHHOLDS Produce/CreateTopics responses — TestRealProducerCancellationAbortsStalledProduce (real sender.Loop + real Sender, bounded stop, sent count 0, cursor nil) and TestRealAdminCancellationAbortsCreateTopic (context.Canceled bounded), superseding round3's stalled-produce fake; (5) waitSendersBounded now returns the still-draining sender set and shutdown SKIPS Finalize AND Close for them, so a transport whose send AND close both block cannot wedge shutdown (TestShutdownSkipsCleanupForDrainingSender). Review round 7 (confirmation; verdict changes-required, 2 majors + 1 minor) all fixed: (1) reconnect success/failure accounting moved OFF the dialer onto franz-go's COMPLETE-connection outcome — brokerConnectHook implements kgo.HookBrokerConnect (fires after ApiVersions + any SASL flow, err set on failure; installed via kgo.WithHooks alongside kgo.Dialer), so a TCP/TLS-only success cannot reset the history and post-connect auth/protocol failures grow the interval (TestDialerTCPOnlySuccessDoesNotResetBackoff, TestBrokerConnectHookDrivesCompleteOutcome, and real-client TestRealClientIncompleteConnectionCountsAsFailure / TestRealClientCompleteConnectionResetsBackoff); (2) each connection attempt — connect AND TLS handshake — runs under the dialer's OWN per-attempt deadline (context.WithTimeout, default 10s), so a broker that accepts TCP but never completes TLS cannot stall initialization without a caller deadline (TestTLSHandshakeBoundedWithoutCallerCancel direct + TestTLSHandshakeBoundedThroughProxy); (3) the reconnect interval is scheduled BEFORE it grows (a consecutive-failure count with intervalFor), so the actual waits are 1s, 2s, 4s, 8s, 10s rather than doubling the initial 1s before first use (TestReconnectBackoffActualWaitsGrowFromBase observes the waits on the virtual clock). CONFIRMED APPROVED 2026-10-11 (ledger -> done). Accurate scope: rounds 1-4 delivered the sender on IBM/sarama; the user then directed the client SWITCH to github.com/twmb/franz-go (kgo + kadm; user decision 2026-10-11, spec rev 8) after the sarama cancellation limitation was established (docs/assess-sender-cancellation.md — sarama has no context support, so cancellation was a socket-force-close workaround with a multi-second floor; docs/assess-kafka-client.md — the franz-go assessment). Two §2 sanctioned deviations persist: (a) the bounded supervisor join (waitSendersBounded, senderJoinHardStop 30 s) — shutdown may complete with a sender still draining, which is excluded from synchronous Finalize/Close and left at its flushed cursor; (b) the dropped pre-0.10 broker version inference — supported brokers are 0.10.0+ (rejected below the floor at load), the intended apache/kafka 4.0 deployment unaffected. Handled NATIVELY by franz-go (our sarama workaround deleted): context-aware dials / ProduceSync / kadm admin; per-connection API-version negotiation via kgo.MaxVersions; SASL PLAIN/SCRAM from pkg/sasl; structured kerr classification (kadm.CreateTopic TopicAlreadyExists; our Retriable keeps kafka-python KAFKA_CONN_ERRORS parity — BrokerNotAvailable NOT retriable). Still ADDED on top, stated precisely and not overclaimed: doWithClose — the one surviving franz-go gap, for a broker that accepts TCP but never answers (cl.Ping(ctx) does NOT abort such an in-flight request even after ctx cancel; doWithClose runs the call in a goroutine, calls cl.Close() on ctx.Done, then joins — used for construction/metadata and admin/topic-create); the client-side delivery deadline distinct from ProduceRequestTimeout; the murmur2 partitioner; the custom socks5/socks5h dialer with TLS folded into it and a per-attempt connect+TLS deadline (rounds 6-7); the per-broker reconnect backoff driven from HookBrokerConnect (rounds 6-7); and topic auto-create tolerance (kgo.AllowAutoTopicCreation). | – | – |
| 23 | Elasticsearch/OpenSearch sender | done | 014ddb2 + a5360a3 + 06de3e8 + 2d3a4da + 423ee33 (+ docs 545fc8f/d1dfee2/cd0646d/796f74b) | internal/sender/elasticsearch ports journalpump/senders/elasticsearch_opensearch_sender.py and is registered as output_types elasticsearch and opensearch, sharing one raw-HTTP transport (Python's _EsOsLogSenderBase); the two differ only in the <type>_* config-key prefix, the state-file type name, and the Elasticsearch ≤7 legacy mapping-type quirks. Config: <type>_url (required, trailing "/" stripped), <type>_timeout (60s), <type>_index_prefix (journalpump), <type>_index_days_max (3), max_send_interval (10s), ca (default system CAs / false / PEM path). Wire: version probe GET / once (Version.from_json requires exactly major.minor.patch); alias census GET /_aliases (both cold-path only); daily rollover <prefix>-<timestamp[:10]> (missing/empty timestamp → current UTC date + warning naming "timestamp", so it survives a WARNING threshold); index create PUT <index> with the mapping from the index's FIRST message (fixed SYSTEMD_SESSION/SESSION_ID text fields + bool→boolean/float→float/int→integer/str→text/dict→recursive properties; "timestamp" skipped; null/list dropped with an "Unmapped fields" warning); 200/201 OR already_exists_exception = success, a failing create is logged and does NOT fail the batch (Python continues to the bulk write); ES ≤7 legacy type: PUT <index>?include_type_name=true, action header carries _type: journal_msg, mapping wrapped under journal_msg. Bulk: one POST <latest-index>/_bulk per flush (URL cosmetic — each action header carries its own _index), NDJSON action + raw payload lines, explicit content-length and content-type application/x-ndjson; response status logged but NOT enforced and per-item errors never inspected (quirk-lock). Retention: MaintenanceOperations at most hourly (and on the first call) — GET /_aliases, keep <prefix>-* sorted, DELETE oldest while > index_days_max; non-prefixed indices untouched. Connection-error logging deduped by error class. Integer-vs-float identity from json.Decoder.UseNumber + literal spelling (isIntegerLiteral). Parity (2026-10-11/12 review rounds; ONE sanctioned §2 deviation, the default trust set): the raw-HTTP plumbing lives in the shared internal/httpx helper — proxy selection parameterized per sender, requests-faithful CA env precedence, and per-request phase-aware inactivity deadlines (connect/upload/read, redirect-aware) — now ready for GCL (Task 26). Per-request INACTIVITY timeout applied at each REQUEST boundary (httpx.InactivityClient; bulk 2×; a reused connection uses the current request's timeout), a non-positive <type>_timeout rejected at load (requests rejects it — unlike logplex's real 60s session default), requests' merge_environment_settings CA precedence (REQUESTS_CA_BUNDLE/CURL_CA_BUNDLE win over ca even when false/empty/explicit path), a CA directory path REFUSED at load (fail closed, user 2026-10-12), and the connect/create/delete stats.unexpected_exception emissions reproduced. Timeout is per-REQUEST and per-PHASE (httpx.InactivityClient: connect+handshake and response-read deadlines armed, upload phase not; the phase is reset per request attempt so a redirect re-enters the upload phase; boundary updates serialized under one mutex so a pending read cannot overwrite them). SANCTIONED §2 deviation (user 2026-10-12): the DEFAULT trust set — Go uses x509.SystemCertPool (loads cert DIRECTORIES, honors SSL_CERT_FILE/SSL_CERT_DIR) while requests uses certifi's single bundle; the sets coincide in the reference RPM deployment ONLY when no additional trust inputs (SSL_CERT_FILE/SSL_CERT_DIR, extra system certificate directories) contribute roots, and can differ for deb/pip. Tests: package-level httptest suite porting test/test_elasticsearch_opensearch_sender.py and the ES/OS cases in test/test_journalpump.py (config defaults/custom, request URL, missing URL, max_send_interval + type name, header/mapping with/without the legacy type, ES index URL, send with/without timestamp), plus the quirk-locks (bulk 500 still succeeds; already-exists tolerated; create failure continues) and TestFixtureStructuralParity, which sends each captured index's 40 payloads through the production HTTP client against an httptest capture server and deep-equals the PUT mapping against systest-go/fixtures/elasticsearch.json while comparing every action header and payload line byte-for-byte. Recorded execution (UNCACHED): make unittest-go PASS; make lint-go 0 issues; make build-go green; go test -race -count=1 -tags systest ./systest-go/... PASS (137.7s); JOURNALPUMP_BIN=go pytest systest/ 38 passed; pytest test/ 160 passed. Row done (approved 2026-10-12). | – | – |
| 24 | Websocket sender | done | bde926f + 41e800b + a740f75 + 9920a6b + c725451 + 94d1639 (+ docs 67e0eb8/10385c5/5361ce4/dfa7a45/8a0a0e5/6b8f1f6) | internal/sender/websocket ports journalpump/senders/websocket.py and is registered as output_type websocket. Python runs the connection as an asyncio event loop inside a Thread (a dummy consumer draining inbound + run_coroutine_threadsafe sends); Go collapses that to ONE runner goroutine that owns the connection, dials/reconnects, drains inbound frames and serializes outbound frames. Config: websocket_uri (required), socks5_proxy, ssl (bool), ca/keyfile/certfile (all three required when ssl), websocket_compression (none default; deflate = permessage-deflate), compression (snappy default; none), max_batch_size (1048576; 0 disables batching), max_send_interval (1.0). Two compression layers: app-level snappy (github.com/golang/snappy Encode = python-snappy compress) and protocol permessage-deflate (gorilla EnableWriteCompression). Batching: max_batch_size > 0 NUL-joins one batch's messages into a SINGLE frame and the connect header Journalpump-Batch-Size carries the configured max_batch_size; 0 sends each message as its own frame. Connect headers: User-Agent journalpump/<version>, Journalpump-Compression when compression != none. TLS: client cert (certfile/keyfile) + CA verification (ca). SOCKS5 (socks5_proxy): destination names resolve at the PROXY (remote DNS, ATYP=0x03, matching python_socks rdns=True); an explicit socks5_proxy overrides env proxies, while an environment HTTP(S) proxy is selected by default per websockets proxy=True (keys ws/socks/https/http for a ws:// endpoint; wss/socks/https for wss://). Inbound cap 2 MiB (MAX_KAFKA_MESSAGE_SIZE*2), close timeout 20 s, reconnect backoff base 20 s factor 1.8 cap 90 s jittered, reset after a connection stable >60 s, send waits up to 20 s for the connected event. Tests: the two test/test_websocket.py cases ported as TestSendMessagesBatchedSnappy (one snappy(NUL-join(batch)) frame) and TestSendMessagesNobatch (per-message frames) against an in-process gorilla server asserting the exact frames and headers; config/framing/header/not-connected/instance-wiring unit tests; the ExponentialBackoff sequence; the reconnect cadence (20/36/64.8 s) pinned under testing/synctest; and a systest-go e2e driver (TestWebsocketSenderEndToEnd) that runs the real binary against an in-process capture server and byte-matches the Task 11 fixture (systest-go/fixtures/websocket.json, 40 messages), the batch-size/compression headers exactly (User-Agent compared by the journalpump/ prefix — the fixture carries the Python version string), and the state sent count + cursor. TWO DEVIATIONS RESOLVED by coordinator rulings 2026-10-12 (charter §2): (1) batch slicing MATCHES PYTHON at the LogSender base defaults (30-byte overhead, 1 MiB limit) regardless of the configured max_batch_size; an earlier plan/spec claim of a 1-byte override was wrong (websocket.py:73 sets batch_message_overhead on the WebsocketRunner Thread, never read; the slicing LogSender keeps base.py:130-131) — runtime probe with max_batch_size=2048 reports 30 / 1048576 — and the plan/spec lines are corrected; the configured max_batch_size drives only batching and the header. (2) strict-at-load config validation KEPT, same class as the Task 22/23 decisions. REVIEW ROUND (commit a740f75) restored Python parity for 9 majors + 5 minors: reject ssl=true with a ws:// URI (gorilla would send PLAINTEXT; Python raises), HTTP Basic for websocket_uri userinfo (strip userinfo from the dial URL), bounded handshake redirects with Python's TLS-downgrade + preconnected-socket restrictions, environment proxy selection like websockets proxy=True (explicit socks5_proxy still wins), SOCKS5 remote DNS (ATYP=0x03), an active 20 s ping / 20 s pong heartbeat (gorilla only auto-answers), a 2 MiB bound on the DECOMPRESSED inbound stream (permessage-deflate bomb), awaiting the peer close response within the deadline, the inner 10 s opening timeout kept separate from the outer 30 s budget, rejecting explicit null compression enums, classifying ordinary transport write failures so they do not emit stats.unexpected_exception, and unblocking an in-flight write on shutdown/reload by setting a deadline on the raw socket (gorilla SetWriteDeadline is a stored-field facade that cannot interrupt a blocked write; assessed as NOT a library-swap trigger — the fix is in-library, no user decision needed). Every item is covered by websocket_fixes_test.go. REVIEW ROUND 2 (changes-required: 5 majors + 4 minors, commit 9920a6b): (1) the raw-socket cancellation is now an IRREVERSIBLE abort (closing the raw conn) because a one-shot deadline could be re-armed by a concurrent gorilla write (gorilla re-arms at each frame write; drain/PongHandler reset read deadlines) — a normal stop still performs the closing handshake first (one-shot write deadline only) then force-closes, so no later write blocks the runner join; TestAbortPreventsWriteRearm + TestCloseUnblocksBlockedWrite (with a reload send); (2) HTTPS proxies are tunneled with a verified TLS hop (gorilla's proxy path accepts only http://) — TestSendMessagesThroughHTTPSProxy against a real TLS CONNECT proxy; (3) urllib env precedence reproduced (a present-but-empty lowercase var removes the uppercase setting; no_proxy bypasses only when the whole value is *) — TestEnvValueUppercaseFallbackAndEmptyRemoval, TestNoProxyStarOnlyWholeValue, TestNoProxyEmptyLowercaseRemovesUppercase, TestSelectEnvProxyUnaffectedByUppercaseLeak; (4) the proxy is re-resolved per redirect destination (scheme + no_proxy) — TestRedirectReselectsProxy covers both proxied-to-direct and direct-to-proxied; (5) the heartbeat tracks the outstanding ping payload and requires a MATCHING pong within pingTimeout, so a peer that streams data but never pongs still times out — TestHeartbeatDetectsPongWithholdingWhileDataFlows; (6) the closing handshake is coordinated with the live inbound reader (drain awaits the peer's close response) — production-path TestCloseAwaitsPeerCloseOnStop that withholds then releases the response, replacing the upper-bound-only TestCloseConnAwaitsPeerClose; (7) credentials are stripped from EVERY dial URL incl. a redirect Location — TestSendMessagesAuthenticatedRedirect; (8) the dial is phased (preconnect under the outer 30 s budget, then the opening handshake under the inner 10 s open_timeout) and TestDialStalledHandshake now asserts ACTUAL dial timing; (9) the stalled-server test helper is a raw-TCP websocket endpoint with an explicit stop signal that closes and joins every accepted connection (no hijacked-context leak). Recorded execution (UNCACHED): go test -race -count=1 ./... PASS; make lint-go 0 issues; make build-go green; go test -race -count=1 -tags systest ./systest-go/... PASS (144.3s); JOURNALPUMP_BIN=go pytest systest/ 38 passed; pytest test/ 160 passed. A THIRD REVIEW ROUND (commit c725451) fixed 3 majors + 2 minors: (1) shutdown is bounded INDEPENDENTLY of writeFrames — a watchdog goroutine irreversibly aborts the connection after closeTimeout even while serve is blocked in writeFrames, because stop()'s one-shot write deadline is re-armed by a later gorilla frame write (TestStopWatchdogAbortsBlockedWrite drives the real Close/reload path with a gorilla-cleared deadline); (2) the ENTIRE CONNECT tunnel exchange is bounded (explicit socket deadline min(now+connectTimeout, ctx deadline) plus close-on-context-cancellation, HandshakeContext for the TLS hop), so a proxy withholding its CONNECT response cannot block past the opening deadline or a runner stop (TestProxyConnectStalledHTTPIsBounded / TestProxyConnectStalledHTTPSIsBounded with hijack-and-hold proxies, TestProxyConnectStopCancelsStalled); (3) CONNECT carries a Python-compatible Basic Proxy-Authorization generated from the proxy URL userinfo, sent only to the proxy (TestProxyConnectAuthenticated also asserts the credential is not forwarded to the destination); (4) phase separation matches Python: ONE open_timeout deadline across ordinary establishment AND the whole redirect sequence, with only an explicit socks5_proxy preconnection under the outer connectTimeout (TestDialOrdinarySharedOpeningDeadline, TestExplicitSocksPreconnectUsesOuterBudget; gorilla's HandshakeTimeout removed); (5) the no_proxy wildcard is compared as the COMPLETE value — a padded " * " does not bypass (TestNoProxyPaddedWildcardDoesNotBypass). Recorded execution (UNCACHED, round 3): go test -race -count=1 ./... PASS; make lint-go 0 issues; make build-go green; go test -race -count=3 ./internal/sender/websocket/ PASS; go test -race -count=1 -tags systest ./systest-go/... PASS (144.2s); JOURNALPUMP_BIN=go pytest systest/ 38 passed; pytest test/ 160 passed. CLOSING REVIEW ROUND (commit 94d1639) fixed 1 major + 1 minor: (1) the shutdown watchdog was skipped when stop() observed no published connection, but run() could publish a dial completed just after stop and serve a ready send without a watchdog (serve/submit selecting a ready send under a closed stopCh), leaving Close/reload unbounded; publication is now synchronized with stopping — a stopped flag is set and stopCh closed under r.mu together with reading conn, and setConnected rejects and closes any connection completed after stop (TestStopBetweenDialAndPublishRejectsConnection completes a dial strictly after stop and asserts it is neither published nor served and is closed); (2) TestExplicitSocksPreconnectUsesOuterBudget now drives runner.dial through a forwarding SOCKS5 proxy that stalls past openTimeout but within connectTimeout, so it genuinely covers the production timeout choice rather than calling preconnect directly. Recorded execution (UNCACHED, round 4): go test -race -count=1 ./... PASS; make lint-go 0 issues; make build-go green; go test -race -count=3 ./internal/sender/websocket/ PASS (16.1s); go test -race -count=1 -tags systest ./systest-go/... PASS (144.1s); JOURNALPUMP_BIN=go pytest systest/ 38 passed; pytest test/ 160 passed. APPROVED (coordinator confirmation 2026-10-12, row flipped to done). Parity scope delivered on gorilla/websocket: Basic-auth userinfo + bounded redirects, permessage-deflate with a 2 MiB bounded drain, 20 s ping / matching-pong heartbeat, urllib env-proxy precedence (empty-lowercase removal + whole-value "*" wildcard), SOCKS5 remote DNS, TLS-CONNECT proxy with Basic Proxy-Authorization, phased open timeout with the explicit-SOCKS exception, and an irreversible shutdown watchdog; SSL + ws:// is rejected. Batch slicing matches Python's 30 / 1 MiB base defaults, and strict-at-load config validation is the recorded deviation (same class as Tasks 22/23). CLIENT-LIBRARY NOTE: gorilla shares the kafka client's (sarama-class) non-interruptible-write limitation — Conn.SetWriteDeadline only stores a field — but the in-library raw-conn close/abort proved sufficient, so NO library swap was needed or made. | – | – |
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