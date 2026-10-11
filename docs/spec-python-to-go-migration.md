# Spec: journalpump migration from Python to Go

Status: final (rev 8) — folds in the Go-idiom review (thread A), the mock
strategy decision (thread B), e2e frontloading (thread C), the
SIGTERM-grace-flush sanctioned deviation (validation round), and the kafka
client revision (D5: IBM/sarama → twmb/franz-go, user decision 2026-10-11).
Date: 2026-10-09 (rev 8: 2026-10-11)
Author: architect (grounded in a full read of the current codebase)

---

## 1. Goal

Replace the Python implementation of journalpump with a full rewrite in Go.
The Go daemon reads journald and forwards entries to the same set of outputs,
with the same operational contract: same config file semantics, same
journald reading behavior, same wire behavior per sender, same packaging
(deb/rpm + systemd unit), same metrics and state file.

### What "done" means

1. A single Go binary `journalpump` accepts the same CLI surface
   (`journalpump <config.json>`), honors SIGHUP reload, SIGTERM shutdown,
   and the full systemd notify protocol (`READY=1`, `RELOADING=1`,
   `STOPPING=1`, `WATCHDOG=1`) as today.
2. All eight senders are implemented and pass byte-level or behavior-level
   parity tests against their Python counterparts (see section 5).
3. The Go process reads and writes state files compatible with the Python
   implementation (same `json_state_file_path` schema), so users can
   switch back and forth between implementations freely without losing
   cursors or replaying the journal.
4. deb and rpm packages install and run under the existing systemd unit
   semantics (binary at `/usr/bin/journalpump`, config at
   `/var/lib/journalpump/journalpump.json`, `Type=notify`,
   `Restart=always`, runtime dir mode 0700, `journalpump` system user on
   deb). The Go implementation lives on a branch of this repo; users build
   their deb/rpm from that branch, with a release-ID variation marking the
   golang build (see §9 D1, D7).
5. CI runs Go lint + unit tests + system tests on every PR, equivalent in
   coverage to today's `make unittest` / `make systest`.
6. Behavior is identical to the Python version, including the quirky
   corners listed in §3 and §5; any deviation must be a user-visible
   decision recorded in NEWS. Sanctioned deviations are listed in §6.1.

## 2. Non-goals

- No new features during the migration (no new senders, no config format
  v2, no clustering/HA). Any feature work waits until after parity.
- No change to the JSON config file format. Existing `journalpump.json`
  files must load unchanged; no key is dropped (socks5-kafka is retained,
  D4).
- No persistent/robust queueing (disk-backed buffers, exactly-once
  delivery). The current at-least-once, in-memory-buffer design is
  preserved, including "buffered-but-unsent messages are lost on crash and
  redelivered from the last sent cursor on restart".
- No new metrics backend (Prometheus, OTLP). statsd/telegraf only, same
  metric names.
- Not porting the `USE_RE2` environment switch: Go's `regexp` stdlib is
  already RE2-style. The `threshold_for_metric_emit` slow-regex metric
  becomes largely moot but the config key stays accepted for compatibility.
- No parallel-run machinery and no release-process change: no canary
  fleet, no side-by-side daemon, no new publishing pipeline (D1).

## 3. Current architecture (baseline being migrated)

Grounded facts from the current code, which the Go design must map onto:

- **Core** (`journalpump/journalpump.py`, 1209 lines): a single-threaded
  `select.poll()` loop (1 s budget per iteration) over the journald fd from
  `systemd.journal.Reader` (python-systemd, private `_next`/`_get_all`/
  `_get_cursor`/`_get_realtime` bindings for speed). Reads in chunks of
  `CHUNK_SIZE = 5000`. Events `NOP/APPEND/INVALIDATE`; on `INVALIDATE` the
  reader fd is dropped and the reader is recreated at most once per 2 s
  (unread entries in deleted files are deliberately dropped). Per-entry
  `OSError` containing "Bad message" is swallowed and counted as
  `journal.corrupted_log_entry`. A reader whose journal has no persistent
  and no runtime files (e.g. the journal dir does not exist yet) is closed
  and retried later on the same ≤1-attempt-per-2 s cadence.
- **Cursors**: each sender tracks `_sent_cursor` = cursor of the last
  acknowledged batch (cursor of the *last* message in the batch). State
  file (`json_state_file_path`) persists per-reader/per-sender cursor,
  counters (including per-reader `searches` hit counters,
  `journalpump.py:328-335`) and health every 10 s and on shutdown, written
  atomically (tmp + rename) with `json.dump(..., sort_keys=True)` — keys
  are alphabetically sorted. Resume semantics are quirky and must be
  replicated exactly: `configure_readers` iterates the persisted per-sender
  state **in state-file JSON key order** (`journalpump.py:890`) and takes
  the **last sender's cursor** as the reader resume cursor (in-code TODO
  says it should pick the oldest); and if **any** sender's persisted
  cursor is missing/None, the resume cursor is None and the reader starts
  from the journal **head** (full replay). On resume the reader seeks to
  the resume cursor and reads one entry to step over the already-processed
  one. `initial_position`: `head` / `tail` / integer (seconds from boot,
  via `seek_monotonic`).
- **Delivery model**: reader serializes each entry to JSON bytes once per
  sender (after field filter, unit log level, secret filters, searches,
  `extra_field_values`) and pushes `(bytes, cursor)` into that sender's
  shared `MsgBuffer` (lock-guarded list, swap-and-clear drain). One daemon
  thread per sender flushes every 100 ms when the buffer holds >1000
  messages or `max_send_interval` has elapsed, batches up to 1 MiB
  (+30 B overhead) per `send_messages` call, and on failure sleeps with
  jittered exponential backoff (base 0.5 s, cap 1800 s), retrying from the
  head of the drained slice. The shared loop keys health reporting
  (`mark_disconnected`), the error ring, and backoff classification off
  the *exception* raised by the sender.
- **Backpressure**: the reader stops reading when
  `msg_buffer_max_length` (default 50000 messages) or `msg_buffer_max_bytes`
  (default 5 MiB) is hit for the *worst* sender of that reader. Nothing is
  ever dropped.
- **Daemon lifecycle** (`daemon.py`): SIGHUP sets a reload flag (no file
  watch; config ctime is compared at handler time). On reload: statsd/
  tags/geoip/filters rebuilt unconditionally; readers rebuilt only if the
  parsed `readers` config differs (Python compares the *parsed dicts* —
  order/whitespace-insensitive, `journalpump.py:875`). Changed readers are
  stopped and recreated; they resume from the persisted sent cursor (see
  the last-sender-wins / any-missing-replays rules above). Corollary that
  must be preserved: when the `readers` blob is unchanged, the rebuilt
  filter objects never reach existing readers, because senders capture
  their filter/unit-log-level objects at construction time
  (`journalpump.py:267` ff.) — i.e. filter edits alone are ineffective
  until the readers section also changes. The Go port must resolve filters
  at sender-construction time too, not at flush time, or reload behavior
  would change.
- **Shutdown (Python ground truth)**: sender threads are
  `daemon=True` (`senders/base.py:117`); `shutdown()`
  (`journalpump.py:972-981`) only sets the running flags and saves state —
  buffered-but-unsent messages are lost on SIGTERM. (The Go implementation
  deliberately differs here; see §6.8 / §9 D9 sanctioned deviation.)
- **Observability**: raw-UDP statsd client with telegraf tag syntax
  (`metric;tag=val:value|type`), with `Tagged` name/value sanitization
  (unsafe chars substituted, `base.py:42-60`). Metric names:
  `journal.read_lines`, `journal.read_bytes`, `journal.last_read_ago`,
  `journal.last_sent_ago`, `journal.sent_bytes`, `journal.sent_lines`,
  `journal.status`, `journal.corrupted_log_entry`, `journal.read_error`
  (tag `error=too_long`), `journal.secret_filter_match`,
  `journal.perform_search_regex_duration`, per-search hit counters,
  `sender.failed_to_start` (re-sent every 10 s), `exception` (tags
  `exception`, `where`). Reader-side gauges (`read_lines`, `read_bytes`,
  `last_read_ago`) are throttled to one emission per 10 s per reader and
  persistent gauges are re-emitted on change (`journalpump.py:350-383`).
  No HTTP health endpoint; health is the state file + systemd notify. No
  Sentry anywhere in the codebase.
- **Packaging**: setuptools entry point `journalpump.cli:main`; deb
  (`Architecture: all`, `adduser --system --group journalpump`, home
  `/var/lib/journalpump`) and rpm (`BuildArch: noarch`) built via
  `make deb` / `make rpm`. Unit file: `Type=notify`, `User=root`,
  `Restart=always`, `ExecStart=/usr/bin/journalpump
  /var/lib/journalpump/journalpump.json`, `ExecReload=/bin/kill -HUP
  $MAINPID`, `RuntimeDirectory=journalpump` mode 0700. CI (build.yml)
  runs ruff/pylint/mypy and `make unittest` + `make systest` on Python
  3.13/3.14; packages are built by maintainers, not published by CI.
- **Size**: ~3,700 lines of Python implementation plus ~2,700 lines of
  unit tests and ~1,100 lines of system tests.

## 4. Go target architecture

### 4.1 Module layout

Work happens on a branch of this repo (D1); the Go tree lives alongside the
Python tree until parity is reached:

```
cmd/journalpump/                   entry point: config path arg, signals, exit codes
internal/daemon/                   supervisor: errgroup lifecycle, SIGHUP reload, sd_notify, watchdog
internal/config/                   JSON loading, typed structs, diffing, running-config writer
internal/reader/                   journald reading, cursor seek, rotation, chunked reads, reader-side stats
internal/journal/                  entry struct, converters, JSON serialization, cursor parse
internal/filter/                   field filters, unit log levels, secret filters, searches
internal/geohash/                  PURE geohash encoder only (GeoIP db loading + search tag callables live with the reader/search handling — see §4.4)
internal/buffer/                   per-sender buffer + admission (backpressure)
internal/sender/                   Sender interface, wiring map, shared flush/backoff loop
internal/sender/kafka/
internal/sender/elasticsearch/     (also serves output_type opensearch)
internal/sender/cloudwatch/
internal/sender/gcl/
internal/sender/logplex/
internal/sender/rsyslog/
internal/sender/websocket/
internal/sender/file/
internal/syslogframing/            SyslogTcpClient equivalent, RFC5424/3164/custom, RFC6587
internal/statsd/                   telegraf-tagged UDP client + Tagged sanitization
internal/state/                    state file read/write (Python-compatible both directions, D3)
```

### 4.2 Sender contract (the central interface)

Unlike Python's `bool`-returning `send_messages`, the Go contract returns
an error so the shared loop has the failure cause it needs — Python keys
the health ring, `mark_disconnected`, and backoff classification off the
sender's exception:

```go
type Sender interface {
    // Send one batch. A non-nil error fails the batch; the shared loop
    // classifies it (retriable vs permanent) via the sender's classifier,
    // owns mark_disconnected/backoff, wraps it with %w when logging, and
    // retries the same batch from its head on the next flush.
    SendMessages(ctx context.Context, batch [][]byte, cursor string) error
    MaintenanceOperations(ctx context.Context) error
}
```

Each sender implementation also exposes a `retriable(error) bool`
classifier seam (e.g. kafka's connection-error set, ES/HTTP 5xx-vs-4xx,
CloudWatch throttling) — the Go equivalent of Python's per-sender
except-clauses. The shared loop (one goroutine per sender) owns: buffer
drain, 100 ms tick, flush trigger (>1000 messages or `max_send_interval`
elapsed), 1 MiB batch slicing with 30 B overhead (all senders, including
websocket — §5), jittered exponential backoff (0.5 s base, 1800 s cap,
reset on `mark_connected`), health tracking (`mark_connected` /
`mark_disconnected`, ≤8 deduped errors truncated to 128 chars), stats
gauges, and sent-cursor bookkeeping. Sender implementations contain only
transport-specific logic. Everything generic lives in one reviewed file,
as in Python.

### 4.3 Concurrency model (replacing Python threads)

- **Lifecycle is prescribed, not improvised** (no hand-rolled done-
  channels or atomic bool flags):
  - `runCtx, runStop := signal.NotifyContext(context.Background(),
    syscall.SIGTERM, syscall.SIGINT)` — the run context.
  - `readerGroup, readerCancel := errgroup.WithCancel(runCtx)` — one
    goroutine per reader; `senderGroup, senderCancel :=
    errgroup.WithCancel(runCtx)` — one goroutine per sender.
  - SIGHUP is NOT cancellation: `hupCh := make(chan os.Signal, 1);
    signal.Notify(hupCh, syscall.SIGHUP)`; the supervisor's `select`
    consumes it and the reload handler does the ctime check at event time.
  - On `runCtx.Done()` (grace-flush shutdown, sanctioned deviation D9):
    cancel readers → `readerGroup.Wait()` → cancel senders with a grace
    timer of `max(max_send_interval)` over all senders (this flush window
    is what Python does NOT do — its daemon sender threads die with
    buffered messages) → `senderGroup.Wait()` → save state → exit 0.
    Startup/config errors exit nonzero, as today.
- One goroutine per reader (the per-reader poll budget of 1 s is preserved
  per reader); one goroutine per sender.
- Buffer handoff: per-sender buffered channel of `(payload []byte, cursor
  string)` plus byte-count accounting:
  - Channel capacity = `msg_buffer_max_length` (+ small headroom for
    in-flight items) so admission control, not channel blocking, is the
    throttling mechanism.
  - **Admission gate**: before reading each chunk, the reader checks the
    worst sender's buffered message count and byte budget against
    `msg_buffer_max_length` / `msg_buffer_max_bytes` (defaults 50000 /
    5 MiB) and stops reading if either is hit — reproducing "pause whole
    reader when the worst sender is full" exactly.
  - Failed enqueue retries: `select { case ch <- item: case
    <-ctx.Done(): }` in a loop — never drops.
  - Flush trigger reads `len(chan)` — the same signal Python's loop uses
    (`len(msg_buffer)`).

### 4.4 journald access and enrichment placement

- Library: `coreos/go-systemd/v22/sdjournal` (purego/dlopen based in
  recent versions; runtime dependency on `libsystemd`, same as
  python-systemd today). Alternatives considered: shelling out to
  `journalctl -o json -f` (rejected: cursor fidelity, rotation control,
  per-field access and `seek_monotonic` initial position are all weaker);
  writing raw sd_journal bindings (rejected: unnecessary).
- The reader seam is consumer-side and small — NOT a mirror of
  python-systemd's private binding surface:
  `Wait(ctx, timeout) (Event, error)`, `ReadBatch(n) ([]Entry, error)`,
  `Seek(cursor string) error`, `Close() error`. `Entry` carries raw field
  bytes and the cursor.
- Must reimplement: the `i=<hex>` seqnum parse out of the cursor string
  (`__SEQNUM` enrichment), the converters table for known journald fields,
  and the APPEND/INVALIDATE event loop with reader recreation on rotation.
- **Enrichment placement**: `internal/geohash` contains ONLY the pure
  geohash encoder; the GeoIP database loading (`geoip_database` config)
  and the search tag callables (`ip_to_geohash(ip_tag_name, precision)`,
  `journalpump.py:488-504`, callables 512-544) live with the reader/
  search handling (reader construction and the search-tag registry), not
  in the geohash package.
- Invalid UTF-8 in journald field values (D8): Python's `process()`
  (`journalpump.py:711-714`) converts undecodable byte values to their
  `repr()` string *before* serialization, so raw invalid bytes never reach
  the JSON wire. The Go port must implement the same repr-style fallback
  (`%q`-style escaped representation matching Python `repr` for the
  fallback path); this also makes byte-identical golden-file parity
  against Python `json.dumps` output achievable.
- fd identity across `sd_journal.Close()`/reopen must be handled
  explicitly (Python's `_STALE_FD` sentinel bookkeeping disappears; in Go
  each reader goroutine owns its journal handle, which removes the shared
  stale-fd problem entirely).

### 4.5 Config, reload, state

- Parse `journalpump.json` into typed structs with the same defaults
  (`msg_buffer_max_length` 50000, `msg_buffer_max_bytes` 5242880,
  `initial_position` head, etc.). Validation errors are clear at startup;
  a failed reload keeps the previous running config, as today. **The
  code, not README.rst, is the source of truth for accepted keys and
  defaults** (the README's Sender Configuration section omits
  `aws_cloudwatch`, `google_cloud_logging`, `rsyslog`, `opensearch` and
  `file` from its documented `output_type` list).
- Reload: SIGHUP channel event (§4.3) + config ctime comparison at
  handler time; rebuild statsd/tags/geoip/filters always; rebuild a
  reader only when the parsed `readers` differs — compared as parsed
  structures (`map[string]any` + `reflect.DeepEqual` or go-cmp), NOT as
  raw JSON text (Python compares parsed dicts: order/whitespace-
  insensitive). Preserve the granularity quirks: any change inside
  `readers` replaces all readers, and the filter-capture corollary (§3).
- Running config writer (`write_running_config`, `json_running_config_path`,
  `RUNTIME_DIRECTORY`, dir 0700 / file 0600): direct port.
- State file: same JSON schema (including per-reader `searches` hit
  counters), atomic tmp+rename, 10 s cadence + shutdown. Same field names
  so monitoring scripts keep working. **Key order**: Python writes the
  state file (and the running-config dump) with
  `json.dump(..., sort_keys=True)` (`journalpump.py:1070`, `:946`), so
  the Go writer must emit alphabetically sorted keys for faithful
  bidirectional handover (D3). Compatibility with Python-written state
  files in **both** directions is required: the Go binary must read a
  Python-written file and the Python binary must be able to resume from a
  Go-written file — locked by permanent tests on both sides, not a
  one-off check. The quirky resume-cursor selection is inherited as-is;
  because it depends on state-file **key order** (§3), the per-reader
  `senders` object is decoded with `json.Decoder` token streaming into
  ordered `(name, cursor)` pairs — `encoding/json` into a map would
  discard the order the quirk depends on. Note that in Python-written
  files the order is alphabetical (sort_keys), so last-sender-wins
  always resolves to the alphabetically-last sender; the "last-listed ≠
  alphabetically-first" quirk-lock fixture must therefore be
  hand-crafted, not captured.
- Logging: `log/slog` with a journald datagram writer
  (`/run/systemd/journal/socket`, `SYSLOG_IDENTIFIER=journalpump`) when
  stdout is not a tty, matching Python's `JournalHandler` behavior and
  message-format fields.

### 4.6 Library choices per sender

| Concern | Python today | Go choice |
|---|---|---|
| journald | python-systemd | coreos/go-systemd/v22/sdjournal |
| sd_notify/watchdog | systemd.daemon | coreos/go-systemd (sdnotify/daemon) |
| Kafka | kafka-python (+Aiven fork socks5) | twmb/franz-go (D5, revised 2026-10-11 — see §9 D5; the client choice was revisited because the sender contract is `SendMessages(ctx)` and sarama has no context support, `docs/assess-sender-cancellation.md`): `kgo.Client.ProduceSync(ctx, batch...)` per batch with per-record results (`ProduceResults.FirstErr()` fails the batch); acks=1 via `RequiredAcks(LeaderAck())` + `DisableIdempotentWrite()` (do NOT reimplement kafka-python's produce-per-message/future/flush dance); `ProducerBatchCompression(SnappyCompression())`; socks5/socks5h via `kgo.Dialer` wrapping golang.org/x/net/proxy (D4); SASL PLAIN/SCRAM from franz-go's in-tree `pkg/sasl`; topic auto-create via `kadm`; our murmur2 partitioner kept behind `kgo.RecordPartitioner` (Python nil-key placement is random, franz-go's default is sticky) |
| ES/OpenSearch | raw HTTP via requests | raw HTTP via net/http (no ES client — matches today) |
| CloudWatch | botocore | aws-sdk-go-v2 (cloudwatchlogs) |
| Google Cloud Logging | google-api-python-client (REST v2) | google.golang.org/api/logging/v2 (same REST surface) |
| Websocket | websockets (asyncio) + aiohttp-socks | coder/websocket (or gorilla) + golang.org/x/net/proxy SOCKS5 |
| Snappy | python-snappy (libsnappy) | github.com/golang/snappy (pure Go — drops libsnappy build dep); used ONLY for websocket app-level compression; kafka compression is franz-go's `SnappyCompression()` (raw snappy block — the same framing the earlier sarama implementation emitted and kafka-python proved to read), not a manual snappy dependency (see §5 kafka quirks re xerial framing) |
| GeoIP | geoip2 | oschwald/geoip2-golang (same .mmdb format) |
| Regex | re / google-re2 | stdlib regexp (RE2 semantics); `\1`-style backrefs in secret_filter `replacement` translated to Go `$1` syntax at load time |
| statsd | raw UDP socket | raw UDP socket (direct port, ~60 lines) |

### 4.7 Metrics and status

Direct port of `statsd.py` and every metric name listed in section 3.
`journal.status`, `last_read_ago`, `last_sent_ago`, health mapping
(connected=3, disconnected/stale=2, stopped=0) and the 10 s gauge refresh
cadence are preserved so existing dashboards keep working. State file
health output likewise.

## 5. Sender inventory and quirks to preserve

The Go port must reproduce these behaviors; each row was verified against
the current source. The config keys below are the complete implemented key
set per sender (code is the source of truth; README.rst is outdated).
Every sender also accepts the shared keys: `output_type` (required,
dispatches the sender class), `field_filter`, `unit_log_level`,
`extra_field_values` (dict merged into every outgoing JSON entry), and
`max_send_interval` (flush-when-idle threshold; per-sender defaults
listed).

| Sender (lines) | Transport | Config keys (defaults) | Quirks that must be preserved |
|---|---|---|---|
| **kafka** (190) | kafka-python producer + admin client | `kafka_topic` (req), `kafka_address`, `kafka_msg_key`, `kafka_api_version` (None = auto; a set value pins `kgo.MaxVersions`; brokers older than 0.10.0 are no longer supported — §6.1(c)), `security_protocol` (`PLAINTEXT\|SSL\|SASL_PLAINTEXT\|SASL_SSL`; else derived from `ssl` bool), `ssl` (bool), `ca` (SSL/SASL_SSL), `certfile`, `keyfile` (SSL), `sasl_mechanism` (`PLAIN`), `sasl_plain_username`, `sasl_plain_password` (both required for SASL), `socks5_proxy` (retained in Go, D4), `kafka_topic_config` (`num_partitions` + `replication_factor`), `max_send_interval` (0.3) | Fixed `kafka_msg_key` for all messages (single-partition ordering); topic auto-create only when both `num_partitions` and `replication_factor` present in `kafka_topic_config` (`TopicAlreadyExistsError` tolerated); producer `linger_ms=500`; compression: kafka-python applies **xerial-framed snappy** (magic-byte block stream) — franz-go's `SnappyCompression()` emits a raw snappy block (not xerial-framed) and kafka-python's decoder accepts both; this must be proven by a kafka-python-consumer-vs-Go-producer interop test on a real broker (first proven against the sarama implementation, re-run after the franz-go switch); if framing differs for a supported consumer, record in NEWS; ack-then-advance semantics (Python: produce+flush+await futures with 1 s timeout — Go: one `ProduceSync(ctx, batch...)` call, `FirstErr()` fails the whole batch); on retriable error: full teardown + re-init loop while `running && !connected`; client logging silenced. |
| **elasticsearch / opensearch** (354) | raw HTTP (requests), not elasticsearch-py | `elasticsearch_url` / `opensearch_url` (req), `elasticsearch_timeout` / `opensearch_timeout` (60.0), `elasticsearch_index_prefix` / `opensearch_index_prefix` (`journalpump`), `elasticsearch_index_days_max` / `opensearch_index_days_max` (3), `ca` (default `True` = system CAs), `max_send_interval` (10.0) | Daily index rollover `prefix-YYYY-MM-DD` from first 10 chars of entry `timestamp` (missing → current UTC date + warning); mapping auto-created from the FIRST message seen (bool→boolean, float→float, int→integer, str→text, dict→recursive; unmappable skipped with warning; fixed `SYSTEMD_SESSION`/`SESSION_ID` fields); `?include_type_name=true` + `_type: journal_msg` for ES ≤7; success = HTTP 200/201 only, `_bulk` per-item errors NOT inspected (mark_sent regardless); `already_exists_exception` counts as success; hourly retention deletes oldest indices while count > `index_days_max`; connection-error logging de-duplicated by error class; bulk timeout = 2× configured timeout. |
| **aws_cloudwatch** (142) | botocore `logs` client | `aws_cloudwatch_log_group` (req), `aws_cloudwatch_log_stream` (req), `aws_region`, `aws_access_key_id`, `aws_secret_access_key` (all optional → botocore default chain), `max_send_interval` (0.3) | Sequence-token protocol: first PutLogEvents without token, then `nextSequenceToken` (AWS now ignores tokens — keep sending for parity); init: 3 attempts to create group/stream ignoring already-exists, paginator over describe_log_streams to recover token, AccessDenied → backoff → `SenderInitializationError`; timestamp = `REALTIME_TIMESTAMP` ms (fallback wall clock); message body = raw JSON line; no explicit 1 MB/10k-event guard — base 1 MiB batching is the guard (keep batches ≤1 MB and ≤10k events in Go). |
| **google_cloud_logging** (132) | REST v2 via googleapiclient | `google_cloud_logging_project_id` (required; the service account credentials may override the value after load), `google_cloud_logging_log_id` (req), `google_cloud_logging_resource_labels` (None), `google_service_account_credentials` (None → Application Default Credentials), `max_send_interval` (0.3) | Entry quota logic: 250 KiB quota / 200 KiB per-message → truncate `MESSAGE` + `"[MESSAGE TRUNCATED]"`, then fallback replacement entry with the fixed "can't be logged" message; entries without `MESSAGE` replaced; `timestamp` popped, truncated to 26 chars + `Z`; `PRIORITY` → GCP severity map (7→DEBUG … 0→EMERGENCY, unknown→DEFAULT); `MESSAGE` attempted JSON-parse → structured jsonPayload; resource type fixed `generic_node`; logName `projects/{id}/logs/{log_id}`; optimistic reconnect after failure (mark_connected immediately). |
| **logplex** (49) | HTTPS POST, requests | `logplex_log_input_url` (req), `logplex_token` (req), `logplex_request_timeout` (2 s), `max_send_interval` (5.0) | Behavior kept identical (D6): fire-and-forget — response status IGNORED, no retry, always mark_sent; frame: `LEN <n> <190>1 <now-timestamp> <hostname> <token> <pid> - - <MESSAGE>` with PRI fixed 190, timestamp = format time (not journal time), msg id and structured-data `-`; `verify=False` (TLS verification deliberately disabled). **Hostname/pid quirk**: entry keys are processed with leading underscores stripped (`journalpump.py:711-714`), so the `_HOSTNAME`/`_PID` lookups never match — hostname and pid are *always* the literal `"localhost"`. Port must keep this. |
| **rsyslog** (130 + 259 shared client) | raw TCP, optional TLS | `rsyslog_server` (req), `rsyslog_port` (514), `format` (`rfc5424`; `RFC5424\|RFC3164\|CUSTOM`, CUSTOM requires `logline`), `logline` (rsyslog template with `%token%` vars; unknown token → literal `-`; newline auto-appended), `max_message_size` (2048), `ssl` (bool; `ca_certs` implies ssl), `ca_certs`, `client_cert`, `client_key`, `octet_counted_framing` (false), `escape_newlines` (false), `default_facility` (1), `default_severity` (6), `structured_data` (None), `max_send_interval` (0.3) | RFC6587 framing: octet-counted sends `<len> <body>` (truncated, no trailing newline); non-transparent truncates to `max_message_size-1` and appends `\n` only when truncation happened; PRI = facility*8+severity, defaults 1/6, out-of-range → PRI 13; facility from `SYSLOG_FACILITY` (first element if list; parse failure → `default_facility`), severity from `PRIORITY` — a list-valued `PRIORITY` raises and forces sender re-init (only FACILITY has the list guard); timestamp truncated to 26 chars + `Z`, fallback current UTC; RFC3164 date from first 19 chars parsed as UTC; app-name chain `SYSLOG_IDENTIFIER`→`SYSTEMD_UNIT`→`PROCESS_NAME`; NILVALUE `-` for missing fields; utf-8 encode with `replace`; `escape_newlines` rewrites `\r`/`\n` in message only; custom `logline` template with `%token%` vars (unknown token → literal `-`, newline auto-appended); socket send retries once on EPIPE/ECONNRESET/ETIMEDOUT/SSLEOF; init loop = backoff + extra 5 s sleep. |
| **websocket** (392) | websockets (asyncio) + snappy + SOCKS5 | `websocket_uri` (req), `socks5_proxy`, `ssl` (bool), `ca`/`keyfile`/`certfile` (all three required when ssl), `websocket_compression` (`none` default; `deflate` = permessage-deflate), `compression` (`snappy` default; app-level via github.com/golang/snappy), `max_batch_size` (1048576; 0 disables batching), `max_send_interval` (1.0) | Two-level compression: app-level snappy and permessage-deflate; batching joins messages with `\x00` separators into ONE frame; the `Journalpump-Batch-Size` header carries the *configured* `max_batch_size` (default 1 MiB), not the actual batch size; batch SLICING uses the LogSender base defaults (30 B overhead, 1 MiB limit) regardless of the configured `max_batch_size` — `websocket.py:73` sets `batch_message_overhead = 1` on the `WebsocketRunner` Thread, which is never read; the `WebsocketSender` LogSender that slices (`base.py:262,266`) keeps `base.py:130-131` (30 / 1 MiB); handshake header `User-Agent: journalpump/{version}`, `Journalpump-Compression`; inbound cap 2 MiB, close timeout 20 s; dummy consumer drains inbound (connection dead when it exits); reconnect backoff base 20 s factor 1.8 cap 90 s jittered, reset after 60 s stable connection; TLS requires all of ca/keyfile/certfile; graceful close cancels pending sends and waits 2 s. In Go this collapses to a single goroutine (no asyncio-in-a-thread dance). |
| **file** (19) | local append | `file_output` (req, opened append-binary), `max_send_interval` (0.3) | Open once (`ab`), one JSON line + `\n` per message, never fails (always mark_sent), no fsync. |

Shared behaviors (all senders): 100 ms flush tick; flush when >1000
buffered or idle > `max_send_interval`; 1 MiB batch slicing (30 B overhead,
websocket included — its configured `max_batch_size` drives only batching and
the header, never the slicing limit); jittered exponential backoff
0.5 s→1800 s, reset on connect; health ring of ≤8 deduped 128-char errors;
`maintenance_operations` hook with 60 s failure retry; buffer content lost
on exit (by design).

## 6. Compatibility requirements

1. **Config**: every existing key documented in README.rst *and every key
   accepted by the current code* (complete sets in §5) parses with
   identical defaults and validation outcomes. The code is the source of
   truth, not the README. `journalpump.json` loads unchanged (D2).
   **Sanctioned deviations (NEWS-recorded)**: (a) config values of the
   wrong JSON type are rejected at load time by typed decoding, where
   Python crashed mid-run when the value was used; (b) the SIGTERM
   grace-flush window (§6.8, D9); (c) **kafka broker version floor**
   (user decision 2026-10-11, with the D5 client switch): Python's
   `_infer_broker_version_from_api_versions` port and the pre-0.10
   legacy probe (ListGroups/FindCoordinator/FetchOffset fallbacks) are
   dropped — franz-go negotiates per-request versions via ApiVersions on
   every connection, so **Kafka 0.10.0 and newer are supported** (auto
   when `kafka_api_version` is unset; a set value pins
   `kgo.MaxVersions(kversion.V<x>())`); brokers older than 0.10.0 are no
   longer supported and an explicit `kafka_api_version` below 0.10.0 is
   rejected at load with a clear error (recommended; the builder confirms
   the exact outcome in the Task 22 parity record).
2. **Journald reading**: chunk size 5000; poll budget 1000 ms;
   `initial_position` head/tail/seconds-from-boot; cursor seek + one-entry
   skip on resume; rotation = drop-and-recreate (≤1 attempt/2 s); "Bad
   message" tolerance; missing-journal-files readers closed and retried
   later; `journald_filters`, `journal_flags`, `journal_path`,
   `journal_files`, `journal_namespace`, `units_to_match`,
   `match_key`/`match_value` (entry-level equality applied in the handler,
   NOT sd_journal matches); invalid UTF-8 field values converted to
   repr-style strings exactly as Python does (D8).
3. **Resume semantics**: replicate the exact `configure_readers` rules —
   resume cursor = last sender's persisted cursor **in state-file key
   order** (in-code TODO acknowledges this should be oldest; do NOT fix
   silently); if any sender's cursor is missing ⇒ resume cursor None ⇒
   read from journal head (full replay).
4. **Outputs**: per-sender wire behavior per section 5 (framing bytes,
   index/mapping logic, headers, truncation thresholds).
5. **Packaging** (D7): deb and rpm still produced, from the migration
   branch; arch-specific variants acceptable (rpm leaves `noarch`; deb
   leaves `Architecture: all`). The rpm release ID (and deb version
   suffix) gains a variation marking the golang build so the two artifact
   lines are distinguishable. Binary at `/usr/bin/journalpump`; unit file
   semantics unchanged.
6. **Operations**: same statsd metric names; same state file schema
   (Python-compatible both directions, D3, incl. per-reader `searches`
   counters and `sort_keys=True` alphabetical key order); same sd_notify
   lifecycle; same exit codes; same log-to-journald behavior when stdout
   is not a tty.
7. **Delivery semantics**: at-least-once, cursor-per-sender, backpressure
   by stopping reads, buffered messages lost on unclean exit, filter
   objects captured at sender construction (reload corollary, §3).
8. **Shutdown** (D9 — **sanctioned deviation from Python**): Python's
   sender threads are daemon threads (`senders/base.py:117`) and
   `shutdown()` (`journalpump.py:972-981`) only sets running flags and
   saves state, so buffered messages ARE lost on SIGTERM. The user has
   decided the Go implementation keeps a grace-flush window instead:
   SIGTERM stops reading, gives senders up to `max_send_interval` to
   flush, saves state, exits 0. NEWS-recorded.

## 7. Testing strategy

### What ports, what gets rewritten

- **Frontloaded e2e (user decision, thread C)**: the systest harness is
  implementation-agnostic from the start — it spawns *a binary* (an env
  var selects the Python module or the Go binary), so black-box suites
  (running-config, reload, and later rotation/sigterm/rsyslog/interop)
  run against BOTH implementations. Before sender tasks are implemented,
  e2e tests are run against the **Python binary** to capture ground-truth
  wire fixtures (logplex POST bodies, ES index-create + bulk NDJSON,
  websocket frames, file output, state files, missing-cursor replay,
  kafka wire behavior) that then become the Go-side parity assertions.
  Python-internals-coupled tests (`test_rotation.py` in-process,
  `test_sigterm.py` sitecustomize) become Go-native tests, not ports.
- **Ports nearly 1:1 (black-box, language-neutral):**
  - Systest harness: spawn binary with `RUNTIME_DIRECTORY` +
    `NOTIFY_SOCKET` abstract socket, assert `READY=1`/`RELOADING=1`/
    `WATCHDOG=1`, SIGHUP reload, SIGTERM exit 0, running-config file
    content and 0600/0700 modes.
  - Syslog framing tests: byte-exact wire assertions (octet-counted vs
    non-transparent, truncation, escape_newlines) against a stub TCP
    server.
  - Rotation systests: reuse the binary `.journal` fixtures in
    `systest/data/` verbatim; drive the Go reader over copies that emulate
    rotation (move → new file → delete old); assert all 30 messages
    delivered, buffer-limit batching boundaries, corrupt-entry skipping,
    fd release (lsof check).
  - Live rsyslog end-to-end: journald injection (process-spawn driver +
    `sd_journal` writer) into real journald, real rsyslogd on TCP, assert
    delivered lines.
- **Mock strategy (standing rule, thread B)**: wire-level fake at the
  seam. Source fakes from third-party mock tooling FIRST — for kafka, a
  narrow `producer` interface fake in `internal/sender/kafka` plus
  franz-go's in-tree `pkg/kfake` in-process cluster for stall/cancel
  tests (franz-go ships no SyncProducer-style mock; the earlier sarama
  mocks are retired with the D5 switch), hand-written or
  mockgen-generated fakes for the CloudWatch client-interface seam
  (aws-sdk-go-v2 ships no generated mocks), official Elastic Go client
  tooling, Google Cloud Logging fakes; write custom
  httptest/net.Pipe/localhost fakes only where no maintained one exists
  (rsyslog framing, logplex, websocket via httptest, ES rollover/mapping
  assertions if the official tooling is insufficient). **Every fake must
  assert wire bytes, not library call shape.** Kafka: real broker for
  system tests, producer-interface fake / kfake for unit tests.
- **Rewritten (Python-internals-bound):** mocking of python-systemd
  private methods, `botocore.stub.Stubber`, `responses`, GCP
  `RequestMockBuilder`, registry mutation.
  - CloudWatch: the Stubber-based Python cases live inside
    `test/test_journalpump.py:1263-1520` and port to Go fake-client tests.
  - journald: the consumer-side reader seam (§4.4) gets a fake for unit
    tests; the real sdjournal implementation is covered by rotation
    systests against fixture files.
- **Unit test parity checklist**: for each of the ~127 Python unit test
  functions (test_journalpump.py alone has 64), the Go port either (a)
  ports the assertion, (b) maps it to an equivalent Go-idiomatic test, or
  (c) is explicitly dropped with a recorded reason (Python-internal only).
  Go-native tests with no Python counterpart (e.g. the SIGTERM
  grace-flush suite) are recorded as `go-native` so coverage deltas stay
  explicit.
- **New tests Go makes cheap**: race detector (`-race`) on the buffer/
  shutdown paths; fuzzing the syslog framer and cursor parser; golden-file
  tests for JSON serialization parity against Python output for a fixture
  journal.
- **Timing-dependent code**: `testing/synctest` bubbles (stable/default
  from Go 1.25; Go 1.24 requires `GOEXPERIMENT=synctest` — D10 mandates
  the latest stable toolchain, so the default-on path applies) cover the
  flush tick, the 0.5 s→1800 s jittered backoff, the 60 s maintenance
  retry, the 10 s state cadence, the 2 s rotation retry, and the shutdown
  flush window; a small `func() time.Time` clock seam is injected where a
  synctest bubble cannot apply (real socket deadlines).
- **State-file interop tests** (D3): round-trip a Python-written state
  file through the Go reader, and a Go-written file through the Python
  reader. The Python direction is locked by a permanent pytest fixture
  test added on the branch (reads a committed Go-written fixture), not a
  one-off manual check.
- **CI** (D10): latest stable Go toolchain only; golangci-lint with a
  modern linter set (gocyclo, revive/staticcheck, govet, ineffassign,
  errcheck, gocritic), `go test -race ./...` for unit tests, and the
  systest suite on ubuntu-latest runners (real journald + rsyslogd +
  broker as needed). Keep `scripts/systest-on-podman` concept: privileged
  Fedora container with systemd-journald.

## 8. Migration plan outline (detailed task list in
docs/plan-python-to-go-migration.md)

- **Phase 0 — Scaffold.** Branch setup, Go module, CI/lint workflow,
  config loader, journal entry model + JSON serialization, filters,
  geohash/GeoIP enrichment, statsd, state file I/O, sd_notify/watchdog/
  signals, then frontloaded e2e: implementation-agnostic systest harness +
  first black-box suites, and ground-truth wire fixtures captured from
  the Python binary (file/logplex/ES/websocket/state/replay/kafka).
- **Phase 1 — Core pipeline + file sender.** Sender framework (error-
  returning contract) + buffer admission, sdjournal reader, run loop +
  per-sender pipeline, reload/resume/shutdown, core systest suites.
  *Milestone: Go binary tails a real journal to a file with cursors
  surviving restart, SIGHUP reload, and Python↔Go state-file handover.*
- **Phase 2 — Syslog + logplex.** SyslogTcpClient port (byte-exact
  framing tests), rsyslog sender (+ ported rsyslog systest driver),
  logplex sender. *Milestone: end-to-end journal→rsyslogd parity with
  the Python output.*
- **Phase 3 — Kafka, ES/OpenSearch, websocket.** The three heavyweight
  senders (franz-go + snappy-framing interop proof, raw HTTP,
  websocket+snappy).
- **Phase 4 — CloudWatch + GCL.** aws-sdk-go-v2 and logging/v2 ports with
  quota/truncation logic.
- **Phase 5 — Packaging + cutover.** deb/rpm on the branch with the
  golang release-ID variation, unit file, Makefile targets, docs update,
  final parity sweep. No parallel-run machinery (D1): users cut over by
  installing the branch-built package; the Python↔Go-compatible state
  file makes each host's cutover (and rollback) cursor-safe.
- **Ordering rationale**: frontloaded e2e fixtures mean every sender task
  starts with a captured ground truth to diff against; file → syslog/
  logplex → kafka/ES/websocket goes from simplest to most stateful; each
  phase ends with an end-to-end systest so regressions surface early.
- **Risk register** (top items; owner = builder per task):
  1. **Regex semantics** — Python `re` ≠ RE2 for existing user patterns
     (backrefs in `replacement`, lookarounds). Mitigation: document the
     supported subset; translate `\1`→`$1` at load time; add a startup
     warning for unsupported constructs; test with corpus of existing
     Aiven configs if obtainable.
  2. **JSON serialization byte differences** — Go `encoding/json` escapes
     HTML by default and orders map keys; consumers may parse differently.
     Mitigation: `SetEscapeHTML(false)`, deterministic key order matching
     the Python construction order; golden-file diff tests against Python
     output (achievable with the repr-style UTF-8 fallback, D8).
  3. **sdjournal binding maturity** — go-systemd sdjournal exposes fewer
     conveniences than python-systemd's private API; `seek_monotonic`
     boot-time semantics and cursor parsing need verification. Mitigation:
     Phase 1 spike with the rotation fixtures before building on it.
  4. **Packaging arch change** — rpm leaves `noarch`, deb leaves `all`
     (accepted, D7); the new golang release-ID variation must be
     unmistakable so automation comparing package versions does not get
     confused. Mitigation: explicit suffix scheme + NEWS entry.
  5. **Quirk drift** — the intentionally-preserved oddities (last-sender-
     wins resume, logplex localhost, fire-and-forget, no `_bulk` item
     checks, state-file key-order dependence) are easy to "accidentally
     fix" during the rewrite. Mitigation: each quirk gets a named test
     that fails if behavior changes.
  6. **At-least-once duplication during cutover** — if the Go process
     starts without a compatible state file, the whole journal replays.
     Mitigation: D3 state-file compatibility (permanent interop tests on
     both sides) + cutover runbook in the branch README.
  7. **Silent coverage loss** — ~127 unit tests don't translate 1:1.
     Mitigation: parity checklist reviewed by cross-reviewer per phase.
  8. **Resume-cursor quirk replication** — last-sender-wins + any-missing
     ⇒ replay-from-head is surprising but load-bearing for existing
     deployments; "fixing" it during the rewrite would silently change
     restart behavior. Mitigation: ported unit tests asserting both rules.
  9. **Unenriched geohash tags** — a config using `ip_to_geohash` tag
     callables would silently produce empty tags if the GeoIP/geohash port
     is forgotten. Mitigation: dedicated task + tests (plan Phase 0).
  10. **Kafka snappy framing mismatch** — kafka-python xerial-framed
      snappy vs the Go client's codec framing (franz-go emits a raw
      snappy block) may not be byte-identical on the wire for consumers.
      Mitigation: kafka-python-consumer-vs-Go-producer interop test on a
      real broker (plan kafka task; re-run after the D5 client switch);
      NEWS entry if a supported consumer is affected.
  11. **Shutdown semantics drift** — the grace-flush window is a
      deliberate deviation from Python (daemon threads, buffered messages
      lost); a later "parity fix" that removes the flush would silently
      regress the user decision. Mitigation: Go-native-only sigterm suite
      + NEWS entry (plan Task 18/28).

## 9. Resolved decisions (user answers, 2026-10-09)

1. **Cutover/repo**: work happens in a branch of this repo. The rpm spec
   stays in that branch; users wanting the Go version build rpm/deb from
   the branch instead of main. The builder provides a variation on the
   release ID to signify a golang build. No parallel-run machinery, no
   release-process change.
2. **Behavior**: identical to the Python version; `journalpump.json` must
   load unchanged.
3. **State files**: compatible with Python-written state files so users
   can switch back and forth between implementations freely.
4. **socks5-kafka**: retained.
5. **Kafka library**: twmb/franz-go (**revised 2026-10-11**; originally
   IBM/sarama). Revisited because the sender contract is
   `SendMessages(ctx)` and sarama has no context support — uninterruptible
   retry sleeps and a context-less dial cost four Task 22 review rounds of
   socket-force-close workarounds with a ~2-3 s cancellation floor
   (`docs/assess-sender-cancellation.md`). franz-go is ctx-first
   (`ProduceSync(ctx)`, `Close()` fails buffered records promptly),
   emits kafka-python-readable raw snappy, takes a custom `kgo.Dialer`
   for socks5h, ships SASL SCRAM in-tree, and removes ~500 lines of
   workaround code (`docs/assess-kafka-client.md`). kafka-go (acks
   defaults to none; cancel only abandons the wait) and
   confluent-kafka-go (cgo) were eliminated. Accepted deviation:
   pre-0.10 broker version inference dropped — §6.1(c).
6. **Logplex**: identical behavior — including the underscore-stripping
   quirk that makes hostname/pid always literal `"localhost"`, and the
   fire-and-forget send.
7. **Packaging**: rpm/deb still produced; arch-specific variants
   (instead of cross-arch packages) are acceptable.
8. **Invalid UTF-8**: match Python exactly — undecodable byte field
   values are converted to their `repr()`-style string before JSON
   serialization (`journalpump.py:711-714`), so raw invalid bytes never
   reach the wire.
9. **SIGTERM** (**sanctioned deviation**): keep the grace-flush window —
   stop reading, give senders up to `max_send_interval` to flush, then
   exit. Python's actual behavior loses buffered messages (daemon sender
   threads, `shutdown()` only sets flags + saves state); the user chose
   the flush deliberately. NEWS-recorded.
10. **Go policy**: latest Go toolchain; modern lint CI enabled
    (golangci-lint, gocyclo, etc.).
11. **Mock strategy** (thread B): third-party mock tooling first, custom
    wire-level fakes only where none maintained; fakes assert wire bytes,
    not call shape (§7).
12. **E2e frontloading** (thread C): implementation-agnostic systest
    harness; Python-binary ground-truth capture for uncovered quirk-locks
    before Go sender work (§7, plan tasks 10–12).

---

## Appendix: source map (Python → Go)

| Python source | Lines | Go home |
|---|---|---|
| journalpump/journalpump.py | 1209 | internal/reader, internal/journal, internal/daemon |
| journalpump/daemon.py | 142 | internal/daemon |
| journalpump/cli.py | 13 | cmd/journalpump/main.go |
| journalpump/__main__.py | 4 | cmd/journalpump/main.go |
| journalpump/__init__.py | small | version constant (User-Agent source) |
| journalpump/senders/base.py | 281 | internal/sender (shared loop) |
| journalpump/senders/websocket.py | 392 | internal/sender/websocket |
| journalpump/senders/elasticsearch_opensearch_sender.py | 354 | internal/sender/elasticsearch |
| journalpump/senders/kafka_sender.py | 190 | internal/sender/kafka |
| journalpump/rsyslog.py | 259 | internal/syslogframing |
| journalpump/senders/aws_cloudwatch.py | 142 | internal/sender/cloudwatch |
| journalpump/senders/google_cloud_logging.py | 132 | internal/sender/gcl |
| journalpump/senders/rsyslog.py | 130 | internal/sender/rsyslog |
| journalpump/geohash.py | 82 | internal/geohash (pure encoder) |
| journalpump/statsd.py | 61 | internal/statsd |
| journalpump/util.py | 105 | internal/* (atomic file replace, etc.) |
| journalpump/senders/logplex.py | 49 | internal/sender/logplex |
| journalpump/types.py | 44 | internal/journal |
| journalpump/senders/file.py | 19 | internal/sender/file |
| journalpump/senders/__init__.py | 35 | internal/sender (wiring map) |
| conftest.py (repo root) | 1 | (empty; no Go equivalent needed) |
