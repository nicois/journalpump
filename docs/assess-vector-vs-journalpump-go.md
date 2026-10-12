# Assessment: replace journalpump with Vector?

Question from the user: "identify the pros and cons of switching to using vector
instead. we already use it internally for delivering Kafka sources."

Scope: replacing journalpump (Python today, Go port on `python-to-go` at Task 27 of
32) with Vector (Datadog, Rust, MPL-2.0). journalpump's surface below is taken from
the code on this branch and `docs/spec-python-to-go-migration.md` §5. Vector
capabilities are from training knowledge only; **this environment has no web access,
so nothing about Vector was verified against current docs.** Items marked
**[verify]** are the ones I am not confident about and that should be checked against
the Vector version you actually run before any decision.

## 0. Bottom line

- Vector covers the *mainstream* half of journalpump well (journald source, Kafka,
  Elasticsearch/OpenSearch, CloudWatch, Google Cloud Logging, file), with better
  buffering and a healthier ecosystem than a bespoke tool.
- Vector cannot reproduce four journalpump behaviours without a Vector fork or a
  sidecar: the **websocket sender's wire protocol** (NUL-joined, snappy-compressed
  batches, custom headers, SOCKS5), **Logplex framing**, **SOCKS5 for Kafka**, and
  **RFC 6587 octet-counted syslog framing**. It also cannot read or write
  journalpump's **state file**, and it emits a **different event schema** (field names
  and types) to every sink unless you rebuild journalpump's normalisation in VRL.
- Recommendation (section 6): **finish the Go port** (about 3 small tasks left) to
  secure the fleet, then run a **bounded Vector pilot on the Kafka path only**. Decide
  on consolidation after the pilot, driven by two facts you have and I do not: whether
  the websocket/logplex/socks5 senders are still live, and whether downstream
  consumers can absorb a schema change.

## 1. Sink coverage

| journalpump sender | What it actually emits (from code) | Vector equivalent | Fidelity |
|---|---|---|---|
| `kafka` | Raw per-entry JSON as record value; optional static `kafka_msg_key`; zstd>snappy>gzip; PLAINTEXT/SSL/SASL_PLAINTEXT/SASL_SSL, PLAIN/SCRAM; **`socks5_proxy`**; optional topic auto-create (`num_partitions`, `replication_factor`) | `kafka` sink (librdkafka). TLS, SASL PLAIN/SCRAM, compression, static or templated key, templated topic | **Near-complete.** Gaps: no SOCKS5 (librdkafka has no proxy support **[verify]**); no topic auto-create. Payload JSON schema differs unless remapped (see §2). |
| `elasticsearch` / `opensearch` | `_bulk` NDJSON, index `{prefix}-{YYYY-MM-DD}` from the entry timestamp; **creates a mapping from the first message's types**; **deletes indices older than `index_days_max`** every hour; ES<=7 `_type=journal_msg`; `ca` only, no auth | `elasticsearch` sink, `mode: bulk`, `bulk.index` template with strftime; OpenSearch supported via `api_version` **[verify exact option name]**; auth supported | **Good for ingest.** Gaps: no mapping creation and **no index retention** (move to ILM/ISM on the cluster, which is the better practice anyway, but it is an operational change). |
| `aws_cloudwatch` | `put_log_events` to a fixed group/stream, creates both, sequence-token handling, timestamp from `REALTIME_TIMESTAMP`; whole JSON line as the message | `aws_cloudwatch_logs` sink, `create_missing_group/stream`, templated group/stream | **Complete.** Vector's batching respects the AWS limits natively. |
| `google_cloud_logging` | `entries.write`, `logName=projects/{p}/logs/{log_id}`, `resource.type=generic_node` + labels, `jsonPayload` = whole entry, `PRIORITY`→severity (7..0 → DEBUG..EMERGENCY), 250 KiB quota truncation, `MESSAGE` re-parsed as JSON if it is JSON | `gcp_stackdriver_logs` sink: `project_id`, `log_id`, `resource.type/labels`, `severity_key` | **Near-complete.** Severity mapping: Vector maps a severity field; journald's inverted 0-7 scale needs a small VRL map. Quota truncation and "MESSAGE is JSON" unpacking are VRL work. |
| `rsyslog` | TCP, optional TLS 1.2+ with client cert; RFC5424 `<pri>1 ts host app procid msgid [sd]msg`, RFC3164, or `CUSTOM` `logline` template; facility from `SYSLOG_FACILITY`, severity from `PRIORITY`, app from `SYSLOG_IDENTIFIER`>`SYSTEMD_UNIT`>`PROCESS_NAME`; **non-transparent (newline) or RFC 6587 octet-counted** framing; `max_message_size` truncation | `socket` sink (tcp, TLS incl. client cert) + `encoding.codec`. A `syslog` encoder (RFC5424/3164) exists in recent Vector **[verify version]**; otherwise `codec: raw_message` with the line built in VRL. Framing options: `newline_delimited`, `character_delimited`, `length_delimited` (4-byte binary prefix), `bytes` | **Partial.** Newline framing: fine. **Octet-counted RFC 6587 framing is not a Vector framing method [verify]**; could be faked by prefixing `"{len} "` in VRL with `framing: bytes`, but that is fragile. `CUSTOM` logline → VRL template, straightforward. |
| `websocket` | WebSocket **client**; each batch = messages joined with `\x00`, optionally **snappy-compressed as one unit**, sent as one binary frame; headers `User-Agent: journalpump/x`, `Journalpump-Batch-Size`, `Journalpump-Compression`; permessage-deflate option; mTLS; **SOCKS5**; reconnect backoff | `websocket` sink (client) sends **one event per frame**, text or binary via codec; TLS yes; custom headers **[verify]**; no SOCKS5; no snappy | **Gap.** The receiving side (presumably an internal Aiven service) speaks the NUL-batch + snappy protocol. Vector cannot produce it. Either the receiver grows a plain one-event-per-frame mode or this stays on journalpump. |
| `logplex` | HTTPS POST, basic auth `token:<logplex_token>`, `Content-Type: application/logplex-1`, **`Logplex-Msg-Count: N`** (per batch), body = concatenated **octet-counted RFC5424 frames** with fixed PRI 190, `verify=False`, fire-and-forget | `http` sink: static headers, batch body via codec/framing | **Gap.** `Logplex-Msg-Count` must equal the batch size; Vector headers are static (templated headers **[verify]**, but batch-count is not a template variable). Octet-counted concatenation needs the same VRL hack as rsyslog. Heroku Logplex itself is legacy; check if anyone still consumes it. |
| `file` | Append raw JSON lines | `file` sink, `codec: json` or `raw_message` | **Complete.** |

Summary: 5 of 8 senders map cleanly (kafka minus socks5, ES/OS minus retention,
cloudwatch, GCL, file); rsyslog is clean only with newline framing; websocket and
logplex are not reproducible.

## 2. Source and semantics

**Journald source.** journalpump uses libsystemd directly (Python bindings today, a
dlopen binding in Go) and supports `journal_path`, `journal_files`, `journal_flags`,
`journal_namespace`, `units_to_match`, `journald_filters` (arbitrary `FIELD=value`
matches), `initial_position` (head/tail/seconds-from-boot). Vector's `journald` source
**shells out to `journalctl --follow`** (needs the binary installed; one subprocess
per source) and offers `journal_directory`, `journal_namespace`, `include_units`,
`exclude_units`, `include_matches`, `exclude_matches`, `current_boot_only`,
`since_now`, `extra_args` **[verify which of these exist in your version]**.
`journal_files` has no direct equivalent (possibly via `extra_args: ["--file", ...]`
**[verify]**). Seconds-from-boot seek has no equivalent.

**Field handling (the biggest hidden cost).** journalpump normalises every entry:
strips leading underscores (`_PID`→`PID`, `_SYSTEMD_UNIT`→`SYSTEMD_UNIT`), converts
types (`PID`, `PRIORITY`, `GID`, `AUDIT_*`→int; `REALTIME_TIMESTAMP`→float seconds;
`BOOT_ID`/`MACHINE_ID`/`MESSAGE_ID`→UUID strings; monotonic timestamps→null), adds a
synthetic naive-UTC ISO `timestamp`, adds `__SEQNUM`, merges `extra_field_values`,
replaces oversize entries (>1 MiB) with an `{error, partial_data, timestamp}` stub,
and renders undecodable bytes as `repr()` strings. Vector passes journald fields
through **with their original names as strings**, adds `host`, `timestamp`,
`message`, `source_type`. Every Kafka consumer, ES mapping and CloudWatch filter that
expects journalpump's schema would break unless a `remap` VRL step rebuilds it. That
is perhaps 40-60 lines of VRL, but it must be written, tested and kept identical
across the fleet. Repeated journald fields and >4 KiB binary fields: Vector's
behaviour depends on the `journalctl` output mode it uses **[verify]**.

**Filters.**

| journalpump | Vector |
|---|---|
| `field_filter` whitelist/blacklist, per sender | `remap` with `only_fields`/`del()` placed in front of each sink. Equivalent. |
| `unit_log_level` (glob on unit, drop above a priority), per sender | `filter` transform with VRL `match()` + integer compare, per sink. Equivalent. |
| `searches` (regex per field, named groups→statsd tags, callable tags incl. `ip_to_geohash`; **a reader with searches forwards only matching entries**) | `filter` + `log_to_metric` transform gives counters with tags. Regex named captures: VRL `parse_regex`. MaxMind lookup: enrichment tables (geoip) exist; **geohash encoding has no VRL function [verify]**, would need a custom VRL or lose that tag. |
| `secret_filters` (regex redaction on `MESSAGE`) + match counter | VRL `replace()` + `log_to_metric`. Equivalent. |
| `match_key`/`match_value` global and per-reader | `include_matches` or `filter`. Equivalent. |
| `extra_field_values` | `remap` set fields. Equivalent. |

**Cursor and state file (the hard requirement in the migration).** journalpump
keeps a state file with one `sent.cursor` **per sender**, saves it every ~10 s, and on
startup resumes from the *last sender's cursor in key order* (a documented quirk the
charter locks), or from the journal head if any sender has no cursor. Vector keeps
**one checkpoint per journald source**, advanced only when every downstream sink has
acknowledged (if `acknowledgements` is enabled), stored in its own `data_dir`
**[verify file layout]**. Consequences:

- The formats are unrelated. Operators **cannot switch back and forth**; a one-way
  cutover script that extracts one cursor from `journalpump.json`'s state and seeds
  Vector's checkpoint is feasible, but the reverse (Vector→journalpump) is not
  something you would want to maintain.
- Vector's model is actually *safer* (min-across-sinks rather than last-in-key-order),
  but it is a behaviour change, and the Go port's charter explicitly forbids fixing
  this quirk. If you adopt Vector you are consciously accepting new resume semantics.
- Vector adds something journalpump lacks: optional **disk buffers per sink**, so a
  sink outage does not stall the journal read for the other sinks (in journalpump a
  full in-memory buffer on one sender pauses reading for the whole reader).

## 3. Operational

| Area | journalpump (Python/Go) | Vector | Note |
|---|---|---|---|
| Reload | SIGHUP; reload only if config file ctime changed; rebuilds statsd, geoip, filters and all readers of a changed reader | SIGHUP or `--watch-config`; topology diff, rebuilds only changed components, validates before swapping | Vector is better. |
| sd_notify / watchdog | `Type=notify`, READY/RELOADING/STOPPING/WATCHDOG=1 | **Not supported as far as I know [verify]**; unit would be `Type=simple` without watchdog | Loss of liveness supervision unless you add an external health check (Vector has an HTTP `api` health endpoint). |
| Metrics | statsd UDP, telegraf tags; names `journal.read_lines`, `journal.sent_lines`, `journal.sent_bytes`, `journal.last_sent_ago`, `journal.status`, `sender.failed_to_start`, `exception`, per-search counters | `internal_metrics` source → `statsd` sink (exists) or `prometheus_exporter`. Metric names are Vector's (`component_sent_events_total`, ...) | Every dashboard and alert on journalpump metric names must be rebuilt. Vector's metrics are richer (per-component buffer depth, errors, lag). |
| SOCKS5 to Kafka | Yes (charter D4, custom dialer in Go) | **No** (librdkafka) **[verify]** | If any Kafka target is reached only via the SOCKS5 proxy, Vector cannot replace that sender without a network-side change (e.g. a local proxy that terminates SOCKS5, or routing). |
| Config | single `journalpump.json`, readers→senders tree | TOML/YAML/JSON, sources/transforms/sinks graph | Mechanical translation script is feasible; the VRL normalisation and per-sink filter chains are hand-written once and templated. Estimate: a few days of work plus validation per sink type. |
| Running-config dump, `write_running_config` | Yes | No equivalent (Vector has `vector validate` and the GraphQL API) | Minor. |
| Existing in-house Vector use | — | Already deployed, operated, upgraded, and understood internally | **This is the strongest argument for Vector**: one tool, one skill set, one upgrade train. |

## 4. Non-technical

- **License.** Vector is MPL-2.0 with no open-core split that I am aware of; Datadog's
  commercial "Observability Pipelines" is a separate managed control plane on top of
  Vector, not a feature-gated build **[verify current state]**. journalpump is
  Apache-2.0 and yours.
- **Maturity and backing.** Vector has been Datadog-backed since 2021, has a large
  user base and active release cadence (roughly every 4-8 weeks, still 0.x versioned).
  Risk: it is a vendor-owned project; roadmap priorities follow Datadog's product.
  journalpump's bus factor is your own team.
- **Upgrade cadence.** Vector's frequent releases occasionally change defaults and
  deprecate options; you already carry that cost for the existing Vector fleet, so the
  marginal cost is small. journalpump changes only when you change it.
- **Footprint.** Rust Vector: static binary roughly 100+ MB on disk, tens of MB RSS
  idle, more with disk buffers; Go journalpump: ~15-30 MB binary, low tens of MB RSS;
  Python journalpump: interpreter plus kafka-python, boto3, google client, so the
  highest RSS of the three and no libsystemd dlopen concerns. Task 31 (benchmark
  harness) was skipped, so there are no measured numbers for Go on this branch. Vector
  also spawns `journalctl` per source.
- **Sunk cost vs ongoing cost.** The Go port is 26 tasks approved, packaging in
  review, ~3 tasks left (docs/NEWS, parity sweep, CI systest). The *remaining* cost to
  ship is small. The *ongoing* cost of journalpump is owning eight sink clients
  (franz-go, AWS SDK, Google API, websockets, TLS, syslog) and their security updates
  forever; Vector amortises that across its whole community. Two tools on the fleet
  means two upgrade trains, two metric vocabularies, two on-call runbooks.
- **Strategic direction.** If the organisation intends to converge on Vector for
  pipelines, journalpump is the odd one out and every year it survives is a year of
  divergence. The question is whether the four gaps (websocket protocol, logplex,
  socks5-kafka, octet-counted syslog) are live requirements or historical baggage.

## 5. Pros and cons

**Pros of adopting Vector**

1. Already operated internally: one tool, existing expertise, shared upgrade and
   security process.
2. Native, well-maintained sinks for Kafka, ES/OpenSearch, CloudWatch, GCL, file,
   TCP/TLS; no bespoke client code to own.
3. Disk buffers and end-to-end acknowledgements: a dead sink no longer stalls the
   journal read for every other sink, and restarts do not lose the in-memory buffer.
4. Safer resume semantics (checkpoint advances on all-sinks-acked) versus
   journalpump's last-in-key-order quirk.
5. Better reload (topology diff, pre-validation), richer internal metrics, and VRL
   is more expressive than journalpump's fixed filter set.
6. Removes a Python/Go codebase, its tests, packaging, and the charter's quirk-locks.

**Cons of adopting Vector**

1. **Cannot reproduce the websocket sender protocol** (NUL-batched, snappy, custom
   headers, SOCKS5). If an internal receiver depends on it, Vector is a non-starter
   for that path without changing the receiver.
2. **No Logplex sink**; `http` sink cannot set the per-batch `Logplex-Msg-Count` or
   octet-count frames faithfully.
3. **No SOCKS5 for Kafka**; the migration charter treats socks5-kafka as a
   non-negotiable (D4), which signals a live use.
4. **No RFC 6587 octet-counted syslog framing**; only newline-framed syslog is clean.
5. **Schema change at every sink**: journald field names and types differ unless a
   VRL normalisation layer rebuilds journalpump's output exactly; all consumers,
   mappings, and dashboards are affected.
6. **State file incompatibility**: one-way cutover only, no fallback to journalpump
   without a replay or a hand-seeded cursor.
7. No sd_notify/watchdog integration (as far as I know); ES index retention and
   Kafka topic auto-create move elsewhere.
8. Metric names change; every alert and dashboard on `journal.*` is rewritten.
9. Throws away a nearly finished, reviewed Go port with a parity test suite.
10. Vector depends on `journalctl` being present and on Datadog's roadmap.

**Pros of finishing the Go port** (the status quo plan)

- Three small tasks from done; behaviour, config, state file, metrics and packaging
  all preserved; zero downstream change; zero retraining of consumers.
- Keeps the four hard-to-replace senders working.

**Cons of finishing the Go port**

- You continue to own eight sink clients and a libsystemd FFI binding indefinitely.
- No disk buffers, in-memory at-least-once only, quirky resume semantics locked in.
- Divergence from the internal Vector standard grows.

## 6. Recommendation

**Finish the Go port now; pilot Vector on the Kafka path in parallel; decide
consolidation on the pilot's evidence.** Concretely:

1. **Complete Tasks 28-30** (docs/NEWS, parity sweep, CI systest) and ship the Go
   binary. This is days of work, de-risks the Python end-of-life, and loses nothing.
   Do not start a Vector cutover from the Python binary: you would be doing two
   migrations at once.
2. **Answer the two unknowns that decide everything** (you can; I cannot from the
   repo):
   - Are the `websocket`, `logplex`, and `socks5_proxy` senders still configured
     anywhere? If none are live, the Vector gap list shrinks to octet-counted syslog
     and schema/metrics changes, and consolidation becomes attractive.
   - Can downstream consumers (Kafka topics, ES indices, CloudWatch filters) accept
     either a schema change or a VRL-normalised schema that you commit to maintain?
3. **Run a bounded pilot**: one host class, Vector `journald` → `remap` (journalpump
   schema) → `kafka`, with disk buffer, `internal_metrics` → `statsd`. Measure: field
   fidelity against the Go parity fixtures already in `systest-go/`, RSS/CPU vs the Go
   binary (this also discharges the skipped Task 31), reload behaviour, and the
   cutover script from journalpump state → Vector checkpoint. Verify every
   **[verify]** item above against your deployed Vector version during the pilot.
4. **Then choose**:
   - If the gap senders are dead and the pilot is clean: migrate sink by sink (Kafka,
     then ES/OS, CloudWatch, GCL, file, newline-framed syslog), retire journalpump at
     the end. The Go port then served as a short-lived bridge; that is an acceptable
     outcome because its cost was already paid.
   - If websocket/logplex/socks5 are live: **hybrid**. Vector for the mainstream
     sinks, Go journalpump kept only for the gap senders, with a plan to retire each
     gap (teach the websocket receiver a plain one-event-per-frame mode, drop Logplex,
     fix the network path for SOCKS5). Accept that two tools coexist until then, and
     set a sunset date so the hybrid does not become permanent.
   - Do **not** fork Vector to add custom sinks; that recreates the maintenance burden
     you are trying to shed, in a language the team may not staff for.

What I would not do: abandon the Go port today in favour of a Vector big-bang. The
port is almost free to finish, and Vector's known gaps (websocket protocol, logplex,
socks5, octet counting, state file) are exactly the places where a big-bang would
fail in production.
