# Assessment: Go Kafka client for the journalpump kafka sender

Date: 2026-10-11. Branch: python-to-go. Read-only investigation; no code changed.

Companion: `docs/assess-sender-cancellation.md` (confirmed the cancellation problem
is a sarama limitation, not a design error on our side). This document answers the
follow-on question: *sarama was selected by default — is there a better client for
our requirements, and is switching worth it now?*

## Verdict

**Switch to `github.com/twmb/franz-go` (kgo + kadm + pkg/sasl).** Reject
`segmentio/kafka-go` and `confluent-kafka-go`.

How close is it? **Which client is better is clear-cut.** franz-go meets every one
of our eight requirements natively; sarama meets five and we have hand-built the
other three (cancellation, broker-version detection, SCRAM client) around it. The
only genuine argument for staying is sunk cost: Task 22 has passed three review
rounds and has an approved parity record. That argument is weaker than it looks,
because what the rounds bought us is a *workaround* (force-closing sockets under
the library through an abused proxy hook) with a documented 2-3 s floor and a
process-hang risk at the supervisor join — the thing the user is asking us to
reconsider. The switch is a bounded rewrite of one isolated package with an
existing ground-truth fixture to validate against, and the result is *less* code.

## Evidence summary

Sources: our code (`internal/sender/kafka/*`, `internal/sender/sender.go`,
`systest-go/`), the Task 12 fixture `kafka.json`, the module cache for
sarama v1.45.2 and eapache/go-xerial-snappy, and the upstream repositories at
pinned tags (franz-go v1.22.1, kafka-go v0.4.51, sarama v1.61.1) checked today.

### Maintenance status (as of 2026-10-11)

| | sarama (incumbent) | franz-go | segmentio/kafka-go |
|---|---|---|---|
| Latest release | v1.61.1, 2026-09-27 | v1.22.1, 2026-09-27 | v0.4.51, 2026-04-23 |
| Releases, last 12 mo | 16 | 18 | 3 (17-month gap before 2025-05) |
| Last default-branch commit | 2026-10-01 | 2026-10-07 | 2026-04-23 (2 commits in 12 mo) |
| Open issues / PRs | 15 / 42 | 3 / 4 | 182 / 84 |
| Maintainers | IBM (dnwe) + bots | one person (twmb), very high activity | ex-maintainers say "fork or use franz-go" (issue #1356) |
| License | MIT | BSD-3 | MIT |
| Direct deps | 12 incl. gokrb5, go-metrics (unconditional) | 3 (klauspost/compress, lz4, kmsg) | 5, pinned to 2022-era versions |
| Known adopters | Shopify origin; Redpanda Connect (still) | Redpanda, Redpanda Connect, Grafana Mimir + Loki (verified in their go.mod), Benthos | none verified |
| Go requirement | 1.26 | 1.26 (our go.mod: 1.27) | 1.23 |

kafka-go is effectively maintenance-only and still 0.x. Its `WriteMessages(ctx)`
godoc says a cancelled ctx *abandons the wait* — "messages ... might also still be
written after this method has already returned" — it does not abort the produce.
It also defaults to `RequiredAcks = RequireNone`. **Eliminated.**

confluent-kafka-go (v2.16.0, 2026-10-07) is a cgo binding to librdkafka:
`CGO_ENABLED=0` builds are impossible, C toolchain in CI, C crash surface.
Unacceptable for a static daemon binary. **Eliminated.**

franz-go's single-maintainer concentration is the one real risk on that column.
Mitigants: three direct deps, BSD, used by Grafana and Redpanda in production, 3
open issues, and the API has been stable at v1 since 2021.

### Requirement-by-requirement

Requirement 1 — context cancellation (the trigger).

- sarama: confirmed zero `context.Context` parameters on SyncProducer,
  AsyncProducer, ClusterAdmin, Client, Broker (`grep -c` over those files = 0 at
  v1.61.1 too — upgrading does not help). `Net.Proxy.Dialer` is
  `x/net/proxy.Dialer`, whose only method is `Dial(network, addr)`; the one
  context PR (#2217) was closed unmerged in 2023. Our workaround installs a
  `trackingDialer` through the proxy hook and force-closes its connections
  (`dialer.go`, `kafka.go:1007-1031`); floor is ~2-3 s of uninterruptible
  retry sleeps; the supervisor join is unbounded (`pump.go:1487-1490`).
- franz-go: `Produce(ctx, ...)`, `ProduceSync(ctx, ...)`, `Flush(ctx)`,
  `AbortBufferedRecords(ctx)`, `kadm.CreateTopics(ctx, ...)`, and
  `kgo.Dialer(func(ctx, network, host) (net.Conn, error))` — the dial ctx is
  "the context used in the request that caused the dial". Verified from the
  v1.22.1 godoc, not assumed: "Once a record is buffered into a batch, it can be
  canceled in three ways: canceling the context, the record timing out, or
  hitting the maximum retries. ... A record is not safe to fail if the client
  is idempotently producing and a request has been sent." **We produce with
  acks=1, which franz-go only allows with `DisableIdempotentWrite()`** (config
  validation: "idempotency requires acks=all"). So in *our* configuration
  batches are cancellable including after the request is sent — the
  idempotent-safety carve-out does not apply to us.
- Honest limit: the cancel check runs "before writing a request or after a
  produce response", so a record whose request is in flight to a stalled broker
  is failed when the response arrives, the request times out, or the connection
  closes. The supported shutdown path is therefore: on ctx.Done, call
  `cl.Close()` — it "closes all connections" and `failBufferedRecords(
  ErrClientClosed)`, so `ProduceSync` returns promptly. That is a documented
  library API, not a socket closed under the library's feet. Set
  `RetryBackoffFn` to a small bounded value so a retry sleep cannot delay the
  check beyond a few hundred ms.

Requirement 2 — socks5 / socks5h.

- Our `socks5.go` builds an `x/net/proxy` dialer from `proxy.FromURL` and
  already prefers `DialContext` when available (`socks5.go:102-130`); socks5h
  works by leaving the resolver nil so x/net resolves remotely. This transfers
  to franz-go unchanged: `kgo.Dialer` has "the same signature as
  net.Dialer's DialContext". Cleaner than today, because it is a first-class
  option, not the `Net.Proxy` hook that bypasses `Net.DialTimeout/KeepAlive`
  (which we had to re-implement, `kafka.go:683-686`).

Requirement 3 — snappy interop with kafka-python.

- Framing facts, verified in source: sarama v1.45.2 (`compress.go:174`) calls
  eapache `snappy.Encode`, documented "snappy with no framing header" — i.e. a
  **raw** snappy block (sarama `main` has since moved to klauspost
  `xerial.Encode`, which *does* frame). franz-go's default path is
  `s2.EncodeSnappy` — also a **raw** block; xerial framing is emitted only with
  the new opt-in `StreamingCompression()`. kafka-python's `snappy_decode`
  checks for the xerial magic and otherwise calls `snappy.decompress`, so it
  reads both.
- Conclusion: franz-go produces the *same* framing our systest already proved
  kafka-python reads. No interop risk; the interop systest
  (`systest-go/kafka_python_consumer.py`) must simply be re-run.

Requirement 4 — topic creation.

- Python: `create_topics` when both `num_partitions` and `replication_factor`
  are set; `TopicAlreadyExistsError` is logged and tolerated; the producer is
  re-initialised on send failure. Go today: `ClusterAdmin.CreateTopic`,
  tolerate `sarama.ErrTopicAlreadyExists` (`kafka.go:967-994`).
- franz-go: `kadm.Client.CreateTopic(ctx, partitions, rf, configs, topic)`
  returns `kerr.ErrorForCode(...)`; test `errors.Is(err, kerr.TopicAlreadyExists)`.
  Semantically identical; the ctx is a bonus.

Requirement 5 — broker version / `kafka_api_version`.

- We ported kafka-python's `_infer_broker_version_from_api_versions` and a
  pre-0.10 legacy probe (~230 lines, `kafka.go:664-900`) because sarama needs
  `Config.Version` set up front. franz-go sends ApiVersions on every new
  connection and caps each request key to what the broker supports; no
  version knob is needed. The `kafka_api_version` config pin maps to
  `kgo.MaxVersions(kversion.V2_6_0())` etc. (kversion has constructors for
  every release). `kversion.FromApiVersionsResponse(...).VersionGuess()`
  exists if we want to keep logging the detected version.
- Deviation to record: franz-go says "specific max version pinning is required
  if trying to interact with versions pre 0.10.0". Our legacy probe for
  pre-0.10 brokers (ListGroups/FindCoordinator/FetchOffset fallbacks) would
  be dropped; such brokers are a decade old and already unsupported by the
  current Python kafka-python line in practice. Operator-visible, so the user
  rules on it (see Open items).
- Note for the "stay" branch: sarama v1.46+ (2025-08) now also negotiates via
  ApiVersions, so even staying we could delete most of the detection code
  after a bump from v1.45.2.

Requirement 6 — produce semantics.

| Need | sarama today | franz-go |
|---|---|---|
| murmur2 keyed placement (`&0x7fffffff % n`) | our `partitioner.go` (93 lines) | default `UniformBytesPartitioner` hashes keys with Java-compatible murmur2, same mask and mod; or keep our partitioner via the 2-method `kgo.TopicPartitioner` interface |
| nil-key placement | random over all partitions (= Python `random.choice`) | default is sticky-per-64KiB (Java semantics), **not** random. Keep our partitioner to preserve Python behaviour; cost is nil since it is already written |
| acks=1 | `WaitForLocal` | `RequiredAcks(LeaderAck())` + `DisableIdempotentWrite()` (mandatory pairing) |
| linger 500 ms | `Flush.Frequency` | `ProducerLinger(500ms)`; `ProduceSync` "stops lingering and triggers an immediate drain" |
| one batch, ack-then-advance cursor | `SyncProducer.SendMessages` returns after all acked | `ProduceSync(ctx, recs...)` returns `ProduceResults`; `.FirstErr()` non-nil → fail batch, loop retries from head. Same at-least-once contract |
| request timeout 30 s / delivery 120 s / retries 20 | `Producer.Timeout`, our wrapper, `Retry.Max` | `ProduceRequestTimeout`, `RecordDeliveryTimeout`, `RecordRetries` — all first-class |
| SASL PLAIN, SCRAM-256/512 | sarama + our `scram.go` on xdg-go/scram | `pkg/sasl/plain`, `pkg/sasl/scram` in-tree, no external dep; `scram.go` and the xdg-go dependency are deleted |
| TLS (ca, certfile, keyfile, TLS1.2 min) | `Net.TLS.Config` | `kgo.DialTLSConfig(cfg)`; ServerName set per broker automatically |
| compression | `CompressionSnappy` | `ProducerBatchCompression(SnappyCompression())` |
| retriable error classification | ~30 `sarama.Err*` KError constants (`kafka.go:1273-1338`) | `kerr.Error.Retriable` field — the table collapses to one check plus a few overrides |

Requirement 7 — migration cost.

Current size: production 2064 lines (kafka.go 1534, dialer.go 279, socks5.go 120,
partitioner.go 93, scram.go 38); tests 2871 lines.

Expected movement:
- Deleted: `trackingDialer` and `runCancellable` machinery (~260-300 lines),
  version detection and legacy probe (~230), `scram.go` (38), most of the KError
  table (~60). Roughly 600 lines of production code removed.
- Kept nearly verbatim: `socks5.go`, `partitioner.go` (interface rename only),
  config parsing and validation (`kafka.go:230-480`), `topicConfig`,
  `normalizeKafkaVersion`, TLS builder, the sender's Loop integration and
  disconnect/drop-producer state machine.
- Rewritten: client construction (`buildSaramaConfig` → kgo opts, ~120 lines),
  connect/create-topic (~60), `sendBatch` (~60), shutdown path (~40).
- Tests: `kafka_test.go` uses `sarama/mocks.SyncProducer`; `round2/round3`
  use real sarama with fake brokers. These need a producer-interface fake (the
  `loop_test.go` `recordingProducer` pattern already exists) and/or franz-go's
  in-tree `pkg/kfake` in-process cluster for the stall/cancel tests.
  `partitioner_test.go`, `socks5_test.go`, config tests, and
  `kafka_e2e_test.go` (systest) carry over.
- Estimate: 5-6 builder tasks, roughly 1,200-1,500 lines touched, net
  production shrink of ~500 lines. Plus one full parity re-verification run.

Requirement 8 — risk and parity.

- Parity fixture (`fixtures/kafka.json`, Task 12) asserts payload bytes,
  key, **partition number**, contiguous offsets from 0, partition count, and
  sent cursor; `kafka_e2e_test.go` checks partition and key, not just values.
  Partition placement is therefore a hard parity assertion: keep our murmur2
  partitioner (not franz-go's default) so keyed and nil-key behaviour are
  byte-for-byte what Python does. Run 2 (keyed, 2 partitions) is the test that
  catches a partitioner regression.
- Compression framing: shown above to be the same raw-block output; the
  kafka-python consumer systest re-run is the proof.
- Behavioural differences to record as accepted deviations: (a) pre-0.10
  broker inference dropped; (b) franz-go retries with jittered exponential
  backoff by default (we pin `RetryBackoffFn` to the current 100 ms to keep
  the documented bound); (c) shutdown latency bound improves from "~2-3 s
  floor, ≤5 s asserted" to "≤ one request timeout, typically sub-second".
- Dependency surface shrinks: drops gokrb5, go-metrics, go-resiliency,
  xdg-go/scram, eapache/*; adds kmsg. All BSD/MIT.
- Residual risk: single upstream maintainer; mitigated by adoption breadth and
  our own interface boundary (`SendMessages(ctx)`), which is what makes this
  swap feasible in the first place and would make a future one equally so.

## If we stay on sarama (not recommended) — what we would be accepting

- Cancellation is bounded, never immediate: ~2-3 s floor from uninterruptible
  retry sleeps; the socket-force-close workaround through `Net.Proxy.Dialer`
  stays as permanent load-bearing code, and the supervisor join must be
  bounded separately (`assess-sender-cancellation.md` recommendation).
- ~300 lines of cancellation plumbing and ~230 lines of version inference
  remain ours to maintain (the latter could shrink after bumping to ≥ v1.46).
- An upgrade from v1.45.2 to current v1.61.1 changes snappy to xerial framing
  (klauspost `xerial.Encode`); harmless for kafka-python but it is a wire
  change that would itself need the interop systest re-run.

## Migration outline (for the plan, if approved)

1. Spike (throwaway, not merged): franz-go `ProduceSync` with
   `DisableIdempotentWrite` + `LeaderAck` against the systest broker; confirm
   (a) ctx cancel during a stalled produce returns within one
   `ProduceRequestTimeout` and `Close()` returns promptly, (b) kafka-python
   consumer reads the records. This is the go/no-go gate; it should take an
   hour and removes the remaining assumption risk before any rewrite.
2. Introduce a narrow `producer` interface in `internal/sender/kafka`
   (connect, produce batch, create topic, close) so tests fake it; port
   `loop_test.go`'s `recordingProducer` to it.
3. Replace `buildSaramaConfig`/connect/`sendBatch`/`maybeCreateTopic` with the
   kgo/kadm implementation; wire `socks5.go` into `kgo.Dialer`, our partitioner
   into `kgo.RecordPartitioner`, SASL via `pkg/sasl/*`, `kafka_api_version` via
   `kgo.MaxVersions`.
4. Delete `trackingDialer`, `runCancellable`, version inference, `scram.go`,
   the KError table; replace classification with `kerr` retriable flag.
5. Rewrite the cancellation tests (`round2/round3`) against `kfake` or the
   interface fake; keep the ≤5 s assertion and add a tighter one.
6. Parity re-verification: re-run `kafka_e2e_test.go` against
   `fixtures/kafka.json` (both runs) with the kafka-python consumer; update
   `docs/go-test-parity.md` and the charter ledger (Task 22 row) with the
   recorded deviations above and the new shutdown bound.

## Open items for the user

1. Approve the switch, and its timing (now, vs. after the remaining migration
   tasks land on sarama and revisit later). Recommendation: now — the
   sarama-specific cancellation machinery would otherwise be reviewed,
   documented and then thrown away.
2. Accept dropping pre-0.10 broker version inference (operator-visible
   deviation; Python still carries it).
3. Confirm whether the supervisor-join bound from
   `assess-sender-cancellation.md` should still be implemented regardless
   (recommended: yes, it is defence in depth independent of client choice).
