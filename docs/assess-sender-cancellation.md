# Assessment: sender cancellation and the Task 22 (kafka) review loop

Date: 2026-10-11. Branch `python-to-go`. Read-only structural reassessment
requested after Task 22 reached a fourth review round on the same theme
(making cancellation/shutdown of the sarama-based kafka sender abortive).

Question asked: is this (A) a sarama limitation, (B) a porting-approach
problem, or (C) a framework/spec design problem?

## Verdict in one paragraph

Primarily **(A)**, with one real, cheap **(B/C)-class** contributor that
raised the stakes and should be fixed. sarama v1.45.2 is structurally not
context-aware: no producer/client API takes a `context.Context`, broker
dials use context-less `Dial`, and every retry/backoff is an uninterruptible
`time.Sleep`. No wrapper can make it stop *immediately*; the best achievable
is *bounded* (a few seconds, set by `Retry.Max x Retry.Backoff`). The kafka
sender's "tracking dialer + force-close + join" machinery is the only lever
sarama exposes and is the right shape, though it can be trimmed. The pattern
is **not** general: rsyslog and logplex have essentially zero bespoke
cancellation code and the `SendMessages(ctx, ...)` contract is the idiomatic
seam. What *is* a design gap: the supervisor joins senders with an
**unbounded** `senderGroup.Wait()` after cancel, so any sender whose library
cannot abort instantly becomes a process-hang risk. That turned "bounded" into
"must be perfect" and fed the review escalation. Fix the supervisor, write
down the latency bound, and close Task 22.

## 1. Evidence by hypothesis

### (A) sarama is not context-aware -- confirmed

All paths under `/home/claude-aiven/go/pkg/mod/github.com/!i!b!m/sarama@v1.45.2/`.

| Fact | Evidence |
|---|---|
| Only one public API accepts a ctx: `ConsumerGroup.Consume` | `consumer_group.go:46`; grep of `context.Context` over non-test files finds nothing in `client.go` (except an internal resolver closure, `:1229-1240`), `sync_producer.go`, `async_producer.go`, `broker.go` |
| Broker dial is context-less | `broker.go:203-204`: `dialer := conf.getDialer(); b.conn, b.connErr = dialer.Dial("tcp", b.addr)`; `config.go:875-886` returns either `Net.Proxy.Dialer` (`proxy.Dialer`, interface is `Dial(network, addr)` only) or a `*net.Dialer` whose `.Dial` is used, never `.DialContext` |
| Retry/backoff sleeps are uninterruptible | `async_producer.go:627` and `:667` (`time.Sleep`), `client.go:961` (metadata), `:1156` (coordinator), `:1205/1210` -- none select on a shutdown channel |
| `SyncProducer.SendMessages` blocks with no abort path | `sync_producer.go:146-147` waits on per-message expectation channels; no timeout, no ctx |
| `AsyncProducer.Close` drains, does not abort | `async_producer.go:372-395` -> `shutdown` `:1284-1302` waits on `inFlight.Wait()`, i.e. for retries to finish their sleeps |
| `Client.Close` does not interrupt in-flight requests or dials | `client.go:298-327`: stops the metadata updater, then `safeAsyncClose(broker)` -- asynchronous, no join |
| `Broker.Close` takes the same lock `Open` holds during a dial | `broker.go:321-337` vs `:179-187`; Close blocks until the dial times out |

Conclusion: with sarama, the **only** way to make a stalled produce/metadata
call return promptly is to close the underlying `net.Conn` from outside
(which makes the in-flight I/O fail) and then wait for sarama's retry chain to
exhaust. That is exactly what the builder built. The floor on abort latency
is therefore not zero; it is roughly
`Producer.Retry.Max x Producer.Retry.Backoff` (20 x 100 ms = 2 s with the
sender's values at `kafka.go:92-94`) plus
`Metadata.Retry.Max x Metadata.Retry.Backoff` (10 x 100 ms = 1 s) on the
metadata path. The round-3 tests assert <= 5 s (`round3_test.go`), which is
consistent with that floor. "Immediate" was never attainable; "bounded by a
few seconds" is.

### (B) porting approach -- mostly not, one real contributor

What is fine:

- The kafka sender does **not** swallow or mis-derive contexts. The ctx
  flows `SendMessages(ctx)` -> `ensureProducer(ctx)` -> `runCancellable` ->
  `connectProducer(ctx)` -> `newTrackingDialer(ctx, ...)`
  (`kafka.go:1054-1059, 939, 550-551, 677-686`). `context.Background()`
  appears only in the context-less `Dial` fallbacks sarama itself calls
  (`dialer.go:98`, `socks5.go:77`).
- Reconnection is **not** owned by the sender in a way that fights the
  framework: on error it drops the producer and returns
  (`kafka.go:1070-1072, 1193-1200`); the Loop classifies, backs off and
  retries (`sender.go:495-498`).
- `runCancellable` (`kafka.go:1009-1033`) is the standard Go idiom for
  wrapping a blocking, ctx-unaware call: run in a goroutine, select on
  `ctx.Done()`, abort the underlying I/O, join. This is what you would write
  around any legacy library.

What is a genuine contributor (shared by B and C):

- **The supervisor's join is unbounded.** `pump.go:1487-1490` cancels each
  sender then calls `p.senderGroup.Wait()` with no deadline; state save
  (`:1527-1533`) and exit happen only after it returns. Spec §4.3
  (`spec-python-to-go-migration.md:227-231`) prescribes exactly this
  ("cancel senders ... -> senderGroup.Wait() -> save state -> exit 0") with
  no hard stop. So the *implicit* contract on every sender is "return from
  `SendMessages` promptly on cancel or the daemon never saves state and
  never exits" (until systemd SIGKILLs it, at which point state is lost
  anyway). For a sender built on a ctx-aware library that is trivially met.
  For sarama it is only met by the full force-close machinery, and even then
  only *within a bound*, which is why reviewers kept finding residual
  tails (dials in flight, retries continuing after Close, a 5 s Close
  timer that abandoned a goroutine) and why each round demanded a stronger
  guarantee. The review standard escalated from "cancellation-aware"
  (round 1) to "join the worker" (round 2) to "genuinely ABORTIVE, not
  graceful; Close must not return on a timer" (round 3,
  `go-test-parity.md:1977-2010`) without ever stating a numeric latency
  bound that the limitation in (A) makes mandatory.
- Three stacked retry/backoff layers now exist on the kafka path: the
  framework Loop's jittered backoff (`sender.go:600-615`), sarama's internal
  `Producer.Retry`/`Metadata.Retry`, and the per-broker `backoffDialer`
  (`dialer.go:62-172`, added in round 2 for kafka-python
  `reconnect_backoff_ms` parity, `go-test-parity.md:1953-1954`). Each
  layer must be independently cancellable (`sleepCtx` at `dialer.go:40-52`
  exists for the third). This is where the line count accumulates
  (~260-300 lines of abort/retry plumbing in `kafka.go` + `dialer.go`,
  excluding config and formatting). It is parity-driven, not wrong, but it
  is the complexity the user is smelling.

Minor shape issues (not causes, but trim candidates):

- `watchCancel` (`kafka.go:533-543`) is a hand-rolled goroutine + stop
  channel; `context.AfterFunc(ctx, td.closeAll)` returns an equivalent
  `stop` with no goroutine and is the stdlib idiom (already used in the
  rsyslog path, `syslogframing/client.go:261-293`).
- Injecting the tracking dialer via `Net.Proxy.Enable = true` +
  `Net.Proxy.Dialer` (`kafka.go:689-690`) is the only hook sarama offers
  (`config.go:875-878`), but it bypasses `Net.DialTimeout`/`KeepAlive`/
  `LocalAddr` (`config.go:880-884`) and makes sarama log "using proxy" on
  every dial. The direct dialer at `dialer.go:269` re-implements the
  timeout. Document this; it will surprise the next reader.

### (C) framework / spec contract -- not the cause

- The contract is already the idiomatic seam:
  `SendMessages(ctx context.Context, batch [][]byte, cursor string) error`
  and `MaintenanceOperations(ctx) error` (`sender.go:83-86`; spec
  `:191-199`). The Loop passes its ctx to every sender call (`sender.go:466,
  495`), checks `ctx.Err()` per batch (`:486-488`), and its own backoff
  sleep is cancellable (`:600-615`). The Loop never calls `Close`/`Flush`;
  `Close()`/`Finalize()` are supervisor-side and ctx-less
  (`pump.go:1507, 1536`), which is fine because they run after the join.
- The other networking senders did **not** need bespoke cancellation:
  - rsyslog: `net.Dialer.DialContext(ctx, ...)` + `tls.HandshakeContext(ctx)`
    (`syslogframing/client.go:167, 186-187`); stalled writes are unblocked
    by `context.AfterFunc` -> `SetWriteDeadline(now)` (`client.go:261-293`,
    ~40 lines). That *is* the standard Go idiom for `net.Conn`, which has no
    ctx on `Write`; the one review round on it was about an AfterFunc race
    detail, not about design. `Close()` is `conn.Close()`, non-blocking
    (`rsyslog.go:416-419`).
  - logplex: `http.NewRequestWithContext(ctx, ...)` (`logplex.go:462`). One
    line. No `Close`, no goroutines, no abort machinery. Its review rounds
    were about inactivity-timeout semantics, not cancellation.
  - file: ignores ctx entirely (`file.go:120`); nothing to cancel.
- Therefore the pattern is **specific to the one sender built on a
  ctx-unaware library**, not a product of the framework. The user's main
  worry (every networking sender reinventing cancellation) is not borne out.

## 2. Is sarama actually uncancellable?

Yes for *immediate* cancellation; no for *bounded* cancellation.

- Idiomatic sarama shutdown is `producer.Close()` (drain, wait for retries)
  then `client.Close()`. It is a graceful drain by design
  (`async_producer.go:372-395, 1284-1302`). There is no abortive variant.
- The de-facto cancellation hook is a caller-supplied `Net.Proxy.Dialer`
  whose connections the caller can close out from under sarama. It is not
  documented as such; it just works because closing the socket fails the
  in-flight read/write and `trackingDialer` refuses re-dials
  (`dialer.go:201-204, 213-216`).
- After the socket is closed, sarama still runs its retry loop with
  uninterruptible sleeps before `SendMessages` returns errors. So the
  builder's machinery is working around an unavoidable property, not an
  avoidable one. What *is* avoidable is chasing "immediate" instead of
  stating and testing the bound.

## 3. Is the current abort machinery the right shape?

Right shape, over-built in places, and missing the one knob that shrinks the
floor.

Keep:
- `trackingDialer` + `closeAll()` (`dialer.go:174-254`) -- the only lever.
- `runCancellable` (`kafka.go:1009-1033`) -- correct wrap-and-join idiom.
- `Close()` aborts then joins without a timer (`kafka.go:1163-1175`,
  `liveProducer.Close` `:626-634`) -- correct given sarama's drain-on-Close;
  the join is bounded by the same retry floor.
- The client-side delivery deadline in `sendBatch` (`kafka.go:1085-1101`) --
  `Producer.Timeout` is broker-side and does not bound `SendMessages`.

Trim / adjust:
- Replace `watchCancel` with `context.AfterFunc`.
- Consider lowering `producerRetryMax` (20) toward sarama's default (3) or
  even 0-1. The framework Loop already retries the whole batch with its own
  backoff after `dropProducer()`, so sarama's internal retries are a second
  layer. Fewer internal retries = smaller abort floor (3 x 100 ms = 300 ms)
  and less duplicated logic. Trade-off: sarama's internal retry handles
  leader moves transparently (refresh metadata, re-send to the new leader)
  whereas surfacing the error to the Loop tears down the client and pays
  the Loop's 0.5 s+ backoff on each leader election. A value of 3-5 keeps
  transparent leader-move handling and still cuts the floor to ~0.5 s.
  This is an operator-visible behaviour knob; record it as a decision, do
  not slip it in.
- Do not add anything further to the kafka sender for cancellation. The
  remaining "tail" reviewers can find (retry sleeps after socket close) is
  the sarama floor and should be documented, not engineered around.

Replacement option (not recommended now): `github.com/twmb/franz-go` is
context-aware end to end (`ProduceSync(ctx, ...)`, a `Dialer` option that
receives the ctx, `Close` that cancels in-flight work) and would remove the
tracking dialer, `runCancellable`, `watchCancel` and the delivery-deadline
wrapper wholesale. It costs re-validating the Task 12 fixtures
(murmur2 key partitioning, snappy framing interop per spec risk 10), the
SCRAM and SOCKS5 adapters, and the systest broker provisioning that were
built around sarama. Worth it only if sarama causes a *second* class of
problem; cancellation alone does not justify it this late in Task 22.

## 4. Is the pattern general?

No. Evidence summary:

| Sender | Library ctx-aware? | Cancellation-specific code | Review rounds about cancellation |
|---|---|---|---|
| file | n/a | 0 lines (`file.go:120` ignores ctx) | 0 |
| logplex | yes (`net/http`) | 1 line (`logplex.go:462`) | 0 |
| rsyslog | yes (`net.Dialer`, `tls`) | ~40 lines, stdlib idiom (`client.go:261-293`) | 1 (race detail) |
| kafka | **no** (sarama) | ~260-300 lines, bespoke | 3 recorded + 1 in progress |

The framework contract and Loop are the same for all four. The variance is
entirely explained by the library.

The one general finding is the supervisor's unbounded join
(`pump.go:1490`), which is latent for *every* sender -- a wedged `net.Conn`
write without a deadline would hang it just the same. Today it is masked
because rsyslog/logplex use ctx-aware primitives.

## 5. Recommendation

Minimal change set, in priority order:

1. **Bound the supervisor join** (`internal/daemon/pump.go:1487-1490`,
   spec §4.3 `:227-231`). After cancelling senders, wait on
   `senderGroup.Wait()` for at most a hard stop (proposal: the larger of
   10 s and one kafka `requestTimeout`), then log which senders are still
   running, proceed to Finalize/state save/exit, and let the process end.
   This converts "every sender must be perfectly abortive" into "every
   sender must be abortive within the hard stop", which is the contract
   Go libraries can actually meet. Record it as a sanctioned deviation
   next to D9 in the charter. ~15 lines + 1 test. Risk: `buildState()`
   reads loop state while a wedged sender goroutine may still be running;
   loop state is already mutex-guarded for the supervisor path, so this is
   acceptable, but the test should run under `-race`.
2. **Write the kafka latency bound down** in `go-test-parity.md` Task 22
   and the charter ledger: "after cancel or deadline, `SendMessages` and
   `Close` return within `Producer.Retry.Max x Retry.Backoff +
   Metadata.Retry.Max x Metadata.Retry.Backoff` (~3 s at current values);
   this is a sarama limitation (uninterruptible `time.Sleep` at
   `async_producer.go:627,667`, `client.go:961,1156`; context-less dial at
   `broker.go:203-204`). Tests assert <= 5 s." This gives reviewers a
   fixed target and ends the escalation.
3. **Trim, do not extend, the kafka machinery**: `watchCancel` ->
   `context.AfterFunc`; add a comment at `kafka.go:689-690` explaining why
   `Net.Proxy.Enable` is abused as the dialer hook and what it bypasses.
   Optionally lower `producerRetryMax` to 3-5 as a recorded decision
   (operator-visible: leader-election behaviour).
4. **Close Task 22** once the round-4 items that are *within the bound* are
   addressed. Any further finding of the form "sarama still sleeps/retries
   after the socket is closed" should be answered by pointing at item 2,
   not by more code.

Should Task 22 continue as-is or be reworked? **Continue**, with items 1-3
applied. The sender is structurally correct for the library it uses; the
rework that would actually simplify it (franz-go) is a library swap, not a
redesign of the port, and is not justified by cancellation alone.

## Open questions for the user

- Hard-stop duration for the supervisor join (10 s? tied to
  `max_send_interval`? a config knob?). This is operator-visible.
- Whether to lower `producerRetryMax` (changes behaviour under leader
  election) or leave it at 20 and accept the ~3 s floor.
- Whether a franz-go spike is worth scheduling as a separate task after
  Task 22 closes, purely to delete the dialer/abort code.
