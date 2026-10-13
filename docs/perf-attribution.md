# Performance attribution: where the Go port's CPU and allocations go

Task 32 follow-up (review item 16) plus the coordinator's attribution
investigation. **Report only — nothing in the production path was
optimised to produce this document.** The one code artifact added is a
measurement-only benchmark/test file,
`internal/journal/jsonv2_bench_test.go`.

Host `e8bfe10d6f4c` (Intel Core Ultra 7 255H, 16 CPUs), governor
`powersave`, `BENCH_ALLOW_ANY_GOVERNOR=1`. Fixture: 300,000 entries
(`bench/gen_journal.py`, seed 20261012), stream SHA-256 `4b473ddd…`,
config hash `5c45fb26…`, `GOMAXPROCS` default (16) unless stated.

## 0. Headline

| metric | Go | Python |
|---|---:|---:|
| whole-run CPU, median of 5 (s) | 6.49 | 9.04 |
| whole-run wall, median of 5 (s) | 75.56 | 71.21 |
| CPU per entry (µs) | **21.6** | **30.1** |
| wall per entry (µs) | 251.9 | 237.4 |
| CPU busy (profile) | 6.98 s / 76.55 s = **9.1 %** | — |
| allocation per entry (objects) | **46.0** | — |
| allocation per entry (bytes) | **4.67 KB** | — |
| live heap (in-use) after run | 5.2 MB | — |

The workload is **poll-cycle-bound**, not CPU-bound: the reader drains up
to `buffer.ChunkSize = 5000` entries per `readMessages` call and then
waits on `sd_journal_wait` for the next journal event, which for this
static fixture is a 1 s timeout (`daemon.pollInterval = 1 s`). That caps
throughput at ~4000-5000 entries/s and leaves the process ~9 % busy. The
21.6 µs/entry CPU figure is therefore the CPU-saturated floor by
construction (§5 measures a variant where the pacing is removed).

## 1. Whole-run CPU attribution — it adds up to ~23 µs/entry

`go tool pprof` over one profiled run (`make bench-profile`,
`JOURNALPUMP_CPU_PROFILE`), total samples **6.98 s over a 76.55 s run =
23.3 µs/entry**. Disjoint buckets at the top of the call tree:

| bucket | cum (s) | µs/entry | share |
|---|---:|---:|---:|
| reader goroutine (`startReader.func1` → `run` → `readSingleRaw`) | 5.52 | 18.4 | 79.1 % |
| ⤷ libsystemd read via cgo (`runtime.cgocall`) | 1.54 | 5.1 | 22.1 % |
| ⤷ `Handler.ProcessRaw` | 3.54 | 11.8 | 50.7 % |
| ⤷ ⤷ `EmitJSON` (serialization) | 2.30 | **7.7** | 33.0 % |
| ⤷ ⤷ entry gates (unit/priority + search/match/secret lookups) | 1.24 | 4.1 | 17.8 % |
| ⤷ reader glue + `ParseBatch` (residual) | 0.44 | 1.5 | 6.3 % |
| GC background (`gcBgMarkWorker` 0.66 + `bgsweep` 0.17 + `bgscavenge` 0.05) | 0.88 | 2.9 | 12.6 % |
| sender loop (`Loop.Run` → file write) | 0.33 | 1.1 | 4.7 % |
| runtime init + other goroutines | 0.25 | 0.8 | 3.6 % |
| **total** | **6.98** | **23.3** | 100 % |

The reviewer's "~18 µs unattributed beyond `BenchmarkEmitJSON`'s 4.6 µs"
decomposes as: **5.1 µs libsystemd cgo read**, **4.1 µs handler gates**,
**2.9 µs background GC**, **1.5 µs reader glue/`ParseBatch`**, **1.1 µs
sender**, **0.8 µs runtime init** — and the emitter itself costs **7.7 µs
in the whole run, not 4.6 µs** (the micro-benchmark reuses its `dst`
buffer; the pipeline calls `EmitJSON(nil, …)` and must allocate and grow a
fresh payload per entry, which is why the same emitter measures higher
under real allocation pressure).

### Inside `EmitJSON`'s 7.7 µs

| site | cum (s) | µs/entry |
|---|---:|---:|
| `appendFieldValue` → `appendSingleValue` (value rendering) | 1.04 | 3.5 |
| ⤷ `PythonFloatRepr` (REALTIME_TIMESTAMP) | 0.33 | 1.1 |
| ⤷ `appendStringValue`/`appendJSONString` (string values) | 0.41 | 1.4 |
| `appendJSONString` (keys) | 0.63 | 2.1 |
| `EmitJSON.func4` (timestamp synthesis → `time.Format`) | 0.34 | 1.1 |
| `EmitJSON.func3` (synth-wrapper closures) | 0.51 | 1.7 |
| `EmitJSON.func1` (`emitKey` closure) | 0.32 | 1.1 |
| `lastStrippedIndex` (O(n²) dict-collision scan) | 0.18 | 0.6 |

Roughly a third of the emitter's cost (float repr, UUID canonicalisation,
ISO timestamp, `ensure_ascii` escaping) is Python-fidelity work: it
reproduces `json.dumps(ensure_ascii=True)` byte-for-byte, which the
charter's "byte-identical where free" clause does not require.

### 1.1 cgo crossings and channel/buffer ops per entry

- **cgo crossings: 1 per entry.** The C batch loop (`jp_read_batch`,
  `ffi.go`) exists to amortise the cgo boundary over a whole batch, but
  the streaming pipeline calls `raw.ReadRaw(1)` once per entry
  (`daemon.readSingleRaw`, `pump.go:1207`) — batch size 1 — so the live
  path pays **one cgo crossing per entry**, and `runtime.cgocall` is the
  single largest flat frame (1.49 s, 21.4 % of samples). Only the
  positioning paths (`applyInitial`/`Seek`, which use the batched
  `ReadBatch` adapter) realise the batch ABI's amortisation. Batching the
  streaming read (e.g. `ReadRaw(N)` with a per-entry walk) is the one
  change that directly attacks this 5.1 µs/entry.
- **channel/buffer ops: 1 channel send + 2 mutex sections per entry.**
  Each emitted payload is `SenderBuffer.Enqueue`d — one `b.ch <- item`
  channel send plus a `b.mu` lock/unlock on the reader side, and one
  `drained` mutex pass on the sender side (`buffer.go:107-137`); the
  enqueue path also runs `Loop.TrackEnqueue` → `rawString`
  (`json.Marshal`, ~3.2 objects/entry). Total sender-side CPU is only
  ~1.1 µs/entry (`sender.Loop.Run` cum 0.33 s), so the channel/mutex
  machinery is not a hot spot — its cost is the per-item `json.Marshal`
  in `rawString`, not the channel itself.

## 2. Whole-run allocation attribution

`go tool pprof -sample_index=alloc_space|alloc_objects` over the same run:

- **13,792,474 objects / 300,000 = 46.0 objects/entry.**
- **1401.5 MB / 300,000 = 4.67 KB/entry.**
- **5.2 MB live** at the end (in-use) — the 4.67 KB/entry is almost all
  garbage; GC is doing heavy turnover for a tiny steady-state heap.

This is already a **3.4× improvement** on the Task 31 converter-path
figure of 158 objects/entry (the raw `ReadRaw` → `ProcessRaw` → `EmitJSON`
path replaced the `materialize` conversion chain). Disjoint buckets:

| phase | objects/entry | share | top sites |
|---|---:|---:|---|
| `readSingleRaw` total (reader goroutine) | 44.8 | 97 % | |
| ⤷ `Handler.ProcessRaw` | 34.5 | 75 % | |
| ⤷ ⤷ `EmitJSON` | 19.7 | 43 % | `appendJSONString`, `canonicalUUID`, `strconv.FormatFloat`, timestamp synthesis, `dst` growth |
| ⤷ ⤷ entry gates | 7.5 | 16 % | `convertedRawValue` → `append([]byte)` + `convertValue` → `any` |
| ⤷ ⤷ `emitRaw` per-sender cache + closures | 6.2 | 13 % | `cache := map[string]rawCacheEntry{}`, `targetExtraFragments`, `concatFragments` |
| ⤷ `daemon.pipeline` (map + slice + closure + `NewHandler`, **per entry**) | 8.5 | 18 % | `resolvedByName` map, `targets` slice, `AddItem` closure, `journal.NewHandler` |
| ⤷ ⤷ sender enqueue (`TrackEnqueue` → `rawString` = `json.Marshal`) | 3.2 | 7 % | `sender.rawString`, `Loop.durableCursorLocked` |
| ⤷ reader `ReadRaw` / batch | 1.1 | 2 % | `readBatchRaw` |
| logging + other | 1.9 | 3 % | `journaldlog.(*Log).Named` 1.4 |
| **total** | **46.0** | 100 % | |

### `BenchmarkEmitJSON`'s own 16 allocations/op

`go test -bench BenchmarkEmitJSON -memprofile` (4.11 M objects / 300 k
samples ≈ 13.7/op sampled, 16/op reported), by site:

| site | objects/op | what it is |
|---|---:|---|
| `canonicalUUID` + `hex.DecodeString` + `strings.Builder` | ~2.4 | `_BOOT_ID`/`_MACHINE_ID` UUID canonicalisation |
| timestamp synthesis (`EmitJSON.func4` → `time.Time.Format` + builder) | ~2.5 | the synthesised `timestamp` field |
| `appendJSONString` | 1.7 | key/value escaping |
| `strconv.FormatFloat` | 1.1 | `REALTIME_TIMESTAMP` |
| `strings.genSplit` | 0.9 | `ParseSeqnumFromCursor` splitting the cursor |
| `pythonIntString` + `strconv.AppendUint` | 0.9 | `SEQNUM` |
| `EmitJSON` flat (`dst` growth, `[]byte(key)`) | 1.3 | output buffer + synth keys |
| `MakeNoZero` | 1.1 | buffer backing allocations |

## 3. Three JSON encoders on the same 16-field entry

`internal/journal/jsonv2_bench_test.go`, median of 5, `-benchmem`,
`GOMAXPROCS=16`, on the entry `bench/gen_journal.py` produces (realtime,
MESSAGE 240 B, UUID ids, the converter-hit keys):

| encoder | ns/op | B/op | allocs/op | vs hand-rolled |
|---|---:|---:|---:|---:|
| **hand-rolled `EmitJSON`** (production) | 5,012 | 520 | **16** | — |
| **`encoding/json/v2` over a struct** | 3,371 | 1,473 | **3** | 1.5× faster, 5.3× fewer allocs |
| **`encoding/json/v2` over a map** | 7,224 | 1,184 | **23** | 1.4× slower, 1.4× more allocs |
| reference: `EmitJSON` into a reused buffer (`serializeRef`) | 1,796 | 154 | 6 | 2.8× faster |

Findings:

- **`json/v2` over a struct is the only encoder that meets the ≤8
  allocs/op gate at the emitter level (3).** But it is **not a drop-in**:
  - **Field order is fixed by declaration.** Verified: `v2.Marshal` emits
    struct fields in declaration order
    (`TestJSONv2StructFieldOrder`). The production payload's keys are in
    *raw journald field order* (data-dependent) with the synthesised
    `REALTIME_TIMESTAMP`/`SEQNUM`/`timestamp` appended — a struct cannot
    express "whatever fields this entry happens to carry", so unknown
    journald fields would be silently dropped.
  - **Separators differ**: `v2` emits compact `,`/`:`; Python emits
    `", "`/`": "`. Byte diff, semantically equal
    (`TestJSONv2StructMatchesEmitJSONModuloSeparators` proves the struct
    output is byte-identical to `EmitJSON` after those two replacements).
  - **Non-ASCII is not escaped**: `v2` emits raw UTF-8, Python emits
    `\uXXXX` (`TestJSONv2NonASCIIIsNotPythonEscaped`). Byte diff.
  - Net: a `v2` struct encoder is **semantically admissible** under the
    charter amendment but **fails the byte-exact fixtures** (the canary
    would log diffs, not fail) and **cannot represent the dynamic field
    set**. It is not a candidate for the wire as-is.
- **`json/v2` over a map is strictly worse than the hand-rolled emitter**
  (23 vs 16 allocs/op, slower) and its output order is **non-deterministic**:
  `TestJSONv2MapOrderNotDeterministic` observes 3 distinct orderings in
  200 marshals (Go map iteration randomness leaks into the bytes). A map
  encoder can never satisfy a byte-exact canary.
- The hand-rolled emitter already beats the struct encoder on bytes
  (520 vs 1,473 B/op) and on speed for the fixed-buffer case.

**Recommendation:** keep the hand-rolled emitter (only it can reproduce
Python's escaping, separators and raw field order) and cut its
allocation count directly (§7). Do **not** adopt `json/v2`.

## 4. Maps and dynamic lookups still in the hot path

The offset-index reader (`RawField`/`RawEntry`, `raw.go`) is
**boxing-free by construction** — "nothing here owns memory, every offset
and length is a view into `Batch.Buf`". The boxing is one layer up, in the
*handler gates* that read those views:

| site | per entry | notes |
|---|---:|---|
| `daemon.pipeline` — `make(map[string]resolvedSender,…)` | 1 map + 1 slice + 1 closure + 1 `Handler` | rebuilt **every entry** though its inputs (senders, filters, extras) are construction-time |
| `journal.emitRaw` — `cache := map[string]rawCacheEntry{}` | 1 map (+entries) | the per-sender json-cache quirk; for the 1-sender bench it holds one entry |
| `journal.effectiveUnitPriority` → `rawGroupValue` → `groupFieldValue` → `convertedRawValue` | ~7.5 objects | two lookups (SYSTEMD_UNIT, PRIORITY); each copies the value (`append([]byte(nil), …)`) and boxes the result into `any` via `convertValue`; `groupFieldValue` allocates a `[]any` for the (rare) duplicate-list case |
| `journal.lastStrippedIndex` | 0 allocs, O(n²) time | rescans all fields per field for the dict-collision rule |
| `filter.FieldFilter.KeepBytes` | 0 allocs | already byte-slice based |
| `journal.ProcessEntry` — `make(map[string]int, len(fields))` | oracle path only | **not live** in the raw streaming path |
| `converters.go` `map[string]func(any)(any,bool)` | oracle path only | the raw path uses the linear `converterTable` instead |

**The offset-index path does not box, but the gate lookups on top of it
do** — `convertedRawValue`/`groupFieldValue` allocate a fresh `[]byte`
copy and an `any` box per lookup, ~7.5 objects/entry across the two
`effectiveUnitPriority` lookups. That is the largest remaining
struct-vs-boxing win in the handler.

## 5. I/O-bound or CPU-bound? The CPU-saturated variant

The production harness is deliberately poll-paced: `readMessages` drains
`ChunkSize = 5000` entries, then `run` waits `pollInterval` (1 s) on
`sd_journal_wait`. 300,000 / 5000 = 60 cycles → the ~75 s wall.
The harness **refuses** `JOURNALPUMP_POLL_INTERVAL_MS` ("production
binaries only"), so the saturated variant is built and driven directly:

- **Go**: `go build -tags systest` (the existing test-only poll seam) +
  `JOURNALPUMP_POLL_INTERVAL_MS=1`, run against the same fixture.
- **Python**: `python -m journalpump` with a `sitecustomize` that sets
  `JournalPump.poll_interval_ms = 1` (mirrors the Go seam; measurement
  shim, `/tmp`, not committed).

| variant | entries/s | µs/entry wall | µs/entry CPU |
|---|---:|---:|---:|
| Go, paced (production) | 3,970 | 251.9 | 21.6 |
| Go, **saturated** (3 runs) | 70,000-71,000 | **14.1-14.3** | 15.7-16.2 |
| Python, paced (production) | 4,213 | 237.4 | 30.1 |
| Python, **saturated** (3 runs) | 54,000-56,000 | **18.4-18.5** | 18.8-19.9 |

**Answer:** the workload is I/O/poll-bound, so in production the wall is
~250 µs/entry for both implementations regardless of language — the
language barely matters there (Python is in fact ~6 % *faster* on wall,
because its drain cadence interacts slightly better with the poll loop).
Once the pacing is removed the language does show up, but **the Go
advantage is only ~1.27× on wall (14.1 vs 18.4 µs/entry) and ~1.2× on
CPU** — far short of what a compiled daemon would imply, because both are
dominated by per-entry allocation + GC + the transform, and Go's
allocation churn (46 objects/entry) is what eats the compiled-language
advantage.

*Comparability:* both saturated runs drive the same 300,000-entry
fixture, same reader+transform+file-sender shape, same host, and each
includes its implementation's own reader FFI (libsystemd for Go,
python-systemd for Python) — so the per-entry figures measure the same
work: read one entry, convert/rewrite it, serialize it, hand it to the
file sender. The one asymmetry is that Python's `sitecustomize` shim is
measurement-only scaffolding outside the repo, whereas Go's
`JOURNALPUMP_POLL_INTERVAL_MS` seam is the existing test-only build tag;
neither changes the per-entry work.

## 6. Verdict

**(c) both, but the dominant term is avoidable CPU, and the wall is
I/O-bound.**

- In production the wall is **I/O/poll-bound** (§5): Go and Python are
  within ~6 % of each other and neither saturates a core.
- Go's CPU is nonetheless **~1.3-1.4× below Python's** overall, and per
  the attribution in §1-§2 that CPU is **not** mostly inherent: only
  ~5.1 µs/entry is the unavoidable libsystemd read and ~1.1 µs is the
  sender write. The rest is our code and the GC it provokes —
  **~7.7 µs/entry of JSON emission**, **~4.1 µs/entry of gate lookups**,
  **~2.9 µs/entry of background GC**, all driven by **46 allocations per
  entry**, of which roughly a third (the float/UUID/timestamp/escaping
  Python-fidelity work) is required only for *byte-exact* parity the
  charter has already relaxed.
- So: **the ceiling is I/O-bound, but our per-entry CPU is roughly
  twice what it needs to be, and that keeps the compiled port at parity
  with the interpreter rather than ahead of it** — exactly the design
  observation flagged in `docs/perf-profile-findings.md`, now with a
  fully-attributed mechanism.

Realistic achievable ratios, if §7 is done: CPU-saturated wall ~9-11
µs/entry Go vs ~18 µs/entry Python (≈1.7-2.0×); production wall unchanged
(poll-paced) but with ~2-3× less CPU-seconds, which matters on busy hosts,
under a lower `pollInterval`, or with many readers.

## 7. Ranked fixes (not applied)

Ranked by µs/entry recovered per unit of risk; all behaviour-preserving
byte-for-byte unless noted.

1. **Build the daemon pipeline once, not per entry.**
   `daemon.pipeline` allocates a `map`, a slice, a closure and a fresh
   `journal.Handler` on every entry, though all of its inputs are
   construction-time. Cache it and rebuild only on reload.
   *Recovers ~8.5 objects/entry ≈ 2-3 µs/entry CPU + GC.* **Lowest risk,
   highest alloc payoff.**
2. **Cut `EmitJSON`'s allocations** (16 → target ≤8): precompute the
   synthesised key `[]byte`s as package vars, replace the three
   `emitSynth` closures with straight-line code, append
   `PythonFloatRepr`/`canonicalUUID` results without the intermediate
   `[]byte`, and make `ParseSeqnumFromCursor` take `[]byte`.
   *Recovers ~8 objects/entry ≈ 1.5-2 µs/entry.* Medium risk (byte-exact
   output must be re-pinned by `TestSerializeGolden`).
3. **Typed gate lookups.** Replace `rawGroupValue`/`convertedRawValue`
   (`any` + `[]byte` copy) with a lookup that returns the raw value view
   plus its converter class, and only materialise a fallback string for
   the `check_match`/`unit_log_level` comparisons that actually need one.
   *Recovers ~7.5 objects/entry ≈ 1-1.5 µs/entry.* Medium risk.
4. **`emitRaw` cache:** for the single-sender (common) case use a
   stack-allocated 1-entry cache instead of `map[string]rawCacheEntry`.
   *Recovers ~1 object/entry.* Low risk.
5. **Sender enqueue:** `rawString` (`json.Marshal` per item) and
   `durableCursorLocked` (`&c` escape) allocate per entry.
   *Recovers ~3 objects/entry.* Low risk.
6. **`lastStrippedIndex` O(n²) → single precomputed map** (rebuilt only
   when a collision is seen), or fold the collision resolution into the
   batch parse. *Recovers ~0.6 µs/entry time, 0 allocs.* Low risk.
7. Re-measure `GOMAXPROCS=1` CPU/entry and the alloc count after 1-6; the
   plan gate (§8) should be re-armed against the post-fix numbers.

## 8. Item 16 — the ≤8 allocs/entry gate: evidence and proposed amendment

**Gate as written** (`docs/plan-python-to-go-migration.md:1127`):
"allocs/entry ≤ 8 (from 158; target ~4)".

**Measured (2026-10-13, this host, committed fixture):**

| scope | objects/entry | vs gate |
|---|---:|---|
| whole-run Go binary (`alloc_objects`, §2) | **46.0** | 5.75× over |
| `EmitJSON` alone (`BenchmarkEmitJSON`, §3) | **16** | 2× over |
| best available encoder (`json/v2` struct, §3) | **3** | meets it — but cannot emit the dynamic field set or Python's bytes |

**Disposition: the gate is EXCEEDED and cannot be met by the emitter
alone.** 46 objects/entry is distributed across four independent phases
(§2: emission 19.7, gates 7.5, per-entry pipeline 8.5, sender 3.2,
reader/logging 3.0); even a zero-allocation emitter leaves ~26. §7's
fixes are expected to reach ~15-20 objects/entry, still above 8.

**Proposed plan amendment (needs coordinator sign-off; not applied
here):** replace the single hard gate with a two-part, measured target
and record the achieved number:

> **Amended 2026-10-13 (Task 32 item 16, evidence in
> `docs/perf-attribution.md`):** the allocs/entry gate is restated as
> (a) **≤ 20 objects/entry whole-run** with the §7 fixes applied, and
> (b) **≤ 8 objects/entry for the emitter (`BenchmarkEmitJSON`)**, and
> (c) the default-`GOMAXPROCS` whole-run CPU-below-Python gate (already
> met: 6.49 s vs 9.04 s, non-overlapping ranges) stays as the headline
> performance gate. The original "≤ 8 whole-run" figure came from the
> Task 31 converter path (158 objects/entry) and is not reachable on the
> raw path without removing the per-entry pipeline construction, gate
> lookups, sender enqueue and the Python-fidelity byte work — none of
> which is free under the byte-exact canary.

I am **not** editing the plan: the plan is the architect/coordinator's
artifact and ledger rows 31/32 remain "review". This section is the
evidence package for that ruling.
