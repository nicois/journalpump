# Root cause: why the Go port uses Python-level CPU (Task 31 investigation, 2026-10-13)

**Verdict up front.** The Go port is not a "port of Python's architecture"
in any deep sense — it is a *transliteration of Python's data model and
serialization code*. The read path builds a `[]Field` of boxed `any`
values, copying every field value three times, then re-escapes every key
and every value through a `bytes.Buffer` one rune at a time. Measured in
isolation, the pure-Go transform costs **18-20 µs per entry, ~2.5-4x
SLOWER than CPython's 6-7 µs** for the identical work. That is the entire
per-entry CPU gap: the transform is the one place a compiled language
must win, and we lose there. Nothing outside the transform is
architecturally wrong — the cgo binding and the poll pacing are ordinary.

The expectation "idiomatic Go should be 1-3 µs/entry and 10-100x faster
than Python" is reachable for the transform (we measured an 8.8x
serializer headroom in Go alone), but **not for the whole daemon**: the
per-entry cost has a hard floor of ~5-7 µs of libsystemd reads and cgo
crossings that Python's C extension also pays. Realistically this task
is worth ~3-4x total CPU, not 10-100x. The current 1.0x is indefensible;
the 10-100x target is not achievable against a C-extension reader.

Evidence below; **nothing has been optimised.**

## Contents

1. [Method](#method)
2. [Per-entry budget](#per-entry-budget)
3. [The hot lines (`pprof -list`)](#the-hot-lines-pprof--list)
4. [cgo overhead](#cgo-overhead)
5. [Allocations](#allocations)
6. [Algorithmic smells](#algorithmic-smells)
7. [Is the data model wrong?](#is-the-data-model-wrong)
8. [Micro-benchmark: the transform in isolation](#micro-benchmark-the-transform-in-isolation)
9. [Verdict and plan](#verdict-and-plan)

## Method

Same host and fixture as `docs/perf-baseline.md` (`e8bfe10d6f4c`,
300,000 entries, 16 fields per entry — verified with
`journalctl --file ... -n1 -o json`; MESSAGE 80-400 B ASCII, every 1000th
entry carrying one invalid UTF-8 byte). Evidence sources:

- The whole-run CPU and heap profiles captured by `make bench-profile`
  (`go-cpu-measured.prof`, `go-mem-measured.prof`): 12.03 s of samples
  over a 73.55 s run (16.4 % CPU busy) and 3.75 GB / 47.4 M allocations.
- `go tool pprof -list`, `-peek`, `-tree` and `-sample_index=alloc_space`
  over those profiles (quoted verbatim below).
- The isolated transform micro-benchmark,
  `internal/journal/transform_bench_test.go` (Go) and
  `bench/transform_microbench.py` (Python) — no pacing, no cgo, no I/O.

The per-entry basis is the **serial** measurement (GOMAXPROCS=1), not the
default-GOMAXPROCS one, so parallel GC cannot flatter or inflate any line
item: **8.62 s / 300,000 entries = 28.7 µs of CPU per entry**
(docs/perf-profile-findings.md). Python's is 8.94 s / 300,000 = 29.8 µs.

## Per-entry budget

Two independent methods agree. Percentages are shares of the 12.03 s
whole-run profile; µs are on the 28.7 µs/entry serial basis.

| line item | profile share | µs/entry | source |
|---|---:|---:|---|
| JSON serialization (`Serialize` incl. escaping) | 26.4 % | ~7.6 | `-list` cum 3.17 s |
| field materialization, Go-side (`materialize`) | 8.9 % | ~2.6 | `-list` cum 2.86 s minus the cgo read |
| libsystemd read + cgo crossings (enumerate/next/cursor/realtime) | 14.6 % | ~4.2 | `runtime.cgocall` cum 1.75 s |
| value conversion (`ConvertEntry`/`convertValue`/`decodeUTF8`) | 7.0 % | ~2.0 | `-list` cum 0.84 s |
| entry key rewriting (`ProcessEntry`) | 2.3 % | ~0.7 | `-list` cum 0.28 s |
| handler glue, sender line copy, statsd, SetField | ~10 % | ~2.9 | remainder |

Cross-check against the **isolated micro-benchmark** (pure transform +
materialize-shaped field bookkeeping, no cgo):

| stage | Go ns/entry | allocs/op | B/op |
|---|---:|---:|---:|
| full transform (`ConvertEntry` → `ProcessEntry` → `SetField` → `Serialize`) | 18,234-20,322 (best 18,234) | 113 | 9,696 |
| of which `Serialize` alone | 13,903-15,352 | 73 | 5,688 |
| of which `ConvertEntry` | 2,757 | 34 | 1,392 |
| of which `ProcessEntry` | 1,468 | 4 | 1,448 |
| materialize-shaped field bookkeeping (16 tuples, no read) | 3,255 | 53 | 2,208 |
| **sum of the two Go halves** | **~21.5 µs** | | |
| cgo path (~20 crossings) | ~5 µs | | |
| **budget total** | **~26.5-28.7 µs** | | ✓ closes on the measured 28.7 |

The whole-run serial CPU per entry is therefore essentially *all*
transform: ~21.5 µs of pure Go plus ~5 µs of cgo, and the poll pacing and
libsystemd dominate wall time, not CPU.

## The hot lines (`pprof -list`)

`PythonJSONString` is 18.8 % of CPU and is called **once per key and once
per string value** — every call allocates a fresh `bytes.Buffer` and walks
the string rune by rune:

```
     520ms      2.26s (flat, cum) 18.79% of Total
         .      350ms    114:	out.WriteByte('"')
     270ms      270ms    115:	for _, r := range s {
      50ms      1.01s    137:			out.WriteRune(r)          // per-character loop
      40ms      140ms    149:	out.WriteByte('"')
         .      330ms    150:	return out.String()          // per-call copy
```

`Serialize` (26.4 % cum) calls it for the key and again inside
`appendPythonJSON` for the value, then `WriteString`s the *result* into
the output buffer — a second copy of every escaped string:

```
      40ms      3.17s (flat, cum) 26.35% of Total
         .      880ms     31:		out.WriteString(PythonJSONString(field.Key))
         .      120ms     32:		out.WriteString(": ")
      10ms      2.09s     33:		if err := appendPythonJSON(&out, field.Key, field.Value); err != nil {
```

`materialize` (23.8 % cum) drops a `memcpy` per field on the floor:

```
      70ms      2.86s    490:func (s *SDJournal) materialize() ([]journal.Field, error) {
         .      2.11s    493:		data, rc, err := s.h.enumerateData()
      10ms       10ms    497:		if rc == 0 {
      10ms       70ms    509:		key := string(data[:idx])
      10ms      220ms    510:		value := append([]byte(nil), data[idx+1:]...)
      30ms       30ms    513:			if fields[i].Key == key {      // O(n^2) duplicate scan
         .      370ms    528:			fields = append(fields, journal.Field{Key: key, Value: value})
```

The flat `bytes.(*Buffer).WriteByte` at 630 ms (5.2 %) and
`bytes.(*Buffer).tryGrowByReslice` at 240 ms are the same per-rune path
counted again in the buffer.

## cgo overhead

**Crossings per entry: 20.** From `sdjournal.go`: `next()` (1),
`materialize()` (16 data tuples + 1 terminating `rc == 0` = 17),
`realtimeUsec()` (1), `getCursor()` (1). Over 300,000 entries that is
**6.0 million cgo crossings**, plus a `sync.Mutex` lock and a map lookup
in `ffiSymbol` for each one.

Per crossing, from `-list`/`-peek`:

- `runtime.cgocall` flat **1.45 s** (`entersyscall` is inlined into it) —
  the Go-side scheduler bookkeeping the crossing forces; 1.45 s / 6.0 M =
  **242 ns per crossing**.
- cgo pointer checking: `runtime.cgoCheckArg` 0.28 s + `cgoInRange` /
  `cgoIsGoPointer` — free of charge in Python's C extension.
- `runtime.entersyscall` 0.18 s + `runtime.exitsyscall` 0.12 s.
- The C-side `_Cfunc_call_*` cums: `call_enumerate_data` 0.92 s,
  `call_step` 0.46 s, `call_get_cursor` 0.24 s, `call_get_realtime`
  0.06 s. `sd_journal_next` at 0.46 s / 300 k = **1.5 µs per entry is real
  journal work**, not overhead.

The whole cgo surface (`-tree -focus runtime.cgocall`) is **2.08 s of
12.03 s = 17.3 %**, or ~5 µs/entry. A Go→C crossing costs roughly
**240 ns of pure runtime bookkeeping** that python-systemd never pays,
because its reader is a C extension: same libsystemd calls, one C stack.

Additionally, `ffiSymbol` is called on *every* operation and costs
**0.51 s (4.2 %)** of CPU in map lookups (`mapaccess2_faststr` 0.30 s) and
mutex traffic (0.09 s lock + 0.05 s unlock) per call — pure, avoidable
overhead; the symbol pointers never change after the first `dlsym`.

**Could batching remove the crossings?** Partly. `sd_journal_enumerate_data`
is inherently one call per field — the sd-journal API has no
"give me the whole entry" call, and python-systemd's `_get_all()` is the
same loop in C. What *is* removable:

- the per-call `ffiSymbol` lookup (mutex + map) — cache the pointers once;
- the 1 MiB pooled buffer and its `memcpy` per field (see below);
- `sd_journal_get_cursor` per entry returns a `malloc`ed string we copy
  and free (0.24 s + a `C.free`), and `sd_journal_get_realtime_usec`
  could be replaced by reading `__REALTIME_TIMESTAMP` — which the
  enumerate loop *already returns* (the field is in the entry!). That is
  one crossing per entry removed for free.

There is no way to get the crossing count below ~17 per entry (one per
field) with this API, so **~4 µs/entry of cgo is a floor**, not a bug.

## Allocations

Whole run: **3.75 GB and 47.4 M allocations over 300,000 entries =
12.5 KB and 158 allocations per entry.** By site (`alloc_space`):

| site | bytes/entry | note |
|---|---:|---|
| `Serialize` line 33 `appendPythonJSON` (value escaping) | 2.72 KB | MESSAGE re-escaped through a Buffer |
| `Serialize` line 31 (key escaping) | 1.16 KB | 16 keys × a fresh Buffer + String copy |
| `enumerateData` line 379 `append([]byte(nil), buf[:length]...)` | 0.53 KB | copy #1 of every value |
| `materialize` line 510 `append([]byte(nil), data[idx+1:]...)` | 0.40 KB | copy #2 of every value |
| `convertValue` → `decodeUTF8` line 69 `string(raw)` | 0.34 KB | copy #3 of every value |
| `enumerateData` line 371 `enumBufPool.Get()` (1 MiB buffer misses) | 0.99 KB | 284 × 1 MiB allocations: the sync.Pool is emptied by GC |
| `SetField` line 54 (append growth, 2-3 calls/entry) | 0.85 KB | |
| `ProcessEntry` lines 12-13 (out slice + `map[string]int`) | 0.85 KB | a map per entry for 16 fields |
| `file.SendMessages` line 124 (`msg + "\n"` line copy) | 0.79 KB | |
| `Serialize` line 32 (`": "` via `WriteString`) | 0.58 KB | |
| `ConvertEntry` line 42 (out slice) | 0.39 KB | |
| `ProcessEntry`/`appendix` `any` boxing (`runtime.convT`) | — | every value boxed into `any` |

Three structural causes:

1. **Every field value is copied three times** before it is used —
   enumerate buffer → new slice → materialize copy → `string()` decode —
   379 MB of pure copying (10 % of all allocated bytes) before any
   serialization, and `Serialize` copies it twice more (escape into a
   Buffer, then `WriteString` into the output).
2. **`PythonJSONString` allocates a `bytes.Buffer` per key and per
   value** and returns a fresh `string`. Idiomatic Go appends straight
   into the output buffer: measured below at 1.7 µs vs 15.4 µs.
3. **Per-entry map and slices** (`ProcessEntry`'s `map[string]int`,
   `ConvertEntry`'s new slice, two `SetField` append-growths) where a
   single reused `[]Field` would do.

A low-allocation idiomatic version needs: one reusable per-journal
read buffer (64 KiB is the journald data threshold, not 1 MiB), field
values as **subslices of that buffer** (valid only until the next
enumerate — fine, since serialization happens per entry), byte-oriented
append serialization into a pooled output buffer, a linear scan instead
of the per-entry map, and no `any` boxing on the hot path.

## Algorithmic smells

- **`materialize`'s duplicate scan is O(n²)** (sdjournal.go:512): for each
  field it re-scans all fields already collected. At 16 fields that is
  136 comparisons per entry = 40.8 M over the run = **~40 ms, 0.3 % of
  CPU**. Real but *not* a bottleneck at this schema; it would matter for
  entries carrying many repeated fields (a whole-message CONSOLE log
  with per-line keys). The cheap fix (dedupe only when
  `len(fields) > 1` and a key repeats, or index once) is worth doing for
  robustness, not for CPU.
- **`ffiSymbol` per call** (0.51 s, 4.2 %): mutex + string map lookup on
  every crossing. Cache once per process.
- **`sd_journal_get_realtime_usec` per entry** when
  `__REALTIME_TIMESTAMP` is already among the enumerated fields (and is
  then overwritten by `SetField` anyway).
- **`enumBufPool` 1 MiB buffers** partly defeat themselves: 284 pool
  misses allocate 1 MiB each (298 MB, 7.6 % of all bytes) and
  `memclrNoHeapPointers` flat is 350 ms.
- `defer enumBufPool.Put(...)` inside the per-field hot loop.

## Is the data model wrong?

The load-bearing question. Classification per element:

| element | class | why |
|---|---|---|
| `[]Field` ordered slice (vs `map[string]any`) | **(a) required** | key order is on the wire and observable; Python emits dict construction order |
| `Value any` boxing of every field | **(c) incidental** | a `[]byte` subslice or a typed union would do; nothing observable changes |
| per-field `[]byte` copy in `materialize` | **(c) incidental** | the bytes live in the read buffer until the next enumerate |
| `key := string(data[:idx])` per field | **(c) incidental** | keys are ASCII; they could be compared as bytes |
| per-entry `map[string]int` in `ProcessEntry` | **(c) incidental** | 16 fields scan linearly |
| new slice per `ConvertEntry` | **(c) incidental** | conversion could be in place |
| `bytes.Buffer` + `String()` per escaped string | **(c) incidental** | appending to the output buffer yields identical bytes |
| `PythonJSONString`'s escape *rules* | **(b)/(a)** | the exact escape bytes are observable parity (`ensure_ascii=True`, lowercase hex, no HTML escaping) — the rules must stay, the implementation must not |
| `checkMatch` linear scan, `SetField` position-preserving update, extra-field merge order, field-filter Keep order | **(a) required** | all observable |
| cgo per-field enumerate | **(a) required** | the sd-journal API offers nothing else |

So: **the data model is (c).** We mirrored Python's shape — a
`dict[str, object]` becomes a slice of `{string, any}`, and Python's
`json.dumps` becomes a hand-written re-escaper over a `bytes.Buffer` per
string — and in doing so inherited Python's allocation pattern without
inheriting its C-level implementation. The only parts that must survive a
rewrite are the *order* and the *escaping rules*; both are cheap to keep.

## Micro-benchmark: the transform in isolation

`internal/journal/transform_bench_test.go` (Go, `-benchtime=2s`,
best of three on the same shared host) and
`bench/transform_microbench.py` (CPython 3.14.7, `python3 -I`,
best of five live runs). Both build the same 16-field entry, apply the
converters, strip leading underscores, add the handler's `timestamp`,
and emit `json.dumps(..., ensure_ascii=True)`-equivalent bytes (~880 B).

| implementation | ns/entry | allocs/op | vs Python |
|---|---:|---:|---:|
| Go, current pipeline | **18,234-20,322** | 113 | **2.4-4.0x slower** |
| Python, current pipeline | **6,180-13,344** (typical ~7,500) | — | 1.0x |
| Go, current `Serialize` alone | 13,903-15,352 | 73 | |
| Go, reference append-based serializer (measurement only) | **1,736** | 6 | **~8.8x faster than our serializer** |

Caveats stated plainly: CPython's `json.dumps` is the C encoder, not
bytecode, so this is "hand-written Go vs C", not "Go vs interpreter"; and
the host is shared (load average 1-2.5 during measurement), which is why
the Python figure is a range. The conclusion is robust to both — even
Python's worst sample (13.3 µs) beats Go's best (18.2 µs), and the
reference serializer shows the headroom is ours to take.

The Go reference serializer is byte-exact by construction (ASCII fast
path; anything needing an escape falls back to `PythonJSONString`), so
the 8.8x is not a correctness trade.

## Verdict and plan

**Root cause: our transform, not the architecture of the daemon.**
Specifically, three implementation decisions inherited from Python:

1. per-string `bytes.Buffer` + `String()` escaping (18.8 % CPU, 1-3
   allocations per string), instead of appending into the output buffer;
2. three copies of every field value plus per-entry maps/slices
   (materialize → field → convert; `map[string]int`; two `SetField`
   growths) — 158 allocations per entry;
3. per-crossing `ffiSymbol` lookup and a 1 MiB pool that GC keeps
   emptying.

Inherent, **not** our fault: ~17 libsystemd enumerate calls per entry and
their cgo crossings (~4-5 µs/entry), and the poll-paced wall time. Python
pays the same libsystemd work through a C extension with no crossing.

### Achievable CPU

| scenario | transform | cgo/read | total per entry | vs today | vs Python (29.8 µs) |
|---|---:|---:|---:|---:|---:|
| today | ~21.5 µs | ~5 µs | **28.7 µs** | 1.0x | 1.0x |
| reference serializer only | ~8 µs | ~5 µs | ~13 µs | 2.2x | 2.3x |
| full idiomatic rewrite (est.) | ~3-4 µs | ~4-5 µs | **~8-9 µs** | ~3.3x | ~3.5x |

A 3-4x CPU win is realistic; 10-100x is not, because the libsystemd read
and the cgo crossings are a hard floor. Wall time will move little either
way — it is poll-paced, ~71-74 s for both implementations (within the
10 s sampling bound).

### Option A — targeted fixes (low parity risk, ~2-3x)

1. `Serialize`/`PythonJSONString` → append into a pooled output buffer
   with an ASCII fast path, keeping the exact escape rules and the
   `PythonJSONString` fallback. Removes 63 % of the transform
   (15.4 µs → 1.7 µs measured here) and most of the GC it drives.
   Parity risk: none (byte-exact fallback); the golden-file tests cover it.
2. `materialize` → keep values as subslices of one reused read buffer, no
   per-value copies, linear dedupe; `ConvertEntry` in place; no `any`
   boxing. Parity risk: low (lifetime discipline is the risk; the
   enumerate buffer must not outlive the entry).
3. Cache `ffiSymbol` pointers; drop `sd_journal_get_realtime_usec` in
   favour of the enumerated `__REALTIME_TIMESTAMP`; shrink the read
   buffer to 64 KiB. Parity risk: low, but the realtime change touches
   entry semantics — needs the parity tests plus a review of the
   `TimestampFromRealtime` path.
4. `ProcessEntry` without the map; file sender writes `msg` then `'\n'`.

### Option B — re-architecture (highest ceiling, higher parity risk)

Stream the enumerate loop directly into the JSON output (never
materialize a `[]Field`), keeping only the small amount of state the
observable semantics need (duplicate-key collapsing, the field filter,
unit log levels, extra fields, key order). This removes materialize,
conversion and the per-entry slices entirely and could reach the
~8 µs/entry estimate. Parity risks are real and must be handled
explicitly: key order across duplicate collapsing, the cross-sender
`jsonObjects` cache contamination quirk, `SetField("timestamp", …)`
position, and the unit-log-level drop path. This is a design change, not
a patch; it needs an architect spec and a cross-review before any code.

**Recommendation:** Option A first (bounded, byte-exact, and it captures
most of the win), then re-measure at GOMAXPROCS=1 to confirm the *serial*
work fell. Treat Option B as a separate, spec-driven follow-up. Both need
a decision on scope from the user; **no optimisation has been made.**

## Task 32 outcome — ledger closed (2026-10-13)

The streaming-pipeline rewrite (design `docs/design-journal-pipeline.md`,
plan Task 32) takes the streaming path: the handler now consumes raw
batch entries and emits JSON straight from the field byte views, with no
`[]Field`, no per-entry maps and no JSON re-parse on the cache-hit path.

Re-recorded baseline (same host, same 300,000-entry fixture,
`bench/results/baseline-2026-10-13-e8bfe10d6f4c.json`):

| metric | before (Task 31) | after (Task 32) |
|---|---:|---:|
| Go whole-run CPU (default GOMAXPROCS, median of 5) | 13.35 s | **6.88 s** |
| Python whole-run CPU, same run | 8.94 s | 8.47 s |
| Go/Python CPU ratio | 1.49 (Go worse) | **0.81 (Go better)** |
| Go whole-run wall (median) | 74.51 s | 76.53 s (poll-paced, within the 10 s bound) |
| Go peak RSS | 253.5 MiB | 252.0 MiB |

Whole-run CPU now sits below CPython's with non-overlapping run ranges
(6.36-7.00 s vs 8.15-8.79 s); the earlier "+49 % worse" finding is gone.
The per-entry transform micro-benchmark on the same 16-field fixture
falls from 23,045 ns/op, 9,696 B/op, 113 allocs/op (old chain) to
4,607 ns/op, 520 B/op, 16 allocs/op (`BenchmarkEmitJSON`,
`internal/journal/transform_bench_test.go`) — about 5x faster and 7x
fewer allocations, short of the ~4-alloc target because the emitter still
allocates the payload per filter key (shared across senders) and the
cache map.

The §2.6 in-process severity pre-filter (design §2.6.1) is implemented
in `internal/journal/prefilter.go`, built once per reader from its sender
targets and consulted in `ProcessRaw` after the searches/match/secret
gates. The debug-heavy pruned-path micro-benchmark is recorded
(`transform_bench_test.go`, `*`/INFO config, `PRIORITY=7`, 16-field
fixture): a pruned entry costs ~79 ns/op, 0 allocs/op on the Go side
(tier-A byte scan ~78 ns/op, 0 allocs/op) versus full emission — the
whole-run saving is the ~2.5-3 µs/entry of un-run emission (design
§2.6.3). The pre-filter is inactive in this baseline config (no
`unit_log_level`), so the committed numbers above are unaffected.

Step 7 cleanup was ruled **option B** (coordinator, 2026-10-13): the old
converted reader/handler chain is retained as the semantic-equivalence
oracle (nothing on it is provably dead), the `ReadBatch` adapter as a
test seam, and `enumBufPool`/`enumerateData`/`materialize`/`readNext` as
live positioning code; each is marked in code. Rewiring the positioning
paths onto `ReadRaw`, the `-tags poison` batch-buffer guard, and any
further deletion are recorded follow-ups, not part of Task 32.

## Reproduction

- `make bench-profile` — whole-run CPU + heap profiles.
- `go test -run '^$' -bench . -benchmem ./internal/journal/` — the Go
  transform micro-benchmark (incl. the reference serializer).
- `python3 -I bench/transform_microbench.py 100000` — the Python half.