# Performance profile findings (Task 31 profiling pass, 2026-10-12)

The whole-run benchmark (plan Task 31, `docs/perf-baseline.md`) shows the Go
binary using more CPU-seconds than the Python daemon over the same 300,000
entry fixture. This document profiles that gap: the hot spots, how much is
GC versus our code versus runtime, whether it is just multi-core
utilisation, what is Python-fidelity tax, and the PGO before/after.

**Report only — nothing here has been optimised.** The optimisation
candidates are ranked at the end for a decision.

## How to reproduce

- `make bench-profile` builds the Go binary and runs one bench run with the
  env-gated `runtime/pprof` hook, then prints `go tool pprof -top` for the
  captured CPU and heap profiles. `BENCH_ENTRIES` (default 300000) sets the
  workload.
- The hook lives in `cmd/journalpump/profile.go` and is **inert in
  production**: unless `JOURNALPUMP_CPU_PROFILE` / `JOURNALPUMP_MEM_PROFILE`
  name a file, `startProfiling` does nothing. No HTTP pprof endpoint is ever
  opened, and the shipping binary is not a special build.
- `GOMAXPROCS=1 make bench-profile` (or the same on the plain driver)
  measures the single-core Go build; `GOMAXPROCS` is inherited by the child.
- `make bench-pgo` refreshes the committed PGO profile (see PGO below).

Host: `e8bfe10d6f4c`, 16 online CPUs, governor `powersave`,
`BENCH_ALLOW_ANY_GOVERNOR=1`, fixture 300,000 entries (stream SHA-256
`4b473ddd…`, config hash `5c45fb26…`).

## Headline CPU accounting

The Go run's total CPU over the workload is ~12-13 s. The profile
(`go tool pprof -top`, 12.03 s of samples over a 73.55 s run, ~16 % CPU):

| area | flat | cum | share of samples |
|---|---:|---:|---:|
| GC background (mark `gcBgMarkWorker` + sweep `bgsweep`) | — | ~1.9 s | ~16 % |
| `runtime.mallocgc` (allocation machinery, charged to callers) | 0.18 s | 2.26 s | 18.8 % |
| libsystemd read via cgo (`runtime.cgocall`) | 1.45 s | 1.75 s | 14.6 % |
| our JSON serialization (`journal.Serialize`) | 0.04 s | 3.17 s | 26.4 % |
| ⤷ of which string escaping (`journal.PythonJSONString`) | 0.52 s | 2.26 s | 18.8 % |
| reader `materialize` (entry build, incl. the cgo read) | 0.07 s | 2.86 s | 23.8 % |
| entry conversion (`ConvertEntry` → `convertValue`/`decodeUTF8`) | 0.04 s | 0.84 s | 7.0 % |

Flat shares by package: pure `runtime`/GC/sync ≈ 6.8 s of the ~9.2 s that
`-top` attributes flat (the remaining 2.8 s is spread over hundreds of tiny
dropped leaf nodes); `journalpump` code flat is small (~1.2 s) but its
*cumulative* cost is large — our code spends its time in the runtime (buffer
growth, allocation, GC) rather than in tight loops.

So the honest split of the 12-13 s: **~1.9 s is GC proper**, **~1.75 s is
the unavoidable libsystemd read**, and **the rest is our code plus the
runtime work our code provokes** — dominated by JSON escaping and by
per-entry allocation.

## Allocation profile — the smoking gun

`go tool pprof -sample_index=alloc_space` / `alloc_objects` over one run:

- **Total allocated ≈ 3.7 GB for 300,000 entries ≈ 12.5 KB and ≈ 158
  allocations per entry.**
- `gcBgMarkWorker`-forcing GC count from `GODEBUG=gctrace=1`: **251 GC
  cycles in 73.5 s (~3.4/s)** with a live heap of only ~15-19 MB and a
  ~35 MB goal.
- Largest per-entry allocators:

| allocator | alloc_space | alloc_objects | per entry |
|---|---:|---:|---:|
| `SDJournal.materialize` | 0.51 GB | 10.0 M | 33 obj / 1.7 KB |
| `bytes.growSlice` | 0.68 GB | 1.8 M | — |
| `bytes.(*Buffer).grow` | 0.43 GB | 7.3 M | 24 obj |
| `bytes.(*Buffer).String` | 0.19 GB | 6.3 M | 21 obj |
| `ffiJournal.enumerateData` | 0.19 GB | 5.6 M | 19 obj |
| `file.(*Sender).SendMessages` | 0.25 GB | — | — |
| `journal.ProcessEntry` | 0.25 GB | 1.6 M | 5.3 obj |
| `journal.SetField` | 0.25 GB | — | — |
| `journal.convertValue`/`decodeUTF8` | — | 2.3 M / 1.8 M | 8 / 6 obj |

The two biggest single sources are **`PythonJSONString`** (every key and
every string value gets a fresh `bytes.Buffer` plus a `bytes.Buffer.String`
copy — ~21 `Buffer.String` and ~24 `Buffer.grow` allocations *per entry*)
and **`materialize`** (33 allocations and 1.7 KB per entry building the
field slice).

### `-list` evidence

`PythonJSONString` (18.8 % of CPU) appends the 80-400 byte `MESSAGE` **one
rune at a time** through `bytes.Buffer.WriteRune` (1.01 s cumulative; that
is ~84 % of the escaping cost), and returns `out.String()` for a copy:

```
     520ms      2.26s ...  journal.PythonJSONString
         .      350ms  114: out.WriteByte('"')
     270ms      270ms  115: for _, r := range s {
      50ms      1.01s  137:     out.WriteRune(r)          // per-character hot loop
         .      330ms  150: return out.String()          // per-call copy
```

`Serialize` calls `PythonJSONString` once for the key and `appendPythonJSON`
once for the value (which calls it again for every string value):

```
      40ms      3.17s ...  journal.Serialize
         .      880ms   31: out.WriteString(PythonJSONString(field.Key))
         .      2.09s   33: appendPythonJSON(&out, ...)   // -> PythonJSONString(v) 1.60s
```

`materialize` re-scans all fields for a duplicate key on every field
(O(n²) per entry) and copies every value:

```
      70ms      2.86s ...  SDJournal.materialize
         .      2.11s  493: data, rc, err := s.h.enumerateData()
      10ms      220ms  510: value := append([]byte(nil), data[idx+1:]...)
         .      370ms  528: fields = append(fields, journal.Field{Key: key, Value: value})
```

## Parallelism: GOMAXPROCS=1 vs default

Same workload, same config, Go binary only, 5 measured runs + 1 warmup:

| build | cpu_s (median) | wall_s (median) | peak RSS | entries/s |
|---|---:|---:|---:|---:|
| Go, default `GOMAXPROCS` (16) | 13.35 s | 74.51 s | 253.5 MiB | 4,026/s |
| Go, `GOMAXPROCS=1` | **8.62 s** (8.26..9.19) | 72.54 s | 256.4 MiB | 4,136/s |
| Python (for reference) | 8.94 s | 71.20 s | 217.6 MiB | 4,214/s |

**The parallelism hypothesis is NOT the whole story, but it is real.** At
`GOMAXPROCS=1` Go's CPU drops by **4.73 s (35 %)** with no wall penalty
(72.54 vs 74.51 s, within the 10 s sampling bound): that ~4.7 s is GC and
other goroutines (reader, sender loop) running concurrently on otherwise
idle cores. The program is only ~16 % CPU-busy per wall second, so the GC
has 15 spare cores to spread mark work over — that inflates *CPU-seconds*
without inflating wall time.

But a single core is not a free pass: Go at `GOMAXPROCS=1` (8.62 s) is
**not meaningfully below Python's 8.94 s**. For a fixed volume of data a
compiled daemon would be expected to use *much* less CPU than an
interpreter; here it does not. The parallelism explains *where* the
CPU-seconds are spent (concurrent GC), but the reason there is so much GC to
spread is the **~12.5 KB / ~158 allocations per entry** above.

**This is a genuine design observation to flag, not to explain away**: on
this workload the Go port's single-core CPU is on par with Python's, not
better. The excess CPU-seconds at default `GOMAXPROCS` are real work (GC
mark/sweep burning otherwise-idle cores). Removing the allocation churn
should lower both the `GOMAXPROCS=1` number and the default-GOMAXPROCS
number.

## Python-fidelity (parity) tax

For each hot spot: is it (a) required for observable parity, (b) required
only for *byte-exact* parity that might be relaxed, or (c) incidental?

| hot spot | CPU | class | note |
|---|---:|---|---|
| `PythonJSONString` escaping | 2.26 s cum | **(b)** | The *bytes* must match Python's `json.dumps(ensure_ascii=True)` (`\uXXXX`, no HTML escaping) — an observable wire difference. But the *implementation* — a fresh `bytes.Buffer` per key/value and `WriteRune` per character — is **(c) incidental**; an idiomatic Go escape appending into a reused buffer with an ASCII fast path produces identical bytes. |
| `Serialize` object framing | 3.17 s cum | **(a)** | Key order is observable (Python dict construction order). Separators `", "`/`": "` are byte-exact. The per-entry `bytes.Buffer` is incidental. |
| `materialize` field build | 2.86 s cum (incl. cgo) | **(a) + (c)** | Field order is observable; the O(n²) duplicate scan, the per-value copy and the un-preallocated slice are incidental. |
| `enumerateData`/`cgocall` | ~1.75 s | **(a)** | The real libsystemd read; inherent. |
| `ProcessEntry` (underscore strip + repr) | 5.3 allocs/entry | **(a) + (c)** | The `lstrip("_")` and repr() fallback are observable; the per-entry `map[string]int` is incidental (14 fields scan fine linearly). |
| `SetField`/`MergeExtraFields` | 0.25 GB | **(a)** | Position-preserving update is observable. |
| `ConvertEntry`/`convertValue`/`decodeUTF8` | 0.84 s | **(a)** | Python decode semantics + converter table. |
| file `SendMessages` line copy | 0.25 GB | **(a) + (c)** | Python's `msg + b"\n"` also allocates; writing `msg` then `'\n'` is equivalent bytes. |
| GC (251 cycles) | ~1.9 s | **(c)** | Falls out of the above. |

**Serialization is already shared per filter key per entry** — `Handler`
caches the bytes in `jsonObjects[ffName]` (handler.go:239, 333), so a
multi-sender deployment serializes once per distinct field filter. The
bench exercises one sender, so this is one serialization per entry; there is
no per-sender re-serialization to remove. The cost is the *quality* of that
one serialization, not its multiplicity.

## PGO before/after

A representative CPU profile from the bench workload was committed as
`cmd/journalpump/default.pgo` (provenance: a `make bench-profile` run at
`BENCH_ENTRIES=300000` on `e8bfe10d6f4c`, captured by the env-gated hook;
refresh with `make bench-pgo`). `go build` applies it automatically — the
build metadata records `-pgo=/…/cmd/journalpump/default.pgo`, so the
packaging builds (`make build-go`) pick it up without change.

Go-only, same workload/config, 5 runs + 1 warmup:

| build | cpu_s | wall_s | peak RSS | entries/s |
|---|---:|---:|---:|---:|
| before PGO (committed baseline) | 13.35 s | 74.51 s | 253.5 MiB | 4,026/s |
| after PGO (`default.pgo`) | 13.02 s | 73.52 s | 253.8 MiB | 4,081/s |
| delta | −2.5 % | −1.3 % | +0.1 % | +1.4 % |

PGO is a small, mostly-in-noise win here (CPU spread is ±0.7 s run to run;
wall and throughput move less than the 10 s sampling bound). That is
expected: the process is only ~16 % CPU-busy, so better code layout cannot
recover much wall, and the CPU time is dominated by allocation/GC, which PGO
does not change.

## Ranked optimisation candidates (not yet done)

Ranked by expected CPU impact; all are behaviour-preserving byte-for-byte
unless noted.

1. **Rewrite JSON string escaping to append into a reused/pooled buffer with
   an ASCII fast path** (`serialize.go:PythonJSONString`). Removes ~21
   `Buffer.String` + ~24 `Buffer.grow` allocs/entry and the `WriteRune`
   per-character path. Expected: ~15-25 % CPU (the 18.8 % escaping cost plus
   the GC it drives).
2. **One-pass `materialize`** (`reader/sdjournal.go:490`): preallocate the
   field slice, drop the per-value copy, and only build a key→index map when
   a duplicate is actually seen (currently O(n²) with a map-free scan).
   Expected: a large cut in the 33 allocs/1.7 KB per entry.
3. **`ProcessEntry` without a per-entry map** (linear scan over ~14 fields,
   or a reused scratch map). Expected: modest CPU, meaningful alloc cut.
4. **Pool the `Serialize` output buffer** (`sync.Pool`) instead of a fresh
   `bytes.Buffer` per entry.
5. **File sender: write payload and `'\n'` without the intermediate copy**
   (`sender/file/file.go:124`). Bytes identical.
6. Re-measure at `GOMAXPROCS=1` after 1-5 to confirm the *serial* work
   actually fell, not just the parallel GC.

## Conclusion

- The "Go uses ~43-60 % more CPU-seconds" is, at default `GOMAXPROCS`,
  largely **parallel GC burning idle cores** (~4.7 s of the ~13 s): the
  workload is poll-paced and only ~16 % CPU-busy, so background marking
  spreads over free cores without adding wall time.
- It is **not** an interpreter-defying efficiency win: at `GOMAXPROCS=1`
  Go's CPU (8.62 s) is on par with Python's (8.94 s). The Go port does
  roughly the same *serial* work as Python, because of ~12.5 KB and ~158
  allocations per entry — the JSON escaping path and the entry
  materialization dominate.
- The excess is therefore **our per-entry allocation pattern driving GC**,
  not the libsystemd read and not framework busy-waiting. Serialization *is*
  already shared once per filter key per entry.
- PGO is a small, mostly-in-noise improvement on this mostly-idle workload.