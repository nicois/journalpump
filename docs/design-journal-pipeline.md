# Design: the idiomatic Go journald → sinks pipeline

Status: **APPROVED by the user 2026-10-13**, with the three rulings in §7
(revision of the 2026-10-13 proposal; the open questions it carried are
closed here). §2.6 (global severity pre-filter) added the same day on a
user question; it is behaviour-preserving and needs no ruling. Supersedes
the Option A/B sketch in `docs/perf-root-cause.md`. Implementation is plan
Task 32 (`docs/plan-python-to-go-migration.md`). No code changed by this
document.

Authored by: **claude-fable** (the model id reported by my runtime; the
gateway table maps the `architect` role to it). I cannot independently verify
the gateway's routing from inside the session — only that my environment
identifies me as `claude-fable`.

## 0. Decision summary

Recommended design, in one paragraph: keep libsystemd via the existing
dlopen binding, but move the per-entry loop into **one C function that
fills a Go-owned batch buffer** (one cgo crossing per `ReadBatch`, not 20
per entry). In Go, build a **16-byte-per-field offset index** over that
buffer — never a `map`, never `[]Field`, never a per-value `string` — run
every filter over byte slices into the buffer, and **emit JSON by appending
straight from the raw field bytes** into one exactly-sized payload per
filter key, shared by all senders of that key exactly as today. Keep
Python's JSON formatting where it is free (it is free everywhere; the
byte-exact reference serializer already measured 1.7 µs), but downgrade
byte-identity from *contract* to *canary*: the tests assert semantic
equivalence, and formatting may change later without a redesign. An
in-process **global severity pre-filter** (§2.6) skips emission entirely
for entries every sender would drop, at ~50 ns per entry and with
Python-identical outcome.

Expected: **~5-6 µs CPU/entry serial (from 28.7), ~4 allocs/entry (from
158), ~1 KB/entry (from 12.5 KB)**, i.e. ~5x less CPU than Python; entries
pruned by §2.6 cost ~2.5-3 µs. Wall time does not move (poll-paced). The
three operator-visible quirks that needed a user ruling are **resolved in
§7**: both handler quirks are emulated exactly (no deviation, no NEWS
entry), and the charter's byte-identical requirement is amended to
"semantically-equivalent, byte-identical where free".

Rejected: pure-Go journal-file reader (§1.2), `journalctl -o export`
subprocess (§1.3), and a journald-side PRIORITY match as the severity
pre-filter (§2.6.2 — it would drop entries Python passes). All are
documented as fallbacks or opt-ins only.

## 1. Sourcing journal entries

### 1.0 What the cgo cost actually is

The "~20 crossings per entry" is **not a libsystemd floor; it is our
wrapper granularity.** `internal/reader/ffi.go` wraps every libsystemd
function in its own `call_*` C stub and crosses once per stub:

- per entry with N=16 fields: `sd_journal_next` (1) + `sd_journal_enumerate_data`
  (N+1 = 17, the last returns 0) + `sd_journal_get_realtime_usec` (1) +
  `sd_journal_get_cursor` (1) = **20 calls**, `runtime.cgocall` flat 1.45 s
  / 6.0 M calls = **242 ns per crossing**, ~4.8 µs/entry of pure transition
  cost.
- libsystemd's own work (hash lookup, mmap window, decompress, cursor
  formatting) is the remainder of the 2.08 s cgo surface: **≈0.6 s / 300k ≈
  2 µs/entry**. That part is irreducible for any design that uses libsystemd.
- on top: `ffiSymbol` map+mutex per call (0.51 s, 4.2%), the 1 MiB
  `enumBufPool` buffer per field (298 MB of pool misses, 7.6% of bytes,
  `memclr` 350 ms), and two Go-side copies of every value.

There is no whole-entry or multi-entry API in libsystemd. The candidates
all operate on the *current* entry one datum at a time:
`sd_journal_enumerate_data` / `sd_journal_enumerate_available_data`
(one `KEY=value` per call), `sd_journal_get_data` (one *named* field per
call; useful only for sparse access), `sd_journal_enumerate_unique`
(iterates distinct *values of one field across the whole journal* — not an
entry API), `sd_journal_restart_data`, `sd_journal_get_realtime_usec`,
`sd_journal_get_monotonic_usec`, `sd_journal_get_cursor`, and the
`sd_journal_seek_*` family (positioning only). The native journal export
protocol is only exposed through `journalctl -o export` /
`systemd-journal-gatewayd` (§1.3).

Conclusion: the only way to reduce crossings is to **do the loop in C**.

### 1.1 Recommended: cgo + libsystemd with a C batch loop

Add one C function to `ffi.go`:

```c
/* Reads up to max_entries entries, each one atomic, into buf.
 * Returns the number of entries written; *status is 0 (buffer full or
 * max_entries reached), 1 (end of journal) or a negative errno from the
 * libsystemd call that failed AFTER the entries already written.
 * Layout per entry (little-endian):
 *   u64 realtime_usec
 *   u16 cursor_len, cursor bytes
 *   u32 nfields, then per field: u32 len, bytes (KEY=value, no NUL)
 */
static int jp_read_batch(const jp_api* api, sd_journal* j,
                         uint8_t* buf, size_t buflen, int max_entries,
                         size_t* used, int* status);
```

- `jp_api` is a struct of the resolved function pointers, filled **once**
  at open from the existing `ffiSymbol` results. This deletes the per-call
  map/mutex lookup (0.51 s) without changing the dlopen approach (keeps the
  no-link-time-dependency property the port chose deliberately).
- The loop per entry: `next`; if rc==0 → status=1, return; if rc<0 →
  status=rc, return (entries before it are kept; position is on the bad
  entry, exactly as today's `readNext` leaves it). Then `restart_data` +
  `enumerate_data` until 0, copying each datum into `buf` (the memcpy from
  libsystemd's mmap window is unavoidable; it is the *only* copy in the new
  design). Then `get_realtime_usec` and `get_cursor` (the malloc'd cursor is
  copied into `buf` and freed in C — no Go-visible malloc/free pair).
- Atomicity: before writing an entry, the loop cannot know its size, so it
  writes speculatively and, if the buffer runs out mid-entry, rewinds `used`
  to the entry start, calls `sd_journal_previous` to re-position, and returns
  status 0 with the entries that fit. Go then grows the buffer (doubling,
  cap 8 MiB) and retries. An entry that does not fit in 8 MiB is reported as
  `ErrBadMessage` — the same observable outcome as today's dead `-EOVERFLOW`
  path (`ffi.go:69-70`).
- Buffer: a single Go `[]byte` owned by `*SDJournal`, initial 256 KiB,
  pinned for the call duration (plain `unsafe.Pointer(&buf[0])` is legal
  for a cgo call argument; Go pins it). No `sync.Pool`, no per-field 1 MiB.
- `max_entries` = the caller's chunk size (today's `ReadBatch(n)`), so the
  batch never introduces latency: the loop returns at end-of-journal, and
  the `sd_journal_wait` pacing is untouched.
- A 64-entry batch spends ~64 × 2 µs ≈ 130 µs in C; the Go scheduler
  hands off the P after 20 µs, so no `LockOSThread` and no starvation.
- `sd_journal_set_data_threshold` stays at the default 64 KiB (Python's
  python-systemd `Reader` does not change it either — identical
  truncation behaviour).
- `get_realtime_usec` stays (inside the loop it costs ~30 ns, no crossing).
  Note: `perf-root-cause.md` claims `__REALTIME_TIMESTAMP` is among the
  enumerated fields; libsystemd does **not** emit the `__`-prefixed address
  fields from `enumerate_data` (they are synthesised by python-systemd and
  `journalctl` from the entry header). The claim is unverified and moot
  here.
- `get_monotonic_usec` is not added (Go does not emit `__MONOTONIC_TIMESTAMP`
  today; the converter maps it to `null` anyway — unchanged behaviour).

Crossings per entry: **1/64 ≈ 0.016** (≈4 ns amortised). cgo cost per
entry drops from ~4.8 µs to ≈0.1 µs (crossing + the C-side memcpy of
~700 B). libsystemd's ~2 µs remains.

Everything else in the reader is unchanged: open modes (namespace,
directory, files), `add_match` for unit filters (still added after initial
positioning), `Seek`, `SeekMonotonicSeconds`, `Wait` with 100 ms slices,
`Reopener`/INVALIDATE, `has_persistent/has_runtime` stats,
`ErrBadMessage` counting.

### 1.2 Rejected: pure-Go reader of the journal files

Feasible but not worth it. What it would take:

- Parse the on-disk format: header (several versions; `COMPACT` mode since
  v252 changes the entry-item layout), data objects with XZ/LZ4/ZSTD
  compression, field/hash-table objects (plain hash and keyed siphash since
  v246), entry arrays, and the `state`/`tail_entry_*` live-header fields
  that journald updates non-atomically while the file is online.
- Reproduce libsystemd's **multi-file interleaving order** (seqnum within a
  seqnum_id, else boot_id+monotonic, else realtime), rotation/vacuum
  detection via inotify on `/var/log/journal/<mid>` and `/run/log/journal`,
  tail-following of the active file, and cursor **parsing and seeking**
  compatible with libsystemd cursors (the state file is bidirectional with
  Python per the charter, so Python-written cursors must seek correctly).
- Realistic size: 4-6 kLOC of Go plus a fixture corpus across systemd
  versions; months of hardening; no mature pure-Go library to lean on
  (`coreos/go-systemd/sdjournal` is cgo; the pure-Go parsers that exist are
  forensic, read-only, no follow).
- Payoff over §1.1: it removes only the ~2 µs of libsystemd work and the
  cgo toolchain. The best case is ~3 µs/entry instead of ~5-6. The risk
  (silent ordering/corruption divergence from journald itself) is
  disproportionate. Revisit only if a cgo-free static binary becomes a
  requirement.

### 1.3 Rejected as primary: `journalctl -o export` subprocess (hybrid)

Zero cgo; the export format is a stable, binary-safe stream
(`KEY=value\n` or `KEY\n<le64 len><bytes>\n`, with `__CURSOR`,
`__REALTIME_TIMESTAMP`, `__MONOTONIC_TIMESTAMP` synthesised), and the Go
parse is ~0.3 µs/entry. It does not *remove* the libsystemd CPU, it moves it
into another process. It changes operator-visible behaviour: process
supervision and restart semantics, loss of `has_persistent/has_runtime`
stats, different corrupted-entry handling (journalctl skips and logs), and
INVALIDATE handling becomes journalctl's. Each is a charter-level deviation
needing a user decision. Keep as the documented fallback if cgo must go.

## 2. The streaming path: no parse, no reserialise

### 2.1 Types (all views, no ownership)

```go
// internal/reader
type Batch struct {
    buf     []byte      // owned by *SDJournal, reused on every ReadBatch
    entries []RawEntry  // reused slice
}

type RawEntry struct {
    Realtime uint64
    Cursor   []byte     // view into buf
    Fields   []RawField // view into a per-batch reused index slice
}

// 16 bytes. Off/len are into Batch.buf (or the secret-filter scratch, see §2.4).
type RawField struct {
    KeyOff, KeyLen uint32
    ValOff, ValLen uint32
    // high bit of KeyLen: "duplicate of an earlier key" — skipped on emission,
    // merged into that key's array. Keys are ASCII [A-Z0-9_] so <2^31 is safe.
}
```

Contract: **nothing downstream may retain a view past the handler's return
for that batch.** The payloads produced are separately allocated and are the
only thing that crosses into sender buffers. A test build (`-tags poison`)
fills `buf` with 0xDB after each batch so a retained view fails loudly.

### 2.2 Why an index at all, and why it is not "materialising"

A pure single pass (emit while enumerating) cannot give equivalent
behaviour, because three decisions need fields that may appear *after* the
field being emitted:

1. **Repeated keys collapse into one array at the first key's position**
   (python-systemd `_get_all`, preserved today in `materialize`).
2. **unit_log_level drop** needs `_SYSTEMD_UNIT` and `PRIORITY`; emitting
   MESSAGE first then discovering the drop wastes the work and leaves a
   half-built payload.
3. **extra_field_values** overwrite an existing key *in place* (SetField
   semantics) — we must know whether the key exists.

The index is N × 16 B in a slice that is reused across entries and grows
to the largest N seen (a hard cap of 4096 fields falls back to a heap
slice for that one entry). Building it is one `bytes.IndexByte('=')` per
field plus a duplicate check. The duplicate check is a 32-entry linear
scan of key hashes (fields ≤ 32, the common case; libsystemd dedupes
nothing) or a tiny open-addressed table above that — ~25 ns/field, ~0.4
µs/entry. No value is ever copied, converted or boxed. This is bookkeeping,
not materialisation; the entry's bytes live in one place.

### 2.3 Processing order (unchanged semantics)

Per entry, the handler runs, in today's order:

1. **searches** — on raw keys (underscored) and raw byte values via
   `regexp.Regexp.Match([]byte)` on the view; zero copy. Needs only the
   named fields: look them up through the index (linear, ≤32 compares).
   Hit counters/statsd unchanged.
2. **pump/reader match** (`checkMatch` on one key) — byte compare against
   the configured value, with the match value pre-normalised at construction
   if the key is int-converted (so `PRIORITY=0003` still matches `3` as
   today's post-conversion string compare does).
3. **secret filters** — regex over the MESSAGE view; on a hit the redacted
   MESSAGE is written into a per-handler scratch `[]byte` and the index entry
   is repointed at it (a flag bit selects scratch vs buf). One scratch,
   reused, bounded by the largest MESSAGE (≤64 KiB by data threshold). Secret
   metric counted once per entry, as today.
3b. **global severity pre-filter** (§2.6) — if every sender would drop the
   entry on unit log level, stop here: nothing is emitted, nothing is
   enqueued, no `IncLineStats` call — exactly the observable result of
   step 4 dropping in every sender today.
4. **per sender, in configured order**, with the per-filter-key cache
   keyed on `fieldFilter.Name()` ("" when none):
   - **cache miss** (first sender of this filter key for this entry):
     the sender's **effective extras** are the extras of every cache-miss
     sender encountered so far for this entry, in config order, plus its
     own (ruling 1, §7 — this is Python's shared-dict contamination,
     reproduced from precomputed fragments with no runtime work beyond
     appending a fragment-list reference per cache-miss sender). The
     `unit_log_level` decision is taken on this **effective entry**: the
     unit and priority views come from the index unless an effective
     extra overrides `SYSTEMD_UNIT`/`PRIORITY`, in which case the
     fragment's value is used — exactly what Python's filter sees in
     the mutated dict. Drop → nothing cached (as today; the next sender
     with the same key is then a cache miss too, which the fragment-list
     rule above handles for free). Keep → `EmitJSON` (§3) into a new
     exactly-sized `[]byte`, store it **together with a 3-word
     decision record** — whether the field filter kept BOTH `PRIORITY`
     and `SYSTEMD_UNIT`, and the unit/priority views of the effective
     entry — and enqueue.
   - **cache hit**: `unit_log_level` is decided from the decision record
     (ruling 2, §7): if the emitting sender's field filter kept both
     keys, run this sender's `UnitLogLevel.Decide(unit, priority)` on the
     recorded views; otherwise the entry passes, which is the outcome
     Python's filter gives for a cached payload lacking either key. No
     JSON re-parse, no allocation. Keep → enqueue the same `[]byte`.
   - `truncateLongMessage` (payload > `MaxKafkaMessageSize`): emits the
     3-field `{error, partial_data, timestamp}` object directly from the
     index; no map, no re-serialise of the big one.

Everything that is static per sender is precomputed at construction: the
field-filter key set (as a `map[string]struct{}` queried with
`m[string(view)]`, which the compiler does without allocating), the
`extra_field_values` as pre-escaped `"key":value` JSON fragments plus a key
set, whether the field filter keeps `PRIORITY` and `SYSTEMD_UNIT`, and the
unit-log-level table. The effective-extras union is therefore a
concatenation of precomputed fragment lists; in the common case (no
unit-level drop by a first-of-key sender) it is identical to the static
"union in config order of earlier first-of-their-key senders" and could be
tabulated at construction — the runtime list form is kept because it also
covers the drop-then-re-miss case exactly, at the same cost.

### 2.4 Sharing across senders

Unchanged shape: `Handler.jsonObjects map[string][]byte` per entry keyed by
field-filter name; first sender of a key emits, later ones reuse the same
slice. `sender.Item{Payload []byte, Cursor string}` moves the reference
through each sender's `SenderBuffer`; the `Gate` never touches bytes. The
cursor becomes one `string(view)` per entry (one alloc) shared by all
Items. Add a fixed-size sidecar to `Item` — `IndexDate [10]byte` (the
`timestamp[:10]` Elasticsearch needs) — so ES stops decoding the JSON to
find its index name. logplex and rsyslog keep decoding the payload for the
7 fields they need (phase 2 may give them a sidecar too; they are consumers
of our JSON and semantic parsing is correct by construction).

### 2.5 Where an entry is materialised

Only two places, both bounded and rare:

- the **secret-filter scratch** (one MESSAGE copy, only on a hit);
- the **repr fallback** for an invalid-UTF-8 value (one string, 1 in 1000
  entries in the bench fixture; keeps the charter's "raw invalid bytes never
  reach the wire").

Nothing else: no `[]Field`, no `fieldMap`, no `ProcessEntry`, no
`ConvertEntry`, no `any` boxing, no `bytes.Buffer`.

### 2.6 Global severity pre-filter (added 2026-10-13, user question)

Question: if DEBUG-level entries are not selected by any sender, can we
avoid processing them at all, or at least check more cheaply?

What the filter actually is. `unit_log_level` is **per unit glob**, not a
global level (`internal/filter/unit_log_level.go:82-104`, porting
`journalpump.py:671-696`): a sender drops an entry only when (i) the entry
has a truthy `PRIORITY` (absent, `0`, or non-numeric → pass), (ii) it has a
non-empty `SYSTEMD_UNIT` (absent → pass), and (iii) some configured
`service_glob` matches that unit with `priority > level` (most-restrictive
wins). A sender with no `unit_log_level` drops nothing. So "DEBUG is not
selected by any sender" is only well-defined per entry, and the pre-filter
must reproduce exactly that per-entry conjunction over all senders.

Why a pre-filter is behaviour-safe here. If every sender drops the entry,
every sender is a **cache miss** (a drop caches nothing — `handler.go:
282-287`), so every decision is the step-4 live decision on the effective
entry and the ruling-2 cache-hit rule is never reached. In that all-drop
scenario the ruling-1 effective extras of sender *s* are static: the union
in config order of **all** senders before *s* plus its own. The pre-filter
therefore evaluates `Decide_s(effective unit, effective priority)` for each
sender with those static all-drop extras, and prunes only when every one
returns drop. Field filters are irrelevant to it — Python applies
`unit_log_level` **before** `filter_fields` (`journalpump.py:771` vs
`:781`), so a field filter that removes or keeps `PRIORITY` cannot change a
cache-miss decision; it matters only on cache hits, which do not occur
when everything drops. Key resolution is the same shared helper `EmitJSON`
uses (leading-`_` strip; an entry carrying both `_SYSTEMD_UNIT` and
`SYSTEMD_UNIT` resolves by the same rule as emission), so the pre-filter
cannot see a different field than the emitter would.

#### 2.6.1 In-process cheap check (recommended; in Task 32 scope)

Two tiers, both computed at sender construction, both zero-alloc at
runtime, both placed at step 3b — **after** searches, match and secret
filters, whose hit counters and metrics Python increments for every entry
regardless of the unit-level outcome.

- **Tier A — unit-agnostic floor (~20-50 ns).** For each sender take the
  level of its catch-all glob (`service_glob` exactly `*`; a `*` glob
  makes condition (iii) hold for every non-empty unit, and
  most-restrictive-wins means other globs can only drop more). If every
  sender has one, the global floor `F` is the **maximum** (most
  permissive) of those levels; if any sender lacks a `*` glob or has no
  `unit_log_level` at all, there is **no floor and no pruning**. At
  runtime: look up the effective `PRIORITY` and `SYSTEMD_UNIT` views in the
  index (two ≤32-compare scans, or the extras fragment value when an
  all-drop effective extra overrides either key — in which case the
  override is constant and the check degenerates at construction to
  "always prune"/"never prune" for that sender). Prune iff the priority
  bytes are a canonical decimal (`[1-9][0-9]*`, so `0` and anything
  non-canonical fall through), the value is `> F`, and the unit view is
  non-empty. Non-canonical priorities (`0006`, whitespace, `+6`) and
  repeated `PRIORITY` fields (a list in Python — truthy but non-numeric →
  pass) are **not** pruned by tier A; they fall through to tier B or to
  step 4, which handle them exactly as today (`pythonIntString`,
  `asPythonNumber`). With `F` for `INFO` only `PRIORITY=7` is pruned; for
  `WARNING`, 5-7.
- **Tier B — exact all-senders decision (~0.3-1 µs).** When tier A does
  not prune (no floor, or the entry's unit/priority shape is outside tier
  A's fast path), run the real `UnitLogLevel.Decide` of every sender over
  the effective views with the all-drop extras. This is the *same*
  computation step 4 would do for each cache-miss sender, done once
  before emission instead of interleaved with it; it saves `EmitJSON`
  and the per-sender work whenever all senders drop and costs one extra
  glob regex pass per sender when they do not. Enable tier B only when
  every sender has a `unit_log_level` (otherwise it can never prune);
  compile a literal `*` glob to a constant-true matcher so tier B over
  catch-all configs costs the same as tier A.

Preserved Python rules, each pinned by a test: absent `PRIORITY` passes;
`PRIORITY=0` passes (falsy); non-numeric or repeated `PRIORITY` passes;
absent or empty `SYSTEMD_UNIT` passes (kernel entries have no
`_SYSTEMD_UNIT` and are **never** pruned, whatever their priority); a
sender without `unit_log_level`, or without a `*` glob for tier A,
disables pruning for that tier; an extras override of `PRIORITY` or
`SYSTEMD_UNIT` is honoured per ruling 1; searches hit counters and the
secret-filter metric still count pruned entries; the return value of
`Process` for a pruned entry equals today's all-dropped result (no
`IncLineStats`, `hasBytes` true, 0 bytes, no sender cursor advance). The
property test in §5.2 step 4 runs random configs/entries through the
pre-filter and the old handler and asserts prune ⇒ old handler enqueued
nothing to any sender.

#### 2.6.2 journald-side PRIORITY match (assessed; NOT recommended)

The reader already adds `sd_journal_add_match` constraints for
`units_to_match` and `journald_filters` (`journalpump.py:480-483`,
`sdjournal.go:71-75`). libsystemd's match language is: matches on the
same field are OR'd, different fields are AND'd, `add_disjunction` ORs
whole groups, `add_conjunction` ANDs them. Adding `PRIORITY=0` … `PRIORITY=F`
would be OR'd among themselves and AND'd with the existing matches — the
right shape, and entries above the floor would never leave libsystemd
(saving the ~2 µs libsystemd work and the index build too). It is the
cheapest possible option and it is **not behaviour-preserving**:

- **Absence cannot be expressed.** Matches are positive tests against
  data objects; there is no negation, no "field unset", and no
  match-anything-including-absent form. An entry without `PRIORITY`
  (Python passes) would be dropped. Checked alternatives: OR-ing a
  disjunction for "no PRIORITY" is impossible; `journalctl -p` has the
  same limitation (it hides entries lacking `PRIORITY`); matching on a
  different always-present field via `add_disjunction` ORs the *whole*
  group and so disables the priority restriction rather than
  complementing it.
- **Unit condition cannot be expressed.** Python drops only when
  `SYSTEMD_UNIT` is present and glob-matched; a journald match would also
  drop high-priority entries with no unit (kernel messages, `_TRANSPORT=
  kernel`) and entries whose unit matches no glob when the floor is not
  from a `*` glob.
- **Value forms differ.** journald matches bytes: `PRIORITY=0006` or a
  non-numeric value would not match `PRIORITY=6` and would be dropped,
  where Python converts or passes.
- **Side effects differ.** Pruned-at-source entries are never read, so
  `journal.read_lines`/`read_bytes` gauges, searches hit counters and the
  secret-filter metric no longer count them; `journal.sent_lines` is
  unchanged. Operator-visible.

Recommendation: **do not implement it in Task 32.** It could only ever be
an explicit operator opt-in (e.g. `journald_priority_prefilter: true`)
whose documentation lists the four differences above — that is a charter
§2 deviation and needs a user ruling if it is ever requested; it is
recorded here so that the question is not re-opened by accident. The
in-process tiers deliver most of the saving (see §2.6.3) with none of the
differences.

#### 2.6.3 Quantification

Per entry in the §5.1 model (16 fields, ASCII): libsystemd ~2.0 µs +
index ~0.4 µs + searches/match/secret ≤0.3 µs are spent before step 3b
regardless; emission and the per-sender path (`EmitJSON` ~1.7 µs, handler
glue ~0.7 µs, payload alloc + GC ~0.3 µs, enqueue, file write) are
~2.5-3 µs. A pruned entry therefore costs **~2.5-3 µs instead of ~5-6 µs**
(≈50 % saving, ≈3 µs) in the streaming design, and the check itself is
~20-50 ns (tier A) or ~0.3-1 µs (tier B) — negligible against the
~20 µs transform of the *current* code and small against the design's
~5-6 µs. Journald-side pruning would save a further ~2.4 µs (libsystemd +
index) but is rejected above.

How many entries: with every sender at `INFO` only `PRIORITY=7` entries
are pruned; at `WARNING`, 5-7 — on most hosts `NOTICE`/`INFO` dominate and
`DEBUG` is a few percent unless a service logs at debug, so a **mixed
workload saves f × ~3 µs** per entry for pruned fraction f (a 10 %
debug share saves ~5 % of CPU); a **DEBUG-only or debug-heavy workload**
(a verbose service with senders at `INFO`) approaches the full ~50 %
saving, and against today's code it saves ~20 of 28.7 µs. The Task 31
bench fixture has no `unit_log_level`, so the pre-filter is inactive in
the committed baseline and the §5.1 gate is measured without it; a
separate debug-heavy micro-benchmark (`transform_bench_test.go` variant
with a `*`/`INFO` config and `PRIORITY=7` entries) records the pruned-path
figure so the saving is measured, not asserted.

#### 2.6.4 Interaction with rulings 1 and 2

- **Ruling 1.** The floor and every tier-B decision use the **effective**
  entry: for sender *s* in the all-drop scenario, the configured raw
  fields overlaid with the union of extras of all senders before *s* plus
  its own. An extra that sets `PRIORITY` (e.g. a constant `"PRIORITY": 3`)
  makes that sender's priority a constant for every entry, and the
  pre-filter folds it at construction (constant below the sender's levels
  → that sender never drops → no pruning at all; constant above → that
  sender's decision reduces to the unit test). An extra that sets
  `SYSTEMD_UNIT` is folded the same way. The pre-filter never reads the
  raw field name when an effective extra overrides it.
- **Ruling 2.** Not reached: pruning only happens when every sender is a
  cache miss. When pruning does **not** happen the step-4 path runs
  unchanged, including the decision record and the cache-hit rule, so the
  pre-filter cannot alter a cache-hit outcome.
- **Field filters.** Irrelevant to pruning (unit level precedes field
  filtering in Python); a filter that drops `PRIORITY` only affects cache
  hits, which require a prior keep.

#### 2.6.5 Two-reader partition — one reader for the "has unit+PRIORITY"
#### set, one for the complement (assessed 2026-10-13; REJECTED)

A later proposal refined §2.6.2: instead of matching `PRIORITY` on the
*main* reader, open **two** readers — one matching records that carry
**both** `SYSTEMD_UNIT` and `PRIORITY` (the fast path), and a second
reader for the **complement** (records missing either field). The
question is whether the complement is expressible as a journald match
at all. It is not, and the Match API was probed empirically to show it.

Empirical result (`sd_journal_add_match` via the FFI's dlopen
`libsystemd.so.0`, libsystemd 0.41.0, against the systest fixtures; two
fixtures used, `rotated_logs` where all 40 records have `PRIORITY=6`, and
`bad_msg` where 10 of 20 records carry `PRIORITY=6` and the other 10
carry **no** `PRIORITY` at all):

| match data | rc | matching entries |
|---|---|---|
| `PRIORITY=6` | 0 | 40 / 10 (the records that have it) |
| `PRIORITY=` (empty value) | 0 | **0** — literal empty value, **not** "present" |
| `PRIORITY` (field only, no `=`) | **-EINVAL** | rejected |
| `PRIORITY=*` (wildcard) | 0 | 0 — `*` has no wildcard meaning, matched literally |
| `PRIORITY!=6` (negation) | **-EINVAL** | rejected |
| `NULL` data / empty string | **-EINVAL** | rejected |

So there is **no existence match, no wildcard, no negation, and no
"field absent" predicate**. The only expressible predicate is
*"key has exactly this byte value"*, and matches are only ever a
positive conjunction/OR; the `bad_msg` complement of `PRIORITY=6` (10
records without the field) cannot be selected by any `add_match`
expression. The partition therefore cannot be done in journald: the
second reader could only ever be the *unmatched* main reader, i.e. the
design reduces back to §2.6.2's single main-reader match plus the
in-process complement handling it already rejects.

Two further reasons, independent of the Match API:

- **No fast path exists to gain from.** In the streaming design an
  absent field is simply an absent byte range — nothing "promotes" or
  synthesises a default for a missing-field record (§2.4: the emitter
  renders only the fields present, and `unit_log_level` treats absent
  `PRIORITY`/`SYSTEMD_UNIT` as *pass*, `internal/filter/unit_log_level.go`).
  A missing-field record is therefore *cheaper* to process, not more
  expensive; a reader-level partition would put the cheap records on the
  slow reader and the expensive ones on the fast reader, the wrong way
  round.
- **State-file and cursor semantics.** The reader/pump assumes one
  resume cursor per source (`state`/`tail_entry_*`, the single
  `ResumeCursor` in `OpenConfig`). Two readers mean two cursors, two
  positions, two resume records, two wait/poll loops and a merge — for a
  partition whose two halves would still both be read on every poll. The
  extra machinery buys nothing.

It also subsumes **nothing** of §2.6.1: the two-reader split is a
journald-side *drop* (§2.6.2's four operator-visible differences all
apply — gauges, hit counters, secret-filter metric, and above-floor
entries never leaving libsystemd), whereas §2.6.1's tier A/B pre-filter
is in-process and behaviour-preserving and already gets the ~2.5-3 µs
saving. §2.6.1 is unaffected and remains the recommended mechanism.

Recommendation: **reject.** Recorded here, alongside §2.6.2, so the
question is not re-opened by accident. No operator opt-in is proposed.

## 3. Serialisation

### 3.1 `EmitJSON`

```go
// internal/journal (new file stream.go; replaces serialize.go/process.go/converters.go)
type EmitOptions struct {
    Filter      *filter.FieldFilter // nil = keep all
    Extras      []extraFragment     // pre-escaped `"K":v`, with key set
    EnsureASCII bool                // default true (see §3.3)
}

// Appends one JSON object for e to dst and returns it. dst is normally a
// fresh make([]byte, 0, estimate) where estimate = Σ(valLen+keyLen+8)+64,
// so the common case is exactly one allocation with no growth.
func EmitJSON(dst []byte, e *reader.RawEntry, o *EmitOptions) ([]byte, error)
```

`o.Extras` is the sender's **effective** extras per §2.3 (ruling 1), not
its configured extras alone.

Algorithm, per field in index order, skipping duplicates-of-earlier:

1. key = raw key with leading `_` stripped (a slice, no copy — the
   `ProcessEntry` rule). Skip if the filter rejects it. If the key is in
   the extras set, emit the extras fragment instead and mark it used.
2. `appendKey`: keys are ASCII `[A-Z0-9_]` by journald contract; copy raw
   between quotes after a cheap byte check (fallback to full escaper if the
   check fails — corrupt data only).
3. value by converter class, chosen with a length-first switch over the 14
   converter keys (no map):
   - **int** (`CODE_LINE PRIORITY _AUDIT_* _GID _PID`): if the raw bytes are
     a canonical decimal (`-?[1-9][0-9]*|0`) append them **verbatim — no
     parse at all**; otherwise run today's `pythonIntString` (whitespace,
     underscores, big ints as `json.Number`) and on failure fall through to
     string, exactly today's "leave untouched" rule.
   - **realtime** (`COREDUMP_TIMESTAMP _SOURCE_REALTIME_TIMESTAMP`):
     parse `uint64` usec, append `PythonFloatRepr(usec/1e6)` (kept — it is
     ~100 ns and makes the output byte-identical; see §3.3).
   - **mon**: `null`.
   - **uuid** (`MESSAGE_ID _BOOT_ID _MACHINE_ID`): 32 hex → `8-4-4-4-12`
     lowercase, written in place; invalid → string.
   - **default**: `appendJSONString` (§3.2); repeated keys → `[` … `]`.
4. Synthesised fields, appended at the end in today's order:
   `REALTIME_TIMESTAMP` (float from `e.Realtime`), `SEQNUM` (hex `i=` parsed
   from the cursor view — today's `ParseSeqnumFromCursor`, live at
   `sdjournal.go:472`), `timestamp` (today's `TimestampFromRealtime`, same
   string), then unused extras in document order. If a raw field's stripped
   key collides with a synthesised key, the synthesised one wins and the raw
   one is skipped (Python's later dict assignment wins too; a JSON object
   must not carry duplicate keys).

### 3.2 `appendJSONString` — the ASCII fast path

Scan the value for the first byte that is `< 0x20`, `"`, `\`, or `>= 0x80`
(a simple loop; `bytes.IndexFunc` is slower — a 256-entry lookup table is
fastest). If none: `dst = append(dst, '"'); append(dst, v...); append('"')`
— one memcpy. Otherwise copy runs between specials, escaping `\" \\ \n \r
\t \b \f` and `\u00XX`; for `>= 0x80` decode the rune: valid → `\uXXXX`
(surrogate pairs) when `EnsureASCII`, raw UTF-8 otherwise; the first
*invalid* sequence aborts the value and re-emits it as the Python
`repr()` string (today's `pythonReprBytes`). This is the measured 1.7
µs/entry reference path.

### 3.3 What "equivalent output" means, and what we keep for free

**Contract (what tests assert; ruling 3, §7):** each payload is valid JSON
(RFC 8259), one object per entry, with the same key set and the same values
as the reference — strings equal after decoding, numbers equal as `float64`
(or as decimal strings when either side is beyond 2^53), `null`/arrays/
nested objects structurally equal. Key order, whitespace, escaping form and
float spelling are *not* part of the contract.

**Policy:** Python formatting is kept wherever it costs nothing, because the
byte-exact reference serializer already hits 1.7 µs and keeping it leaves
every current ground-truth fixture green: `", "`/`": "` separators (2 bytes
per field, 0 CPU), first-occurrence key order (what the index gives
anyway), synthesised keys last, `ensure_ascii=True` (only the non-ASCII
slow path is affected, and it is a loop either way), `PythonFloatRepr`
for the two float values, the repr fallback. The expected result is that
**all current byte-identical fixtures still pass**; the test suite is
changed anyway (§5) so that a future formatting change (e.g. dropping
`ensure_ascii`, which shrinks Kafka payloads for non-ASCII logs) is a
one-line config change plus a NEWS entry, not a redesign.

**Must stay exact (third-party wire contracts, untouched by this design):**
Kafka record framing/compression/key→partition (payload = our JSON),
logplex `application/logplex-1` octet-counted RFC5424 frames and headers,
rsyslog RFC5424/3164/RFC6587 framing in `internal/syslogframing`, ES
`_bulk` NDJSON action lines and mapping, websocket NUL-joined/snappy
frames, file `payload + "\n"`, CloudWatch and GCL request shapes. None of
these read our JSON bytes except to embed or parse them. In one line: sink
**protocols** stay exact; the message-content JSON they carry becomes
semantically-equivalent.

## 4. What still matters vs what is now free

Preserved exactly (not touched by this design):

- all eight senders and their wire protocols (§3.3);
- state file: parsed-document compatibility both ways, resume-cursor quirk
  (last sender in key order; any missing → head), `searches` hit counters;
- cursors: libsystemd-produced, one per entry, ack-then-advance in
  `markSent`, durable checkpoint for file, buffer cursor on admission;
- reload (SIGHUP ctime check, filters captured at sender construction,
  readers rebuilt only on parsed-config change), sd_notify datagrams,
  statsd metric names/tags, journald log output;
- filter semantics and order: searches (raw keys) → match → secret filters
  → per-sender unit_log_level → field filter; `add_match` unit filters;
  the §2.6 pre-filter sits inside the per-sender unit_log_level step's
  observable envelope (it only short-circuits an all-drop outcome);
- entry ordering (libsystemd's), chunking and poll pacing, INVALIDATE
  reopen, `ErrBadMessage` accounting, data-threshold truncation;
- the json-cache quirk pinned by `systest/test_groundtruth_json_cache.py`
  (second filterless sender gets the first's payload, its own extras
  ignored) — falls out of the cache key unchanged;
- the **cross-sender extras contamination** (`handler.go:148-154`; ruling
  1) and the **cache-hit unit_log_level outcome** (`handler.go:337-358`;
  ruling 2) — emulated via effective extras and the decision record
  (§2.3), Python-identical in every case including the drop-then-re-miss
  corner;
- the values of every field, including `REALTIME_TIMESTAMP`, `SEQNUM`,
  `timestamp`, int/uuid/float conversions and the repr fallback.

Now free (policy), kept anyway where cost is zero (§3.3): JSON key order,
separators, `\uXXXX` vs raw UTF-8, float spelling, HTML-escaping (never
done), ordering of synthesised keys.

## 5. Expected result and migration

### 5.1 Numbers (16-field ASCII entry, GOMAXPROCS=1)

| stage | today (µs) | design (µs) | basis |
|---|---|---|---|
| libsystemd work | ~2.0 | ~2.0 | irreducible with libsystemd |
| cgo transitions + ffiSymbol | ~4.8 + 0.5 | ~0.1 | 1 crossing / 64 entries; C memcpy ~700 B |
| materialize / copies / 1 MiB pool | ~2.6 | ~0.4 | index build, 16 × IndexByte + dedupe |
| ConvertEntry + ProcessEntry | ~2.7 | 0 | folded into emission (verbatim ints) |
| Serialize | ~7.6 | ~1.7 | measured reference serializer |
| handler glue, fieldMaps, SetField, file line copy, statsd | ~2.9 | ~0.7 | no maps, Item by value, write+`\n` |
| GC (16% of CPU) | ~4.5 | ~0.3 | 158 → ~4 allocs, 12.5 KB → ~1 KB |
| **total** | **28.7** | **≈5-6** | gate: ≤ 8 µs |
| entry pruned by §2.6 | 28.7 | ≈2.5-3 | no emission; check ≤ 50 ns (tier A) |

Rulings 1 and 2 add nothing to this table: the effective-extras list is a
few pointer appends per entry and the cache-hit decision reads three words
already in cache; the `filterByLogLevel` JSON re-parse that today costs a
full decode per cache-hit sender is gone. The §2.6 pre-filter is inactive
in the bench baseline config (no `unit_log_level`) and is measured by its
own micro-benchmark.

Allocations per entry: payload (1 per filter key, exact size), cursor
string (1), rare repr/secret scratch, plus whatever the sender channel
boxing costs today (target total ≤ 4, gate ≤ 8). Bytes/entry ≈ payload
(~0.8 KB) + cursor (~130 B). A pruned entry allocates nothing.

Transform micro-benchmark (`internal/journal/transform_bench_test.go`):
18-20 µs → ~2 µs (Python 6-13 µs).

Whole-run: CPU 8.6 s → ~1.7 s over 300k entries; wall unchanged (~71-75 s,
poll-paced, as both baselines show). The default-GOMAXPROCS run (13.35 s
CPU, worse than Python's 8.94 because of GC and cgo spinning) should fall
to ~2-3 s — that is the headline a user sees in `top`.

### 5.2 Migration outline (each step is one or two builder dispatches)

Carried into the plan as Task 32 sub-steps (a)–(g).

1. **reader: C batch loop.** `ffi.go` gets `jp_api` + `jp_read_batch`;
   `sdjournal.go` gets `ReadRaw(max int) (*Batch, error)`. Keep today's
   `ReadBatch(n) ([]journal.Entry, error)` as an adapter over `ReadRaw`
   so every existing test passes unchanged. Proof: existing
   `sdjournal_test.go` + a new test asserting adapter output == old
   `readNext` output on the systest `.journal` fixtures; bench shows cgo
   share collapses.
2. **journal: `EmitJSON` + index.** New `stream.go`; the old
   `ConvertEntry/ProcessEntry/Serialize` chain becomes the *oracle* in a
   semantic-equivalence test and a fuzz test (random keys/values/dups/
   invalid UTF-8 → both paths → parse → compare per §3.3). Golden file
   stays as a byte canary.
3. **filters: byte-slice entry points.** `Searcher.MatchRaw`,
   `UnitLogLevel.Decide(unit, prio []byte)`, `FieldFilter.KeepKey`,
   `SecretFilters.ApplyRaw(msg []byte, scratch []byte)`; a literal `*`
   glob compiles to a constant-true matcher. Table tests mirror the
   existing string-based ones.
4. **handler: process `RawEntry`.** Rewrite `Handler.Process` over the
   batch, per-filter-key cache with the decision record (ruling 2),
   effective-extras fragment list (ruling 1), the §2.6 pre-filter (tier A
   floor + tier B exact, computed at construction from the all-drop
   effective extras), `Item.IndexDate`. Delete `fieldMap` and the
   `filterByLogLevel` re-parse. Proof: `handler_test.go` ported to
   semantic assertions, plus quirk-lock tests that run the OLD handler as
   oracle: (i) three senders with distinct filter keys and distinct extras
   — sender C's payload carries A's and B's extras; (ii) a first-of-key
   sender whose unit_log_level drops the entry followed by a same-key
   sender — the second emits with its own extras appended, and a
   cache-hit sender after a filter that removed `PRIORITY` passes; (iii)
   the §2.6.1 preserved-rules table (absent/zero/non-numeric/repeated
   `PRIORITY`, absent unit, no-`*`-glob sender, no-unit_log_level sender,
   extras overriding `PRIORITY`/`SYSTEMD_UNIT`, searches counters still
   incremented) and a property test — random configs and entries, prune
   ⇒ the old handler enqueued nothing to any sender, and no prune ⇒ the
   two handlers' per-sender outputs are semantically equal. Both oracles
   are Python-identical today. `pump_test.go` unchanged.
5. **senders.** file: write payload then `\n` (no copy); ES: use
   `IndexDate`. logplex/rsyslog unchanged.
6. **tests + bench.** systest fixtures compared semantically (§6.1);
   `make bench-baseline` re-recorded; `bench-check` tripwire re-armed at
   the new baseline; micro-benchmark rewired to `EmitJSON`, plus the
   debug-heavy pruned-path variant (§2.6.3).
7. **cleanup.** Delete the adapter, old chain, `enumBufPool`; update
   `perf-root-cause.md` ledger; NEWS entry for ruling 3 only (rulings 1
   and 2 and the §2.6 pre-filter are behaviour-preserving and get none).

Order matters: 1 and 2 are independent and can be built in parallel; 4
needs both; 6 before 7.

## 6. Risks and trade-offs

### 6.1 Relaxing byte-exactness

- **Consumers of exact bytes.** None known inside the repo beyond tests.
  Downstream Kafka/ES/websocket consumers parse JSON; the only plausible
  break is a non-UTF-8-clean consumer if `ensure_ascii` is ever turned off,
  which is why the design keeps it on by default and makes it a documented
  option, not a silent change.
- **Fixtures.** Today: `internal/journal/serialize_test.go` compares the
  `output` string byte-for-byte against `serialization_golden.jsonl`;
  `systest/test_groundtruth_{file,kafka}.py` compare raw line/payload
  strings; `json_cache` and `replay` compare Go-vs-Go (unaffected);
  `logplex` normalises timestamps then compares frames (framing stays
  exact). Change: add a `semantic_equal(a, b)` helper in
  `systest/groundtruth_support.py` (json.loads both; numbers compared as
  float or as Decimal when either exceeds 2^53) and use it for the JSON
  *body* in file and kafka tests, keeping the raw comparison for framing,
  headers, partition/key and counts. In Go, `TestSerializeGolden` becomes
  the canary (`t.Logf` on byte diff, fail only on semantic diff);
  `TestPythonFloatRepr`/`TestPythonJSONString` stay as unit tests of the
  helpers we keep. Expected outcome: zero fixture regeneration.
- **Charter wording — AMENDED (ruling 3).** The charter checklist said
  "golden serialization output byte-identical to Python". By user
  directive of 2026-10-13 that requirement now reads
  **"semantically-equivalent, byte-identical where free"**. The builder
  records the amendment in the charter via its dedicated docs commit (Task
  32) and Task 28 carries the NEWS entry.

### 6.2 Streaming-design risks

- **Buffer lifetime.** A view that escapes the batch (e.g. a sender keeps
  `RawEntry.Cursor`) is a use-after-reuse bug, silent in production.
  Mitigation: `string(view)` for the cursor at the handler boundary (the
  one allocation we accept), the poison build tag in tests, and `go vet`-
  style review rule that no `RawEntry`/`RawField` type appears outside
  `reader` and `journal`. The ruling-2 decision record stores unit/priority
  **views** into the batch buffer; it lives in the per-entry cache that is
  discarded before the handler returns, so it is inside the contract — the
  poison build covers it.
- **Filter semantics over repeated fields.** Python's searches/secret
  filters operate on `entry.get(field)`, which is a *list* for repeated
  fields (regex on a list raises/skips). Today's Go port already has a
  defined behaviour here; the stream version must match the *current Go*
  behaviour (first occurrence vs all) — pin it with a test in step 3
  before changing the implementation.
- **Error mid-batch.** The C loop returns the entries read before the
  failing `next`/`enumerate`; Go processes them, then surfaces the error
  exactly as `ReadBatch` does today ("entries so far plus error"). The
  journal position is on the failed entry. `errFieldWithoutEquals` is
  detected during index build and aborts that entry with the same error
  text; the C side does not inspect data.
- **Oversized entry.** Grow-and-retry up to 8 MiB, then `ErrBadMessage`
  (today's behaviour for >1 MiB fields). Verified by a synthetic journal
  with a 64 KiB field × 200.
- **Batching and latency.** None added: `max_entries` = chunk size and the
  loop returns at end-of-journal; `Wait` pacing untouched. Verified by
  `poll_interval_systest.go`.
- **GOMAXPROCS>1 behaviour.** Longer C calls (~130 µs) are fine; the
  runtime hands off the P. No `LockOSThread`.
- **Key-collision rule** (§3.1 step 4): synthesised wins. Differs from
  Python only for adversarial journals carrying literal
  `_REALTIME_TIMESTAMP`/`_SEQNUM`/`timestamp` fields; cosmetic, coordinator
  rules with a veto mention.
- **Emulated quirks are now load-bearing.** Rulings 1 and 2 keep two
  behaviours that exist only because Python mutates a shared dict. They
  are documented here and pinned by the §5.2 step-4 quirk-lock tests so
  that a future "fix" is a deliberate, NEWS-recorded decision and not an
  accidental drift in either direction.
- **Pre-filter drift.** The §2.6 pre-filter duplicates the all-drop
  outcome of step 4; if `UnitLogLevel.Decide` or the extras rules change
  later, the two can diverge silently. Mitigation: the pre-filter calls the
  same `Decide` and the same key-resolution helper (no second
  implementation of the rule), and the step-4 property test compares it
  against the full per-sender path on every run. The wrong direction
  (pruning an entry a sender would keep) is a data-loss bug; the test
  asserts that direction specifically.

## 7. Rulings (user, 2026-10-13)

The three questions this design put to the user are closed as follows.
Options not chosen are kept for the record.

1. **Cross-sender extras contamination** (`handler.go:148-154`) — **ruled
   (a): EMULATE.** Sender B (filter F2) continues to receive the
   `extra_field_values` of every earlier cache-miss sender, because Python's
   shared `new_entry` dict is mutated in sender order. Implementation:
   per-sender precomputed extras fragments; the emitting sender's effective
   extras are the union, in config order, of the fragments of the earlier
   cache-miss (first-of-their-key) senders plus its own (§2.3). Zero runtime
   cost beyond appending a fragment-list reference per cache-miss sender.
   **No deviation, no NEWS entry.** User rationale: it costs nothing to
   keep, and fixing it could break consumers that have come to rely on the
   extra keys. Option (b) — each sender gets exactly its own extras — was
   the architect's recommendation and is NOT taken. The json-cache quirk
   (same filter key, second sender's extras ignored) is unaffected and
   stays pinned by `systest/test_groundtruth_json_cache.py`.
2. **unit_log_level after a cache hit** (`filterByLogLevel`,
   `handler.go:337-358`) — **ruled (a′): cheap emulation, Python-identical
   outcome.** On a cache hit the sender's unit-level decision is taken from
   the live entry **only if the emitting (first) sender's field filter kept
   BOTH `PRIORITY` and `SYSTEMD_UNIT`**; presence and value are taken from
   that sender's **effective entry** (its field view after the ruling-1
   effective extras have been applied, so an extras override of either key
   is honoured). If either key was filtered out, the entry **passes** — the
   same outcome Python produces when it `json.loads` the cached payload and
   finds the key absent. No JSON re-parse (§2.3 decision record). **No
   deviation, no NEWS entry.** Option (b) — evaluate on the real entry for
   every sender, removing the sender-order dependence — was the architect's
   recommendation and is NOT taken.
3. **Charter amendment** — **ruled: amended.** The charter's
   "byte-identical" serialization requirement becomes
   **"semantically-equivalent, byte-identical where free"** (user directive
   2026-10-13; §3.3 contract and §6.1 test changes). The `ensure_ascii`
   option exists, default true (Python form). This is the one
   operator-visible policy change in the design and the only one that gets
   a NEWS entry (Task 28). The charter text itself is updated by the builder
   in its dedicated docs commit, per the charter's own rule.

No ruling is needed for §2.6.1 (in-process pre-filter, behaviour-
preserving, in scope). §2.6.2 (journald-side PRIORITY match) is **not**
implemented; if an operator opt-in is ever wanted it is a charter §2
deviation requiring a user ruling at that time. §2.6.5 (two-reader
partition) is **rejected** — the complement set is inexpressible as a
journald match (empirically verified), so there is no opt-in to offer.

Everything else in this design is behaviour-preserving and needs no
ruling.
