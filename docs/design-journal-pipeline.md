# Design: the idiomatic Go journald → sinks pipeline

Status: proposal for user decision (2026-10-13). Supersedes the Option A/B
sketch in `docs/perf-root-cause.md`. No code changed by this document.

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
equivalence, and formatting may change later without a redesign.

Expected: **~5-6 µs CPU/entry serial (from 28.7), ~4 allocs/entry (from
158), ~1 KB/entry (from 12.5 KB)**, i.e. ~5x less CPU than Python. Wall
time does not move (poll-paced). Three operator-visible quirks need a user
ruling (§7).

Rejected: pure-Go journal-file reader (§1.2) and `journalctl -o export`
subprocess (§1.3). Both are documented as fallbacks only.

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
4. **per sender, in configured order**, with the per-filter-key cache:
   - `unit_log_level.Decide(unit, priority []byte)` — evaluated on the real
     entry fields from the index, for every sender, no JSON re-parse (see
     §7 Q2 for the quirk this removes).
   - cache lookup on `fieldFilter.Name()` ("" when none). Hit → enqueue the
     same `[]byte`. Miss → `EmitJSON` (§3) into a new exactly-sized `[]byte`,
     store, enqueue.
   - `truncateLongMessage` (payload > `MaxKafkaMessageSize`): emits the
     3-field `{error, partial_data, timestamp}` object directly from the
     index; no map, no re-serialise of the big one.

Everything that is static per sender is precomputed at construction: the
field-filter key set (as a `map[string]struct{}` queried with
`m[string(view)]`, which the compiler does without allocating), the
`extra_field_values` as pre-escaped `"key":value` JSON fragments plus a key
set, and the unit-log-level table.

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

**Contract (what tests assert):** each payload is valid JSON (RFC 8259),
one object per entry, with the same key set and the same values as the
reference — strings equal after decoding, numbers equal as `float64` (or
as decimal strings when either side is beyond 2^53), `null`/arrays/nested
objects structurally equal. Key order, whitespace, escaping form and float
spelling are *not* part of the contract.

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
these read our JSON bytes except to embed or parse them.

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
- entry ordering (libsystemd's), chunking and poll pacing, INVALIDATE
  reopen, `ErrBadMessage` accounting, data-threshold truncation;
- the json-cache quirk pinned by `systest/test_groundtruth_json_cache.py`
  (second filterless sender gets the first's payload, its own extras
  ignored) — falls out of the cache key unchanged;
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

Allocations per entry: payload (1 per filter key, exact size), cursor
string (1), rare repr/secret scratch, plus whatever the sender channel
boxing costs today (target total ≤ 4, gate ≤ 8). Bytes/entry ≈ payload
(~0.8 KB) + cursor (~130 B).

Transform micro-benchmark (`internal/journal/transform_bench_test.go`):
18-20 µs → ~2 µs (Python 6-13 µs).

Whole-run: CPU 8.6 s → ~1.7 s over 300k entries; wall unchanged (~71-75 s,
poll-paced, as both baselines show). The default-GOMAXPROCS run (13.35 s
CPU, worse than Python's 8.94 because of GC and cgo spinning) should fall
to ~2-3 s — that is the headline a user sees in `top`.

### 5.2 Migration outline (each step is one or two builder dispatches)

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
   `SecretFilters.ApplyRaw(msg []byte, scratch []byte)`. Table tests mirror
   the existing string-based ones.
4. **handler: process `RawEntry`.** Rewrite `Handler.Process` over the
   batch, per-filter-key cache, effective-extras precompute (§7 Q1),
   `Item.IndexDate`. Delete `fieldMap`. Proof: `handler_test.go` ported to
   semantic assertions; `pump_test.go` unchanged.
5. **senders.** file: write payload then `\n` (no copy); ES: use
   `IndexDate`. logplex/rsyslog unchanged.
6. **tests + bench.** systest fixtures compared semantically (§6.1);
   `make bench-baseline` re-recorded; `bench-check` tripwire re-armed at
   the new baseline; micro-benchmark rewired to `EmitJSON`.
7. **cleanup.** Delete the adapter, old chain, `enumBufPool`; update
   `perf-root-cause.md` ledger; NEWS entry for the quirk rulings (§7).

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
- **Charter wording.** The charter checklist says "golden serialization
  output byte-identical to Python". This design asks to amend that line to
  "semantically identical; byte-identical where free" — a user decision,
  recorded in NEWS.

### 6.2 Streaming-design risks

- **Buffer lifetime.** A view that escapes the batch (e.g. a sender keeps
  `RawEntry.Cursor`) is a use-after-reuse bug, silent in production.
  Mitigation: `string(view)` for the cursor at the handler boundary (the
  one allocation we accept), the poison build tag in tests, and `go vet`-
  style review rule that no `RawEntry`/`RawField` type appears outside
  `reader` and `journal`.
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

## 7. Open decisions for the user

1. **Cross-sender extras contamination** (`handler.go:148-154`): sender B
   (filter F) today receives sender A's `extra_field_values` because the
   processed slice is shared and mutated. Options: (a) preserve by
   precomputing per sender the "effective extras" (union in sender order of
   extras from cache-miss senders up to it) — zero runtime cost, keeps the
   bug; (b) drop it: each sender gets exactly its own extras. Recommend
   **(b)** as the operator-intended behaviour; it is operator-visible, so it
   is the user's call. The json-cache quirk (same filter key, second
   sender's extras ignored) is kept either way because its systest pins it.
2. **unit_log_level after a cache hit** (`filterByLogLevel`,
   `handler.go:341-358`): today a later sender re-parses the *cached JSON*
   to decide the drop, so if the first sender's field filter removed
   `PRIORITY`/`SYSTEMD_UNIT`, the second sender cannot drop. The design
   evaluates on the real entry for every sender. Recommend **evaluate on the
   real entry** (removes a sender-order dependence). Operator-visible in a
   corner; user's call.
3. **Charter amendment** on byte-identical serialization → semantic
   equivalence with free byte-identity (§6.1), plus the `ensure_ascii`
   option existing at all (default true, Python form).

Everything else in this design is behaviour-preserving and needs no
ruling.
