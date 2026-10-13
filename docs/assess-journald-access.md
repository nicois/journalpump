# Assessment: how the Go port reads journald

Date: 2026-10-13. Branch: `python-to-go`. Author model: Claude Opus (claude-opus, 1M context), acting as architect.
Status: assessment only. No code changed. User decisions recorded in §8 (2026-10-13).

Question from the user: "are we using the optimal package/library for retrieving the entries from journald for the selected units? there have been a few package choices (e.g. sarama) which have bitten us as they were not suitable."

## 0. Answer

**Keep the current hand-written dlopen binding.** We did not pick it by default. It is the only option that meets all the reader requirements, and it already has the cheapest call pattern libsystemd allows.

- **The 5.1 µs/entry is libsystemd's own work, not FFI overhead.** Production reads entries through a C-side loop (`jp_read_batch`, `internal/reader/ffi.go:226-296`) and crosses from Go into C once per entry. That crossing costs about 0.24 µs. The other ~4.9 µs is spent inside `sd_journal_next`, `sd_journal_enumerate_data` (called once per field), and `sd_journal_get_cursor`. libsystemd has no bulk "give me the whole entry" call, so this cost comes from the API's design. **Python pays the same libsystemd cost**, because python-systemd's `_get_all` makes the same per-field calls.
- **Unit selection is efficient and matches Python.** We add one `_SYSTEMD_UNIT=<unit>` match per unit, the same as Python. journald indexes every `FIELD=value` pair, so a match jumps straight to the matching entries instead of scanning the whole journal.
- **A pure-Go journal-file reader is the only option that could go below libsystemd's cost.** The design doc rejected it with a payoff of "only ~2 µs". Current profiling contradicts that figure: up to ~3-4 µs/entry might be removable, about 15-18% of the 21.6 µs/entry total. That is still an estimate, not a measurement. The risks are the same as before, and they are the sarama-style risk here: we would be coupled to an on-disk format whose only authoritative reader is libsystemd. That libsystemd always comes from the same systemd build as the journald writing the files. Do not switch during the migration. If you want that option kept open, run a ≤2-day throwaway prototype to measure it (§5.3).
- **Deployment:** a pure-Go reader would only remove a dependency that is always installed anyway. `libsystemd0` / `systemd-libs` is present on every host that runs journald. The real gain would be a cgo-free build and no gcc needed at build time.

## 1. What the reader must do

Sources: `internal/reader/*.go`, spec §3/§4.4/§6.2, charter §2, design §1.2, `journalpump/journalpump.py:35-133, 415-483, 642-748`.

| Requirement | Where stated | How the Go reader does it today |
|---|---|---|
| Every field of every entry, raw bytes, values up to journald's 64 KiB data threshold | spec §4.4, §5.1 | `sd_journal_enumerate_data` loop in C, copied with `memcpy` into a reusable Go buffer (`ffi.go:226-296`). The buffer grows and retries up to 8 MiB (`readraw.go:17, 42-56`). |
| Repeated fields kept as lists, field order kept | spec §5.1, charter D8 | `ParseBatch` returns views into the buffer and merges repeats into the first occurrence's position (`internal/journal/parse.go:54-73`). |
| Cursor per entry; resume = `seek_cursor` then skip one entry; cursors must stay readable by Python (the state file is shared both ways) | spec §3, §6.3 | `sd_journal_get_cursor` per entry inside the C loop. `Seek` + skip (`sdjournal.go:258-276`). |
| `initial_position` head / tail / seconds since boot | spec §3 | `seek_head`, `seek_tail`+`previous`+`next`, `seek_monotonic_usec` with `sd_id128_get_boot` (`sdjournal.go:278-350`). |
| `journal_namespace` | spec §6.2 | `sd_journal_open_namespace` (`ffi.go:381-394`). |
| `journal_path`, `journal_files`, `journal_flags` (default `LOCAL_ONLY`) | spec §6.2 | `open_directory` / `open_files` / `open`; flag rules at `sdjournal.go:143-163`. |
| `units_to_match` and `journald_filters` applied by libsystemd, after the initial seek | spec §6.2, plan Task 15 | One `sd_journal_add_match` per item (`sdjournal.go:212-223`). |
| `match_key`/`match_value` applied per entry in the handler, not by libsystemd | plan Task 15 | `CheckMatch` (`reader.go:168-179`). |
| Waiting: APPEND/INVALIDATE events; on rotation, close and reopen at most once per 2 s; respond to cancellation | spec §3 | `sd_journal_wait` in ≤100 ms slices (`sdjournal.go:359-398`); `Reopener` (`reopener.go:15-79`). |
| Corrupt entries skipped and counted (`journal.corrupted_log_entry`); missing journal files cause a retry later | spec §3 | `-EBADMSG` becomes `ErrBadMessage` and is counted (`sdjournal.go:112-124`, `stats.go:152-159`); files-available check (`sdjournal.go:189-207`). |
| Packaging: depends on `libsystemd.so.0` at runtime, like python-systemd | spec §4.4, charter §2 | RPM `Requires: libsystemd.so.0()(64bit)`; deb `Depends: libsystemd0`. |

Any replacement has to meet every row, not just the speed.

## 2. Where the 5.1 µs goes, and whether libsystemd has a cheaper path

### 2.1 Attribution

Fixture: 300,000 synthetic entries with 16 fields each (MESSAGE 80-400 B), written to one `.journal` file by `systemd-journal-remote`, read with no matches (`bench/gen_journal.py`, `docs/perf-baseline.md`).

The most detailed per-call figures are from the profile taken before the C loop existed, when Go called libsystemd 20 times per entry (`docs/perf-root-cause.md` §cgo). These figures exclude the cost of the crossings themselves.

| libsystemd call | cum s / 300k | µs/entry | Notes |
|---|---|---|---|
| `sd_journal_enumerate_data` ×17 | 0.92 | ~3.1 | ~180 ns per field. Finds the field's data object in the file, checks it, decompresses it if needed. |
| `sd_journal_next` | 0.46 | ~1.5 | Steps through the entry index and finds the entry object. |
| `sd_journal_get_cursor` | 0.24 | ~0.8 | Builds the cursor string in a newly allocated buffer each time. |
| `sd_journal_get_realtime_usec` | 0.06 | ~0.2 | |
| **libsystemd work total** | | **~5.6** | |

After the C loop was added (`docs/perf-attribution.md` §1), all time under `runtime.cgocall` is 1.54 s, or **5.1 µs/entry**, with **one crossing per entry** (~0.24 µs). That leaves about 4.9 µs, which matches the libsystemd work above. Conclusion: about 95% of the 5.1 µs runs inside libsystemd, and less than 0.3 µs is due to cgo.

### 2.2 Does libsystemd offer a bulk or cheaper path?

I checked the symbols exported by the local `/usr/lib64/libsystemd.so.0.41.0`. No man pages, headers or systemd source are installed here, so what each call does comes from my knowledge of the documented API, not from local verification.

- `sd_journal_enumerate_data` / `sd_journal_restart_data` are the only way to get every field. There is one call per field and no "all fields" variant.
- `sd_journal_enumerate_available_data` does the same work per field. The only difference is that it skips fields it cannot decompress instead of failing. Not cheaper.
- `sd_journal_get_data(field)` looks up a single field by name. It costs more per field when you need all of them.
- `sd_journal_enumerate_fields` lists field names across the whole journal, not per entry. Not relevant.
- `sd_journal_set_data_threshold` only cuts off large values. Using it would break field fidelity.
- `sd_journal_step_one`, `sd_journal_get_seqnum` give no throughput gain.

**Verdict:** the ~5 µs is inherent to libsystemd's per-field API and its implementation. We already removed the only cost we controlled, which was making one crossing per call. Two small savings remain inside the current binding:

- Batch the streaming read (`ReadRaw(N)` instead of `ReadRaw(1)`): ≤0.24 µs/entry.
- Skip `get_realtime`: ~0.2 µs.

Warning: `perf-root-cause.md` suggests replacing `get_realtime` with the "already-enumerated" `__REALTIME_TIMESTAMP`. That looks wrong. In the journal file, `__REALTIME_TIMESTAMP` is stored with the entry itself, not as a data field, so `enumerate_data` does not return it. Verify this before anyone acts on that suggestion.

`get_cursor` (~0.8 µs) is needed: every entry must carry its cursor, because a sender's batch can end on any entry. We could build the cursor ourselves from `get_seqnum`/`get_monotonic`/`get_realtime`. But the `x=` part of the cursor (a hash) is not available through the API, so our cursors would differ from Python's. The state file is shared with Python and operators can see it, so that would be your decision. I do not recommend it.

### 2.3 Python pays the same

python-systemd's `Reader._get_all` calls `sd_journal_enumerate_data` once per field, then `_get_realtime` and `_get_cursor` (`journalpump.py:120-133`). It does the same libsystemd work, plus building Python objects. So the libsystemd cost is not a regression introduced by the port. Go is already cheaper overall: 21.6 µs/entry vs 30.1 µs for Python (perf-attribution §0).

## 3. Unit selection

- **Today, in both Go and Python:** one `add_match("_SYSTEMD_UNIT=<unit>")` per entry in `units_to_match`, and one `add_match` per `journald_filters` entry. They are added after the initial seek (`sdjournal.go:212-223`, `journalpump.py:480-483`). There are no explicit disjunctions and no glob expansion. libsystemd treats several matches on the same field as OR, and matches on different fields as AND. So listing several units means "any of these units", which is correct.
- **Efficiency:** the journal file stores each distinct `FIELD=value` pair once, in a hash table, together with the list of entries that contain it. A `_SYSTEMD_UNIT=x` match is therefore a hash lookup followed by a walk over only the matching entries. That is the fastest option available. Filtering in Go after reading would cost the full ~5 µs for every entry, including the ones we throw away.
- **Correct field:** `_SYSTEMD_UNIT` is set by journald itself, so a process cannot fake it. `SYSLOG_IDENTIFIER` is chosen by the process, can be spoofed, and does not map one-to-one to units. It would be a worse choice.
- **Namespaces:** units with `LogNamespace=` write only to their namespace's journal. Reaching them needs `journal_namespace` (`sd_journal_open_namespace`). We support that, and go-systemd does not (§4b).
- **Gap compared with `journalctl -u`** (this is how Python behaves too, so it is not a port bug): `journalctl -u x` also includes:
  - systemd's own messages about the unit (`_PID=1` + `UNIT=x`);
  - `OBJECT_SYSTEMD_UNIT=x` and `COREDUMP_UNIT=x`;
  - glob expansion over existing unit names.

  `units_to_match` includes none of these, and it does not cover user units (`_SYSTEMD_USER_UNIT`). Widening it would change which entries operators see, so it is your decision, and it is out of scope for a parity port. I recommend recording it as a possible post-migration feature, not changing it now.

## 4. Alternatives, judged on whether they suit us

### a. Keep the current cgo/dlopen binding (recommended)

- **Pros:**
  - It meets every row in §1.
  - It reads the files with the same library that journald uses to write them, and both always come from the same systemd build. Format changes, compression options and file rotation are therefore handled for us.
  - Call pattern is already the cheapest: a C loop, one crossing per entry, no copying in Go.
  - Same runtime dependency as Python had. No systemd headers needed to build.
- **Cons:**
  - Needs cgo and gcc at build time, and 64-bit only (`ExclusiveArch`).
  - About 4.9 µs/entry of libsystemd work that we cannot reduce.
  - ~700 lines of hand-written binding code that we maintain. It is small, has fault-injection tests, and uses a stable C ABI.
- **Risk:** low. The libsystemd functions we call have been stable for many years.

### b. coreos/go-systemd/v22/sdjournal

Checked again: both original objections still hold, and there is a third that is the sarama kind. The source is not available locally, so these points come from my knowledge of v22 and should be checked if anyone wants to reopen this.

- **Headers:** it `#include`s `<systemd/sd-journal.h>`, so builds need systemd-devel / libsystemd-dev. It also loads libsystemd with dlopen at runtime, so it does not reduce the runtime dependency compared with ours.
- **No `sd_journal_open_namespace`:** without it, `journal_namespace` cannot be supported.
- **Unsuitable data model:** `GetEntry()` returns `Fields map[string]string`. A map loses field order and keeps only one value per field name, so **repeated fields are silently dropped**. That breaks the spec's field-fidelity requirement. We would have to work around it with `EnumerateData` called from Go, which brings back one crossing per field. That is the 20-crossings-per-entry pattern we removed, about +4 µs/entry. It also looks up the C function on every call, which is the cost we removed in `ffiSymbol`.
- **Verdict: unsuitable.** It looks like the standard choice, as sarama did, but it cannot meet the field-fidelity requirement without slowing us down.

### c. Pure-Go journal file reader (mmap the files and parse them, no libsystemd)

**What we would have to build** (design §1.2 lists most of this):

- File header across format versions, including the incompatible flags:
  - `COMPACT` (v252+);
  - `KEYED_HASH` with siphash24 (v246+), otherwise the Jenkins lookup3 hash;
  - `COMPRESSED_XZ/LZ4/ZSTD`.
- Data, field, entry and entry-array objects, plus hash-table lookups (needed for matches).
- Decompression: `klauspost/compress` (zstd) and `pierrec/lz4` are already indirect dependencies; xz would add `ulikunitz/xz`.
- Merging entries from several files in libsystemd's order (seqnum within the same seqnum_id, else boot_id+monotonic, else realtime). This covers system + user files and active + archived files.
- Live files: re-reading header fields that journald updates while the file is open; online/offline state; rotation, vacuuming and deletion via inotify.
- **Crash safety:** a file that is truncated while mapped raises SIGBUS. libsystemd handles that itself. In Go it crashes the process unless every access goes through `debug.SetPanicOnFault` with recovery.
- Cursors: build and parse them identically to libsystemd, including `x=`, which is the XOR of the data-object hashes. They must stay interchangeable with Python-written cursors. Plus `seek_cursor`, tail and monotonic seek semantics.
- Directory layout for namespaces and the meaning of the `LOCAL_ONLY` / `RUNTIME_ONLY` / `SYSTEM` / `CURRENT_USER` flags.
- Tests against real files from systemd v239 (RHEL 8), v249/v252 (Debian/Ubuntu) and v255+, plus fuzzing.

**Existing pure-Go code:** I found nothing production-grade. Not verifiable offline, but to my knowledge:

- `Velocidex/go-journalctl`, which backs Velociraptor's journal parser, is a forensic, read-only, single-file parser: it does not follow live files, has no index-based matches, does not interleave multiple files, and cannot seek by cursor.
- Every Go log shipper I know of uses libsystemd (promtail/Alloy via go-systemd, Fluent Bit in C) or `journalctl` (Vector, the OpenTelemetry journald receiver, and recent Filebeat).

Nobody ships a pure-Go journald follower. That alone is a strong suitability signal.

**Possible speed:**

- With the whole file memory-mapped, finding a field is a pointer jump plus bounds and header checks, roughly 30-60 ns per field instead of libsystemd's ~180 ns.
- Without libsystemd's multi-file and cache machinery, `next` could cost well under its 1.5 µs.
- Decompression costs the same in any reader. Real journals zstd-compress values over 512 B; the benchmark fixture probably exercises little of that.

Estimate: ~1-2 µs/entry instead of ~4.9, so **~3-4 µs/entry saved, ~15-18% of the 21.6 µs total**. Neither number is measured. This corrects design §1.2's "removes only ~2 µs": that figure assumed most of the cost was cgo, and the C-loop profile shows it is not.

**Effort:** ~4-6 kLOC including tests, about 6-10 engineer-weeks to production quality, and then permanent work tracking systemd format changes.

**The sarama lens, which decides it:** this would tie us to the on-disk format, which only libsystemd reads authoritatively. systemd's own documentation points non-C consumers to the C API or the export format, not to the raw files. If a distro upgrade enables a new incompatible flag or compression default, a pure-Go reader stops reading or, worse, silently skips or misorders entries. libsystemd never has this problem, because it ships with the journald that writes the files. That is exactly the "looked fine, was unsuitable" failure: it is fast on the test fixture and fragile across versions in production.

**Verdict:** this is the only route below libsystemd's cost, and the gain is real but moderate. Do not adopt it during the migration. Reconsider only if a cgo-free static binary becomes a requirement, or if a measured prototype (§5.3) shows a much larger gain than estimated.

### d. `journalctl` subprocess (`-o json`, or better `-o export`)

**Pros:**
- No cgo.
- The `journalctl` that ships with the host's systemd handles format compatibility, file rotation and namespaces (`--namespace`).
- Matches are given as arguments, with `+` for OR.
- Resume with `--after-cursor`; follow with `--follow`.
- Vector, OpenTelemetry and Filebeat use this approach.

**Cons:**
- It does not save CPU. `journalctl` does the same ~5 µs of libsystemd work, plus formatting. `-o json` adds journalctl's own JSON formatting, our JSON parsing (~3-5 µs) and the pipe. Total host CPU per entry probably goes up by 1.5-2x.
- `-o export` is the cheaper variant: binary-safe and cheap to parse (~0.5-1 µs). It still adds the pipe cost on top of the same libsystemd work.
- Fidelity in `-o json`: values that are not valid UTF-8 come out as byte arrays, which we could convert back. Values over 4096 B become `null` unless `--all` is passed. Repeated fields are grouped, but field-name order follows journalctl's output.
- Corrupt entries are logged to journalctl's stderr instead of being reported to us, so we would lose `corrupted_log_entry` accuracy.
- We would have to supervise the child process and handle restarts.
- Seeking to seconds since boot (`initial_position` as an integer) has no direct flag.

**Verdict:** suitable for avoiding cgo, unsuitable for performance, and riskier for parity. Not recommended.

### e. Other options

- **systemd-journal-gatewayd / journal-remote HTTP:** a network service with the same per-entry cost plus HTTP. It is often not installed. Not suitable.
- **purego (dlopen without cgo):** would remove gcc but keep libsystemd. Its crossings cost about the same or more, and the C loop would have to become Go-side calls, one per field. Slower. Not suitable.
- **Small savings inside the current binding** (§2.2): batched streaming read ≤0.24 µs and skipping `get_realtime` ~0.2 µs. Low risk, together ≤0.5 µs/entry. Worth doing as an ordinary performance task.

## 5. Recommendation

1. **Keep option (a).** The binding is the right choice on the merits: it meets every requirement, reads the files with the library that writes them, and already uses libsystemd as cheaply as possible.
2. **The ~5 µs is libsystemd's floor, and Python pays it too.** Expected CPU change from staying: none. From the small savings in §4e: about −0.3 to −0.5 µs/entry.
3. **Optional, only if you want the pure-Go option kept open after the migration:** a ≤2-day throwaway prototype outside the shipped binary. It would:
   - `perf record` libsystemd internals on the fixture, to split the 4.9 µs into next, field lookup, decompression and cursor;
   - build a ~400-line read-only pure-Go walker for one file (no follow, no matches) and measure µs/entry on the same fixture, including a zstd-compressed variant.

   Only a measured saving well above ~3 µs/entry would justify the 6-10 week build and the permanent format-tracking work.
4. **Correct the record:** amend design §1.2's payoff estimate ("~2 µs", "best case ~3 µs/entry instead of ~5-6") to the evidence-based "~3-4 µs/entry removable at best, unmeasured". Mark the perf-root-cause suggestion about `__REALTIME_TIMESTAMP` as unverified.
5. **Your decision, not in scope now:** whether `units_to_match` should one day work like `journalctl -u` (§3). It changes which entries operators see.

## 6. Deployment reality

- The packages already declare the dependency explicitly, because the rpm/deb dependency scanners cannot see a dlopen: RPM `Requires: libsystemd.so.0()(64bit)` (`journalpump.spec:8-14`) and deb `Depends: libsystemd0` (`debian/control:11`). Builds need `gcc`; the RPM is `ExclusiveArch x86_64 aarch64`.
- What a pure-Go reader would actually change:
  - The runtime dependency would go, but it is **not an operational win**. Every host running journald already has `libsystemd0` / `systemd-libs`, because systemd itself requires it.
  - The real gains are at build time: `CGO_ENABLED=0` static builds, easy cross-compiling, no gcc in BuildRequires/Build-Depends, and possibly no 64-bit-only restriction.
  - Those are worth having, but they do not justify taking on the format-coupling risk. That matches design §1.2's existing trigger: reconsider only if a cgo-free static binary becomes a requirement.

## 7. Limits of this assessment

- Not verified locally (no headers, man pages or module source installed): how go-systemd v22 behaves (`map[string]string`, no namespace call, header include), the maturity of pure-Go parsers, and the internals of libsystemd's per-field cost. The list of exported symbols was verified against the local `libsystemd.so.0.41.0`.
- The per-call split in §2.1 comes from the profile taken before the C loop. The post-fix figure is one `runtime.cgocall` total. The benchmark fixture is one file with little compression. Real hosts with many files and zstd-compressed values will spend more time in `next` and decompression, and any reader pays the decompression part.

## 8. Decisions (user, 2026-10-13)

The three open questions this assessment raised are **decided — keep current
behaviour**. No code, no behaviour and no NEWS entry change for any of them;
they are recorded here so the reasoning survives.

1. **Unit matching — decided: keep the current `_SYSTEMD_UNIT` matching.**
   Do **not** adopt the `journalctl -u`-style wider matching
   (`OBJECT_SYSTEMD_UNIT` / `COREDUMP_UNIT` and friends, §3). The current
   behaviour is Python-equivalent and is a parity requirement; widening the
   match set would silently change which entries operators see, which is
   out of scope for a parity port. Rationale: correctness/parity over a
   cosmetic feature; revisit only as an explicit post-migration feature.
2. **Cursors — decided: keep sharing the same state file; do not build our
   own cursors.** The estimated ~0.8 µs/entry saving (rebuilding the cursor
   from `get_seqnum`/`get_monotonic`/`get_realtime`, §2.2) is **not worth
   diverging from Python's cursor format**. The `x=` hash component is not
   reachable through the API, so our cursors would differ from Python's, and
   the state file is shared both ways (and operator-visible) — that is a
   hard requirement. Keep `sd_journal_get_cursor` per entry.
3. **Pure-Go journal reader — decided: no prototype.** The user judges the
   option not worth pursuing. Accept the ~3-4 µs/entry figure (§4c, §5.3)
   as an **unquantified ceiling**; keep the cgo/libsystemd binding. Rationale:
   the sarama-style on-disk-format coupling risk (§4c) outweighs a moderate,
   unmeasured gain, and the runtime dependency is present anyway (§6). The
   §4e micro-savings inside the current binding (batched `ReadRaw(N)`,
   ≤0.24 µs/entry) remain available as ordinary performance work.

Note (2026-10-13): the `__REALTIME_TIMESTAMP` suggestion flagged as
suspect in §2.2 and §5.4 has been **retracted** in
`docs/perf-root-cause.md` — libsystemd does not return `__`-prefixed fields
through `sd_journal_enumerate_data`, so `sd_journal_get_realtime_usec`
stays inside the C loop (no extra crossing). The corresponding note in
`docs/design-journal-pipeline.md` §1.2 is updated too.
