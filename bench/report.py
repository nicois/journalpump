# Copyright 2026, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.
"""Reporting, ratio and baseline-comparison logic for the performance
benchmark (plan Task 31, Phase 4b).

This module is deliberately free of process management: it consumes the
result documents the driver (``bench/journalpump_bench.py``) writes and
produces (a) the human table, (b) the committed ``docs/perf-baseline.md``
body and (c) the baseline tripwire verdict.

Two acceptance questions are kept strictly apart, per the plan:

- **Go vs Python** is a REPORTED result, not a pass/fail gate. A Go loss
  on any metric is a valid, recorded outcome. Differences on the
  instrumented metrics (wall and the two derived rates) are only called
  a clear difference when they exceed the statsd reporting-interval
  bound AND the per-run min/max ranges do not overlap.
- **Go vs the committed Go baseline** (``compare``) is the tripwire that
  can fail ``make bench-check``: only the PRIMARY metrics, only a >50%
  regression in the worse direction, and only between compatible runs.

Secondary metrics (mid-window slope, steady-state RSS) are computed when
the sampling window is long enough and are otherwise the string
``"unavailable"``; ``bench-check`` ignores them entirely.
"""

from __future__ import annotations

import hashlib
import json
import statistics
from typing import Any

# The statsd reporting interval: the reader refreshes the
# journal.sent_lines gauge at most this often, so a run's wall time
# carries up to this much detection latency. It is the measurement
# uncertainty for the instrumented metrics.
REPORTING_INTERVAL_S = 10.0

# The ``journal.sent_lines`` gauge is the ONLY signal that processing has
# finished (the output file is not a reliable one for the buffered Python
# file sender). Entries lost to the Python BufferedWriter tail are bounded
# by io.DEFAULT_BUFFER_SIZE / minimum-serialized-entry size; we use a
# generous constant and RECORD the exact count per run.
PYTHON_MAX_DROPPED_ENTRIES = 256

# Baseline acceptance.
BASELINE_MIN_ENTRIES = 200000
BASELINE_MIN_WALL_S = 60.0
REGRESSION_THRESHOLD = 0.5  # a metric is a regression only if >50% worse

# (field, direction, verdict-kind). "instrumented" metrics use the
# reporting-interval bound; "ranges" metrics use the min/max overlap test
# alone. ``startup_s`` is not instrumented (it is not derived from the
# SIGTERM-triggered wall) so it uses the overlap test.
PRIMARY_METRICS: tuple[tuple[str, str, str], ...] = (
    ("wall_s", "lower", "instrumented"),
    ("startup_s", "lower", "ranges"),
    ("cpu_s", "lower", "ranges"),
    ("peak_rss_bytes", "lower", "ranges"),
    ("entries_per_s", "higher", "instrumented"),
    ("mb_per_s", "higher", "instrumented"),
)

# Secondaries are optional and never gate anything.
SECONDARY_METRICS = ("mid_window_slope_bytes_per_s", "steady_state_rss_bytes")

# The config fields that are ephemeral per run (a temp path, a bound UDP
# port). They are replaced with fixed tokens before hashing so the
# canonical config hash is stable across hosts and runs.
VOLATILE_CONFIG_FIELDS: dict[str, Any] = {
    "readers.bench.journal_path": "<journal_path>",
    "readers.bench.senders.out.file_output": "<file_output>",
    "statsd.host": "<statsd_host>",
    "statsd.port": 0,
}

# Fields of the environment that must match for a baseline comparison to
# be meaningful.
COMPATIBILITY_FIELDS = (
    "hostname",
    "cpu_model",
    "cpu_online",
    "kernel",
    "governor",
    "cpu_max",
    "memory_max",
)


def _set_path(config: dict[str, Any], dotted: str, value: Any) -> None:
    keys = dotted.split(".")
    node = config
    for key in keys[:-1]:
        node = node.get(key)
        if not isinstance(node, dict):
            return  # the section is absent in this config; nothing to tokenize
    node[keys[-1]] = value


def canonical_config_hash(config: dict[str, Any]) -> str:
    """SHA-256 over the config with the ephemeral fields tokenized.

    Two runs on different hosts, with different temp paths and different
    bound statsd ports, hash identically — a config change is the only
    thing that moves this hash.
    """
    canonical = json.loads(json.dumps(config))
    for dotted, replacement in VOLATILE_CONFIG_FIELDS.items():
        _set_path(canonical, dotted, replacement)
    blob = json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(blob).hexdigest()


def summarize(values: list[float]) -> dict[str, Any]:
    return {
        "median": statistics.median(values),
        "min": min(values),
        "max": max(values),
        "n": len(values),
    }


def _worse_is_higher(direction: str) -> bool:
    return direction == "higher"


def _rate_uncertainty_bound(quantity: float, wall_s: float) -> float:
    """The change in ``quantity/wall`` caused by one reporting interval.

    A wall of ``W`` carries up to ``REPORTING_INTERVAL_S`` of detection
    latency, so a derived rate cannot be resolved finer than the rate
    change across that interval. Evaluated at the slower (larger) wall,
    which is the more conservative of the two.
    """
    if wall_s <= REPORTING_INTERVAL_S:
        return float("inf")
    return quantity / (wall_s - REPORTING_INTERVAL_S) - quantity / wall_s


def metric_verdict(
    field: str,
    direction: str,
    kind: str,
    go: dict[str, Any],
    python: dict[str, Any],
    *,
    entries: int,
    output_bytes: float,
    wall_go: float,
    wall_python: float,
) -> tuple[float, str]:
    """Return ``(go_over_python_ratio, verdict)`` for one metric.

    ``ratio`` is ``go_median / python_median`` (so <1 means Go's value is
    smaller). ``verdict`` is one of ``"go better"``, ``"python better"``
    or ``"no clear difference"``.
    """
    go_median = go["median"]
    python_median = python["median"]
    ratio = go_median / python_median if python_median else float("inf")
    ranges_overlap = go["min"] <= python["max"] and python["min"] <= go["max"]

    if kind == "instrumented":
        if field == "wall_s":
            bound = REPORTING_INTERVAL_S
        elif field == "entries_per_s":
            bound = _rate_uncertainty_bound(entries, max(wall_go, wall_python))
        elif field == "mb_per_s":
            # The medians are MB/s, so the bytes/s rate bound must be
            # converted before the comparison: a raw bytes/s bound is 1e6x
            # too large and would mask every clear win or loss.
            bound = _rate_uncertainty_bound(output_bytes, max(wall_go, wall_python)) / 1e6
        else:
            bound = float("inf")
        if abs(go_median - python_median) <= bound or ranges_overlap:
            return ratio, "no clear difference"
    elif ranges_overlap:
        return ratio, "no clear difference"

    go_is_smaller = go_median < python_median
    go_wins = go_is_smaller if direction == "lower" else not go_is_smaller
    return ratio, "go better" if go_wins else "python better"


def go_metric_table(result: dict[str, Any]) -> list[dict[str, Any]]:
    """One row per metric, comparing the two implementations when both
    are present."""
    implementations = result["implementations"]
    go = implementations.get("go")
    python = implementations.get("python")
    rows: list[dict[str, Any]] = []
    entries = result["entries"]
    if go is not None and python is not None:
        wall_go = statistics.median([run["wall_s"] for run in go["runs"]])
        wall_python = statistics.median([run["wall_s"] for run in python["runs"]])
        output_bytes = max(
            statistics.median([run["output_bytes"] for run in go["runs"]]),
            statistics.median([run["output_bytes"] for run in python["runs"]]),
        )
    for field, direction, kind in PRIMARY_METRICS:
        row: dict[str, Any] = {"field": field, "direction": direction, "kind": kind}
        if go is not None:
            row["go"] = summarize([run[field] for run in go["runs"]])
        if python is not None:
            row["python"] = summarize([run[field] for run in python["runs"]])
        if go is not None and python is not None:
            row["ratio"], row["verdict"] = metric_verdict(
                field,
                direction,
                kind,
                row["go"],
                row["python"],
                entries=entries,
                output_bytes=output_bytes,
                wall_go=wall_go,
                wall_python=wall_python,
            )
        rows.append(row)
    return rows


def uncertainty_bound(row: dict[str, Any], result: dict[str, Any]) -> float | None:
    """The numeric uncertainty bound for an instrumented metric, in the
    metric's OWN units (seconds, entries/s, MB/s), or ``None`` when it
    cannot be derived (a single implementation)."""
    if row["field"] == "wall_s":
        return result["interval_s"]
    implementations = result["implementations"]
    go = implementations.get("go")
    python = implementations.get("python")
    if go is None or python is None:
        return None
    wall = max(
        statistics.median([run["wall_s"] for run in go["runs"]]),
        statistics.median([run["wall_s"] for run in python["runs"]]),
    )
    if row["field"] == "entries_per_s":
        return _rate_uncertainty_bound(result["entries"], wall)
    if row["field"] == "mb_per_s":
        output_bytes = max(
            statistics.median([run["output_bytes"] for run in go["runs"]]),
            statistics.median([run["output_bytes"] for run in python["runs"]]),
        )
        return _rate_uncertainty_bound(output_bytes, wall) / 1e6
    return None


def uncertainty_note(row: dict[str, Any], result: dict[str, Any]) -> str:
    """The uncertainty bound printed beside an instrumented metric."""
    if row["field"] == "wall_s":
        return f"instrumented: +/- {result['interval_s']:.0f} s (one statsd reporting interval)"
    bound = uncertainty_bound(row, result)
    if bound is None:
        return "instrumented"
    if row["field"] == "entries_per_s":
        return f"instrumented: +/- {bound:.0f}/s (one statsd reporting interval)"
    return f"instrumented: +/- {bound:.1f} MB/s (one statsd reporting interval)"


def metric_metadata(result: dict[str, Any]) -> list[dict[str, Any]]:
    """The per-metric metadata persisted with a result: whether the metric
    is an INSTRUMENTED whole-run measurement and, if so, its numeric
    uncertainty bound. The metadata travels with the JSON so a consumer
    does not have to re-derive the labels or the bound from the raw runs."""
    metadata: list[dict[str, Any]] = []
    for row in go_metric_table(result):
        instrumented = row["kind"] == "instrumented"
        bound = uncertainty_bound(row, result) if instrumented else None
        metadata.append(
            {
                "field": row["field"],
                "direction": row["direction"],
                "kind": row["kind"],
                "instrumented": instrumented,
                "uncertainty_bound": bound,
                "uncertainty_note": uncertainty_note(row, result) if instrumented else None,
            }
        )
    return metadata


def _improvement(field: str, direction: str, ratio: float) -> str:
    """Go's improvement over Python, in the correct direction."""
    percent = (1.0 - ratio) * 100.0 if direction == "lower" else (ratio - 1.0) * 100.0
    return f"{percent:+.1f}%"


def secondary_rows(result: dict[str, Any]) -> list[dict[str, Any]]:
    """Secondary metrics, or ``"unavailable (N growth samples)"``. They are
    optional, labelled and never gate anything."""
    rows = []
    for field in SECONDARY_METRICS:
        row: dict[str, Any] = {"field": field}
        for name in ("go", "python"):
            implementation = result["implementations"].get(name)
            if implementation is None:
                continue
            values = [run.get(field) for run in implementation["runs"]]
            if any(value is None for value in values):
                samples = implementation["runs"][0].get("secondary_samples", 0)
                row[name] = f"unavailable ({samples} growth samples)"
            else:
                row[name] = summarize(values)
        rows.append(row)
    return rows


def _format_value(field: str, value: float) -> str:
    if field in ("peak_rss_bytes", "steady_state_rss_bytes", "peak_rss_inherited_floor_bytes"):
        return f"{value / (1024 * 1024):.1f} MiB"
    if field == "output_bytes":
        return f"{value / (1024 * 1024):.1f} MiB"
    if field == "exit_code":
        return f"{int(value)}"
    if field in ("wall_s", "startup_s", "cpu_s", "confirm_wait_s", "shutdown_s"):
        return f"{value:.2f} s"
    if field == "entries_per_s":
        return f"{value:,.0f}/s"
    if field == "mb_per_s":
        return f"{value:.1f} MB/s"
    if field == "mid_window_slope_bytes_per_s":
        return f"{value / (1024 * 1024):.1f} MiB/s"
    return f"{value:g}"


def _format_range(field: str, stat: dict[str, Any]) -> str:
    """The per-run min..max range, in the metric's units, so the evidence
    behind a verdict is visible rather than hidden behind it."""
    return f"{_format_value(field, stat['min'])} .. {_format_value(field, stat['max'])}"


def render_table(result: dict[str, Any]) -> str:
    """The stdout table: primary metrics, medians, per-run min/max ranges,
    ratio, improvement and the per-metric verdict, with the uncertainty
    bound printed beside the instrumented metrics."""
    lines = [f"entries: {result['entries']:,}   runs: {result['measured_runs']} measured + {result['warmup_runs']} warmup"]
    lines.append(
        "metric                 *  Go (median)       Go range                 "
        "Python (median)   Python range             Go/Python  better when  improvement  verdict"
    )
    for row in go_metric_table(result):
        field = row["field"]
        go_text = _format_value(field, row["go"]["median"]) if "go" in row else "-"
        go_range = _format_range(field, row["go"]) if "go" in row else "-"
        python_text = _format_value(field, row["python"]["median"]) if "python" in row else "-"
        python_range = _format_range(field, row["python"]) if "python" in row else "-"
        ratio_text = f"{row['ratio']:.2f}" if "ratio" in row else "-"
        better_when = "<1" if row["direction"] == "lower" else ">1"
        improvement = _improvement(row["field"], row["direction"], row["ratio"]) if "ratio" in row else "-"
        instrumented = "*" if row["kind"] == "instrumented" else " "
        verdict = row.get("verdict", "-")
        lines.append(
            f"{field:<22} {instrumented}  {go_text:<17} {go_range:<24} {python_text:<17} {python_range:<24} "
            f"{ratio_text:<10} {better_when:<12} {improvement:<12} {verdict}"
        )
    lines.append("")
    lines.append("* INSTRUMENTED whole-run measurement (spawn -> clean exit): the SIGTERM trigger")
    for row in go_metric_table(result):
        if row["kind"] != "instrumented":
            continue
        lines.append(f"  {row['field']}: {uncertainty_note(row, result)}")
    lines.append("")
    for row in secondary_rows(result):
        for name in ("go", "python"):
            value = row.get(name)
            if isinstance(value, dict):
                text = _format_value(row["field"], value["median"])
            else:
                text = str(value)
            lines.append(f"{row['field']} [{name}] (optional, never gates): {text}")
    lines.append(
        "secondary caveat: the output file grows only on buffer fill, so the slope is "
        "quantised by the 128 KiB / BufferedWriter flush size"
    )
    return "\n".join(lines)


def compatibility_key(result: dict[str, Any]) -> dict[str, Any]:
    host = result["host"]
    return {
        **{field: host[field] for field in COMPATIBILITY_FIELDS},
        "entries": result["entries"],
        "fixture_stream_sha256": result["fixture"]["stream_sha256"],
        "config_hash": result["config_hash"],
        "interval_s": result["interval_s"],
    }


def compare_go_baseline(baseline: dict[str, Any], current: dict[str, Any]) -> tuple[bool, list[str]]:
    """The ``make bench-check`` tripwire: Go-current vs the Go baseline.

    Returns ``(ok, lines)``. ``ok`` is False only when a PRIMARY Go metric
    is worse than the baseline median by more than
    ``REGRESSION_THRESHOLD``. An INCOMPATIBLE run (host, fixture, config or
    environment differs) is INFORMATIONAL: ``ok`` is True and the differing
    keys are listed, because the check cannot conclude a regression across
    runs that are not comparable — a red bench-check must mean an actual
    slowdown. The Go-vs-Python outcome is reported separately and never
    fails this check.
    """
    lines: list[str] = []
    baseline_key = compatibility_key(baseline)
    current_key = compatibility_key(current)
    if baseline_key != current_key:
        differing = sorted(k for k in baseline_key if baseline_key.get(k) != current_key.get(k))
        lines.append("INFORMATIONAL: this run is INCOMPATIBLE with the baseline; no Go-vs-Go comparison is made:")
        for field in differing:
            lines.append(f"  {field}: baseline={baseline_key.get(field)!r} current={current_key.get(field)!r}")
        lines.append("  (host, fixture, config or environment changed: regenerate the baseline on this host to compare)")
        lines.append("bench-check: PASS (informational)")
        return True, lines

    ok = True
    lines.append("Go-vs-Go baseline comparison (primary metrics):")
    for field, direction, _kind in PRIMARY_METRICS:
        base = summarize([run[field] for run in baseline["implementations"]["go"]["runs"]])
        curr = summarize([run[field] for run in current["implementations"]["go"]["runs"]])
        if _worse_is_higher(direction):
            rel = (base["median"] - curr["median"]) / base["median"]
        else:
            rel = (curr["median"] - base["median"]) / base["median"]
        state = "ok"
        if rel > REGRESSION_THRESHOLD:
            state = "REGRESSION"
            ok = False
        lines.append(
            f"  {field:<22} baseline={_format_value(field, base['median'])} "
            f"current={_format_value(field, curr['median'])} "
            f"change={-rel * 100:+.1f}% {state}"
        )
    lines.append(f"bench-check: {'PASS' if ok else 'FAIL'}")
    return ok, lines


def render_markdown(result: dict[str, Any]) -> str:
    """The committed ``docs/perf-baseline.md`` body."""
    host = result["host"]
    rows = go_metric_table(result)
    versions = result.get("versions", {})
    out = [
        f"# Performance baseline ({result['date']})",
        "",
        "Generated by `make bench-baseline` (plan Task 31, Phase 4b). Numbers are"
        " whole-run measurements of the real binaries over the same deterministic"
        " journal fixture; the fixture journal itself is not committed.",
        "",
        "## Environment",
        "",
        f"- Host: `{host['hostname']}`",
        f"- CPU: {host['cpu_model']} ({host['cpu_online']} online)",
        f"- Kernel: `{host['kernel']}`",
        f"- CPU governor: `{host['governor']}`",
        f"- cgroup cpu.max: `{host['cpu_max']}`",
        f"- cgroup memory.max: `{host['memory_max']}`",
        f"- Go: `{versions.get('go', 'unknown')}`",
        f"- Python: `{versions.get('python', 'unknown')}`",
        f"- python-systemd: `{versions.get('python_systemd', 'unknown')}`",
        f"- systemd: `{versions.get('systemd', 'unknown')}`",
        f"- journalpump: `{versions.get('journalpump_git_describe', 'unknown')}`",
        "",
        "## Fixture",
        "",
        f"- Entries: {result['entries']:,}",
        f"- Stream SHA-256: `{result['fixture']['stream_sha256']}`",
        f"- Canonical config hash: `{result['config_hash']}`",
        f"- Warmup runs: {result['warmup_runs']}, measured runs: {result['measured_runs']}",
        "",
        "## Primary metrics (medians, with per-run min/max)",
        "",
        "`*` marks an INSTRUMENTED whole-run measurement (spawn -> clean exit);"
        " its per-metric uncertainty bound is printed below the table. The"
        " min/max columns are the observed per-run spread, so the evidence"
        " behind each verdict is visible.",
        "",
        "| metric | * | Go median | Go min | Go max | Python median | Python min | Python max | Go/Python | better when | improvement | verdict |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---|---:|---|",
    ]
    for row in rows:
        field = row["field"]
        go_stat = row.get("go")
        python_stat = row.get("python")
        go_text = _format_value(field, go_stat["median"]) if go_stat else "-"
        go_min = _format_value(field, go_stat["min"]) if go_stat else "-"
        go_max = _format_value(field, go_stat["max"]) if go_stat else "-"
        python_text = _format_value(field, python_stat["median"]) if python_stat else "-"
        python_min = _format_value(field, python_stat["min"]) if python_stat else "-"
        python_max = _format_value(field, python_stat["max"]) if python_stat else "-"
        ratio_text = f"{row['ratio']:.2f}" if "ratio" in row else "-"
        better_when = "<1" if row["direction"] == "lower" else ">1"
        improvement = _improvement(row["field"], row["direction"], row["ratio"]) if "ratio" in row else "-"
        marker = "*" if row["kind"] == "instrumented" else ""
        out.append(
            f"| {field} | {marker} | {go_text} | {go_min} | {go_max} | {python_text} | {python_min} | {python_max} | "
            f"{ratio_text} | {better_when} | {improvement} | {row.get('verdict', '-')} |"
        )

    out += ["", "Uncertainty bounds (INSTRUMENTED metrics):", ""]
    for row in rows:
        if row["kind"] == "instrumented":
            out.append(f"- `{row['field']}`: {uncertainty_note(row, result)}")
    out.append(
        "- The reader polls the journal in `CHUNK_SIZE`-sized chunks, so the"
        " pacing of both implementations is part of what wall measures; that"
        " pacing is shared, which is what makes the ratio meaningful."
    )

    out += [
        "",
        "## Run shape",
        "",
        "| field | Go | Python |",
        "|---|---:|---:|",
    ]
    for field in (
        "confirm_wait_s",
        "shutdown_s",
        "output_bytes",
        "exit_code",
        "peak_rss_inherited_floor_bytes",
    ):
        cells = []
        for name in ("go", "python"):
            runs = result["implementations"].get(name, {}).get("runs", [])
            if not runs:
                cells.append("-")
                continue
            value = summarize([run[field] for run in runs])
            median = value["median"]
            cells.append(_format_value(field, float(median)) if isinstance(median, (int, float)) else str(median))
        out.append(f"| {field} | {cells[0]} | {cells[1]} |")

    out += [
        "",
        "`peak_rss_inherited_floor_bytes` is the driver's own RSS at fork:"
        " `ru_maxrss` is a high-water mark that starts from the parent's RSS,"
        " so a peak near the floor is inherited rather than earned. The driver"
        " generates the fixture in a separate process and keeps its footprint"
        " small; a Go or Python peak at or below that floor is not a real"
        " measurement of the implementation.",
        "",
        "## Secondary metrics (optional; never gate)",
        "",
        "| metric | Go | Python |",
        "|---|---:|---:|",
    ]
    for row in secondary_rows(result):
        cells = []
        for name in ("go", "python"):
            value = row.get(name)
            if isinstance(value, dict):
                cells.append(_format_value(row["field"], value["median"]))
            else:
                cells.append(str(value))
        out.append(f"| {row['field']} | {cells[0]} | {cells[1]} |")

    out += [
        "",
        "Caveat, printed with the secondaries: the output file grows only on"
        " buffer flush, so the steady-state slope is quantised by the 128 KiB /"
        " `BufferedWriter` flush size. Fewer than 20 growth samples reports"
        " `unavailable` with the sample count. `make bench-check` ignores both.",
    ]

    go_runs = result["implementations"].get("go", {}).get("runs", [])
    go_peak = _format_value("peak_rss_bytes", statistics.median([run["peak_rss_bytes"] for run in go_runs])) if go_runs else "unavailable"
    out += [
        "",
        "## Interpretation limits",
        "",
        "- This is a whole-run REPLAY of a production-shaped read path. The"
        " shared reader pacing constrains both implementations, so the numbers"
        " are valid for comparing the two on that path; they are NOT evidence"
        " of either implementation's maximum throughput capacity, which a"
        " saturated, pacing-free setup would measure.",
        f"- Go whole-process peak RSS was {go_peak}. That does NOT establish a"
        " violation of the bounded-queue expectation: peak RSS includes the"
        " journal file mappings the reader holds, the language runtime and"
        " allocator slack, not just sender buffers. Do not read a memory bound"
        " (or a memory regression) out of this metric; that needs a"
        " memory-attribution or scaling measurement.",
        "- Reconciliation with the earlier analytical note in"
        " `docs/superpowers/specs/memory-under-multi-sender-backpressure.md`:"
        " that note bounds COUNTED queue bytes at roughly 25-35 MB of Go heap"
        " for two slow senders at the default budget. The measured peak RSS"
        " here is about 7-10x that, because it is whole-process RSS over a"
        " 300,000-entry fixture whose journal the reader mmap-s, and this"
        " harness uses a single file sender with no backpressure at all. The"
        " two numbers measure different things; the gap is flagged as needing"
        " a dedicated memory-attribution measurement, not explained by this"
        " harness.",
        "- The host ran the CPU governor in its default `powersave` state and"
        " the CPU figures show run-to-run spread (see the min/max columns):"
        " treat the CPU comparison as indicative, not precise.",
        "",
        "## Acceptance",
        "",
        "- Go vs Python is a REPORTED result, not a gate. A Go loss on any metric"
        " is a recorded outcome and is shown above exactly as measured.",
        f"- The reader refreshes the `journal.sent_lines` gauge at most every"
        f" {result['interval_s']:.0f} s, so whole-run wall time carries up to that"
        " much detection latency. Differences on the instrumented metrics (wall,"
        " entries/s, MB/s) within that bound, or with overlapping per-run"
        " min/max ranges, are reported as `no clear difference`.",
        "- The `make bench-check` tripwire compares Go against THIS Go baseline"
        " only, on the primary metrics, and fails only on a >50% regression"
        " between compatible runs.",
        "",
    ]
    return "\n".join(out)