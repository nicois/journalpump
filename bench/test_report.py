# Copyright 2026, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.
"""Unit tests for the benchmark reporting and comparison logic (plan
Task 31). All pure-function tests: no processes, no journals."""

from pathlib import Path

import copy
import json
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))

import report  # noqa: E402


def summary(median: float, low: float | None = None, high: float | None = None) -> dict:
    return {"median": median, "min": median if low is None else low, "max": median if high is None else high, "n": 5}


def fake_result(*, go_wall=100.0, py_wall=100.0, go_cpu=10.0, py_cpu=10.0, entries=200000, config_hash="abc") -> dict:
    def implementation(wall, cpu):
        runs = []
        for _ in range(5):
            runs.append(
                {
                    "wall_s": wall,
                    "startup_s": 0.4,
                    "confirm_wait_s": wall - 5.0,
                    "shutdown_s": 1.0,
                    "cpu_s": cpu,
                    "peak_rss_bytes": 50 * 1024 * 1024,
                    "peak_rss_inherited_floor_bytes": 20 * 1024 * 1024,
                    "entries_per_s": entries / wall,
                    "mb_per_s": 30 * 1024 * 1024 / wall,
                    "output_bytes": 30 * 1024 * 1024,
                    "output_lines": entries,
                    "dropped_entries": 0,
                    "exit_code": 0,
                    "sent_lines_seen": entries,
                }
            )
        return {"preflight": {"wall_s": wall}, "runs": runs}

    return {
        "schema": 1,
        "date": "2026-10-12",
        "host": {
            "hostname": "bench-host",
            "cpu_model": "Test CPU",
            "cpu_online": "8",
            "kernel": "1.0",
            "governor": "powersave",
            "cpu_max": "800000 100000",
            "memory_max": "max",
        },
        "config_hash": config_hash,
        "fixture": {"entries": entries, "seed": 1, "stream_sha256": "deadbeef", "journal": "system.journal"},
        "entries": entries,
        "interval_s": 10.0,
        "warmup_runs": 1,
        "measured_runs": 5,
        "implementations": {"go": implementation(go_wall, go_cpu), "python": implementation(py_wall, py_cpu)},
    }


# --- canonical config hash -------------------------------------------------


def test_canonical_hash_ignores_volatile_fields():
    config = {
        "statsd": {"host": "127.0.0.1", "port": 41234},
        "readers": {"bench": {"journal_path": "/run/user/1001/x", "senders": {"out": {"file_output": "/tmp/a.jsonl"}}}},
    }
    other = copy.deepcopy(config)
    other["statsd"]["port"] = 59999
    other["readers"]["bench"]["journal_path"] = "/run/user/1001/y"
    other["readers"]["bench"]["senders"]["out"]["file_output"] = "/tmp/b.jsonl"
    assert report.canonical_config_hash(config) == report.canonical_config_hash(other)


def test_canonical_hash_changes_with_real_config():
    config = {"readers": {"bench": {"journal_path": "/x", "initial_position": "head"}}}
    other = copy.deepcopy(config)
    other["readers"]["bench"]["initial_position"] = "tail"
    assert report.canonical_config_hash(config) != report.canonical_config_hash(other)


# --- verdicts --------------------------------------------------------------


def test_wall_difference_within_reporting_interval_is_no_clear_difference():
    go = summary(100.0, 99.0, 101.0)
    python = summary(105.0, 104.0, 106.0)
    ratio, verdict = report.metric_verdict(
        "wall_s", "lower", "instrumented", go, python, entries=200000, output_bytes=1, wall_go=100.0, wall_python=105.0
    )
    assert verdict == "no clear difference"
    assert ratio == 100.0 / 105.0


def test_overlapping_ranges_are_no_clear_difference_even_beyond_the_bound():
    go = summary(100.0, 90.0, 120.0)
    python = summary(115.0, 110.0, 125.0)
    _, verdict = report.metric_verdict(
        "wall_s", "lower", "instrumented", go, python, entries=200000, output_bytes=1, wall_go=100.0, wall_python=115.0
    )
    assert verdict == "no clear difference"


def test_go_wall_win_beyond_the_bound():
    go = summary(80.0, 79.0, 81.0)
    python = summary(100.0, 99.0, 101.0)
    ratio, verdict = report.metric_verdict(
        "wall_s", "lower", "instrumented", go, python, entries=200000, output_bytes=1, wall_go=80.0, wall_python=100.0
    )
    assert verdict == "go better"
    assert ratio == 0.8


def test_python_wall_win_is_reported_plainly():
    go = summary(130.0, 129.0, 131.0)
    python = summary(100.0, 99.0, 101.0)
    ratio, verdict = report.metric_verdict(
        "wall_s", "lower", "instrumented", go, python, entries=200000, output_bytes=1, wall_go=130.0, wall_python=100.0
    )
    assert verdict == "python better"
    assert ratio == 1.3


def test_throughput_uses_the_rate_domain_bound():
    # 200k entries over ~100 s: one reporting interval moves the rate by
    # ~190 entries/s, so an 8% gap (~150/s) is still "no clear difference".
    go = summary(2000.0, 1995.0, 2005.0)
    python = summary(1850.0, 1845.0, 1855.0)
    _, verdict = report.metric_verdict(
        "entries_per_s", "higher", "instrumented", go, python, entries=200000, output_bytes=1, wall_go=100.0, wall_python=108.0
    )
    assert verdict == "no clear difference"

    # ... while a gap wider than the bound is a clear Go win.
    python = summary(1500.0, 1495.0, 1505.0)
    _, verdict = report.metric_verdict(
        "entries_per_s", "higher", "instrumented", go, python, entries=200000, output_bytes=1, wall_go=100.0, wall_python=133.0
    )
    assert verdict == "go better"


def test_non_instrumented_metric_uses_overlap_only():
    go = summary(10.0, 9.9, 10.1)
    python = summary(11.0, 10.9, 11.1)
    _, verdict = report.metric_verdict(
        "cpu_s", "lower", "ranges", go, python, entries=200000, output_bytes=1, wall_go=100.0, wall_python=100.0
    )
    assert verdict == "go better"


def test_secondary_metrics_may_be_unavailable():
    result = fake_result()
    for implementation in result["implementations"].values():
        for run in implementation["runs"]:
            run["mid_window_slope_bytes_per_s"] = None
            run["steady_state_rss_bytes"] = None
    rows = report.secondary_rows(result)
    assert all(str(row["go"]).startswith("unavailable") for row in rows)


def test_render_table_marks_secondaries_optional():
    result = fake_result(go_wall=80.0, py_wall=100.0)
    for implementation in result["implementations"].values():
        for run in implementation["runs"]:
            run["mid_window_slope_bytes_per_s"] = 1.0e6
            run["steady_state_rss_bytes"] = 40 * 1024 * 1024
    table = report.render_table(result)
    assert "go better" in table
    assert "(optional, never gates)" in table
    assert "* INSTRUMENTED whole-run measurement" in table
    assert "better when" in table
    markdown = report.render_markdown(result)
    assert "Go vs Python is a REPORTED result" in markdown
    assert "no clear difference" in markdown or "go better" in markdown
    assert "| better when |" in markdown
    assert "Uncertainty bounds" in markdown


# --- baseline tripwire ------------------------------------------------------


def test_baseline_comparison_passes_at_identical_numbers():
    baseline = fake_result()
    ok, lines = report.compare_go_baseline(baseline, fake_result())
    assert ok
    assert any("PASS" in line for line in lines)


def test_baseline_comparison_tolerates_a_sub_threshold_slowdown():
    baseline = fake_result(go_wall=100.0)
    current = fake_result(go_wall=140.0)  # 40% worse wall
    ok, _ = report.compare_go_baseline(baseline, current)
    assert ok


def test_baseline_comparison_fails_on_a_50_percent_regression():
    baseline = fake_result(go_wall=100.0)
    current = fake_result(go_wall=160.0)  # 60% worse wall
    ok, lines = report.compare_go_baseline(baseline, current)
    assert not ok
    assert any("REGRESSION" in line for line in lines)
    assert any("FAIL" in line for line in lines)


def test_baseline_comparison_ignores_python_side_regressions():
    baseline = fake_result()
    current = fake_result(py_wall=1000.0, py_cpu=100.0)
    ok, _ = report.compare_go_baseline(baseline, current)
    assert ok


def test_baseline_comparison_refuses_incompatible_runs():
    baseline = fake_result()
    current = fake_result(config_hash="different")
    ok, lines = report.compare_go_baseline(baseline, current)
    assert not ok
    assert any("INCOMPATIBLE" in line for line in lines)


def test_baseline_comparison_refuses_a_different_fixture():
    baseline = fake_result()
    current = fake_result()
    current["fixture"]["stream_sha256"] = "cafebabe"
    ok, lines = report.compare_go_baseline(baseline, current)
    assert not ok
    assert any("fixture_stream_sha256" in line for line in lines)


def test_result_json_round_trips():
    result = fake_result()
    assert json.loads(json.dumps(result))["entries"] == 200000