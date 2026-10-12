# Copyright 2026, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.
"""End-to-end smoke test for the performance benchmark driver (plan Task
31).

Runs the real driver (``bench/journalpump_bench.py``) over a small
fixture and the real binaries, so the whole path is exercised: fixture
generation through ``systemd-journal-remote``, the UNSHIMMED preflight
against both implementations, the ``journal.sent_lines``-driven run
protocol, the single ``os.wait4``, and the result document.

Deliberately NOT part of the required correctness checks
(``make bench-test`` only). It is skipped when the fixture toolchain is
unavailable.
"""

from pathlib import Path

import json
import os
import shutil
import subprocess
import sys

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "bench"))

import gen_journal  # noqa: E402

BENCH_DRIVER = REPO_ROOT / "bench" / "journalpump_bench.py"
GO_BINARY = REPO_ROOT / "build" / "journalpump"
SMOKE_ENTRIES = 2000


def journal_toolchain_available() -> bool:
    try:
        gen_journal.resolve_journal_remote(None)
    except SystemExit:
        return False
    return True


pytestmark = pytest.mark.skipif(
    not journal_toolchain_available(),
    reason="systemd-journal-remote and no podman fallback: cannot build a fixture",
)


def bench_env(**overrides: str) -> dict[str, str]:
    env = os.environ.copy()
    env.update(
        {
            "BENCH_ENTRIES": str(SMOKE_ENTRIES),
            "BENCH_RUNS": "1",
            "BENCH_WARMUP": "0",
            "BENCH_TIMEOUT": "180",
            # The reference host runs powersave; the smoke test measures
            # nothing but the protocol, so it does not refuse.
            "BENCH_ALLOW_ANY_GOVERNOR": "1",
        }
    )
    env.update(overrides)
    return env


def runtime_dir(name: str) -> Path:
    return Path(f"/run/user/{os.getuid()}") / f"jp-bench-{name}-{os.getpid()}"


@pytest.mark.skipif(not GO_BINARY.exists(), reason="build/journalpump is missing; run make build-go")
def test_bench_driver_smoke_runs_both_implementations():
    work = runtime_dir("smoke")
    shutil.rmtree(work, ignore_errors=True)
    try:
        completed = subprocess.run(
            [sys.executable, str(BENCH_DRIVER), "--out", str(work / "result.json")],
            env=bench_env(BENCH_JOURNAL_DIR=str(work)),
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=900,
        )
        assert completed.returncode == 0, completed.stdout + completed.stderr

        result = json.loads((work / "result.json").read_text(encoding="utf-8"))
        assert set(result["implementations"]) == {"go", "python"}
        assert result["entries"] == SMOKE_ENTRIES
        assert result["config_hash"]
        assert result["interval_s"] == 10.0
        assert result["versions"]["python"]

        for implementation in ("go", "python"):
            block = result["implementations"][implementation]
            assert block["preflight"]["mode"] == "preflight"
            assert len(block["runs"]) == 1
            run = block["runs"][0]
            assert run["exit_code"] == 0
            assert run["wall_s"] > 0
            assert run["entries_per_s"] > 0
            assert run["mb_per_s"] > 0
            assert run["peak_rss_bytes"] > 0
            assert run["peak_rss_inherited_floor_bytes"] > 0
            # The driver keeps its own footprint small, so the measured peak
            # is the implementation's, not an inherited floor.
            assert run["peak_rss_bytes"] > run["peak_rss_inherited_floor_bytes"]
            assert run["cpu_s"] >= 0
            assert run["sent_lines_seen"] >= SMOKE_ENTRIES

            # Lifecycle ordering: t_spawn <= t_ready <= t_confirm <= t_sigterm <= t_exit.
            confirm_at = run["startup_s"] + run["confirm_wait_s"]
            sigterm_at = run["wall_s"] - run["shutdown_s"]
            assert 0 <= run["startup_s"] <= confirm_at <= sigterm_at <= run["wall_s"]
            assert run["confirm_wait_s"] >= 0
            assert run["shutdown_s"] >= 0

            # Secondaries are either numeric or "unavailable" with a count.
            assert isinstance(run["secondary_samples"], int)
            for field in ("mid_window_slope_bytes_per_s", "steady_state_rss_bytes"):
                assert run[field] is None or isinstance(run[field], float)

        assert result["implementations"]["go"]["runs"][0]["output_lines"] == SMOKE_ENTRIES
        python_run = result["implementations"]["python"]["runs"][0]
        assert 0 <= python_run["dropped_entries"] <= 256

        # The instrumented label and the uncertainty bound reach the table.
        assert "* INSTRUMENTED whole-run measurement" in completed.stdout
        assert "one statsd reporting interval" in completed.stdout
        assert "secondary caveat" in completed.stdout
    finally:
        shutil.rmtree(work, ignore_errors=True)


@pytest.mark.skipif(not GO_BINARY.exists(), reason="build/journalpump is missing; run make build-go")
def test_bench_driver_profile_writes_go_profiles():
    work = runtime_dir("profile")
    shutil.rmtree(work, ignore_errors=True)
    profile_dir = work / "profiles"
    try:
        completed = subprocess.run(
            [sys.executable, str(BENCH_DRIVER), "--profile", "--profile-dir", str(profile_dir), "--out", str(work / "r.json")],
            env=bench_env(BENCH_JOURNAL_DIR=str(work)),
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=900,
        )
        assert completed.returncode == 0, completed.stdout + completed.stderr
        # The env-gated hook captured a real CPU and heap profile of the Go
        # binary, and only the Go implementation was run.
        assert (profile_dir / "go-cpu-measured.prof").stat().st_size > 0
        assert (profile_dir / "go-mem-measured.prof").stat().st_size > 0
        result = json.loads((work / "r.json").read_text(encoding="utf-8"))
        assert set(result["implementations"]) == {"go"}
    finally:
        shutil.rmtree(work, ignore_errors=True)


def test_bench_driver_refuses_a_baseline_below_the_minimum():
    completed = subprocess.run(
        [sys.executable, str(BENCH_DRIVER), "--baseline"],
        env=bench_env(BENCH_ENTRIES="5000"),
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert completed.returncode != 0
    assert "at least 200000 entries" in completed.stderr or "at least 200000 entries" in completed.stdout