# Copyright 2026, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.
"""Go-vs-Python journalpump performance benchmark driver (plan Task 31).

The driver measures WHOLE-RUN behaviour of the real binaries over one
deterministic journal fixture:

1. generate the fixture once (``bench/gen_journal.py``) into a
   runtime-storage directory (libsystemd reports runtime/persistent files
   there, so neither implementation needs any shim);
2. PREFLIGHT each implementation UNSHIMMED against the fixture and abort
   if either refuses it — a benchmark built on a shimmed run would not
   measure what ships;
3. run one warmup plus ``BENCH_RUNS`` measured runs per implementation.
   Each run: spawn -> ``READY=1`` -> wait for the ``journal.sent_lines``
   gauge to confirm every fixture entry was processed -> ``SIGTERM`` ->
   ONE single ``os.wait4`` for the exit status and the whole-run CPU
   usage. Wall time spans spawn to clean exit and therefore includes the
   shutdown grace-flush.

The ``journal.sent_lines`` gauge is used ONLY to confirm processing and
to trigger ``SIGTERM``. The reader refreshes it at most every 10 s, so a
run's wall time carries up to that much detection latency; the report
labels that as measurement uncertainty (``REPORTING_INTERVAL_S``) rather
than pretending to a precision the instrument does not have.

The same config file (``bench/config.json.tmpl``) drives both binaries —
a dedicated statsd UDP listener, the fixture journal and a per-run output
file are the only substitutions, and they are canonicalized out of the
config hash so a baseline stays comparable across hosts.

Environment knobs (all optional): ``BENCH_ENTRIES`` (>= the baseline
minimum for ``--baseline``), ``BENCH_SEED``, ``BENCH_RUNS``,
``BENCH_WARMUP``, ``BENCH_TIMEOUT``, ``BENCH_STOP_TIMEOUT``,
``BENCH_IMPL`` (``go`` / ``python`` / ``both``), ``BENCH_JOURNAL_DIR``,
``BENCH_ALLOW_ANY_GOVERNOR=1``.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import queue
import shutil
import signal
import socket
import statistics
import string
import subprocess
import sys
import threading
import time
from datetime import date
from pathlib import Path
from typing import Any

BENCH_DIR = Path(__file__).resolve().parent
REPO_ROOT = BENCH_DIR.parent
sys.path.insert(0, str(BENCH_DIR))
sys.path.insert(0, str(REPO_ROOT))

import report  # noqa: E402
from systest.journalpump_process import NotifyListener, build_environment, command_for  # noqa: E402

RESULTS_DIR = BENCH_DIR / "results"
CONFIG_TEMPLATE = BENCH_DIR / "config.json.tmpl"

DEFAULT_ENTRIES = 200000
DEFAULT_SEED = 20261012
DEFAULT_RUNS = 5
DEFAULT_WARMUP = 1
DEFAULT_TIMEOUT_S = 600.0
DEFAULT_STOP_TIMEOUT_S = 30.0
SAMPLE_INTERVAL_S = 0.1
PREFLIGHT_GROWTH_TIMEOUT_S = 10.0
READY_TIMEOUT_S = 15.0
MIN_WINDOW_SAMPLES = 20

# The Python file sender writes through a BufferedWriter that is never
# flushed before the process is torn down, so its output can trail the
# gauge count by up to one buffer. Go must be exact.
PYTHON_MAX_DROPPED_ENTRIES = 256


class BenchmarkError(RuntimeError):
    """A run failed its protocol or correctness gate (not a metric)."""


# --------------------------------------------------------------------------
# Host facts


def read_cpu_model() -> str:
    with contextlib.suppress(OSError):
        for line in Path("/proc/cpuinfo").read_text(encoding="utf-8", errors="replace").splitlines():
            if line.startswith("model name"):
                return line.split(":", 1)[1].strip()
    return "unknown"


def read_governor() -> str:
    with contextlib.suppress(OSError):
        return Path("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor").read_text(encoding="utf-8").strip()
    return "unavailable"


def read_cgroup_limit(name: str) -> str:
    """Read a cgroup v2 limit (``cpu.max`` / ``memory.max``) for this
    process's own cgroup."""
    with contextlib.suppress(OSError, IndexError):
        line = Path("/proc/self/cgroup").read_text(encoding="utf-8").strip().splitlines()[-1]
        relative = line.split("::", 1)[1]
        return Path(f"/sys/fs/cgroup{relative}/{name}").read_text(encoding="utf-8").strip()
    return "unavailable"


def host_facts() -> dict[str, str]:
    return {
        "hostname": socket.gethostname(),
        "cpu_model": read_cpu_model(),
        "cpu_online": str(os.cpu_count() or 0),
        "kernel": os.uname().release,
        "governor": read_governor(),
        "cpu_max": read_cgroup_limit("cpu.max"),
        "memory_max": read_cgroup_limit("memory.max"),
    }


def _run_version(argv: list[str]) -> str:
    with contextlib.suppress(OSError, subprocess.SubprocessError):
        completed = subprocess.run(argv, capture_output=True, text=True, timeout=20)
        if completed.returncode == 0:
            return completed.stdout.strip().splitlines()[0]
    return "unknown"


def versions() -> dict[str, str]:
    """The version block the baseline records (plan Task 31)."""
    go_version = _run_version(["go", "version", "-m", str(REPO_ROOT / "build" / "journalpump")])
    if go_version == "unknown":
        go_version = _run_version(["go", "version"])
    python_systemd = "unknown"
    with contextlib.suppress(Exception):
        from importlib.metadata import version as package_version  # noqa: PLC0415

        python_systemd = package_version("systemd-python")
    return {
        "go": go_version,
        "python": sys.version.split()[0],
        "python_systemd": python_systemd,
        "systemd": _run_version(["systemctl", "--version"]),
        "journalpump_git_describe": _run_version(["git", "describe", "--always", "--dirty"]),
    }


def generate_fixture(fixture_dir: Path, *, entries: int, seed: int) -> dict[str, Any]:
    """Build the fixture in a SEPARATE process and return its manifest.

    Doing this in-process would hold the whole export stream (hundreds of
    MB at the baseline size) in the driver's memory, and a child inherits
    the parent's RSS: every measured run would then report a peak RSS
    dominated by the driver's allocation rather than by the workload. The
    generator is run through its CLI so the measured child forks from a
    small parent.
    """
    shutil.rmtree(fixture_dir, ignore_errors=True)
    completed = subprocess.run(
        [
            sys.executable,
            str(BENCH_DIR / "gen_journal.py"),
            "--entries",
            str(entries),
            "--seed",
            str(seed),
            "--out",
            str(fixture_dir),
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=3600,
    )
    if completed.returncode != 0:
        sys.stderr.write(completed.stdout)
        sys.stderr.write(completed.stderr)
        raise SystemExit(f"fixture generation failed (rc={completed.returncode})")
    return json.loads(completed.stdout.strip().splitlines()[-1])


def assert_no_shims() -> None:
    """The bench must run the production binaries: the systest-only
    overrides must not be set in the environment."""
    for name in ("JOURNALPUMP_POLL_INTERVAL_MS", "JOURNALPUMP_FILES_AVAILABLE"):
        if os.environ.get(name):
            raise SystemExit(f"{name} must be unset for the benchmark (production binaries only)")


def check_governor() -> None:
    governor = read_governor()
    if governor == "performance" or os.environ.get("BENCH_ALLOW_ANY_GOVERNOR") == "1":
        return
    raise SystemExit(
        f"CPU governor is {governor!r}, not 'performance': results would be noisy.\n"
        "Set the governor to performance, or set BENCH_ALLOW_ANY_GOVERNOR=1 to "
        "measure anyway (and record that the numbers are governor-limited)."
    )


# --------------------------------------------------------------------------
# Config rendering


def render_config(template: str, *, journal_path: Path, file_output: Path, statsd_port: int) -> str:
    return string.Template(template).substitute(
        journal_path=str(journal_path),
        file_output=str(file_output),
        statsd_host="127.0.0.1",
        statsd_port=str(statsd_port),
    )


def canonical_config(template: str) -> str:
    """The config hash, independent of temp paths and bound UDP ports."""
    rendered = render_config(
        template,
        journal_path=Path("<journal_path>"),
        file_output=Path("<file_output>"),
        statsd_port=0,
    )
    return report.canonical_config_hash(json.loads(rendered))


# --------------------------------------------------------------------------
# Sampling and statsd


def _process_exited(pid: int) -> bool:
    """True once the child is gone or a zombie.

    Deliberately NOT ``Popen.poll()``: that reaps the child and would steal
    the status from the single ``os.wait4`` the whole-run rusage needs.
    """
    try:
        stat = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
    except OSError:
        return True
    after_comm = stat.rsplit(") ", 1)[-1]
    return bool(after_comm) and after_comm[0] in ("Z", "X")


def _rss_bytes(pid: int) -> int:
    with contextlib.suppress(OSError, ValueError):
        for line in Path(f"/proc/{pid}/status").read_text(encoding="utf-8").splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
    return 0


class Sampler(threading.Thread):
    """Samples the output-file size and the child's RSS while it runs."""

    def __init__(self, pid: int, output_path: Path, started_at: float) -> None:
        super().__init__(name="bench-sampler", daemon=True)
        self._pid = pid
        self._output_path = output_path
        self._started_at = started_at
        self._stop = threading.Event()
        self.samples: list[tuple[float, int, int]] = []  # (t, output_bytes, rss_bytes)

    def run(self) -> None:
        while not self._stop.is_set():
            self.samples.append((time.monotonic() - self._started_at, self._output_bytes(), self._rss_bytes()))
            self._stop.wait(SAMPLE_INTERVAL_S)

    def stop(self) -> None:
        self._stop.set()
        self.join(timeout=2)

    def _output_bytes(self) -> int:
        with contextlib.suppress(OSError):
            return self._output_path.stat().st_size
        return 0

    def _rss_bytes(self) -> int:
        return _rss_bytes(self._pid)


class StatsdListener:
    """A loopback UDP statsd sink recording the maximum ``journal.sent_lines``
    gauge value it sees."""

    GAUGE_NAME = "journal.sent_lines"

    def __init__(self) -> None:
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(("127.0.0.1", 0))
        self.socket.settimeout(0.2)
        self.port = self.socket.getsockname()[1]
        self.max_sent_lines = 0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, name="bench-statsd", daemon=True)
        self._thread.start()

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                datagram = self.socket.recv(65535)
            except (TimeoutError, OSError):
                continue
            for line in datagram.decode("ascii", "replace").split("\n"):
                # Both clients use the telegraf name-tag shape
                # ("journal.sent_lines,sender=out:2|g": the tags sit in the
                # name portion, before the colon).
                head, _, rest = line.partition(":")
                name = head.split(",", 1)[0]
                if not name.endswith(self.GAUGE_NAME):
                    continue
                with contextlib.suppress(ValueError):
                    value = int(float(rest.split("|", 1)[0]))
                    if value > self.max_sent_lines:
                        self.max_sent_lines = value

    def close(self) -> None:
        self._stop.set()
        self.socket.close()
        self._thread.join(timeout=2)


# --------------------------------------------------------------------------
# One run


def _count_lines(path: Path) -> tuple[int, int]:
    lines = 0
    size = 0
    with contextlib.suppress(FileNotFoundError):
        with path.open("rb") as handle:
            while chunk := handle.read(1 << 20):
                size += len(chunk)
                lines += chunk.count(b"\n")
    return lines, size


def _secondary_metrics(samples: list[tuple[float, int, int]], start: float, end: float) -> dict[str, float | int | None]:
    """Steady-state throughput slope and steady-state RSS.

    From the OBSERVED growth samples (the 100 ms samples where the output
    file got bigger, between first and last growth), take the middle 50 %
    by sample and fit a least-squares slope (bytes/s) over them; the
    steady-state RSS is the median VmRSS over that same sample range.

    Fewer than ``MIN_WINDOW_SAMPLES`` growth samples -> both ``None``,
    reported as "unavailable" with the sample count. They never gate
    anything.
    """
    unavailable = {"mid_window_slope_bytes_per_s": None, "steady_state_rss_bytes": None, "secondary_samples": 0}
    if end <= start:
        return unavailable

    growth: list[tuple[float, int]] = []
    previous = 0
    for timestamp, size, _ in samples:
        if timestamp > end:
            break
        if size > previous:
            if timestamp >= start:
                growth.append((timestamp, size))
            previous = size

    if len(growth) < MIN_WINDOW_SAMPLES:
        return {**unavailable, "secondary_samples": len(growth)}

    window = len(growth) // 4
    middle = growth[window : len(growth) - window] or growth
    times = [timestamp for timestamp, _ in middle]
    sizes = [size for _, size in middle]
    count = len(times)
    mean_t = sum(times) / count
    mean_s = sum(sizes) / count
    variance = sum((t - mean_t) ** 2 for t in times)
    slope = None if variance == 0 else sum((t - mean_t) * (s - mean_s) for t, s in zip(times, sizes)) / variance

    first, last = times[0], times[-1]
    rss_values = [rss for timestamp, _, rss in samples if first <= timestamp <= last and rss > 0]
    return {
        "mid_window_slope_bytes_per_s": slope,
        "steady_state_rss_bytes": statistics.median(rss_values) if rss_values else None,
        "secondary_samples": len(growth),
    }


def run_once(
    *,
    implementation: str,
    template: str,
    journal_path: Path,
    output_path: Path,
    config_path: Path,
    entries: int,
    mode: str,
    timeout_s: float,
    stop_timeout_s: float,
    work_dir: Path,
) -> dict[str, Any]:
    """One spawn/measure cycle. ``mode`` is ``"measured"`` or ``"preflight"``."""
    output_path.unlink(missing_ok=True)
    stderr_path = work_dir / f"stderr-{implementation}-{mode}.log"

    listener = NotifyListener()
    statsd = StatsdListener()
    config_path.write_text(
        render_config(template, journal_path=journal_path, file_output=output_path, statsd_port=statsd.port),
        encoding="utf-8",
    )

    argv = command_for("go" if implementation == "go" else "python")
    env = build_environment(
        runtime_directory=work_dir,
        notify_socket_name=listener.name,
        env={"JOURNALPUMP_BIN": "go" if implementation == "go" else ""},
        export_runtime_directory=False,
    )
    # UNSHIMMED: strip any PYTHONPATH a caller may have set, so no
    # sitecustomize compatibility shim can reach the child.
    env.pop("PYTHONPATH", None)

    started_at = time.monotonic()
    # ru_maxrss is a high-water mark that STARTS at the driver's own RSS at
    # fork (a child inherits the parent's RSS until it execs, and the
    # kernel does not reset the high-water mark). The driver keeps its own
    # footprint small (the fixture is generated in another process) and
    # records this floor so a peak at or below it is visible as inherited.
    parent_rss_bytes = _rss_bytes(os.getpid())
    try:
        with stderr_path.open("wb") as stderr_file:
            process = subprocess.Popen(
                argv + [str(config_path)], env=env, cwd=REPO_ROOT, stdout=subprocess.DEVNULL, stderr=stderr_file
            )
            pid = process.pid
            sampler = Sampler(pid, output_path, started_at)
            sampler.start()
            try:
                ready_at = _wait_ready(listener, pid, stderr_path)
                if mode == "preflight":
                    _wait_output_growth(output_path)
                else:
                    _wait_for_sent_lines(statsd, entries, started_at, timeout_s, pid, stderr_path)
                confirm_at = time.monotonic()
                sigterm_at = time.monotonic()
                with contextlib.suppress(ProcessLookupError):
                    os.kill(pid, signal.SIGTERM)
                exit_at, status, rusage = _wait4(process, pid, stop_timeout_s)
            finally:
                sampler.stop()
    finally:
        listener.close()
        statsd.close()

    wall_s = exit_at - started_at
    startup_s = ready_at - started_at
    if status != 0:
        raise BenchmarkError(
            f"{implementation} {mode} run exited with {status}\n{stderr_path.read_text(errors='replace')[-2000:]}"
        )

    lines, size = _count_lines(output_path)
    output_path.unlink(missing_ok=True)  # keep only the numbers; tmpfs is finite
    result: dict[str, Any] = {
        "mode": mode,
        "wall_s": wall_s,
        "startup_s": startup_s,
        "confirm_wait_s": confirm_at - ready_at,
        "shutdown_s": exit_at - sigterm_at,
        "cpu_s": rusage.ru_utime + rusage.ru_stime,
        "peak_rss_bytes": rusage.ru_maxrss * 1024,
        "peak_rss_inherited_floor_bytes": parent_rss_bytes,
        "entries_per_s": entries / wall_s if wall_s else 0.0,
        "mb_per_s": size / wall_s / 1e6 if wall_s else 0.0,
        "output_lines": lines,
        "output_bytes": size,
        "dropped_entries": entries - lines,
        "exit_code": status,
        "sent_lines_seen": statsd.max_sent_lines,
    }
    result.update(_secondary_metrics(sampler.samples, startup_s, confirm_at - started_at))

    if mode != "preflight":
        if implementation == "go" and lines != entries:
            raise BenchmarkError(f"Go wrote {lines} lines, expected exactly {entries}")
        if implementation == "python" and not 0 <= result["dropped_entries"] <= PYTHON_MAX_DROPPED_ENTRIES:
            raise BenchmarkError(
                f"Python dropped {result['dropped_entries']} entries, above the one-buffer bound "
                f"({PYTHON_MAX_DROPPED_ENTRIES})"
            )
    return result


def _wait_ready(listener: NotifyListener, pid: int, stderr_path: Path) -> float:
    deadline = time.monotonic() + READY_TIMEOUT_S
    while time.monotonic() < deadline:
        try:
            if listener.notifications.get(timeout=0.2) == "READY=1":
                return time.monotonic()
        except queue.Empty:
            pass
        if _process_exited(pid):
            raise BenchmarkError(
                f"journalpump exited before READY=1 (fixture rejected?)\n"
                f"{stderr_path.read_text(errors='replace')[-2000:]}"
            )
    raise BenchmarkError(f"journalpump did not send READY=1 within {READY_TIMEOUT_S:.0f}s")


def _wait_output_growth(output_path: Path) -> None:
    deadline = time.monotonic() + PREFLIGHT_GROWTH_TIMEOUT_S
    while time.monotonic() < deadline:
        with contextlib.suppress(OSError):
            if output_path.stat().st_size > 0:
                return
        time.sleep(0.05)
    raise BenchmarkError(f"no output grew within {PREFLIGHT_GROWTH_TIMEOUT_S:.0f}s of READY=1")


def _wait_for_sent_lines(
    statsd: StatsdListener, entries: int, started_at: float, timeout_s: float, pid: int, stderr_path: Path
) -> None:
    deadline = started_at + timeout_s
    while time.monotonic() < deadline:
        if statsd.max_sent_lines >= entries:
            return
        if _process_exited(pid):
            raise BenchmarkError(
                f"journalpump exited before processing all entries "
                f"(sent_lines seen: {statsd.max_sent_lines}/{entries})\n"
                f"{stderr_path.read_text(errors='replace')[-2000:]}"
            )
        time.sleep(0.05)
    raise BenchmarkError(f"journal.sent_lines never reached {entries} within {timeout_s:.0f}s (saw {statsd.max_sent_lines})")


def _wait4(process: subprocess.Popen, pid: int, stop_timeout_s: float) -> tuple[float, int, Any]:
    """Exactly ONE ``os.wait4``: the exit status and the whole-run rusage."""
    deadline = time.monotonic() + stop_timeout_s
    while True:
        reaped, status, rusage = os.wait4(pid, os.WNOHANG)
        if reaped != 0:
            process.returncode = os.waitstatus_to_exitcode(status)
            return time.monotonic(), process.returncode, rusage
        if time.monotonic() > deadline:
            with contextlib.suppress(ProcessLookupError):
                os.kill(pid, signal.SIGKILL)
            _, status, rusage = os.wait4(pid, 0)
            process.returncode = os.waitstatus_to_exitcode(status)
            return time.monotonic(), process.returncode, rusage
        time.sleep(0.02)


# --------------------------------------------------------------------------
# Benchmark orchestration


def run_implementation(
    *,
    implementation: str,
    template: str,
    journal_path: Path,
    work_dir: Path,
    entries: int,
    runs: int,
    warmup: int,
    timeout_s: float,
    stop_timeout_s: float,
) -> dict[str, Any]:
    config_path = work_dir / f"config-{implementation}.json"

    preflight = run_once(
        implementation=implementation,
        template=template,
        journal_path=journal_path,
        output_path=work_dir / f"preflight-{implementation}.jsonl",
        config_path=config_path,
        entries=entries,
        mode="preflight",
        timeout_s=timeout_s,
        stop_timeout_s=stop_timeout_s,
        work_dir=work_dir,
    )

    measured: list[dict[str, Any]] = []
    for index in range(warmup + runs):
        measured.append(
            run_once(
                implementation=implementation,
                template=template,
                journal_path=journal_path,
                output_path=work_dir / f"{implementation}-run-{index}.jsonl",
                config_path=config_path,
                entries=entries,
                mode="measured",
                timeout_s=timeout_s,
                stop_timeout_s=stop_timeout_s,
                work_dir=work_dir,
            )
        )
    return {"preflight": preflight, "runs": measured[warmup:]}


def build_result(
    *, implementations: dict[str, Any], entries: int, fixture: dict[str, Any], config_hash: str, warmup: int, runs: int
) -> dict[str, Any]:
    return {
        "schema": 1,
        "date": date.today().isoformat(),
        "host": host_facts(),
        "versions": versions(),
        "config_hash": config_hash,
        "fixture": fixture,
        "entries": entries,
        "interval_s": report.REPORTING_INTERVAL_S,
        "warmup_runs": warmup,
        "measured_runs": runs,
        "implementations": implementations,
    }


def _newest_baseline() -> Path:
    candidates = sorted(RESULTS_DIR.glob("baseline-*.json"))
    if not candidates:
        raise SystemExit("no committed baseline found under bench/results/")
    return candidates[-1]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--baseline", action="store_true", help="write the committed baseline artifacts")
    parser.add_argument("--check", action="store_true", help="compare this run against the committed Go baseline")
    parser.add_argument("--out", type=Path, default=None, help="write the result JSON here")
    parser.add_argument("--fixture-dir", type=Path, default=None)
    parser.add_argument("--entries", type=int, default=int(os.environ.get("BENCH_ENTRIES", str(DEFAULT_ENTRIES))))
    parser.add_argument("--seed", type=int, default=int(os.environ.get("BENCH_SEED", str(DEFAULT_SEED))))
    parser.add_argument("--runs", type=int, default=int(os.environ.get("BENCH_RUNS", str(DEFAULT_RUNS))))
    parser.add_argument("--warmup", type=int, default=int(os.environ.get("BENCH_WARMUP", str(DEFAULT_WARMUP))))
    parser.add_argument("--timeout", type=float, default=float(os.environ.get("BENCH_TIMEOUT", str(DEFAULT_TIMEOUT_S))))
    parser.add_argument(
        "--stop-timeout", type=float, default=float(os.environ.get("BENCH_STOP_TIMEOUT", str(DEFAULT_STOP_TIMEOUT_S)))
    )
    args = parser.parse_args(argv)

    check_governor()
    assert_no_shims()

    selection = os.environ.get("BENCH_IMPL", "both")
    if args.baseline or args.check:
        selection = "both"
    if selection == "both":
        impls = ["go", "python"]
    elif selection in ("go", "python"):
        impls = [selection]
    else:
        raise SystemExit(f"BENCH_IMPL must be go, python or both (got {selection!r})")

    if args.baseline:
        if args.entries < report.BASELINE_MIN_ENTRIES:
            raise SystemExit(
                f"a baseline needs at least {report.BASELINE_MIN_ENTRIES} entries (got {args.entries}); raise BENCH_ENTRIES"
            )

    runtime_root = Path(os.environ.get("BENCH_JOURNAL_DIR", f"/run/user/{os.getuid()}/journalpump-bench"))
    runtime_root.mkdir(parents=True, exist_ok=True)
    fixture_dir = args.fixture_dir or (runtime_root / f"fixture-{args.entries}-{args.seed}")
    fixture = generate_fixture(fixture_dir, entries=args.entries, seed=args.seed)

    template = CONFIG_TEMPLATE.read_text(encoding="utf-8")
    config_hash = canonical_config(template)

    work_dir = runtime_root / "work"
    work_dir.mkdir(parents=True, exist_ok=True)
    implementations: dict[str, Any] = {}

    for implementation in impls:
        print(f"benchmarking {implementation} ...", flush=True)
        implementations[implementation] = run_implementation(
            implementation=implementation,
            template=template,
            journal_path=fixture_dir,
            work_dir=work_dir,
            entries=args.entries,
            runs=args.runs,
            warmup=args.warmup,
            timeout_s=args.timeout,
            stop_timeout_s=args.stop_timeout,
        )

    result = build_result(
        implementations=implementations,
        entries=args.entries,
        fixture=fixture,
        config_hash=config_hash,
        warmup=args.warmup,
        runs=args.runs,
    )

    if args.baseline:
        go_median = statistics.median([run["wall_s"] for run in implementations["go"]["runs"]])
        if go_median < report.BASELINE_MIN_WALL_S:
            raise SystemExit(
                f"Go median wall is {go_median:.1f}s, below the {report.BASELINE_MIN_WALL_S:.0f}s baseline minimum: "
                "raise BENCH_ENTRIES until a run is long enough to be stable"
            )

    stem = f"{'baseline' if args.baseline else 'run'}-{result['date']}-{socket.gethostname()}"
    out_path = args.out or (RESULTS_DIR / f"{stem}.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print()
    print(report.render_table(result))
    print(f"\nresult: {out_path}")

    if args.baseline:
        docs = REPO_ROOT / "docs" / "perf-baseline.md"
        docs.write_text(report.render_markdown(result) + "\n", encoding="utf-8")
        print(f"wrote {docs}")

    if args.check:
        baseline = json.loads(_newest_baseline().read_text(encoding="utf-8"))
        ok, lines = report.compare_go_baseline(baseline, result)
        print()
        print("\n".join(lines))
        return 0 if ok else 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())