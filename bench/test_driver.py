# Copyright 2026, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.
"""Unit tests for the benchmark driver's instrument plumbing (plan Task
31). The statsd gauge parser is the load-bearing part: a mis-parsed name
would leave a run waiting forever for a count it already reached."""

from pathlib import Path

import os
import shutil
import socket
import subprocess
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import journalpump_bench as driver  # noqa: E402

TEMPLATE = driver.CONFIG_TEMPLATE.read_text(encoding="utf-8")


def wait_for(listener: driver.StatsdListener, target: int, timeout: float = 3.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if listener.max_sent_lines >= target:
            return
        time.sleep(0.02)
    raise AssertionError(f"gauge never reached {target} (saw {listener.max_sent_lines})")


def test_statsd_listener_reads_both_client_shapes():
    listener = driver.StatsdListener()
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # Go shape: telegraf tags in the name portion.
        sock.sendto(b"journal.sent_lines,sender=out:2000|g\n", ("127.0.0.1", listener.port))
        wait_for(listener, 2000)

        # Python shape (untagged) and an unrelated metric that must be
        # ignored.
        sock.sendto(b"journal.read_lines:99999|g\njournal.sent_lines:2001|g\n", ("127.0.0.1", listener.port))
        wait_for(listener, 2001)
        assert listener.max_sent_lines == 2001

        # A lower reading must not lower the maximum.
        sock.sendto(b"journal.sent_lines:5|g\n", ("127.0.0.1", listener.port))
        time.sleep(0.2)
        assert listener.max_sent_lines == 2001
    finally:
        sock.close()
        listener.close()


def test_count_lines_counts_newlines(tmp_path: Path):
    path = tmp_path / "out.jsonl"
    path.write_bytes(b"a\nb\nc\n")
    lines, size = driver._count_lines(path)
    assert (lines, size) == (3, 6)
    assert driver._count_lines(tmp_path / "missing.jsonl") == (0, 0)


def test_process_exited_sees_a_live_and_a_finished_child():
    assert driver._process_exited(os.getpid()) is False

    child = subprocess.Popen(["/bin/true"])
    os.wait4(child.pid, 0)
    assert driver._process_exited(child.pid) is True


def test_read_governor_and_cgroup_limits_never_raise():
    assert isinstance(driver.read_governor(), str)
    assert isinstance(driver.read_cgroup_limit("memory.max"), str)


# --- exceptional-path cleanup ----------------------------------------------


def _stub(tmp_path: Path, body: str) -> list[str]:
    script = tmp_path / "stub.py"
    script.write_text(body, encoding="utf-8")
    return [sys.executable, str(script)]


def _run_stub(tmp_path: Path, command: list[str], **overrides):
    kwargs = {
        "implementation": "python",
        "template": TEMPLATE,
        "journal_path": tmp_path,
        "output_path": tmp_path / "out.jsonl",
        "config_path": tmp_path / "cfg.json",
        "entries": 2000,
        "mode": "measured",
        "timeout_s": 5.0,
        "stop_timeout_s": 5.0,
        "work_dir": tmp_path,
        "command": command,
    }
    kwargs.update(overrides)
    return driver.run_once(**kwargs)


def test_run_once_terminates_and_reaps_a_child_that_never_readies(tmp_path, monkeypatch):
    monkeypatch.setattr(driver, "READY_TIMEOUT_S", 1.0)
    pidfile = tmp_path / "pid"
    body = f"import os, time\nopen({str(pidfile)!r}, 'w').write(str(os.getpid()))\ntime.sleep(600)\n"
    with pytest.raises(driver.BenchmarkError):
        _run_stub(tmp_path, _stub(tmp_path, body))
    assert driver._process_exited(int(pidfile.read_text()))


def test_run_once_terminates_and_reaps_a_child_that_never_reaches_the_gauge(tmp_path):
    pidfile = tmp_path / "pid"
    body = (
        "import os, socket, time\n"
        f"open({str(pidfile)!r}, 'w').write(str(os.getpid()))\n"
        "addr = os.environ['NOTIFY_SOCKET']\n"
        "if addr.startswith('@'):\n"
        "    addr = '\\x00' + addr[1:]\n"
        "sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)\n"
        "sock.sendto(b'READY=1', addr)\n"
        "time.sleep(600)\n"
    )
    with pytest.raises(driver.BenchmarkError):
        _run_stub(tmp_path, _stub(tmp_path, body), timeout_s=1.0)
    assert driver._process_exited(int(pidfile.read_text()))


def test_run_once_passes_extra_env_to_the_child(tmp_path):
    seen = tmp_path / "seen.txt"
    body = f"import os\nopen({str(seen)!r}, 'w').write(os.environ.get('JOURNALPUMP_CPU_PROFILE', ''))\n"
    with pytest.raises(driver.BenchmarkError):
        _run_stub(tmp_path, _stub(tmp_path, body), extra_env={"JOURNALPUMP_CPU_PROFILE": "/tmp/x.prof"}, timeout_s=1.0)
    assert seen.read_text() == "/tmp/x.prof"


def test_terminate_and_reap_kills_a_live_child_and_is_idempotent():
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(600)"])
    driver._terminate_and_reap(child, child.pid, 5.0)
    assert child.returncode is not None
    assert driver._process_exited(child.pid)
    driver._terminate_and_reap(child, child.pid, 5.0)  # must not double-reap


# --- output/work directory --------------------------------------------------


def test_resolve_work_dir_defaults_under_bench_tmpfs(monkeypatch):
    root = Path(f"/run/user/{os.getuid()}/jp-bench-tmpfs-test")
    monkeypatch.delenv("BENCH_WORK_DIR", raising=False)
    monkeypatch.setenv("BENCH_TMPFS", str(root))
    try:
        assert driver.resolve_work_dir() == root / "work"
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_resolve_work_dir_accepts_an_explicit_tmpfs_override(monkeypatch):
    explicit = Path(f"/run/user/{os.getuid()}/jp-bench-workdir-test")
    monkeypatch.setenv("BENCH_WORK_DIR", str(explicit))
    try:
        assert driver.resolve_work_dir() == explicit
    finally:
        shutil.rmtree(explicit, ignore_errors=True)


def test_resolve_work_dir_refuses_a_disk_backed_directory(monkeypatch):
    disk = Path("/var/tmp") / f"jp-bench-disk-{os.getpid()}"
    if driver.filesystem_type(disk.parent) == "tmpfs":  # pragma: no cover - host-dependent
        pytest.skip("no disk-backed path available on this host")
    monkeypatch.setenv("BENCH_WORK_DIR", str(disk))
    try:
        with pytest.raises(SystemExit):
            driver.resolve_work_dir()
    finally:
        shutil.rmtree(disk, ignore_errors=True)

# --- profiling capture ------------------------------------------------------


def test_main_rejects_profile_combined_with_baseline_or_check(capsys):
    # A baseline or a check must measure BOTH implementations; --profile runs
    # the Go binary alone, so the combination would silently write a Go-only
    # baseline. It is rejected before anything runs or is written.
    for flags in (["--profile", "--baseline"], ["--profile", "--check"]):
        with pytest.raises(SystemExit) as excinfo:
            driver.main(flags)
        assert excinfo.value.code == 2
        assert "--profile cannot be combined" in capsys.readouterr().err


def test_clear_profiles_removes_every_known_profile(tmp_path):
    for _, name in driver.PROFILE_FILES:
        (tmp_path / name).write_bytes(b"stale")
    driver.clear_profiles(tmp_path)
    assert list(tmp_path.iterdir()) == []


def test_verify_profiles_rejects_a_missing_profile(tmp_path):
    driver.clear_profiles(tmp_path)
    with pytest.raises(driver.BenchmarkError, match="produced no"):
        driver.verify_profiles(tmp_path, time.time())


def test_verify_profiles_rejects_an_empty_profile(tmp_path):
    driver.clear_profiles(tmp_path)
    (tmp_path / "go-cpu-measured.prof").write_bytes(b"")
    with pytest.raises(driver.BenchmarkError, match="is empty"):
        driver.verify_profiles(tmp_path, time.time())


def test_verify_profiles_rejects_a_stale_profile(tmp_path):
    driver.clear_profiles(tmp_path)
    path = tmp_path / "go-cpu-measured.prof"
    path.write_bytes(b"not a profile")
    old = time.time() - 3600
    os.utime(path, (old, old))
    with pytest.raises(driver.BenchmarkError, match="stale"):
        driver.verify_profiles(tmp_path, time.time() - 60)


def test_verify_profiles_rejects_a_profile_pprof_cannot_read(tmp_path):
    driver.clear_profiles(tmp_path)
    for _, name in driver.PROFILE_FILES:
        (tmp_path / name).write_bytes(b"garbage, not a pprof profile")
    with pytest.raises(driver.BenchmarkError, match="could not read"):
        driver.verify_profiles(tmp_path, time.time() - 1)


def test_print_profiles_raises_when_a_profile_is_missing(tmp_path):
    driver.clear_profiles(tmp_path)
    with pytest.raises(driver.BenchmarkError, match="no profile at"):
        driver.print_profiles(tmp_path)
