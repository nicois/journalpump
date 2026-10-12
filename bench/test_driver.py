# Copyright 2026, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.
"""Unit tests for the benchmark driver's instrument plumbing (plan Task
31). The statsd gauge parser is the load-bearing part: a mis-parsed name
would leave a run waiting forever for a count it already reached."""

from pathlib import Path

import os
import socket
import subprocess
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))

import journalpump_bench as driver  # noqa: E402


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