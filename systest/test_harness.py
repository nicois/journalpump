# Copyright 2026, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.
"""Harness unit tests (plan Task 10): the notify socket and exit-code
capture are exercised against a deterministic stub binary, independent
of the implementation under test. These also pin the JOURNALPUMP_BIN
selection that makes the harness implementation-agnostic."""

from systest.journalpump_process import JournalpumpProcess, _command
from pathlib import Path

import json
import shutil
import pytest
import sys
import stat

_STUB_SOURCE = Path(__file__).parent / "data" / "notify_stub.py"


@pytest.fixture
def stub_path(tmp_path: Path) -> Path:
    stub = tmp_path / "notify_stub"
    shutil.copy(_STUB_SOURCE, stub)
    stub.chmod(0o755)
    return stub


def _config(tmp_path: Path) -> Path:
    config_path = tmp_path / "journalpump.json"
    config_path.write_text(json.dumps({"readers": {"reader": {"senders": {}}}}), encoding="utf-8")
    return config_path


def test_command_selection() -> None:
    # unset or "python": the Python module invocation (today's default,
    # make systest exercises the Python binary during the migration).
    assert _command(None) == [sys.executable, "-m", "journalpump"]
    assert _command("python") == [sys.executable, "-m", "journalpump"]
    # "go": the built Go binary (make build-go).
    assert _command("go") == [str(Path(__file__).resolve().parent.parent / "build" / "journalpump")]
    # any other value: an explicit binary path (stubs included).
    assert _command("/some/journalpump-bin") == ["/some/journalpump-bin"]


def test_harness_captures_notify_datagrams_and_exit_code(tmp_path: Path, stub_path: Path) -> None:
    config_path = _config(tmp_path)
    with JournalpumpProcess() as journalpump_process:
        # start() itself asserts the READY=1 datagram arrived on the
        # notify socket; the WATCHDOG=1 ping follows it on the queue.
        journalpump_process.start(config_path, env={"JOURNALPUMP_BIN": str(stub_path)})
        journalpump_process._wait_notification("WATCHDOG=1", timeout=5)
        # SIGHUP reload sequence through the same socket.
        journalpump_process.reload()
        # SIGTERM is answered with STOPPING=1 and a captured exit code 0.
        returncode = journalpump_process.stop()
        assert returncode == 0


def test_harness_reports_exit_before_notification(tmp_path: Path, tmp_path_factory: pytest.TempPathFactory) -> None:
    # A binary that exits 1 immediately without any notification: start
    # must surface the exit (code + stderr) instead of hanging.
    broken = tmp_path / "broken_stub"
    broken.write_text("#!/bin/sh\nexit 1\n", encoding="utf-8")
    broken.chmod(0o755)
    config_path = tmp_path / "journalpump.json"
    config_path.write_text("{}", encoding="utf-8")
    with JournalpumpProcess() as journalpump_process:
        with pytest.raises(AssertionError, match="exited with 1 before READY=1"):
            journalpump_process.start(config_path, env={"JOURNALPUMP_BIN": str(broken)})


def test_harness_timeout_on_silent_binary(tmp_path: Path, stub_path: Path) -> None:
    # A binary that never notifies: start fails with the harness
    # timeout, not a silent hang.
    silent = stub_path.parent / "silent_stub"
    silent.write_text("#!/bin/sh\nexec sleep 30\n", encoding="utf-8")
    silent.chmod(0o755)
    config_path = tmp_path / "journalpump.json"
    config_path.write_text("{}", encoding="utf-8")
    with JournalpumpProcess() as journalpump_process:
        with pytest.raises(TimeoutError, match="did not send READY=1"):
            journalpump_process.start(config_path, env={"JOURNALPUMP_BIN": str(silent)})


def test_stub_notify_protocol_speaks_reload_pair(tmp_path: Path, stub_path: Path) -> None:
    # Indirect coverage of the stub's reload pair via the harness: the
    # reload() helper asserts RELOADING=1 → READY=1 ordering.
    config_path = _config(tmp_path)
    with JournalpumpProcess() as journalpump_process:
        journalpump_process.start(config_path, env={"JOURNALPUMP_BIN": str(stub_path)})
        journalpump_process.reload()
        journalpump_process.stop()


def test_stub_source_is_executable_script(stub_path: Path) -> None:
    # The stub must be directly executable: JOURNALPUMP_BIN selects a
    # BINARY, not a module invocation.
    assert stat.S_IMODE(stub_path.stat().st_mode) & 0o111
    assert stub_path.read_text(encoding="utf-8").startswith("#!")
