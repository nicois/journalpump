# Copyright 2026, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.
"""Implementation-agnostic systest harness (spec §7, plan Task 10).

The harness spawns a journalpump BINARY — never an in-process import —
so every black-box suite built on it runs against BOTH implementations.
The ``JOURNALPUMP_BIN`` environment variable selects the implementation:

- unset or empty, or ``python``: ``python3 -m journalpump`` (the Python
  daemon; the default keeps ``make systest`` exercising the Python
  binary during the migration);
- ``go``: the built Go binary at ``build/journalpump`` (``make build-go``);
- any other value: treated as the path of a journalpump-compatible
  binary (or executable stub, as the harness unit tests use).

Whatever binary is selected gets the same environment: ``RUNTIME_DIRECTORY``
(unless suppressed), an abstract-socket ``NOTIFY_SOCKET``, and the
caller-provided ``env``. The harness asserts the systemd notify
datagrams (``READY=1``, ``RELOADING=1`` → ``READY=1``, ``WATCHDOG=1``)
and captures the exit code on SIGTERM.
"""

from pathlib import Path
from typing import Self

import os
import queue
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time

_GO_BINARY_PATH = Path(__file__).resolve().parent.parent / "build" / "journalpump"


def command_for(selection: str | None) -> list[str]:
    """Return the argv for the journalpump implementation selected by
    the ``JOURNALPUMP_BIN`` value (see the module docstring). An empty
    string is treated like unset (the environment may carry an empty
    variable where the intent was "no selection").

    Importable so the benchmark driver (``bench/journalpump_bench.py``)
    selects the same binary as the systests without duplicating the rule.
    """
    if not selection or selection == "python":
        return [sys.executable, "-m", "journalpump"]
    if selection == "go":
        return [str(_GO_BINARY_PATH)]
    return [selection]


# Backwards-compatible private alias (the module-internal call sites and
# any early callers used the underscored name).
_command = command_for


def build_environment(
    *,
    runtime_directory: Path,
    notify_socket_name: str,
    env: dict[str, str] | None = None,
    export_runtime_directory: bool = True,
) -> dict[str, str]:
    """Return the environment a journalpump child is started with: the
    caller's environment plus the optional overrides, ``RUNTIME_DIRECTORY``
    (unless suppressed) and the abstract-socket ``NOTIFY_SOCKET``, and
    ``PYTHONUNBUFFERED=1``.

    Factored out of :meth:`JournalpumpProcess.start` with no behavior
    change so the benchmark driver constructs an identical environment.
    """
    journalpump_env = os.environ.copy()
    if env:
        journalpump_env.update(env)
    if export_runtime_directory:
        journalpump_env["RUNTIME_DIRECTORY"] = str(runtime_directory)
    else:
        journalpump_env.pop("RUNTIME_DIRECTORY", None)
    journalpump_env["NOTIFY_SOCKET"] = "@" + notify_socket_name
    journalpump_env["PYTHONUNBUFFERED"] = "1"
    return journalpump_env


class NotifyListener:
    """An abstract-socket ``NOTIFY_SOCKET`` listener: owns the bound
    datagram socket, the reader thread and the decoded ``READY=1`` /
    ``RELOADING=1`` / ``WATCHDOG=1`` notification queue.

    Factored out of :class:`JournalpumpProcess` (same socket name shape and
    read loop) so the benchmark driver can wait for ``READY=1`` with the
    identical listener.
    """

    def __init__(self) -> None:
        self.name = f"jp-systest-{os.getpid()}-{os.urandom(4).hex()}"
        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        self.socket.bind("\0" + self.name)
        self.notifications: queue.SimpleQueue[str] = queue.SimpleQueue()
        self._thread = threading.Thread(
            target=_read_notifications,
            args=(self.socket, self.notifications),
            name="journalpump-notify-reader",
            daemon=True,
        )
        self._thread.start()

    def close(self) -> None:
        self.socket.close()
        self._thread.join(timeout=2)


def _raise_if_journalpump_exited(journalpump: subprocess.Popen[str] | None, *, waiting_for: str) -> None:
    if journalpump is None or journalpump.poll() is None:
        return
    stderr = journalpump.stderr.read() if journalpump.stderr is not None else ""
    raise AssertionError(f"journalpump exited with {journalpump.returncode} before {waiting_for}\n{stderr}")


def _read_notifications(
    notify_socket: socket.socket,
    notifications: queue.SimpleQueue[str],
) -> None:
    while True:
        try:
            notify_datagram = notify_socket.recv(4096)
        except OSError:
            return
        if not notify_datagram:
            return
        for line in notify_datagram.decode("ascii").split("\n"):
            if line:
                notifications.put(line)


class JournalpumpProcess:
    def __enter__(self) -> Self:
        self.runtime_directory = Path(tempfile.mkdtemp(prefix="jp-runtime-"))
        self._listener = NotifyListener()
        self._notify_socket_name = self._listener.name
        self._notifications = self._listener.notifications
        self._journalpump: subprocess.Popen[str] | None = None
        return self

    def __exit__(self, *_exc: object) -> None:
        self.stop()
        self._listener.close()
        shutil.rmtree(self.runtime_directory, ignore_errors=True)

    def _wait_notification(self, notification: str, *, timeout: float) -> None:
        start_time = time.monotonic()
        elapsed_time = 0.0
        while elapsed_time < timeout:
            try:
                if self._notifications.get(timeout=timeout - elapsed_time) == notification:
                    return
            except queue.Empty:
                break
            elapsed_time = time.monotonic() - start_time
        _raise_if_journalpump_exited(self._journalpump, waiting_for=notification)
        raise TimeoutError(f"journalpump did not send {notification}")

    def start(self, config_path: Path, *, env: dict[str, str] | None = None, export_runtime_directory: bool = True) -> None:
        journalpump_env = build_environment(
            runtime_directory=self.runtime_directory,
            notify_socket_name=self._notify_socket_name,
            env=env,
            export_runtime_directory=export_runtime_directory,
        )
        argv = command_for(journalpump_env.get("JOURNALPUMP_BIN"))
        self._journalpump = subprocess.Popen(
            [*argv, str(config_path)],
            env=journalpump_env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        self._wait_notification("READY=1", timeout=10)

    def reload(self) -> None:
        if self._journalpump is None or self._journalpump.pid is None:
            raise RuntimeError("journalpump is not running")
        os.kill(self._journalpump.pid, signal.SIGHUP)
        self._wait_notification("RELOADING=1", timeout=3)
        self._wait_notification("READY=1", timeout=3)

    def stop(self) -> int | None:
        if self._journalpump is None:
            return None
        if self._journalpump.poll() is None:
            self._journalpump.send_signal(signal.SIGTERM)
            try:
                self._journalpump.wait(timeout=8)
            except subprocess.TimeoutExpired:
                self._journalpump.kill()
                self._journalpump.wait(timeout=5)
        return self._journalpump.returncode
