#!/usr/bin/env python3
# Copyright 2026, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.
"""Minimal journalpump stand-in for harness unit tests (plan Task 10:
harness unit tests for the notify socket and exit-code capture).

Executable script (chmod +x): it speaks the systemd notify protocol on
the inherited NOTIFY_SOCKET like the real daemon — READY=1 on start,
RELOADING=1 → READY=1 on SIGHUP, WATCHDOG=1 once started, STOPPING=1
and exit 0 on SIGTERM — without touching journald or any sender.
"""

import os
import signal
import socket
import sys
import time


def notify(state: str) -> None:
    addr = os.environ["NOTIFY_SOCKET"]
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    target = "\0" + addr[1:] if addr.startswith("@") else addr
    sock.connect(target)
    sock.sendall(state.encode("ascii"))
    sock.close()


def on_hup(signum: int, frame: object) -> None:
    notify("RELOADING=1")
    notify("READY=1")


def on_term(signum: int, frame: object) -> None:
    notify("STOPPING=1")
    sys.exit(0)


def main() -> None:
    signal.signal(signal.SIGHUP, on_hup)
    signal.signal(signal.SIGTERM, on_term)
    notify("READY=1")
    notify("WATCHDOG=1")
    while True:
        time.sleep(0.05)


if __name__ == "__main__":
    main()
