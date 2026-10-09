# Copyright 2026, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.
"""Capture harness for the syslog-framing wire fixture (plan Task 19).

``internal/syslogframing/testdata/logline_groundtruth.json`` must be the
exact bytes the REAL ``journalpump.rsyslog.SyslogTcpClient`` puts on the
wire — never hand-written. This module is the reproducible generator: it
drives the real client with ``_connect`` patched to a recording stub and
either writes the fixture (``GROUNDTRUTH_UPDATE=1 pytest
test/test_rsyslog_groundtruth.py``) or verifies the committed fixture
matches a fresh capture.

The Go parity test (``internal/syslogframing/syslogframing_test.go``:
``TestLogMatchesPythonGroundtruth``) replays the same committed JSON, so
the two implementations are pinned to one capture.
"""

from journalpump.rsyslog import SyslogTcpClient
from pathlib import Path
from unittest import mock

import json
import os

FIXTURE = Path(__file__).resolve().parent.parent / "internal" / "syslogframing" / "testdata" / "logline_groundtruth.json"


class _Stub:
    def __init__(self):
        self.sent = bytearray()

    def sendall(self, data):
        self.sent.extend(data)

    def close(self):
        pass


def _cases():
    cases = []

    def add(**kw):
        cases.append(kw)

    # formatter/PRI/date coverage across RFCs
    for rfc, lf in [
        ("RFC5424", None),
        ("RFC3164", None),
        ("CUSTOM", "%pri% %timestamp% %HOSTNAME% %app-name% %procid% %msgid% %msg%"),
        ("CUSTOM", "%timestamp:::date-rfc3339% <%pri%> %unknown-token% {literal} %msg%"),
    ]:
        for fac, sev in [(1, 6), (23, 7), (0, 0), (24, 6), (1, 8), (-1, 3), (1, -3)]:
            add(
                rfc=rfc,
                log_format=lf,
                facility=fac,
                severity=sev,
                timestamp="2024-03-09T13:45:07.123456Z",
                hostname="host.example",
                program="myapp",
                pid=4321,
                msgid="ID47",
                msg="hello world",
                sd=None,
                octet_counted_framing=False,
                escape_newlines=False,
                max_msg=4096,
            )

    # structured data variants (RFC5424)
    for sd in [None, "", 'x="y"']:
        add(
            rfc="RFC5424",
            log_format=None,
            facility=1,
            severity=6,
            timestamp="2024-01-01T00:00:00.000000Z",
            hostname="h",
            program="a",
            pid=1,
            msgid="m",
            msg="body",
            sd=sd,
            octet_counted_framing=False,
            escape_newlines=False,
            max_msg=4096,
        )

    # CUSTOM %structured-data% rendering: sd=None formats as the literal
    # characters "None" (Python str.format), a present value as itself.
    for sd in [None, "", 'x="y"']:
        add(
            rfc="CUSTOM",
            log_format="%structured-data%",
            facility=1,
            severity=6,
            timestamp="2024-01-01T00:00:00.000000Z",
            hostname="h",
            program="a",
            pid=1,
            msgid="m",
            msg="body",
            sd=sd,
            octet_counted_framing=False,
            escape_newlines=False,
            max_msg=4096,
        )

    # NILVALUE defaults (hostname/program/proc/msgid/msg)
    add(
        rfc="RFC5424",
        log_format=None,
        facility=1,
        severity=6,
        timestamp="2024-01-01T00:00:00.000000Z",
        hostname=None,
        program="",
        pid=0,
        msgid="",
        msg="",
        sd=None,
        octet_counted_framing=False,
        escape_newlines=False,
        max_msg=4096,
    )

    # escape_newlines
    for esc in [False, True]:
        add(
            rfc="RFC5424",
            log_format=None,
            facility=1,
            severity=6,
            timestamp="2024-01-01T00:00:00.000000Z",
            hostname="host",
            program="app",
            pid=None,
            msgid=None,
            msg="l1\nl2\r\nl3",
            sd=None,
            octet_counted_framing=False,
            escape_newlines=esc,
            max_msg=4096,
        )

    # misc timestamps
    for ts in ["2024-12-31T23:59:59Z", "2024-06-01T00:00:00+02:00", "1999-01-05T09:08:07.999Z"]:
        add(
            rfc="RFC3164",
            log_format=None,
            facility=3,
            severity=4,
            timestamp=ts,
            hostname="ho",
            program="pr",
            pid=7,
            msgid=None,
            msg="x",
            sd=None,
            octet_counted_framing=False,
            escape_newlines=False,
            max_msg=4096,
        )

    # max_msg edge values: ONLY 0 falls back to the default (2048);
    # negatives are kept and applied as Python negative slice indices.
    for octet in [False, True]:
        for max_msg in [-1, -3, 0, 6, 7]:
            add(
                rfc="RFC5424",
                log_format=None,
                facility=1,
                severity=6,
                timestamp="2024-01-01T00:00:00.000000Z",
                hostname="h",
                program="a",
                pid=1,
                msgid="m",
                msg="abcdef",
                sd=None,
                octet_counted_framing=octet,
                escape_newlines=False,
                max_msg=max_msg,
            )

    return cases


def capture():
    """Run the real SyslogTcpClient for every case and return the wire
    records (implementation-neutral JSON-ready dicts)."""
    out = []
    for kw in _cases():
        with mock.patch.object(SyslogTcpClient, "_connect"):
            client = SyslogTcpClient(
                server="127.0.0.1",
                port=1,
                rfc=kw["rfc"],
                max_msg=kw["max_msg"],
                log_format=kw["log_format"],
                octet_counted_framing=kw["octet_counted_framing"],
                escape_newlines=kw["escape_newlines"],
            )
        stub = _Stub()
        client.socket = stub
        client.log(
            facility=kw["facility"],
            severity=kw["severity"],
            timestamp=kw["timestamp"],
            hostname=kw["hostname"],
            program=kw["program"],
            pid=kw["pid"],
            msgid=kw["msgid"],
            msg=kw["msg"],
            sd=kw["sd"],
        )
        record = dict(kw)
        record["wire_hex"] = bytes(stub.sent).hex()
        out.append(record)
    return out


def test_groundtruth_fixture_matches_python_capture():
    records = capture()
    if os.environ.get("GROUNDTRUTH_UPDATE") == "1":
        FIXTURE.parent.mkdir(parents=True, exist_ok=True)
        FIXTURE.write_text(json.dumps(records, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    committed = json.loads(FIXTURE.read_text(encoding="utf-8"))
    assert committed == records, "fixture drifted from the real Python capture; rerun with GROUNDTRUTH_UPDATE=1"