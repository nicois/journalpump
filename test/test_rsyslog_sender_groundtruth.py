# Copyright 2026, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.
"""Capture harness for the rsyslog SENDER wire fixture (plan Task 20).

``internal/sender/rsyslog/testdata/wire_groundtruth.json`` must be the
exact bytes the REAL ``journalpump.senders.rsyslog.RsyslogSender`` puts on
the wire. This module drives the real sender against a stubbed rsyslogd
(an in-process recording TCP server) and either writes the fixture
(``GROUNDTRUTH_UPDATE=1 pytest test/test_rsyslog_sender_groundtruth.py``)
or verifies the committed fixture matches a fresh capture.

The Go parity test
(``internal/sender/rsyslog/wire_groundtruth_test.go``) replays the same
committed JSON through the Go sender, so both implementations are pinned
to one capture. Cases deliberately carry a timestamp on every message:
the missing-timestamp fallback is wall-clock and is covered by the unit
tests instead.
"""

from journalpump.senders.rsyslog import RsyslogSender
from pathlib import Path
from unittest import mock

import json
import os
import socketserver
import threading

FIXTURE = Path(__file__).resolve().parent.parent / "internal" / "sender" / "rsyslog" / "testdata" / "wire_groundtruth.json"


class _RecordingServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True

    def __init__(self):
        super().__init__(("127.0.0.1", 0), _Handler)
        self.buffer = bytearray()
        self._lock = threading.Lock()
        self.closed = threading.Event()


class _Handler(socketserver.BaseRequestHandler):
    def handle(self):
        try:
            while True:
                data = self.request.recv(65536)
                if not data:
                    break
                with self.server._lock:
                    self.server.buffer.extend(data)
        finally:
            self.server.closed.set()


def _capture(config, messages):
    """Send the payloads through the real sender and return wire bytes."""
    server = _RecordingServer()
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        port = server.server_address[1]
        sender = RsyslogSender(
            config={**config, "rsyslog_server": "127.0.0.1", "rsyslog_port": port},
            name="test-rsyslog",
            reader=mock.Mock(),
            stats=mock.Mock(),
            field_filter=None,
        )
        encoded = [json.dumps(m).encode("utf-8") for m in messages]
        assert sender.send_messages(messages=encoded, cursor="c") is True
        # Close the client so the recorder observes EOF promptly, then wait
        # for the handler to finish draining (no sleep-based guessing).
        if sender.rsyslog_client is not None:
            sender.rsyslog_client.close()
        if not server.closed.wait(timeout=5):
            raise AssertionError("recording server did not observe the client EOF")
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()
    return bytes(server.buffer)


def _cases():
    ts = "2025-01-01T00:00:00.000000"
    cases = []

    cases.append(
        {
            "name": "rfc5424_priorities",
            "config": {},
            "messages": [
                {"MESSAGE": "Info message", "PRIORITY": "6", "timestamp": ts, "HOSTNAME": "h", "PID": "100"},
                {"MESSAGE": "Critical message", "PRIORITY": "2", "timestamp": ts, "HOSTNAME": "h", "PID": "100"},
                {"MESSAGE": "Ident message", "PRIORITY": "3", "timestamp": ts, "SYSLOG_IDENTIFIER": "ident"},
            ],
        }
    )
    cases.append(
        {
            "name": "multiline_nontransparent",
            "config": {"octet_counted_framing": False},
            "messages": [{"MESSAGE": "Info message\nexample\nstack\ntrace", "PRIORITY": "6", "timestamp": ts}],
        }
    )
    cases.append(
        {
            "name": "multiline_octet_counted",
            "config": {"octet_counted_framing": True},
            "messages": [{"MESSAGE": "Info message\nexample\nstack\ntrace", "PRIORITY": "6", "timestamp": ts}],
        }
    )
    cases.append(
        {
            "name": "multiline_escape_newlines",
            "config": {"escape_newlines": True},
            "messages": [{"MESSAGE": "Info message\nexample\nstack\ntrace", "PRIORITY": "6", "timestamp": ts}],
        }
    )
    cases.append(
        {
            "name": "oversize_octet_counted_truncation",
            "config": {"octet_counted_framing": True, "max_message_size": 200},
            "messages": [
                {"MESSAGE": "Info message\n" + "X" * 300, "PRIORITY": "6", "timestamp": ts},
                {"MESSAGE": "Critical message", "PRIORITY": "2", "timestamp": ts},
            ],
        }
    )
    cases.append(
        {
            "name": "custom_logline",
            "config": {
                "format": "custom",
                "logline": "<%pri%>%timestamp% %HOSTNAME% %app-name%[%procid%]: %msg% {%%} %not-valid-tag%",
            },
            "messages": [{"MESSAGE": "Info message", "PRIORITY": "6", "timestamp": ts, "HOSTNAME": "h", "PID": "100"}],
        }
    )
    cases.append(
        {
            "name": "numeric_pid_and_fields",
            "config": {},
            "messages": [
                {"MESSAGE": "numeric pid", "PRIORITY": 6, "timestamp": ts, "HOSTNAME": "h", "PID": 100, "SYSLOG_FACILITY": 3},
                {"MESSAGE": "zero pid is nilvalue", "PRIORITY": 2, "timestamp": ts, "PID": 0},
            ],
        }
    )
    cases.append(
        {
            "name": "facility_list_and_structured_data",
            "config": {"structured_data": 'x="y"', "default_facility": 3},
            "messages": [
                {"MESSAGE": "with sd", "PRIORITY": "6", "timestamp": ts, "SYSLOG_FACILITY": ["4", "5"]},
                {"MESSAGE": "default facility", "PRIORITY": "6", "timestamp": ts, "SYSLOG_FACILITY": "bogus"},
            ],
        }
    )
    return cases


def capture():
    out = []
    for case in _cases():
        wire = _capture(case["config"], case["messages"])
        out.append({"name": case["name"], "config": case["config"], "messages": case["messages"], "wire_hex": wire.hex()})
    return out


def test_groundtruth_fixture_matches_python_capture():
    records = capture()
    if os.environ.get("GROUNDTRUTH_UPDATE") == "1":
        FIXTURE.parent.mkdir(parents=True, exist_ok=True)
        FIXTURE.write_text(json.dumps(records, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    committed = json.loads(FIXTURE.read_text(encoding="utf-8"))
    assert committed == records, "fixture drifted from the real Python capture; rerun with GROUNDTRUTH_UPDATE=1"