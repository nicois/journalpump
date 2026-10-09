# Copyright 2026, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.
"""Ground-truth capture, logplex sender (plan Task 11).

Runs the real Python binary against a local HTTP server that answers
500 to every request, and pins the exact POST bodies (length-prefixed
RFC5424-style frames) and headers.

Quirk-lock ground truth captured here:

- the server answers 500 to EVERY request, yet the captured frame
  stream contains each of the 40 messages exactly once in order —
  a 500 response does NOT trigger retry or disconnect, and the sender
  still marks the batch sent;
- hostname and pid are the literal "localhost" fallbacks: the sender
  looks up ``_HOSTNAME``/``_PID`` in the payload, whose keys are
  stored un-underscored (``HOSTNAME``/``PID``);
- frame timestamps are wall clock (``datetime.now``) and are
  normalized to a fixed-width placeholder in the fixture.
"""

from systest import groundtruth_support as gt
from pathlib import Path

import base64
import pytest

_FIXTURE_NAME = "logplex"
_TOKEN = "token"


def _config(state_path: Path, base_url: str, journal_path: Path) -> dict:
    config = gt.base_reader_config(
        journal_path,
        {
            "out": {
                "output_type": "logplex",
                "logplex_log_input_url": f"{base_url}/",
                "logplex_token": _TOKEN,
            }
        },
    )
    config["json_state_file_path"] = str(state_path)
    return config


def test_logplex_wire_bytes(tmp_path: Path) -> None:
    fixture = gt.load_fixture(_FIXTURE_NAME)
    routes = {("POST", "/"): (500, b"error", "text/plain")}
    server = gt.CaptureHTTPServer(routes)
    server.start()
    try:
        journal_path = gt.copy_journals(tmp_path / "logs")
        state_path = tmp_path / "state.json"
        with gt.groundtruth_pump(_config(state_path, server.base_url, journal_path), tmp_path) as process:
            gt.wait_until(
                lambda: sum(len(gt.parse_logplex_frames(r["body"])) for r in server.requests_matching("POST", "/")) >= 40,
                description="40 captured logplex frames",
            )
            process.stop()
    finally:
        server.stop()

    posts = server.requests_for("POST", "/")
    assert posts, "no logplex POST captured"
    frames: list[bytes] = []
    for post in posts:
        # The quirk-lock evidence: every response was 500, yet each
        # message appears exactly once across the captured requests.
        assert post["headers"]["logplex-msg-count"] == str(len(gt.parse_logplex_frames(post["body"])))
        frames.extend(gt.parse_logplex_frames(post["body"]))
    assert len(frames) == 40
    normalized = [gt.normalize_logplex_frame(frame).decode("utf-8") for frame in frames]

    expected_auth = "Basic " + base64.b64encode(f"token:{_TOKEN}".encode("ascii")).decode("ascii")
    sent = gt.read_sender_state(state_path, "out")
    if fixture is None or gt.update_mode():
        fixture = {
            "name": _FIXTURE_NAME,
            "capture": {
                "journals": "systest/data/rotated_logs/*.journal (Messages 0..39)",
                "sender": {
                    "output_type": "logplex",
                    "logplex_log_input_url": "<capture server>/",
                    "logplex_token": _TOKEN,
                },
                "response_status": 500,
            },
            "request": {
                "headers": {
                    "authorization": expected_auth,
                    "content-type": "application/logplex-1",
                },
                "method": "POST",
                "path": "/",
            },
            "frames": normalized,
            "expected": {
                "sent_count": sent["sent"]["count"],
                "sent_cursor": sent["sent"]["cursor"],
                "health_status": sent["health"]["status"],
            },
            "notes": [
                "every POST got a 500 response; the captured stream holds each of the 40 "
                "messages exactly once in order => 500 does NOT trigger retry/disconnect, "
                "and the sender still marks the batch sent",
                "WIRE SHAPE: each length prefix is followed by the Python bytes REPR of the "
                "packet, b'...', which is len+3 characters on the wire (format_msg f-strings "
                "the encoded bytes packet); the length prefix counts the packet content only; "
                "the frames stored here are that content with timestamps normalized",
                "frame timestamps normalized to 0000-00-00T00:00:00+00:00 (wall clock in Python)",
                "hostname and pid are the literal 'localhost' fallbacks: the sender looks up "
                "_HOSTNAME/_PID but payload keys are stored un-underscored (HOSTNAME/PID)",
                "batch/POST boundaries are timing-dependent: compare the concatenated frame "
                "stream, not per-request splits",
            ],
        }
        gt.save_fixture(_FIXTURE_NAME, fixture)
        if not gt.update_mode():
            pytest.fail("logplex fixture was missing and has been generated; commit it and rerun")

    assert normalized == fixture["frames"]
    for post in posts:
        assert post["method"] == "POST"
        assert post["path"] == "/"
        assert post["headers"]["authorization"] == fixture["request"]["headers"]["authorization"]
        assert post["headers"]["content-type"] == fixture["request"]["headers"]["content-type"]
    assert sent["sent"]["count"] == fixture["expected"]["sent_count"]
    assert sent["sent"]["cursor"] == fixture["expected"]["sent_cursor"]
    assert sent["health"]["status"] == fixture["expected"]["health_status"]
