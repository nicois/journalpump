# Copyright 2026, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.
"""Ground-truth capture, Python-written state file (plan Task 11).

Runs the real Python binary with the file sender over the four fixture
journals and commits the state file it writes at SIGTERM shutdown,
minus wall-clock fields (``start_time``, ``health.elapsed``). This is
the outer leg of the state-file round trip: the Go-side state decoder
(Task 8) and resume tests must accept exactly this shape and yield the
recorded cursor values.
"""

from systest import groundtruth_support as gt
from pathlib import Path

import pytest

_FIXTURE_NAME = "state_file"
_PAD = "x" * 3072  # same padding as the file-sender fixture: forces flushes


def _config(state_path: Path, output_path: Path, journal_path: Path) -> dict:
    config = gt.base_reader_config(
        journal_path,
        {"out": {"output_type": "file", "file_output": str(output_path), "extra_field_values": {"PADDING": _PAD}}},
    )
    config["json_state_file_path"] = str(state_path)
    return config


def test_state_file_skeleton(tmp_path: Path) -> None:
    fixture = gt.load_fixture(_FIXTURE_NAME)
    journal_path = gt.copy_journals(tmp_path / "logs")
    output_path = tmp_path / "output.jsonl"
    state_path = tmp_path / "state.json"

    with gt.groundtruth_pump(_config(state_path, output_path, journal_path), tmp_path) as process:
        gt.wait_until(
            lambda: output_path.exists() and sum(1 for _ in open(output_path, "rb")) >= 10,
            description="flushed output lines",
        )
        process.stop()

    state = gt.deterministic_state(state_path)
    sender = state["readers"][gt.READER_NAME]["senders"]["out"]
    assert sender["sent"]["count"] == 40
    if fixture is None or gt.update_mode():
        fixture = {
            "name": _FIXTURE_NAME,
            "capture": {
                "journals": "systest/data/rotated_logs/*.journal (Messages 0..39)",
                "sender": {"output_type": "file", "extra_field_values": {"PADDING": _PAD}},
                "written_at": "SIGTERM shutdown save",
            },
            "state": state,
            "notes": [
                "Python-written state file with wall-clock fields dropped (start_time, "
                "health.elapsed); every remaining field is deterministic",
                "Go parity: internal/state ResumeCursor(reader, readerName) must yield "
                "senders.out.sent.cursor and tolerate this exact shape (Task 8 decode)",
            ],
        }
        gt.save_fixture(_FIXTURE_NAME, fixture)
        if not gt.update_mode():
            pytest.fail("state_file fixture was missing and has been generated; commit it and rerun")

    assert state == fixture["state"]
    assert fixture["state"]["readers"][gt.READER_NAME]["senders"]["out"]["sent"]["cursor"] == sender["sent"]["cursor"]
