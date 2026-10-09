# Copyright 2026, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.
"""Ground-truth capture, file sender (plan Task 11).

Runs the real Python binary against the four fixture journals with a
``file`` sender and pins the bytes it writes to the output file.

The Python FileSender opens the output with ``open(..., "ab")`` and
never flushes or closes it; a write only reaches the file when a later
write would overflow the BufferedWriter. ``extra_field_values`` padding
makes each payload large enough that intermediate flushes happen during
the run, so the file ends up holding a deterministic PREFIX of the sent
messages (the tail stays buffered and is lost when the daemon sender
thread dies on SIGTERM — Python behavior, pinned by Task 17's D9
deviation note). The fixture pins the first 10 lines, which is stable
across ``io.DEFAULT_BUFFER_SIZE`` values.
"""

from systest import groundtruth_support as gt
from pathlib import Path

import pytest

_FIXTURE_NAME = "file_sender"
_PAD = "x" * 3072  # forces BufferedWriter flushes during the run
_EXPECTED_LINES = 10


def _config(state_path: Path, output_path: Path, journal_path: Path) -> dict:
    config = gt.base_reader_config(
        journal_path,
        {"out": {"output_type": "file", "file_output": str(output_path), "extra_field_values": {"PADDING": _PAD}}},
    )
    config["json_state_file_path"] = str(state_path)
    return config


def test_file_sender_wire_bytes(tmp_path: Path) -> None:
    fixture = gt.load_fixture(_FIXTURE_NAME)
    journal_path = gt.copy_journals(tmp_path / "logs")
    output_path = tmp_path / "output.jsonl"
    state_path = tmp_path / "state.json"

    with gt.groundtruth_pump(_config(state_path, output_path, journal_path), tmp_path) as process:
        gt.wait_until(
            lambda: output_path.exists() and sum(1 for _ in open(output_path, "rb")) >= _EXPECTED_LINES,
            description=f"{_EXPECTED_LINES} flushed output lines",
        )
        process.stop()

    lines = output_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) >= _EXPECTED_LINES  # the buffered tail may still be lost; the prefix is deterministic
    sent = gt.read_sender_state(state_path, "out")["sent"]
    if fixture is None or gt.update_mode():
        fixture = {
            "name": _FIXTURE_NAME,
            "capture": {
                "journals": "systest/data/rotated_logs/*.journal (Messages 0..39)",
                "sender": {"output_type": "file", "extra_field_values": {"PADDING": _PAD}},
            },
            "expected": {
                "lines": lines[:_EXPECTED_LINES],
                "sent_count": sent["count"],
                "sent_cursor": sent["cursor"],
            },
            "notes": [
                "file content is a PREFIX of the sent messages: Python never flushes the "
                "FileSender BufferedWriter, the daemon sender thread dies with the tail buffered",
                f"first {_EXPECTED_LINES} lines pinned; byte-exact payload JSON per line",
                "Go parity: same payload bytes per line, same sent.cursor/count state values",
            ],
        }
        gt.save_fixture(_FIXTURE_NAME, fixture)
        if not gt.update_mode():
            pytest.fail("file_sender fixture was missing and has been generated; commit it and rerun")

    assert lines[:_EXPECTED_LINES] == fixture["expected"]["lines"]
    assert sent["count"] == fixture["expected"]["sent_count"]
    assert sent["cursor"] == fixture["expected"]["sent_cursor"]
