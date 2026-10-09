# Copyright 2026, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.
"""Ground-truth capture, file sender (plan Task 11).

Runs the real Python binary against the four fixture journals with a
``file`` sender and pins the bytes it writes to the output file.

The Python FileSender opens the output with ``open(..., "ab")`` and
never flushes or closes it; on SIGTERM the buffered tail is lost. The
capture composes an unbuffered-write shim for the output path so the
committed fixture holds the FULL 40-message output — the same bytes the
Go sender produces, since Go flushes its BufferedWriter within the D9
shutdown grace window. The steady-state buffering itself stays
Python-faithful (Go buffers 131072 bytes, exactly like Python's
BufferedWriter); only the shutdown flush differs (the D9 deviation).
"""

from systest import groundtruth_support as gt
from pathlib import Path

import pytest

_FIXTURE_NAME = "file_sender"
_PAD = "x" * 3072  # keeps every payload a realistic multi-KB line
_EXPECTED_LINES = 40  # Messages 0..39


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

    unbuffered_shim = gt.SHIM_UNBUFFERED_FILE.format(target=str(output_path))
    with gt.groundtruth_pump(
        _config(state_path, output_path, journal_path), tmp_path, extra_shim=unbuffered_shim
    ) as process:
        gt.wait_until(
            lambda: output_path.exists() and sum(1 for _ in open(output_path, "rb")) >= _EXPECTED_LINES,
            description=f"{_EXPECTED_LINES} output lines",
        )
        process.stop()

    lines = output_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == _EXPECTED_LINES  # unbuffered shim: the full output is on disk
    sent = gt.read_sender_state(state_path, "out")["sent"]
    if fixture is None or gt.update_mode():
        fixture = {
            "name": _FIXTURE_NAME,
            "capture": {
                "journals": "systest/data/rotated_logs/*.journal (Messages 0..39)",
                "sender": {"output_type": "file", "extra_field_values": {"PADDING": _PAD}},
                "shim": "unbuffered open() for the output path (capture only; steady-state buffering is Python-faithful)",
            },
            "expected": {
                "lines": lines[:_EXPECTED_LINES],
                "sent_count": sent["count"],
                "sent_cursor": sent["cursor"],
            },
            "notes": [
                "full 40-message output pinned; byte-exact payload JSON per line",
                "Go parity: identical payload bytes per line, same sent.cursor/count state values",
                "D9 deviation: Python's BufferedWriter tail is lost on SIGTERM; Go flushes it within the "
                "shutdown grace window, so Go reaches all 40 lines without the capture shim",
            ],
        }
        gt.save_fixture(_FIXTURE_NAME, fixture)
        if not gt.update_mode():
            pytest.fail("file_sender fixture was missing and has been generated; commit it and rerun")

    assert lines[:_EXPECTED_LINES] == fixture["expected"]["lines"]
    assert sent["count"] == fixture["expected"]["sent_count"]
    assert sent["cursor"] == fixture["expected"]["sent_cursor"]