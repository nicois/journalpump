# Copyright 2026, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.
"""Ground-truth capture, missing-cursor replay (plan Task 11).

Two-phase run of the real Python binary with the file sender:

1. The first run consumes all 40 fixture-journal messages and saves the
   state file with a full ``sent.cursor``.
2. The state file is then edited to null the sender's ``sent.cursor``
   (simulating the missing-cursor condition). The second run resumes
   and Python replays EVERYTHING from the head of the journal — the
   quirk-lock ground truth for "any missing sender cursor => full
   resync" (see Task 8's ``ResumeCursor`` semantics).

Assertions: every line the second run appends equals the line at the
same position of the first run (byte-identical replay), and the final
state carries the same cursor with the second run's own send count
(sent counters are per-process: ``LogSender.__init__`` resets
``_sent_count``; only ``sent.cursor`` is restored for resume).
"""

from systest import groundtruth_support as gt
from pathlib import Path

import json
import pytest

_FIXTURE_NAME = "replay"
_PAD = "x" * 3072  # same padding as the file-sender fixture: forces flushes


def _config(state_path: Path, output_path: Path, journal_path: Path) -> dict:
    config = gt.base_reader_config(
        journal_path,
        {"out": {"output_type": "file", "file_output": str(output_path), "extra_field_values": {"PADDING": _PAD}}},
    )
    config["json_state_file_path"] = str(state_path)
    return config


def test_missing_cursor_replay_replays_from_head(tmp_path: Path) -> None:
    fixture = gt.load_fixture(_FIXTURE_NAME)
    journal_path = gt.copy_journals(tmp_path / "logs")
    output_path = tmp_path / "output.jsonl"
    state_path = tmp_path / "state.json"

    # Phase 1: consume everything once.
    with gt.groundtruth_pump(_config(state_path, output_path, journal_path), tmp_path) as process:
        gt.wait_until(
            lambda: output_path.exists() and sum(1 for _ in open(output_path, "rb")) >= 10,
            description="first-run flushed output lines",
        )
        process.stop()
    phase1_lines = output_path.read_text(encoding="utf-8").splitlines()
    phase1_sent = gt.read_sender_state(state_path, "out")["sent"]
    assert phase1_sent["count"] == 40

    # Phase 2: null the sender cursor and rerun against the SAME output
    # file and state file: Python must replay the whole journal.
    state = json.loads(state_path.read_text(encoding="utf-8"))
    sender_state = state["readers"][gt.READER_NAME]["senders"]["out"]
    sender_state["sent"]["cursor"] = None
    state_path.write_text(json.dumps(state), encoding="utf-8")

    with gt.groundtruth_pump(_config(state_path, output_path, journal_path), tmp_path) as process:
        gt.wait_until(
            lambda: sum(1 for _ in open(output_path, "rb")) >= len(phase1_lines) + 10,
            description="second-run replay lines",
        )
        process.stop()

    all_lines = output_path.read_text(encoding="utf-8").splitlines()
    phase2_lines = all_lines[len(phase1_lines) :]
    final_sent = gt.read_sender_state(state_path, "out")["sent"]

    # Byte-identical replay: each phase-2 line equals the phase-1 line
    # at the same message position.
    assert len(phase2_lines) >= 10
    assert phase2_lines == phase1_lines[: len(phase2_lines)]
    # Every phase-1 line is still the fixture's expected line (already
    # pinned byte-exactly by the file-sender fixture).
    file_fixture = gt.load_fixture("file_sender")
    assert file_fixture is not None
    expected_lines = file_fixture["expected"]["lines"]
    # The file sender only flushes a prefix mid-run, so phase1_lines is
    # that prefix; compare it against the fixture's matching prefix. The
    # file-sender fixture was expanded (10 -> 40 lines) in Task 16 while
    # this Task 11 assertion kept a literal [:10], so it went red without
    # any consumer suite catching it (see the gate-integrity note in
    # docs/go-test-parity.md).
    assert len(phase1_lines) >= 10
    assert expected_lines[: len(phase1_lines)] == phase1_lines

    if fixture is None or gt.update_mode():
        fixture = {
            "name": _FIXTURE_NAME,
            "capture": {
                "journals": "systest/data/rotated_logs/*.journal (Messages 0..39)",
                "sender": {"output_type": "file", "extra_field_values": {"PADDING": _PAD}},
                "phase_2_state_edit": "senders.out.sent.cursor set to null before the second run",
            },
            "expected": {
                "final_sent_count": final_sent["count"],
                "final_sent_cursor": final_sent["cursor"],
                # Derived from this capture, not hardcoded: the replay
                # re-sent exactly as many lines as the first run sent.
                "replayed_line_count_matches_first_run": len(phase2_lines) == len(phase1_lines),
            },
            "notes": [
                "Python replays the ENTIRE journal from the head when any sender cursor is "
                "missing: the second run re-sends all 40 messages and every replayed line is "
                "byte-identical to the first run's line",
                "final sent.count is the SECOND RUN's own count (40), not cumulative: "
                "LogSender.__init__ resets _sent_count; state stores sent.cursor for resume "
                "only",
                "file line counts are buffer-size dependent (see the file-sender fixture); "
                "the byte-level equality is the fixture",
            ],
        }
        gt.save_fixture(_FIXTURE_NAME, fixture)
        if not gt.update_mode():
            pytest.fail("replay fixture was missing and has been generated; commit it and rerun")

    assert final_sent["count"] == fixture["expected"]["final_sent_count"]
    assert final_sent["cursor"] == fixture["expected"]["final_sent_cursor"]
    assert phase2_lines == phase1_lines[: len(phase2_lines)]
    assert (len(phase2_lines) == len(phase1_lines)) == fixture["expected"]["replayed_line_count_matches_first_run"]
