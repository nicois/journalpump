"""Permanent interop test: Python reads the Go-written state file.

The Go port (internal/state, migration Task 8) writes the state file
with Go 1.27's encoding/json/v2 encoder — a sanctioned deviation
(user decision 2026-10-09, charter §2): the parsed document stays
equal to Python's json.dump(indent=4, sort_keys=True), but the key
order is Go-native and NOT guaranteed to be sorted (or even stable
across writes). This test locks BOTH directions' contract from the
Python side, order-independently:

1. the Go-written committed fixture parses with plain json.load, and
2. the resume-cursor selection that handle_new_config performs on it
   (journalpump.py:889-905: last sender in FILE key order wins, any
   null sent.cursor means a full resync) matches what the Go
   ResumeCursor helper computes — asserted against the fixture's
   committed expectations file. The walk reads the file's key order
   whatever it is; no assertion may depend on a Go-written ordering.

Run: pytest test/test_state_file_interop.py
"""

import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
GO_STATE = REPO_ROOT / "internal/state/testdata/state_go_written.json"
GO_EXPECTATIONS = REPO_ROOT / "internal/state/testdata/state_go_expectations.json"


def resume_cursor(reader_state):
    """The resume selection of journalpump.py:889-905, verbatim."""
    resume_cursor = reader_state.get("cursor")
    for sender_name, sender in reader_state.get("senders", {}).items():
        sender_cursor = sender["sent"]["cursor"]
        if sender_cursor is None:
            # Full sync from the beginning; the walk aborts.
            return None
        resume_cursor = sender_cursor
    return resume_cursor


def test_go_written_state_file_resume_selection():
    state = json.loads(GO_STATE.read_text(encoding="utf-8"))
    expectations = json.loads(GO_EXPECTATIONS.read_text(encoding="utf-8"))

    assert set(state.keys()) == {"readers", "start_time"}
    assert state["start_time"] == "2026-10-09T13:00:00.000000"

    for reader_name, reader_state in state["readers"].items():
        # Key order is Go-native and nondeterministic — nothing here may
        # assert an ordering; the resume walk reads the file's order.
        assert resume_cursor(reader_state) == expectations["resume_cursors"][reader_name]

    # reader_two's senders (file order is Go-native and nondeterministic,
    # so the walk's outcome must not depend on it): every sender order
    # ends at kafka_primary's null cursor — the walk hits the null and
    # requires a full resync.
    assert expectations["resume_cursors"]["reader_two"] is None


def test_go_written_state_file_floats_round_trip():
    """Python parses the Go file's floats and raw-UTF-8 strings."""
    state = json.loads(GO_STATE.read_text(encoding="utf-8"))
    elapsed = state["readers"]["reader_two"]["senders"]["kafka_backup"]["health"]["elapsed"]
    assert elapsed == 3.5
    # Go-native strings: non-ASCII is raw UTF-8 (not ensure_ascii), and
    # json.load decodes it to the same Python string.
    errors = state["readers"]["reader_two"]["senders"]["kafka_backup"]["health"]["errors"]
    assert errors == ["connection reset été", "first error stays"]


def test_python_written_state_file_is_present():
    """The Python→Go direction's fixture exists and parses."""
    state = json.loads((REPO_ROOT / "internal/state/testdata/state_python_written.json").read_text(encoding="utf-8"))
    assert set(state.keys()) == {"readers", "start_time"}
    assert resume_cursor(state["readers"]["reader_one"]) is None


def test_go_rewrite_of_python_fixture_round_trips(tmp_path):
    """Python json.load parses Go's rewrite of the Python-written fixture
    to the equivalent structure (parsed equality, order-independent —
    the Go writer's key order is Go-native and nondeterministic)."""
    import os
    import subprocess

    rewrite = tmp_path / "state_go_rewrite.json"
    env = {**os.environ, "STATE_REWRITE_OUT": str(rewrite)}
    subprocess.run(
        ["go", "test", "./internal/state/", "-run", "TestWriteRoundTripParsedEqual", "-count=1"],
        cwd=REPO_ROOT,
        env=env,
        check=True,
        capture_output=True,
    )
    rewritten = json.loads(rewrite.read_text(encoding="utf-8"))
    original = json.loads((REPO_ROOT / "internal/state/testdata/state_python_written.json").read_text(encoding="utf-8"))
    assert rewritten == original
