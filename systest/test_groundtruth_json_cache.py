# Copyright 2026, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.
"""Ground-truth capture, per-entry JSON cache quirk (plan Task 11).

Exercises the quirk documented from ``JournalObjectHandler.json_objects``
(journalpump.py:752-790) with two CONCURRENT senders: one reader, two
filterless ``file`` senders whose payloads differ only through the
second sender's ``extra_field_values`` marker.

The handler caches the serialized entry per ``field_filter`` name — two
senders WITHOUT a field filter share the FIRST sender's cached payload,
so the second sender's ``extra_field_values`` are silently ignored. The
capture locks that: both outputs are byte-identical, and neither
carries the second sender's marker (without the cache, the second
sender's lines would carry ``"MARKER": "second"``).
"""

from systest import groundtruth_support as gt
from pathlib import Path

_PAD = "x" * 3072  # same padding as the file-sender fixture: forces flushes


def _config(state_path: Path, out1: Path, out2: Path, journal_path: Path) -> dict:
    config = gt.base_reader_config(
        journal_path,
        {
            "first": {"output_type": "file", "file_output": str(out1), "extra_field_values": {"PADDING": _PAD}},
            "second": {
                "output_type": "file",
                "file_output": str(out2),
                "extra_field_values": {"PADDING": _PAD, "MARKER": "second"},
            },
        },
    )
    config["json_state_file_path"] = str(state_path)
    return config


def test_per_entry_json_cache_two_filterless_senders(tmp_path: Path) -> None:
    journal_path = gt.copy_journals(tmp_path / "logs")
    out1 = tmp_path / "first.jsonl"
    out2 = tmp_path / "second.jsonl"
    state_path = tmp_path / "state.json"

    with gt.groundtruth_pump(_config(state_path, out1, out2, journal_path), tmp_path) as process:
        gt.wait_until(
            lambda: out1.exists() and out2.exists() and sum(1 for _ in open(out2, "rb")) >= 10,
            description="10 flushed lines in both sender outputs",
        )
        process.stop()

    first = out1.read_text(encoding="utf-8")
    second = out2.read_text(encoding="utf-8")

    # The quirk: both senders share the FIRST sender's cached payload.
    # The outputs are byte-identical, including the buffered tails (the
    # senders write identical bytes in lockstep, so their flush patterns
    # coincide).
    assert first == second
    assert first.splitlines(), "both outputs must have flushed lines"
    # The second sender's extra_field_values were silently ignored: its
    # marker never reaches the wire.
    assert '"MARKER": "second"' not in second
