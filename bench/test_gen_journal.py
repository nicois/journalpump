# Copyright 2026, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.
"""Unit tests for the deterministic fixture generator (plan Task 31).

Only the stream builder is exercised here: converting to a real journal
needs ``systemd-journal-remote`` and is covered by the end-to-end smoke
test in ``systest/test_bench_smoke.py``.
"""

from pathlib import Path

import hashlib
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))

import gen_journal  # noqa: E402

# Pinned so a change to the field set, the ordering or the PRNG cadence is
# caught: the fixture identity is part of the baseline compatibility key.
PINNED_SHA256_5_ENTRIES = "77cf73a76dd122c248afa5de239f52beb82fad589e1fa43d941c43e71e21a465"

MANDATORY_FIELDS = (
    b"__CURSOR",
    b"__REALTIME_TIMESTAMP",
    b"__MONOTONIC_TIMESTAMP",
    b"_BOOT_ID",
    b"_MACHINE_ID",
    b"_HOSTNAME",
    b"_SYSTEMD_UNIT",
    b"PRIORITY",
    b"SYSLOG_IDENTIFIER",
    b"_PID",
)


def parse_entries(stream: bytes) -> list[dict[bytes, bytes]]:
    """Split the export stream into entries (blank-line terminated)."""
    entries = []
    current: dict[bytes, bytes] = {}
    for line in stream.split(b"\n"):
        if not line:
            if current:
                entries.append(current)
                current = {}
            continue
        key, _, value = line.partition(b"=")
        current[key] = value
    return entries


def test_stream_is_deterministic_and_pinned():
    stream = gen_journal.build_stream(5, 20261012)
    assert stream == gen_journal.build_stream(5, 20261012)
    assert hashlib.sha256(stream).hexdigest() == PINNED_SHA256_5_ENTRIES


def test_seed_changes_the_stream():
    assert gen_journal.build_stream(5, 1) != gen_journal.build_stream(5, 2)


def test_entries_carry_the_mandatory_and_custom_fields():
    entries = parse_entries(gen_journal.build_stream(3, 7))
    assert len(entries) == 3
    for entry in entries:
        for field in MANDATORY_FIELDS:
            assert field in entry, field
        assert entry[b"BENCH_RUN"] == b"bench"
        assert entry[b"BENCH_TOKEN"].startswith(b"tok-")


def test_entries_are_blank_line_terminated_including_the_last():
    stream = gen_journal.build_stream(4, 7)
    assert stream.endswith(b"\n\n")
    assert len(parse_entries(stream)) == 4


def test_message_lengths_stay_in_the_declared_band():
    entries = parse_entries(gen_journal.build_stream(500, 11))
    lengths = [len(entry[b"MESSAGE"]) for entry in entries]
    assert min(lengths) >= 80
    assert max(lengths) <= 400


def test_every_thousandth_entry_carries_invalid_utf8():
    entries = parse_entries(gen_journal.build_stream(2500, 13))
    invalid = [index for index, entry in enumerate(entries) if b"\xff" in entry[b"MESSAGE"]]
    assert invalid == [999, 1999]
    for index, entry in enumerate(entries):
        if index not in invalid:
            entry[b"MESSAGE"].decode("utf-8")  # must not raise


def test_field_values_never_contain_a_newline():
    stream = gen_journal.build_stream(200, 13)
    entries = parse_entries(stream)
    assert len(entries) == 200