#!/usr/bin/env python3
"""Regenerates serialization_golden.jsonl by running the real Python
pipeline, so the Go golden-file test asserts bytes that Python's
json.dumps actually produced (not hand-written expectations).

The pipeline mirrors journalpump.py exactly:
- PumpReader.get_next: __REALTIME_TIMESTAMP overwrite, __SEQNUM append
  from the cursor, PumpReader.convert_entry (converter table + UTF-8
  decode attempt) on raw byte values;
- JournalObjectHandler.process: key.lstrip("_") + repr() fallback for
  undecodable bytes (run unmodified via a stub reader/pump);
- JournalObjectHandler._get_or_generate_json: timestamp assignment,
  extra_field_values update, json.dumps(data,
  default=default_json_serialization) (run unmodified).

The fixture table here is the canonical fixture list; the Go side
(internal/journal/serialize_test.go) mirrors it by name. If a fixture
changes, update both and re-run this script from the repo root:

    python3 internal/journal/testdata/generate_golden.py
"""

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from journalpump import journalpump  # noqa: E402


class StubPump:
    stats = None

    def check_match(self, entry):
        return True


class StubBuffer:
    def __init__(self):
        self.items = []

    def add_item(self, item, cursor):
        self.items.append((item, cursor))


class StubSender:
    def __init__(self, extra_field_values):
        self.field_filter = None
        self.unit_log_levels = None
        self.extra_field_values = extra_field_values
        self.msg_buffer = StubBuffer()


class StubReader:
    searches = None
    secret_filters = None

    def __init__(self, senders):
        self.senders = senders

    def check_match(self, entry):
        return True

    def perform_searches(self, jobject):
        return True

    def inc_line_stats(self, journal_bytes=0, journal_lines=0):
        pass


reader = journalpump.PumpReader()


def run_pipeline(name, entry, cursor, realtime, extra_field_values):
    """Runs the real get_next -> process -> json.dumps path for one entry."""
    entry = dict(entry)
    entry["__REALTIME_TIMESTAMP"] = realtime
    seqnum = journalpump.PumpReader._parse_seqnum_from_cursor(cursor)
    if seqnum is not None:
        entry["__SEQNUM"] = seqnum
    entry = reader.convert_entry(entry)

    sender = StubSender(extra_field_values)
    jobject = journalpump.JournalObject(cursor=cursor, entry=entry)
    handler = journalpump.JournalObjectHandler(
        jobject=jobject, reader=StubReader(senders={"file": sender}), pump=StubPump()
    )
    handler.process()
    json_entry, _ = sender.msg_buffer.items[0]
    return {"name": name, "output": json_entry.decode("utf8")}


FIXTURES = [
    {
        "name": "typical",
        "entry": {
            "MESSAGE": b"hello world",
            "PRIORITY": b"6",
            "_PID": b"1234",
            "SYSTEMD_UNIT": b"sshd.service",
        },
        "cursor": "s=abc;i=1f4;b=def;m=123;t=456;x=789",
        "realtime": 1728475023123456,
        "extra_field_values": {"cluster": "test-cluster"},
    },
    {
        "name": "invalid-utf8",
        "entry": {
            "MESSAGE": b"\xff\xfe invalid \x00 bytes",
            "_HOSTNAME": b"host\xff",
        },
        "cursor": "s=abc;b=def;m=123;t=456;x=789",
        "realtime": 1728475023123999,
        "extra_field_values": {},
    },
    {
        "name": "converters",
        "entry": {
            "CODE_LINE": b"42",
            "MESSAGE_ID": b"550E8400-E29B-41D4-A716-446655440000",
            "_BOOT_ID": b"not-a-uuid",
            "__MONOTONIC_TIMESTAMP": b"12345",
            "PRIORITY": b"abc",
            "_GID": b" 1_0\n",
        },
        "cursor": "s=abc;i=0x1f4;b=def",
        "realtime": 1000000000000000,
        "extra_field_values": {},
    },
    {
        "name": "extras-overwrite",
        "entry": {"MESSAGE": b"original", "PRIORITY": b"6"},
        "cursor": "s=abc;i=1f4;b=def",
        "realtime": 1728475023123457,
        "extra_field_values": {
            "MESSAGE": "overwritten",
            "cluster": "c-1",
            "extra_float": 60.0,
            "extra_small": 1e-05,
            "extra_big": 1e20,
            "extra_bool": True,
            "extra_null": None,
            "extra_int": 42,
            "extra_nonascii": "héllo 世界",
        },
    },
    {
        "name": "escaping",
        "entry": {
            "MESSAGE": "héllo\tworld\n\"quoted\" \\backslash emoji \U0001F600 DEL\x7f ctl\x01"
        },
        "cursor": "s=abc;i=1f4;b=def",
        "realtime": 1728475023123456,
        "extra_field_values": {},
    },
    {
        "name": "list-value",
        "entry": {"MESSAGE": [b"first", b"second"]},
        "cursor": "s=abc;i=1f4;b=def",
        "realtime": 1728475023123456,
        "extra_field_values": {},
    },
    {
        "name": "timestamp-overwrite",
        "entry": {"timestamp": b"custom", "MESSAGE": b"x"},
        "cursor": "s=abc;i=1f4;b=def",
        "realtime": 1728475023123456,
        "extra_field_values": {"cluster": "c-1"},
    },
]


def main():
    lines = [json.dumps(run_pipeline(**fixture)) for fixture in FIXTURES]
    out_path = Path(__file__).with_name("serialization_golden.jsonl")
    out_path.write_text("\n".join(lines) + "\n", encoding="utf8")
    print(f"wrote {len(lines)} golden fixtures to {out_path}")


if __name__ == "__main__":
    main()