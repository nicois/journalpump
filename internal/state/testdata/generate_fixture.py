"""Generate the Python-written state-file fixture for the Go tests.

This exercises the REAL journalpump save path — JournalReader.get_state,
Sender.get_state and JournalPump.save_state (atomic_replace_file +
json.dump(indent=4, sort_keys=True)) — with objects whose fields are set
directly (the constructors need a live journald/Kafka environment).

Run from the repo root:  python3 -I internal/state/testdata/generate_fixture.py
Writes: internal/state/testdata/state_python_written.json
"""

import logging
import os
import sys
import types
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, os.fspath(REPO_ROOT))

from journalpump.journalpump import JournalPump, JournalReader  # noqa: E402
import journalpump.senders.base as senders_base  # noqa: E402
from journalpump.senders.base import LogSender, MsgBuffer  # noqa: E402

# Pin time.monotonic() so health.elapsed is deterministic (byte-exact
# fixtures): the script's monotonic clock always reads 1000.0.
senders_base.time = types.SimpleNamespace(monotonic=lambda: 1000.0)


def make_sender(name, cursor, count, byte_count, elapsed, errors, running=True):
    # A real LogSender (the sender base class); the reader stub only
    # needs .name for the constructor's log name.
    sender = LogSender(
        name=name,
        reader=type("ReaderStub", (), {"name": name})(),
        config={},
        field_filter=None,
        stats=None,
        max_send_interval=30.0,
    )
    sender.msg_buffer.cursor = cursor
    sender.msg_buffer.total_size = 2048
    sender.msg_buffer.entry_num = 17
    sender.msg_buffer.messages = [object()] * 3  # current_queue = 3
    sender._sent_cursor = cursor
    sender._sent_count = count
    sender._sent_bytes = byte_count
    sender.running = running
    sender._connected = True
    sender._connected_changed = senders_base.time.monotonic() - elapsed
    sender._errors = errors
    return sender


def make_reader(name, cursor, searches, senders, total_lines, total_bytes):
    reader = JournalReader(
        name=name,
        config={},
        field_filters={},
        geoip=None,
        stats=None,
        searches=searches,
    )
    reader.cursor = cursor
    reader.read_lines = total_lines
    reader.read_bytes = total_bytes
    reader.senders = senders
    return reader


def main():
    out = Path(__file__).resolve().parent / "state_python_written.json"

    primary = make_sender(
        name="kafka_primary",
        cursor="cursor=aabbccdd-1",
        count=1234,
        byte_count=5_000_000,
        elapsed=12.5,
        errors=["Connection refused", "Timed out"],
    )
    backup = make_sender(
        name="kafka_backup",
        cursor=None,
        count=5,
        byte_count=4096,
        elapsed=0.25,
        errors=[],
        running=False,
    )
    reader = make_reader(
        name="reader_one",
        cursor="cursor=11223344-0",
        searches=[{"name": "errors"}, {"name": "payment_lines", "hits": 7}],
        senders={"kafka_primary": primary, "kafka_backup": backup},
        total_lines=1_000_000,
        total_bytes=999_999_999,
    )

    pump = object.__new__(JournalPump)
    pump.log = logging.getLogger("fixture")
    pump.config = {"json_state_file_path": str(out)}
    pump.readers = {"reader_one": reader}
    pump.start_time_str = "2026-10-09T12:00:00.000000"
    pump.previous_state = None
    pump.save_state()

    print(f"wrote {out}")
    print(out.read_text(encoding="utf-8"))


if __name__ == "__main__":
    main()