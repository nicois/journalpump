# Copyright 2026, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.
"""Python half of the transform micro-benchmark (docs/perf-root-cause.md).

Measures the same per-entry pipeline the Go benchmark
``internal/journal/transform_bench_test.go`` measures, with no reader
pacing and no journal I/O: converters over the raw entry, ``process()``'s
key rewriting (``lstrip("_")``), the handler's timestamp assignment, and
``json.dumps(..., ensure_ascii=True)``. That separates "the transform is
slow" from "the reader/FFI dominates".

The field set mirrors ``bench/gen_journal.py`` (16 fields, 240-byte ASCII
MESSAGE), the same fixture the Go side uses.

Run: ``python3 -I bench/transform_microbench.py [iterations]``
"""

from __future__ import annotations

import json
import random
import string
import sys
import time
import uuid

CONVERTERS = {
    "PRIORITY": int,
    "_PID": int,
    "_BOOT_ID": lambda s: str(uuid.UUID(s)),
    "_MACHINE_ID": lambda s: str(uuid.UUID(s)),
    "__REALTIME_TIMESTAMP": lambda t: int(t) / 1000000.0,
    "__MONOTONIC_TIMESTAMP": lambda _t: None,
}

_ALPHABET = string.ascii_letters + string.digits + " .,:;-_"


def _message(rng: random.Random, length: int) -> str:
    return "".join(rng.choice(_ALPHABET) for _ in range(length))


def raw_entry() -> dict[str, str]:
    rng = random.Random(20261012)
    return {
        "__CURSOR": "s=91ae92bb6cbd414bad3894fb8df2e74a;i=493e0;b=0123456789abcdef0123456789abcdef;m=0;t=5af3119859f18;x=7631647777d4934f",
        "__SEQNUM": "299999",
        "__SEQNUM_ID": "91ae92bb6cbd414bad3894fb8df2e74a",
        "_SYSTEMD_UNIT": "bench-c.service",
        "BENCH_TOKEN": "tok-00299999",
        "_PID": "5999",
        "SYSLOG_IDENTIFIER": "bench-c.service",
        "MESSAGE": _message(rng, 240),
        "PRIORITY": "7",
        "__MONOTONIC_TIMESTAMP": "0",
        "_MACHINE_ID": "fedcba9876543210fedcba9876543210",
        "_HOSTNAME": "journalpump-bench",
        "_BOOT_ID": "0123456789abcdef0123456789abcdef",
        "BENCH_SEQ": "299999",
        "__REALTIME_TIMESTAMP": "1600000299999000",
        "BENCH_RUN": "bench",
    }


def convert_entry(entry: dict[str, str]) -> dict[str, object]:
    """journalpump.PumpReader.convert_entry."""
    output: dict[str, object] = {}
    for key, value in entry.items():
        convert = CONVERTERS.get(key)
        if convert is not None:
            try:
                value = convert(value)
            except ValueError:
                pass
        output[key] = value
    return output


def process(entry: dict[str, object]) -> dict[str, object]:
    """JournalObjectHandler.process's key rewriting."""
    return {key.lstrip("_"): value for key, value in entry.items()}


def transform(raw: dict[str, str]) -> bytes:
    entry = process(convert_entry(raw))
    entry["timestamp"] = 1600000299.999  # the handler's timestamp field
    return json.dumps(entry, ensure_ascii=True).encode("utf8")


def main(argv: list[str]) -> int:
    iterations = int(argv[1]) if len(argv) > 1 else 20000
    raw = raw_entry()
    transform(raw)  # warm up
    start = time.perf_counter()
    total_bytes = 0
    for _ in range(iterations):
        total_bytes += len(transform(raw))
    elapsed = time.perf_counter() - start
    print(f"python transform: {elapsed / iterations * 1e9:.0f} ns/entry over {iterations} iterations " f"({total_bytes / iterations:.0f} B/entry)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))