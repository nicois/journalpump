# Copyright 2026, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.
"""Deterministic journal fixture generator for the performance benchmark
(plan Task 31, Phase 4b).

The generator emits a deterministic journal-export stream (seeded PRNG;
``MESSAGE`` 80-400 B, ``_SYSTEMD_UNIT``, ``PRIORITY``, ``_PID``,
``SYSLOG_IDENTIFIER``, a handful of custom fields, and every 1000th entry
carrying raw invalid UTF-8 to exercise the D8 ``repr()`` path) and converts
it to a native ``.journal`` file with ``systemd-journal-remote -o``.

The stream is what defines determinism: its SHA-256 is recorded in every
benchmark result and is part of the baseline compatibility key, so a fixture
change invalidates a baseline comparison. The journal itself is NOT committed
(it is large); the generator and the seed are.

``systemd-journal-remote`` is not always installed on the bench host (it is
not installed on the reference machine). When it is absent the generator
falls back to running the SAME tool inside a throwaway podman container with
the stream and output directory bind-mounted. This is not a shim: the real
tool produces the real journal in both cases. The container path is only a
tool-location fallback, selected by ``BENCH_JOURNAL_REMOTE`` (an explicit
path/command) or auto-detected.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

# The invalid-UTF-8 cadence (every Nth entry carries a raw 0xFF byte).
INVALID_UTF8_EVERY = 1000

# Custom fields carried on every entry, to exercise field copying.
_CUSTOM_FIELDS = ("BENCH_RUN", "BENCH_SEQ", "BENCH_TOKEN")

# A fixed 32-hex boot/machine id keeps the fixture byte-stable.
_BOOT_ID = "0123456789abcdef0123456789abcdef"
_MACHINE_ID = "fedcba9876543210fedcba9876543210"
_HOSTNAME = "journalpump-bench"

_DEFAULT_UNITS = ("bench-a.service", "bench-b.service", "bench-c.service")

_REMOTE_FALLBACK = "/usr/lib/systemd/systemd-journal-remote"
_CONTAINER_IMAGE = "registry.fedoraproject.org/fedora:latest"


def _message(rng: random.Random, length: int) -> bytes:
    """A deterministic ASCII-ish message of exactly ``length`` bytes."""
    alphabet = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 .,:;-_"
    return bytes(rng.choice(alphabet) for _ in range(length))


def build_stream(entries: int, seed: int) -> bytes:
    """Return the deterministic journal-export stream for ``entries``.

    Pure function of ``(entries, seed)`` — no clock, no PID, no hostname is
    read — so two generations produce the identical bytes (and SHA-256).
    """
    rng = random.Random(seed)
    out = bytearray()
    for i in range(entries):
        unit = _DEFAULT_UNITS[i % len(_DEFAULT_UNITS)]
        length = rng.randint(80, 400)
        message = _message(rng, length)
        if i % INVALID_UTF8_EVERY == INVALID_UTF8_EVERY - 1:
            # Raw invalid UTF-8 byte inside MESSAGE: the reader's D8 path
            # must repr()-fallback on it. Placed after position 0 so the
            # field is unambiguously non-UTF-8.
            message = message[:1] + b"\xff" + message[2:]
        fields: list[tuple[str, bytes]] = [
            ("__CURSOR", f"s=bench;i={i};b={_BOOT_ID}".encode()),
            ("__REALTIME_TIMESTAMP", str(1_600_000_000_000_000 + i * 1000).encode()),
            ("__MONOTONIC_TIMESTAMP", b"0"),
            ("_BOOT_ID", _BOOT_ID.encode()),
            ("_MACHINE_ID", _MACHINE_ID.encode()),
            ("_HOSTNAME", _HOSTNAME.encode()),
            ("_SYSTEMD_UNIT", unit.encode()),
            ("PRIORITY", str(rng.choice((3, 4, 6, 7))).encode()),
            ("SYSLOG_IDENTIFIER", unit.encode()),
            ("_PID", str(1000 + (i % 5000)).encode()),
            ("BENCH_RUN", b"bench"),
            ("BENCH_SEQ", str(i).encode()),
            ("BENCH_TOKEN", ("tok-%08d" % i).encode()),
            ("MESSAGE", message),
        ]
        for key, value in fields:
            assert b"\n" not in value
            out += key.encode() + b"=" + value + b"\n"
        out += b"\n"  # entries are blank-line terminated, incl. the last
    return bytes(out)


def resolve_journal_remote(explicit: str | None) -> tuple[str, str]:
    """Return ``(kind, command)`` for the journal-remote tool.

    ``kind`` is ``"host"`` for a local binary or ``"podman"`` for the
    container fallback.
    """
    if explicit:
        return "host", explicit
    found = shutil.which("systemd-journal-remote")
    if found:
        return "host", found
    if Path(_REMOTE_FALLBACK).exists():
        return "host", _REMOTE_FALLBACK
    if shutil.which("podman"):
        return "podman", _REMOTE_FALLBACK
    raise SystemExit(
        "systemd-journal-remote is not installed and podman is unavailable; "
        "install systemd-journal-remote or set BENCH_JOURNAL_REMOTE"
    )


def generate_journal(stream_path: Path, out_dir: Path) -> Path:
    """Convert an export stream to ``out_dir/system.journal``."""
    out_dir.mkdir(parents=True, exist_ok=True)
    journal_path = out_dir / "system.journal"
    kind, command = resolve_journal_remote(os.environ.get("BENCH_JOURNAL_REMOTE"))
    if kind == "host":
        argv = [command, "-o", str(journal_path), str(stream_path)]
        run_env = None
    else:
        # Bind-mount the parent directory so both the stream and the output
        # journal are visible; the tool path inside is the same.
        parent = out_dir.parent if out_dir.parent != out_dir else out_dir
        rel_stream = stream_path.relative_to(parent)
        rel_out = journal_path.relative_to(parent)
        argv = [
            "podman", "run", "--rm",
            "-v", f"{parent}:/bench:Z",
            _CONTAINER_IMAGE, "bash", "-c",
            "dnf -y -q install systemd-journal-remote >/dev/null 2>&1 && "
            f"{command} -o /bench/{rel_out} /bench/{rel_stream}",
        ]
        run_env = None
    result = subprocess.run(argv, env=run_env, capture_output=True, text=True)
    if result.returncode != 0 or not journal_path.exists():
        sys.stderr.write(result.stdout)
        sys.stderr.write(result.stderr)
        raise SystemExit(f"systemd-journal-remote failed (rc={result.returncode})")
    return journal_path


def generate_fixture(out_dir: Path, *, entries: int, seed: int, stream_path: Path | None = None) -> dict[str, Any]:
    """Generate the journal fixture and return its manifest.

    The manifest is what the benchmark result records and what the
    baseline compatibility key compares on, so the fixture identity is
    transitive: generator + seed + entries -> stream SHA-256.
    """
    stream = build_stream(entries, seed)
    digest = hashlib.sha256(stream).hexdigest()

    out_dir.mkdir(parents=True, exist_ok=True)
    # systemd-journal-remote ADDS to existing journal files in the output
    # directory, so a stale fixture would silently double the entry count.
    for stale in out_dir.glob("*.journal"):
        stale.unlink()
    keep = stream_path is not None
    stream_path = stream_path or (out_dir / "export.stream")
    stream_path.write_bytes(stream)

    journal_path = generate_journal(stream_path, out_dir)
    if not keep:
        stream_path.unlink()  # keep only the journal unless asked

    return {
        "entries": entries,
        "seed": seed,
        "stream_sha256": digest,
        "journal": journal_path.name,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--entries", type=int, default=int(os.environ.get("BENCH_ENTRIES", "200000")))
    parser.add_argument("--seed", type=int, default=int(os.environ.get("BENCH_SEED", "20261012")))
    parser.add_argument("--out", type=Path, required=True, help="directory to write system.journal into")
    parser.add_argument("--stream", type=Path, default=None, help="keep the export stream at this path")
    parser.add_argument("--manifest", type=Path, default=None, help="write the fixture manifest JSON here")
    args = parser.parse_args(argv)

    manifest = generate_fixture(args.out, entries=args.entries, seed=args.seed, stream_path=args.stream)
    if args.manifest is not None:
        args.manifest.write_text(json.dumps(manifest, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(manifest, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())