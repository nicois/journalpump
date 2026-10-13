# Copyright 2026, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.
"""Ground-truth wire fixture support (plan Task 11).

Every capture test here runs the REAL Python journalpump binary through
the Task 10 harness (``JournalpumpProcess``) against a recording
endpoint — a capture HTTP server, a capture websocket server, or an
output file — and pins the exact bytes the senders put on the wire.
The captured manifests are committed under ``systest-go/fixtures/`` in
implementation-neutral JSON so the Go parity tests (Task 18 and the
sender tasks) can load and assert against the same files.

Regenerate with: ``GROUNDTRUTH_UPDATE=1 make systest`` (or run the
``test_groundtruth_*`` modules directly).

Determinism strategy, shared by all fixtures:

- The fixture journals (``systest/data/rotated_logs/``) carry fixed
  2021 timestamps, so the payload JSON built from them is byte
  deterministic.
- Sender batch / POST boundaries are timing-dependent. Comparisons are
  therefore ordered STREAMS across everything captured (all request
  bodies concatenated, all websocket frames decompressed and
  concatenated), which are identical no matter how the messages were
  split into batches.
- Wall-clock fields are excluded or normalized: logplex frame
  timestamps are replaced by a fixed-width placeholder; state-file
  ``health.elapsed`` and ``start_time`` are dropped; only value-level
  state fields (cursor, counts, bytes, status) are pinned.
- The Python file sender never flushes its ``BufferedWriter``: writes
  only reach the file when the next write would overflow
  ``io.DEFAULT_BUFFER_SIZE``, and the daemon sender thread dies with
  the tail still buffered on SIGTERM. The file therefore holds a
  deterministic PREFIX of the sent messages; the fixture pins the
  first N lines, which is stable across buffer sizes (pinned with
  ``extra_field_values`` padding large enough to force intermediate
  flushes on any CPython).
- The fixture journals report ``has_persistent_files() == False`` for
  path/files-mode readers (python-systemd storage-origin probe), which
  would make the real binary retry forever. A ``sitecustomize`` shim
  (``build_shim``) patches only that check; the whole senders path is
  the real code.
"""

from systest.journalpump_process import JournalpumpProcess
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable, Iterator

import json
import os
import re
import threading
import time

REPO_ROOT = Path(__file__).resolve().parent.parent
FIXTURE_DIR = REPO_ROOT / "systest-go" / "fixtures"
JOURNAL_DIR = REPO_ROOT / "systest" / "data" / "rotated_logs"
READER_NAME = "my-reader"

SHIM_SITECUSTOMIZE = '''"""Test-environment shim: fixture journal files are not on persistent
storage, so python-systemd's storage-origin check reports False; the
real binary would then never start the reader (it retries forever).
The rotation systest mocks these same methods in-process; here the
shim reaches the spawned binary through PYTHONPATH/sitecustomize.

Also mirrors the deployment dependency set for the Kafka sender: this
dev box has a system-wide ``zstandard``, which the real deployment
(requirements.txt: python-snappy, no zstandard) does not ship — without
the block the sender would silently pick zstd instead of snappy
(kafka_sender.py prefers zstd whenever importable)."""
import systemd.journal as _journal

_journal.Reader.has_persistent_files = lambda self: True
_journal.Reader.has_runtime_files = lambda self: True

import sys as _sys

_sys.modules["zstandard"] = None
'''

# Capture-side shim for the file sender: Python's FileSender opens the
# output with open(path, "ab") — a BufferedWriter that is never flushed,
# so on SIGTERM the tail is lost and the output file holds only a
# deterministic PREFIX of the sent messages. To pin the FULL output (the
# bytes the Go sender produces, since it flushes within the D9 grace
# window) the capture forces unbuffered writes for that one path.
SHIM_UNBUFFERED_FILE = '''
import builtins as _builtins

_open = _builtins.open
_TARGET = {target!r}


def _open_unbuffered(file, mode="r", buffering=-1, *args, **kwargs):
    if str(file) == _TARGET and buffering == -1 and "a" in mode:
        buffering = 0
    return _open(file, mode, buffering, *args, **kwargs)


_builtins.open = _open_unbuffered
'''


def update_mode() -> bool:
    """True when GROUNDTRUTH_UPDATE=1: regenerate the committed fixtures."""
    return os.environ.get("GROUNDTRUTH_UPDATE") == "1"


def fixture_path(name: str) -> Path:
    return FIXTURE_DIR / f"{name}.json"


def load_fixture(name: str) -> dict[str, Any] | None:
    path = fixture_path(name)
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def save_fixture(name: str, data: dict[str, Any]) -> None:
    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)
    fixture_path(name).write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _reject_constant(token: str) -> Any:
    """parse_constant hook: reject the non-standard NaN/Infinity tokens."""
    raise ValueError(f"non-standard JSON constant {token!r}")


def _json_values_equal(a: Any, b: Any) -> bool:
    """Type-aware structural equality for parsed JSON values.

    Python's ``==`` treats ``True == 1`` and ``False == 0``, so a payload
    carrying a boolean would compare equal to one carrying the integer —
    a real emitter divergence. Booleans therefore compare only with
    booleans, ``None`` only with ``None``, and strings only with strings.
    Numbers compare numerically (Python's int/float comparison is exact,
    so no large-integer precision is lost).
    """
    if isinstance(a, bool) or isinstance(b, bool):
        return isinstance(a, bool) and isinstance(b, bool) and a == b
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, dict) and isinstance(b, dict):
        if a.keys() != b.keys():
            return False
        return all(_json_values_equal(a[key], b[key]) for key in a)
    if isinstance(a, list) and isinstance(b, list):
        return len(a) == len(b) and all(_json_values_equal(x, y) for x, y in zip(a, b))
    if isinstance(a, str) or isinstance(b, str):
        return isinstance(a, str) and isinstance(b, str) and a == b
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return a == b
    return type(a) is type(b) and a == b


def semantic_equal(a: bytes | str, b: bytes | str) -> bool:
    """Compare two JSON payloads by their parsed data, not their bytes.

    Task 32 charter amendment: serialization is "semantically-equivalent,
    byte-identical where free". Key order and free numeric/whitespace
    formatting may differ between the Python ground truth and the Go
    emitter; the parsed values must not. Comparison is type-aware (bool is
    not a number — Task 32 review item 13). Input that is not strict JSON
    (including NaN/Infinity, which json.loads otherwise accepts) falls back
    to a byte comparison, so framing/header bytes compare raw.
    """
    try:
        va = json.loads(a, parse_constant=_reject_constant)
        vb = json.loads(b, parse_constant=_reject_constant)
    except (ValueError, TypeError):
        return a == b
    return _json_values_equal(va, vb)


def build_shim(directory: Path, extra_shim: str = "") -> Path:
    """Write the storage-origin shim (plus any capture-specific extra
    snippet) and return the PYTHONPATH entry."""
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "sitecustomize.py").write_text(SHIM_SITECUSTOMIZE + extra_shim, encoding="utf-8")
    return directory


def wait_until(predicate: Callable[[], Any], *, timeout: float = 30.0, interval: float = 0.1, description: str) -> Any:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    raise TimeoutError(f"timed out after {timeout}s waiting for {description}")


def copy_journals(directory: Path) -> Path:
    """Copy the four fixture journal files (Messages 0..39, fixed 2021
    timestamps) into a test-local directory."""
    directory.mkdir(parents=True, exist_ok=True)
    for journal in sorted(JOURNAL_DIR.glob("*.journal")):
        (directory / journal.name).write_bytes(journal.read_bytes())
    return directory


def base_reader_config(journal_path: Path, senders: dict[str, dict[str, Any]]) -> dict[str, Any]:
    return {
        "readers": {
            READER_NAME: {
                "journal_path": str(journal_path),
                "initial_position": "head",
                "senders": senders,
            }
        },
    }


@contextmanager
def groundtruth_pump(config: dict[str, Any], tmp_path: Path, extra_shim: str = "") -> Iterator[JournalpumpProcess]:
    """Start the REAL Python binary through the Task 10 harness.

    JOURNALPUMP_BIN is forced to "" (the Python module) regardless of
    the surrounding environment: ground truth must be captured from the
    Python implementation. The storage-origin shim is injected through
    PYTHONPATH (see the module docstring); extra_shim appends
    capture-specific snippets (e.g. the unbuffered file-output shim).
    """
    shim_dir = build_shim(tmp_path / "shim", extra_shim)
    config_path = tmp_path / "journalpump.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    with JournalpumpProcess() as process:
        process.start(config_path, env={"PYTHONPATH": str(shim_dir), "JOURNALPUMP_BIN": ""})
        yield process


def read_sender_state(state_path: Path, sender_name: str) -> dict[str, Any]:
    state = json.loads(state_path.read_text(encoding="utf-8"))
    return state["readers"][READER_NAME]["senders"][sender_name]


def deterministic_state(state_path: Path) -> dict[str, Any]:
    """The Python-written state file with wall-clock fields dropped:
    everything a Go-side ResumeCursor/parity test can pin byte-for-byte."""
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state.pop("start_time")
    for reader in state["readers"].values():
        for sender in reader.get("senders", {}).values():
            sender.get("health", {}).pop("elapsed", None)
    return state


class CaptureHTTPServer:
    """Recording HTTP server for ground-truth capture.

    Routes map (method, path-without-query) to (status, body bytes,
    content type). Every request is recorded with method, path, raw
    header dict (lower-cased names) and raw body bytes.
    """

    def __init__(self, routes: dict[tuple[str, str], tuple[int, bytes, str]]) -> None:
        self.routes = routes
        self.requests: list[dict[str, Any]] = []
        outer = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def _capture(self) -> None:
                length = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(length) if length else b""
                outer.requests.append(
                    {
                        "method": self.command,
                        "path": self.path,
                        "headers": {k.lower(): v for k, v in self.headers.items()},
                        "body": body,
                    }
                )
                route = outer.routes.get((self.command, self.path))
                if route is None:
                    status, payload, content_type = 404, b'{"error": "not found"}', "application/json"
                else:
                    status, payload, content_type = route
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            do_GET = do_PUT = do_POST = do_DELETE = _capture

            def log_message(self, *args: object) -> None:
                pass

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._server.daemon_threads = True
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self._server.server_address[1]}"

    def requests_for(self, method: str, path: str) -> list[dict[str, Any]]:
        return [r for r in self.requests if r["method"] == method and r["path"] == path]

    def requests_matching(self, method: str, path_suffix: str) -> list[dict[str, Any]]:
        return [r for r in self.requests if r["method"] == method and r["path"].endswith(path_suffix)]


LOGPLEX_TIMESTAMP = re.compile(rb"^(<190>1 )\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+00:00 ")
LOGPLEX_TIMESTAMP_PLACEHOLDER = b"0000-00-00T00:00:00+00:00"


def parse_logplex_frames(body: bytes) -> list[bytes]:
    """Split a logplex POST body into its length-prefixed frames.

    Python wire shape; sanctioned deviation — Go sends the clean RFC
    frame (charter §2/D6). Quirk captured from the real binary:
    LogplexSender.format_msg f-strings the ENCODED packet
    (``f"{len(pkt)} {pkt}"`` with bytes ``pkt``), so each length prefix
    is followed by the Python bytes REPR of the packet (``b'...'``) —
    length + 3 characters on the wire. The fixture pins that shape as
    captured Python ground truth only; Go's content parity is asserted
    via the repr-stripped frame list, not these bytes.
    """
    frames = []
    index = 0
    while index < len(body):
        space = body.index(b" ", index)
        length = int(body[index:space])
        start = space + 1
        segment = body[start : start + length + 3]
        assert segment.startswith(b"b'") and segment.endswith(b"'"), segment[:20]
        frames.append(segment[2:-1])
        index = start + length + 3
    return frames


def normalize_logplex_frame(frame: bytes) -> bytes:
    """Replace the wall-clock RFC5424 timestamp with a fixed-width
    placeholder (same 25-character shape) so frames become comparable."""
    return LOGPLEX_TIMESTAMP.sub(lambda match: match.group(1) + LOGPLEX_TIMESTAMP_PLACEHOLDER + b" ", frame)
