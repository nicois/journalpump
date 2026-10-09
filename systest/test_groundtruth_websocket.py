# Copyright 2026, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.
"""Ground-truth capture, websocket sender (plan Task 11).

Runs the real Python binary against a local websocket server and pins
the connect headers and the binary frames it sends.

With the default config the sender batches (``max_batch_size`` > 0)
and snappy-compresses each batch: a frame is
``snappy.compress(b"\\x00".join(batch_messages))``. Batch boundaries are
timing-dependent, so the fixture stores the DECOMPRESSED message stream
(concatenation across all frames, NUL-split) — identical for any
batching — while the capture test verifies every frame is snappy
compressed and decompresses to the same stream.

Standard websocket handshake headers (``Sec-WebSocket-Key`` etc.) are
per-connection random and excluded from the fixture.
"""

from systest import groundtruth_support as gt
from pathlib import Path

import asyncio
import pytest
import snappy
import threading
import websockets.asyncio.server

_FIXTURE_NAME = "websocket"


class WebsocketCaptureServer:
    """Capture server recording each connection's headers and binary frames."""

    def __init__(self) -> None:
        self.connections: list[dict] = []
        self._loop: asyncio.AbstractEventLoop | None = None
        self._server: websockets.asyncio.server.Server | None = None

    async def _handler(self, websocket: websockets.asyncio.server.ServerConnection) -> None:
        record: dict = {"headers": dict(websocket.request.headers), "frames": []}
        self.connections.append(record)
        async for message in websocket:
            record["frames"].append(message)

    def start(self) -> str:
        self._loop = asyncio.new_event_loop()
        ready = threading.Event()
        port: list[int] = []

        async def _serve() -> None:
            self._server = await websockets.asyncio.server.serve(self._handler, "127.0.0.1", 0)
            port.append(self._server.sockets[0].getsockname()[1])
            ready.set()
            await self._server.serve_forever()

        def _run() -> None:
            try:
                self._loop.run_until_complete(_serve())
            except RuntimeError:  # loop.stop() during shutdown
                pass

        threading.Thread(target=_run, daemon=True).start()
        ready.wait(timeout=10)
        return f"ws://127.0.0.1:{port[0]}/"

    def stop(self) -> None:
        assert self._loop is not None and self._server is not None

        async def _shutdown() -> None:
            self._server.close()
            await self._server.wait_closed()

        asyncio.run_coroutine_threadsafe(_shutdown(), self._loop).result(timeout=5)
        self._loop.call_soon_threadsafe(self._loop.stop)

    def frames(self) -> list[bytes]:
        return [frame for connection in self.connections for frame in connection["frames"]]

    def first_headers(self) -> dict[str, str]:
        return {k.lower(): v for k, v in self.connections[0]["headers"].items()}


def _config(state_path: Path, uri: str, journal_path: Path) -> dict:
    config = gt.base_reader_config(
        journal_path,
        {"out": {"output_type": "websocket", "websocket_uri": uri}},
    )
    config["json_state_file_path"] = str(state_path)
    return config


def test_websocket_wire_bytes(tmp_path: Path) -> None:
    fixture = gt.load_fixture(_FIXTURE_NAME)
    server = WebsocketCaptureServer()
    uri = server.start()
    try:
        journal_path = gt.copy_journals(tmp_path / "logs")
        state_path = tmp_path / "state.json"
        with gt.groundtruth_pump(_config(state_path, uri, journal_path), tmp_path) as process:
            gt.wait_until(
                lambda: sum(
                    len(snappy.decompress(frame).split(b"\x00")) for frame in server.frames()
                )
                >= 40,
                description="40 captured websocket messages",
            )
            process.stop()
    finally:
        server.stop()

    frames = server.frames()
    assert frames, "no websocket frames captured"
    headers = server.first_headers()
    messages: list[str] = []
    for frame in frames:
        decompressed = snappy.decompress(frame)
        messages.extend(part.decode("utf-8") for part in decompressed.split(b"\x00"))
    assert len(messages) == 40

    sent = gt.read_sender_state(state_path, "out")["sent"]
    if fixture is None or gt.update_mode():
        fixture = {
            "name": _FIXTURE_NAME,
            "capture": {
                "journals": "systest/data/rotated_logs/*.journal (Messages 0..39)",
                "sender": {"output_type": "websocket", "websocket_uri": "<capture server>"},
            },
            "connect_headers": {
                "journalpump-batch-size": headers["journalpump-batch-size"],
                "journalpump-compression": headers["journalpump-compression"],
                "user-agent": headers["user-agent"],
            },
            "messages": messages,
            "expected": {
                "sent_count": sent["count"],
                "sent_cursor": sent["cursor"],
            },
            "notes": [
                "each binary frame is snappy.compress(NUL-join(batch)); batch boundaries are "
                "timing-dependent so the fixture stores the decompressed message stream",
                "standard websocket handshake headers are per-connection random and excluded",
                "Journalpump-Batch-Size 1048576 is the default max_batch_size (>0 => batching on)",
            ],
        }
        gt.save_fixture(_FIXTURE_NAME, fixture)
        if not gt.update_mode():
            pytest.fail("websocket fixture was missing and has been generated; commit it and rerun")

    assert headers["user-agent"] == fixture["connect_headers"]["user-agent"]
    assert headers["journalpump-batch-size"] == fixture["connect_headers"]["journalpump-batch-size"]
    assert headers["journalpump-compression"] == fixture["connect_headers"]["journalpump-compression"]
    assert messages == fixture["messages"]
    assert sent["count"] == fixture["expected"]["sent_count"]
    assert sent["cursor"] == fixture["expected"]["sent_cursor"]
