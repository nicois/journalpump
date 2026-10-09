# Copyright 2026, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.
"""Ground-truth capture, OpenSearch sender (plan Task 11).

Runs the real Python binary against a fake OpenSearch HTTP server and
pins the request sequence, the index-create mapping and the `_bulk`
NDJSON bytes.

Daily rollover is captured with TWO sequential runs of a single
sender: one default (messages date to 2021-12-27 from their payload
``timestamp``) and one with an ``extra_field_values`` timestamp
override (2022-01-05). The index name is derived per message from
``timestamp[:10]`` — that per-message date derivation is what drives
daily rollover, and the mapping is derived from each index's FIRST
message.

The two runs are sequential because of a quirk the capture itself
locks: ``JournalObjectHandler.json_objects`` caches the serialized
entry per ``field_filter`` name, so two senders WITHOUT a field filter
on the same reader share the first sender's cached payload and the
second sender's ``extra_field_values`` are silently ignored.

Batch/POST boundaries are timing-dependent: each index's bulk payload
stream is compared as one ordered concatenation, independent of how
the messages were split into POSTs. ``GET /_aliases`` polls are
time-gated (hourly index check) and may interleave anywhere; they carry
no payload and are excluded from the ordered comparison.
"""

from systest import groundtruth_support as gt
from pathlib import Path

import json
import pytest

_FIXTURE_NAME = "elasticsearch"
_OVERRIDE_TIMESTAMP = "2022-01-05T12:00:00.000000"


def _config(state_path: Path, base_url: str, journal_path: Path, override: bool) -> dict:
    sender: dict = {"output_type": "opensearch", "opensearch_url": base_url}
    if override:
        sender["extra_field_values"] = {"timestamp": _OVERRIDE_TIMESTAMP}
    config = gt.base_reader_config(journal_path, {"out": sender})
    config["json_state_file_path"] = str(state_path)
    return config


def _routes() -> dict[tuple[str, str], tuple[int, bytes, str]]:
    return {
        ("GET", "/"): (200, b'{"version": {"number": "9.0.0"}}', "application/json"),
        ("GET", "/_aliases"): (200, b"{}", "application/json"),
        ("PUT", "/journalpump-2021-12-27"): (200, b'{"acknowledged": true}', "application/json"),
        ("PUT", "/journalpump-2022-01-05"): (200, b'{"acknowledged": true}', "application/json"),
        ("POST", "/journalpump-2021-12-27/_bulk"): (200, b'{"errors": false, "items": []}', "application/json"),
        ("POST", "/journalpump-2022-01-05/_bulk"): (200, b'{"errors": false, "items": []}', "application/json"),
    }


def _bulk_index_and_payloads(body: bytes) -> list[tuple[str, str]]:
    """Split an NDJSON bulk body into (index, payload) message pairs."""
    lines = body.decode("utf-8").splitlines()
    assert len(lines) % 2 == 0, "bulk body must be action/payload line pairs"
    result = []
    for action, payload in zip(lines[0::2], lines[1::2]):
        index = action.rsplit('"_index": "', 1)[1].rsplit('"', 1)[0]
        result.append((index, payload))
    return result


def test_elasticsearch_wire_bytes(tmp_path: Path) -> None:
    fixture = gt.load_fixture(_FIXTURE_NAME)
    mapping_by_index: dict[str, dict] = {}
    payloads_by_index: dict[str, list[str]] = {}
    bulk_headers: dict[str, str] = {}
    version_get_count = 0
    ordered_non_alias: list[tuple[str, str]] = []
    sent_values: list[dict] = []

    for override in (False, True):
        server = gt.CaptureHTTPServer(_routes())
        server.start()
        try:
            journal_path = gt.copy_journals(tmp_path / f"logs-{override}")
            state_path = tmp_path / f"state-{override}.json"
            with gt.groundtruth_pump(_config(state_path, server.base_url, journal_path, override), tmp_path) as process:
                gt.wait_until(
                    lambda: (
                        sum(len(_bulk_index_and_payloads(r["body"])) for r in server.requests_matching("POST", "/_bulk"))
                        >= 40
                    ),
                    description="40 captured bulk messages",
                )
                process.stop()
        finally:
            server.stop()

        # Ordered non-alias request stream: version probe, index create,
        # bulks. Alias polls are time-gated and excluded (module docstring).
        run_ordered = [
            (r["method"], r["path"]) for r in server.requests if not (r["method"] == "GET" and r["path"] == "/_aliases")
        ]
        assert run_ordered.count(("GET", "/")) == 1, "the sender probes the version exactly once"
        assert run_ordered.count(("PUT", "/journalpump-2021-12-27" if not override else "/journalpump-2022-01-05")) == 1
        ordered_non_alias.extend(run_ordered)
        version_get_count += run_ordered.count(("GET", "/"))

        for request in [r for r in server.requests if r["method"] == "PUT" and r["path"].startswith("/journalpump-")]:
            index_name = request["path"].rsplit("/", 1)[1]
            assert index_name not in mapping_by_index, "each index is created exactly once"
            mapping_by_index[index_name] = json.loads(request["body"])
        for request in server.requests_matching("POST", "/_bulk"):
            assert request["path"].endswith("/_bulk")
            bulk_headers = request["headers"]
            for index_name, payload in _bulk_index_and_payloads(request["body"]):
                payloads_by_index.setdefault(index_name, []).append(payload)
        sent = gt.read_sender_state(state_path, "out")["sent"]
        sent_values.append({"count": sent["count"], "cursor": sent["cursor"]})
        # The sender sets content-length explicitly from the buffered
        # size — asserted per run, covering BOTH runs.
        assert all(int(r["headers"]["content-length"]) == len(r["body"]) for r in server.requests_matching("POST", "/_bulk"))

    assert version_get_count == 2
    assert sorted(payloads_by_index) == ["journalpump-2021-12-27", "journalpump-2022-01-05"]
    assert sorted(mapping_by_index) == ["journalpump-2021-12-27", "journalpump-2022-01-05"]

    if fixture is None or gt.update_mode():
        fixture = {
            "name": _FIXTURE_NAME,
            "capture": {
                "journals": "systest/data/rotated_logs/*.journal (Messages 0..39)",
                "runs": [
                    {
                        "sender": {"output_type": "opensearch", "opensearch_url": "<capture server>"},
                        "index": "journalpump-2021-12-27",
                    },
                    {
                        "sender": {
                            "output_type": "opensearch",
                            "opensearch_url": "<capture server>",
                            "extra_field_values": {"timestamp": _OVERRIDE_TIMESTAMP},
                        },
                        "index": "journalpump-2022-01-05",
                    },
                ],
            },
            "version_response": {"version": {"number": "9.0.0"}},
            # Ordered non-alias request sequence, deduplicated to the
            # first-seen order of distinct (method, path) pairs: POST
            # counts are timing-dependent (see the payload-stream note),
            # so only the distinct sequence is pinned. Aliases polls are
            # time-gated and excluded.
            "request_sequence": list(dict.fromkeys(ordered_non_alias)),
            "indices": {
                index: {"mapping": mapping_by_index[index], "payloads": payloads_by_index[index]}
                for index in sorted(mapping_by_index)
            },
            "bulk_request": {
                "method": "POST",
                "path_suffix": "/_bulk",
                "headers": {"content-type": bulk_headers.get("content-type", "")},
            },
            "expected": sent_values[0],
            "notes": [
                "daily rollover: the index name is <prefix>-<timestamp[:10]> derived per "
                "message from its payload timestamp; the second run's extra_field_values "
                "timestamp override lands every message in journalpump-2022-01-05",
                "the mapping is derived from each index's FIRST message",
                "the _bulk URL is the most recent index name (cosmetic); each action header "
                "carries the message's own _index",
                "two senders WITHOUT a field filter share the handler's per-entry JSON cache, "
                "so the second sender's extra_field_values are ignored: the rollover runs are "
                "sequential, not concurrent",
                "GET /_aliases polls are time-gated and excluded from the ordered comparison",
                "batch/POST boundaries are timing-dependent: compare concatenated payload streams per index",
            ],
        }
        gt.save_fixture(_FIXTURE_NAME, fixture)
        if not gt.update_mode():
            pytest.fail("elasticsearch fixture was missing and has been generated; commit it and rerun")

    assert sorted(mapping_by_index) == sorted(fixture["indices"])
    for index, expected_index in fixture["indices"].items():
        assert mapping_by_index[index] == expected_index["mapping"]
        assert payloads_by_index[index] == expected_index["payloads"]
        assert len(payloads_by_index[index]) == 40
    assert bulk_headers["content-type"] == fixture["bulk_request"]["headers"]["content-type"]
    # JSON round-trips pairs as lists, so normalize the captured tuples.
    assert [list(pair) for pair in dict.fromkeys(ordered_non_alias)] == fixture["request_sequence"]
    for sent in sent_values:
        assert sent["count"] == fixture["expected"]["count"]
        assert sent["cursor"] == fixture["expected"]["cursor"]
