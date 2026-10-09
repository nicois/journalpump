# Copyright 2026, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.
"""Ground-truth capture, Kafka sender against a real broker (plan Task 12).

Runs the REAL Python binary (kafka-python producer, per requirements.txt
``kafka-python<2.4.0``) against a real Kafka 4.0 broker (KRaft, PLAINTEXT)
and records what the consumer side sees: delivered byte payloads, message
key, partition placement, headers, per-record offsets and the sender's
saved state.

TWO runs with different configurations pin the two topic-lifecycle and
keying behaviors:

1. auto-create: no ``kafka_topic_config``, no ``kafka_msg_key`` — the
   broker auto-creates the topic with its default 1 partition and the
   records carry no key (partition 0).
2. manual create + keying: ``kafka_topic_config`` {num_partitions: 2,
   replication_factor: 1} makes the sender create the topic itself via
   the admin client (the path for clusters with
   auto.create.topics.enable = false), and a fixed ``kafka_msg_key``
   pins the key→partition placement: kafka-python's default partitioner
   is murmur2(key) % num_partitions, so ALL records land on ONE
   deterministic partition.

Compression: the sender prefers zstd whenever ``zstandard`` is
importable, else snappy, else gzip (kafka_sender.py:96-101). The
DEPLOYMENT ships python-snappy and no zstandard (requirements.txt), so
the shared sitecustomize shim blocks the system zstandard — the capture
pins the snappy configuration. The consumer side is transparent
decompression; the on-wire compressed-batch framing (kafka-python's
snappy codec = xerial block framing, header
``\\x82SNAPPY\\x00\\x00\\x00\\x01\\x00\\x00\\x00\\x01`` = 0x82 + 'SNAPPY'
+ int32 version 1 + int32 compat 1, kafka/codec.py
``_XERIAL_V1_HEADER``) is
producer↔broker detail that the fixture documents but the consumer
cannot observe — the sarama-vs-kafka-python framing interop verdict is
Task 22's.

The broker is real, not mocked: the test manages a single-node Apache
Kafka container via podman (image ``docker.io/apache/kafka:4.0.0``,
KRaft, auto-create enabled) unless ``JOURNALPUMP_KAFKA_BROKER`` points
at an already-running broker (CI service). Record timestamps are
broker-assigned wall clock — dropped like every wall-clock field.

Re-initialization behavior documented from source (not captured): a
re-running sender whose topic already exists catches
``TopicAlreadyExistsError`` and logs "already exists"
(kafka_sender.py:150-151); per-message ``send()`` + ``flush()`` +
``get(timeout=1)`` must ALL succeed before ``mark_sent``
(kafka_sender.py:163-172), and any retriable error re-initializes the
producer.
"""

from systest import groundtruth_support as gt
from pathlib import Path
from uuid import uuid4

import importlib.util
import os
import pytest
import socket
import subprocess
import time

from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic

_FIXTURE_NAME = "kafka"
_BROKER_ENV = "JOURNALPUMP_KAFKA_BROKER"
_MESSAGE_COUNT = 40


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _podman(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["podman", *args], check=True, capture_output=True, text=True)


def _wait_ready(address: str, timeout: float = 120.0) -> None:
    deadline = time.monotonic() + timeout
    last: Exception | None = None
    while time.monotonic() < deadline:
        try:
            KafkaAdminClient(bootstrap_servers=address).close()
            return
        except Exception as ex:  # noqa: BLE001 - broker not up yet, any error retries
            last = ex
            time.sleep(2)
    raise TimeoutError(f"Kafka broker {address} not ready: {last}")


@pytest.fixture(scope="module")
def kafka_broker() -> str:
    """A real Kafka broker address: the CI-provided one from
    JOURNALPUMP_KAFKA_BROKER, or a single-node KRaft container this
    module starts and removes."""
    override = os.environ.get(_BROKER_ENV)
    if override:
        yield override
        return
    port = _free_port()
    container = f"journalpump-kafka-gt-{uuid4().hex[:8]}"
    _podman(
        "run",
        "-d",
        "--name",
        container,
        "-p",
        f"{port}:9092",
        "-e",
        "KAFKA_NODE_ID=1",
        "-e",
        "KAFKA_PROCESS_ROLES=broker,controller",
        "-e",
        "KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093",
        "-e",
        "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
        "-e",
        "KAFKA_CONTROLLER_QUORUM_VOTERS=1@127.0.0.1:9093",
        "-e",
        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        "-e",
        # Advertise the MAPPED host port: after the bootstrap connection
        # every subsequent client connection goes to the advertised
        # listener, so a hardcoded 9092 would silently redirect producer
        # and consumer away from the mapped port.
        f"KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:{port}",
        "-e",
        "KAFKA_AUTO_CREATE_TOPICS_ENABLE=true",
        "-e",
        "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
        "-e",
        "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
        "-e",
        "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
        "-e",
        "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",
        "docker.io/apache/kafka:4.0.0",
    )
    try:
        _wait_ready(f"localhost:{port}")
        yield f"localhost:{port}"
    finally:
        subprocess.run(["podman", "rm", "-f", container], capture_output=True)


def _config(state_path: Path, broker: str, topic: str, *, msg_key: str | None, topic_config: dict | None) -> dict:
    sender: dict = {"output_type": "kafka", "kafka_address": broker, "kafka_topic": topic}
    if msg_key is not None:
        sender["kafka_msg_key"] = msg_key
    if topic_config is not None:
        sender["kafka_topic_config"] = topic_config
    config = gt.base_reader_config(gt.copy_journals(state_path.parent / "logs"), {"out": sender})
    config["json_state_file_path"] = str(state_path)
    return config


def _partition_count(broker: str, topic: str) -> int:
    admin = KafkaAdminClient(bootstrap_servers=broker)
    try:
        description = admin.describe_topics([topic])[0]
    finally:
        admin.close()
    return 0 if description["error_code"] != 0 else len(description["partitions"])


def _consume_all(broker: str, topic: str) -> list:
    """Consume every record of a freshly-written topic (fresh consumer
    group, earliest offset)."""
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=broker,
        group_id=f"gt-{uuid4().hex[:8]}",
        auto_offset_reset="earliest",
        consumer_timeout_ms=10_000,
    )
    try:
        records: list = []
        deadline = time.monotonic() + 60
        while len(records) < _MESSAGE_COUNT and time.monotonic() < deadline:
            batch = consumer.poll(timeout_ms=2_000, max_records=500)
            for fetched in batch.values():
                records.extend(fetched)
        assert len(records) >= _MESSAGE_COUNT, f"consumed {len(records)} of {_MESSAGE_COUNT} records"
        return sorted(records, key=lambda record: (record.partition, record.offset))
    finally:
        consumer.close()


def _run_and_capture(
    broker: str,
    tmp_path: Path,
    topic: str,
    *,
    msg_key: str | None,
    topic_config: dict | None,
    expected_partitions: int | None = None,
) -> dict:
    """Run the real binary against the broker and capture the consumed
    records plus the sender's saved state.

    With ``expected_partitions`` the consumer waits until the sender's
    admin client has created the topic BEFORE it subscribes: a
    subscribing consumer issues metadata requests containing the topic
    name, which auto-create it on an auto-create-enabled broker with the
    default 1 partition and would race (and lose against) the sender's
    own create_topics — the very behavior being captured here."""
    state_path = tmp_path / "state.json"
    with gt.groundtruth_pump(_config(state_path, broker, topic, msg_key=msg_key, topic_config=topic_config), tmp_path) as process:
        if expected_partitions is not None:
            gt.wait_until(
                lambda: _partition_count(broker, topic) == expected_partitions,
                description=f"sender-created topic {topic} with {expected_partitions} partitions",
            )
        records = _consume_all(broker, topic)
        process.stop()

    partitions = {record.partition for record in records}
    assert len(partitions) == 1, f"expected one partition, got {sorted(partitions)}"
    partition = partitions.pop()
    offsets = [record.offset for record in records]
    assert offsets == list(range(len(records))), f"offsets not contiguous from 0: {offsets[:5]}..."

    # Timestamps are broker-assigned wall clock — excluded like every
    # wall-clock field. Headers: the Python producer sends none.
    assert all(record.headers == [] for record in records), "unexpected record headers"
    keys = {record.key for record in records}
    assert len(keys) == 1, f"expected one key, got {keys}"
    sent = gt.read_sender_state(state_path, "out")["sent"]
    # Payloads are pinned as UTF-8 strings, which is byte-exact only for
    # ASCII — fail loud on non-ASCII capture bytes (the fixture would
    # then have to switch to base64 pinning).
    payloads = [record.value.decode("utf-8") for record in records]
    assert all(payload.isascii() for payload in payloads), "non-ASCII payload bytes: fixture pinning must switch to base64"
    return {
        "partition": partition,
        "key": records[0].key.decode("utf-8") if records[0].key is not None else None,
        "payloads": payloads,
        "sent_count": sent["count"],
        "sent_cursor": sent["cursor"],
    }


def test_kafka_wire_records(tmp_path: Path, kafka_broker: str) -> None:
    fixture = gt.load_fixture(_FIXTURE_NAME)
    broker = kafka_broker

    # The admin probe counts as ground truth too: the AUTO-CREATE topic
    # does not exist before the run and its partition count afterwards
    # is the broker default.
    topic_auto = f"journalpump-gt-auto-{uuid4().hex[:8]}"
    topic_manual = f"journalpump-gt-manual-{uuid4().hex[:8]}"

    auto = _run_and_capture(broker, tmp_path / "auto", topic_auto, msg_key=None, topic_config=None)
    admin = KafkaAdminClient(bootstrap_servers=broker)
    try:
        auto_partitions = len(admin.describe_topics([topic_auto])[0]["partitions"])
    finally:
        admin.close()

    manual = _run_and_capture(
        broker,
        tmp_path / "manual",
        topic_manual,
        msg_key="journalpump",
        topic_config={"num_partitions": 2, "replication_factor": 1},
        expected_partitions=2,
    )
    # The manual topic's 2 partitions must exist AFTER its run settled:
    # the sender's own admin client created them before producing.
    admin = KafkaAdminClient(bootstrap_servers=broker)
    try:
        manual_partitions = len(admin.describe_topics([topic_manual])[0]["partitions"])
    finally:
        admin.close()
    assert manual_partitions == 2, f"manual create produced {manual_partitions} partitions"

    if fixture is None or gt.update_mode():
        # The "compression" field below pins snappy. The sender falls
        # back silently when python-snappy is absent
        # (kafka_sender.py:96-101), so refuse to capture a fixture that
        # would misstate the codec for Task 22. The test process shares
        # the child's site-packages (the shim only adds PYTHONPATH), so
        # find_spec is a faithful proxy for the child's import.
        if importlib.util.find_spec("snappy") is None:
            raise AssertionError(
                "python-snappy not importable: the Kafka sender would fall back to gzip "
                "(kafka_sender.py:96-101); refusing to capture a 'snappy' fixture"
            )
        fixture = {
            "name": _FIXTURE_NAME,
            "capture": {
                "journals": "systest/data/rotated_logs/*.journal (Messages 0..39)",
                "broker": "<real Kafka 4.0 KRaft broker, PLAINTEXT>",
                "runs": [
                    {"mode": "auto-create", "kafka_msg_key": None, "kafka_topic_config": None},
                    {
                        "mode": "manual create via admin client",
                        "kafka_msg_key": "journalpump",
                        "kafka_topic_config": {"num_partitions": 2, "replication_factor": 1},
                    },
                ],
            },
            "compression": "snappy",
            "expected": {
                "auto_create": {
                    "num_partitions": auto_partitions,
                    **auto,
                },
                "manual_create_keyed": {
                    "num_partitions": manual_partitions,
                    **manual,
                },
            },
            "notes": [
                "auto-create: no kafka_topic_config and auto.create.topics.enable=true — the broker creates the "
                "topic with its default partition count; records are keyless and land on partition 0",
                "manual create: kafka_topic_config num_partitions=2 replication_factor=1 makes the sender create "
                "the topic via KafkaAdminClient.create_topics (the path for auto.create.topics.enable=false "
                "clusters); TopicAlreadyExistsError on re-init logs 'already exists' (kafka_sender.py:150-151)",
                "consumer race: a subscribing consumer's metadata request containing the topic name auto-creates "
                "it on an auto-create-enabled broker with the DEFAULT 1 partition, so the manual-create capture "
                "waits for the sender's own create_topics (verified 2 partitions in the journal) before "
                "subscribing — probed: producer construction alone does not auto-create; a send() does",
                "keying: a fixed kafka_msg_key pins kafka-python's default partitioner murmur2(key) % "
                "num_partitions — ALL keyed records land on ONE deterministic partition",
                "compression: producer chose snappy (zstd preferred when zstandard is importable — blocked by "
                "the test shim because the deployment ships no zstandard, requirements.txt); consumer-side "
                "decompression is transparent",
                "xerial framing: kafka-python's snappy codec compresses batches in Java-compatible xerial block "
                "framing (header b'\\x82SNAPPY\\x00\\x00\\x00\\x01\\x00\\x00\\x00\\x01' = 0x82 + 'SNAPPY' + int32 "
                "version 1 + int32 compat 1, kafka/codec.py _XERIAL_V1_HEADER, followed by the block table) — "
                "producer-to-broker detail invisible to the consumer; the sarama framing interop verdict is "
                "Task 22's",
                "produce path pinned from source: per-message send() + flush() + future.get(timeout=1) must all "
                "succeed before mark_sent (kafka_sender.py:163-172); producer linger_ms=500",
                "record timestamps are broker-assigned wall clock (excluded); headers are empty (the Python "
                "producer sends none); offsets are contiguous from 0 on the fresh topic",
            ],
        }
        gt.save_fixture(_FIXTURE_NAME, fixture)
        if not gt.update_mode():
            pytest.fail("kafka fixture was missing and has been generated; commit it and rerun")

    expected = fixture["expected"]
    auto_expected = expected["auto_create"]
    manual_expected = expected["manual_create_keyed"]
    assert auto_partitions == auto_expected["num_partitions"]
    assert manual_partitions == manual_expected["num_partitions"]
    assert auto["partition"] == auto_expected["partition"]
    assert auto["key"] == auto_expected["key"] is None
    assert auto["payloads"] == auto_expected["payloads"]
    assert auto["sent_count"] == auto_expected["sent_count"]
    assert auto["sent_cursor"] == auto_expected["sent_cursor"]
    assert manual["partition"] == manual_expected["partition"]
    assert manual["key"] == manual_expected["key"] == "journalpump"
    assert manual["payloads"] == manual_expected["payloads"]
    assert manual["sent_count"] == manual_expected["sent_count"]
    assert manual["sent_cursor"] == manual_expected["sent_cursor"]
