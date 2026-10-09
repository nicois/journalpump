#!/usr/bin/env python3
# Copyright 2026, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.
"""Consume a Kafka topic with kafka-python and dump the records as JSON.

The Go end-to-end test (systest-go/kafka_e2e_test.go) uses this to prove the
sarama producer's snappy output is byte-compatible with the deployed
kafka-python client (spec risk 10 / charter D5): the same consumer library
the Python journalpump used reads the Go-produced records.

Usage: kafka_python_consumer.py BROKER TOPIC PARTITIONS COUNT TIMEOUT_SECONDS

Prints a single JSON object to stdout:
    {"partitions": <int>, "records": [
        {"partition": int, "offset": int, "key_hex": str|null, "value_b64": str}]}
with the records ordered by (partition, offset). Non-zero exit on failure.
"""

import base64
import json
import sys
import time

from kafka import KafkaConsumer, TopicPartition


def main() -> int:
    broker, topic = sys.argv[1], sys.argv[2]
    partitions, count = int(sys.argv[3]), int(sys.argv[4])
    timeout = float(sys.argv[5])

    consumer = KafkaConsumer(
        bootstrap_servers=broker,
        enable_auto_commit=False,
        consumer_timeout_ms=2000,
        request_timeout_ms=30000,
    )
    records = []
    seen_partitions = 0
    deadline = time.monotonic() + timeout
    try:
        while time.monotonic() < deadline and len(records) < count:
            parts = consumer.partitions_for_topic(topic)
            if not parts:
                time.sleep(0.5)
                continue
            seen_partitions = len(parts)
            # Re-read from the beginning every pass so a partially
            # populated topic does not duplicate records across passes.
            records = []
            tps = [TopicPartition(topic, p) for p in sorted(parts)]
            consumer.assign(tps)
            for tp in tps:
                consumer.seek_to_beginning(tp)
            while time.monotonic() < deadline and len(records) < count:
                polled = consumer.poll(timeout_ms=1000)
                if not polled:
                    break
                for msgs in polled.values():
                    for msg in msgs:
                        records.append(
                            {
                                "partition": msg.partition,
                                "offset": msg.offset,
                                "key_hex": None if msg.key is None else msg.key.hex(),
                                "value_b64": base64.b64encode(msg.value).decode("ascii"),
                            }
                        )
    finally:
        consumer.close()

    records.sort(key=lambda r: (r["partition"], r["offset"]))
    print(json.dumps({"partitions": seen_partitions, "expected_partitions": partitions, "records": records}))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:  # noqa: BLE001 - report any failure to the caller
        print(f"kafka python consumer failed: {exc!r}", file=sys.stderr)
        sys.exit(1)