"""Kafka 실브로커 왕복 통합 테스트 (옵션).

기본/CI 에서는 스킵된다. 실행하려면 로컬 브로커를 띄우고 환경변수를 켠다:

    docker compose up -d
    RUN_KAFKA_IT=1 uv run pytest tests/integration -q
    docker compose down -v

KafkaSink 로 produce 한 메시지를 confluent consumer 로 다시 읽어
토픽명·키·페이로드가 보존되는지 확인한다.
"""
from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime

import pytest

RUN = os.environ.get("RUN_KAFKA_IT") == "1"
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")

pytestmark = pytest.mark.skipif(not RUN, reason="RUN_KAFKA_IT!=1 (실브로커 필요)")


def test_produce_then_consume():
    confluent_kafka = pytest.importorskip("confluent_kafka")
    from realtime_generator.sinks.kafka import KafkaSink

    prefix = f"it{uuid.uuid4().hex[:8]}"  # 테스트 격리용 유니크 토픽 프리픽스
    topic = f"{prefix}.events"

    sink = KafkaSink(bootstrap_servers=BOOTSTRAP, topic_prefix=prefix)
    ts = datetime(2025, 1, 1, 12, 0, 0)
    sink.emit("events", "u7", {"event_type": "purchase", "user_id": 7}, ts)
    sink.close()
    assert sink.delivered == 1
    assert sink.failed == 0

    consumer = confluent_kafka.Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": f"it-{uuid.uuid4().hex[:8]}",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([topic])
    try:
        deadline = time.time() + 15
        msg = None
        while time.time() < deadline:
            m = consumer.poll(1.0)
            if m is not None and not m.error():
                msg = m
                break
        assert msg is not None, "메시지를 15초 내 수신하지 못함"
        assert msg.key() == b"u7"
        assert json.loads(msg.value().decode("utf-8"))["event_type"] == "purchase"
    finally:
        consumer.close()
