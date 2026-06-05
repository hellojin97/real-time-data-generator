"""Kafka 실브로커 통합 테스트 (옵션).

기본/CI(단위) 에서는 스킵된다. 실행하려면 로컬 브로커를 띄우고 환경변수를 켠다:

    docker compose up -d
    RUN_KAFKA_IT=1 uv run pytest tests/integration -q
    docker compose down -v

두 가지를 검증한다:
1) test_produce_then_consume        — 단일 메시지 왕복(배관 smoke)
2) test_generator_stream_fidelity   — 실제 생성기 스트림이 중개를 거쳐 '예상대로' 회수되는지
   (토픽 라우팅·건수·키·페이로드 전체 동일성·payments⊆orders⊆purchase events 참조무결성)
"""
from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime

import pytest

from realtime_generator import clock, dimensions, traffic
from realtime_generator.engine import Engine

RUN = os.environ.get("RUN_KAFKA_IT") == "1"
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
SIM_START = datetime(2025, 1, 1, 0, 0, 0)

pytestmark = pytest.mark.skipif(not RUN, reason="RUN_KAFKA_IT!=1 (실브로커 필요)")

# 스트림별 고유 식별자 필드 (회수 결과 정렬/비교용)
_ID_FIELD = {"events": "event_id", "orders": "order_id", "payments": "payment_id"}


def test_produce_then_consume():
    """배관 smoke: 단일 메시지가 produce→consume 왕복으로 보존되는지."""
    confluent_kafka = pytest.importorskip("confluent_kafka")
    from realtime_generator.sinks.kafka import KafkaSink

    prefix = f"it{uuid.uuid4().hex[:8]}"
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


class _Collect:
    """기대 결과 산출용 in-process 싱크: 스트림별 {id: (key, value)} 로 모은다."""

    def __init__(self) -> None:
        self.by_stream: dict[str, dict] = {"events": {}, "orders": {}, "payments": {}}
        self.total = 0

    def emit(self, stream: str, key: str, value: dict, ts: datetime) -> None:
        self.by_stream[stream][value[_ID_FIELD[stream]]] = (key, value)
        self.total += 1

    def flush(self) -> None:
        pass

    def close(self) -> None:
        pass


def _build_engine(sink, *, seed: int):
    """동일 파라미터로 엔진 구성(결정론). conversion 높여 orders/payments 보장."""
    pool = dimensions.generate(n_users=500, n_products=100, seed=seed)
    return Engine(
        pool,
        traffic.TrafficModel(sessions_per_sec=5.0),
        sink,
        clock.SimulatedClock(SIM_START),
        seed=seed,
        conversion_rate=0.3,
        null_rate_search=0.1,
    )


def test_generator_stream_fidelity():
    """실제 생성기 스트림이 Kafka 중개를 거쳐 '예상대로' 회수되는지 전수 비교."""
    confluent_kafka = pytest.importorskip("confluent_kafka")
    from realtime_generator.sinks.kafka import KafkaSink

    seed = 123
    # 주문/결제는 세션 시작 후 수 분 뒤(퍼널 끝)에 발생하므로, 충분히 큰 윈도우라야
    # orders/payments 가 흐른다(작게 잡으면 초반 page_view 만 잡혀 무의미해짐).
    max_events = 4000

    # 1) 기대 결과: in-process 결정론 실행
    expected = _Collect()
    n_expected = _build_engine(expected, seed=seed).run(max_events=max_events)
    assert n_expected == expected.total == max_events
    # orders 와 payments 가 실제로 흐르는지(테스트가 무의미해지지 않도록)
    assert len(expected.by_stream["orders"]) > 0
    assert len(expected.by_stream["payments"]) > 0

    # 2) 동일 설정으로 Kafka 에 produce
    prefix = f"it{uuid.uuid4().hex[:8]}"
    sink = KafkaSink(bootstrap_servers=BOOTSTRAP, topic_prefix=prefix)
    n_produced = _build_engine(sink, seed=seed).run(max_events=max_events)
    assert n_produced == max_events
    assert sink.delivered == max_events  # 전 건 전달 성공
    assert sink.failed == 0

    # 3) 3개 토픽을 consume 해서 전부 회수
    topics = [f"{prefix}.{s}" for s in ("events", "orders", "payments")]
    consumer = confluent_kafka.Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": f"it-{uuid.uuid4().hex[:8]}",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe(topics)
    actual: dict[str, dict] = {"events": {}, "orders": {}, "payments": {}}
    try:
        deadline = time.time() + 40
        total = 0
        while time.time() < deadline and total < n_produced:
            m = consumer.poll(1.0)
            if m is None or m.error():
                continue
            stream = m.topic().split(".", 1)[1]
            value = json.loads(m.value().decode("utf-8"))
            actual[stream][value[_ID_FIELD[stream]]] = (m.key(), value)
            total += 1
    finally:
        consumer.close()

    # 4) 검증: 건수 → id 집합 → 페이로드 전체 동일성 → 키 → 정합성
    assert total == n_produced, f"회수 {total} != 생산 {n_produced} (유실/중복)"

    for stream in ("events", "orders", "payments"):
        exp, act = expected.by_stream[stream], actual[stream]
        assert set(act) == set(exp), f"{stream} id 집합 불일치"
        for _id, (exp_key, exp_val) in exp.items():
            act_key, act_val = act[_id]
            assert act_val == exp_val, f"{stream} 페이로드 불일치: {_id}"
            assert act_key == exp_key.encode("utf-8"), f"{stream} 키 불일치: {_id}"

    # 참조 무결성(중개를 거쳐도 FK 가 보존되는지). 결제는 주문보다, 주문은 purchase
    # 이벤트보다 늦지 않게 방출되므로 윈도우 절단과 무관하게 부분집합 관계가 성립한다.
    order_ids = set(actual["orders"])
    payment_order_ids = {v["order_id"] for _k, v in actual["payments"].values()}
    purchase_order_ids = {
        v["order_id"] for _k, v in actual["events"].values() if v["event_type"] == "purchase"
    }
    assert payment_order_ids <= order_ids, "회수된 결제가 참조하는 주문이 누락됨"
    assert order_ids <= purchase_order_ids, "회수된 주문에 대응하는 purchase 이벤트가 누락됨"

    # 키 파티셔닝: events=user_id, orders/payments=order_id
    for _id, (key, val) in actual["events"].items():
        assert key == str(val["user_id"]).encode("utf-8")
    for _id, (key, val) in actual["orders"].items():
        assert key == val["order_id"].encode("utf-8")
    for _id, (key, val) in actual["payments"].items():
        assert key == val["order_id"].encode("utf-8")
