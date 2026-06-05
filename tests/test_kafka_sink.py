"""KafkaSink 단위 테스트 — 브로커 없이 가짜 Producer 주입으로 검증.

토픽명/키/값 인코딩, 버퍼 포화 재시도, 전달 집계, flush/close 를 확인한다.
실브로커 왕복은 tests/integration/test_kafka_roundtrip.py(옵션) 가 담당한다.
"""
from __future__ import annotations

import json
from datetime import datetime

from realtime_generator.sinks.kafka import KafkaSink

TS = datetime(2025, 1, 1, 12, 0, 0)


class FakeProducer:
    """confluent-kafka Producer 의 최소 호환 더블."""

    def __init__(self, fail_first_buffer: bool = False):
        self.produced: list[tuple[str, bytes, bytes]] = []
        self.flushed = False
        self.poll_calls = 0
        self._fail_first_buffer = fail_first_buffer

    def produce(self, topic, key=None, value=None, on_delivery=None):
        if self._fail_first_buffer:
            self._fail_first_buffer = False
            raise BufferError("queue full")
        self.produced.append((topic, key, value))
        if on_delivery is not None:
            on_delivery(None, object())  # 전달 성공 시뮬레이션

    def poll(self, timeout=0):
        self.poll_calls += 1
        return 0

    def flush(self, *args):
        self.flushed = True
        return 0


def test_topic_name_and_encoding():
    fp = FakeProducer()
    sink = KafkaSink(topic_prefix="ecom", producer=fp)
    sink.emit("events", "u42", {"event_type": "page_view", "user_id": 42}, TS)

    topic, key, value = fp.produced[0]
    assert topic == "ecom.events"
    assert key == b"u42"
    assert json.loads(value.decode("utf-8")) == {"event_type": "page_view", "user_id": 42}


def test_custom_prefix():
    fp = FakeProducer()
    KafkaSink(topic_prefix="shop", producer=fp).emit("orders", "o1", {"x": 1}, TS)
    assert fp.produced[0][0] == "shop.orders"


def test_delivery_accounting():
    fp = FakeProducer()
    sink = KafkaSink(producer=fp)
    for i in range(5):
        sink.emit("events", f"u{i}", {"n": i}, TS)
    sink.flush()
    assert sink.delivered == 5
    assert sink.failed == 0


def test_buffer_error_is_retried():
    # 첫 produce 가 BufferError → poll 로 비우고 재시도해 결국 1건이 적재돼야 함
    fp = FakeProducer(fail_first_buffer=True)
    sink = KafkaSink(producer=fp)
    sink.emit("events", "u1", {"n": 1}, TS)
    assert len(fp.produced) == 1
    assert fp.poll_calls >= 1


def test_close_flushes():
    fp = FakeProducer()
    sink = KafkaSink(producer=fp)
    sink.emit("payments", "o1", {"n": 1}, TS)
    sink.close()
    assert fp.flushed is True


def test_make_sink_builds_kafka_without_broker(monkeypatch):
    # make_sink('kafka') 가 confluent Producer 를 만들되 브로커 연결은 지연됨을 확인.
    from realtime_generator.sinks import kafka as kafka_mod

    created = {}

    def fake_default(bootstrap):
        created["bootstrap"] = bootstrap
        return FakeProducer()

    monkeypatch.setattr(kafka_mod, "_default_producer", fake_default)
    from realtime_generator.sinks import make_sink

    sink = make_sink({"type": "kafka", "kafka": {"bootstrap_servers": "h:9092", "topic_prefix": "p"}})
    assert isinstance(sink, KafkaSink)
    assert created["bootstrap"] == "h:9092"
    assert sink.topic_prefix == "p"
