"""kafka 싱크 — 스트림별 토픽으로 produce.

토픽명 = "{topic_prefix}.{stream}"  예) ecom.events / ecom.orders / ecom.payments
key(보통 user_id 또는 order_id)로 파티셔닝되어 같은 엔티티의 이벤트 순서가 보존된다.

`confluent-kafka` extra 가 필요하다:  uv sync --extra kafka
로컬 브로커는 저장소의 docker-compose.yml 로 띄운다:
    docker compose up -d
    uv run stream-data --sink kafka --duration 30

테스트 용이성: 실제 Producer 대신 호환 객체를 주입할 수 있다(producer 인자).
이로써 브로커 없이도 싱크 로직(토픽명·인코딩·버퍼 처리·flush)을 단위 검증한다.
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any


def _default_producer(bootstrap_servers: str) -> Any:
    """confluent-kafka Producer 생성(지연 임포트). 브로커에 즉시 연결하진 않는다."""
    try:
        from confluent_kafka import Producer
    except ImportError as e:  # pragma: no cover - 환경 의존
        raise ImportError(
            "kafka 싱크에는 confluent-kafka 가 필요합니다. "
            "`uv sync --extra kafka` 후 다시 실행하세요."
        ) from e
    # linger.ms 로 약간 배칭해 처리량을 올린다. 단일노드 학습용 기본 acks(=1).
    return Producer({"bootstrap.servers": bootstrap_servers, "linger.ms": 50})


class KafkaSink:
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic_prefix: str = "ecom",
        *,
        producer: Any | None = None,
    ):
        self.topic_prefix = topic_prefix
        # 전달 결과 집계(콜백 기반). close() 시 요약 로그 대신 외부에서 조회 가능.
        self.delivered = 0
        self.failed = 0
        self._producer = producer if producer is not None else _default_producer(bootstrap_servers)

    def _on_delivery(self, err: Any, _msg: Any) -> None:
        if err is not None:
            self.failed += 1
        else:
            self.delivered += 1

    def emit(self, stream: str, key: str, value: dict, ts: datetime) -> None:
        topic = f"{self.topic_prefix}.{stream}"
        payload = json.dumps(value, ensure_ascii=False).encode("utf-8")
        k = key.encode("utf-8")
        try:
            self._producer.produce(topic, key=k, value=payload, on_delivery=self._on_delivery)
        except BufferError:
            # 내부 송신 큐 포화 → 전달 콜백을 처리해 큐를 비우고 1회 재시도
            self._producer.poll(1.0)
            self._producer.produce(topic, key=k, value=payload, on_delivery=self._on_delivery)
        # 누적된 전달 콜백을 비차단으로 처리
        self._producer.poll(0)

    def flush(self) -> None:
        # 남은 메시지를 모두 전송 완료할 때까지 블록
        self._producer.flush()

    def close(self) -> None:
        self.flush()
