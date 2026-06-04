"""kafka 싱크 — 스트림별 토픽으로 produce. (Phase 2)

토픽명 = "{topic_prefix}.{stream}"  예) ecom.events / ecom.orders / ecom.payments
key 로 파티셔닝되어 같은 user/order 의 이벤트 순서가 보존된다.

`confluent-kafka` extra 가 필요하다:  uv sync --extra kafka
로컬 브로커는 저장소의 docker-compose.yml 로 띄울 수 있다(Phase 2에서 추가 예정).
"""
from __future__ import annotations

import json
from datetime import datetime


class KafkaSink:
    def __init__(self, bootstrap_servers: str = "localhost:9092", topic_prefix: str = "ecom"):
        try:
            from confluent_kafka import Producer
        except ImportError as e:  # pragma: no cover - 환경 의존
            raise ImportError(
                "kafka 싱크에는 confluent-kafka 가 필요합니다. "
                "`uv sync --extra kafka` 후 다시 실행하세요."
            ) from e

        self.topic_prefix = topic_prefix
        self._producer = Producer({"bootstrap.servers": bootstrap_servers})

    def emit(self, stream: str, key: str, value: dict, ts: datetime) -> None:
        topic = f"{self.topic_prefix}.{stream}"
        self._producer.produce(
            topic,
            key=key.encode("utf-8"),
            value=json.dumps(value, ensure_ascii=False).encode("utf-8"),
        )
        # 비차단: 내부 큐가 차면 일부 콜백을 처리
        self._producer.poll(0)

    def flush(self) -> None:
        self._producer.flush()

    def close(self) -> None:
        self.flush()
