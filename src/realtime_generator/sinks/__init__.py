"""플러그인형 출력 싱크.

엔진은 Sink 인터페이스에만 의존한다. 같은 생성 로직을 stdout/file/kafka 어디로든
흘려보낼 수 있게 하는 것이 이 추상화의 목적이다.

레코드 계약: value 는 이미 JSON-직렬화 가능한 dict(타임스탬프는 ISO 문자열)다.
싱크는 이를 그대로 내보내기만 하면 된다.
"""
from __future__ import annotations

from datetime import datetime
from typing import Protocol, runtime_checkable


@runtime_checkable
class Sink(Protocol):
    """스트림 레코드 출력 대상.

    - emit: 레코드 1건 출력. stream 은 논리 스트림명(events/orders/payments),
            key 는 파티셔닝/정렬 키(보통 user_id 또는 order_id).
    - flush: 버퍼 비우기.  - close: 자원 정리(파일/커넥션).
    """

    def emit(self, stream: str, key: str, value: dict, ts: datetime) -> None: ...

    def flush(self) -> None: ...

    def close(self) -> None: ...


def make_sink(cfg: dict) -> Sink:
    """설정의 sink.type 에 따라 적절한 싱크를 만든다.

    cfg 는 config.yml 의 `sink` 블록 전체({type, file, kafka, ...}).
    kafka 는 외부 의존성이 필요하므로 지연 임포트한다.
    """
    sink_type = cfg.get("type", "stdout")

    if sink_type == "stdout":
        from .stdout import StdoutSink
        return StdoutSink()

    if sink_type == "file":
        from .file import FileSink
        fcfg = cfg.get("file", {})
        return FileSink(
            out_dir=fcfg.get("out_dir", "./output/stream"),
            rows_per_file=int(fcfg.get("rows_per_file", 10000)),
        )

    if sink_type == "kafka":
        from .kafka import KafkaSink
        kcfg = cfg.get("kafka", {})
        return KafkaSink(
            bootstrap_servers=kcfg.get("bootstrap_servers", "localhost:9092"),
            topic_prefix=kcfg.get("topic_prefix", "ecom"),
        )

    raise ValueError(f"알 수 없는 sink type: {sink_type!r} (stdout|file|kafka 중 하나)")
