"""테스트 공용 픽스처/헬퍼."""
from __future__ import annotations

from datetime import datetime

from realtime_generator.sinks import Sink


class CollectingSink(Sink):
    """방출 레코드를 메모리에 모으는 테스트용 싱크."""

    def __init__(self) -> None:
        self.records: list[tuple[str, str, dict, datetime]] = []

    def emit(self, stream: str, key: str, value: dict, ts: datetime) -> None:
        self.records.append((stream, key, value, ts))

    def flush(self) -> None:
        pass

    def close(self) -> None:
        pass
