"""stdout 싱크 — 레코드를 JSONL 한 줄씩 표준출력으로.

가장 가벼운 싱크. `stream-data | jq` 처럼 파이프로 바로 관찰하거나,
다른 프로세스로 넘기기에 좋다.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime


class StdoutSink:
    def emit(self, stream: str, key: str, value: dict, ts: datetime) -> None:
        # 어느 스트림(events/orders/payments)인지 함께 실어 한 줄 JSON 으로 출력
        line = json.dumps({"stream": stream, "key": key, "value": value}, ensure_ascii=False)
        sys.stdout.write(line + "\n")

    def flush(self) -> None:
        sys.stdout.flush()

    def close(self) -> None:
        self.flush()
