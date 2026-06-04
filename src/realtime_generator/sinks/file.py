"""file 싱크 — 스트림별 JSONL 파일로 롤링 기록.

out_dir/
├── events/   events-00000.jsonl, events-00001.jsonl, ...
├── orders/   orders-00000.jsonl, ...
└── payments/ payments-00000.jsonl, ...

rows_per_file 마다 새 파일로 회전한다. Spark Auto Loader / 파일 tail 등으로
소비하기 좋은 형태다.
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import TextIO


class FileSink:
    def __init__(self, out_dir: str | Path, rows_per_file: int = 10000):
        self.out_dir = Path(out_dir)
        self.rows_per_file = max(1, rows_per_file)
        # 스트림별 상태: {stream: [열린 파일핸들, 현재 파일 행수, 파일 시퀀스]}
        self._handles: dict[str, TextIO] = {}
        self._counts: dict[str, int] = {}
        self._seq: dict[str, int] = {}

    def _open_next(self, stream: str) -> TextIO:
        seq = self._seq.get(stream, 0)
        stream_dir = self.out_dir / stream
        stream_dir.mkdir(parents=True, exist_ok=True)
        path = stream_dir / f"{stream}-{seq:05d}.jsonl"
        handle = open(path, "w", encoding="utf-8")
        self._handles[stream] = handle
        self._counts[stream] = 0
        self._seq[stream] = seq + 1
        return handle

    def emit(self, stream: str, key: str, value: dict, ts: datetime) -> None:
        handle = self._handles.get(stream)
        if handle is None or self._counts[stream] >= self.rows_per_file:
            if handle is not None:
                handle.close()
            handle = self._open_next(stream)
        handle.write(json.dumps({"key": key, "value": value}, ensure_ascii=False) + "\n")
        self._counts[stream] += 1

    def flush(self) -> None:
        for handle in self._handles.values():
            handle.flush()

    def close(self) -> None:
        for handle in self._handles.values():
            handle.close()
        self._handles.clear()
