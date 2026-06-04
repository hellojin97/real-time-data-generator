"""싱크: stdout JSONL·file 롤링·팩토리."""
from __future__ import annotations

import json
from datetime import datetime

import pytest

from realtime_generator.sinks import make_sink
from realtime_generator.sinks.file import FileSink
from realtime_generator.sinks.stdout import StdoutSink

TS = datetime(2025, 1, 1, 12, 0, 0)


def test_stdout_sink_emits_jsonl(capsys):
    sink = StdoutSink()
    sink.emit("events", "u1", {"event_type": "page_view", "user_id": 1}, TS)
    sink.close()
    out = capsys.readouterr().out.strip()
    rec = json.loads(out)
    assert rec["stream"] == "events"
    assert rec["key"] == "u1"
    assert rec["value"]["event_type"] == "page_view"


def test_file_sink_rolls_files(tmp_path):
    sink = FileSink(out_dir=tmp_path, rows_per_file=2)
    for i in range(5):
        sink.emit("events", f"u{i}", {"n": i}, TS)
    sink.close()

    files = sorted((tmp_path / "events").glob("events-*.jsonl"))
    # 5건 / 2 = 3개 파일 (2,2,1)
    assert len(files) == 3
    line_counts = [len(f.read_text(encoding="utf-8").splitlines()) for f in files]
    assert line_counts == [2, 2, 1]


def test_file_sink_separates_streams(tmp_path):
    sink = FileSink(out_dir=tmp_path, rows_per_file=100)
    sink.emit("events", "u1", {"n": 1}, TS)
    sink.emit("orders", "o1", {"n": 2}, TS)
    sink.close()
    assert (tmp_path / "events").is_dir()
    assert (tmp_path / "orders").is_dir()


def test_make_sink_factory():
    assert isinstance(make_sink({"type": "stdout"}), StdoutSink)
    assert isinstance(make_sink({"type": "file", "file": {"out_dir": "/tmp/x"}}), FileSink)
    with pytest.raises(ValueError):
        make_sink({"type": "nope"})
