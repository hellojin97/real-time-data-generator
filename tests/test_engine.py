"""엔진: 결정론(재현성)·종료조건·시각 단조성."""
from __future__ import annotations

from datetime import datetime

from realtime_generator import dimensions
from realtime_generator.clock import SimulatedClock
from realtime_generator.engine import Engine
from realtime_generator.traffic import TrafficModel

from .conftest import CollectingSink

SIM_START = datetime(2025, 1, 1, 0, 0, 0)


def _run(seed: int, max_events: int, conversion_rate: float = 0.1):
    pool = dimensions.generate(n_users=1000, n_products=200, seed=seed)
    sink = CollectingSink()
    engine = Engine(
        pool,
        TrafficModel(sessions_per_sec=5.0),
        sink,
        SimulatedClock(SIM_START),
        seed=seed,
        conversion_rate=conversion_rate,
        null_rate_search=0.1,
    )
    emitted = engine.run(max_events=max_events)
    return emitted, sink.records


def test_simulated_run_is_deterministic():
    _, a = _run(42, 800)
    _, b = _run(42, 800)
    # (stream, key, value, ts) 전체가 동일해야 한다
    assert a == b


def test_different_seed_changes_stream():
    _, a = _run(42, 800)
    _, b = _run(7, 800)
    assert a != b


def test_max_events_is_honored():
    emitted, records = _run(1, 500)
    assert emitted == 500
    assert len(records) == 500


def test_emitted_timestamps_are_non_decreasing():
    _, records = _run(3, 1000)
    ts = [r[3] for r in records]
    assert ts == sorted(ts)


def test_conversion_produces_orders_and_payments():
    # 전환율을 높여 주문/결제가 확실히 나오도록
    _, records = _run(2, 2000, conversion_rate=0.5)
    streams = {r[0] for r in records}
    assert "orders" in streams
    assert "payments" in streams


def test_duration_stop_in_simulated_clock():
    pool = dimensions.generate(n_users=500, n_products=100, seed=9)
    sink = CollectingSink()
    engine = Engine(
        pool, TrafficModel(sessions_per_sec=5.0), sink, SimulatedClock(SIM_START),
        seed=9, conversion_rate=0.1, null_rate_search=0.1,
    )
    engine.run(max_duration_s=30.0)
    # 모든 방출 시각은 시작 + 30초 이내
    assert all((r[3] - SIM_START).total_seconds() <= 30.0 for r in sink.records)
