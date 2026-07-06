"""지각(late) 이벤트 주입 — out-of-order 방출·지연 상한·재현성·비활성 시 무영향."""
from __future__ import annotations

from datetime import datetime

from realtime_generator import dimensions
from realtime_generator.clock import SimulatedClock
from realtime_generator.engine import Engine
from realtime_generator.traffic import TrafficModel

from .conftest import CollectingSink

SIM_START = datetime(2025, 1, 1, 0, 0, 0)

# 페이로드 타임스탬프는 초 단위 ISO 문자열로 절단되므로(records._iso),
# 배달시각 - 이벤트시각 비교에는 1초의 절단 여유를 둔다.
_ISO_TRUNCATION_S = 1.0


def _run(seed: int, max_events: int, **late_kwargs):
    pool = dimensions.generate(n_users=1000, n_products=200, seed=seed)
    sink = CollectingSink()
    engine = Engine(
        pool,
        TrafficModel(sessions_per_sec=5.0),
        sink,
        SimulatedClock(SIM_START),
        seed=seed,
        conversion_rate=0.1,
        null_rate_search=0.1,
        **late_kwargs,
    )
    engine.run(max_events=max_events)
    return sink.records


def _event_ts(value: dict) -> datetime:
    """스트림별 이벤트타임 필드(event_ts/order_ts/payment_ts)를 파싱."""
    raw = value.get("event_ts") or value.get("order_ts") or value.get("payment_ts")
    return datetime.fromisoformat(raw)


def test_disabled_lateness_preserves_baseline_stream():
    # late_rate=0(기본)이면 rng 를 소모하지 않아 지각 인자 없는 기존 스트림과 동일
    baseline = _run(42, 500)
    explicit = _run(42, 500, late_rate=0.0, late_max_delay_s=300.0)
    assert baseline == explicit


def test_disabled_lateness_delivers_at_event_time():
    records = _run(42, 500, late_rate=0.0)
    for _, _, value, delivery in records:
        lag = (delivery - _event_ts(value)).total_seconds()
        assert 0.0 <= lag < _ISO_TRUNCATION_S


def test_late_injection_creates_out_of_order_stream():
    records = _run(42, 800, late_rate=0.5, late_max_delay_s=300.0)
    delivery = [r[3] for r in records]
    assert delivery == sorted(delivery)  # 방출(배달) 순서는 여전히 단조
    event_ts = [_event_ts(r[2]) for r in records]
    assert event_ts != sorted(event_ts)  # 이벤트타임은 뒤섞여 있어야 한다


def test_late_payload_is_untouched():
    # 지각 추출은 별도 rng(seed+2)라 배달 시각만 미뤄지고 콘텐츠는 동일해야 한다.
    # 같은 시드의 지각 없는 실행이 정답지(ground truth)가 되는 성질의 검증.
    def key(v: dict) -> str:
        return v.get("payment_id") or (
            v.get("order_id") if "items" in v else None) or v["event_id"]

    plain = {key(r[2]): r[2] for r in _run(42, 500, late_rate=0.0)}
    late = {key(r[2]): r[2] for r in _run(42, 500, late_rate=1.0, late_max_delay_s=120.0)}
    # max_events 경계에서 배달 순서가 바뀌어 방출 부분집합이 조금 다를 수 있다 —
    # 대부분 겹치고, 겹치는 레코드의 페이로드는 완전히 동일해야 한다.
    common = plain.keys() & late.keys()
    assert len(common) > 400
    assert all(plain[k] == late[k] for k in common)


def test_late_delay_is_bounded():
    max_delay = 120.0
    records = _run(7, 800, late_rate=1.0, late_max_delay_s=max_delay)
    for _, _, value, delivery in records:
        lag = (delivery - _event_ts(value)).total_seconds()
        assert 0.0 <= lag < max_delay + _ISO_TRUNCATION_S


def test_late_stream_is_reproducible():
    a = _run(42, 500, late_rate=0.3, late_max_delay_s=120.0)
    b = _run(42, 500, late_rate=0.3, late_max_delay_s=120.0)
    assert a == b
