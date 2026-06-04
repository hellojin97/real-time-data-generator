"""세션 계획: 퍼널 구조·전환·FK 정합성."""
from __future__ import annotations

from datetime import datetime

from realtime_generator import dimensions
from realtime_generator.base import make_rng
from realtime_generator.sessions import PURCHASE_FUNNEL, plan_session

START = datetime(2025, 1, 1, 20, 0, 0)  # 저녁 피크


def _pool():
    return dimensions.generate(n_users=200, n_products=80, seed=11)


def test_purchase_session_has_full_funnel_order_and_payment():
    pool = _pool()
    rng = make_rng(123)
    recs = plan_session(
        pool, rng, 0, START, conversion_rate=1.0, null_rate_search=0.0,
    )
    streams = [r.stream for r in recs]
    assert streams.count("events") == len(PURCHASE_FUNNEL)
    assert streams.count("orders") == 1
    assert streams.count("payments") == 1

    # 퍼널 이벤트 타입 순서가 고정 5단계와 일치
    event_types = [r.value["event_type"] for r in recs if r.stream == "events"]
    assert event_types == PURCHASE_FUNNEL

    # purchase 이벤트가 order_id 를 참조하고, order/payment 와 동일 order_id
    purchase = next(r.value for r in recs if r.value.get("event_type") == "purchase")
    order = next(r.value for r in recs if r.stream == "orders")
    payment = next(r.value for r in recs if r.stream == "payments")
    assert purchase["order_id"] == order["order_id"] == payment["order_id"]


def test_order_amount_matches_line_items():
    pool = _pool()
    rng = make_rng(7)
    recs = plan_session(pool, rng, 1, START, conversion_rate=1.0, null_rate_search=0.0)
    order = next(r.value for r in recs if r.stream == "orders")
    expected = round(sum(i["qty"] * i["unit_price_usd"] for i in order["items"]), 2)
    assert order["amount_usd"] == expected
    assert order["amount_local"] == round(expected * order["fx_rate"], 2)


def test_browse_session_has_only_events():
    pool = _pool()
    rng = make_rng(99)
    recs = plan_session(pool, rng, 2, START, conversion_rate=0.0, null_rate_search=0.0)
    assert all(r.stream == "events" for r in recs)
    assert all(r.value.get("order_id") is None for r in recs)
    assert 1 <= len(recs) <= 8


def test_plan_is_reproducible():
    pool = _pool()
    r1 = plan_session(pool, make_rng(5), 3, START,
                      conversion_rate=0.5, null_rate_search=0.1)
    r2 = plan_session(pool, make_rng(5), 3, START,
                      conversion_rate=0.5, null_rate_search=0.1)
    assert [r.value for r in r1] == [r.value for r in r2]
