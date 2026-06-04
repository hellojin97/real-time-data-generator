"""세션/퍼널 상태머신 — 세션 1개의 '이벤트 계획'을 만든다.

엔진은 세션 도착 시각(start_ts)을 정해 넘기고, 여기서 그 세션이 만들 모든
레코드를 (emit 시각, stream, key, value)로 펼쳐 돌려준다. 각 레코드의 논리 시각은
곧 그 레코드가 흘러나갈 시각(start_ts + offset)과 같다. 두 종류의 세션:

- 구매 세션(전환): page_view → product_view → add_to_cart → begin_checkout → purchase.
  purchase 이벤트가 order_id 를 참조하고, 같은 시각에 order, 직후 payment 를 낸다.
- 브라우징 세션(이탈): view/search/cart 위주의 짧은 탐색. 구매로 이어지지 않는다.

전환율 = 구매 세션 / 전체 세션 을 현실치(2~5%)로 맞춰 퍼널 분석이 가능하다.
재현성: 모든 추출은 엔진이 넘긴 단일 rng 로 이뤄지며 draw 순서가 고정돼 있다.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import NamedTuple

import numpy as np

from .dimensions import DimensionPool
from .records import build_event, build_order, build_payment

# 구매 세션 퍼널(고정 5단계)과 상품을 참조하는 단계
PURCHASE_FUNNEL = ["page_view", "product_view", "add_to_cart", "begin_checkout", "purchase"]
_PRODUCT_STEPS = {"product_view", "add_to_cart", "purchase"}

# 브라우징 세션 이벤트 타입과 가중치(탐색 위주, 깊은 퍼널은 드물게)
BROWSE_EVENT_TYPES = ["page_view", "product_view", "search", "add_to_cart", "begin_checkout"]
BROWSE_EVENT_WEIGHTS = [0.30, 0.30, 0.20, 0.13, 0.07]
_BROWSE_PRODUCT_TYPES = {"product_view", "add_to_cart", "begin_checkout"}

# search 이벤트용 정적 쿼리 풀(현실감)
SEARCH_QUERIES = [
    "laptop", "wireless earbuds", "gaming chair", "4k tv", "smartphone",
    "running shoes", "winter jacket", "office desk", "coffee maker", "air fryer",
    "yoga mat", "backpack", "sunglasses", "bluetooth speaker", "monitor",
    "mechanical keyboard", "desk lamp", "water bottle", "headphones", "sneakers",
]

# 결제 수단/상태 분포
PAYMENT_METHODS = ["card", "paypal", "bank_transfer"]
PAYMENT_METHOD_WEIGHTS = [0.70, 0.20, 0.10]
PAYMENT_STATUSES = ["approved", "failed", "refunded"]
PAYMENT_STATUS_WEIGHTS = [0.92, 0.06, 0.02]

# 이벤트 간 간격(초)
_PURCHASE_GAP = (5.0, 180.0)   # 구매 퍼널 단계 간
_BROWSE_GAP = (10.0, 300.0)    # 브라우징 이벤트 간
_PAY_GAP = (2.0, 30.0)         # 주문 → 결제 승인까지

# 브라우징 세션 이벤트 수 상한 기본값
DEFAULT_MAX_BROWSE_EVENTS = 8


class PlannedRecord(NamedTuple):
    """특정 시각에 흘려보낼 레코드 1건."""

    ts: datetime
    stream: str   # events | orders | payments
    key: str
    value: dict


def plan_session(
    pool: DimensionPool,
    rng: np.random.Generator,
    session_seq: int,
    start_ts: datetime,
    *,
    conversion_rate: float,
    null_rate_search: float,
    max_browse_events: int = DEFAULT_MAX_BROWSE_EVENTS,
) -> list[PlannedRecord]:
    """세션 1개를 계획해 시간순 PlannedRecord 목록을 반환."""
    user_idx = pool.pick_user(rng)
    user_id = int(pool.user_ids[user_idx])
    currency = str(pool.user_currency[user_idx])
    session_id = f"sess-{session_seq:08d}"

    if rng.random() < conversion_rate:
        return _plan_purchase(
            pool, rng, session_seq, session_id, user_id, currency, start_ts,
        )
    return _plan_browse(
        pool, rng, session_seq, session_id, user_id, start_ts,
        null_rate_search=null_rate_search, max_browse_events=max_browse_events,
    )


def _plan_purchase(
    pool: DimensionPool,
    rng: np.random.Generator,
    session_seq: int,
    session_id: str,
    user_id: int,
    currency: str,
    start_ts: datetime,
) -> list[PlannedRecord]:
    hero_idx = pool.pick_product(rng)
    hero_id = int(pool.product_ids[hero_idx])
    order_id = f"ord-{session_seq:08d}"

    records: list[PlannedRecord] = []
    offset = 0.0
    purchase_ts = start_ts
    for pos, etype in enumerate(PURCHASE_FUNNEL):
        if pos > 0:
            offset += float(rng.uniform(*_PURCHASE_GAP))
        ts = start_ts + timedelta(seconds=offset)
        is_purchase = etype == "purchase"
        if is_purchase:
            purchase_ts = ts
        event = build_event(
            event_id=f"evt-{session_seq:08d}-{pos:03d}",
            event_type=etype,
            ts=ts,
            user_id=user_id,
            session_id=session_id,
            product_id=hero_id if etype in _PRODUCT_STEPS else None,
            order_id=order_id if is_purchase else None,
        )
        records.append(PlannedRecord(ts, "events", str(user_id), event))

    # 주문 라인아이템: hero + 추가 활성 상품 0~2개
    n_extra = int(rng.integers(0, 3))
    item_idxs = [hero_idx] + [pool.pick_product(rng) for _ in range(n_extra)]
    items = [
        {
            "product_id": int(pool.product_ids[idx]),
            "qty": int(rng.integers(1, 4)),
            "unit_price_usd": round(float(pool.product_price[idx]), 2),
        }
        for idx in item_idxs
    ]
    order = build_order(
        order_id=order_id, ts=purchase_ts, user_id=user_id,
        session_id=session_id, currency=currency, items=items,
    )
    records.append(PlannedRecord(purchase_ts, "orders", order_id, order))

    # 결제: 주문 직후. 일부 실패/환불.
    pay_ts = purchase_ts + timedelta(seconds=float(rng.uniform(*_PAY_GAP)))
    method = str(rng.choice(PAYMENT_METHODS, p=PAYMENT_METHOD_WEIGHTS))
    status = str(rng.choice(PAYMENT_STATUSES, p=PAYMENT_STATUS_WEIGHTS))
    payment = build_payment(
        payment_id=f"pay-{session_seq:08d}", order_id=order_id, ts=pay_ts,
        method=method, status=status, amount_usd=order["amount_usd"], currency=currency,
    )
    records.append(PlannedRecord(pay_ts, "payments", order_id, payment))

    return records


def _plan_browse(
    pool: DimensionPool,
    rng: np.random.Generator,
    session_seq: int,
    session_id: str,
    user_id: int,
    start_ts: datetime,
    *,
    null_rate_search: float,
    max_browse_events: int,
) -> list[PlannedRecord]:
    n_events = int(rng.integers(1, max_browse_events + 1))
    records: list[PlannedRecord] = []
    offset = 0.0
    for pos in range(n_events):
        if pos > 0:
            offset += float(rng.uniform(*_BROWSE_GAP))
        ts = start_ts + timedelta(seconds=offset)
        etype = str(rng.choice(BROWSE_EVENT_TYPES, p=BROWSE_EVENT_WEIGHTS))
        product_id = None
        search_query = None
        if etype in _BROWSE_PRODUCT_TYPES:
            product_id = int(pool.product_ids[pool.pick_product(rng)])
        elif etype == "search" and rng.random() >= null_rate_search:
            search_query = str(rng.choice(SEARCH_QUERIES))
        event = build_event(
            event_id=f"evt-{session_seq:08d}-{pos:03d}",
            event_type=etype,
            ts=ts,
            user_id=user_id,
            session_id=session_id,
            product_id=product_id,
            search_query=search_query,
        )
        records.append(PlannedRecord(ts, "events", str(user_id), event))
    return records
