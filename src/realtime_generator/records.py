"""레코드 빌더 — event / order / payment 의 JSON-직렬화 가능한 dict 생성.

타임스탬프는 ISO8601 문자열로 고정해 싱크가 그대로 내보낼 수 있게 한다.
금액은 USD 기준으로 계산하고, 유저 통화로 환산한 amount_local 을 함께 싣는다
(배치 생성기의 fx_rate/amount_local 현실성을 계승).
"""
from __future__ import annotations

from datetime import datetime

# USD 기준 환율(현지통화 = USD × rate). 학습용 근사치.
FX_RATE = {"USD": 1.0, "KRW": 1350.0, "JPY": 150.0, "GBP": 0.79, "EUR": 0.92}


def _iso(ts: datetime) -> str:
    return ts.isoformat(timespec="seconds")


def build_event(
    *,
    event_id: str,
    event_type: str,
    ts: datetime,
    user_id: int,
    session_id: str,
    product_id: int | None = None,
    search_query: str | None = None,
    order_id: str | None = None,
) -> dict:
    """클릭스트림 이벤트 1건."""
    return {
        "event_id": event_id,
        "event_type": event_type,
        "event_ts": _iso(ts),
        "user_id": user_id,
        "session_id": session_id,
        "product_id": product_id,
        "search_query": search_query,
        "order_id": order_id,
    }


def build_order(
    *,
    order_id: str,
    ts: datetime,
    user_id: int,
    session_id: str,
    currency: str,
    items: list[dict],
) -> dict:
    """주문 1건. items = [{product_id, qty, unit_price_usd}, ...]."""
    amount_usd = round(sum(i["qty"] * i["unit_price_usd"] for i in items), 2)
    fx = FX_RATE.get(currency, 1.0)
    return {
        "order_id": order_id,
        "user_id": user_id,
        "session_id": session_id,
        "order_ts": _iso(ts),
        "status": "created",
        "currency": currency,
        "amount_usd": amount_usd,
        "amount_local": round(amount_usd * fx, 2),
        "fx_rate": fx,
        "items": items,
    }


def build_payment(
    *,
    payment_id: str,
    order_id: str,
    ts: datetime,
    method: str,
    status: str,
    amount_usd: float,
    currency: str,
) -> dict:
    """결제 1건. status = approved | failed | refunded."""
    fx = FX_RATE.get(currency, 1.0)
    return {
        "payment_id": payment_id,
        "order_id": order_id,
        "payment_ts": _iso(ts),
        "method": method,
        "status": status,
        "amount_usd": amount_usd,
        "currency": currency,
        "amount_local": round(amount_usd * fx, 2),
    }
