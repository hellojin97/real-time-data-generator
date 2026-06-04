"""차원(users / products) 풀.

스트림이 참조할 회원/상품 마스터를 보관한다. 두 가지 소스를 지원한다:
- generate(): 시작 시 시드로 메모리에 생성 (단독 실행, 기본값)
- load_parquet(): 배치 생성기(ecommerce-data-generator) 산출물 재사용 (옵션)

설계 메모: 풀 생성은 전용 rng(seed)로 1회 수행해 결정론적이다. 스트리밍 중의
유저/상품 추출은 엔진이 보유한 별도 rng(seed+1)로 이뤄진다 — 트래픽 설정이 바뀌어도
풀 구성은 고정되도록 분리했다.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np

from .base import make_rng

# 회원 세그먼트와 가입 분포 가중치
SEGMENTS = ["new", "regular", "vip", "churned"]
SEGMENT_WEIGHTS = [0.25, 0.50, 0.10, 0.15]
# 세그먼트별 브라우징 활성도(세션 유저 선택 가중치). 배치 SEGMENT_BROWSE_MULT 와 동일 의도.
SEGMENT_ACTIVITY = {"new": 0.8, "regular": 1.0, "vip": 2.5, "churned": 0.2}

# 국가 → 통화. 주문 금액의 현지통화 환산에 사용.
COUNTRY_CURRENCY = {
    "US": "USD", "KR": "KRW", "JP": "JPY",
    "GB": "GBP", "DE": "EUR", "FR": "EUR",
}
COUNTRIES = list(COUNTRY_CURRENCY.keys())
COUNTRY_WEIGHTS = [0.40, 0.15, 0.12, 0.13, 0.10, 0.10]

PRODUCT_CATEGORIES = [
    "electronics", "computers", "fashion", "home", "beauty",
    "sports", "books", "toys", "grocery", "automotive",
]


@dataclass
class DimensionPool:
    """차원 마스터 + 추출 헬퍼.

    배열은 인덱스 정렬되어 있어 user_ids[i] 의 속성은 user_segment[i] 등으로 접근한다.
    """

    user_ids: np.ndarray
    user_segment: np.ndarray
    user_country: np.ndarray
    user_currency: np.ndarray
    _user_weights: np.ndarray  # 세션 유저 선택 확률(활성도 기반, 정규화됨)

    product_ids: np.ndarray
    product_price: np.ndarray      # USD 기준 단가
    product_category: np.ndarray
    product_active: np.ndarray     # 단종이 아니어서 추천/탐색에 노출되는지
    _active_idx: np.ndarray        # product_active=True 인 행 인덱스
    _active_weights: np.ndarray    # 활성 상품 인기 가중치(정규화됨)

    @property
    def n_users(self) -> int:
        return len(self.user_ids)

    @property
    def n_products(self) -> int:
        return len(self.product_ids)

    def pick_user(self, rng: np.random.Generator) -> int:
        """활성도 가중으로 세션 주인 1명을 고른다. 배열 인덱스를 반환."""
        return int(rng.choice(self.n_users, p=self._user_weights))

    def pick_product(self, rng: np.random.Generator) -> int:
        """인기 가중으로 활성 상품 1개를 고른다. 배열 인덱스를 반환."""
        return int(rng.choice(self._active_idx, p=self._active_weights))


def _finalize(
    user_ids, user_segment, user_country, user_currency,
    product_ids, product_price, product_category, product_active,
) -> DimensionPool:
    """원시 배열로부터 추출 가중치를 계산해 DimensionPool 을 조립."""
    activity = np.array([SEGMENT_ACTIVITY[s] for s in user_segment], dtype=float)
    user_weights = activity / activity.sum()

    active_idx = np.flatnonzero(product_active)
    # 인기 가중치: 활성 상품에 멱법칙(power-law)풍 분포를 부여해 일부 '핫템'을 만든다.
    rank = np.arange(1, len(active_idx) + 1, dtype=float)
    pop = 1.0 / rank
    active_weights = pop / pop.sum()

    return DimensionPool(
        user_ids=user_ids,
        user_segment=user_segment,
        user_country=user_country,
        user_currency=user_currency,
        _user_weights=user_weights,
        product_ids=product_ids,
        product_price=product_price,
        product_category=product_category,
        product_active=product_active,
        _active_idx=active_idx,
        _active_weights=active_weights,
    )


def generate(n_users: int, n_products: int, seed: int, discontinued_rate: float = 0.12) -> DimensionPool:
    """시드로 메모리에 users/products 풀을 생성한다."""
    rng = make_rng(seed)

    # --- users ---
    user_ids = np.arange(1, n_users + 1, dtype=np.int64)
    user_segment = rng.choice(SEGMENTS, size=n_users, p=SEGMENT_WEIGHTS)
    country_idx = rng.choice(len(COUNTRIES), size=n_users, p=COUNTRY_WEIGHTS)
    user_country = np.array(COUNTRIES, dtype=object)[country_idx]
    user_currency = np.array([COUNTRY_CURRENCY[c] for c in user_country], dtype=object)

    # --- products ---
    product_ids = np.arange(1, n_products + 1, dtype=np.int64)
    # 가격: 로그정규(중앙값 ~$40), USD 기준. 소수 둘째자리 반올림.
    product_price = np.round(rng.lognormal(mean=3.7, sigma=0.8, size=n_products), 2)
    product_category = rng.choice(PRODUCT_CATEGORIES, size=n_products)
    product_active = rng.random(n_products) >= discontinued_rate

    # 활성 상품이 0개면 추출이 불가하므로 최소 1개 보장
    if not product_active.any():
        product_active[0] = True

    return _finalize(
        user_ids, user_segment, user_country, user_currency,
        product_ids, product_price, product_category, product_active,
    )


def load_parquet(path: str | Path) -> DimensionPool:
    """배치 산출물(users.parquet / products.parquet)에서 풀을 로드한다.

    `polars` extra 가 필요하다:  uv sync --extra parquet
    `path`는 users.parquet/products.parquet 가 들어있는 디렉토리.
    """
    try:
        import polars as pl
    except ImportError as e:  # pragma: no cover - 환경 의존
        raise ImportError(
            "parquet 차원 로드에는 polars 가 필요합니다. `uv sync --extra parquet` 후 다시 실행하세요."
        ) from e

    base = Path(path)
    users = pl.read_parquet(base / "users.parquet")
    products = pl.read_parquet(base / "products.parquet")

    user_ids = users["user_id"].to_numpy().astype(np.int64)
    user_segment = users["segment"].to_numpy().astype(object)
    user_country = users["country"].to_numpy().astype(object)
    user_currency = np.array([COUNTRY_CURRENCY.get(c, "USD") for c in user_country], dtype=object)

    product_ids = products["product_id"].to_numpy().astype(np.int64)
    product_price = products["price"].to_numpy().astype(float)
    product_category = products["category_id"].to_numpy().astype(object)
    product_active = ~products["is_discontinued"].to_numpy().astype(bool)
    if not product_active.any():
        product_active[0] = True

    return _finalize(
        user_ids, user_segment, user_country, user_currency,
        product_ids, product_price, product_category, product_active,
    )
