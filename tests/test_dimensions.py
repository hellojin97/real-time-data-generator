"""차원 풀 생성: 재현성·정합성."""
from __future__ import annotations

import numpy as np
import pytest

from realtime_generator import dimensions


def test_generate_is_reproducible():
    a = dimensions.generate(n_users=500, n_products=100, seed=42)
    b = dimensions.generate(n_users=500, n_products=100, seed=42)
    assert np.array_equal(a.user_ids, b.user_ids)
    assert np.array_equal(a.user_segment, b.user_segment)
    assert np.array_equal(a.product_price, b.product_price)
    assert np.array_equal(a.product_active, b.product_active)


def test_different_seed_differs():
    a = dimensions.generate(n_users=500, n_products=100, seed=1)
    b = dimensions.generate(n_users=500, n_products=100, seed=2)
    assert not np.array_equal(a.user_segment, b.user_segment)


def test_weights_are_normalized():
    pool = dimensions.generate(n_users=300, n_products=80, seed=7)
    assert abs(pool._user_weights.sum() - 1.0) < 1e-9
    assert abs(pool._active_weights.sum() - 1.0) < 1e-9


def test_active_products_exist_and_ids_are_1_based():
    pool = dimensions.generate(n_users=10, n_products=50, seed=3)
    assert pool.product_active.any()
    assert pool.product_ids[0] == 1
    assert pool.user_ids[0] == 1


def test_pickers_return_valid_indices():
    pool = dimensions.generate(n_users=100, n_products=40, seed=5)
    rng = np.random.default_rng(0)
    u = pool.pick_user(rng)
    p = pool.pick_product(rng)
    assert 0 <= u < pool.n_users
    assert bool(pool.product_active[p])  # 항상 활성 상품만 추출


def test_popularity_is_decorrelated_from_product_id():
    # 베스트셀러가 항상 가장 작은 product_id 가 되면 안 된다(시드마다 달라져야 함).
    import collections

    def top_product(seed):
        pool = dimensions.generate(n_users=10, n_products=60, seed=seed)
        rng = np.random.default_rng(seed)
        c = collections.Counter(
            int(pool.product_ids[pool.pick_product(rng)]) for _ in range(5000)
        )
        return c.most_common(1)[0][0]

    tops = {top_product(s) for s in range(6)}
    # 시드별로 최고 인기 상품이 갈려야 한다(전부 product_id=1 이면 버그)
    assert tops != {1}
    assert len(tops) > 1


def test_load_parquet_matches_sibling_schema(tmp_path):
    # 형제 프로젝트(ecommerce-data-generator) 산출물 스키마로 라운드트립.
    pl = pytest.importorskip("polars")
    pl.DataFrame({
        "user_id": [1, 2, 3],
        "email": ["a@x", "b@x", "c@x"],
        "gender": ["M", None, "F"],
        "country": ["US", "KR", "DE"],
        "segment": ["new", "vip", "regular"],
    }).write_parquet(tmp_path / "users.parquet")
    pl.DataFrame({
        "product_id": [10, 20, 30],
        "sku": ["s1", "s2", "s3"],
        "name": ["n1", "n2", "n3"],
        "category_id": [1, 2, 3],
        "brand": ["b1", None, "b3"],
        "base_price": [9.99, 19.5, 100.0],
        "cost": [5.0, 10.0, 50.0],
        "is_active": [True, True, False],
    }).write_parquet(tmp_path / "products.parquet")

    pool = dimensions.load_parquet(tmp_path)
    assert list(pool.user_ids) == [1, 2, 3]
    assert list(pool.product_price) == [9.99, 19.5, 100.0]
    # is_active=False 인 30번은 비활성으로 잡혀 추출 대상에서 제외
    assert list(pool.product_active) == [True, True, False]
    assert pool.user_currency[1] == "KRW"  # country=KR
