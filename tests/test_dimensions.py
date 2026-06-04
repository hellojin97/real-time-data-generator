"""차원 풀 생성: 재현성·정합성."""
from __future__ import annotations

import numpy as np

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
