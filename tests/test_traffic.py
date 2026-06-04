"""트래픽 모델: 도착이 전진하고, 율이 높을수록 간격이 좁아진다."""
from __future__ import annotations

from datetime import datetime

import numpy as np
import pytest

from realtime_generator.traffic import TrafficModel

START = datetime(2025, 1, 1, 20, 0, 0)


def test_next_arrival_advances():
    rng = np.random.default_rng(0)
    t = TrafficModel(sessions_per_sec=2.0).next_arrival(START, rng)
    assert t > START


def test_higher_rate_means_shorter_gaps_on_average():
    def avg_gap(rate):
        rng = np.random.default_rng(0)
        gaps = []
        t = START
        for _ in range(2000):
            nt = TrafficModel(sessions_per_sec=rate).next_arrival(t, rng)
            gaps.append((nt - t).total_seconds())
            t = nt
        return float(np.mean(gaps))

    assert avg_gap(10.0) < avg_gap(1.0)


def test_zero_rate_rejected():
    with pytest.raises(ValueError):
        TrafficModel(sessions_per_sec=0.0)
