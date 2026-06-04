"""트래픽 모델 — 세션 도착 과정.

세션 도착을 비균질 포아송 과정으로 본다. 순간 도착률
    λ(t) = sessions_per_sec(하루 평균) × hour_multiplier(t)
이며, 다음 도착까지의 간격을 지수분포에서 뽑는다(piecewise-constant 근사).
저녁엔 촘촘하게, 새벽엔 듬성듬성 세션이 태어난다.
"""
from __future__ import annotations

from datetime import datetime, timedelta

import numpy as np

from .base import hour_multiplier


class TrafficModel:
    def __init__(self, sessions_per_sec: float):
        if sessions_per_sec <= 0:
            raise ValueError("sessions_per_sec 는 양수여야 합니다.")
        self.base_rate = sessions_per_sec

    def next_arrival(self, after: datetime, rng: np.random.Generator) -> datetime:
        """`after` 이후 다음 세션 도착 시각을 반환."""
        rate = self.base_rate * hour_multiplier(after)
        gap_s = float(rng.exponential(1.0 / rate))
        return after + timedelta(seconds=gap_s)
