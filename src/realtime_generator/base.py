"""공통 유틸리티.

배치 생성기와 동일한 현실성 자산을 실시간 버전에서도 재사용한다:
- 시드 기반 RNG (재현성)
- 시간대 가중치(HOUR_WEIGHTS) — 저녁 피크/새벽 저점
"""
from datetime import datetime
from pathlib import Path

import numpy as np
import yaml

# 시간대별 활동 가중치(0~23시). 저녁(20~22시) 피크, 새벽(3~5시) 저점.
# 배치 생성기의 HOUR_WEIGHTS 와 같은 의도. 합이 아닌 '상대비'로만 의미를 가진다.
HOUR_WEIGHTS: list[float] = [
    0.3, 0.2, 0.15, 0.1, 0.1, 0.15,   # 0~5시
    0.3, 0.6, 0.9, 1.0, 1.1, 1.2,     # 6~11시
    1.3, 1.2, 1.1, 1.1, 1.2, 1.4,     # 12~17시
    1.7, 1.9, 2.0, 1.8, 1.2, 0.6,     # 18~23시
]

_HOUR_MEAN = float(np.mean(HOUR_WEIGHTS))


def load_config(path: str | Path | None = None) -> dict:
    """YAML 설정 로드. path가 None이면 패키지 내장 config.yml 사용."""
    if path is None:
        path = Path(__file__).parent / "config.yml"
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def make_rng(seed: int) -> np.random.Generator:
    """시드 기반 RNG. 같은 시드 → 같은 추출 결과(재현성)."""
    return np.random.default_rng(seed)


def hour_multiplier(ts: datetime) -> float:
    """해당 시각의 활동 배수. 하루 평균이 1.0이 되도록 정규화한다.

    sessions_per_sec(하루 평균)에 이 값을 곱하면 그 시각의 순간 도착률이 된다.
    """
    return HOUR_WEIGHTS[ts.hour] / _HOUR_MEAN
