"""시계 추상화.

실시간 생성기의 핵심 분기점: 시간이 '진짜로 흐르느냐'(RealClock) vs
'결정론적으로 점프하느냐'(SimulatedClock). 엔진은 Clock 인터페이스에만 의존하므로
같은 코드로 실제 스트리밍과 테스트/리플레이를 모두 돌릴 수 있다.
"""
import time
from datetime import datetime, timedelta
from typing import Protocol, runtime_checkable


@runtime_checkable
class Clock(Protocol):
    """now()로 현재 시각을, sleep()으로 시간 경과를 표현."""

    def now(self) -> datetime: ...

    def sleep(self, seconds: float) -> None: ...


class RealClock:
    """실제 벽시계. now()는 로컬 현재 시각, sleep()은 실제 대기."""

    def now(self) -> datetime:
        return datetime.now()

    def sleep(self, seconds: float) -> None:
        if seconds > 0:
            time.sleep(seconds)


class SimulatedClock:
    """가상 시계. sleep()이 실제 대기 없이 내부 시각만 전진시킨다.

    고정 start 시각에서 출발하므로 같은 시드 → 같은 (상대) 타임스탬프 스트림이
    재현된다. 테스트와 백필/리플레이(과거 구간 빠른 재생)에 쓴다.
    """

    def __init__(self, start: datetime):
        self._t = start

    def now(self) -> datetime:
        return self._t

    def sleep(self, seconds: float) -> None:
        if seconds > 0:
            self._t += timedelta(seconds=seconds)
