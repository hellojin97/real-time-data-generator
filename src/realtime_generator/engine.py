"""이산사건(discrete-event) 엔진 — 트래픽·세션·싱크·시계를 결합한 메인 루프.

동작:
1. TrafficModel 로 다음 세션 도착 시각을 뽑는다.
2. 도착한 세션을 plan_session 으로 펼쳐 모든 레코드를 시각순 힙에 넣는다.
3. 시계가 각 레코드의 시각에 도달하면 싱크로 흘려보낸다.

겹치는 세션들의 이벤트가 시간순으로 자연스럽게 인터리빙된다. RealClock 이면 실제
스트리밍, SimulatedClock 이면 같은 시드로 결정론적 재현(테스트/리플레이)이 된다.

재현성: 풀 생성과 분리된 별도 rng(seed+1)로 모든 스트리밍 추출을 수행하고,
같은 시각 레코드는 삽입 순서(tiebreak)로 안정 정렬해 .emit 순서까지 고정한다.
"""
from __future__ import annotations

import heapq
import itertools
from datetime import timedelta

from .base import make_rng
from .clock import Clock
from .dimensions import DimensionPool
from .sessions import DEFAULT_MAX_BROWSE_EVENTS, plan_session
from .sinks import Sink
from .traffic import TrafficModel


class Engine:
    def __init__(
        self,
        pool: DimensionPool,
        traffic: TrafficModel,
        sink: Sink,
        clock: Clock,
        *,
        seed: int,
        conversion_rate: float,
        null_rate_search: float,
        max_browse_events: int = DEFAULT_MAX_BROWSE_EVENTS,
    ):
        self.pool = pool
        self.traffic = traffic
        self.sink = sink
        self.clock = clock
        # 풀 생성(seed)과 분리해 스트리밍 추출은 seed+1 로 — 트래픽이 바뀌어도 풀 고정
        self.rng = make_rng(seed + 1)
        self.conversion_rate = conversion_rate
        self.null_rate_search = null_rate_search
        self.max_browse_events = max_browse_events

    def run(self, *, max_events: int | None = None, max_duration_s: float | None = None) -> int:
        """루프 실행. max_events 또는 max_duration_s 도달 시 종료. 방출 건수 반환.

        둘 다 None 이면 무한 실행(Ctrl-C 로 중단). 종료 시 싱크를 flush/close 한다.
        """
        clock, sink = self.clock, self.sink
        start = clock.now()
        deadline = start + timedelta(seconds=max_duration_s) if max_duration_s else None

        heap: list[tuple] = []          # (ts, tiebreak, PlannedRecord)
        tiebreak = itertools.count()    # 동일 시각 안정 정렬용
        session_seq = 0
        emitted = 0
        next_arrival = self.traffic.next_arrival(start, self.rng)

        try:
            while True:
                if max_events is not None and emitted >= max_events:
                    break
                now = clock.now()
                if deadline is not None and now >= deadline:
                    break

                # 다음으로 무언가 일어날 시각 = min(다음 도착, 힙 최상단)
                next_heap_ts = heap[0][0] if heap else None
                candidates = [t for t in (next_arrival, next_heap_ts) if t is not None]
                next_t = min(candidates)
                if deadline is not None and next_t > deadline:
                    next_t = deadline  # 데드라인 너머로 오버슬립 방지

                wait = (next_t - now).total_seconds()
                if wait > 0:
                    clock.sleep(wait)
                now = clock.now()

                # 도착한 세션 펼치기
                while next_arrival <= now:
                    for rec in plan_session(
                        self.pool, self.rng, session_seq, next_arrival,
                        conversion_rate=self.conversion_rate,
                        null_rate_search=self.null_rate_search,
                        max_browse_events=self.max_browse_events,
                    ):
                        heapq.heappush(heap, (rec.ts, next(tiebreak), rec))
                    session_seq += 1
                    next_arrival = self.traffic.next_arrival(next_arrival, self.rng)

                # 시각이 도래한 레코드 방출
                while heap and heap[0][0] <= now:
                    if max_events is not None and emitted >= max_events:
                        break
                    _, _, rec = heapq.heappop(heap)
                    sink.emit(rec.stream, rec.key, rec.value, rec.ts)
                    emitted += 1
        finally:
            sink.flush()
            sink.close()

        return emitted
