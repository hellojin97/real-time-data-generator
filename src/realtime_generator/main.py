"""실시간 이커머스 이벤트 스트림 생성기 — CLI 진입점.

사용 예:
    # stdout 으로 1000건만 빠르게 (관찰/디버그)
    stream-data --sink stdout --max-events 1000

    # 결정론 모드(실제 대기 없이 즉시 생성). 테스트/리플레이.
    stream-data --simulated --max-events 5000 --sink file --out-dir ./output/stream

    # 실시간으로 60초간 흘려보내기
    stream-data --duration 60 --sessions-per-sec 5

    # 배치 산출물의 차원 재사용
    stream-data --dim-parquet /path/to/raw --max-events 1000

로그/배너는 stderr 로 나가므로, stdout 싱크의 JSONL 을 그대로 파이프할 수 있다:
    stream-data --max-events 100 | jq .
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime

from . import dimensions
from .base import load_config
from .clock import RealClock, SimulatedClock
from .engine import Engine
from .sinks import make_sink
from .traffic import TrafficModel


def _log(msg: str) -> None:
    print(msg, file=sys.stderr)


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="실시간 이커머스 이벤트 스트림 생성기")
    p.add_argument("--config", default=None, help="config.yml 경로(기본: 패키지 내장)")
    p.add_argument("--seed", type=int, default=None, help="RNG seed(기본: config seed)")

    # 차원
    p.add_argument("--users", type=int, default=None, help="유저 풀 크기(기본: config)")
    p.add_argument("--products", type=int, default=None, help="상품 풀 크기(기본: config)")
    p.add_argument(
        "--dim-parquet", default=None,
        help="배치 산출물 경로(users/products.parquet 가 있는 디렉토리). 지정 시 메모리 생성 대신 로드",
    )

    # 트래픽 / 퍼널
    p.add_argument("--sessions-per-sec", type=float, default=None, help="하루 평균 세션 도착률(초당)")
    p.add_argument("--conversion-rate", type=float, default=None, help="구매 전환율(0~1)")
    p.add_argument("--null-rate-search", type=float, default=None, help="search_query NULL 비율")
    p.add_argument("--max-browse-events", type=int, default=None, help="브라우징 세션 이벤트 상한")

    # 지각(late) 이벤트 주입
    p.add_argument("--late-rate", type=float, default=None, help="지각시킬 레코드 비율(0~1, 기본: config)")
    p.add_argument("--late-max-delay", type=float, default=None, help="지각 지연 상한(초, 기본: config)")

    # 싱크
    p.add_argument("--sink", default=None, choices=["stdout", "file", "kafka"], help="출력 싱크(기본: config)")
    p.add_argument("--out-dir", default=None, help="file 싱크 출력 디렉토리")

    # 실행 제어
    p.add_argument("--max-events", type=int, default=None, help="이만큼 방출 후 종료")
    p.add_argument("--duration", type=float, default=None, help="이만큼(초) 실행 후 종료")
    p.add_argument(
        "--simulated", action="store_true",
        help="가상 시계로 실제 대기 없이 즉시 생성(결정론). 미지정 시 실시간(wall-clock)",
    )
    p.add_argument(
        "--sim-start", default="2025-01-01T00:00:00",
        help="--simulated 시작 시각(ISO8601). 기본 2025-01-01T00:00:00",
    )
    return p


def _resolve(args_val, cfg_val):
    return args_val if args_val is not None else cfg_val


def main(argv: list[str] | None = None) -> None:
    args = _build_parser().parse_args(argv)
    cfg = load_config(args.config)

    seed = _resolve(args.seed, cfg["seed"])
    n_users = _resolve(args.users, cfg["dimensions"]["users"])
    n_products = _resolve(args.products, cfg["dimensions"]["products"])
    sessions_per_sec = _resolve(args.sessions_per_sec, cfg["traffic"]["sessions_per_sec"])
    conversion_rate = _resolve(args.conversion_rate, cfg["funnel"]["conversion_rate"])
    null_rate_search = _resolve(args.null_rate_search, cfg["dirty_data"]["null_rate_search"])
    # lateness 는 뒤에 추가된 섹션 — 구버전 커스텀 config 에도 동작하도록 .get 으로 읽는다
    late_cfg = cfg.get("lateness", {})
    late_rate = _resolve(args.late_rate, late_cfg.get("late_rate", 0.0))
    late_max_delay_s = _resolve(args.late_max_delay, late_cfg.get("late_max_delay_s", 120.0))

    # 싱크 설정: CLI 오버라이드 반영
    sink_cfg = dict(cfg["sink"])
    if args.sink is not None:
        sink_cfg["type"] = args.sink
    if args.out_dir is not None:
        sink_cfg = {**sink_cfg, "file": {**sink_cfg.get("file", {}), "out_dir": args.out_dir}}

    # 차원 준비
    if args.dim_parquet:
        _log(f"[dim] loading from parquet: {args.dim_parquet}")
        pool = dimensions.load_parquet(args.dim_parquet)
    else:
        pool = dimensions.generate(n_users=n_users, n_products=n_products, seed=seed)

    # 시계
    clock = SimulatedClock(datetime.fromisoformat(args.sim_start)) if args.simulated else RealClock()

    sink = make_sink(sink_cfg)
    traffic = TrafficModel(sessions_per_sec=sessions_per_sec)
    engine = Engine(
        pool, traffic, sink, clock,
        seed=seed,
        conversion_rate=conversion_rate,
        null_rate_search=null_rate_search,
        max_browse_events=_resolve(args.max_browse_events, 8),
        late_rate=late_rate,
        late_max_delay_s=late_max_delay_s,
    )

    _log("=" * 60)
    _log("Real-Time E-Commerce Event Stream")
    _log(f"  users={pool.n_users:,}  products={pool.n_products:,}  seed={seed}")
    _log(f"  sessions/sec(avg)={sessions_per_sec}  conversion={conversion_rate}")
    _log(f"  sink={sink_cfg['type']}  clock={'simulated' if args.simulated else 'real'}")
    if late_rate > 0:
        _log(f"  late: rate={late_rate}  max_delay={late_max_delay_s}s")
    stop = f"max_events={args.max_events}" if args.max_events else (
        f"duration={args.duration}s" if args.duration else "infinite (Ctrl-C to stop)")
    _log(f"  stop: {stop}")
    _log("=" * 60)

    t0 = time.time()
    try:
        emitted = engine.run(max_events=args.max_events, max_duration_s=args.duration)
    except KeyboardInterrupt:
        _log("\n[interrupted]")
        return
    elapsed = time.time() - t0
    _log(f"[done] emitted={emitted:,} records in {elapsed:.2f}s")


if __name__ == "__main__":
    main()
