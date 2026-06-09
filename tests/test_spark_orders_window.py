"""Spark 소비자의 윈도우 집계 로직 단위 테스트.

Kafka·스트리밍 없이 정적(batch) DataFrame 으로 aggregate_orders 를 검증한다.
pyspark 미설치(기본 ci)면 자동 스킵된다.
"""
from __future__ import annotations

from datetime import datetime

import pytest

pytest.importorskip("pyspark")  # spark extra 없으면 스킵

from pyspark.sql import SparkSession  # noqa: E402

from realtime_generator.consumers.spark_orders_window import aggregate_orders  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.appName("test-orders-window")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def test_aggregate_orders_groups_into_one_minute_windows(spark):
    # 00:00~00:01 창에 2건(100+50), 00:01~00:02 창에 1건(25.5)
    rows = [
        ("ord-1", datetime(2025, 1, 1, 0, 0, 5), 100.0),
        ("ord-2", datetime(2025, 1, 1, 0, 0, 30), 50.0),
        ("ord-3", datetime(2025, 1, 1, 0, 1, 10), 25.5),
    ]
    df = spark.createDataFrame(rows, ["order_id", "order_ts", "amount_usd"])

    res = aggregate_orders(df, "1 minute").collect()

    assert len(res) == 2  # 두 개의 1분 윈도우
    by_orders = {r["orders"]: r["revenue_usd"] for r in res}
    assert by_orders[2] == 150.0   # 첫 창: 2건, 매출 150.0
    assert by_orders[1] == 25.5    # 둘째 창: 1건, 매출 25.5

    # 윈도우 경계가 1분 폭이고 시간순 정렬됨
    starts = [r["window_start"] for r in res]
    assert starts == sorted(starts)
    for r in res:
        assert (r["window_end"] - r["window_start"]).total_seconds() == 60
