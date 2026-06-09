"""Spark Structured Streaming 소비자 — ecom.orders 를 1분 윈도우로 집계.

생산자(stream-data --sink kafka)가 흘려보낸 `ecom.orders` 토픽을 읽어,
주문 시각(event-time) 기준 **텀블링 윈도우**로 분당 매출/주문수를 집계해 콘솔에 출력한다.
**워터마크**로 지각 데이터를 일정 시간까지 허용한다.

배우는 개념: 이벤트타임 윈도우, 워터마크/지각 데이터, 상태 저장 스트리밍 집계.

실행:
    uv sync --extra spark            # pyspark (local[*] 임베디드)
    docker compose up -d             # Kafka 브로커
    uv run stream-data --sink kafka --duration 60 &   # 생산
    uv run consume-orders            # 소비/집계 (Ctrl-C 로 종료)

설계 메모: 윈도우 집계는 순수 함수 `aggregate_orders` 로 분리해, Kafka·스트리밍 없이
정적 DataFrame 으로 단위 검증할 수 있게 했다(tests/test_spark_orders_window.py).
"""
from __future__ import annotations

import argparse

# pyspark 는 optional extra. 임포트 실패 시 친절한 안내.
try:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
    )
except ImportError as e:  # pragma: no cover - 환경 의존
    raise ImportError(
        "Spark 소비자에는 pyspark 가 필요합니다. `uv sync --extra spark` 후 다시 실행하세요."
    ) from e


# ecom.orders 메시지(JSON value)의 부분 스키마 — 집계에 필요한 필드만.
ORDER_SCHEMA = StructType([
    StructField("order_id", StringType()),
    StructField("user_id", LongType()),
    StructField("order_ts", StringType()),
    StructField("amount_usd", DoubleType()),
    StructField("currency", StringType()),
    StructField("status", StringType()),
])


def parse_orders(raw: DataFrame) -> DataFrame:
    """Kafka raw(record의 value=bytes)를 주문 컬럼으로 파싱하고 order_ts 를 timestamp 로."""
    return (
        raw.select(F.from_json(F.col("value").cast("string"), ORDER_SCHEMA).alias("o"))
        .select("o.*")
        .withColumn("order_ts", F.to_timestamp("order_ts"))
    )


def aggregate_orders(orders: DataFrame, window: str = "1 minute") -> DataFrame:
    """주문 DataFrame을 이벤트타임 윈도우로 집계 → (window_start, window_end, orders, revenue_usd).

    순수 변환이라 배치/스트리밍 DataFrame 모두에 동일하게 적용된다(단위 테스트 대상).
    스트리밍 경로에서는 호출 전에 withWatermark 를 적용한다.
    """
    return (
        orders.groupBy(F.window(F.col("order_ts"), window))
        .agg(
            F.count(F.lit(1)).alias("orders"),
            F.round(F.sum("amount_usd"), 2).alias("revenue_usd"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "orders",
            "revenue_usd",
        )
        .orderBy("window_start")
    )


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Spark Structured Streaming: ecom.orders 윈도우 집계")
    p.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap servers")
    p.add_argument("--topic", default="ecom.orders", help="구독할 토픽")
    p.add_argument("--window", default="1 minute", help="텀블링 윈도우 크기 (예: '1 minute')")
    p.add_argument("--watermark", default="2 minutes", help="지각 데이터 허용 워터마크")
    p.add_argument(
        "--starting-offsets", default="earliest", choices=["earliest", "latest"],
        help="구독 시작 오프셋",
    )
    p.add_argument("--master", default="local[*]", help="Spark master (기본: 임베디드 local[*])")
    return p


def build_spark(master: str) -> SparkSession:
    """Kafka 커넥터 패키지를 붙인 SparkSession 생성. pyspark 버전에 맞는 커넥터를 자동 선택."""
    import pyspark

    kafka_pkg = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark.__version__}"
    spark = (
        SparkSession.builder.appName("ecom-orders-window")
        .master(master)
        .config("spark.jars.packages", kafka_pkg)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main(argv: list[str] | None = None) -> None:
    args = _build_parser().parse_args(argv)
    spark = build_spark(args.master)

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .load()
    )

    orders = parse_orders(raw).withWatermark("order_ts", args.watermark)
    metrics = aggregate_orders(orders, args.window)

    query = (
        metrics.writeStream.format("console")
        .outputMode("complete")  # orderBy 포함 집계 → 매 배치 전체 윈도우 테이블 출력
        .option("truncate", False)
        .start()
    )
    print(
        f"[consume-orders] {args.topic}@{args.bootstrap} | window={args.window} "
        f"watermark={args.watermark} | Ctrl-C 로 종료"
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
