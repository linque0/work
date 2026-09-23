"""实时链路：Structured Streaming 实时指标计算。

模拟 Kafka 数据源：从 data/landing/repay_stream/ 目录增量读取 JSON Lines
消息（生产对应 Kafka topic），按 1 分钟滚动窗口聚合还款指标，写入
ads_realtime_metrics 表，供实时大屏与流批对账使用。

用法：
    python -m spark_jobs.streaming_job --once     # 处理当前已到达数据后退出（测试/对账友好）
    python -m spark_jobs.streaming_job            # 持续运行（生产形态）
    python -m spark_jobs.streaming_job --sink console  # 控制台观察

核心机制（面试常考）：
- Watermark：允许 1 分钟乱序，兼顾延迟与完整性；
- Checkpoint：offset + 状态持久化，保证 Exactly-Once 语义；
- availableNow 触发器：一次处理完所有已积累数据，让对账脚本可确定性地跑。
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from spark_jobs.common.paths import Paths, load_config  # noqa: E402
from spark_jobs.common.spark_session import get_spark  # noqa: E402

STREAM_SCHEMA = StructType(
    [
        StructField("flow_id", StringType()),
        StructField("application_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("event_time", TimestampType()),
        StructField("repay_amount", DoubleType()),
        StructField("overdue_days", IntegerType()),
    ]
)


def run(once: bool, sink: str) -> None:
    cfg = load_config()
    paths = Paths.from_config(cfg)
    spark = get_spark(
        app_name=f"{cfg['spark']['app_name']}-streaming",
        master=cfg["spark"]["master"],
        shuffle_partitions=cfg["spark"]["shuffle_partitions"],
    )
    spark.sparkContext.setLogLevel("WARN")

    source_dir = os.path.join(paths.landing_dir, "repay_stream")

    raw = (
        spark.readStream.schema(STREAM_SCHEMA)
        .option("maxFilesPerTrigger", 10)
        .json(source_dir)
    )

    windowed = (
        raw.withWatermark("event_time", "1 minute")
        .groupBy(F.window(F.col("event_time"), "1 minute"))
        .agg(
            F.count("*").alias("msg_cnt"),
            F.round(F.sum("repay_amount"), 2).alias("repay_amt_sum"),
            F.sum(F.when(F.col("overdue_days") > 0, 1).otherwise(0)).alias("overdue_cnt"),
            F.round(
                F.sum(F.when(F.col("overdue_days") > 0, F.col("repay_amount")).otherwise(0.0)), 2
            ).alias("overdue_amt_sum"),
        )
        .select(
            F.date_format(F.col("window.start"), "yyyy-MM-dd HH:mm:ss").alias("window_start"),
            F.date_format(F.col("window.end"), "yyyy-MM-dd HH:mm").alias("window_end"),
            "msg_cnt",
            "repay_amt_sum",
            "overdue_cnt",
            "overdue_amt_sum",
        )
    )

    if sink == "console":
        query = (
            windowed.writeStream.outputMode("complete")
            .format("console")
            .option("truncate", "false")
            .trigger(availableNow=True)
            .start()
        )
    else:
        out_path = paths.ads("ads_realtime_metrics")
        checkpoint = os.path.join(paths.warehouse_root, "ads", "_checkpoints", "realtime_metrics")
        writer = (
            windowed.writeStream.outputMode("append")
            .format("parquet")
            .option("path", out_path)
            .option("checkpointLocation", checkpoint)
        )
        query = writer.trigger(availableNow=True).start() if once else writer.start()

    query.awaitTermination()
    if sink != "console":
        print(f"[streaming] metrics written -> {paths.ads('ads_realtime_metrics')}")
    spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="实时数仓指标计算（Structured Streaming）")
    parser.add_argument("--once", action="store_true", help="处理完当前已到达数据后退出")
    parser.add_argument("--sink", choices=["parquet", "console"], default="parquet")
    args = parser.parse_args()
    run(once=args.once, sink=args.sink)


if __name__ == "__main__":
    main()
