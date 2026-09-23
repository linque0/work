"""ODS 层：业务库数据接入（贴源层）。

职责：
- 读取业务库每日导出 CSV（data/business/{table}/dt={dt}/）；
- 统一字段类型（显式 Schema，避免推断漂移）；
- 打上 ETL 审计字段（etl_time / source_file），写入 parquet 到 ods/{table}/dt={dt}；
- 幂等：同一 dt 重跑直接覆盖，保证调度重试安全。

生产对应：DataX/Canal 同步业务库 -> HDFS ODS 分区表。
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime

# 兼容 spark-submit 直接提交脚本文件的调用方式
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

SCHEMAS = {
    "customer": StructType(
        [
            StructField("customer_id", StringType()),
            StructField("customer_name", StringType()),
            StructField("gender", StringType()),
            StructField("age", IntegerType()),
            StructField("education", StringType()),
            StructField("marital_status", StringType()),
            StructField("occupation", StringType()),
            StructField("city", StringType()),
            StructField("register_date", StringType()),
        ]
    ),
    "loan_application": StructType(
        [
            StructField("application_id", StringType()),
            StructField("customer_id", StringType()),
            StructField("apply_time", TimestampType()),
            StructField("loan_amnt", DoubleType()),
            StructField("term", IntegerType()),
            StructField("interest_rate", DoubleType()),
            StructField("purpose", StringType()),
            StructField("grade", StringType()),
            StructField("annual_income", DoubleType()),
            StructField("dti", DoubleType()),
            StructField("fico_score", IntegerType()),
            StructField("verification_status", StringType()),
            StructField("home_ownership", StringType()),
            StructField("is_approved", IntegerType()),
            StructField("is_default", IntegerType()),
        ]
    ),
    "repay_flow": StructType(
        [
            StructField("flow_id", StringType()),
            StructField("application_id", StringType()),
            StructField("customer_id", StringType()),
            StructField("due_date", StringType()),
            StructField("repay_time", TimestampType()),
            StructField("due_amount", DoubleType()),
            StructField("repay_amount", DoubleType()),
            StructField("overdue_days", IntegerType()),
        ]
    ),
}


def ingest_table(spark: SparkSession, paths: Paths, table: str, dt: str) -> int:
    src = paths.business_table(table, dt)
    schema = SCHEMAS[table]
    df = (
        spark.read.schema(schema)
        .option("header", "true")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .csv(src)
    )
    df = df.withColumn("etl_time", F.current_timestamp()).withColumn("source_file", F.lit(os.path.basename(src)))
    df.write.mode("overwrite").parquet(paths.ods(table, dt))
    n = spark.read.parquet(paths.ods(table, dt)).count()
    print(f"[ods] {table} dt={dt} rows={n} -> {paths.ods(table, dt)}")
    return n


def run(dt: str) -> None:
    cfg = load_config()
    paths = Paths.from_config(cfg)
    spark = get_spark(
        app_name=f"{cfg['spark']['app_name']}-ods",
        master=cfg["spark"]["master"],
        shuffle_partitions=cfg["spark"]["shuffle_partitions"],
    )
    try:
        for table in ("customer", "loan_application", "repay_flow"):
            ingest_table(spark, paths, table, dt)
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="ODS 层数据接入")
    parser.add_argument("--dt", required=True, help="业务日期 YYYY-MM-DD")
    args = parser.parse_args()
    run(args.dt)


if __name__ == "__main__":
    main()
