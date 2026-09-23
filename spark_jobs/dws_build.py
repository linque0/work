"""DWS 层：汇总数据层（轻度汇总，面向复用）。

职责：
- dws_apply_day：按业务日期的申请域汇总（申请量/通过率/放款金额/违约率），
  ADS 层的滚动指标与环比都基于本表做窗口计算；
- dws_customer_day：客户×日期粒度的累计口径汇总（历史申请、历史放款、
  历史还款、历史逾期、最近行为日期），是评分卡特征与客户经营分析的数据底座。

设计要点：
- DWS 只做"轻度汇总"（不掺业务口径），保证一张表被多个 ADS 需求复用；
- 客户累计口径读取全部事实分区、按 dt 截断，保证"截至当日"语义正确。
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.window import Window  # noqa: E402

from spark_jobs.common.paths import Paths, load_config  # noqa: E402
from spark_jobs.common.spark_session import get_spark  # noqa: E402


def build_apply_day(spark: SparkSession, paths: Paths, dt: str) -> None:
    """申请域按日汇总（只处理当日事实，幂等覆盖）。"""
    fact = spark.read.parquet(paths.dwd("dwd_fact_loan_apply", dt))

    agg = fact.agg(
        F.count("*").alias("apply_cnt"),
        F.sum(F.when(F.col("is_approved") == 1, 1).otherwise(0)).alias("approve_cnt"),
        F.sum(F.when(F.col("is_approved") == 1, F.col("loan_amnt")).otherwise(0.0)).alias("loan_amt_sum"),
        F.avg("interest_rate").alias("avg_interest_rate"),
        F.avg("annual_income").alias("avg_annual_income"),
        F.avg("dti").alias("avg_dti"),
        F.sum(F.when(F.col("is_approved") == 1, F.col("is_default")).otherwise(0)).alias("default_cnt"),
        F.countDistinct("customer_id").alias("apply_customer_cnt"),
    ).withColumn("apply_date", F.to_date(F.lit(dt)))

    agg = agg.withColumn(
        "approve_rate", F.round(F.col("approve_cnt") / F.col("apply_cnt"), 4)
    ).withColumn(
        "default_rate",
        F.round(F.col("default_cnt") / F.greatest(F.col("approve_cnt"), F.lit(1)), 4),
    )

    agg.write.mode("overwrite").parquet(paths.dws("dws_apply_day", dt))
    print(f"[dws] dws_apply_day dt={dt} rows=1")


def build_customer_day(spark: SparkSession, paths: Paths, dt: str) -> None:
    """客户×日期累计口径：读取全部事实分区，按 dt 截断后按客户聚合。"""
    apply_all = spark.read.parquet(paths.dwd("dwd_fact_loan_apply"))
    repay_all = spark.read.parquet(paths.dwd("dwd_fact_repay"))

    apply_hist = apply_all.where(F.col("apply_date") <= dt)
    repay_hist = repay_all.where(F.to_date(F.col("repay_time")) <= dt)

    a = apply_hist.groupBy("customer_id").agg(
        F.count("*").alias("hist_apply_cnt"),
        F.sum(F.when(F.col("is_approved") == 1, 1).otherwise(0)).alias("hist_approve_cnt"),
        F.sum(F.when(F.col("is_approved") == 1, F.col("loan_amnt")).otherwise(0.0)).alias("hist_loan_amt"),
        F.max("apply_date").alias("last_apply_date"),
    )
    r = repay_hist.groupBy("customer_id").agg(
        F.count("*").alias("hist_repay_cnt"),
        F.sum("repay_amount").alias("hist_repay_amt"),
        F.sum(F.when(F.col("overdue_days") > 0, 1).otherwise(0)).alias("hist_overdue_cnt"),
        F.max(F.to_date(F.col("repay_time"))).alias("last_repay_date"),
    )

    cust_day = (
        a.join(r, "customer_id", "full_outer")
        .select(
            # 按列名 full outer join 后 customer_id 已被 Spark 合并为单列，直接引用
            F.col("customer_id"),
            F.coalesce("hist_apply_cnt", F.lit(0)).alias("hist_apply_cnt"),
            F.coalesce("hist_approve_cnt", F.lit(0)).alias("hist_approve_cnt"),
            F.coalesce("hist_loan_amt", F.lit(0.0)).alias("hist_loan_amt"),
            F.coalesce("hist_repay_cnt", F.lit(0)).alias("hist_repay_cnt"),
            F.coalesce("hist_repay_amt", F.lit(0.0)).alias("hist_repay_amt"),
            F.coalesce("hist_overdue_cnt", F.lit(0)).alias("hist_overdue_cnt"),
            "last_apply_date",
            "last_repay_date",
        )
        .withColumn("dt", F.lit(dt))
        # 逾期占比：历史逾期笔数 / 历史还款笔数
        .withColumn(
            "overdue_ratio",
            F.round(F.col("hist_overdue_cnt") / F.greatest(F.col("hist_repay_cnt"), F.lit(1)), 4),
        )
    )

    cust_day.write.mode("overwrite").parquet(paths.dws("dws_customer_day", dt))
    n = spark.read.parquet(paths.dws("dws_customer_day", dt)).count()
    print(f"[dws] dws_customer_day dt={dt} rows={n}")


def run(dt: str) -> None:
    cfg = load_config()
    paths = Paths.from_config(cfg)
    spark = get_spark(
        app_name=f"{cfg['spark']['app_name']}-dws",
        master=cfg["spark"]["master"],
        shuffle_partitions=cfg["spark"]["shuffle_partitions"],
    )
    try:
        build_apply_day(spark, paths, dt)
        build_customer_day(spark, paths, dt)
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="DWS 层汇总建模")
    parser.add_argument("--dt", required=True, help="业务日期 YYYY-MM-DD")
    args = parser.parse_args()
    run(args.dt)


if __name__ == "__main__":
    main()
