"""ADS 层：应用数据层（面向三类消费场景）。

职责：
- ads_risk_daily：风控日报——当日指标 + 7 日滚动 + 日环比（窗口函数），
  供 BI 报表与实时大屏的"离线侧"对照；
- ads_scorecard_features：评分卡特征宽表（申请粒度，拼装客户历史累计特征），
  直接供算法团队做违约模型训练——对应 JD 中"算法训练"消费场景；
- _export_ads_risk_daily.csv：给看板的 CSV 快照（Spark 直出，pandas 直读，
  避免本地环境对 pyarrow 的依赖）。

设计要点：
- ADS 是唯一允许"带业务口径"的层（如违约率分母取通过笔数）；
- 全表基于 DWS 重算后覆盖写，保证历史日期口径修订后自动一致。
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


def build_risk_daily(spark: SparkSession, paths: Paths, dt: str) -> None:
    """风控日报：7 日滚动 + 日环比（窗口函数）。"""
    dws = spark.read.parquet(paths.dws("dws_apply_day"))

    w = Window.orderBy("apply_date")
    ads = (
        dws.withColumn("apply_cnt_7d", F.round(F.sum("apply_cnt").over(w.rowsBetween(-6, 0)), 0).cast("long"))
        .withColumn("loan_amt_7d", F.round(F.sum("loan_amt_sum").over(w.rowsBetween(-6, 0)), 2))
        .withColumn("apply_cnt_prev", F.lag("apply_cnt", 1).over(w))
        .withColumn(
            "apply_cnt_dod",
            F.round(
                (F.col("apply_cnt") - F.col("apply_cnt_prev")) / F.greatest(F.col("apply_cnt_prev"), F.lit(1)), 4
            ),
        )
        .withColumn("etl_time", F.current_timestamp())
        .select(
            "apply_date",
            "apply_cnt",
            "approve_cnt",
            "approve_rate",
            "loan_amt_sum",
            "avg_interest_rate",
            "default_cnt",
            "default_rate",
            "apply_cnt_7d",
            "loan_amt_7d",
            "apply_cnt_dod",
            "etl_time",
        )
    )
    ads.write.mode("overwrite").parquet(paths.ads("ads_risk_daily"))
    print(f"[ads] ads_risk_daily rows={spark.read.parquet(paths.ads('ads_risk_daily')).count()}")


def build_scorecard_features(spark: SparkSession, paths: Paths, dt: str) -> None:
    """评分卡特征宽表：申请属性 + 客户历史累计特征 + 衍生比率特征。"""
    fact = spark.read.parquet(paths.dwd("dwd_fact_loan_apply", dt))
    cust = spark.read.parquet(paths.dws("dws_customer_day", dt)).select(
        "customer_id",
        "hist_apply_cnt",
        "hist_approve_cnt",
        "hist_loan_amt",
        "hist_repay_cnt",
        "hist_overdue_cnt",
        "overdue_ratio",
    )

    features = (
        fact.alias("f")
        .join(cust.alias("c"), "customer_id", "left")
        .select(
            F.col("f.application_id"),
            F.col("f.customer_id"),
            F.col("f.customer_sk"),
            F.col("f.apply_date"),
            F.col("f.loan_amnt"),
            F.col("f.term"),
            F.col("f.interest_rate"),
            F.col("f.purpose"),
            F.col("f.grade"),
            F.col("f.annual_income"),
            F.col("f.dti"),
            F.col("f.fico_score"),
            F.col("f.verification_status"),
            F.col("f.home_ownership"),
            F.col("f.is_approved"),
            F.col("f.is_default").alias("label"),
            # 衍生比率特征
            F.round(F.col("f.loan_amnt") / F.col("f.term"), 2).alias("installment"),
            F.round(F.col("f.annual_income") / 12, 2).alias("monthly_income"),
            F.round(
                F.col("f.loan_amnt") / F.greatest(F.col("f.annual_income"), F.lit(1.0)), 4
            ).alias("loan_to_income"),
            # 客户历史特征（缺失填 0，表示新客）
            F.coalesce("c.hist_apply_cnt", F.lit(0)).alias("hist_apply_cnt"),
            F.coalesce("c.hist_approve_cnt", F.lit(0)).alias("hist_approve_cnt"),
            F.coalesce("c.hist_loan_amt", F.lit(0.0)).alias("hist_loan_amt"),
            F.coalesce("c.hist_overdue_cnt", F.lit(0)).alias("hist_overdue_cnt"),
            F.coalesce("c.overdue_ratio", F.lit(0.0)).alias("hist_overdue_ratio"),
            F.coalesce("c.hist_repay_cnt", F.lit(0)).alias("hist_repay_cnt"),
        )
    )
    features.write.mode("overwrite").parquet(paths.ads("ads_scorecard_features", dt))
    n = spark.read.parquet(paths.ads("ads_scorecard_features", dt)).count()
    print(f"[ads] ads_scorecard_features dt={dt} rows={n}")


def export_dashboard_snapshot(spark: SparkSession, paths: Paths, dt: str) -> None:
    """导出风控日报全量历史 CSV 快照，供看板做趋势（Spark 直出，pandas 直读，
    避免本地环境对 pyarrow 的依赖）。"""
    ads = spark.read.parquet(paths.ads("ads_risk_daily")).orderBy("apply_date")
    ads.coalesce(1).write.mode("overwrite").option("header", "true").csv(paths.ads_export("ads_risk_daily"))
    print(f"[ads] exported dashboard snapshot (history up to dt={dt})")


def run(dt: str) -> None:
    cfg = load_config()
    paths = Paths.from_config(cfg)
    spark = get_spark(
        app_name=f"{cfg['spark']['app_name']}-ads",
        master=cfg["spark"]["master"],
        shuffle_partitions=cfg["spark"]["shuffle_partitions"],
    )
    try:
        build_risk_daily(spark, paths, dt)
        build_scorecard_features(spark, paths, dt)
        export_dashboard_snapshot(spark, paths, dt)
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="ADS 层应用建模")
    parser.add_argument("--dt", required=True, help="业务日期 YYYY-MM-DD")
    args = parser.parse_args()
    run(args.dt)


if __name__ == "__main__":
    main()
