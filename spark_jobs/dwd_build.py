"""DWD 层：明细数据层（清洗 + 维度建模）。

职责：
- dwd_dim_customer：客户维度拉链表（SCD2），记录属性变更历史，
  通过 start_date/end_date 维护版本，支撑"按业务日期回溯当时客户状态"；
- dwd_fact_loan_apply：贷款申请事实表（申请粒度，关联客户维度代理键）；
- dwd_fact_repay：还款流水事实表（还款粒度，含逾期分桶）。

设计要点：
- 事实表只存维度代理键（customer_sk），不冗余维度属性——保证口径单一出处；
- 拉链表合并逻辑：快照 vs 当前版本逐属性比对，变更则关闭旧版本、插入新版本，
  未变更保留，新增客户直接插入；本作业用 overwrite 全量重写维度表，
  生产环境（Hudi/Iceberg）改用 merge into 增量更新。
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from spark_jobs.common.paths import END_DATE, Paths, load_config  # noqa: E402
from spark_jobs.common.spark_session import get_spark  # noqa: E402
from table_store import TableStore  # noqa: E402

DIM_ATTRS = ["gender", "age", "education", "marital_status", "occupation", "city"]
DIM_TABLE = "dwd_dim_customer"
DIM_KEYS = ["customer_id", "start_date"]
# 固定列序：merge 输出为「键在前」，与首快照列序不同；
# 增量两侧必须按同一显式列序 select 后再 union，否则按位置错位
DIM_COLS = ["customer_id", *DIM_ATTRS, "customer_sk", "start_date", "end_date"]


def build_dim_customer(spark: SparkSession, paths: Paths, dt: str) -> None:
    """客户维度拉链表（SCD2），基于 TableStore 快照机制。

    每日把"关闭的旧版本 + 新版本"作为增量，按 (customer_id, start_date)
    merge 进存量全量，产出不可变新快照；历史快照保留，可时间旅行回溯。
    """
    store = TableStore()
    prev_date = (datetime.strptime(dt, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")

    snap = spark.read.parquet(paths.ods("customer", dt)).select("customer_id", *DIM_ATTRS)
    parent = store.current_snapshot(DIM_TABLE)

    if parent is None:
        # 首次加载：快照全部作为 V1 版本
        merged = (
            snap.withColumn("customer_sk", F.xxhash64("customer_id", F.lit(dt)))
            .withColumn("start_date", F.lit(dt))
            .withColumn("end_date", F.lit(END_DATE))
        )
    else:
        current = spark.read.parquet(str(store.snapshot_path(DIM_TABLE, parent)))
        cur = current.where(F.col("end_date") == END_DATE)

        # 快照左连当前版本，找出"新客户"与"属性变更客户"
        joined = snap.alias("s").join(cur.alias("c"), "customer_id", "left")
        changed = joined.where(
            F.col("c.customer_id").isNull()
            | (F.col("s.occupation") != F.col("c.occupation"))
            | (F.col("s.city") != F.col("c.city"))
            | (F.col("s.age") != F.col("c.age"))
            | (F.col("s.education") != F.col("c.education"))
        )
        changed_ids = changed.select("customer_id")

        # 增量 = 被变更旧版本关闭（end_date 到前一天）+ 新版本（start_date = dt）；
        # 两侧按 DIM_COLS 显式列序 select 后再 union（防按位置错位）
        closed = (
            cur.join(changed_ids, "customer_id", "left_semi")
            .withColumn("end_date", F.lit(prev_date))
            .select(*DIM_COLS)
        )
        new_versions = (
            changed.select("s.*")
            .withColumn("customer_sk", F.xxhash64("customer_id", F.lit(dt)))
            .withColumn("start_date", F.lit(dt))
            .withColumn("end_date", F.lit(END_DATE))
            .select(*DIM_COLS)
        )
        incoming = closed.union(new_versions)

        # 通用 merge：主键 (customer_id, start_date) 命中则覆盖（关闭旧版本），
        # 未命中则插入（新版本）；未涉及的历史版本与未变更版本原样保留
        merged = store.merge(current, incoming, DIM_KEYS)

    new_id = store.new_snapshot_id(DIM_TABLE)
    out_path = store.snapshot_path(DIM_TABLE, new_id)
    out_path.mkdir(parents=True, exist_ok=True)
    merged.write.mode("overwrite").parquet(str(out_path))
    store.write_manifest(DIM_TABLE, new_id, parent, merged.count(), DIM_KEYS)
    print(f"[dwd] {DIM_TABLE} snapshot={new_id} -> {out_path}")


def build_fact_apply(spark: SparkSession, paths: Paths, dt: str) -> None:
    """贷款申请事实表：关联业务日期当时有效的客户维度版本。"""
    apps = spark.read.parquet(paths.ods("loan_application", dt))
    dim = spark.read.parquet(str(TableStore().snapshot_path(DIM_TABLE)))
    apply_date = F.to_date(F.col("apply_time"))

    fact = (
        apps.alias("a")
        .join(
            dim.alias("d"),
            (F.col("a.customer_id") == F.col("d.customer_id"))
            & (F.col("d.start_date") <= apply_date)
            & (apply_date <= F.col("d.end_date")),
            "left",
        )
        .select(
            F.col("a.application_id"),
            F.col("a.customer_id"),
            F.col("d.customer_sk"),
            F.col("a.apply_time"),
            apply_date.alias("apply_date"),
            F.col("a.loan_amnt"),
            F.col("a.term"),
            F.col("a.interest_rate"),
            F.col("a.purpose"),
            F.col("a.grade"),
            F.col("a.annual_income"),
            F.col("a.dti"),
            F.col("a.fico_score"),
            F.col("a.verification_status"),
            F.col("a.home_ownership"),
            F.col("a.is_approved"),
            F.col("a.is_default"),
        )
    )
    fact.write.mode("overwrite").parquet(paths.dwd("dwd_fact_loan_apply", dt))
    n = spark.read.parquet(paths.dwd("dwd_fact_loan_apply", dt)).count()
    print(f"[dwd] dwd_fact_loan_apply dt={dt} rows={n}")


def build_fact_repay(spark: SparkSession, paths: Paths, dt: str) -> None:
    """还款流水事实表：逾期天数分桶（M0/M1/M2/M3+）。"""
    flows = spark.read.parquet(paths.ods("repay_flow", dt))
    dim = spark.read.parquet(str(TableStore().snapshot_path(DIM_TABLE)))
    repay_date = F.to_date(F.col("repay_time"))

    fact = (
        flows.alias("f")
        .join(
            dim.alias("d"),
            (F.col("f.customer_id") == F.col("d.customer_id"))
            & (F.col("d.start_date") <= repay_date)
            & (repay_date <= F.col("d.end_date")),
            "left",
        )
        .select(
            F.col("f.flow_id"),
            F.col("f.application_id"),
            F.col("f.customer_id"),
            F.col("d.customer_sk"),
            F.col("f.due_date"),
            F.col("f.repay_time"),
            F.col("f.due_amount"),
            F.col("f.repay_amount"),
            F.col("f.overdue_days"),
            F.when(F.col("f.overdue_days") == 0, "M0")
            .when(F.col("f.overdue_days") <= 30, "M1")
            .when(F.col("f.overdue_days") <= 60, "M2")
            .otherwise("M3+")
            .alias("overdue_bucket"),
            (F.col("f.due_amount") - F.col("f.repay_amount")).alias("unpaid_amount"),
        )
    )
    fact.write.mode("overwrite").parquet(paths.dwd("dwd_fact_repay", dt))
    n = spark.read.parquet(paths.dwd("dwd_fact_repay", dt)).count()
    print(f"[dwd] dwd_fact_repay dt={dt} rows={n}")


def run(dt: str) -> None:
    cfg = load_config()
    paths = Paths.from_config(cfg)
    spark = get_spark(
        app_name=f"{cfg['spark']['app_name']}-dwd",
        master=cfg["spark"]["master"],
        shuffle_partitions=cfg["spark"]["shuffle_partitions"],
    )
    try:
        build_dim_customer(spark, paths, dt)
        build_fact_apply(spark, paths, dt)
        build_fact_repay(spark, paths, dt)
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="DWD 层建模（拉链表 + 事实表）")
    parser.add_argument("--dt", required=True, help="业务日期 YYYY-MM-DD")
    args = parser.parse_args()
    run(args.dt)


if __name__ == "__main__":
    main()
