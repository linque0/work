"""bench/skew_bench.py：数据倾斜实战基准。

场景：按客户维度聚合时，少数头部客户贡献绝大部分流水（幂律分布），
直接 groupBy 会让热点 key 进入单个 reduce 分区——个别 task 耗时数倍于
中位数，整体作业被拖慢（数据开发面试高频题）。

对比两种方案（同一份倾斜数据、同一集群）：
  A. 基线：直接 groupBy(customer_id).agg(sum(amt))
  B. 优化：两阶段加盐聚合——
     1) 打散：customer_id -> (customer_id, salt)，salt = rand % N，热点 key
        被均匀打散到 N 个分区；
     2) 局部聚合：groupBy(customer_id, salt).agg(sum(amt))；
     3) 合并：groupBy(customer_id).agg(sum(...))，此时每个 key 只有 N 行，
        shuffle 量从 O(热点行数) 降为 O(N)。

指标采集：开启 Spark EventLog，每个变体跑完后解析新增事件中的
SparkListenerTaskEnd（task 级 shuffle 读写字节、执行耗时），
输出 max/median task 耗时、总 shuffle 字节、墙钟时间前后对比。

用法：python bench/skew_bench.py [--rows 300000] [--salts 16]
"""
from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from spark_jobs.common.spark_session import _configure_windows_native  # noqa: E402

PROJECT_ROOT = Path(__file__).resolve().parent.parent
EVENT_LOG_DIR = PROJECT_ROOT / "quality" / "skew_eventlog"


class EventLogMetrics:
    """基于 Spark EventLog 的 task 级指标采集（按字节偏移读取新增事件）。"""

    def __init__(self, spark: SparkSession):
        # eventLog.enabled / dir 已在 main() 的 builder 中设置（启动期配置，
        # SparkContext 创建后无法再修改），这里只按偏移读取新增事件
        self.log_dir = EVENT_LOG_DIR
        self._offset = 0
        self._sync_offset()
    def _log_file(self) -> Path | None:
        # Spark 事件日志写在 eventlog_v2_*/events_* 子目录结构中
        files = sorted(self.log_dir.rglob("events_*"), key=lambda p: p.stat().st_mtime)
        return files[-1] if files else None

    def _sync_offset(self) -> None:
        f = self._log_file()
        self._offset = f.stat().st_size if f else 0

    def read_new_events(self) -> list[dict]:
        """读取自上次偏移以来的新事件（JSON Lines）。"""
        f = self._log_file()
        if not f:
            return []
        size = f.stat().st_size
        if size <= self._offset:
            return []
        with f.open("rb") as fh:
            fh.seek(self._offset)
            chunk = fh.read(size - self._offset).decode("utf-8", errors="replace")
        self._offset = size
        events = []
        for line in chunk.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                continue  # 读取瞬间可能撞上半行
        return events

    def collect_task_metrics(self) -> dict:
        """聚合新增事件中的 task 指标。"""
        durations, shuffle_write, shuffle_read, tasks = [], 0, 0, 0
        for ev in self.read_new_events():
            if ev.get("Event") != "SparkListenerTaskEnd":
                continue
            tasks += 1
            tm = ev.get("Task Metrics") or {}
            durations.append(tm.get("Executor Run Time") or 0)
            swm = tm.get("Shuffle Write Metrics") or {}
            srm = tm.get("Shuffle Read Metrics") or {}
            shuffle_write += swm.get("Shuffle Bytes Written") or 0
            shuffle_read += srm.get("Total Bytes Read") or 0
        return {
            "tasks": tasks,
            "task_duration_max_ms": max(durations) if durations else 0,
            "task_duration_median_ms": int(statistics.median(durations)) if durations else 0,
            "shuffle_write_mb": round(shuffle_write / 1024 / 1024, 2),
            "shuffle_read_mb": round(shuffle_read / 1024 / 1024, 2),
        }


def build_skewed_df(spark: SparkSession, rows: int, hot_ratio: float = 0.95, hot_keys: int = 100):
    """合成幂律分布流水：hot_ratio 的行集中到 hot_keys 个热点客户。"""
    base = spark.range(0, rows, 1, numPartitions=8)
    return (
        base.withColumn("r", F.rand(42))
        .withColumn(
            "customer_id",
            F.when(
                F.col("r") < hot_ratio,
                F.concat(F.lit("C"), (F.col("id") % hot_keys).cast("string")),
            ).otherwise(
                F.concat(F.lit("C"), (hot_keys + F.col("id") % 100000).cast("string"))
            ),
        )
        .withColumn("amt", F.round(F.rand(43) * 5000, 2))
        .select("customer_id", "amt")
    )


def run_variant(spark: SparkSession, df, metrics: EventLogMetrics, salted: bool, salts: int) -> dict:
    """执行一个变体并返回指标（触发一次 action 让作业完整跑完）。"""
    metrics.read_new_events()  # 丢弃上一变体残留事件
    t0 = time.time()

    if salted:
        result = (
            df.withColumn("salt", (F.rand(44) * salts).cast("int"))
            .groupBy("customer_id", "salt")
            .agg(F.sum("amt").alias("local_amt"))
            .groupBy("customer_id")
            .agg(F.sum("local_amt").alias("total_amt"))
        )
    else:
        result = df.groupBy("customer_id").agg(F.sum("amt").alias("total_amt"))

    out_rows = result.count()  # action：触发 shuffle
    wall_ms = int((time.time() - t0) * 1000)

    # 事件日志异步刷盘，等待落盘后再按偏移读取
    time.sleep(2)
    m = metrics.collect_task_metrics()
    m.update({"wall_ms": wall_ms, "out_rows": out_rows})
    return m


def main() -> None:
    parser = argparse.ArgumentParser(description="数据倾斜实战基准")
    parser.add_argument("--rows", type=int, default=300000)
    parser.add_argument("--salts", type=int, default=16)
    args = parser.parse_args()

    _configure_windows_native()
    EVENT_LOG_DIR.mkdir(parents=True, exist_ok=True)  # 事件日志目录须先于 SparkContext 创建
    spark = (
        SparkSession.builder.appName("skew-bench")
        .master("local[*]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.compress", "false")  # 关闭压缩，便于 Python 直接解析
        .config("spark.eventLog.dir", EVENT_LOG_DIR.as_uri())
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        df = build_skewed_df(spark, args.rows).cache()
        total = df.count()
        distinct = df.select("customer_id").distinct().count()
        print(f"[bench] rows={total} distinct_customers={distinct} salts={args.salts}")

        metrics = EventLogMetrics(spark)
        metrics.read_new_events()  # 跳过 cache/count 阶段的事件

        baseline = run_variant(spark, df, metrics, salted=False, salts=args.salts)
        print(f"[bench] baseline : {baseline}")

        salted = run_variant(spark, df, metrics, salted=True, salts=args.salts)
        print(f"[bench] salted   : {salted}")

        speedup = round(baseline["wall_ms"] / salted["wall_ms"], 2) if salted["wall_ms"] else None
        report = {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "rows": total,
            "distinct_customers": distinct,
            "salts": args.salts,
            "baseline": baseline,
            "salted_two_phase": salted,
            "wall_speedup": speedup,
        }
        out = PROJECT_ROOT / "quality" / "skew_bench_report.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[bench] report -> {out}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
