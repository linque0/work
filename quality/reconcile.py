"""流批对账：同一指标在离线链路与实时链路的双跑比对。

对账口径：
- 离线侧：dwd_fact_repay（dt 分区）的 repay_amount 合计、笔数；
- 实时侧（按优先级取第一个可用）：
  1. Flink filesystem sink：warehouse/ads/flink_repay_metrics 下的 JSON 文件
     （window_start 属于 dt 的窗口聚合）；
  2. Spark Structured Streaming 表：ads_realtime_metrics（parquet）。

两侧数据同源（生成器把同一批还款事件分别落批量快照与 Kafka topic），
因此相对偏差应 <= reconcile.tolerance（默认 1%）。

用法：python -m quality.reconcile --dt 2026-09-22
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from spark_jobs.common.paths import PROJECT_ROOT, Paths, load_config  # noqa: E402
from spark_jobs.common.spark_session import get_spark  # noqa: E402


def read_flink_sink(sink_dir: str, dt: str) -> tuple[int, float] | None:
    """读取 Flink filesystem sink（含 .part-*.inprogress.* 滚动中的文件）。

    返回 (msg_cnt 合计, repay_amt_sum 合计)；目录不存在或无数据返回 None。
    """
    sink = Path(sink_dir)
    if not sink.is_dir():
        return None
    total_cnt, total_amt = 0, 0.0
    found = False
    for f in sink.iterdir():
        if not f.is_file() or f.name.startswith("._"):
            continue
        try:
            text = f.read_text(encoding="utf-8")
        except OSError:
            continue
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue  # 滚动写入可能读到半行
            if not str(row.get("window_start", "")).startswith(dt):
                continue
            found = True
            total_cnt += int(row.get("msg_cnt") or 0)
            total_amt += float(row.get("repay_amt_sum") or 0.0)
    return (total_cnt, round(total_amt, 2)) if found else None


def run(dt: str) -> bool:
    cfg = load_config()
    paths = Paths.from_config(cfg)
    tolerance = cfg["reconcile"]["tolerance"]

    spark = get_spark(
        app_name=f"{cfg['spark']['app_name']}-reconcile",
        master=cfg["spark"]["master"],
        shuffle_partitions=cfg["spark"]["shuffle_partitions"],
    )

    report = {"dt": dt, "generated_at": datetime.now().isoformat(), "metrics": [], "passed": True}
    try:
        # ---------- 离线侧 ----------
        batch = spark.read.parquet(paths.dwd("dwd_fact_repay", dt))
        batch_agg = batch.agg(
            F.count("*").alias("cnt"),
            F.round(F.sum("repay_amount"), 2).alias("amt"),
        ).collect()[0]
        batch_cnt, batch_amt = batch_agg["cnt"], batch_agg["amt"] or 0.0

        # ---------- 实时侧：优先 Flink sink，回退 Spark Streaming 表 ----------
        rt_source = None
        rt_cnt = rt_amt = None

        flink_result = read_flink_sink(paths.ads("flink_repay_metrics"), dt)
        if flink_result is not None:
            rt_source = "flink-filesystem-sink"
            rt_cnt, rt_amt = flink_result
        else:
            rt_path = paths.ads("ads_realtime_metrics")
            if os.path.exists(rt_path):
                rt = spark.read.parquet(rt_path).where(F.to_date("window_start") == dt)
                rt_agg = rt.agg(
                    F.sum("msg_cnt").alias("cnt"),
                    F.round(F.sum("repay_amt_sum"), 2).alias("amt"),
                ).collect()[0]
                if rt_agg["cnt"] is not None:
                    rt_source = "spark-structured-streaming"
                    rt_cnt, rt_amt = rt_agg["cnt"] or 0, rt_agg["amt"] or 0.0

        if rt_source is None:
            report["passed"] = False
            report["error"] = "实时侧无数据：Flink sink 与 Spark Streaming 表均为空"
        else:
            report["realtime_source"] = rt_source
            for name, b_val, r_val in (("repay_cnt", batch_cnt, rt_cnt), ("repay_amt", batch_amt, rt_amt)):
                diff = abs(r_val - b_val)
                rel = diff / b_val if b_val else (0.0 if r_val == 0 else 1.0)
                ok = rel <= tolerance
                report["metrics"].append(
                    {
                        "metric": name,
                        "batch": b_val,
                        "realtime": r_val,
                        "abs_diff": round(diff, 4),
                        "rel_diff": round(rel, 6),
                        "tolerance": tolerance,
                        "passed": ok,
                    }
                )
                report["passed"] = report["passed"] and ok
    finally:
        spark.stop()

    report_path = Path(PROJECT_ROOT, "quality", "reconcile_report.json")
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    for m in report.get("metrics", []):
        status = "PASS" if m["passed"] else "FAIL"
        print(
            f"[reconcile] {status} {m['metric']}: batch={m['batch']} realtime={m['realtime']} "
            f"rel_diff={m['rel_diff']} (tol={m['tolerance']}, src={report.get('realtime_source')})"
        )
    if report.get("error"):
        print(f"[reconcile] ERROR: {report['error']}")
    print(f"[reconcile] report -> {report_path}")
    return report["passed"]


def main() -> None:
    parser = argparse.ArgumentParser(description="流批对账")
    parser.add_argument("--dt", required=True, help="业务日期 YYYY-MM-DD")
    args = parser.parse_args()
    sys.exit(0 if run(args.dt) else 1)


if __name__ == "__main__":
    main()
