"""数据质量检查：完整性 / 准确性 / 稳定性 三类规则。

检查项：
1. 主键唯一率（完整性）：count(*) vs count(distinct pk)；
2. 关键列空值率（准确性）：关键列为空的行占比；
3. 行数波动（稳定性）：与 ledger 中上一业务日行数对比，超阈值告警。

产出：quality/quality_report.json（机读）+ 控制台摘要；任一规则失败退出码为 1，
Airflow 任务随之标红并阻断下游。ledger 记录每表每日行数，作为波动基线。

用法：python -m quality.checks --dt 2026-09-22
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

from spark_jobs.common.paths import Paths, PROJECT_ROOT, load_config  # noqa: E402
from spark_jobs.common.spark_session import get_spark  # noqa: E402

from table_store import TableStore  # noqa: E402

# 表级检查规格：(layer, table, 主键列, 关键非空列, 是否按 dt 分区)
# 注意：ODS 层目录为 warehouse/ods/{业务表名}，表名不带层前缀；
# DWD/DWS/ADS 层目录为 warehouse/{layer}/{完整表名}，表名带层前缀；
# dwd_dim_customer 由 TableStore 管理（warehouse/tables/ 下的快照链），单独取当前快照。
TABLE_SPECS = [
    ("ods", "customer", ["customer_id"], ["customer_id"], True),
    ("ods", "loan_application", ["application_id"], ["application_id", "customer_id"], True),
    ("ods", "repay_flow", ["flow_id"], ["flow_id", "application_id"], True),
    ("dwd", "dwd_dim_customer", ["customer_id", "start_date"], ["customer_id", "customer_sk"], False),
    ("dwd", "dwd_fact_loan_apply", ["application_id"], ["application_id", "customer_id"], True),
    ("dwd", "dwd_fact_repay", ["flow_id"], ["flow_id", "application_id"], True),
    ("dws", "dws_apply_day", ["apply_date"], ["apply_date"], True),
    ("dws", "dws_customer_day", ["customer_id", "dt"], ["customer_id"], True),
    ("ads", "ads_scorecard_features", ["application_id"], ["application_id"], True),
]


def _table_path(paths: Paths, layer: str, table: str, dt: str, partitioned: bool) -> str:
    if table == "dwd_dim_customer":
        return str(TableStore().snapshot_path(table))
    return getattr(paths, layer)(table, dt) if partitioned else getattr(paths, layer)(table)


def _read_ledger(ledger_path: Path) -> dict:
    """读取行数台账：{(table, dt): rowcount}。"""
    import csv

    ledger = {}
    if ledger_path.is_file():
        for row in csv.DictReader(ledger_path.read_text(encoding="utf-8").splitlines()):
            try:
                ledger[(row["table"], row["dt"])] = int(row["rowcount"])
            except (KeyError, ValueError, TypeError):
                continue  # 跳过异常行（如历史版本重复写入的表头）
    return ledger


def _append_ledger(ledger_path: Path, table: str, dt: str, rowcount: int) -> None:
    import csv
    import io

    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    need_header = (not ledger_path.exists()) or ledger_path.stat().st_size == 0
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["table", "dt", "rowcount", "checked_at"])
    if need_header:
        writer.writeheader()
    writer.writerow(
        {
            "table": table,
            "dt": dt,
            "rowcount": rowcount,
            "checked_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
    )
    with ledger_path.open("a", encoding="utf-8") as f:
        f.write(buf.getvalue())


def check_table(spark: SparkSession, paths: Paths, spec: tuple, dt: str, ledger: dict, thresholds: dict) -> dict:
    layer, table, pk_cols, not_null_cols, partitioned = spec
    path = _table_path(paths, layer, table, dt, partitioned)

    result = {"table": table, "layer": layer, "path": path, "dt": dt, "checks": [], "passed": True}
    if not os.path.exists(path):
        result["checks"].append({"rule": "existence", "passed": False, "detail": "路径不存在"})
        result["passed"] = False
        return result

    df = spark.read.parquet(path)
    total = df.count()
    result["rowcount"] = total
    if total == 0:
        result["checks"].append({"rule": "non_empty", "passed": False, "detail": "0 行"})
        result["passed"] = False
        return result

    # 1) 主键唯一率
    distinct_pk = df.select(*pk_cols).distinct().count()
    uniqueness = distinct_pk / total
    ok = uniqueness >= thresholds["pk_uniqueness_min"]
    result["checks"].append(
        {"rule": "pk_uniqueness", "passed": ok, "value": round(uniqueness, 6), "detail": f"{distinct_pk}/{total}"}
    )

    # 2) 关键列空值率
    null_exprs = [F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in not_null_cols]
    null_row = df.agg(*null_exprs).collect()[0].asDict()
    max_null_rate = 0.0
    for c in not_null_cols:
        rate = (null_row[c] or 0) / total
        max_null_rate = max(max_null_rate, rate)
    ok = max_null_rate <= thresholds["max_null_rate"]
    result["checks"].append({"rule": "null_rate", "passed": ok, "value": round(max_null_rate, 6)})

    # 3) 行数波动（与台账中同表上一行数对比）
    prev = None
    for (t, d), n in ledger.items():
        if t == table and d < dt:
            if prev is None or d > prev[0]:
                prev = (d, n)
    if prev is not None and prev[1] > 0:
        volatility = abs(total - prev[1]) / prev[1]
        ok = volatility <= thresholds["rowcount_volatility"]
        result["checks"].append(
            {"rule": "rowcount_volatility", "passed": ok, "value": round(volatility, 4), "detail": f"prev={prev[1]}"}
        )

    result["passed"] = all(c["passed"] for c in result["checks"])
    _append_ledger(Path(thresholds["ledger_file"]), table, dt, total)
    return result


def run(dt: str) -> bool:
    cfg = load_config()
    paths = Paths.from_config(cfg)
    thresholds = dict(cfg["quality"])
    thresholds["ledger_file"] = os.path.join(PROJECT_ROOT, cfg["quality"]["ledger_file"])

    ledger = _read_ledger(Path(thresholds["ledger_file"]))
    spark = get_spark(
        app_name=f"{cfg['spark']['app_name']}-quality",
        master=cfg["spark"]["master"],
        shuffle_partitions=cfg["spark"]["shuffle_partitions"],
    )
    results = []
    try:
        for spec in TABLE_SPECS:
            results.append(check_table(spark, paths, spec, dt, ledger, thresholds))
    finally:
        spark.stop()

    report = {"dt": dt, "generated_at": datetime.now().isoformat(), "tables": results}
    report_path = Path(PROJECT_ROOT, "quality", "quality_report.json")
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    failed = [r for r in results if not r["passed"]]
    for r in results:
        status = "PASS" if r["passed"] else "FAIL"
        detail = "; ".join(f"{c['rule']}={c.get('value', c.get('detail'))}" for c in r["checks"])
        print(f"[quality] {status} {r['table']} rows={r.get('rowcount', '-')} ({detail})")
    print(f"[quality] report -> {report_path}")
    if failed:
        print(f"[quality] FAILED tables: {[r['table'] for r in failed]}")
        return False
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="数据质量检查")
    parser.add_argument("--dt", required=True, help="业务日期 YYYY-MM-DD")
    args = parser.parse_args()
    sys.exit(0 if run(args.dt) else 1)


if __name__ == "__main__":
    main()
