"""端到端冒烟测试：生成器 -> ODS -> DWD -> DWS -> ADS -> 实时 -> 质量 -> 对账。

用法：
    python tests/smoke_test.py                 # 默认小数据量
    python tests/smoke_test.py --keep-going    # 失败不中断，跑完看全貌

测试策略：
- 两个业务日期（D-2, D-1）验证跨日累积口径与拉链表版本合并；
- 断言各层 parquet 行数、质量报告通过、流批对账通过；
- 全部步骤进程内调用各模块 run()，不启子进程，避免命令行拼接。
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Windows 本地开发：提供 hadoop 原生库路径（Linux 集群无需）
HADOOP_HOME = PROJECT_ROOT / ".hadoop"
if HADOOP_HOME.is_dir():
    os.environ.setdefault("HADOOP_HOME", str(HADOOP_HOME))

PASSED: list[str] = []
FAILED: list[str] = []


def run_step(name: str, fn, keep_going: bool = False) -> bool:
    print(f"\n{'=' * 60}\n>>> {name}\n{'=' * 60}", flush=True)
    try:
        result = fn()
        ok = result is not False  # quality/reconcile 的 run() 返回布尔
    except Exception as exc:  # noqa: BLE001 - 测试框架需要捕获所有步骤异常
        print(f"--- {name} raised: {exc!r}", flush=True)
        ok = False
    if ok:
        PASSED.append(name)
        print(f"--- {name} PASS", flush=True)
        return True
    FAILED.append(name)
    print(f"--- {name} FAIL", flush=True)
    if not keep_going:
        sys.exit(1)
    return False


def gen_step(dt: str, n_customers: int, n_applications: int, stream: int):
    def _fn():
        from data import generator

        sys.argv = [
            "generator",
            "--dt", dt,
            "--n-customers", str(n_customers),
            "--n-applications", str(n_applications),
            "--stream", str(stream),
        ]
        generator.main()

    return _fn


def main() -> None:
    parser = argparse.ArgumentParser(description="端到端冒烟测试")
    parser.add_argument("--keep-going", action="store_true")
    parser.add_argument("--n-customers", type=int, default=80)
    parser.add_argument("--n-applications", type=int, default=200)
    args = parser.parse_args()

    from data import generator  # noqa: F401  提前暴露导入错误
    from governance import validate_schema
    from quality import checks, reconcile
    from spark_jobs import ads_build, dwd_build, dws_build, ods_ingest, streaming_job
    from spark_jobs.common.paths import Paths, load_config
    from spark_jobs.common.spark_session import get_spark
    from table_store import TableStore

    cfg = load_config()
    paths = Paths.from_config(cfg)

    d2 = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
    d1 = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    def gen(dt):
        return gen_step(dt, args.n_customers, args.n_applications, 3)

    def pipeline(dt: str):
        run_step(f"gen {dt}", gen(dt), args.keep_going)
        run_step(f"ods {dt}", lambda: ods_ingest.run(dt), args.keep_going)
        run_step(f"dwd {dt}", lambda: dwd_build.run(dt), args.keep_going)
        run_step(f"dws {dt}", lambda: dws_build.run(dt), args.keep_going)
        run_step(f"ads {dt}", lambda: ads_build.run(dt), args.keep_going)
        # 实时链路必须早于对账：先消费当日流消息，再与离线侧双跑比对
        run_step(f"streaming once {dt}", lambda: streaming_job.run(once=True, sink="parquet"), args.keep_going)
        run_step(f"quality {dt}", lambda: checks.run(dt), args.keep_going)
        run_step(f"reconcile {dt}", lambda: reconcile.run(dt), args.keep_going)

    pipeline(d2)
    pipeline(d1)

    # ---------- 治理：TableStore 快照链 + Schema 漂移检测 ----------
    store = TableStore()
    snaps = store.history("dwd_dim_customer")
    print(f"[governance] dwd_dim_customer snapshots={len(snaps)}")
    for s in snaps:
        print(f"  - snapshot={s['snapshot_id']} parent={s['parent_id']} rows={s['row_count']}")
    assert len(snaps) >= 2, "TableStore 快照链应至少 2 个快照（两个业务日）"
    PASSED.append("table_store snapshot chain")

    run_step("schema drift", lambda: validate_schema.validate(None, None), args.keep_going)

    # ---------- 行数断言 ----------
    print(f"\n{'=' * 60}\n>>> 行数断言\n{'=' * 60}", flush=True)
    spark = get_spark(app_name="smoke-count", master="local[1]", shuffle_partitions=2)
    targets = {
        "ods_customer": paths.ods("customer", d1),
        "ods_loan_application": paths.ods("loan_application", d1),
        "ods_repay_flow": paths.ods("repay_flow", d1),
        "dwd_dim_customer": str(TableStore().snapshot_path("dwd_dim_customer")),
        "dwd_fact_loan_apply": paths.dwd("dwd_fact_loan_apply", d1),
        "dwd_fact_repay": paths.dwd("dwd_fact_repay", d1),
        "dws_customer_day": paths.dws("dws_customer_day", d1),
        "ads_scorecard_features": paths.ads("ads_scorecard_features", d1),
    }
    counts = {}
    try:
        for name, p in targets.items():
            counts[name] = spark.read.parquet(p).count()
            print(f"  {counts[name]:>8}  {name}")
    finally:
        spark.stop()

    for name in ("ods_loan_application", "ods_repay_flow", "dwd_fact_loan_apply", "ads_scorecard_features"):
        assert counts.get(name, 0) > 0, f"{name} 为空"
    PASSED.append("rowcount assertions")

    print(f"\n{'=' * 60}")
    print(f"SMOKE TEST DONE: {len(PASSED)} passed, {len(FAILED)} failed")
    if FAILED:
        print(f"failed: {FAILED}")
        sys.exit(1)
    print("ALL PASS")


if __name__ == "__main__":
    main()
