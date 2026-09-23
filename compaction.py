"""compaction：小文件治理工具（类 Iceberg/Hudi 的 compact 机制）。

问题：增量/流式写入与按天分区会产生大量小文件，拖慢读取、增加元数据压力。
方案：
1. 扫描目标目录的文件数与平均大小；
2. 触发条件（满足其一）：文件数 > max_files，或平均大小 < min_avg_mb；
3. 重写：读入后 coalesce 到目标文件数，写入 .compact_tmp 临时目录；
4. 原子替换：逐文件删除旧数据（pathlib unlink），临时目录 rename 上位；
5. 输出前后对比报告（文件数/平均大小/总大小），落盘 quality/compaction_report.json。

用法：
    python -m compaction --table dwd_fact_repay --dt 2026-09-27
    python -m compaction --table dwd_fact_loan_apply --dt 2026-09-27 --target-files 2
    python -m compaction --table dws_customer_day --dt 2026-09-27 --dry-run
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from spark_jobs.common.paths import PROJECT_ROOT, Paths, load_config  # noqa: E402
from spark_jobs.common.spark_session import get_spark  # noqa: E402

# 可治理的表登记（layer, 是否按 dt 分区）
COMPACTABLE = {
    "dwd_fact_loan_apply": ("dwd", True),
    "dwd_fact_repay": ("dwd", True),
    "dws_apply_day": ("dws", True),
    "dws_customer_day": ("dws", True),
    "ads_scorecard_features": ("ads", True),
}


def _scan_dir(path: Path) -> dict:
    """统计目录下 parquet 数据文件的数量与大小（忽略 _SUCCESS 等标记文件）。"""
    files = [p for p in path.rglob("*") if p.is_file() and not p.name.startswith(("_", "."))]
    sizes = [p.stat().st_size for p in files]
    return {
        "files": len(files),
        "total_mb": round(sum(sizes) / 1024 / 1024, 3),
        "avg_mb": round(sum(sizes) / len(sizes) / 1024 / 1024, 3) if sizes else 0.0,
    }


def _remove_dir(path: Path) -> None:
    """递归删除目录（逐文件 unlink + 逆序 rmdir）。"""
    for f in path.rglob("*"):
        if f.is_file():
            f.unlink()
    for d in sorted((p for p in path.rglob("*") if p.is_dir()), reverse=True):
        d.rmdir()
    path.rmdir()


def compact(table: str, dt: str, target_files: int, dry_run: bool) -> dict:
    cfg = load_config()
    paths = Paths.from_config(cfg)
    if table not in COMPACTABLE:
        raise ValueError(f"表不在可治理清单: {table}（{list(COMPACTABLE)}）")
    layer, partitioned = COMPACTABLE[table]
    target = Path(getattr(paths, layer)(table, dt) if partitioned else getattr(paths, layer)(table))
    if not target.is_dir():
        raise FileNotFoundError(f"目录不存在: {target}")

    thresholds = {
        "max_files": int(cfg.get("compaction", {}).get("max_files", 50)),
        "min_avg_mb": float(cfg.get("compaction", {}).get("min_avg_mb", 8)),
    }
    before = _scan_dir(target)
    need = before["files"] > thresholds["max_files"] or (
        before["files"] > 0 and before["avg_mb"] < thresholds["min_avg_mb"]
    )

    report = {
        "table": table,
        "dt": dt,
        "path": str(target),
        "before": before,
        "thresholds": thresholds,
        "triggered": need,
        "dry_run": dry_run,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }

    if not need:
        report["action"] = "skip"
        print(f"[compaction] {table} dt={dt} 无需治理（files={before['files']} avg={before['avg_mb']}MB）")
        return report
    if dry_run:
        report["action"] = "dry-run"
        print(f"[compaction] {table} dt={dt} 触发治理（dry-run，不执行）")
        return report

    spark = get_spark(
        app_name=f"{cfg['spark']['app_name']}-compaction",
        master=cfg["spark"]["master"],
        shuffle_partitions=cfg["spark"]["shuffle_partitions"],
    )
    tmp = target.parent / (target.name + ".compact_tmp")
    try:
        df = spark.read.parquet(str(target))
        df.coalesce(target_files).write.mode("overwrite").parquet(str(tmp))
        # 原子替换（崩溃安全）：旧目录先改名避让 -> 临时目录上位 -> 清理避让目录；
        # 任一步失败可回退，不会出现「删了旧的、新的没上位」的数据丢失
        backup = target.parent / (target.name + ".compact_old")
        if backup.exists():
            _remove_dir(backup)
        target.rename(backup)
        try:
            tmp.rename(target)
        except OSError:
            backup.rename(target)  # 回滚：恢复旧目录
            raise
        _remove_dir(backup)
    finally:
        spark.stop()

    after = _scan_dir(target)
    report["after"] = after
    report["action"] = "compacted"
    print(
        f"[compaction] {table} dt={dt}: files {before['files']}->{after['files']}, "
        f"avg {before['avg_mb']}MB->{after['avg_mb']}MB"
    )
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="小文件治理")
    parser.add_argument("--table", required=True, choices=sorted(COMPACTABLE))
    parser.add_argument("--dt", required=True)
    parser.add_argument("--target-files", type=int, default=1)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    report = compact(args.table, args.dt, args.target_files, args.dry_run)
    out = Path(PROJECT_ROOT, "quality", "compaction_report.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
