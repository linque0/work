"""table_store：迷你湖仓表格式的核心机制（类 Iceberg/Hudi）。

职责边界（安全约束）：本模块只做"路径计算 + manifest 元数据 + 纯 DataFrame
合并计算"；parquet 的读写由调用方执行（调用处表名为字面量，路径来源可信）。

提供三件事：
1. merge()：按主键合并存量与增量 DataFrame（命中则更新、未命中则插入），
   纯计算不落盘——调用方把结果写入新快照目录，天然规避"读旧表+写同路径"；
2. 不可变快照：write_manifest() 记录父快照、时间戳、行数、操作类型，
   历史版本不被后续写入影响；
3. 时间旅行：snapshot_path()/current_snapshot()/history() 让调用方能定位
   并读取任意历史版本——数据可回溯是对账与口径修订的基础。

目录结构：
    warehouse/tables/{table}/
        snapshots/{snapshot_id}/
            data/*.parquet
            manifest.json

CLI：
    python -m table_store history dwd_dim_customer
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path

from pyspark.sql import DataFrame  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

# 本 store 登记在册的表（白名单：新表需在此登记后才可管理）
MANAGED_TABLES = ("dwd_dim_customer",)

_SAFE_NAME = re.compile(r"^[A-Za-z0-9_.-]+$")


def _validate_table(table: str) -> str:
    """表白名单 + 字符校验：阻断 ../ 穿越与未登记表。"""
    if table not in MANAGED_TABLES:
        raise ValueError(f"表未登记在 TableStore: {table!r}（MANAGED_TABLES={MANAGED_TABLES}）")
    if not _SAFE_NAME.match(table) or ".." in table:
        raise ValueError(f"非法表名: {table!r}")
    return table


def _validate_snapshot(snapshot_id: str) -> str:
    if not _SAFE_NAME.match(snapshot_id) or ".." in snapshot_id:
        raise ValueError(f"非法快照ID: {snapshot_id!r}")
    return snapshot_id


class TableStore:
    """版本化表存储的元数据与合并逻辑。parquet 读写由调用方执行。"""

    def __init__(self, root: str | None = None):
        default_root = os.path.join(
            os.path.abspath(os.path.dirname(__file__)), "warehouse", "tables"
        )
        self.root = root or default_root

    # ---------------------------------------------------------------- 路径
    def table_dir(self, table: str) -> Path:
        return Path(self.root) / _validate_table(table)

    def snapshots_dir(self, table: str) -> Path:
        return self.table_dir(table) / "snapshots"

    def snapshot_dir(self, table: str, snapshot_id: str) -> Path:
        return self.snapshots_dir(table) / _validate_snapshot(snapshot_id)

    def snapshot_path(self, table: str, snapshot_id: str | None = None) -> Path:
        """快照数据目录（调用方在此路径上做 spark.read/write）。"""
        sid = _validate_snapshot(snapshot_id) if snapshot_id else self.current_snapshot(table)
        if sid is None:
            raise FileNotFoundError(f"表不存在或无快照: {table}")
        return self.snapshot_dir(table, sid) / "data"

    def manifest_path(self, table: str, snapshot_id: str) -> Path:
        return self.snapshot_dir(table, snapshot_id) / "manifest.json"

    # ---------------------------------------------------------------- 元数据
    def _list_snapshots(self, table: str) -> list[str]:
        d = self.snapshots_dir(table)
        if not d.is_dir():
            return []
        # 只认「有 manifest」的快照目录：manifest 是提交点，
        # 崩溃可能在建目录后、写 manifest 前留下空壳（防御性一致性）
        return sorted(
            (_validate_snapshot(e.name) for e in d.iterdir()
             if e.is_dir() and (e / "manifest.json").is_file())
        )

    def current_snapshot(self, table: str) -> str | None:
        snaps = self._list_snapshots(table)
        return snaps[-1] if snaps else None

    def new_snapshot_id(self, table: str) -> str:
        """分配新快照 ID（epoch 毫秒，单调递增，不来自文件系统列举结果）。"""
        _validate_table(table)
        return str(int(time.time() * 1000))

    def history(self, table: str) -> list[dict]:
        """版本链：每个快照的 manifest 摘要（最新在前）。"""
        out = []
        for sid in reversed(self._list_snapshots(table)):
            mp = self.manifest_path(table, sid)
            if mp.is_file():
                m = json.loads(mp.read_text(encoding="utf-8"))
                out.append(
                    {
                        "snapshot_id": sid,
                        "parent_id": m.get("parent_id"),
                        "timestamp": m.get("timestamp"),
                        "operation": m.get("operation"),
                        "row_count": m.get("row_count"),
                        "merge_keys": m.get("merge_keys"),
                    }
                )
        return out

    def write_manifest(
        self,
        table: str,
        snapshot_id: str,
        parent_id: str | None,
        row_count: int,
        merge_keys: list[str],
        operation: str = "merge_upsert",
    ) -> None:
        """写入快照 manifest（提交点：manifest 落盘即视为快照生效）。"""
        _validate_table(table)
        manifest = {
            "snapshot_id": snapshot_id,
            "parent_id": parent_id,
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "operation": operation,
            "merge_keys": merge_keys,
            "row_count": row_count,
            "format": "parquet",
        }
        mp = self.manifest_path(table, snapshot_id)
        mp.parent.mkdir(parents=True, exist_ok=True)
        mp.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[table_store] {table} snapshot={snapshot_id} parent={parent_id} rows={row_count} ({operation})")

    # ---------------------------------------------------------------- 合并（纯计算）
    def merge(
        self,
        current: DataFrame | None,
        incoming: DataFrame,
        merge_keys: list[str],
    ) -> DataFrame:
        """按主键合并存量与增量（纯 DataFrame 计算，不做任何 I/O）。

        语义：主键命中用增量行覆盖存量行；未命中的增量行插入；
        未命中的存量行保留。current 为 None 时直接返回增量（首次加载）。
        """
        if current is None:
            return incoming
        # 显式等值条件（不用列名列表形式：列表形式会合并键列，与 c./i. 别名引用冲突）
        cond = None
        for k in merge_keys:
            eq = F.col(f"c.{k}") == F.col(f"i.{k}")
            cond = eq if cond is None else (cond & eq)
        joined = current.alias("c").join(incoming.alias("i"), cond, "full_outer")
        key_cols = [F.coalesce(f"i.{k}", f"c.{k}").alias(k) for k in merge_keys]
        other_cols = [
            F.when(F.col(f"i.{c}").isNotNull(), F.col(f"i.{c}")).otherwise(F.col(f"c.{c}")).alias(c)
            for c in current.columns
            if c not in merge_keys
        ]
        return joined.select(*key_cols, *other_cols)


def _cli() -> None:
    parser = argparse.ArgumentParser(description="table_store CLI")
    parser.add_argument("action", choices=["history"])
    parser.add_argument("table")
    args = parser.parse_args()

    store = TableStore()
    if args.action == "history":
        rows = store.history(args.table)
        if not rows:
            print(f"(无快照: {args.table})")
        for h in rows:
            print(f"snapshot={h['snapshot_id']} parent={h['parent_id']} {h['timestamp']} "
                  f"{h['operation']} rows={h['row_count']} keys={h['merge_keys']}")


if __name__ == "__main__":
    _cli()
