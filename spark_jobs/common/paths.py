"""数仓路径管理。

所有作业通过本模块获取各层存储路径，保证 ODS/DWD/DWS/ADS 的目录约定唯一。
生产环境中这些路径对应 HDFS/Hive 表 location；本地开发时对应文件系统目录。
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

# 项目根目录：spark_jobs/common/paths.py 的上两级
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# 拉链表当前版本的有效截止日期（业界约定的"无限远"哨兵值）
END_DATE = "9999-12-31"


def load_config() -> dict:
    """读取 config/config.yaml。"""
    import yaml

    cfg_path = Path(PROJECT_ROOT, "config", "config.yaml")
    return yaml.safe_load(cfg_path.read_text(encoding="utf-8"))


@dataclass
class Paths:
    warehouse_root: str
    business_root: str
    landing_dir: str

    @classmethod
    def from_config(cls, cfg: dict) -> "Paths":
        storage = cfg["storage"]
        return cls(
            warehouse_root=os.path.join(PROJECT_ROOT, storage["warehouse_root"]),
            business_root=os.path.join(PROJECT_ROOT, storage["business_root"]),
            landing_dir=os.path.join(PROJECT_ROOT, storage["landing_dir"]),
        )

    # ---------- 源数据 ----------
    def business_table(self, table: str, dt: str) -> str:
        """业务库导出文件目录：data/business/{table}/dt={dt}/"""
        return os.path.join(self.business_root, table, f"dt={dt}")

    # ---------- ODS ----------
    def ods(self, table: str, dt: str | None = None) -> str:
        p = os.path.join(self.warehouse_root, "ods", table)
        return os.path.join(p, f"dt={dt}") if dt else p

    # ---------- DWD ----------
    def dwd(self, table: str, dt: str | None = None) -> str:
        p = os.path.join(self.warehouse_root, "dwd", table)
        return os.path.join(p, f"dt={dt}") if dt else p

    # ---------- DWS ----------
    def dws(self, table: str, dt: str | None = None) -> str:
        p = os.path.join(self.warehouse_root, "dws", table)
        return os.path.join(p, f"dt={dt}") if dt else p

    # ---------- ADS ----------
    def ads(self, table: str, dt: str | None = None) -> str:
        p = os.path.join(self.warehouse_root, "ads", table)
        return os.path.join(p, f"dt={dt}") if dt else p

    def ads_export(self, name: str) -> str:
        """ADS 层给看板用的 CSV 快照（无 pyarrow 依赖，pandas 直读）。"""
        return os.path.join(self.warehouse_root, "ads", f"_export_{name}.csv")
