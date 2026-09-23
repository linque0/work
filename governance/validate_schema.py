"""governance/validate_schema.py：Schema 漂移检测。

拿血缘注册表（governance/lineage_registry.yaml）中登记的列定义与实际
parquet 结构比对，检测三类漂移：
  - missing：实际表缺少登记列（下游作业会炸）
  - extra  ：实际表多出登记外的列（未登记口径，治理风险）
  - type   ：列类型与登记不符（如 string -> int）

直接读 parquet footer（pyarrow），不起 Spark——治理校验要轻、要能进 CI。

用法：
    python -m governance.validate_schema                    # 校验全部表
    python -m governance.validate_schema --table dwd.dwd_fact_repay
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import yaml  # noqa: E402

PROJECT_ROOT = Path(__file__).resolve().parent.parent
REGISTRY = PROJECT_ROOT / "governance" / "lineage_registry.yaml"

# parquet 逻辑类型 -> 登记类型的归一化映射
_TYPE_ALIASES = {
    "STRING": "string",
    "UTF8": "string",
    "INT32": "int",
    "INT64": "long",
    "INT96": "timestamp",
    "DOUBLE": "double",
    "FLOAT": "float",
    "BOOL": "boolean",
    "BOOLEAN": "boolean",
    "DATE32": "date",
    "TIMESTAMP": "timestamp",
}


def _resolve_path(pattern: str, dt: str | None) -> Path:
    """把注册表路径模式解析为实际目录（优先取最新 dt 分区）。"""
    if "{dt}" in pattern:
        if dt:
            return PROJECT_ROOT / pattern.replace("{dt}", dt)
        base = PROJECT_ROOT / pattern.split("dt=")[0].rstrip("/")
        if not base.is_dir():
            return base
        parts = sorted(base.glob("dt=*"))
        if not parts:
            return base / "dt=unknown"
        return parts[-1]
    if "{snapshot}" in pattern:
        base = PROJECT_ROOT / pattern.split("snapshots/")[0] / "snapshots"
        # 与 TableStore 一致：只认有 manifest 的快照目录（提交点）
        snaps = sorted(
            (d for d in base.iterdir()
             if d.is_dir() and (d / "manifest.json").is_file()),
            key=lambda d: d.name,
        ) if base.is_dir() else []
        if not snaps:
            return base / "no-snapshot" / "data"
        return snaps[-1] / "data"
    return PROJECT_ROOT / pattern


def _actual_schema(path: Path) -> dict[str, str] | None:
    if not path.is_dir():
        return None
    import pyarrow.parquet as pq

    files = [p for p in path.rglob("*.parquet") if p.is_file() and not p.name.startswith(("_", "."))]
    if not files:
        return None
    schema = pq.read_schema(files[0])
    out = {}
    for field in schema:
        t = str(field.type).upper()
        # 剥单位后缀：timestamp[ns] -> TIMESTAMP，date32[day] -> DATE32
        t = t.split("[")[0]
        out[field.name] = _TYPE_ALIASES.get(t, t.lower())
    return out


def validate(table_filter: str | None, dt: str | None) -> bool:
    registry = yaml.safe_load(REGISTRY.read_text(encoding="utf-8"))
    results = []
    all_ok = True

    for full_name, spec in registry["tables"].items():
        if table_filter and full_name != table_filter:
            continue
        path = _resolve_path(spec["path"], dt)
        registered = {c: meta.get("type") for c, meta in spec.get("columns", {}).items()}

        entry = {"table": full_name, "path": str(path), "issues": []}

        # 非 parquet 表（如 Flink JSON sink）只做存在性检查；
        # optional 表在数据缺失时记 SKIP（实时链路未启用），不算失败
        if spec.get("format") == "json":
            files = [p for p in path.rglob("*") if p.is_file()] if path.is_dir() else []
            if not files:
                if spec.get("optional"):
                    entry["skipped"] = True
                    entry["issues"].append({"rule": "skipped", "detail": "optional 表无数据（实时链路未启用）"})
                else:
                    entry["issues"].append({"rule": "existence", "detail": f"无数据文件: {path}"})
            entry["passed"] = not [i for i in entry["issues"] if i["rule"] != "skipped"]
            all_ok = all_ok and entry["passed"]
            results.append(entry)
            status = "PASS" if entry["passed"] else ("SKIP" if entry.get("skipped") else "FAIL")
            print(f"[schema] {status} {full_name} (json, {len(files)} files) {entry['issues'] or ''}")
            continue

        actual = _actual_schema(path)
        if actual is None:
            entry["issues"].append({"rule": "existence", "detail": f"无 parquet 数据: {path}"})
        else:
            missing = sorted(set(registered) - set(actual))
            extra = sorted(set(actual) - set(registered))
            type_diff = sorted(
                c for c in set(registered) & set(actual) if registered[c] != actual[c]
            )
            if missing:
                entry["issues"].append({"rule": "missing_columns", "columns": missing})
            if extra:
                entry["issues"].append({"rule": "extra_columns", "columns": extra})
            if type_diff:
                entry["issues"].append(
                    {"rule": "type_mismatch", "columns": {c: [registered[c], actual[c]] for c in type_diff}}
                )
            entry["registered_columns"] = len(registered)
            entry["actual_columns"] = len(actual)

        entry["passed"] = not entry["issues"]
        all_ok = all_ok and entry["passed"]
        results.append(entry)
        status = "PASS" if entry["passed"] else "FAIL"
        print(f"[schema] {status} {full_name} ({entry.get('actual_columns', 0)} cols) {entry['issues'] or ''}")

    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "passed": all_ok,
        "tables": results,
    }
    out = PROJECT_ROOT / "quality" / "schema_drift_report.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[schema] report -> {out}")
    return all_ok


def main() -> None:
    parser = argparse.ArgumentParser(description="Schema 漂移检测")
    parser.add_argument("--table", default=None, help="只校验指定表（如 dwd.dwd_fact_repay）")
    parser.add_argument("--dt", default=None, help="指定业务日期（默认取最新分区）")
    args = parser.parse_args()
    sys.exit(0 if validate(args.table, args.dt) else 1)


if __name__ == "__main__":
    main()
