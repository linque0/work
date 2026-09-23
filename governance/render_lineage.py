"""governance/render_lineage.py：从血缘注册表渲染 mermaid 血缘图。

把列级血缘聚合为表级边（源表 -> 目标表），输出 mermaid flowchart，
可粘贴进 Obsidian/Typora/GitHub 渲染，也是数据资产文档的一部分。

用法：
    python -m governance.render_lineage                 # 打印到 stdout
    python -m governance.render_lineage --out docs/lineage.md
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import yaml  # noqa: E402

PROJECT_ROOT = Path(__file__).resolve().parent.parent
REGISTRY = PROJECT_ROOT / "governance" / "lineage_registry.yaml"

_LAYER_ORDER = {"ods": 0, "dwd": 1, "dws": 2, "ads": 3}


def _source_table(from_expr: str) -> str | None:
    """从 from 表达式中提取源表名（如 'dws.dws_apply_day.apply_cnt' -> 'dws.dws_apply_day'）。"""
    m = re.match(r"^(ods|dwd|dws|ads)\.([A-Za-z0-9_]+)\.", from_expr)
    return f"{m.group(1)}.{m.group(2)}" if m else None


def render() -> str:
    registry = yaml.safe_load(REGISTRY.read_text(encoding="utf-8"))
    tables = registry["tables"]

    # 聚合表级血缘边
    edges: set[tuple[str, str]] = set()
    for name, spec in tables.items():
        for col_meta in spec.get("columns", {}).values():
            src = _source_table(str(col_meta.get("from", "")))
            if src and src != name and src in tables:
                edges.add((src, name))
        source = spec.get("source")  # 如 kafka://repay_stream
        if source:
            edges.add((source.replace("://", "."), name))

    lines = ["```mermaid", "flowchart TD"]
    # 按层分子图，保证 ODS -> DWD -> DWS -> ADS 自上而下
    by_layer: dict[str, list[str]] = {}
    for name, spec in tables.items():
        by_layer.setdefault(spec.get("layer", "other"), []).append(name)
    for source in sorted({s for s, _ in edges if s not in tables}):
        lines.append(f'    {source.replace(".", "_")}["{source}"]')

    for layer in sorted(by_layer, key=lambda x: _LAYER_ORDER.get(x, 9)):
        lines.append(f"    subgraph {layer}")
        for name in sorted(by_layer[layer]):
            lines.append(f'        {name.replace(".", "_")}["{name}"]')
        lines.append("    end")

    for src, dst in sorted(edges):
        lines.append(f"    {src.replace('.', '_')} --> {dst.replace('.', '_')}")

    lines.append("```")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="渲染 mermaid 血缘图")
    parser.add_argument("--out", default=None, help="输出文件（默认打印 stdout）")
    args = parser.parse_args()

    content = render()
    if args.out:
        out = PROJECT_ROOT / args.out
        out.parent.mkdir(parents=True, exist_ok=True)
        header = "# 数仓表级血缘图\n\n> 由 governance/render_lineage.py 从血缘注册表自动生成，请勿手改。\n\n"
        out.write_text(header + content + "\n", encoding="utf-8")
        print(f"[lineage] written -> {out}")
    else:
        print(content)


if __name__ == "__main__":
    main()
