#!/usr/bin/env bash
# 一键本地运行：完整跑一个业务日的全链路（含质量与对账）。
# 用法：bash scripts/run_local.sh 2026-09-22
set -euo pipefail

DT="${1:-$(date -d yesterday +%Y-%m-%d 2>/dev/null || date -v-1d +%Y-%m-%d)}"
cd "$(dirname "$0")/.."

export PYTHONPATH="$PWD"
export PYSPARK_PYTHON="${PYSPARK_PYTHON:-python}"
if [ -d ".hadoop" ]; then
  export HADOOP_HOME="$PWD/.hadoop"   # Windows 原生库；Linux/Mac 无此目录时忽略
fi

echo "==> [1/8] 生成业务数据 + 实时流 (dt=$DT)"
python -m data.generator --dt "$DT" --stream 5

echo "==> [2/8] ODS 接入"
python -m spark_jobs.ods_ingest --dt "$DT"

echo "==> [3/8] DWD 建模（拉链表 + 事实表）"
python -m spark_jobs.dwd_build --dt "$DT"

echo "==> [4/8] DWS 汇总"
python -m spark_jobs.dws_build --dt "$DT"

echo "==> [5/8] ADS 应用层"
python -m spark_jobs.ads_build --dt "$DT"

echo "==> [6/8] 实时链路（availableNow）"
python -m spark_jobs.streaming_job --once

echo "==> [7/8] 数据质量检查"
python -m quality.checks --dt "$DT"

echo "==> [8/8] 流批对账"
python -m quality.reconcile --dt "$DT"

echo "==> 完成。启动看板：streamlit run dashboard/app.py"
