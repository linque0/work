#!/usr/bin/env bash
# 2026-09-23 全链路试运行驱动脚本（Kafka + Flink 真实时链路版）。
#
# 用途：一条命令复现「试运行结果效果图」对应的完整链路，等价于 Airflow DAG
# credit_dw_daily 的手动执行序列：
#
#   gen_source -> ingest_ods -> build_dwd -> build_dws -> build_ads
#   -> submit_flink_job -> run_quality -> run_reconcile
#   -> compact_dwd_facts -> validate_schema -> render_lineage
#
# 前置条件：
#   bash scripts/infra_up.sh                       # Kafka(KRaft) + Flink(JM+TM) 已在跑
#   python -m data.generator --dt <DT> --sink kafka 之前的 topic 已有历史消息亦可
#
# 注意：每个业务日期只应生产一次（Kafka topic 追加累积，批量文件覆盖），
# 复跑本脚本前先换一个 --dt。
set -uo pipefail

cd "$(dirname "$0")/.."
export PYTHONIOENCODING=utf-8
export PYTHONPATH="$PWD"
export HADOOP_HOME="$PWD/.hadoop"   # Windows 原生库；Linux 集群删除本行
export MSYS2_ARG_CONV_EXCL="*"      # Git Bash 路径转换屏蔽

DT="${1:-2026-09-23}"
# 入参格式校验：只接受 YYYY-MM-DD，拒绝任何其他形态（防注入面收敛）
if ! [[ "$DT" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "usage: $0 [YYYY-MM-DD]" >&2
    exit 2
fi
export DEMO_DT="$DT"
banner() { echo; echo "########## $* ##########"; }

banner "0/11 重置 Flink sink 目录（本地演示产物，仓库忽略该路径）"
rm -rf "$PWD/warehouse/ads/flink_repay_metrics"
mkdir -p "$PWD/warehouse/ads/flink_repay_metrics"
echo "sink reset: $(ls "$PWD/warehouse/ads/flink_repay_metrics" | wc -l) files"

banner "1/11 生成业务数据（批量 CSV；默认 --sink kafka 双写消息总线）"
if [ "${SKIP_PRODUCE:-0}" = "1" ]; then
    # 复跑场景：topic 已含当日消息，重复生产会翻倍导致窗口聚合与对账口径失真
    echo "[demo] SKIP_PRODUCE=1：跳过 Kafka 生产，只重建批量 CSV"
    python -m data.generator --dt "$DT" --sink file
else
    python -m data.generator --dt "$DT" --sink kafka
fi

banner "2/11 ODS 贴源层（显式 Schema + 审计字段）"
python -m spark_jobs.ods_ingest --dt "$DT"

banner "3/11 DWD 明细层（TableStore 拉链表 SCD2 合并）"
python -m spark_jobs.dwd_build --dt "$DT"

banner "4/11 DWS 汇总层（按日汇总 + 客户累计）"
python -m spark_jobs.dws_build --dt "$DT"

banner "5/11 ADS 应用层（风控日报 + 评分卡特征宽表）"
python -m spark_jobs.ads_build --dt "$DT"

banner "6/11 提交 Flink SQL 实时作业（双源表双消费者组）"
bash scripts/flink_submit.sh

banner "7/11 等待 Flink 1min 窗口数据落 filesystem sink ..."
python - <<'PY'
import os, pathlib, time

dt = os.environ["DEMO_DT"]
sink = pathlib.Path("warehouse/ads/flink_repay_metrics")
for i in range(72):
    files = [f for f in sink.iterdir() if f.is_file()] if sink.is_dir() else []
    hit = any(
        dt in f.read_text(encoding="utf-8", errors="ignore")
        for f in files
        if f.stat().st_size > 0
    )
    if hit:
        print(f"[wait] flink sink ready after ~{i * 5}s (files={len(files)})")
        break
    time.sleep(5)
else:
    print(f"[wait] WARN: sink 未见 {dt} 窗口（检查 Flink WebUI / 作业日志）")
PY

banner "8/11 数据质量检查（主键唯一 / 空值率 / 行数波动）"
python -m quality.checks --dt "$DT"

banner "9/11 流批对账（离线 T+1 vs Flink filesystem sink）"
python -m quality.reconcile --dt "$DT"

banner "10/11 小文件治理 compaction（dwd_fact_repay）"
python -m compaction --table dwd_fact_repay --dt "$DT" --target-files 1

banner "11/11 Schema 漂移校验 + 列级血缘渲染"
python -m governance.validate_schema
python -m governance.render_lineage --out docs/lineage.md

banner "附：Kafka topic 现状与消息抽样"
java -cp "C:/kafka/libs/*" org.apache.kafka.tools.TopicCommand --describe \
    --topic repay_stream --bootstrap-server localhost:9092 2>/dev/null || true
java -cp "C:/kafka/libs/*" org.apache.kafka.tools.consumer.ConsoleConsumer \
    --bootstrap-server localhost:9092 --topic repay_stream \
    --from-beginning --max-messages 2 --timeout-ms 8000 2>/dev/null || true

banner "DONE $DT"
