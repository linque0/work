"""信贷数仓每日调度 DAG（Airflow）。

调度链路：
    gen_source -> ingest_ods -> build_dwd -> build_dws -> build_ads
    -> submit_flink_job -> run_quality -> run_reconcile
    -> compact_dwd_facts -> validate_schema -> render_lineage

设计要点：
- 每层作业幂等（同 dt 覆盖写 / 新快照），调度重试安全；
- quality / reconcile / schema 校验任一失败即中断下游并告警，对应
  "数据准确性、一致性、稳定性"的工程保障；
- 任务通过 PYTHONPATH 定位项目根目录，不依赖 cwd；
- Windows 本地开发需在 env 中提供 HADOOP_HOME（Linux 集群可去掉）；
- submit_flink_job 为可选项：Flink 集群未启动时脚本会跳过（exit 0）。

生产形态替换点：DataX/Canal 替 gen_source、spark-submit on YARN 替本地
local[*]、Kafka/Flink 常驻作业替 availableNow 提交、Hudi/Iceberg 替
TableStore 自实现快照。
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
HADOOP_HOME = os.path.join(PROJECT_ROOT, ".hadoop")

COMMON_ENV = {
    "PYTHONPATH": PROJECT_ROOT,
    "PYSPARK_PYTHON": "python",
    # Windows 本地开发：提供 winutils.exe/hadoop.dll；Linux 集群删除此行
    "HADOOP_HOME": HADOOP_HOME if os.path.isdir(HADOOP_HOME) else "",
}

DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
    "email_on_failure": False,  # 生产环境替换为值班邮件组
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="credit_dw_daily",
    description="信贷流批一体数仓每日调度（ODS->DWD->DWS->ADS + 质量 + 对账 + 治理）",
    default_args=DEFAULT_ARGS,
    schedule="@daily",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    tags=["data-warehouse", "credit", "spark", "flink"],
    doc_md=__doc__,
) as dag:
    gen_source = BashOperator(
        task_id="gen_source",
        bash_command="python -m data.generator --dt {{ ds }} --stream 5 --sink kafka",
        env=COMMON_ENV,
    )

    ingest_ods = BashOperator(
        task_id="ingest_ods",
        bash_command="python -m spark_jobs.ods_ingest --dt {{ ds }}",
        env=COMMON_ENV,
    )

    build_dwd = BashOperator(
        task_id="build_dwd",
        bash_command="python -m spark_jobs.dwd_build --dt {{ ds }}",
        env=COMMON_ENV,
    )

    build_dws = BashOperator(
        task_id="build_dws",
        bash_command="python -m spark_jobs.dws_build --dt {{ ds }}",
        env=COMMON_ENV,
    )

    build_ads = BashOperator(
        task_id="build_ads",
        bash_command="python -m spark_jobs.ads_build --dt {{ ds }}",
        env=COMMON_ENV,
    )

    submit_flink_job = BashOperator(
        task_id="submit_flink_job",
        # Flink 集群未启动时内部跳过（exit 0）；已启动则提交/续跑实时作业
        bash_command="bash scripts/flink_submit.sh || true",
        env=COMMON_ENV,
    )

    run_quality = BashOperator(
        task_id="run_quality",
        bash_command="python -m quality.checks --dt {{ ds }}",
        env=COMMON_ENV,
    )

    run_reconcile = BashOperator(
        task_id="run_reconcile",
        bash_command="python -m quality.reconcile --dt {{ ds }}",
        env=COMMON_ENV,
    )

    compact_dwd_facts = BashOperator(
        task_id="compact_dwd_facts",
        bash_command="python -m compaction --table dwd_fact_repay --dt {{ ds }} --target-files 1",
        env=COMMON_ENV,
    )

    validate_schema = BashOperator(
        task_id="validate_schema",
        bash_command="python -m governance.validate_schema",
        env=COMMON_ENV,
    )

    render_lineage = BashOperator(
        task_id="render_lineage",
        bash_command="python -m governance.render_lineage --out docs/lineage.md",
        env=COMMON_ENV,
    )

    (
        gen_source
        >> ingest_ods
        >> build_dwd
        >> build_dws
        >> build_ads
        >> submit_flink_job
        >> run_quality
        >> run_reconcile
        >> compact_dwd_facts
        >> validate_schema
        >> render_lineage
    )
