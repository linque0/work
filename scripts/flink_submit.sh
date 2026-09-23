#!/usr/bin/env bash
# 提交 Flink SQL 实时作业到本地集群。
#
# Windows/Git Bash 适配说明（都踩过坑）：
# 1) Flink 官方 .sh 脚本在 MSYS 下 classpath 分隔符不转换（manglePathList 只认
#    CYGWIN），':' 分隔的 classpath 喂给 Windows java 会 ClassNotFound，
#    因此这里不用 flink 脚本，直接用等价 java 命令直启 SqlClient；
# 2) SQL 作业文件路径含空格会被截断，先复制到 C:/flink/submit-job.sql；
# 3) sink 路径占位符 {{SINK_PATH}} 替换为项目 warehouse 绝对路径。
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
FLINK_HOME="${FLINK_HOME:-C:/flink}"
FLINK_CONF="$FLINK_HOME/conf"

# 集群未启动（无 FLINK_HOME 或 REST 8081 无响应）时安静跳过，退出码 0——
# 让 Airflow DAG 在没有 Flink 的环境也能跑完其余环节。
# 注意：探测用 shell 重定向到 /dev/null，不能用 curl -o /dev/null——
# MSYS2_ARG_CONV_EXCL="*" 下该参数不转换，Windows curl 写 POSIX /dev/null
# 会失败并返回非零，导致集群明明在线却误判未就绪（实测踩坑）。
if [ ! -d "$FLINK_HOME/lib" ] || ! curl -s --max-time 3 http://localhost:8081/config > /dev/null 2>&1; then
    echo "[flink] 集群未就绪（$FLINK_HOME 或 localhost:8081），跳过提交"
    exit 0
fi

# cygpath -m：转成 Windows 混合路径（F:/...），Flink/Hadoop 的 Path 才认
SINK_PATH="$(cygpath -m "$PROJECT_ROOT/warehouse/ads/flink_repay_metrics")"
SUBMIT_COPY="$FLINK_HOME/submit-job.sql"

# 从 config.yaml 提取 Java 17 模块访问参数
FLINK_JAVA_OPTS=$(python -c "
import yaml
cfg = yaml.safe_load(open('$FLINK_CONF/config.yaml', encoding='utf-8'))
print(cfg['env']['java']['opts']['all'])
")

export MSYS2_ARG_CONV_EXCL="*"
export FLINK_CONF_DIR="$FLINK_CONF"

sed "s|{{SINK_PATH}}|$SINK_PATH|g" "$PROJECT_ROOT/flink/jobs/repay_realtime.sql" > "$SUBMIT_COPY"

echo "[flink] submitting job from $SUBMIT_COPY"
java -Xmx1g $FLINK_JAVA_OPTS \
    -Dlog4j.configuration="file:$FLINK_CONF/log4j-cli.properties" \
    -classpath "$FLINK_HOME/lib/*;$FLINK_HOME/opt/flink-sql-client-1.20.0.jar;$FLINK_HOME/opt/flink-sql-gateway-1.20.0.jar;$FLINK_HOME/opt/flink-python-1.20.0.jar" \
    org.apache.flink.table.client.SqlClient -f "$SUBMIT_COPY" 2>&1 | tail -20

echo "[flink] job submitted (check http://localhost:8081)"
