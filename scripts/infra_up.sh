#!/usr/bin/env bash
# Kafka + Flink 本地基础设施一键启停（Windows / Git Bash）。
#
#   bash scripts/infra_up.sh     启动 Kafka(KRaft) + Flink(JM+TM)，建好 topic
#   bash scripts/infra_down.sh   停止全部
#
# 安装（一次性，约 300MB，装到工作区 infra/ 避免撑大项目目录）：
#   curl -O https://mirrors.huaweicloud.com/apache/kafka/3.9.2/kafka_2.12-3.9.2.tgz
#   curl -O https://mirrors.huaweicloud.com/apache/flink/flink-1.20.0/flink-1.20.0-bin-scala_2.12.tgz
#   curl -O https://maven.aliyun.com/repository/public/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar
#   解压到 infra/{kafka,flink-1.20.0}，connector jar 复制到 flink-1.20.0/lib/
#   （连接器版本须与 Flink 小版本匹配；本机已就绪）
set -euo pipefail

# Git Bash 会对 /F、/J 等开关做 MSYS 路径转换，必须禁用
export MSYS2_ARG_CONV_EXCL="*"

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
INFRA_ROOT="$(cd "$PROJECT_ROOT/../.." && pwd)/infra"
KAFKA_HOME="$INFRA_ROOT/kafka"
FLINK_HOME="$INFRA_ROOT/flink-1.20.0"
KAFKA_CONF="$PROJECT_ROOT/config/kafka-server.properties"
KAFKA_CLUSTER_ID="8a2fc471-150e-4070-acdc-8eac100d7271"

# 无空格 junction：Windows .bat/脚本在 Git Bash 下遇空格路径会被截断
cmd /c mklink /J C:\kafka "$(cygpath -w "$KAFKA_HOME" 2>/dev/null || echo "$KAFKA_HOME")" >/dev/null 2>&1 || true
cmd /c mklink /J C:\flink "$(cygpath -w "$FLINK_HOME" 2>/dev/null || echo "$FLINK_HOME")" >/dev/null 2>&1 || true

wait_port() {
    local port=$1 name=$2
    for i in $(seq 1 40); do
        # 用 shell 重定向探测，不用 curl -o /dev/null：MSYS2_ARG_CONV_EXCL="*"
        # 下 -o 参数不转换，Windows curl 写 POSIX /dev/null 必失败（实测踩坑）
        if curl -s --max-time 2 "http://127.0.0.1:$port" > /dev/null 2>&1 \
           || python -c "import socket;s=socket.socket();s.settimeout(1);s.connect(('127.0.0.1',$port));s.close()" 2>/dev/null; then
            echo "[infra] $name is up (port $port)"
            return 0
        fi
        sleep 2
    done
    echo "[infra] WARN: $name port $port not responding"
    return 1
}

flink_java_opts() {
    python -c "
import yaml
cfg = yaml.safe_load(open('C:/flink/conf/config.yaml', encoding='utf-8'))
print(cfg['env']['java']['opts']['all'])
"
}

case "${1:-up}" in
up)
    echo "[infra] starting Kafka (KRaft single node) ..."
    cp "$KAFKA_CONF" C:/kafka/config/server.properties
    java -cp "C:/kafka/libs/*" kafka.tools.StorageTool format \
        -t "$KAFKA_CLUSTER_ID" -c C:/kafka/config/server.properties --ignore-formatted >/dev/null 2>&1 || true
    nohup java -Xmx1G -Xms1G \
        -Dlog4j.configuration="file:C:/kafka/config/log4j.properties" \
        -cp "C:/kafka/libs/*" kafka.Kafka C:/kafka/config/server.properties \
        > C:/kafka/server.log 2>&1 &
    wait_port 9092 "kafka"

    for topic in repay_stream repay_dws_1min; do
        java -cp "C:/kafka/libs/*" org.apache.kafka.tools.TopicCommand --create \
            --topic "$topic" --bootstrap-server localhost:9092 \
            --partitions 3 --replication-factor 1 2>/dev/null \
            && echo "[infra] topic $topic ready" || echo "[infra] topic $topic exists"
    done

    echo "[infra] starting Flink (JobManager + TaskManager) ..."
    nohup java -Xmx1600m $(flink_java_opts) \
        -Dlog4j.configurationFile="file:C:/flink/conf/log4j-console.properties" \
        -Dlog.file="C:/flink/log/jobmanager.log" -Dconsole.log.level=OFF \
        -classpath "C:/flink/lib/*" \
        org.apache.flink.runtime.entrypoint.StandaloneSessionClusterEntrypoint \
        --configDir C:/flink/conf > C:/flink/log/jm-console.log 2>&1 &
    wait_port 8081 "flink-jobmanager"

    nohup java -Xmx1600m $(flink_java_opts) \
        -Dlog4j.configurationFile="file:C:/flink/conf/log4j-console.properties" \
        -Dlog.file="C:/flink/log/taskmanager.log" -Dconsole.log.level=OFF \
        -classpath "C:/flink/lib/*" \
        org.apache.flink.runtime.taskexecutor.TaskManagerRunner \
        --configDir C:/flink/conf > C:/flink/log/tm-console.log 2>&1 &
    sleep 15
    echo "[infra] all up. Flink WebUI: http://localhost:8081"
    ;;

down)
    echo "[infra] stopping ..."
    python - <<'PY'
import subprocess, json, urllib.request
# 停 Flink 作业与进程
try:
    running = json.load(urllib.request.urlopen("http://localhost:8081/jobs/", timeout=5))
    for j in running.get("jobs", []):
        if j.get("status") == "RUNNING":
            urllib.request.urlopen(
                urllib.request.Request(
                    f"http://localhost:8081/jobs/{j['id']}?mode=cancel", method="PATCH"
                ), timeout=10)
            print("cancelled job", j["id"])
except Exception as e:
    print("flink jobs:", e)
PY
    taskkill /F /IM java.exe >/dev/null 2>&1 || true
    echo "[infra] all java processes stopped"
    ;;

*)
    echo "usage: $0 [up|down]"
    exit 1
    ;;
esac
