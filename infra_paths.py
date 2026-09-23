"""Kafka / Flink 本地基础设施路径解析。

Kafka 与 Flink 发行包体积大（合计约 300MB），不进入项目目录，
统一安装到工作区 infra/ 下（可用环境变量 INFRA_DIR 覆盖）：

    F:/deepseek workplace/infra/
      ├── kafka/            # kafka_2.12-3.9.2 解压目录（KRaft 单节点）
      └── flink-1.20.0/     # flink-1.20.0-bin-scala_2.12 解压目录

安装命令见 README「Kafka + Flink 本地基础设施」一节；
scripts/ 下的启动脚本会调用这里的路径。
"""
from __future__ import annotations

import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# 默认指向工作区 infra/：项目位于 <工作区>/素材/学年论文/简历项目
INFRA_DIR = os.environ.get("INFRA_DIR") or os.path.abspath(
    os.path.join(PROJECT_ROOT, "..", "..", "infra")
)

KAFKA_HOME = os.path.join(INFRA_DIR, "kafka")
FLINK_HOME = os.path.join(INFRA_DIR, "flink-1.20.0")

KAFKA_BIN = os.path.join(KAFKA_HOME, "bin", "windows")  # Windows 批处理入口
FLINK_BIN = os.path.join(FLINK_HOME, "bin")


def flink_lib_dir() -> str:
    """Flink 连接器 jar 存放目录（lib/ 下所有 jar 会随集群加载）。"""
    return os.path.join(FLINK_HOME, "lib")
