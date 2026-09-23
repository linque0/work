"""SparkSession 工厂。

职责：
1. 统一 Spark 配置（自适应执行、shuffle 分区数等）；
2. Windows 本地开发适配：自动定位 .hadoop/bin 下的 winutils.exe 与 hadoop.dll，
   设置 HADOOP_HOME 与 java.library.path，避免 RawLocalFileSystem 的
   chmod/access 原生调用失败（Linux/Mac 无需此逻辑）。
"""
from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession

from spark_jobs.common.paths import PROJECT_ROOT, Paths  # noqa: F401  (Paths re-export)


def _configure_windows_native() -> None:
    """Windows 下配置 hadoop 原生库路径；非 Windows 或找不到 .hadoop 时静默跳过。"""
    if not sys.platform.startswith("win"):
        return

    hadoop_home = os.path.join(PROJECT_ROOT, ".hadoop")
    bin_dir = os.path.join(hadoop_home, "bin")
    if not os.path.isdir(bin_dir):
        return

    # 1) HADOOP_HOME：Shell.execCommand 据此定位 winutils.exe（chmod 等命令）
    os.environ.setdefault("HADOOP_HOME", hadoop_home)

    # 2) java.library.path：NativeCodeLoader 据此加载 hadoop.dll。
    #    路径必须用正斜杠并整体加引号——反斜杠会被 JVM 当转义字符吞掉，
    #    空格会被 spark-submit 的参数切分切断，这两点都踩过坑。
    lib_path = bin_dir.replace(os.sep, "/")
    os.environ["PYSPARK_JAVA_LIBRARY_PATH"] = lib_path


def get_spark(app_name: str, master: str = "local[*]", shuffle_partitions: int = 8) -> SparkSession:
    _configure_windows_native()

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        # Windows 本机 driver 绑定回环地址，避免主机名解析慢导致启动卡顿
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
    )

    lib_path = os.environ.get("PYSPARK_JAVA_LIBRARY_PATH")
    if lib_path:
        builder = builder.config(
            "spark.driver.extraJavaOptions", '-Djava.library.path="%s"' % lib_path
        )

    return builder.getOrCreate()
