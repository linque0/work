# 信贷业务流批一体数仓（简历项目）

基于信贷业务场景的**离线 + 实时双链路数据仓库**，覆盖数据开发工程师 JD 全链路：
Spark 四层建模（ODS/DWD/DWS/ADS）、Kafka 消息总线、Flink SQL 实时数仓、
TableStore 自实现湖仓表格式（merge_upsert + 不可变快照 + 时间旅行）、
小文件治理（compaction）、数据倾斜实战基准、数据质量与流批对账、
FastAPI 数据服务、列级血缘与 Schema 漂移检测、Airflow 每日调度、Streamlit 看板。

## 架构总览

```
                 ┌──────────────────────────────────────────────┐
                 │               业务源（模拟）                  │
                 │  data/generator.py  合成三表 / 天池数据映射    │
                 │  --sink kafka → Kafka(repay_stream)          │
                 │  --stream     → data/landing（文件流兜底）     │
                 └───────────────┬──────────────────┬───────────┘
                                 │ 批量 CSV          │ Kafka topic
                   ┌─────────────▼──────────┐  ┌────▼─────────────┐
                   │  ODS ods_ingest.py     │  │ Flink SQL 作业   │
                   │  贴源层（类型统一+审计） │  │ 1min 窗口+水印   │
                   └─────────────┬──────────┘  └────┬─────────────┘
                                 │                  │ filesystem JSON + Kafka
                   ┌─────────────▼──────────┐       │ (flink_repay_metrics)
                   │  DWD dwd_build.py      │       │
                   │  TableStore 拉链表维度   │       │
                   │  (merge_upsert+快照)    │       │
                   └─────────────┬──────────┘       │
                   ┌─────────────▼──────────┐       │
                   │  DWS dws_build.py      │       │
                   │  按日汇总 + 客户累计口径 │       │
                   └─────────────┬──────────┘       │
                   ┌─────────────▼──────────┐       │
                   │  ADS ads_build.py      │       │
                   │  风控日报 + 评分卡特征宽表│      │
                   └──┬──────────────┬──────┘       │
                      │              │              │
        ┌─────────────▼───┐   ┌──────▼──────┐  ┌────▼──────────┐
        │ quality/checks  │   │ 看板/BI 报表 │  │ reconcile 对账 │
        │ 质量规则+台账    │   │ (Streamlit) │  │ 离线 vs Flink  │
        └────────┬────────┘   └─────────────┘  └───────────────┘
                 │
        ┌────────▼─────────┐   ┌──────────────┐   ┌─────────────┐
        │ compaction.py    │   │ service/app  │   │ governance/ │
        │ 小文件治理        │   │ FastAPI 服务 │   │ 血缘+Schema  │
        └──────────────────┘   └──────────────┘   └─────────────┘

调度：dags/credit_dw_dag.py（Airflow @daily，11 任务链）
```

## 目录结构

```
├── config/
│   ├── config.yaml               # 全部路径/阈值/参数配置（含 kafka/flink 段）
│   └── kafka-server.properties   # Kafka KRaft 单节点配置
├── data/generator.py             # 业务数据生成器（合成/天池映射/Kafka producer）
├── spark_jobs/
│   ├── common/
│   │   ├── paths.py              # 数仓路径管理 + 配置加载
│   │   └── spark_session.py      # SparkSession 工厂（含 Windows 原生库适配）
│   ├── ods_ingest.py             # ODS：业务库接入
│   ├── dwd_build.py              # DWD：TableStore 拉链表 + 事实表
│   ├── dws_build.py              # DWS：按日汇总 + 客户累计口径
│   ├── ads_build.py              # ADS：风控日报 + 评分卡特征宽表
│   └── streaming_job.py          # 实时链路兜底：Structured Streaming
├── table_store.py                # 迷你湖仓表格式：merge_upsert + 快照 + 时间旅行
├── compaction.py                 # 小文件治理（阈值触发 + 原子替换 + 前后报告）
├── bench/skew_bench.py           # 数据倾斜实战（加盐两阶段聚合 + EventLog 指标）
├── quality/
│   ├── checks.py                 # 质量规则：主键唯一/空值率/行数波动 + 台账
│   └── reconcile.py              # 流批对账（Flink sink / Spark Streaming 双来源）
├── flink/jobs/repay_realtime.sql # Flink SQL 实时作业（Kafka 源 + 双 sink）
├── governance/
│   ├── lineage_registry.yaml     # 列级血缘注册表（单一事实来源）
│   ├── validate_schema.py        # Schema 漂移检测（parquet footer，不起 Spark）
│   └── render_lineage.py         # mermaid 血缘图渲染
├── service/app.py                # FastAPI 数据服务（日报/特征点查 + TTL 缓存）
├── dashboard/app.py              # Streamlit 看板（离线日报 + Flink 实时两栏）
├── dags/credit_dw_dag.py         # Airflow 每日调度 DAG（11 任务）
├── tests/smoke_test.py           # 端到端冒烟测试（两业务日全链路 + 治理断言）
├── scripts/
│   ├── infra_up.sh               # Kafka + Flink 一键启停
│   ├── flink_submit.sh           # Flink SQL 作业提交（含 Windows 适配）
│   └── run_local.sh              # 单业务日全链路（无集群版）
├── docs/lineage.md               # 自动生成的血缘图
└── warehouse/                    # 数仓存储（parquet，本地模拟 HDFS）
```

## 快速开始

环境要求：Python 3.10+、Java 17+。Windows 原生库（winutils.exe/hadoop.dll）
已附在 `.hadoop/`，开箱即跑。

```bash
pip install -r requirements.txt

# 一键端到端冒烟测试（两业务日全链路 + 快照链 + Schema 校验，约 6 分钟）
python tests/smoke_test.py
```

**Kafka + Flink 实时链路**（一次性安装，约 300MB，装到工作区 infra/）：

```bash
# 安装（华为云 Apache 镜像 + 阿里云 Maven）
curl -O https://mirrors.huaweicloud.com/apache/kafka/3.9.2/kafka_2.12-3.9.2.tgz
curl -O https://mirrors.huaweicloud.com/apache/flink/flink-1.20.0/flink-1.20.0-bin-scala_2.12.tgz
curl -O https://maven.aliyun.com/repository/public/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar
# 解压到 infra/{kafka,flink-1.20.0}，connector jar 复制到 flink-1.20.0/lib/

bash scripts/infra_up.sh          # 启动 Kafka(KRaft) + Flink(JM+TM)，建好 topic
bash scripts/flink_submit.sh      # 提交 Flink SQL 实时作业
python -m data.generator --dt 2026-09-22 --sink kafka   # 生产数据进 Kafka
python -m quality.reconcile --dt 2026-09-22             # 流批对账
```

看板与服务：

```bash
streamlit run dashboard/app.py                    # 看板（离线日报 + Flink 实时）
python -m uvicorn service.app:app --port 8000     # 数据服务
curl "http://127.0.0.1:8000/api/v1/risk/daily?dt=2026-09-22"
```

## 核心设计（面试要点）

**数仓分层**：ODS 贴源（只做类型统一和审计字段）→ DWD 明细（事实表只存维度
代理键，维度属性单一出处）→ DWS 轻度汇总（一张表被多个 ADS 复用）→ ADS 应用
（唯一允许带业务口径的层，如违约率分母取通过笔数）。

**TableStore（自实现迷你湖仓）**：`warehouse/tables/{table}/snapshots/{id}/`
不可变快照链 + manifest 提交点；`merge()` 按主键合并增量（命中覆盖、未命中
插入），拉链表用 `(customer_id, start_date)` 作合并键，"关闭旧版本 + 插入
新版本"作为每日增量。解决"读旧表 + 写同路径"的文件失效问题，历史版本保留
可时间旅行：`python -m table_store history dwd_dim_customer`。

**流批一体**：生成器把同一批还款事件分别落批量快照（repay_flow.csv）与
Kafka topic；Flink SQL 作业（Kafka 源 → 1min TUMBLE 窗口 + Watermark →
filesystem JSON / Kafka 双 sink）与离线 T+1 双跑，`quality/reconcile.py`
对账（容差 1%）。**实测对账偏差 0.0%**（笔数差来自 Watermark 丢弃的迟到消息）。

**Kafka/Flink 工程细节**（README 里保留踩坑记录，面试可讲）：
- 两个 INSERT 作业必须用**不同 group.id**，否则同一消费者组把 topic 分区
  分家，每个作业只拿到部分数据（实测踩坑）；
- Flink 官方 .sh 在 Git Bash 下 classpath 分隔符不转换（manglePathList 只认
  CYGWIN），本项目用等价 java 命令直启 JM/TM/SqlClient；
- Windows 下 TM 工作目录名 `tm_localhost:端口` 含冒号是非法文件名，需
  `taskmanager.resource-id` 覆盖；内存组件路径要求每个键显式存在；
- 取消作业要确认 0 running 再提交——残留作业的共享消费者组会陷入重平衡死锁。

**数据质量**：主键唯一率、关键列空值率、行数波动（对照台账）三类规则 +
行数台账；失败即退出码非零，Airflow 任务标红并阻断下游。

**小文件治理（compaction）**：文件数 > 50 或平均大小 < 8MB 触发重写，
coalesce 到目标文件数后原子替换（先写 .compact_tmp，再逐文件删旧、
rename 上位），输出前后对比报告。

**数据倾斜实战（skew_bench）**：合成 95% 流量集中到 100 个热点客户的幂律
数据，对比直接 groupBy 与两阶段加盐聚合（打散 → 局部聚合 → 合并）；
通过 Spark EventLog 采集 task 级 shuffle 字节与耗时，输出 max/median 对比。

**数据服务（FastAPI）**：风控日报、评分卡特征点查、客户特征、维度当前版本
四类 API + 进程内 TTL 缓存（60s）；parquet 用 pandas + pyarrow 读，Web 服务
不起 Spark。

**数据治理**：`governance/lineage_registry.yaml` 登记全部 11 张表的列级血缘；
`validate_schema.py` 直接读 parquet footer 比对（missing/extra/type 三类漂移），
不起 Spark、可进 CI；`render_lineage.py` 渲染 mermaid 表级血缘图（docs/lineage.md）。

## 实测结果（2026-09-23，Windows + PySpark 4.2.0 + Java 17 + Kafka 3.9.2 + Flink 1.20）

- 冒烟测试：**17 个步骤全部 PASS**（两业务日：gen→ods→dwd→dws→ads→streaming→quality→reconcile + 快照链 + Schema 校验）
- 流批对账：repay_amt 偏差 **0.0%**、repay_cnt 偏差 0.65%（容差 1% 内）
- TableStore：两业务日快照链 60 → 61 行（漂移客户正确关闭旧版本 + 插入新版本）
- Schema 校验：**11/11 表 PASS**
- Kafka→Flink 全链路：topic 482 条消息全部消费，窗口聚合落 filesystem sink + 回写 Kafka topic
- FastAPI 四个端点全部验证通过；Streamlit 看板 HTTP 200

## 与生产环境的差异（诚实声明）

| 本项目（本地演示） | 生产环境 |
|---|---|
| CSV 模拟业务库导出 | DataX/Canal CDC |
| local[*] 单机 + 本地 Kafka/Flink | YARN/K8s 集群 |
| TableStore overwrite 新快照 | Hudi/Iceberg merge into |
| 合成数据（百~万级） | 真实业务数据（亿级） |
| Flink availableNow 提交 | 常驻流作业 + Savepoint |
| Streamlit/FastAPI 本地服务 | BI 平台 + API 网关 |

## 已知边界

- 同一 dt 重复生产：批量文件会覆盖，Kafka topic 追加累积——每个业务日期
  只应生产一次（对账前先用新 dt）；
- `--mode tianchi` 映射真实数据集未在本机实测（数据文件 175MB，路径见 config）；
- Airflow 未安装（Python 3.14 暂不支持），DAG 文件作为交付物，本地用
  `tests/smoke_test.py` 或 `scripts/run_local.sh` 等价验证。
