# 试运行结果效果图

> 运行时间：2026-09-23 ｜ 环境：Windows 11 + Python 3.14.7 + PySpark 4.2.0（local[*]）+
> Java 17.0.10 + Kafka 3.9.2（KRaft 单节点）+ Flink 1.20.0（JM+TM 本地集群）
>
> 复现方式：
> ```bash
> bash scripts/infra_up.sh                                        # Kafka + Flink
> bash scripts/run_realtime_demo.sh 2026-09-23                    # 实时链路全流程（复跑加 SKIP_PRODUCE=1）
> python tests/smoke_test.py --n-customers 2000 --n-applications 5000   # 冒烟测试
> ```

## 图片索引

| # | 文件 | 内容 |
|---|---|---|
| 1 | `01-smoke-test.png` | 端到端冒烟测试：两业务日 16 个步骤全 PASS、快照链、行数断言，`19 passed, 0 failed` |
| 2 | `02-realtime-pipeline.png` | Kafka + Flink 实时链路全流程：gen→ods→dwd→dws→ads→flink_submit→quality→reconcile→compaction→schema→lineage |
| 3 | `03-reconcile.png` | 流批对账：离线 T+1 与 Flink 1min 窗口双跑比对（repay_cnt / repay_amt 偏差 0.0%） |
| 4 | `04-quality.png` | 数据质量：9 张表 × 3 类规则（主键唯一率 / 空值率 / 行数波动）全通过 |
| 5 | `05-kafka.png` | Kafka 总线证据：repay_stream 3 分区描述 + 还款事件消息抽样（JSON） |
| 6 | `06-api.png` | FastAPI 数据服务响应：风控日报、维度当前版本（SCD2）、评分卡特征点查 |
| 7 | `07-governance.png` | 数据治理：Schema 漂移 11/11 PASS、TableStore 快照链、倾斜基准（加盐聚合） |
| 8 | `08-compaction.png` | 小文件治理：构造 8 个小文件场景 → compaction 原子替换合并为 1 个 |
| 9 | `09-flink-webui.png` | Flink WebUI：有界作业消费完成 FINISHED（`scan.bounded.mode=latest-offset`） |
| 10 | `10-dashboard.png` | Streamlit 风控日报看板：KPI 卡片 + 趋势 + Flink 实时窗口明细 |
| 11 | `11-api-docs.png` | FastAPI Swagger 文档（/docs）四个端点 |

## 关键数字

- 冒烟测试：**19 passed, 0 failed**（两业务日 2000 客户 / 5000 申请）
- 实时链路：生成 3557 条还款事件 → Kafka 3557 条消息被 Flink **全量消费**（有界模式），
  聚合出 **1295 个 1min 窗口**
- 流批对账：repay_cnt 3557 vs 3557、repay_amt 19,542,249.63 vs 19,542,249.63，
  相对偏差 **0.0%**（容差 1%）
- 数据质量：**9/9 表 PASS**（行数波动 0.0 ~ 0.0155，远低于 0.5 阈值）
- Schema 漂移：**11/11 表 PASS**（parquet footer 直比，不起 Spark）
- 数据倾斜：95% 流量集中 100 热点客户，加盐两阶段聚合把 task 耗时 max 从
  **76ms → 51ms**（median 60ms → 37ms）
- 小文件治理：**8 个文件（0.023MB/个）→ 1 个（0.138MB）**，原子替换

## 运行中发现并修复的工程问题（详见 README「Kafka/Flink 工程细节」）

1. `curl -o /dev/null` 在 `MSYS2_ARG_CONV_EXCL="*"` 下写 POSIX `/dev/null` 失败返回非零，
   集群在线却被提交脚本误判为未就绪 —— 探测统一改 shell 重定向 `> /dev/null`；
2. 无界流式作业在本地演示里无法判定"消费完成"，sink 停在部分数据（694/3557）——
   源表加 `scan.bounded.mode='latest-offset'`，作业消费到启动位点后 FINISHED；
3. Kafka topic 跨会话累积导致同一 dt 消息翻倍、窗口与对账口径失真 —— 重置 topic 后
   重新生产一次；Windows 删除 topic 曾因文件占用触发 broker 退出，恢复步骤已记入 README；
4. 冒烟测试重跑时 landing 文件同名被 availableNow 视为"已处理"跳过 —— 测试加状态隔离
   （清理 checkpoint / 实时输出表 / landing 批次文件）；
5. 报告页截图：Streamlit / Flink WebUI 为 JS 驱动页面，Edge 无头 `--virtual-time-budget`
   不等数据渲染 —— 改用 CDP（websocket）轮询等待页面条件后整页截图。
