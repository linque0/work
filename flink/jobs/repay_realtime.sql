-- =============================================================================
-- 实时还款指标作业（Flink SQL）
-- 链路：Kafka(repay_stream) -> 1min 滚动窗口聚合 -> filesystem JSON + Kafka
-- 提交：scripts/flink_submit.sh（内部直启 SqlClient，见脚本内 Windows 适配说明）
-- 说明：sink 路径占位符 {{SINK_PATH}} 由提交脚本替换为项目绝对路径
-- =============================================================================

-- 提交后不挂起（测试/调度友好）；作业在集群上持续运行
SET 'execution.attached' = 'false';

-- ---------- ODS：Kafka 原始流 ----------
-- 关键设计：两个 INSERT 作业（fs sink / kafka sink）必须使用不同 group.id。
-- 同一消费者组会把 topic 分区分配给组内多个消费者，每个作业只拿到部分
-- 分区（实测踩坑：fs sink 丢数据、流批对账必挂），故按 sink 拆两个源表。
CREATE TABLE IF NOT EXISTS repay_stream_fs (
    flow_id        STRING,
    application_id STRING,
    customer_id    STRING,
    event_time     TIMESTAMP(3),
    repay_amount   DOUBLE,
    overdue_days   INT,
    -- 允许 10s 乱序；Watermark 是实时数仓处理乱序与窗口触发的核心机制
    WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'repay_stream',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id'          = 'flink-repay-dw-fs',
    'scan.startup.mode'            = 'earliest-offset',
    -- 有界读取：消费到启动时的最新位点后作业自然结束（等价 availableNow），
    -- 窗口全部触发、结果完整可复现；生产环境改回无界流 + checkpoint 常驻
    'scan.bounded.mode'            = 'latest-offset',
    'format'                       = 'json',
    'json.ignore-parse-errors'     = 'true'
);

CREATE TABLE IF NOT EXISTS repay_stream_kafka (
    flow_id        STRING,
    application_id STRING,
    customer_id    STRING,
    event_time     TIMESTAMP(3),
    repay_amount   DOUBLE,
    overdue_days   INT,
    WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'repay_stream',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id'          = 'flink-repay-dw-kafka',
    'scan.startup.mode'            = 'earliest-offset',
    -- 有界读取：消费到启动时的最新位点后作业自然结束（等价 availableNow），
    -- 窗口全部触发、结果完整可复现；生产环境改回无界流 + checkpoint 常驻
    'scan.bounded.mode'            = 'latest-offset',
    'format'                       = 'json',
    'json.ignore-parse-errors'     = 'true'
);

-- ---------- DWS sink 1：filesystem JSON（供流批对账与看板读取） ----------
CREATE TABLE repay_metrics_fs (
    window_start     TIMESTAMP(3),
    window_end       TIMESTAMP(3),
    msg_cnt          BIGINT,
    repay_amt_sum    DOUBLE,
    overdue_cnt      BIGINT,
    overdue_amt_sum  DOUBLE
) WITH (
    'connector'                                = 'filesystem',
    'path'                                     = '{{SINK_PATH}}',
    'format'                                   = 'json',
    'sink.rolling-policy.file-size'            = '128MB',
    'sink.rolling-policy.rollover-interval'    = '30s',
    'sink.rolling-policy.check-interval'       = '10s'
);

-- ---------- DWS sink 2：回写 Kafka（演示 Kafka 既做源也做 sink） ----------
CREATE TABLE repay_metrics_kafka (
    window_start     TIMESTAMP(3),
    window_end       TIMESTAMP(3),
    msg_cnt          BIGINT,
    repay_amt_sum    DOUBLE,
    overdue_cnt      BIGINT,
    overdue_amt_sum  DOUBLE
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'repay_dws_1min',
    'properties.bootstrap.servers' = 'localhost:9092',
    'format'                       = 'json'
);

-- ---------- 指标计算：1 分钟滚动窗口 ----------
INSERT INTO repay_metrics_fs
SELECT
    window_start,
    window_end,
    COUNT(*)                                                        AS msg_cnt,
    ROUND(SUM(repay_amount), 2)                                     AS repay_amt_sum,
    SUM(CASE WHEN overdue_days > 0 THEN 1 ELSE 0 END)               AS overdue_cnt,
    ROUND(SUM(CASE WHEN overdue_days > 0 THEN repay_amount ELSE 0 END), 2) AS overdue_amt_sum
FROM TABLE(
    TUMBLE(TABLE repay_stream_fs, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
)
GROUP BY window_start, window_end;

INSERT INTO repay_metrics_kafka
SELECT
    window_start,
    window_end,
    COUNT(*)                                                        AS msg_cnt,
    ROUND(SUM(repay_amount), 2)                                     AS repay_amt_sum,
    SUM(CASE WHEN overdue_days > 0 THEN 1 ELSE 0 END)               AS overdue_cnt,
    ROUND(SUM(CASE WHEN overdue_days > 0 THEN repay_amount ELSE 0 END), 2) AS overdue_amt_sum
FROM TABLE(
    TUMBLE(TABLE repay_stream_kafka, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
)
GROUP BY window_start, window_end;
