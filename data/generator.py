"""业务源数据生成器（模拟业务库每日导出 + 实时消息流）。

三种用法：
    # 合成模式：生成当日业务三表快照（默认）
    python -m data.generator --dt 2026-09-22 --mode synthetic

    # 天池模式：把真实天池信贷数据集映射为业务三表
    python -m data.generator --dt 2026-09-22 --mode tianchi

    # 实时流模式：向 landing 目录滚动写入还款流 JSON（模拟 Kafka topic）
    python -m data.generator --dt 2026-09-22 --stream 10

产出目录：
    data/business/customer/dt={dt}/customer.csv           客户全量快照
    data/business/loan_application/dt={dt}/loan_application.csv
    data/business/repay_flow/dt={dt}/repay_flow.csv
    data/landing/repay_stream/batch_{seq}.json             实时流消息（JSON Lines）

说明：合成数据使用 sha256 驱动的确定性伪随机序列（_DetRand），保证同一
(seed, dt) 永远产出同一份数据，便于测试复现与流批对账。
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import re
import time
from datetime import date, datetime, timedelta
from pathlib import Path

from spark_jobs.common.paths import PROJECT_ROOT, Paths

CITIES = ["上海", "北京", "深圳", "广州", "杭州", "成都", "武汉", "南京", "西安", "苏州"]
OCCUPATIONS = ["企业职员", "公务员", "自由职业", "个体经营", "专业技术", "服务业", "其他"]
EDUCATIONS = ["高中及以下", "大专", "本科", "硕士", "博士"]
MARITALS = ["未婚", "已婚", "离异", "丧偶"]
PURPOSES = ["购车", "装修", "教育", "医疗", "经营周转", "消费分期", "其他"]
GRADES = ["A", "B", "C", "D", "E", "F", "G"]
HOME_OWNERSHIP = ["自有", "按揭", "租房", "其他"]

_DT_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _stable_hash(*parts: str) -> int:
    """确定性哈希：同一输入永远得到同一数值，保证合成数据可复现。"""
    return int(hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:12], 16)


def _validate_dt(dt: str) -> str:
    """业务日期必须严格为 YYYY-MM-DD：它参与拼结存储路径，必须阻断 .. 穿越。"""
    if not _DT_PATTERN.match(dt):
        raise ValueError(f"--dt 必须为 YYYY-MM-DD 格式: {dt!r}")
    datetime.strptime(dt, "%Y-%m-%d")  # round-trip 校验（如 2026-02-30 会在此拒绝）
    return dt


def _ensure_within(base: str, target: str) -> Path:
    """防御性校验：目标路径必须位于 base 之内，阻断 .. 穿越。"""
    base_abs = os.path.abspath(base)
    target_abs = os.path.abspath(target)
    if not target_abs.startswith(base_abs + os.sep):
        raise ValueError(f"路径越界: {target_abs} 不在 {base_abs} 内")
    return Path(target_abs)


class _DetRand:
    """确定性伪随机数发生器：sha256(state) 递推，[0,1) 均匀分布。

    合成演示数据需要可复现序列（测试断言、流批对账都依赖确定性），
    因此不使用 random 模块，改用哈希递推实现同等功能。
    """

    def __init__(self, seed: str):
        self._state = _stable_hash(seed)

    def _next(self) -> float:
        self._state = _stable_hash("detrand", str(self._state))
        return (self._state % 10**12) / 10**12

    def randint(self, lo: int, hi: int) -> int:
        return lo + int(self._next() * (hi - lo + 1)) % (hi - lo + 1)

    def uniform(self, lo: float, hi: float) -> float:
        return lo + self._next() * (hi - lo)

    def choice(self, seq: list):
        return seq[int(self._next() * len(seq)) % len(seq)]


def _rand_dt_datetime(rng: _DetRand, dt: str) -> str:
    base = datetime.strptime(dt, "%Y-%m-%d")
    return (
        base.replace(hour=rng.randint(0, 23), minute=rng.randint(0, 59), second=rng.randint(0, 59))
    ).strftime("%Y-%m-%d %H:%M:%S")


def gen_customers(n: int, dt: str) -> list[dict]:
    """客户全量快照。

    约 1% 的客户属性会按日期确定性漂移（职业/城市变更），用于演示 DWD 层
    拉链表（SCD2）的版本合并逻辑。
    """
    rows = []
    for i in range(1, n + 1):
        cid = f"C{i:06d}"
        h = _stable_hash(cid)
        drifted = _stable_hash(cid, dt) % 100 == 0
        rows.append(
            {
                "customer_id": cid,
                "customer_name": f"用户{i:06d}",
                "gender": "M" if h % 2 else "F",
                "age": 22 + h % 40,
                "education": EDUCATIONS[h % len(EDUCATIONS)],
                "marital_status": MARITALS[(h // 5) % len(MARITALS)],
                "occupation": OCCUPATIONS[(h // 7 + (1 if drifted else 0)) % len(OCCUPATIONS)],
                "city": CITIES[(h // 11 + (2 if drifted else 0)) % len(CITIES)],
                "register_date": (date(2020, 1, 1) + timedelta(days=h % 1500)).isoformat(),
            }
        )
    return rows


def gen_applications(customers: list[dict], n: int, dt: str, rng: _DetRand) -> list[dict]:
    rows = []
    for i in range(1, n + 1):
        cust = customers[rng.randint(0, len(customers) - 1)]
        h = _stable_hash("app", dt, str(i))
        annual_income = 60000 + h % 400000
        loan_amnt = 5000 + (h // 7) % 295000
        term = rng.choice([12, 24, 36, 48, 60])
        grade = GRADES[h % len(GRADES)]
        is_approved = 1 if rng._next() < 0.72 else 0
        # 违约率随 grade 恶化而上升，模拟真实风险分布
        default_prob = 0.05 + GRADES.index(grade) * 0.03
        rows.append(
            {
                "application_id": f"A{dt.replace('-', '')}{i:07d}",
                "customer_id": cust["customer_id"],
                "apply_time": _rand_dt_datetime(rng, dt),
                "loan_amnt": loan_amnt,
                "term": term,
                "interest_rate": round(0.045 + GRADES.index(grade) * 0.015 + rng._next() * 0.02, 4),
                "purpose": PURPOSES[h % len(PURPOSES)],
                "grade": grade,
                "annual_income": annual_income,
                "dti": round(rng.uniform(0.05, 0.6), 4),
                "fico_score": 620 + h % 240,
                "verification_status": rng.choice(["已核实", "未核实", "核实中"]),
                "home_ownership": HOME_OWNERSHIP[h % len(HOME_OWNERSHIP)],
                "is_approved": is_approved,
                "is_default": (1 if (is_approved and rng._next() < default_prob) else 0),
            }
        )
    return rows


def gen_repay_flows(applications: list[dict], customers: list[dict], dt: str, rng: _DetRand) -> list[dict]:
    """当日还款流水：对当日已放款申请生成一期应还/已还记录。"""
    approved = [a for a in applications if a["is_approved"] == 1]
    rows = []
    for i, app in enumerate(approved, start=1):
        installment = round(app["loan_amnt"] / app["term"], 2)
        overdue = rng._next() < 0.12
        rows.append(
            {
                "flow_id": f"R{dt.replace('-', '')}{i:07d}",
                "application_id": app["application_id"],
                "customer_id": app["customer_id"],
                "due_date": dt,
                "repay_time": _rand_dt_datetime(rng, dt),
                "due_amount": installment,
                "repay_amount": 0 if (overdue and rng._next() < 0.5) else installment,
                "overdue_days": rng.choice([1, 3, 7, 15, 30]) if overdue else 0,
            }
        )
    return rows


# ---------------------------------------------------------------- 天池模式
def _resolve_tianchi_csv(raw_path: str) -> Path:
    """路径 Jail：规范化后必须位于本工作区内且真实存在，拒绝 .. 穿越。"""
    path = os.path.normpath(os.path.abspath(raw_path))
    if ".." in path.split(os.sep):
        raise ValueError(f"拒绝包含 .. 的路径: {raw_path}")
    workspace = os.path.abspath(os.path.join(PROJECT_ROOT, "..", ".."))
    if not path.startswith(workspace + os.sep):
        raise ValueError(f"路径必须位于工作区内({workspace}): {path}")
    p = Path(path)
    if not p.is_file():
        raise FileNotFoundError(f"天池数据文件不存在: {path}")
    return p


def gen_from_tianchi(tianchi_csv: str, n: int, dt: str) -> tuple[list[dict], list[dict], list[dict]]:
    """把天池信贷数据集（LendingClub 风格）映射为业务三表。"""
    import pandas as pd

    csv_path = _resolve_tianchi_csv(tianchi_csv)
    df = pd.read_csv(csv_path, nrows=max(n * 3, 1000))
    df = df.sample(n=min(n, len(df)), random_state=42)

    customers, applications = [], []
    for row in df.itertuples(index=False):
        cid = f"C{int(row.id):08d}"
        customers.append(
            {
                "customer_id": cid,
                "customer_name": f"用户{int(row.id):08d}",
                "gender": "M" if int(row.id) % 2 else "F",
                "age": 25 + int(row.id) % 35,
                "education": "本科" if row.annualIncome > 100000 else "大专",
                "marital_status": "已婚" if int(row.id) % 3 else "未婚",
                "occupation": str(row.employmentTitle)[:20] if pd.notna(row.employmentTitle) else "其他",
                "city": f"区域{int(row.regionCode)}",
                "register_date": "2021-01-01",
            }
        )
        applications.append(
            {
                "application_id": f"A{dt.replace('-', '')}{int(row.id):08d}",
                "customer_id": cid,
                "apply_time": f"{dt} 12:00:00",
                "loan_amnt": float(row.loanAmnt),
                "term": int(row.term),
                "interest_rate": float(row.interestRate),
                "purpose": str(row.purpose),
                "grade": str(row.grade),
                "annual_income": float(row.annualIncome) if pd.notna(row.annualIncome) else 0.0,
                "dti": float(row.dti) if pd.notna(row.dti) else 0.0,
                "fico_score": int(row.ficoRangeLow),
                "verification_status": str(row.verificationStatus),
                "home_ownership": str(row.homeOwnership),
                "is_approved": 1,
                "is_default": int(row.isDefault),
            }
        )

    rng = _DetRand(f"tianchi-{dt}")
    flows = gen_repay_flows(applications, customers, dt, rng)
    return customers, applications, flows


# ---------------------------------------------------------------- 输出
def _write_csv(base_root: str, path_dir: str, filename: str, rows: list[dict]) -> Path:
    """写 CSV（pathlib 落盘，路径经 _ensure_within 收敛在项目目录内）。"""
    path = _ensure_within(base_root, os.path.join(path_dir, filename))
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        return path
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
    path.write_text(buf.getvalue(), encoding="utf-8")
    return path


def write_stream_batches(landing: str, dt: str, flows: list[dict], batches: int, interval: float) -> None:
    """模拟 Kafka：把当日还款流水按事件时间切片，分批写入 landing 目录。

    流消息与批量还款流水同源同值（金额、事件时间一致），这样流批对账
    比较的是"同一批事件的两种消费方式"，而不是两批无关数据。
    """
    landing_root = Path(os.path.abspath(landing))
    topic_dir = _ensure_within(landing, os.path.join(str(landing_root), "repay_stream"))
    topic_dir.mkdir(parents=True, exist_ok=True)

    if not flows:
        return
    per_batch = max(1, (len(flows) + batches - 1) // batches)
    for seq, start in enumerate(range(0, len(flows), per_batch), start=1):
        chunk = flows[start : start + per_batch]
        msgs = [
            {
                "flow_id": f["flow_id"],
                "application_id": f["application_id"],
                "customer_id": f["customer_id"],
                "event_time": f["repay_time"],
                "repay_amount": f["repay_amount"],
                "overdue_days": f["overdue_days"],
            }
            for f in chunk
        ]
        # 文件名带业务日期：每日文件是新路径，流作业（带 checkpoint 的
        # 文件源）只会消费新文件，不会因同名覆盖而被跳过
        path = _ensure_within(str(landing_root), os.path.join(str(topic_dir), f"batch_{dt}_{seq:03d}.json"))
        path.write_text(
            "\n".join(json.dumps(m, ensure_ascii=False) for m in msgs) + "\n",
            encoding="utf-8",
        )
        print(f"[stream] wrote batch_{seq:03d}.json ({len(msgs)} msgs)")
        if seq < batches:
            time.sleep(interval)


def _load_config() -> dict:
    import yaml

    cfg_path = Path(PROJECT_ROOT, "config", "config.yaml")
    return yaml.safe_load(cfg_path.read_text(encoding="utf-8"))


def produce_to_kafka(flows: list[dict], bootstrap_servers: str, topic: str) -> int:
    """把还款事件以 JSON 消息发布到 Kafka topic（模拟业务系统 CDC/埋点上报）。"""
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
        retries=3,
    )
    sent = 0
    for f in flows:
        producer.send(
            topic,
            {
                "flow_id": f["flow_id"],
                "application_id": f["application_id"],
                "customer_id": f["customer_id"],
                "event_time": f["repay_time"],
                "repay_amount": f["repay_amount"],
                "overdue_days": f["overdue_days"],
            },
        )
        sent += 1
    producer.flush()
    producer.close()
    print(f"[kafka] produced {sent} msgs -> {topic}@{bootstrap_servers}")
    return sent


def main() -> None:
    cfg = _load_config()
    paths = Paths.from_config(cfg)
    gen_cfg = cfg["generator"]

    parser = argparse.ArgumentParser(description="信贷业务数据生成器")
    parser.add_argument("--dt", required=True, help="业务日期 YYYY-MM-DD")
    parser.add_argument("--mode", choices=["synthetic", "tianchi"], default=cfg["source"]["mode"])
    parser.add_argument("--tianchi-csv", default=cfg["source"]["tianchi_csv"])
    parser.add_argument("--n-customers", type=int, default=gen_cfg["n_customers"])
    parser.add_argument("--n-applications", type=int, default=gen_cfg["n_applications"])
    parser.add_argument("--stream", type=int, default=0, help="同时生成 N 个实时流批次文件")
    parser.add_argument(
        "--sink",
        choices=["file", "kafka", "both"],
        default="file",
        help="实时流出口：file=落地 JSON 文件（Spark Streaming 用）；kafka=真 Kafka topic；both=两者",
    )
    args = parser.parse_args()

    dt = _validate_dt(args.dt)

    if args.mode == "tianchi":
        customers, applications, flows = gen_from_tianchi(args.tianchi_csv, args.n_applications, dt)
    else:
        rng = _DetRand(f"{gen_cfg['seed']}-{dt}")
        customers = gen_customers(args.n_customers, dt)
        applications = gen_applications(customers, args.n_applications, dt, rng)
        flows = gen_repay_flows(applications, customers, dt, rng)

    print(f"[gen] mode={args.mode} dt={dt} customers={len(customers)} "
          f"applications={len(applications)} repay_flows={len(flows)}")
    print(" ->", _write_csv(PROJECT_ROOT, paths.business_table("customer", dt), "customer.csv", customers))
    print(" ->", _write_csv(PROJECT_ROOT, paths.business_table("loan_application", dt), "loan_application.csv", applications))
    print(" ->", _write_csv(PROJECT_ROOT, paths.business_table("repay_flow", dt), "repay_flow.csv", flows))

    kafka_cfg = cfg.get("kafka", {})
    if args.sink in ("kafka", "both"):
        produce_to_kafka(
            flows,
            bootstrap_servers=kafka_cfg.get("bootstrap_servers", "localhost:9092"),
            topic=kafka_cfg.get("topic_repay_stream", "repay_stream"),
        )
    if args.stream or args.sink in ("file", "both"):
        write_stream_batches(
            paths.landing_dir, dt, flows,
            batches=max(args.stream, 1),
            interval=gen_cfg["stream_interval_sec"],
        )


if __name__ == "__main__":
    main()
