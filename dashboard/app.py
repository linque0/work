"""风控日报看板（Streamlit）。

读取 ADS 层导出的 CSV 快照（warehouse/ads/_export_ads_risk_daily.csv），
展示当日核心指标与历史趋势。运行：

    streamlit run dashboard/app.py

设计说明：看板只读 ADS 导出层，不直接查数仓明细——与生产环境
"报表走 ADS 汇总表"的用法一致；CSV 快照是为了本地无 pyarrow 也能跑，
生产环境换成直连 Hive/Doris。
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pandas as pd  # noqa: E402
import streamlit as st  # noqa: E402

from spark_jobs.common.paths import PROJECT_ROOT, Paths, load_config  # noqa: E402


def load_export() -> pd.DataFrame:
    """读取 ADS 层导出的 CSV 目录（part-*.csv）。"""
    paths = Paths.from_config(load_config())
    export_dir = Path(paths.ads_export("ads_risk_daily"))
    if not export_dir.is_dir():
        return pd.DataFrame()
    parts = sorted(export_dir.glob("part-*.csv"))
    if not parts:
        return pd.DataFrame()
    frames = [pd.read_csv(p) for p in parts]
    return pd.concat(frames, ignore_index=True)


def load_flink_realtime() -> pd.DataFrame:
    """读取 Flink filesystem sink 的窗口聚合（JSON Lines，含 inprogress 文件）。"""
    import json

    paths = Paths.from_config(load_config())
    sink = Path(paths.ads("flink_repay_metrics"))
    if not sink.is_dir():
        return pd.DataFrame()
    rows = []
    for f in sink.iterdir():
        if not f.is_file() or f.name.startswith("._"):
            continue
        try:
            for line in f.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    continue  # 滚动写入可能读到半行
        except OSError:
            continue
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows).sort_values("window_start")
    df["dt"] = df["window_start"].str[:10]
    return df


def main() -> None:
    st.set_page_config(page_title="信贷风控日报", layout="wide")
    st.title("信贷风控日报看板")
    st.caption("数据来源：ADS 层 ads_risk_daily（离线 T+1，流批对账保障一致性）")

    df = load_export()
    if df.empty:
        st.warning("未找到 ADS 导出数据，请先运行 spark_jobs.ads_build。")
        return

    df = df.sort_values("apply_date")
    latest = df.iloc[-1]

    # KPI 卡片
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("业务日期", str(latest["apply_date"]))
    c2.metric("当日申请量", f"{int(latest['apply_cnt']):,}")
    c3.metric("通过率", f"{latest['approve_rate']:.1%}")
    c4.metric("当日放款金额", f"{latest['loan_amt_sum']:,.0f}")
    c5.metric("违约率", f"{latest['default_rate']:.1%}")

    st.divider()

    # 历史趋势
    left, right = st.columns(2)
    with left:
        st.subheader("申请量 / 通过量趋势")
        st.line_chart(df.set_index("apply_date")[["apply_cnt", "approve_cnt"]])
    with right:
        st.subheader("放款金额与 7 日滚动")
        st.line_chart(df.set_index("apply_date")[["loan_amt_sum", "loan_amt_7d"]])

    st.subheader("风控日报明细")
    st.dataframe(
        df[
            [
                "apply_date",
                "apply_cnt",
                "approve_cnt",
                "approve_rate",
                "loan_amt_sum",
                "avg_interest_rate",
                "default_cnt",
                "default_rate",
                "apply_cnt_7d",
                "apply_cnt_dod",
            ]
        ],
        use_container_width=True,
        hide_index=True,
    )

    # ---------- 实时链路（Flink sink） ----------
    st.divider()
    st.subheader("实时还款指标（Flink，1 分钟窗口）")
    rt = load_flink_realtime()
    if rt.empty:
        st.info("未找到 Flink 实时数据：先启动基础设施（scripts/infra_up.sh）并提交作业（scripts/flink_submit.sh）")
    else:
        latest_dt = rt["dt"].max()
        today = rt[rt["dt"] == latest_dt]
        c1, c2, c3 = st.columns(3)
        c1.metric("最新业务日", latest_dt)
        c2.metric("实时消息数", f"{int(today['msg_cnt'].sum()):,}")
        c3.metric("实时还款金额", f"{today['repay_amt_sum'].sum():,.0f}")
        st.caption("流批对账保障：同一指标离线 T+1 与本实时链路偏差 < 1%（quality/reconcile.py）")
        st.line_chart(today.set_index("window_start")[["msg_cnt", "repay_amt_sum"]])
        st.dataframe(
            today[["window_start", "window_end", "msg_cnt", "repay_amt_sum", "overdue_cnt", "overdue_amt_sum"]].tail(20),
            use_container_width=True,
            hide_index=True,
        )


if __name__ == "__main__":
    main()
