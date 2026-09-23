"""service/app.py：数据服务层（FastAPI）。

把数仓 ADS 层数据包装成 HTTP API，对应 JD 中"数据服务"消费场景：
- GET /health                          健康检查
- GET /api/v1/risk/daily?dt=...        风控日报（ads_risk_daily）
- GET /api/v1/scorecard/features/{application_id}
                                       评分卡特征点查（ads_scorecard_features）
- GET /api/v1/customer/{customer_id}/features
                                       客户最新特征（dws_customer_day 当前快照）

工程要点：
- parquet 读取用 pandas + pyarrow（Web 服务不起 Spark，避免重量级依赖）；
- 进程内 TTL 缓存（默认 60s），热点查询不打盘；
- 数据目录解析复用 spark_jobs.common.paths，与离线作业同一套口径。

运行：uvicorn service.app:app --port 8000  （或 python -m service.app）
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pandas as pd  # noqa: E402
from fastapi import FastAPI, HTTPException, Query  # noqa: E402

from spark_jobs.common.paths import Paths, load_config  # noqa: E402
from table_store import TableStore  # noqa: E402

CACHE_TTL_SEC = 60
_cache: dict[str, tuple[float, pd.DataFrame]] = {}

app = FastAPI(title="信贷数仓数据服务", version="1.0.0")


def _cached_read(key: str, loader) -> pd.DataFrame:
    """TTL 缓存读取：60s 内的重复查询直接命中内存。"""
    now = time.time()
    hit = _cache.get(key)
    if hit and now - hit[0] < CACHE_TTL_SEC:
        return hit[1]
    df = loader()
    _cache[key] = (now, df)
    return df


def _read_parquet_dir(path: str) -> pd.DataFrame:
    p = Path(path)
    if not p.is_dir():
        return pd.DataFrame()
    return pd.read_parquet(p)


def _read_parquet_file(path: str) -> pd.DataFrame:
    p = Path(path)
    if not p.is_file():
        return pd.DataFrame()
    return pd.read_parquet(p)


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "cache_entries": len(_cache)}


@app.get("/api/v1/risk/daily")
def risk_daily(dt: str = Query(..., description="业务日期 YYYY-MM-DD")) -> dict:
    """风控日报：当日申请量/通过率/放款金额/违约率 + 7 日滚动。"""
    paths = Paths.from_config(load_config())

    def _load() -> pd.DataFrame:
        return _read_parquet_dir(paths.ads("ads_risk_daily"))

    df = _cached_read("ads_risk_daily", _load)
    if df.empty:
        raise HTTPException(status_code=404, detail="ads_risk_daily 无数据")
    row = df[df["apply_date"].astype(str) == dt]
    if row.empty:
        raise HTTPException(status_code=404, detail=f"dt={dt} 无日报数据")
    return {"dt": dt, "data": row.to_dict(orient="records")[0]}


@app.get("/api/v1/scorecard/features/{application_id}")
def scorecard_features(application_id: str) -> dict:
    """评分卡特征点查：按申请号返回特征宽表整行。"""
    paths = Paths.from_config(load_config())
    dt_list = sorted(
        d.name.split("=")[-1]
        for d in Path(paths.ads("ads_scorecard_features")).parent.glob("ads_scorecard_features/dt=*")
    ) if Path(paths.ads("ads_scorecard_features")).parent.is_dir() else []
    for dt in reversed(dt_list):
        df = _cached_read(
            f"features:{dt}",
            lambda dt=dt: _read_parquet_dir(paths.ads("ads_scorecard_features", dt)),
        )
        row = df[df["application_id"] == application_id]
        if not row.empty:
            return {"dt": dt, "data": row.to_dict(orient="records")[0]}
    raise HTTPException(status_code=404, detail=f"application_id={application_id} 无特征记录")


@app.get("/api/v1/customer/{customer_id}/features")
def customer_features(customer_id: str) -> dict:
    """客户最新特征：dws_customer_day 当前业务日快照。"""
    paths = Paths.from_config(load_config())
    dws_dir = Path(paths.dws("dws_customer_day")).parent
    dt_list = sorted(d.name.split("=")[-1] for d in dws_dir.glob("dws_customer_day/dt=*")) if dws_dir.is_dir() else []
    for dt in reversed(dt_list):
        df = _cached_read(
            f"cust_day:{dt}",
            lambda dt=dt: _read_parquet_dir(paths.dws("dws_customer_day", dt)),
        )
        row = df[df["customer_id"] == customer_id]
        if not row.empty:
            return {"dt": dt, "data": row.to_dict(orient="records")[0]}
    raise HTTPException(status_code=404, detail=f"customer_id={customer_id} 无特征记录")


@app.get("/api/v1/dim/customer/{customer_id}")
def dim_customer(customer_id: str) -> dict:
    """客户维度当前版本（TableStore 当前快照点查）。"""
    paths = Paths.from_config(load_config())

    def _load() -> pd.DataFrame:
        return _read_parquet_dir(str(TableStore().snapshot_path("dwd_dim_customer")))

    df = _cached_read("dim_customer", _load)
    if df.empty:
        raise HTTPException(status_code=404, detail="dwd_dim_customer 无数据")
    row = df[(df["customer_id"] == customer_id) & (df["end_date"] == "9999-12-31")]
    if row.empty:
        raise HTTPException(status_code=404, detail=f"customer_id={customer_id} 无当前维度版本")
    return {"data": row.to_dict(orient="records")[0]}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
