"""
Rolling Stats Service
- ローリング相関・β・ラグ相関の計算
"""

import math
from datetime import date
from typing import Optional

import polars as pl

from app.core.logging import get_logger
from app.storage.repositories import cross_asset_repo

logger = get_logger(__name__)


def compute_rolling_stats(
    pair: str,
    start: date,
    end: date,
    corr_window: int = 20,
    beta_window: int = 60,
    max_lag: int = 5,
) -> dict:
    """
    FX × USATECH のローリング相関・β・ラグ相関を計算する。
    """
    records = cross_asset_repo.get_panel(pair, start, end)
    if not records:
        return {"time_series": [], "lag_correlations": {}, "row_count": 0}

    df = pl.DataFrame(records, infer_schema_length=None)

    # pair_ret_1d と usatech_ret_1d が必要
    if "pair_ret_1d" not in df.columns or "usatech_ret_1d" not in df.columns:
        return {"time_series": [], "lag_correlations": {}, "row_count": 0}

    df = df.sort("obs_date")

    # ローリング相関
    df = df.with_columns([
        pl.rolling_corr(
            pl.col("usatech_ret_1d"), pl.col("pair_ret_1d"),
            window_size=corr_window,
        ).alias("rolling_corr"),
        pl.rolling_cov(
            pl.col("usatech_ret_1d"), pl.col("pair_ret_1d"),
            window_size=beta_window,
        ).alias("_cov"),
        pl.col("usatech_ret_1d").rolling_var(window_size=beta_window).alias("_var"),
    ])

    # ローリングβ = cov / var
    df = df.with_columns(
        (pl.col("_cov") / pl.col("_var")).alias("rolling_beta")
    )

    # time_series 出力
    ts_records = df.select([
        "obs_date", "rolling_corr", "rolling_beta"
    ]).to_dicts()

    # NaN → None
    for rec in ts_records:
        for k in ["rolling_corr", "rolling_beta"]:
            v = rec.get(k)
            if v is not None and isinstance(v, float) and math.isnan(v):
                rec[k] = None

    # ラグ相関（全期間のスカラー値）
    lag_correlations = {}
    pair_ret = df["pair_ret_1d"].drop_nulls()
    usatech_ret = df["usatech_ret_1d"].drop_nulls()

    for lag in range(0, max_lag + 1):
        try:
            if lag == 0:
                corr_val = pair_ret.pearson_corr(usatech_ret)
            else:
                # usatech_ret を lag 日シフト
                shifted = df.with_columns(
                    pl.col("usatech_ret_1d").shift(lag).alias("usatech_lagged")
                ).drop_nulls(subset=["pair_ret_1d", "usatech_lagged"])
                corr_val = shifted["pair_ret_1d"].pearson_corr(shifted["usatech_lagged"])

            if corr_val is not None and not math.isnan(corr_val):
                lag_correlations[f"lag_{lag}"] = round(corr_val, 6)
            else:
                lag_correlations[f"lag_{lag}"] = None
        except Exception:
            lag_correlations[f"lag_{lag}"] = None

    return {
        "pair": pair,
        "corr_window": corr_window,
        "beta_window": beta_window,
        "time_series": ts_records,
        "lag_correlations": lag_correlations,
        "row_count": len(ts_records),
    }
