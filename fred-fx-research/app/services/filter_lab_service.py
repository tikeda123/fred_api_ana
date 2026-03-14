"""
Filter Lab Service
- カスタム閾値フィルタ評価
- フィルタ適用 PnL シミュレーション
"""

import math
from datetime import date
from typing import Optional

import polars as pl

from app.core.logging import get_logger
from app.storage.repositories import cross_asset_repo

logger = get_logger(__name__)


def evaluate_custom_filter(
    pair: str,
    start: date,
    end: date,
    dd_threshold: float = -8.0,
    rv_percentile: int = 80,
    vix_percentile: int = 80,
    usd_z_threshold: float = 1.0,
    min_conditions: int = 2,
) -> dict:
    """
    カスタム閾値でリスクフィルタを評価する。
    """
    records = cross_asset_repo.get_panel(pair, start, end)
    if not records:
        return {"records": [], "row_count": 0, "thresholds_used": {}}

    df = pl.DataFrame(records, infer_schema_length=None).sort("obs_date")

    # 各条件を評価
    # 1. Drawdown condition
    dd_col = "usatech_drawdown_20d"
    has_dd = dd_col in df.columns and df[dd_col].drop_nulls().len() > 0
    if has_dd:
        df = df.with_columns(
            (pl.col(dd_col) * 100 <= dd_threshold).fill_null(False).alias("cond_dd")
        )
    else:
        df = df.with_columns(pl.lit(False).alias("cond_dd"))

    # 2. RV condition (percentile-based)
    rv_col = "usatech_rv_20d"
    has_rv = rv_col in df.columns and df[rv_col].drop_nulls().len() > 0
    rv_threshold_val = None
    if has_rv:
        rv_vals = df[rv_col].drop_nulls()
        rv_threshold_val = float(rv_vals.quantile(rv_percentile / 100.0))
        df = df.with_columns(
            (pl.col(rv_col) > rv_threshold_val).fill_null(False).alias("cond_rv")
        )
    else:
        df = df.with_columns(pl.lit(False).alias("cond_rv"))

    # 3. VIX condition (percentile-based)
    vix_col = "vix_close"
    has_vix = vix_col in df.columns and df[vix_col].drop_nulls().len() > 0
    vix_threshold_val = None
    if has_vix:
        vix_vals = df[vix_col].drop_nulls()
        vix_threshold_val = float(vix_vals.quantile(vix_percentile / 100.0))
        df = df.with_columns(
            (pl.col(vix_col) > vix_threshold_val).fill_null(False).alias("cond_vix")
        )
    else:
        df = df.with_columns(pl.lit(False).alias("cond_vix"))

    # 4. USD Broad Z-score condition
    usd_col = "usd_broad_close"
    has_usd = usd_col in df.columns and df[usd_col].drop_nulls().len() > 0
    usd_mean = None
    usd_std = None
    if has_usd:
        usd_vals = df[usd_col].drop_nulls()
        usd_mean = float(usd_vals.mean())
        usd_std = float(usd_vals.std()) or 1.0
        df = df.with_columns(
            (((pl.col(usd_col) - usd_mean) / usd_std) >= usd_z_threshold)
            .fill_null(False)
            .alias("cond_usd")
        )
    else:
        df = df.with_columns(pl.lit(False).alias("cond_usd"))

    # 条件数カウント → フィルタフラグ
    df = df.with_columns(
        (
            pl.col("cond_dd").cast(pl.Int32)
            + pl.col("cond_rv").cast(pl.Int32)
            + pl.col("cond_vix").cast(pl.Int32)
            + pl.col("cond_usd").cast(pl.Int32)
        ).alias("conditions_met")
    )
    df = df.with_columns(
        pl.when(pl.col("conditions_met") >= min_conditions)
        .then(pl.lit("avoid"))
        .otherwise(pl.lit("ok"))
        .alias("custom_flag")
    )

    # 出力
    output_cols = ["obs_date", "custom_flag", "conditions_met", "cond_dd", "cond_rv", "cond_vix", "cond_usd"]
    result_records = df.select([c for c in output_cols if c in df.columns]).to_dicts()

    # NaN cleanup
    for rec in result_records:
        for k, v in rec.items():
            if isinstance(v, float) and math.isnan(v):
                rec[k] = None

    avoid_count = sum(1 for r in result_records if r.get("custom_flag") == "avoid")
    total = len(result_records)

    return {
        "pair": pair,
        "records": result_records,
        "row_count": total,
        "avoid_count": avoid_count,
        "avoid_pct": round(avoid_count / total * 100, 2) if total > 0 else 0,
        "thresholds_used": {
            "drawdown_pct": dd_threshold,
            "rv_percentile": rv_percentile,
            "rv_threshold_value": rv_threshold_val,
            "vix_percentile": vix_percentile,
            "vix_threshold_value": vix_threshold_val,
            "usd_z": usd_z_threshold,
            "usd_mean": usd_mean,
            "usd_std": usd_std,
            "min_conditions": min_conditions,
        },
    }


def simulate_pnl(
    pair: str,
    start: date,
    end: date,
    dd_threshold: float = -8.0,
    rv_percentile: int = 80,
    vix_percentile: int = 80,
    usd_z_threshold: float = 1.0,
    min_conditions: int = 2,
) -> dict:
    """
    フィルタ適用の PnL シミュレーション。
    """
    # まずフィルタ評価
    filter_result = evaluate_custom_filter(
        pair, start, end, dd_threshold, rv_percentile,
        vix_percentile, usd_z_threshold, min_conditions,
    )

    if not filter_result["records"]:
        return {
            "pair": pair,
            "pnl_series": [],
            "stats": {},
        }

    # パネルデータを再取得してリターンベースのPnL計算
    records = cross_asset_repo.get_panel(pair, start, end)
    df = pl.DataFrame(records, infer_schema_length=None).sort("obs_date")

    if "pair_ret_1d" not in df.columns:
        return {"pair": pair, "pnl_series": [], "stats": {}}

    # フィルタフラグを結合
    filter_df = pl.DataFrame(filter_result["records"])
    if "obs_date" in filter_df.columns:
        # obs_date 型を合わせる
        df = df.with_columns(pl.col("obs_date").cast(pl.Utf8).alias("_date_str"))
        filter_df = filter_df.with_columns(pl.col("obs_date").cast(pl.Utf8).alias("_date_str"))
        df = df.join(
            filter_df.select(["_date_str", "custom_flag"]),
            on="_date_str",
            how="left",
        ).drop("_date_str")
    else:
        df = df.with_columns(pl.lit("ok").alias("custom_flag"))

    # PnL 計算
    pnl_series = []
    base_cum = 0.0
    filtered_cum = 0.0

    for rec in df.iter_rows(named=True):
        ret = rec.get("pair_ret_1d")
        flag = rec.get("custom_flag", "ok")
        obs = rec.get("obs_date")

        if ret is None or (isinstance(ret, float) and math.isnan(ret)):
            ret = 0.0

        base_cum += ret
        if flag != "avoid":
            filtered_cum += ret

        pnl_series.append({
            "obs_date": str(obs) if obs else None,
            "base_cumulative": round(base_cum * 100, 4),
            "filtered_cumulative": round(filtered_cum * 100, 4),
            "is_avoid": flag == "avoid",
        })

    avoid_count = filter_result["avoid_count"]
    total = filter_result["row_count"]

    return {
        "pair": pair,
        "pnl_series": pnl_series,
        "stats": {
            "base_final_return_pct": round(base_cum * 100, 4),
            "filtered_final_return_pct": round(filtered_cum * 100, 4),
            "improvement_pct": round((filtered_cum - base_cum) * 100, 4),
            "avoid_days": avoid_count,
            "total_days": total,
            "avoid_pct": round(avoid_count / total * 100, 2) if total > 0 else 0,
        },
        "thresholds_used": filter_result["thresholds_used"],
    }
