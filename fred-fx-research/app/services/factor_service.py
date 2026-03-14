"""
FactorService
- macro panel から rate spread / curve slope / vol / momentum 因子を計算
- OHLC データから Parkinson vol / daily range / overnight gap 因子を計算
- fact_derived_factors に保存
- Parquet derived 層にも保存
"""

import math
from datetime import date, datetime
from typing import Optional

import polars as pl

from app.core.exceptions import StorageError
from app.core.logging import get_logger
from app.storage.duckdb import get_connection
from app.storage.parquet_io import write_derived

logger = get_logger(__name__)

# pair → 因子グループの定義
FACTOR_SPEC: dict[str, list[dict]] = {
    "USDJPY": [
        {"name": "us_jp_policy_spread","group": "rate_spreads", "formula": "us_policy - jp_policy", "cols": ("us_policy", "jp_policy")},
        {"name": "us_jp_3m_spread",  "group": "rate_spreads",  "formula": "us_3m - jp_3m",          "cols": ("us_3m",     "jp_3m")},
        {"name": "us_jp_10y_spread", "group": "rate_spreads",  "formula": "us_10y - jp_10y",         "cols": ("us_10y",    "jp_10y")},
        {"name": "us_curve",         "group": "curve_slopes",  "formula": "us_10y - us_3m",          "cols": ("us_10y",    "us_3m")},
        {"name": "jp_curve",         "group": "curve_slopes",  "formula": "jp_10y - jp_3m",          "cols": ("jp_10y",    "jp_3m")},
        {"name": "us_jp_reer_spread","group": "reer",          "formula": "us_reer - jp_reer",        "cols": ("us_reer",   "jp_reer")},
    ],
    "EURUSD": [
        {"name": "us_ez_policy_spread","group": "rate_spreads","formula": "us_policy - ez_policy",   "cols": ("us_policy", "ez_policy")},
        {"name": "us_ez_3m_spread",  "group": "rate_spreads",  "formula": "us_3m - ez_3m",           "cols": ("us_3m",     "ez_3m")},
        {"name": "us_ez_10y_spread", "group": "rate_spreads",  "formula": "us_10y - ez_10y",         "cols": ("us_10y",    "ez_10y")},
        {"name": "us_curve",         "group": "curve_slopes",  "formula": "us_10y - us_3m",          "cols": ("us_10y",    "us_3m")},
        {"name": "ez_curve",         "group": "curve_slopes",  "formula": "ez_10y - ez_3m",          "cols": ("ez_10y",    "ez_3m")},
        {"name": "us_ez_reer_spread","group": "reer",          "formula": "us_reer - ez_reer",        "cols": ("us_reer",   "ez_reer")},
    ],
    "AUDUSD": [
        {"name": "au_us_policy_spread","group": "rate_spreads","formula": "au_policy - us_policy",   "cols": ("au_policy", "us_policy")},
        {"name": "au_us_3m_spread",  "group": "rate_spreads",  "formula": "au_3m - us_3m",           "cols": ("au_3m",     "us_3m")},
        {"name": "au_us_10y_spread", "group": "rate_spreads",  "formula": "au_10y - us_10y",         "cols": ("au_10y",    "us_10y")},
        {"name": "us_curve",         "group": "curve_slopes",  "formula": "us_10y - us_3m",          "cols": ("us_10y",    "us_3m")},
        {"name": "au_curve",         "group": "curve_slopes",  "formula": "au_10y - au_3m",          "cols": ("au_10y",    "au_3m")},
        {"name": "au_us_reer_spread","group": "reer",          "formula": "au_reer - us_reer",        "cols": ("au_reer",   "us_reer")},
    ],
}

# 全 pair 共通因子
COMMON_FACTOR_SPEC: list[dict] = [
    {"name": "spot_return_1d",   "group": "momentum",      "formula": "spot.pct_change(1)"},
    {"name": "spot_return_5d",   "group": "momentum",      "formula": "spot.pct_change(5)"},
    {"name": "spot_return_20d",  "group": "momentum",      "formula": "spot.pct_change(20)"},
    {"name": "realized_vol_20d", "group": "volatility",    "formula": "spot.pct_change(1).rolling_std(20)"},
    {"name": "vix_change_1d",    "group": "risk",          "formula": "vix.diff(1)"},
    {"name": "vix_change_5d",    "group": "risk",          "formula": "vix.diff(5)"},
    {"name": "usd_broad_5d",     "group": "usd",           "formula": "usd_broad.pct_change(5)"},
    {"name": "usd_broad_20d",    "group": "usd",           "formula": "usd_broad.pct_change(20)"},
    {"name": "jpnassets_yoy",    "group": "balance_sheet", "formula": "boj_assets.pct_change(252)"},
]

# OHLC ベースのファクター定義
OHLC_FACTOR_SPEC: list[dict] = [
    {"name": "parkinson_vol_20d", "group": "ohlc_volatility", "formula": "sqrt(rolling_mean(ln(H/L)^2, 20) / (4*ln2))"},
    {"name": "daily_range_pct",   "group": "ohlc_range",      "formula": "(high - low) / open * 100"},
    {"name": "daily_range_ma20",  "group": "ohlc_range",      "formula": "daily_range_pct.rolling_mean(20)"},
    {"name": "overnight_gap",     "group": "ohlc_range",      "formula": "open_today / close_yesterday - 1"},
]


def compute_ohlc_factors(
    pair: str,
    start: Optional[date] = None,
    end: Optional[date] = None,
) -> pl.DataFrame:
    """
    CSV OHLC データから Parkinson vol / daily range / overnight gap を計算する。

    Returns:
        obs_date + 4 ファクター列の DataFrame。OHLC データがなければ空 DataFrame。
    """
    from app.services.csv_loader_service import get_daily_ohlc

    ohlc = get_daily_ohlc(pair, start, end)
    if ohlc.is_empty():
        logger.info("ohlc_factors_no_data", pair=pair)
        return pl.DataFrame()

    # daily_range_pct = (high - low) / open * 100
    ohlc = ohlc.with_columns(
        ((pl.col("high") - pl.col("low")) / pl.col("open") * 100).alias("daily_range_pct")
    )

    # daily_range_ma20 = daily_range_pct の 20 日移動平均
    ohlc = ohlc.with_columns(
        pl.col("daily_range_pct").rolling_mean(window_size=20).alias("daily_range_ma20")
    )

    # overnight_gap = open_today / close_yesterday - 1
    ohlc = ohlc.with_columns(
        (pl.col("open") / pl.col("close").shift(1) - 1).alias("overnight_gap")
    )

    # parkinson_vol_20d = sqrt(rolling_mean(ln(H/L)^2, 20) / (4*ln2))
    ohlc = ohlc.with_columns(
        (pl.col("high") / pl.col("low")).log().pow(2).alias("_hl_log_sq")
    )
    ohlc = ohlc.with_columns(
        (pl.col("_hl_log_sq").rolling_mean(window_size=20) / (4 * math.log(2))).pow(0.5).alias("parkinson_vol_20d")
    )

    return ohlc.select(["obs_date", "parkinson_vol_20d", "daily_range_pct", "daily_range_ma20", "overnight_gap"])


def compute_factors(panel: pl.DataFrame, pair: str) -> pl.DataFrame:
    """
    panel DataFrame から因子を計算して返す。
    obs_date 列 + 各因子列の DataFrame。
    """
    df = panel.clone()
    pair_specs = FACTOR_SPEC.get(pair, [])

    # ── rate spread / curve slope ─────────────────────
    for spec in pair_specs:
        a, b = spec["cols"]
        if a in df.columns and b in df.columns:
            df = df.with_columns(
                (pl.col(a) - pl.col(b)).alias(spec["name"])
            )

    # ── momentum / vol (spot ベース) ──────────────────
    if "spot" in df.columns:
        df = df.with_columns([
            pl.col("spot").pct_change(1).alias("spot_return_1d"),
            pl.col("spot").pct_change(5).alias("spot_return_5d"),
            pl.col("spot").pct_change(20).alias("spot_return_20d"),
            pl.col("spot").pct_change(1).rolling_std(window_size=20).alias("realized_vol_20d"),
        ])

    # ── VIX change ────────────────────────────────────
    if "vix" in df.columns:
        df = df.with_columns([
            pl.col("vix").diff(1).alias("vix_change_1d"),
            pl.col("vix").diff(5).alias("vix_change_5d"),
        ])

    # ── USD broad change ──────────────────────────────
    if "usd_broad" in df.columns:
        df = df.with_columns([
            pl.col("usd_broad").pct_change(5).alias("usd_broad_5d"),
            pl.col("usd_broad").pct_change(20).alias("usd_broad_20d"),
        ])

    # ── BoJ assets YoY (252 営業日変化率) ─────────────
    if "boj_assets" in df.columns:
        df = df.with_columns(
            pl.col("boj_assets").pct_change(252).alias("jpnassets_yoy")
        )

    factor_cols = (
        [s["name"] for s in pair_specs if s["name"] in df.columns]
        + [s["name"] for s in COMMON_FACTOR_SPEC if s["name"] in df.columns]
    )
    select_cols = ["obs_date"] + factor_cols
    return df.select([c for c in select_cols if c in df.columns])


def save_factors(factors: pl.DataFrame, pair: str) -> int:
    """fact_derived_factors + Parquet に保存"""
    if factors.is_empty():
        return 0

    conn = get_connection()
    now = datetime.utcnow()
    rows = []
    factor_cols = [c for c in factors.columns if c != "obs_date"]

    for row in factors.to_dicts():
        obs_date = row["obs_date"]
        for fname in factor_cols:
            val = row.get(fname)
            rows.append({
                "factor_id": f"{pair}_{fname}",
                "obs_date": obs_date,
                "pair": pair,
                "factor_group": _factor_group(fname, pair),
                "factor_name": fname,
                "value": val,
                "input_series_ids": fname,
                "formula": fname,
                "created_at": now,
            })

    if not rows:
        return 0

    df_insert = pl.DataFrame(rows)
    try:
        conn.register("_factor_df", df_insert.to_arrow())
        conn.execute("""
            INSERT OR REPLACE INTO fact_derived_factors
            SELECT factor_id, obs_date, pair, factor_group, factor_name,
                   value, input_series_ids, formula, created_at
            FROM _factor_df
        """)
        conn.unregister("_factor_df")
    except Exception as e:
        raise StorageError(f"factors 保存失敗: {e}") from e

    write_derived(factors, "factors", pair, f"factors_{pair}")
    logger.info("factors_saved", pair=pair, rows=len(factors))
    return len(factors)


def load_factors(pair: str, start: Optional[date] = None, end: Optional[date] = None) -> pl.DataFrame:
    """fact_derived_factors から pivot して wide 形式で返す"""
    conn = get_connection()
    cond = ["pair = ?"]
    params: list = [pair]
    if start:
        cond.append("obs_date >= ?")
        params.append(start)
    if end:
        cond.append("obs_date <= ?")
        params.append(end)

    sql = f"""
        SELECT obs_date, factor_name, value
        FROM fact_derived_factors
        WHERE {' AND '.join(cond)}
        ORDER BY obs_date
    """
    rows = conn.execute(sql, params).fetchall()
    if not rows:
        return pl.DataFrame()

    long_df = pl.DataFrame({
        "obs_date": [r[0] for r in rows],
        "factor_name": [r[1] for r in rows],
        "value": [r[2] for r in rows],
    })
    return long_df.pivot(index="obs_date", on="factor_name", values="value")


def _factor_group(name: str, pair: str) -> str:
    all_specs = FACTOR_SPEC.get(pair, []) + COMMON_FACTOR_SPEC + OHLC_FACTOR_SPEC
    for s in all_specs:
        if s["name"] == name:
            return s["group"]
    return "misc"
