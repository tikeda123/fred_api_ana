"""
RegimeService - rule-based regime tagging
各日付に対して複数のレジームラベルを付与する。

レジーム:
  risk_state       : risk_on / risk_off
  usd_state        : usd_strong / usd_neutral / usd_weak
  carry_state      : carry_positive / carry_negative / carry_neutral
  curve_state_us   : normal / flat / inverted
  event_pressure   : high / low  (Phase4 でリリースカレンダーと接続後に実装)
"""

from datetime import date
from typing import Optional

import polars as pl

from app.core.logging import get_logger
from app.storage.duckdb import get_connection
from app.storage.parquet_io import write_derived

logger = get_logger(__name__)

# ── VIX しきい値 ─────────────────────────────────────────────
VIX_PERCENTILE_WINDOW = 252   # 252 営業日 ≒ 1年

# ── USD broad zscore しきい値 ─────────────────────────────────
USD_ZSCORE_WINDOW = 252
USD_STRONG_THRESHOLD = 1.0
USD_WEAK_THRESHOLD = -1.0


def tag_regimes(
    panel: pl.DataFrame,
    factors: pl.DataFrame,
    pair: str,
) -> pl.DataFrame:
    """
    panel + factors を受け取り、regime タグ列を付与した DataFrame を返す。
    obs_date, risk_state, usd_state, carry_state, curve_state_us
    """
    df = panel.join(factors, on="obs_date", how="left")

    # ── risk_state: VIX > rolling 252d p75 → risk_off ────────
    if "vix" in df.columns:
        df = df.with_columns(
            pl.col("vix")
            .rolling_quantile(quantile=0.75, window_size=VIX_PERCENTILE_WINDOW, min_samples=20)
            .alias("vix_p75")
        )
        df = df.with_columns(
            pl.when(pl.col("vix") > pl.col("vix_p75"))
            .then(pl.lit("risk_off"))
            .otherwise(pl.lit("risk_on"))
            .alias("risk_state")
        )
    else:
        df = df.with_columns(pl.lit("unknown").alias("risk_state"))

    # ── usd_state: usd_broad zscore ──────────────────────────
    if "usd_broad" in df.columns:
        df = df.with_columns([
            pl.col("usd_broad")
            .rolling_mean(window_size=USD_ZSCORE_WINDOW, min_samples=20)
            .alias("usd_roll_mean"),
            pl.col("usd_broad")
            .rolling_std(window_size=USD_ZSCORE_WINDOW, min_samples=20)
            .alias("usd_roll_std"),
        ])
        df = df.with_columns(
            ((pl.col("usd_broad") - pl.col("usd_roll_mean")) / pl.col("usd_roll_std"))
            .alias("usd_zscore")
        )
        df = df.with_columns(
            pl.when(pl.col("usd_zscore") > USD_STRONG_THRESHOLD)
            .then(pl.lit("usd_strong"))
            .when(pl.col("usd_zscore") < USD_WEAK_THRESHOLD)
            .then(pl.lit("usd_weak"))
            .otherwise(pl.lit("usd_neutral"))
            .alias("usd_state")
        )
    else:
        df = df.with_columns(pl.lit("unknown").alias("usd_state"))

    # ── carry_state: pair 別の short rate spread ──────────────
    spread_col = _carry_col(pair)
    if spread_col and spread_col in df.columns:
        df = df.with_columns(
            pl.when(pl.col(spread_col) > 0.1)
            .then(pl.lit("carry_positive"))
            .when(pl.col(spread_col) < -0.1)
            .then(pl.lit("carry_negative"))
            .otherwise(pl.lit("carry_neutral"))
            .alias("carry_state")
        )
    else:
        df = df.with_columns(pl.lit("unknown").alias("carry_state"))

    # ── curve_state_us: us_curve (10y - 3m) ──────────────────
    if "us_curve" in df.columns:
        df = df.with_columns(
            pl.when(pl.col("us_curve") > 0.5)
            .then(pl.lit("normal"))
            .when(pl.col("us_curve") < 0)
            .then(pl.lit("inverted"))
            .otherwise(pl.lit("flat"))
            .alias("curve_state_us")
        )
    else:
        df = df.with_columns(pl.lit("unknown").alias("curve_state_us"))

    # 必要な列だけ返す
    regime_cols = ["obs_date", "risk_state", "usd_state", "carry_state", "curve_state_us"]
    return df.select([c for c in regime_cols if c in df.columns])


def save_regimes(regimes: pl.DataFrame, pair: str) -> int:
    write_derived(regimes, "regimes", pair, f"regimes_{pair}")
    logger.info("regimes_saved", pair=pair, rows=len(regimes))
    return len(regimes)


def compute_regime_stats(
    panel: pl.DataFrame,
    regimes: pl.DataFrame,
    pair: str,
) -> dict:
    """
    regime 別の平均リターン・出現頻度を計算。
    """
    if "spot" not in panel.columns:
        return {}

    df = panel.select(["obs_date", "spot"]).join(regimes, on="obs_date", how="left")
    df = df.with_columns(pl.col("spot").pct_change(1).alias("spot_return_1d"))

    stats: dict = {}

    for state_col in ["risk_state", "carry_state", "curve_state_us", "usd_state"]:
        if state_col not in df.columns:
            continue
        grp = (
            df.group_by(state_col)
            .agg([
                pl.col("spot_return_1d").mean().alias("mean_return"),
                pl.col("spot_return_1d").std().alias("std_return"),
                pl.len().alias("count"),
            ])
            .sort(state_col)
        )
        stats[state_col] = grp.to_dicts()

    return stats


def _carry_col(pair: str) -> Optional[str]:
    return {
        "USDJPY": "us_jp_3m_spread",
        "EURUSD": "us_ez_3m_spread",
        "AUDUSD": "au_us_3m_spread",
    }.get(pair)
