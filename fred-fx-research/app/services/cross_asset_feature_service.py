"""
Cross Asset Feature Service
- instrument-level 特徴量: USATECH 自身のモメンタム / ボラ / ドローダウン
- pair-level 特徴量: FX × USATECH の相関 / beta / lag-corr / risk_filter_flag
"""

import math
import uuid
from datetime import date, datetime
from typing import Optional

import polars as pl

from app.core.logging import get_logger
from app.storage.duckdb import get_connection
from app.storage.repositories import cross_asset_repo

logger = get_logger(__name__)

# ── 特徴量定義 ─────────────────────────────────────────────────────

INSTRUMENT_FEATURES = [
    {"name": "close",           "group": "price",      "horizon": "spot"},
    {"name": "ret_1d",          "group": "return",     "horizon": "1d"},
    {"name": "mom_5d",          "group": "momentum",   "horizon": "5d"},
    {"name": "mom_20d",         "group": "momentum",   "horizon": "20d"},
    {"name": "rv_5d",           "group": "volatility", "horizon": "5d"},
    {"name": "rv_20d",          "group": "volatility", "horizon": "20d"},
    {"name": "drawdown_20d",    "group": "drawdown",   "horizon": "20d"},
    {"name": "range_pct_1d",    "group": "range",      "horizon": "1d"},
    {"name": "weekend_gap_pct", "group": "range",      "horizon": "spot"},
]

PAIR_FEATURES = [
    {"name": "corr_usatech_pair_20d",       "group": "correlation",     "horizon": "20d"},
    {"name": "beta_usatech_pair_60d",       "group": "correlation",     "horizon": "60d"},
    {"name": "lagcorr_usatech_pair_l1_20d", "group": "lag_correlation", "horizon": "20d"},
    {"name": "lagcorr_usatech_pair_l2_20d", "group": "lag_correlation", "horizon": "20d"},
    {"name": "risk_filter_flag",            "group": "filter",          "horizon": "spot"},
]

# FX pair → FRED series_id
PAIR_TO_SERIES = {
    "USDJPY": "DEXJPUS",
    "EURUSD": "DEXUSEU",
    "AUDUSD": "DEXUSAL",
}


# ── instrument-level ────────────────────────────────────────────────

def rebuild_instrument_features(
    instrument_id: str,
    start: Optional[date] = None,
    end: Optional[date] = None,
) -> int:
    """
    fact.market_bars_daily から instrument-level 特徴量を計算し
    fact.cross_asset_feature_daily に long-form で保存する。
    """
    df = _load_daily_bars(instrument_id, start, end)
    if df.is_empty():
        logger.info("no_daily_bars", instrument_id=instrument_id)
        return 0

    df = _compute_instrument_features(df, instrument_id)

    build_id = _new_build_id()
    now = datetime.utcnow()
    long_rows = _wide_to_long(df, instrument_id, "instrument", instrument_id, build_id, now)

    if not long_rows:
        return 0

    long_df = pl.DataFrame(long_rows, schema_overrides={
        "feature_value": pl.Float64,
        "feature_value_text": pl.Utf8,
    })
    min_date = long_df["obs_date"].min()
    max_date = long_df["obs_date"].max()

    n = cross_asset_repo.upsert_features("instrument", instrument_id, min_date, max_date, long_df)
    logger.info("instrument_features_rebuilt", instrument_id=instrument_id, rows=n)
    return n


def _compute_instrument_features(df: pl.DataFrame, instrument_id: str) -> pl.DataFrame:
    """wide-form で特徴量列を追加して返す"""
    df = df.sort("obs_date")

    ret_1d = pl.col("close") / pl.col("close").shift(1) - 1

    df = df.with_columns([
        ret_1d.alias("ret_1d"),
        (pl.col("close") / pl.col("close").shift(5) - 1).alias("mom_5d"),
        (pl.col("close") / pl.col("close").shift(20) - 1).alias("mom_20d"),
        # realized vol = rolling std of ret_1d * sqrt(252)
        ret_1d.rolling_std(5).alias("_rv_5d_raw"),
        ret_1d.rolling_std(20).alias("_rv_20d_raw"),
        # drawdown = close / rolling_max(20) - 1
        (pl.col("close") / pl.col("close").rolling_max(20) - 1).alias("drawdown_20d"),
        # range_pct_1d from daily bars
        ((pl.col("high") - pl.col("low")) / pl.col("close")).alias("range_pct_1d"),
        # weekend_gap_pct = gap_from_prev_close_pct
        pl.col("gap_from_prev_close_pct").alias("weekend_gap_pct"),
    ])

    # annualize vol
    sqrt_252 = math.sqrt(252)
    df = df.with_columns([
        (pl.col("_rv_5d_raw")  * sqrt_252).alias("rv_5d"),
        (pl.col("_rv_20d_raw") * sqrt_252).alias("rv_20d"),
    ]).drop(["_rv_5d_raw", "_rv_20d_raw"])

    return df


def _wide_to_long(
    df: pl.DataFrame,
    source_instrument_id: str,
    feature_scope: str,
    scope_id: str,
    build_id: str,
    built_at: datetime,
) -> list[dict]:
    """wide DataFrame を long-form レコードのリストへ変換する"""
    rows = []
    feat_map = {f["name"]: f for f in INSTRUMENT_FEATURES}

    for rec in df.iter_rows(named=True):
        obs_date = rec["obs_date"]
        for feat in INSTRUMENT_FEATURES:
            val = rec.get(feat["name"])
            if val is None or (isinstance(val, float) and math.isnan(val)):
                val = None
            rows.append({
                "feature_scope":        feature_scope,
                "scope_id":             scope_id,
                "obs_date":             obs_date,
                "feature_group":        feat["group"],
                "feature_name":         feat["name"],
                "feature_horizon":      feat["horizon"],
                "feature_value":        val,
                "feature_value_text":   None,
                "source_instrument_id": source_instrument_id,
                "source_table":         "fact.market_bars_daily",
                "build_id":             build_id,
                "built_at":             built_at,
            })
    return rows


# ── pair-level ──────────────────────────────────────────────────────

def rebuild_pair_features(
    pairs: list[str],
    source_instrument_id: str,
    start: Optional[date] = None,
    end: Optional[date] = None,
) -> int:
    """
    pair × USATECH の相関 / beta / lag-corr / risk_filter_flag を計算し保存する。
    """
    # USATECH daily bars
    usatech_df = _load_daily_bars(source_instrument_id, start, end)
    if usatech_df.is_empty():
        logger.info("no_usatech_bars_for_pair_features")
        return 0

    usatech_df = usatech_df.sort("obs_date").with_columns(
        (pl.col("close") / pl.col("close").shift(1) - 1).alias("usatech_ret")
    )

    # VIX と USD Broad は risk_filter_flag 計算に使う
    vix_df = _load_fred_series("VIXCLS", start, end)
    usd_df = _load_fred_series("DTWEXBGS", start, end)

    total = 0
    for pair in pairs:
        n = _rebuild_one_pair(pair, source_instrument_id, usatech_df, vix_df, usd_df, start, end)
        total += n

    return total


def _rebuild_one_pair(
    pair: str,
    source_instrument_id: str,
    usatech_df: pl.DataFrame,
    vix_df: pl.DataFrame,
    usd_df: pl.DataFrame,
    start: Optional[date],
    end: Optional[date],
) -> int:
    series_id = PAIR_TO_SERIES.get(pair)
    if not series_id:
        logger.warning("unknown_pair", pair=pair)
        return 0

    fx_df = _load_fred_series(series_id, start, end)
    if fx_df.is_empty():
        logger.info("no_fx_data", pair=pair, series_id=series_id)
        return 0

    # FX リターン計算
    fx_df = fx_df.sort("obs_date").with_columns(
        (pl.col("value") / pl.col("value").shift(1) - 1).alias("fx_ret")
    )

    # USATECH と FX を結合
    joined = usatech_df.select(["obs_date", "usatech_ret"]).join(
        fx_df.select(["obs_date", "fx_ret", "value"]),
        on="obs_date",
        how="inner",
    ).sort("obs_date")

    if len(joined) < 21:
        logger.info("insufficient_data_for_pair_features", pair=pair, rows=len(joined))
        return 0

    # VIX / USD Broad を結合（risk_filter_flag 用）
    if not vix_df.is_empty():
        joined = joined.join(
            vix_df.select(["obs_date", pl.col("value").alias("vix")]),
            on="obs_date",
            how="left",
        )
    else:
        joined = joined.with_columns(pl.lit(None).cast(pl.Float64).alias("vix"))

    if not usd_df.is_empty():
        joined = joined.join(
            usd_df.select(["obs_date", pl.col("value").alias("usd_broad")]),
            on="obs_date",
            how="left",
        )
    else:
        joined = joined.with_columns(pl.lit(None).cast(pl.Float64).alias("usd_broad"))

    # 特徴量計算
    joined = _compute_pair_features(joined, source_instrument_id)

    build_id = _new_build_id()
    now = datetime.utcnow()
    long_rows = _pair_wide_to_long(joined, pair, source_instrument_id, build_id, now)

    if not long_rows:
        return 0

    long_df = pl.DataFrame(long_rows, schema_overrides={
        "feature_value": pl.Float64,
        "feature_value_text": pl.Utf8,
    })
    min_date = long_df["obs_date"].min()
    max_date = long_df["obs_date"].max()

    n = cross_asset_repo.upsert_features("pair", pair, min_date, max_date, long_df)
    logger.info("pair_features_rebuilt", pair=pair, rows=n)
    return n


def _compute_pair_features(df: pl.DataFrame, source_instrument_id: str) -> pl.DataFrame:
    """pair-level 特徴量を計算して列を追加する"""
    # rolling_map_batches を使って rolling corr / cov を計算
    # Polars の rolling_corr は 2 列間の相関
    w20 = 20
    w60 = 60

    df = df.with_columns([
        pl.rolling_corr(pl.col("usatech_ret"), pl.col("fx_ret"), window_size=w20)
          .alias("corr_usatech_pair_20d"),
        pl.rolling_cov(pl.col("usatech_ret"), pl.col("fx_ret"), window_size=w60)
          .alias("_cov_60"),
        pl.col("usatech_ret").rolling_var(window_size=w60).alias("_var_usatech_60"),
        # lag-1 corr
        pl.rolling_corr(pl.col("usatech_ret").shift(1), pl.col("fx_ret"), window_size=w20)
          .alias("lagcorr_usatech_pair_l1_20d"),
        # lag-2 corr
        pl.rolling_corr(pl.col("usatech_ret").shift(2), pl.col("fx_ret"), window_size=w20)
          .alias("lagcorr_usatech_pair_l2_20d"),
        # USATECH drawdown_20d (pair 計算用)
        (pl.col("usatech_ret").cum_prod() / pl.col("usatech_ret").cum_prod().rolling_max(w20) - 1)
          .alias("_usatech_dd20"),
        # USATECH rv_20d
        pl.col("usatech_ret").rolling_std(w20).alias("_usatech_rv20"),
    ])

    # beta = cov / var
    df = df.with_columns(
        (pl.col("_cov_60") / pl.col("_var_usatech_60")).alias("beta_usatech_pair_60d")
    )

    # risk_filter_flag
    df = _apply_risk_filter(df)

    return df.drop(["_cov_60", "_var_usatech_60", "_usatech_dd20", "_usatech_rv20"])


def _apply_risk_filter(df: pl.DataFrame) -> pl.DataFrame:
    """ルールベースの risk_filter_flag を算出する"""
    # USATECH drawdown_20d
    dd_cond = pl.col("_usatech_dd20") <= -0.08

    # USATECH rv_20d >= p80 (全期間の分位数)
    rv_vals = df["_usatech_rv20"].drop_nulls()
    rv_p80 = rv_vals.quantile(0.80) if len(rv_vals) > 0 else float("inf")
    rv_cond = pl.col("_usatech_rv20") > rv_p80

    # VIX > p80
    if "vix" in df.columns:
        vix_vals = df["vix"].drop_nulls()
        vix_p80 = vix_vals.quantile(0.80) if len(vix_vals) > 0 else float("inf")
        vix_cond = pl.col("vix") > vix_p80
    else:
        vix_cond = pl.lit(False)

    # USD Broad z-score >= 1.0
    if "usd_broad" in df.columns:
        usd_vals = df["usd_broad"].drop_nulls()
        usd_mean = usd_vals.mean() if len(usd_vals) > 0 else 0.0
        usd_std  = usd_vals.std()  if len(usd_vals) > 0 else 1.0
        usd_z_cond = ((pl.col("usd_broad") - usd_mean) / (usd_std or 1.0)) >= 1.0
    else:
        usd_z_cond = pl.lit(False)

    # 2 条件以上 → avoid
    hit = (
        dd_cond.cast(pl.Int32) +
        rv_cond.cast(pl.Int32) +
        vix_cond.cast(pl.Int32) +
        usd_z_cond.cast(pl.Int32)
    )
    df = df.with_columns(
        pl.when(hit >= 2).then(pl.lit("avoid")).otherwise(pl.lit("ok"))
        .alias("risk_filter_flag")
    )
    return df


def _pair_wide_to_long(
    df: pl.DataFrame,
    pair: str,
    source_instrument_id: str,
    build_id: str,
    built_at: datetime,
) -> list[dict]:
    """pair wide DataFrame を long-form レコードへ変換する"""
    rows = []
    for rec in df.iter_rows(named=True):
        obs_date = rec["obs_date"]
        for feat in PAIR_FEATURES:
            name = feat["name"]
            val = rec.get(name)
            is_text = name == "risk_filter_flag"

            if is_text:
                fval = None
                ftext = val
            else:
                fval = val if (val is not None and not (isinstance(val, float) and math.isnan(val))) else None
                ftext = None

            rows.append({
                "feature_scope":        "pair",
                "scope_id":             pair,
                "obs_date":             obs_date,
                "feature_group":        feat["group"],
                "feature_name":         name,
                "feature_horizon":      feat["horizon"],
                "feature_value":        fval,
                "feature_value_text":   ftext,
                "source_instrument_id": source_instrument_id,
                "source_table":         "fact.market_bars_daily",
                "build_id":             build_id,
                "built_at":             built_at,
            })
    return rows


# ── 内部ヘルパー ────────────────────────────────────────────────────

def _load_daily_bars(
    instrument_id: str,
    start: Optional[date],
    end: Optional[date],
) -> pl.DataFrame:
    """fact.market_bars_daily からデイリーバーを Polars DF で返す"""
    conn = get_connection()
    conditions = ["instrument_id = ?"]
    params: list = [instrument_id]
    if start:
        conditions.append("obs_date >= ?")
        params.append(start)
    if end:
        conditions.append("obs_date <= ?")
        params.append(end)

    rows = conn.execute(
        f"""
        SELECT obs_date, open, high, low, close, volume,
               gap_from_prev_close_pct
        FROM fact.market_bars_daily
        WHERE {' AND '.join(conditions)}
        ORDER BY obs_date
        """,
        params,
    ).fetchall()

    if not rows:
        return pl.DataFrame()

    cols = ["obs_date", "open", "high", "low", "close", "volume", "gap_from_prev_close_pct"]
    return pl.DataFrame([dict(zip(cols, r)) for r in rows])


def _load_fred_series(
    series_id: str,
    start: Optional[date],
    end: Optional[date],
) -> pl.DataFrame:
    """fact_market_series_normalized から series の最新 realtime 値を返す"""
    conn = get_connection()
    conditions = ["series_id = ?", "value IS NOT NULL"]
    params: list = [series_id]
    if start:
        conditions.append("obs_date >= ?")
        params.append(start)
    if end:
        conditions.append("obs_date <= ?")
        params.append(end)

    rows = conn.execute(
        f"""
        SELECT obs_date, value
        FROM fact_market_series_normalized
        WHERE {' AND '.join(conditions)}
        ORDER BY obs_date, as_of_realtime_start DESC
        """,
        params,
    ).fetchall()

    if not rows:
        return pl.DataFrame()

    # obs_date 重複は最新 realtime を優先（既に ORDER BY で先頭に来ている）
    seen: set[str] = set()
    deduped = []
    for r in rows:
        k = str(r[0])
        if k not in seen:
            seen.add(k)
            deduped.append({"obs_date": r[0], "value": r[1]})

    return pl.DataFrame(deduped)


def _new_build_id() -> str:
    return f"build_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
