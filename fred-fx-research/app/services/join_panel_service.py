"""
Join Panel Service
- mart.fx_cross_asset_daily_panel の構築
- FX spot + USATECH features + FRED 系列 (VIX / USD broad / spreads) を結合
"""

import uuid
from datetime import date, datetime
from typing import Optional

import polars as pl

from app.core.logging import get_logger
from app.storage.duckdb import get_connection
from app.storage.repositories import cross_asset_repo

logger = get_logger(__name__)

PAIR_TO_SERIES = {
    "USDJPY": "DEXJPUS",
    "EURUSD": "DEXUSEU",
    "AUDUSD": "DEXUSAL",
}

# FRED の対応 series_id
VIX_SERIES     = "VIXCLS"
USD_BROAD_SERIES = "DTWEXBGS"


def build_panel(
    pair: str,
    start: Optional[date] = None,
    end: Optional[date] = None,
) -> int:
    """
    mart.fx_cross_asset_daily_panel を構築して保存する。

    Returns:
        保存した行数
    """
    conn = get_connection()
    series_id = PAIR_TO_SERIES.get(pair)
    if not series_id:
        logger.warning("unknown_pair_for_panel", pair=pair)
        return 0

    # ── 1. FX spot ────────────────────────────────────────────────
    fx_df = _load_fred_normalized(conn, series_id, start, end)
    if fx_df.is_empty():
        logger.info("no_fx_spot_for_panel", pair=pair)
        return 0

    # FX リターン
    fx_df = fx_df.sort("obs_date").with_columns(
        (pl.col("pair_close") / pl.col("pair_close").shift(1) - 1).alias("pair_ret_1d")
    )

    # ── 2. USATECH 特徴量 (pivot) ─────────────────────────────────
    usatech_names = [
        "close", "ret_1d", "mom_5d", "mom_20d",
        "rv_5d", "rv_20d", "drawdown_20d", "range_pct_1d",
    ]
    usatech_df = _load_feature_pivot(
        conn,
        feature_scope="instrument",
        scope_id="usatechidxusd_h4",
        feature_names=usatech_names,
        start=start,
        end=end,
        rename_prefix="usatech_",
    )

    # ── 3. VIX ───────────────────────────────────────────────────
    vix_df = _load_fred_normalized(conn, VIX_SERIES, start, end).rename({"pair_close": "vix_close"})

    # ── 4. USD Broad ─────────────────────────────────────────────
    usd_df = _load_fred_normalized(conn, USD_BROAD_SERIES, start, end).rename({"pair_close": "usd_broad_close"})

    # ── 5. rate_spread_3m / yield_spread_10y (fact_derived_factors) ──
    spread_df = _load_spreads(conn, pair, start, end)

    # ── 6. pair-level 特徴量 (risk_filter_flag / corr) ───────────
    pair_feat_df = _load_pair_feature_pivot(conn, pair, start, end)

    # ── 7. 結合 ──────────────────────────────────────────────────
    panel = fx_df
    for df, on_col in [
        (usatech_df, "obs_date"),
        (vix_df,     "obs_date"),
        (usd_df,     "obs_date"),
        (spread_df,  "obs_date"),
        (pair_feat_df, "obs_date"),
    ]:
        if not df.is_empty():
            panel = panel.join(df, on=on_col, how="left")

    # 不足列をNullで補完
    for col in [
        "vix_close", "usd_broad_close",
        "rate_spread_3m", "yield_spread_10y",
        "event_risk_flag", "regime_label",
        "usatech_close", "usatech_ret_1d",
        "usatech_mom_5d", "usatech_mom_20d",
        "usatech_rv_5d", "usatech_rv_20d",
        "usatech_drawdown_20d", "usatech_range_pct_1d",
    ]:
        if col not in panel.columns:
            panel = panel.with_columns(pl.lit(None).cast(pl.Float64).alias(col))

    if "event_risk_flag" not in panel.columns:
        panel = panel.with_columns(pl.lit(None).cast(pl.Utf8).alias("event_risk_flag"))
    if "regime_label" not in panel.columns:
        panel = panel.with_columns(pl.lit(None).cast(pl.Utf8).alias("regime_label"))

    build_id = f"build_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    now = datetime.utcnow()
    panel = panel.with_columns([
        pl.lit(pair).alias("pair"),
        pl.lit(build_id).alias("build_id"),
        pl.lit(now).alias("built_at"),
    ])

    # 列を最終テーブル順に並べ替え
    final_cols = [
        "pair", "obs_date",
        "pair_close", "pair_ret_1d",
        "usatech_close", "usatech_ret_1d",
        "usatech_mom_5d", "usatech_mom_20d",
        "usatech_rv_5d", "usatech_rv_20d",
        "usatech_drawdown_20d", "usatech_range_pct_1d",
        "vix_close", "usd_broad_close",
        "rate_spread_3m", "yield_spread_10y",
        "event_risk_flag", "regime_label",
        "build_id", "built_at",
    ]
    existing = [c for c in final_cols if c in panel.columns]
    panel = panel.select(existing)

    # 不足している最終列を NULL で追加
    for col in final_cols:
        if col not in panel.columns:
            if col in ("event_risk_flag", "regime_label", "build_id", "pair"):
                panel = panel.with_columns(pl.lit(None).cast(pl.Utf8).alias(col))
            elif col == "built_at":
                panel = panel.with_columns(pl.lit(now).alias(col))
            else:
                panel = panel.with_columns(pl.lit(None).cast(pl.Float64).alias(col))
    panel = panel.select(final_cols)

    min_date = panel["obs_date"].min()
    max_date = panel["obs_date"].max()
    n = cross_asset_repo.upsert_panel(pair, min_date, max_date, panel)
    logger.info("panel_built", pair=pair, rows=n, build_id=build_id)
    return n


# ── 内部ヘルパー ────────────────────────────────────────────────────

def _load_fred_normalized(
    conn,
    series_id: str,
    start: Optional[date],
    end: Optional[date],
) -> pl.DataFrame:
    """fact_market_series_normalized から series の日次値を返す"""
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

    seen: set[str] = set()
    deduped = []
    for r in rows:
        k = str(r[0])
        if k not in seen:
            seen.add(k)
            deduped.append({"obs_date": r[0], "pair_close": r[1]})
    return pl.DataFrame(deduped)


def _load_feature_pivot(
    conn,
    feature_scope: str,
    scope_id: str,
    feature_names: list[str],
    start: Optional[date],
    end: Optional[date],
    rename_prefix: str = "",
) -> pl.DataFrame:
    """特徴量を obs_date × feature_name のピボットで返す"""
    conditions = ["feature_scope = ?", "scope_id = ?"]
    params: list = [feature_scope, scope_id]
    if feature_names:
        placeholders = ", ".join(["?"] * len(feature_names))
        conditions.append(f"feature_name IN ({placeholders})")
        params.extend(feature_names)
    if start:
        conditions.append("obs_date >= ?")
        params.append(start)
    if end:
        conditions.append("obs_date <= ?")
        params.append(end)

    rows = conn.execute(
        f"""
        SELECT obs_date, feature_name, feature_value
        FROM fact.cross_asset_feature_daily
        WHERE {' AND '.join(conditions)}
        ORDER BY obs_date, feature_name
        """,
        params,
    ).fetchall()

    if not rows:
        return pl.DataFrame()

    pivot: dict[str, dict] = {}
    for obs_date, feat_name, feat_val in rows:
        d = str(obs_date)
        if d not in pivot:
            pivot[d] = {"obs_date": obs_date}
        pivot[d][f"{rename_prefix}{feat_name}"] = feat_val

    return pl.DataFrame(list(pivot.values()))


def _load_pair_feature_pivot(
    conn,
    pair: str,
    start: Optional[date],
    end: Optional[date],
) -> pl.DataFrame:
    """pair-level 特徴量を pivot して返す（risk_filter_flag, corr 等）"""
    conditions = ["feature_scope = 'pair'", "scope_id = ?"]
    params: list = [pair]
    if start:
        conditions.append("obs_date >= ?")
        params.append(start)
    if end:
        conditions.append("obs_date <= ?")
        params.append(end)

    rows = conn.execute(
        f"""
        SELECT obs_date, feature_name, feature_value, feature_value_text
        FROM fact.cross_asset_feature_daily
        WHERE {' AND '.join(conditions)}
        ORDER BY obs_date, feature_name
        """,
        params,
    ).fetchall()

    if not rows:
        return pl.DataFrame()

    pivot: dict[str, dict] = {}
    for obs_date, feat_name, feat_val, feat_text in rows:
        d = str(obs_date)
        if d not in pivot:
            pivot[d] = {"obs_date": obs_date}
        pivot[d][feat_name] = feat_val if feat_val is not None else feat_text

    df = pl.DataFrame(list(pivot.values()))

    # risk_filter_flag を event_risk_flag / regime_label へマッピング
    if "risk_filter_flag" in df.columns:
        df = df.with_columns(
            pl.col("risk_filter_flag").alias("event_risk_flag")
        ).drop("risk_filter_flag")

    return df


def _load_spreads(
    conn,
    pair: str,
    start: Optional[date],
    end: Optional[date],
) -> pl.DataFrame:
    """fact_derived_factors から rate_spread_3m / yield_spread_10y を取得する"""
    conditions = ["pair = ?", "factor_name IN ('rate_spread_3m', 'yield_spread_10y')"]
    params: list = [pair]
    if start:
        conditions.append("obs_date >= ?")
        params.append(start)
    if end:
        conditions.append("obs_date <= ?")
        params.append(end)

    try:
        rows = conn.execute(
            f"""
            SELECT obs_date, factor_name, value
            FROM fact_derived_factors
            WHERE {' AND '.join(conditions)}
            ORDER BY obs_date
            """,
            params,
        ).fetchall()
    except Exception:
        return pl.DataFrame()

    if not rows:
        return pl.DataFrame()

    pivot: dict[str, dict] = {}
    for obs_date, factor_name, val in rows:
        d = str(obs_date)
        if d not in pivot:
            pivot[d] = {"obs_date": obs_date}
        pivot[d][factor_name] = val

    return pl.DataFrame(list(pivot.values()))
