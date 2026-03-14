"""
Cross Asset リポジトリ
- md.dim_market_instrument
- fact.cross_asset_feature_daily
- mart.fx_cross_asset_daily_panel
"""

from datetime import date, datetime
from typing import Optional

import polars as pl

from app.core.exceptions import StorageError
from app.core.logging import get_logger
from app.storage.duckdb import get_connection

logger = get_logger(__name__)


# ── instrument マスタ ────────────────────────────────────────────

def upsert_instrument(instrument_id: str, metadata: dict) -> None:
    """md.dim_market_instrument を upsert する"""
    conn = get_connection()
    now = datetime.utcnow()
    conn.execute(
        """
        INSERT INTO md.dim_market_instrument (
            instrument_id, source_system, source_vendor,
            vendor_symbol, canonical_symbol, instrument_name,
            asset_class, base_ccy, quote_ccy,
            timeframe_native, timezone_native,
            is_active, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'UTC', TRUE, ?, ?)
        ON CONFLICT (instrument_id) DO UPDATE SET
            updated_at = excluded.updated_at,
            is_active  = TRUE
        """,
        [
            instrument_id,
            metadata.get("source_system", "local_csv"),
            metadata.get("source_vendor", "manual_upload"),
            metadata.get("vendor_symbol", instrument_id),
            metadata.get("canonical_symbol", instrument_id),
            metadata.get("instrument_name"),
            metadata.get("asset_class", "equity_index"),
            metadata.get("base_ccy"),
            metadata.get("quote_ccy"),
            metadata.get("timeframe_native", "240"),
            now, now,
        ],
    )


# ── 特徴量 (long-form) ───────────────────────────────────────────

def upsert_features(
    feature_scope: str,
    scope_id: str,
    min_date: date,
    max_date: date,
    df: pl.DataFrame,
) -> int:
    """
    fact.cross_asset_feature_daily を対象期間だけ delete + insert する。
    df 列: feature_scope, scope_id, obs_date, feature_group,
           feature_name, feature_horizon, feature_value, feature_value_text,
           source_instrument_id, source_table, build_id, built_at
    """
    conn = get_connection()
    try:
        conn.execute(
            """
            DELETE FROM fact.cross_asset_feature_daily
            WHERE feature_scope = ? AND scope_id = ?
              AND obs_date BETWEEN ? AND ?
            """,
            [feature_scope, scope_id, min_date, max_date],
        )
        conn.register("_feat_df", df.to_arrow())
        conn.execute("""
            INSERT INTO fact.cross_asset_feature_daily
            SELECT feature_scope, scope_id, obs_date, feature_group,
                   feature_name, feature_horizon, feature_value, feature_value_text,
                   source_instrument_id, source_table, build_id, built_at
            FROM _feat_df
        """)
        conn.unregister("_feat_df")
    except Exception as e:
        raise StorageError(f"特徴量 upsert 失敗: {e}") from e

    logger.info("features_upserted", scope=feature_scope, scope_id=scope_id, rows=len(df))
    return len(df)


def get_features(
    feature_scope: str,
    scope_id: str,
    feature_names: Optional[list[str]] = None,
    start: Optional[date] = None,
    end: Optional[date] = None,
) -> list[dict]:
    """fact.cross_asset_feature_daily から long-form で取得する"""
    conn = get_connection()
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
        SELECT obs_date, feature_group, feature_name, feature_horizon,
               feature_value, feature_value_text, build_id
        FROM fact.cross_asset_feature_daily
        WHERE {' AND '.join(conditions)}
        ORDER BY obs_date, feature_name
        """,
        params,
    ).fetchall()

    cols = ["obs_date", "feature_group", "feature_name", "feature_horizon",
            "feature_value", "feature_value_text", "build_id"]
    result = []
    for row in rows:
        rec = dict(zip(cols, row))
        rec["obs_date"] = str(rec["obs_date"])
        result.append(rec)
    return result


def get_features_pivot(
    feature_scope: str,
    scope_id: str,
    feature_names: Optional[list[str]] = None,
    start: Optional[date] = None,
    end: Optional[date] = None,
) -> list[dict]:
    """long-form 取得後に wide-form へ pivot して返す"""
    rows = get_features(feature_scope, scope_id, feature_names, start, end)
    if not rows:
        return []

    # obs_date ごとに feature_name → feature_value を集約
    pivot: dict[str, dict] = {}
    for r in rows:
        d = r["obs_date"]
        if d not in pivot:
            pivot[d] = {"obs_date": d}
        key = r["feature_name"]
        if r["feature_horizon"]:
            key = f"{r['feature_name']}_{r['feature_horizon']}" if r["feature_horizon"] not in r["feature_name"] else r["feature_name"]
        pivot[d][key] = r["feature_value"] if r["feature_value"] is not None else r["feature_value_text"]

    return sorted(pivot.values(), key=lambda x: x["obs_date"])


# ── パネル (wide-form) ────────────────────────────────────────────

def upsert_panel(
    pair: str,
    min_date: date,
    max_date: date,
    df: pl.DataFrame,
) -> int:
    """mart.fx_cross_asset_daily_panel を delete + insert する"""
    conn = get_connection()
    try:
        conn.execute(
            """
            DELETE FROM mart.fx_cross_asset_daily_panel
            WHERE pair = ? AND obs_date BETWEEN ? AND ?
            """,
            [pair, min_date, max_date],
        )
        conn.register("_panel_df", df.to_arrow())
        conn.execute("""
            INSERT INTO mart.fx_cross_asset_daily_panel
            SELECT pair, obs_date,
                   pair_close, pair_ret_1d,
                   usatech_close, usatech_ret_1d,
                   usatech_mom_5d, usatech_mom_20d,
                   usatech_rv_5d, usatech_rv_20d,
                   usatech_drawdown_20d, usatech_range_pct_1d,
                   vix_close, usd_broad_close,
                   rate_spread_3m, yield_spread_10y,
                   event_risk_flag, regime_label,
                   build_id, built_at
            FROM _panel_df
        """)
        conn.unregister("_panel_df")
    except Exception as e:
        raise StorageError(f"パネル upsert 失敗: {e}") from e

    logger.info("panel_upserted", pair=pair, rows=len(df))
    return len(df)


def get_panel(
    pair: str,
    start: Optional[date] = None,
    end: Optional[date] = None,
) -> list[dict]:
    """mart.fx_cross_asset_daily_panel から取得する"""
    conn = get_connection()
    conditions = ["pair = ?"]
    params: list = [pair]
    if start:
        conditions.append("obs_date >= ?")
        params.append(start)
    if end:
        conditions.append("obs_date <= ?")
        params.append(end)

    rows = conn.execute(
        f"""
        SELECT pair, obs_date,
               pair_close, pair_ret_1d,
               usatech_close, usatech_ret_1d,
               usatech_mom_5d, usatech_mom_20d,
               usatech_rv_5d, usatech_rv_20d,
               usatech_drawdown_20d, usatech_range_pct_1d,
               vix_close, usd_broad_close,
               rate_spread_3m, yield_spread_10y,
               event_risk_flag, regime_label
        FROM mart.fx_cross_asset_daily_panel
        WHERE {' AND '.join(conditions)}
        ORDER BY obs_date
        """,
        params,
    ).fetchall()

    cols = [
        "pair", "obs_date",
        "pair_close", "pair_ret_1d",
        "usatech_close", "usatech_ret_1d",
        "usatech_mom_5d", "usatech_mom_20d",
        "usatech_rv_5d", "usatech_rv_20d",
        "usatech_drawdown_20d", "usatech_range_pct_1d",
        "vix_close", "usd_broad_close",
        "rate_spread_3m", "yield_spread_10y",
        "event_risk_flag", "regime_label",
    ]
    result = []
    for row in rows:
        rec = dict(zip(cols, row))
        rec["obs_date"] = str(rec["obs_date"])
        result.append(rec)
    return result
