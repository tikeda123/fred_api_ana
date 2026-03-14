"""
Panel 用クエリ
- normalized / derived を Polars DataFrame として返す
"""

from datetime import date
from typing import Optional

import polars as pl

from app.core.exceptions import StorageError
from app.core.logging import get_logger
from app.storage.duckdb import get_connection

logger = get_logger(__name__)

# pair → 取得すべき series_id の一覧
PAIR_SERIES_MAP: dict[str, dict[str, str]] = {
    "USDJPY": {
        "spot":       "DEXJPUS",
        "us_policy":  "EFFR",
        "jp_policy":  "IRSTCI01JPM156N",
        "us_3m":      "IR3TIB01USM156N",
        "jp_3m":      "IR3TIB01JPM156N",
        "us_10y":     "IRLTLT01USM156N",
        "jp_10y":     "IRLTLT01JPM156N",
        "vix":        "VIXCLS",
        "usd_broad":  "DTWEXBGS",
        "boj_assets": "JPNASSETS",
        "us_reer":    "RBUSBIS",
        "jp_reer":    "RBJPBIS",
    },
    "EURUSD": {
        "spot":      "DEXUSEU",
        "us_policy": "EFFR",
        "ez_policy": "ECBDFR",
        "us_3m":     "IR3TIB01USM156N",
        "ez_3m":     "IR3TIB01EZM156N",
        "us_10y":    "IRLTLT01USM156N",
        "ez_10y":    "IRLTLT01EZM156N",
        "vix":       "VIXCLS",
        "usd_broad": "DTWEXBGS",
        "us_reer":   "RBUSBIS",
        "ez_reer":   "RBXMBIS",
    },
    "AUDUSD": {
        "spot":      "DEXUSAL",
        "us_policy": "EFFR",
        "au_policy": "IRSTCI01AUM156N",
        "au_3m":     "IR3TIB01AUM156N",
        "us_3m":     "IR3TIB01USM156N",
        "au_10y":    "IRLTLT01AUM156N",
        "us_10y":    "IRLTLT01USM156N",
        "vix":       "VIXCLS",
        "usd_broad": "DTWEXBGS",
        "oil":       "DCOILWTICO",
        "us_reer":   "RBUSBIS",
        "au_reer":   "RBAUBIS",
    },
}


def _get_ohlc_close(conn, pair: str, date_start: date, date_end: date) -> pl.DataFrame:
    """
    fact_ohlc_intraday (日足) の close を取得する。
    FRED spot データが不足している期間の補完に使用。
    """
    try:
        rows = conn.execute("""
            SELECT CAST(datetime_utc AS DATE) AS obs_date, close
            FROM fact_ohlc_intraday
            WHERE pair = ? AND timeframe = '1440'
              AND CAST(datetime_utc AS DATE) BETWEEN ? AND ?
            ORDER BY obs_date
        """, [pair, date_start, date_end]).fetchall()
        if rows:
            return pl.DataFrame({
                "obs_date": [r[0] for r in rows],
                "_ohlc_close": [r[1] for r in rows],
            })
    except Exception as e:
        logger.warning("ohlc_close_fetch_failed", pair=pair, error=str(e))
    return pl.DataFrame()


def build_panel(
    pair: str,
    date_start: date,
    date_end: date,
    features: Optional[list[str]] = None,
    vintage_mode: bool = False,
) -> pl.DataFrame:
    """
    指定 pair の macro panel を構築して返す。
    - 日付軸を生成して left join
    - FRED spot が欠損している日は OHLC 日足 close で自動補完
    - 欠測率を metadata として付与
    """
    series_map = PAIR_SERIES_MAP.get(pair)
    if not series_map:
        raise StorageError(f"未対応の pair: {pair}")

    if features:
        series_map = {k: v for k, v in series_map.items() if k in features}

    conn = get_connection()

    # 日付軸生成（営業日は考慮せず calendar date）
    date_range = pl.date_range(date_start, date_end, interval="1d", eager=True)
    panel = pl.DataFrame({"obs_date": date_range})

    missing_rates: dict[str, float] = {}

    for feature_name, sid in series_map.items():
        sql = """
            SELECT obs_date, value
            FROM fact_market_series_normalized
            WHERE series_id = ?
              AND obs_date BETWEEN ? AND ?
            ORDER BY obs_date
        """
        rows = conn.execute(sql, [sid, date_start, date_end]).fetchall()

        if not rows:
            logger.warning("panel_series_missing", feature=feature_name, series_id=sid)
            panel = panel.with_columns(pl.lit(None).cast(pl.Float64).alias(feature_name))
            missing_rates[feature_name] = 1.0
            continue

        series_df = pl.DataFrame({
            "obs_date": [r[0] for r in rows],
            feature_name: [r[1] for r in rows],
        })

        # as-of join: forward fill で最新値を伝搬（FX は eop 基準）
        panel = panel.join(series_df, on="obs_date", how="left")
        panel = panel.with_columns(
            pl.col(feature_name).forward_fill()
        )

        null_count = panel[feature_name].null_count()
        missing_rates[feature_name] = null_count / len(panel) if len(panel) > 0 else 1.0

    # ── OHLC close で spot 欠損を補完 ────────────────
    if "spot" in panel.columns:
        spot_nulls = panel["spot"].null_count()
        if spot_nulls > 0:
            ohlc = _get_ohlc_close(conn, pair, date_start, date_end)
            if not ohlc.is_empty():
                panel = panel.join(ohlc, on="obs_date", how="left")
                panel = panel.with_columns(
                    pl.when(pl.col("spot").is_null())
                    .then(pl.col("_ohlc_close"))
                    .otherwise(pl.col("spot"))
                    .alias("spot")
                )
                panel = panel.drop("_ohlc_close")
                panel = panel.with_columns(pl.col("spot").forward_fill())
                new_nulls = panel["spot"].null_count()
                missing_rates["spot"] = new_nulls / len(panel) if len(panel) > 0 else 1.0
                logger.info("spot_ohlc_supplemented", pair=pair,
                            before_nulls=spot_nulls, after_nulls=new_nulls)

    logger.info("panel_built", pair=pair, rows=len(panel), features=list(series_map.keys()))
    return panel, missing_rates
