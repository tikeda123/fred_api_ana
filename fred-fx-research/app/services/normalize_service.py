"""
NormalizeService
- raw observations → fact_market_series_normalized
- FX quote 方向の正規化
- derived cross 生成 (EURJPY, AUDJPY, EURAUD)
- raw は上書きしない
"""

from datetime import date, datetime
from typing import Optional

import polars as pl

from app.core.exceptions import NormalizationError
from app.core.logging import get_logger
from app.models.domain_models import FX_SERIES_MAP
from app.storage.duckdb import get_connection
from app.storage.parquet_io import write_normalized
from app.storage.repositories.observation_repo import get_by_series

logger = get_logger(__name__)

# pair → 構成 series の対応
PAIR_SERIES_DEPS: dict[str, list[str]] = {
    "USDJPY": ["DEXJPUS"],
    "EURUSD": ["DEXUSEU"],
    "AUDUSD": ["DEXUSAL"],
    "EURJPY": ["DEXUSEU", "DEXJPUS"],   # EURUSD * USDJPY
    "AUDJPY": ["DEXUSAL", "DEXJPUS"],   # AUDUSD * USDJPY
    "EURAUD": ["DEXUSEU", "DEXUSAL"],   # EURUSD / AUDUSD
}


def normalize_series(
    series_id: str,
    start: Optional[date] = None,
    end: Optional[date] = None,
    realtime_start: Optional[date] = None,
    realtime_end: Optional[date] = None,
) -> int:
    """
    series_id の raw 観測値を normalized layer に書き込む。
    returns: 正規化した行数
    """
    obs = get_by_series(series_id, start, end)
    if not obs:
        logger.warning("normalize_no_data", series_id=series_id)
        return 0

    fx_info = FX_SERIES_MAP.get(series_id, {})
    domain = fx_info.get("domain", "unknown")
    pair = fx_info.get("pair", series_id)
    base_ccy = fx_info.get("base_ccy")
    quote_ccy = fx_info.get("quote_ccy")
    rt_start = realtime_start or date.today()
    rt_end = realtime_end or date.today()

    # dim_series_registry から frequency_native を取得
    conn = get_connection()
    reg_row = conn.execute(
        "SELECT frequency_native FROM dim_series_registry WHERE series_id = ?",
        [series_id],
    ).fetchone()
    source_freq = reg_row[0] if reg_row and reg_row[0] else ""

    rows = []
    for o in obs:
        rows.append({
            "series_id": series_id,
            "obs_date": o.date,
            "domain": domain,
            "pair": pair,
            "base_ccy": base_ccy,
            "quote_ccy": quote_ccy,
            "value": o.value_num,          # "." は already None
            "units_normalized": "price_quote_per_base",
            "is_derived": False,
            "is_supplemental": False,
            "transformation": "none",
            "source_frequency": source_freq,
            "frequency_requested": source_freq,
            "aggregation_method": "eop",
            "as_of_realtime_start": rt_start,
            "as_of_realtime_end": rt_end,
            "created_at": datetime.utcnow(),
        })

    df = pl.DataFrame(rows, schema_overrides={
        "pair": pl.Utf8,
        "base_ccy": pl.Utf8,
        "quote_ccy": pl.Utf8,
        "value": pl.Float64,
        "as_of_realtime_start": pl.Date,
        "as_of_realtime_end": pl.Date,
    })
    _upsert_normalized(df)
    write_normalized(df, domain, pair, series_id)

    logger.info("normalized", series_id=series_id, rows=len(df))
    return len(df)


def compute_derived_crosses(
    start: Optional[date] = None,
    end: Optional[date] = None,
) -> dict[str, int]:
    """
    EURJPY / AUDJPY / EURAUD の derived cross を計算して保存。
    returns: pair → 行数 の辞書
    """
    results: dict[str, int] = {}

    usdjpy = _load_normalized("DEXJPUS", start, end)  # USDJPY
    eurusd = _load_normalized("DEXUSEU", start, end)  # EURUSD
    audusd = _load_normalized("DEXUSAL", start, end)  # AUDUSD

    if not usdjpy.is_empty() and not eurusd.is_empty():
        eurjpy = _cross_multiply(eurusd, usdjpy, "EURJPY", "EUR", "JPY")
        results["EURJPY"] = _save_derived(eurjpy, "EURJPY", "EURJPY = EURUSD * USDJPY")

    if not usdjpy.is_empty() and not audusd.is_empty():
        audjpy = _cross_multiply(audusd, usdjpy, "AUDJPY", "AUD", "JPY")
        results["AUDJPY"] = _save_derived(audjpy, "AUDJPY", "AUDJPY = AUDUSD * USDJPY")

    if not eurusd.is_empty() and not audusd.is_empty():
        euraud = _cross_divide(eurusd, audusd, "EURAUD", "EUR", "AUD")
        results["EURAUD"] = _save_derived(euraud, "EURAUD", "EURAUD = EURUSD / AUDUSD")

    return results


def _load_normalized(series_id: str, start: Optional[date], end: Optional[date]) -> pl.DataFrame:
    conn = get_connection()
    conditions = ["series_id = ?"]
    params: list = [series_id]
    if start:
        conditions.append("obs_date >= ?")
        params.append(start)
    if end:
        conditions.append("obs_date <= ?")
        params.append(end)
    sql = f"SELECT obs_date, value FROM fact_market_series_normalized WHERE {' AND '.join(conditions)} ORDER BY obs_date"
    rows = conn.execute(sql, params).fetchall()
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame({"obs_date": [r[0] for r in rows], "value": [r[1] for r in rows]})


def _cross_multiply(df_a: pl.DataFrame, df_b: pl.DataFrame, pair: str, base: str, quote: str) -> pl.DataFrame:
    joined = df_a.join(df_b, on="obs_date", how="inner", suffix="_b")
    return joined.with_columns([
        pl.lit(pair).alias("pair"),
        pl.lit(base).alias("base_ccy"),
        pl.lit(quote).alias("quote_ccy"),
        (pl.col("value") * pl.col("value_b")).alias("value"),
        pl.lit(True).alias("is_derived"),
    ]).select(["obs_date", "pair", "base_ccy", "quote_ccy", "value", "is_derived"])


def _cross_divide(df_a: pl.DataFrame, df_b: pl.DataFrame, pair: str, base: str, quote: str) -> pl.DataFrame:
    joined = df_a.join(df_b, on="obs_date", how="inner", suffix="_b")
    return joined.with_columns([
        pl.lit(pair).alias("pair"),
        pl.lit(base).alias("base_ccy"),
        pl.lit(quote).alias("quote_ccy"),
        (pl.col("value") / pl.col("value_b")).alias("value"),
        pl.lit(True).alias("is_derived"),
    ]).select(["obs_date", "pair", "base_ccy", "quote_ccy", "value", "is_derived"])


def _save_derived(df: pl.DataFrame, pair: str, formula: str) -> int:
    if df.is_empty():
        return 0
    now = datetime.utcnow()
    full_df = df.with_columns([
        pl.lit(f"derived_{pair}").alias("series_id"),
        pl.lit("fx_derived").alias("domain"),
        pl.lit("price_quote_per_base").alias("units_normalized"),
        pl.lit(False).alias("is_supplemental"),
        pl.lit(formula).alias("transformation"),
        pl.lit("").alias("source_frequency"),
        pl.lit("D").alias("frequency_requested"),
        pl.lit("eop").alias("aggregation_method"),
        pl.lit(date.today()).alias("as_of_realtime_start"),
        pl.lit(date.today()).alias("as_of_realtime_end"),
        pl.lit(now).alias("created_at"),
    ])
    _upsert_normalized(full_df)
    from app.storage.parquet_io import write_normalized
    write_normalized(full_df, "fx_derived", pair, f"derived_{pair}")
    logger.info("derived_saved", pair=pair, rows=len(df))
    return len(df)


def _upsert_normalized(df: pl.DataFrame) -> None:
    conn = get_connection()
    cols = [
        "series_id", "obs_date", "domain", "pair", "base_ccy", "quote_ccy",
        "value", "units_normalized", "is_derived", "is_supplemental",
        "transformation", "source_frequency", "frequency_requested",
        "aggregation_method", "as_of_realtime_start", "as_of_realtime_end", "created_at",
    ]
    placeholders = ", ".join(["?"] * len(cols))
    col_list = ", ".join(cols)
    try:
        rows = [
            tuple(rec.get(c) for c in cols)
            for rec in df.iter_rows(named=True)
        ]
        conn.executemany(
            f"INSERT OR REPLACE INTO fact_market_series_normalized ({col_list}) VALUES ({placeholders})",
            rows,
        )
    except Exception as e:
        raise NormalizationError(f"normalized upsert 失敗: {e}") from e
