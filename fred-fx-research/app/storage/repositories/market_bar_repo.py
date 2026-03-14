"""
Market Bar リポジトリ
- fact.market_bars_raw  : append-only insert
- fact.market_bars_norm : delete + insert (upsert)
- fact.market_bars_daily: delete + insert (upsert)
"""

from datetime import date, datetime
from typing import Optional

import polars as pl

from app.core.exceptions import StorageError
from app.core.logging import get_logger
from app.storage.duckdb import get_connection

logger = get_logger(__name__)


# ── raw ───────────────────────────────────────────────────────────

def insert_raw(df: pl.DataFrame) -> int:
    """
    fact.market_bars_raw に行を追加する（append-only）。
    df には raw テーブルの全列が必要。
    """
    conn = get_connection()
    try:
        conn.register("_raw_df", df.to_arrow())
        conn.execute("""
            INSERT INTO fact.market_bars_raw
            SELECT upload_id, instrument_id, timeframe, source_file_name,
                   source_row_number, ts_utc, open, high, low, close, volume,
                   source_line_hash, ingest_status, quality_flags, ingested_at
            FROM _raw_df
        """)
        conn.unregister("_raw_df")
    except Exception as e:
        raise StorageError(f"market_bars_raw 挿入失敗: {e}") from e

    logger.info("raw_inserted", rows=len(df))
    return len(df)


# ── norm ──────────────────────────────────────────────────────────

def upsert_norm(
    instrument_id: str,
    timeframe: str,
    min_ts: datetime,
    max_ts: datetime,
    df: pl.DataFrame,
) -> int:
    """
    fact.market_bars_norm を対象期間だけ delete + insert する。
    """
    conn = get_connection()
    try:
        conn.execute(
            """
            DELETE FROM fact.market_bars_norm
            WHERE instrument_id = ? AND timeframe = ?
              AND ts_utc BETWEEN ? AND ?
            """,
            [instrument_id, timeframe, min_ts, max_ts],
        )
        conn.register("_norm_df", df.to_arrow())
        conn.execute("""
            INSERT INTO fact.market_bars_norm
            SELECT instrument_id, timeframe, ts_utc, trade_date_utc,
                   bar_year, bar_month, open, high, low, close, volume,
                   simple_ret_1bar, log_ret_1bar, hl_range_pct, oc_body_pct,
                   gap_from_prev_close_pct, h4_slot_utc, session_bucket,
                   is_weekend_gap, quality_status, source_upload_id, created_at
            FROM _norm_df
        """)
        conn.unregister("_norm_df")
    except Exception as e:
        raise StorageError(f"market_bars_norm upsert 失敗: {e}") from e

    logger.info("norm_upserted", instrument_id=instrument_id, timeframe=timeframe, rows=len(df))
    return len(df)


def get_norm_bars(
    instrument_id: str,
    timeframe: str,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    limit: int = 10000,
) -> list[dict]:
    """fact.market_bars_norm からバーを返す"""
    conn = get_connection()
    conditions = ["instrument_id = ?", "timeframe = ?"]
    params: list = [instrument_id, timeframe]
    if start:
        conditions.append("ts_utc >= ?")
        params.append(start)
    if end:
        conditions.append("ts_utc <= ?")
        params.append(end)
    params.append(limit)

    rows = conn.execute(
        f"""
        SELECT ts_utc, trade_date_utc, open, high, low, close, volume,
               simple_ret_1bar, log_ret_1bar, hl_range_pct, session_bucket,
               is_weekend_gap, quality_status
        FROM fact.market_bars_norm
        WHERE {' AND '.join(conditions)}
        ORDER BY ts_utc
        LIMIT ?
        """,
        params,
    ).fetchall()

    cols = [
        "ts_utc", "trade_date_utc", "open", "high", "low", "close", "volume",
        "simple_ret_1bar", "log_ret_1bar", "hl_range_pct", "session_bucket",
        "is_weekend_gap", "quality_status",
    ]
    result = []
    for row in rows:
        rec = dict(zip(cols, row))
        rec["ts_utc"] = str(rec["ts_utc"])
        rec["trade_date_utc"] = str(rec["trade_date_utc"])
        result.append(rec)
    return result


# ── daily ─────────────────────────────────────────────────────────

def upsert_daily(
    instrument_id: str,
    min_date: date,
    max_date: date,
    df: pl.DataFrame,
) -> int:
    """
    fact.market_bars_daily を対象期間だけ delete + insert する。
    """
    conn = get_connection()
    try:
        conn.execute(
            """
            DELETE FROM fact.market_bars_daily
            WHERE instrument_id = ? AND obs_date BETWEEN ? AND ?
            """,
            [instrument_id, min_date, max_date],
        )
        conn.register("_daily_df", df.to_arrow())
        conn.execute("""
            INSERT INTO fact.market_bars_daily
            SELECT instrument_id, obs_date, timeframe_source,
                   open, high, low, close, volume, bar_count,
                   simple_ret_1d, log_ret_1d, range_pct_1d,
                   gap_from_prev_close_pct, quality_status, build_id, built_at
            FROM _daily_df
        """)
        conn.unregister("_daily_df")
    except Exception as e:
        raise StorageError(f"market_bars_daily upsert 失敗: {e}") from e

    logger.info("daily_upserted", instrument_id=instrument_id, rows=len(df))
    return len(df)


def get_daily_bars(
    instrument_id: str,
    start: Optional[date] = None,
    end: Optional[date] = None,
) -> list[dict]:
    """fact.market_bars_daily から日次バーを返す"""
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
        SELECT obs_date, timeframe_source, open, high, low, close, volume,
               bar_count, simple_ret_1d, log_ret_1d, range_pct_1d,
               gap_from_prev_close_pct, quality_status
        FROM fact.market_bars_daily
        WHERE {' AND '.join(conditions)}
        ORDER BY obs_date
        """,
        params,
    ).fetchall()

    cols = [
        "obs_date", "timeframe_source", "open", "high", "low", "close", "volume",
        "bar_count", "simple_ret_1d", "log_ret_1d", "range_pct_1d",
        "gap_from_prev_close_pct", "quality_status",
    ]
    result = []
    for row in rows:
        rec = dict(zip(cols, row))
        rec["obs_date"] = str(rec["obs_date"])
        result.append(rec)
    return result


def count_by_instrument(instrument_id: str) -> dict:
    """raw / norm / daily それぞれの行数を返す"""
    conn = get_connection()
    raw_count = conn.execute(
        "SELECT count(*) FROM fact.market_bars_raw WHERE instrument_id = ?",
        [instrument_id],
    ).fetchone()[0]
    norm_count = conn.execute(
        "SELECT count(*) FROM fact.market_bars_norm WHERE instrument_id = ?",
        [instrument_id],
    ).fetchone()[0]
    daily_count = conn.execute(
        "SELECT count(*) FROM fact.market_bars_daily WHERE instrument_id = ?",
        [instrument_id],
    ).fetchone()[0]
    return {"raw": raw_count, "norm": norm_count, "daily": daily_count}
