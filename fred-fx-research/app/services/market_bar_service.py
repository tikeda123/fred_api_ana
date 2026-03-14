"""
Market Bar Service
- fact.market_bars_norm の H4 バーから fact.market_bars_daily を再構築
- 品質レポート生成
"""

import uuid
from datetime import date, datetime
from typing import Optional

import polars as pl

from app.core.logging import get_logger
from app.storage.duckdb import get_connection
from app.storage.repositories import market_bar_repo

logger = get_logger(__name__)


def rebuild_daily(
    instrument_id: str,
    timeframe_source: str = "240",
    start: Optional[date] = None,
    end: Optional[date] = None,
) -> int:
    """
    fact.market_bars_norm の H4 バーを日次に集約して
    fact.market_bars_daily を再構築する。

    Returns:
        保存した日数
    """
    conn = get_connection()
    conditions = ["instrument_id = ?", "timeframe = ?"]
    params: list = [instrument_id, timeframe_source]
    if start:
        conditions.append("trade_date_utc >= ?")
        params.append(start)
    if end:
        conditions.append("trade_date_utc <= ?")
        params.append(end)

    # DuckDB で日次集約（first/last は ORDER BY 付きで使用）
    rows = conn.execute(
        f"""
        WITH agg AS (
            SELECT
                instrument_id,
                trade_date_utc  AS obs_date,
                first(open  ORDER BY ts_utc) AS open,
                max(high)                    AS high,
                min(low)                     AS low,
                last(close  ORDER BY ts_utc) AS close,
                sum(volume)                  AS volume,
                count(*)                     AS bar_count
            FROM fact.market_bars_norm
            WHERE {' AND '.join(conditions)}
            GROUP BY instrument_id, trade_date_utc
        )
        SELECT
            instrument_id, obs_date,
            open, high, low, close, volume, bar_count,
            close / lag(close) OVER (ORDER BY obs_date) - 1          AS simple_ret_1d,
            ln(close / lag(close) OVER (ORDER BY obs_date))          AS log_ret_1d,
            (high - low) / nullif(close, 0)                          AS range_pct_1d,
            (open - lag(close) OVER (ORDER BY obs_date))
                / nullif(lag(close) OVER (ORDER BY obs_date), 0)     AS gap_from_prev_close_pct
        FROM agg
        ORDER BY obs_date
        """,
        params,
    ).fetchall()

    if not rows:
        logger.info("rebuild_daily_no_data", instrument_id=instrument_id)
        return 0

    cols = [
        "instrument_id", "obs_date",
        "open", "high", "low", "close", "volume", "bar_count",
        "simple_ret_1d", "log_ret_1d", "range_pct_1d", "gap_from_prev_close_pct",
    ]
    build_id = f"build_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    now = datetime.utcnow()

    daily_records = []
    for row in rows:
        rec = dict(zip(cols, row))
        rec["timeframe_source"] = timeframe_source
        rec["quality_status"] = "ok"
        rec["build_id"] = build_id
        rec["built_at"] = now
        daily_records.append(rec)

    df = pl.DataFrame(daily_records)

    min_date = df["obs_date"].min()
    max_date = df["obs_date"].max()
    n = market_bar_repo.upsert_daily(instrument_id, min_date, max_date, df)

    logger.info("rebuild_daily_done", instrument_id=instrument_id, days=n, build_id=build_id)
    return n


def get_normalized_bars(
    instrument_id: str,
    timeframe: str,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    limit: int = 10000,
) -> list[dict]:
    """fact.market_bars_norm からバーを返す"""
    return market_bar_repo.get_norm_bars(instrument_id, timeframe, start, end, limit)


def get_daily_bars(
    instrument_id: str,
    start: Optional[date] = None,
    end: Optional[date] = None,
) -> list[dict]:
    """fact.market_bars_daily からデイリーバーを返す"""
    return market_bar_repo.get_daily_bars(instrument_id, start, end)


def export_to_parquet(
    instrument_id: str,
    output_dir: str,
    partition_by: Optional[list[str]] = None,
) -> dict:
    """
    fact.market_bars_norm を Parquet に書き出す。

    Args:
        instrument_id: 対象 instrument_id
        output_dir: 出力先ディレクトリ
        partition_by: パーティション列（デフォルト: instrument_id, timeframe, bar_year）

    Returns:
        {"output_dir": str, "norm_rows": int, "daily_rows": int}
    """
    from pathlib import Path as _Path

    if partition_by is None:
        partition_by = ["instrument_id", "timeframe", "bar_year"]

    out = _Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    conn = get_connection()

    # norm バーをエクスポート
    norm_path = str(out / "norm")
    part_cols = ", ".join(partition_by)
    conn.execute(
        f"""
        COPY (
            SELECT *, year(ts_utc) AS bar_year
            FROM fact.market_bars_norm
            WHERE instrument_id = ?
        )
        TO '{norm_path}'
        (FORMAT parquet, PARTITION_BY ({part_cols}))
        """,
        [instrument_id],
    )
    norm_rows = conn.execute(
        "SELECT count(*) FROM fact.market_bars_norm WHERE instrument_id = ?",
        [instrument_id],
    ).fetchone()[0]

    # daily バーをエクスポート
    daily_path = str(out / "daily")
    conn.execute(
        f"""
        COPY (
            SELECT *
            FROM fact.market_bars_daily
            WHERE instrument_id = ?
        )
        TO '{daily_path}'
        (FORMAT parquet, PARTITION_BY (instrument_id))
        """,
        [instrument_id],
    )
    daily_rows = conn.execute(
        "SELECT count(*) FROM fact.market_bars_daily WHERE instrument_id = ?",
        [instrument_id],
    ).fetchone()[0]

    logger.info(
        "export_to_parquet_done",
        instrument_id=instrument_id,
        output_dir=output_dir,
        norm_rows=norm_rows,
        daily_rows=daily_rows,
    )
    return {
        "output_dir": output_dir,
        "norm_rows": norm_rows,
        "daily_rows": daily_rows,
    }


def quality_report(
    instrument_id: str,
    start: Optional[date] = None,
    end: Optional[date] = None,
) -> dict:
    """gap / 欠損 / 重複 / 異常 range のレポートを返す"""
    conn = get_connection()
    conditions = ["instrument_id = ?"]
    params: list = [instrument_id]
    if start:
        conditions.append("obs_date >= ?")
        params.append(start)
    if end:
        conditions.append("obs_date <= ?")
        params.append(end)

    where = f"WHERE {' AND '.join(conditions)}"

    # 総日数
    total = conn.execute(
        f"SELECT count(*) FROM fact.market_bars_daily {where}", params
    ).fetchone()[0]

    # 異常 range（range_pct_1d が上位 1% 超）
    p99 = conn.execute(
        f"SELECT quantile_cont(range_pct_1d, 0.99) FROM fact.market_bars_daily {where}", params
    ).fetchone()[0]
    abnormal_range_count = conn.execute(
        f"SELECT count(*) FROM fact.market_bars_daily {where} AND range_pct_1d > ?",
        params + [p99 or 0],
    ).fetchone()[0] if p99 else 0

    # 日付連続性チェック（trading day gap 検出）
    gap_rows = conn.execute(
        f"""
        SELECT obs_date,
               lag(obs_date) OVER (ORDER BY obs_date) AS prev_date,
               datediff('day', lag(obs_date) OVER (ORDER BY obs_date), obs_date) AS day_gap
        FROM fact.market_bars_daily {where}
        QUALIFY day_gap > 5
        ORDER BY obs_date
        LIMIT 20
        """,
        params,
    ).fetchall()
    gaps = [{"obs_date": str(r[0]), "prev_date": str(r[1]), "day_gap": r[2]} for r in gap_rows]

    # 行数サマリ
    counts = market_bar_repo.count_by_instrument(instrument_id)

    return {
        "instrument_id": instrument_id,
        "total_daily_rows": total,
        "raw_rows": counts["raw"],
        "norm_rows": counts["norm"],
        "daily_rows": counts["daily"],
        "abnormal_range_count": abnormal_range_count,
        "range_p99": p99,
        "date_gaps": gaps,
    }
