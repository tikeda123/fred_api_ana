"""
fact_series_observations_raw CRUD
"""

from datetime import date
from typing import Optional

from app.core.exceptions import StorageError
from app.core.logging import get_logger
from app.models.domain_models import RawObservation
from app.storage.duckdb import get_connection

logger = get_logger(__name__)


def bulk_insert(observations: list[RawObservation]) -> int:
    if not observations:
        return 0
    conn = get_connection()
    rows = [
        (
            o.series_id, o.date, o.value_raw, o.value_num,
            o.realtime_start, o.realtime_end,
            o.retrieved_at_utc, o.file_batch_id,
            o.source_last_updated,
        )
        for o in observations
    ]
    try:
        conn.executemany(
            """
            INSERT OR REPLACE INTO fact_series_observations_raw (
                series_id, date, value_raw, value_num,
                realtime_start, realtime_end, retrieved_at_utc, file_batch_id,
                source_last_updated
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        logger.info("observations_inserted", count=len(rows))
        return len(rows)
    except Exception as e:
        raise StorageError(f"observations 保存失敗: {e}") from e


def get_by_series(
    series_id: str,
    start: Optional[date] = None,
    end: Optional[date] = None,
) -> list[RawObservation]:
    conn = get_connection()
    conditions = ["series_id = ?"]
    params: list = [series_id]
    if start:
        conditions.append("date >= ?")
        params.append(start)
    if end:
        conditions.append("date <= ?")
        params.append(end)
    sql = f"SELECT * FROM fact_series_observations_raw WHERE {' AND '.join(conditions)} ORDER BY date"
    rows = conn.execute(sql, params).fetchall()
    if not rows:
        return []
    cols = [d[0] for d in conn.execute(
        "SELECT * FROM fact_series_observations_raw LIMIT 0"
    ).description]
    result = []
    for row in rows:
        r = dict(zip(cols, row))
        result.append(RawObservation(
            series_id=r["series_id"],
            date=r["date"],
            value_raw=r["value_raw"],
            value_num=r["value_num"],
            realtime_start=r["realtime_start"],
            realtime_end=r["realtime_end"],
            retrieved_at_utc=r["retrieved_at_utc"],
            file_batch_id=r["file_batch_id"],
            source_last_updated=r.get("source_last_updated"),
        ))
    return result
