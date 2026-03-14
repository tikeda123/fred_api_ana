"""
dim_series_registry CRUD
"""

from datetime import datetime
from typing import Optional

from app.core.exceptions import StorageError
from app.core.logging import get_logger
from app.models.domain_models import SeriesMetadata
from app.storage.duckdb import get_connection

logger = get_logger(__name__)


def upsert(meta: SeriesMetadata) -> None:
    conn = get_connection()
    now = datetime.utcnow()
    try:
        conn.execute(
            """
            INSERT INTO dim_series_registry (
                series_id, title, source_name, release_id, release_name,
                units_native, frequency_native, seasonal_adjustment,
                observation_start, observation_end, last_updated, notes,
                domain, base_ccy, quote_ccy, pair, quote_direction,
                freshness_status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (series_id) DO UPDATE SET
                title = excluded.title,
                source_name = excluded.source_name,
                units_native = excluded.units_native,
                frequency_native = excluded.frequency_native,
                observation_end = excluded.observation_end,
                last_updated = excluded.last_updated,
                freshness_status = excluded.freshness_status,
                updated_at = excluded.updated_at
            """,
            (
                meta.series_id, meta.title, meta.source_name,
                meta.release_id, meta.release_name,
                meta.units_native, meta.frequency_native,
                meta.seasonal_adjustment,
                meta.observation_start, meta.observation_end,
                meta.last_updated, meta.notes,
                meta.domain, meta.base_ccy, meta.quote_ccy,
                meta.pair, meta.quote_direction,
                meta.freshness_status, now, now,
            ),
        )
        logger.info("registry_upserted", series_id=meta.series_id)
    except Exception as e:
        raise StorageError(f"registry upsert 失敗 [{meta.series_id}]: {e}") from e


def get(series_id: str) -> Optional[SeriesMetadata]:
    conn = get_connection()
    row = conn.execute(
        "SELECT * FROM dim_series_registry WHERE series_id = ?", [series_id]
    ).fetchone()
    if row is None:
        return None
    cols = [d[0] for d in conn.execute(
        "SELECT * FROM dim_series_registry WHERE series_id = ?", [series_id]
    ).description]
    return _row_to_meta(dict(zip(cols, row)))


def search(keyword: str, limit: int = 50) -> list[SeriesMetadata]:
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT * FROM dim_series_registry
        WHERE title ILIKE ? OR series_id ILIKE ?
        LIMIT ?
        """,
        [f"%{keyword}%", f"%{keyword}%", limit],
    ).fetchall()
    if not rows:
        return []
    cols = [d[0] for d in conn.execute(
        "SELECT * FROM dim_series_registry LIMIT 0"
    ).description]
    return [_row_to_meta(dict(zip(cols, r))) for r in rows]


def _row_to_meta(row: dict) -> SeriesMetadata:
    return SeriesMetadata(
        series_id=row["series_id"],
        title=row["title"],
        frequency_native=row.get("frequency_native", ""),
        units_native=row.get("units_native", ""),
        seasonal_adjustment=row.get("seasonal_adjustment", ""),
        observation_start=row.get("observation_start"),
        observation_end=row.get("observation_end"),
        last_updated=row.get("last_updated"),
        source_name=row.get("source_name"),
        release_id=row.get("release_id"),
        release_name=row.get("release_name"),
        notes=row.get("notes"),
        domain=row.get("domain"),
        base_ccy=row.get("base_ccy"),
        quote_ccy=row.get("quote_ccy"),
        pair=row.get("pair"),
        quote_direction=row.get("quote_direction"),
        freshness_status=row.get("freshness_status", "unknown"),
    )
