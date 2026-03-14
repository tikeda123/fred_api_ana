"""
Ingestion Service
- FRED から observations を取得
- RawObservation に変換
- DuckDB に保存
- 監査ログを記録
"""

import uuid
from datetime import date, datetime
from typing import Optional

from app.core.exceptions import FredApiError
from app.core.logging import get_logger
from app.models.api_models import ObservationRecord, ObservationsFetchResponse
from app.models.domain_models import RawObservation
from app.services.fred_client import FredClient
from app.services.registry_service import upsert_from_fred_response
from app.storage.duckdb import get_connection
from app.storage.repositories import observation_repo

logger = get_logger(__name__)


async def fetch_and_store(
    series_id: str,
    observation_start: Optional[date] = None,
    observation_end: Optional[date] = None,
    units: str = "lin",
    frequency: Optional[str] = None,
    aggregation_method: str = "eop",
    realtime_start: Optional[date] = None,
    realtime_end: Optional[date] = None,
    vintage_dates: Optional[list[date]] = None,
    store_raw: bool = True,
    normalize: bool = False,
) -> ObservationsFetchResponse:

    batch_id = str(uuid.uuid4())
    started_at = datetime.utcnow()
    run_id = str(uuid.uuid4())
    _audit_start(run_id, "fetch_observations", series_id, started_at)

    try:
        async with FredClient() as client:
            # メタデータを先に取得して registry に登録
            meta_resp = await client.get_series_metadata(series_id)
            source_last_updated: Optional[datetime] = None
            if "seriess" in meta_resp:
                upsert_from_fred_response(meta_resp["seriess"])
                seriess = meta_resp["seriess"]
                if seriess and seriess[0].get("last_updated"):
                    try:
                        from datetime import timezone
                        lu = seriess[0]["last_updated"]
                        # "2026-03-14T16:16:06-05:00" 形式
                        source_last_updated = datetime.fromisoformat(lu).astimezone(timezone.utc).replace(tzinfo=None)
                    except Exception:
                        pass

            # observations 取得
            raw_resp = await client.get_observations(
                series_id=series_id,
                observation_start=observation_start,
                observation_end=observation_end,
                units=units,
                frequency=frequency,
                aggregation_method=aggregation_method,
                realtime_start=realtime_start,
                realtime_end=realtime_end,
                vintage_dates=vintage_dates,
            )

        observations_raw = raw_resp.get("observations", [])
        retrieved_at = datetime.utcnow()
        rt_start = _parse_date(raw_resp.get("realtime_start"))
        rt_end = _parse_date(raw_resp.get("realtime_end"))

        raw_obs = [
            RawObservation(
                series_id=series_id,
                date=_parse_date(o["date"]),
                value_raw=o["value"],
                value_num=_to_float(o["value"]),
                realtime_start=rt_start or date.today(),
                realtime_end=rt_end or date.today(),
                retrieved_at_utc=retrieved_at,
                file_batch_id=batch_id,
                source_last_updated=source_last_updated,
            )
            for o in observations_raw
        ]

        count = 0
        if store_raw:
            count = observation_repo.bulk_insert(raw_obs)

        _audit_finish(run_id, "ok", count)
        logger.info("ingest_complete", series_id=series_id, count=count, batch_id=batch_id)

        return ObservationsFetchResponse(
            series_id=series_id,
            count=count,
            observations=[
                ObservationRecord(
                    date=o.date,
                    value_raw=o.value_raw,
                    value_num=o.value_num,
                    realtime_start=o.realtime_start,
                    realtime_end=o.realtime_end,
                )
                for o in raw_obs
            ],
            retrieved_at_utc=retrieved_at,
            batch_id=batch_id,
        )

    except Exception as e:
        _audit_finish(run_id, "failed", 0, str(e))
        raise


def _parse_date(s: Optional[str]) -> Optional[date]:
    if not s:
        return None
    try:
        return date.fromisoformat(s)
    except ValueError:
        return None


def _to_float(s: str) -> Optional[float]:
    if s == "." or not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _audit_start(run_id: str, job_name: str, series_id: str, started_at: datetime) -> None:
    try:
        conn = get_connection()
        conn.execute(
            """
            INSERT INTO audit_ingestion_runs (run_id, job_name, started_at, status, endpoint, request_params)
            VALUES (?, ?, ?, 'running', 'series/observations', ?)
            """,
            (run_id, job_name, started_at, series_id),
        )
    except Exception:
        pass


def _audit_finish(run_id: str, status: str, count: int, error: str = "") -> None:
    try:
        conn = get_connection()
        conn.execute(
            """
            UPDATE audit_ingestion_runs
            SET finished_at = ?, status = ?, record_count = ?, error_message = ?
            WHERE run_id = ?
            """,
            (datetime.utcnow(), status, count, error or None, run_id),
        )
    except Exception:
        pass
