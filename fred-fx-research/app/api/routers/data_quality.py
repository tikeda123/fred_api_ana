"""
Data Quality API
- Freshness audit
- Ingestion log
- Series registry
"""

from datetime import datetime

from fastapi import APIRouter, Query

from app.models.api_models import ApiResponse
from app.storage.duckdb import get_connection

router = APIRouter(prefix="/data-quality", tags=["data-quality"])


@router.post("/audit")
async def run_freshness_audit() -> ApiResponse:
    """全 series の鮮度監査を実行"""
    from app.services.freshness_service import audit_all
    results = audit_all()
    return ApiResponse(data=results, as_of=datetime.utcnow())


@router.get("/ingestion-log")
async def get_ingestion_log(limit: int = Query(50, le=200)) -> ApiResponse:
    """Ingestion log を取得"""
    conn = get_connection()
    rows = conn.execute("""
        SELECT run_id, job_name, status, record_count, endpoint,
               started_at, finished_at, error_message
        FROM audit_ingestion_runs
        ORDER BY started_at DESC
        LIMIT ?
    """, [limit]).fetchall()
    cols = ["run_id", "job_name", "status", "record_count", "endpoint",
            "started_at", "finished_at", "error_message"]
    records = [dict(zip(cols, r)) for r in rows]
    # datetime を文字列に変換
    for rec in records:
        for k in ("started_at", "finished_at"):
            if rec[k] is not None:
                rec[k] = str(rec[k])
    return ApiResponse(data=records, as_of=datetime.utcnow())


@router.get("/registry")
async def get_registry() -> ApiResponse:
    """Series registry を取得"""
    conn = get_connection()
    rows = conn.execute("""
        SELECT series_id, title, domain, pair, frequency_native,
               observation_start, observation_end, last_updated, freshness_status
        FROM dim_series_registry
        ORDER BY domain, series_id
    """).fetchall()
    cols = ["series_id", "title", "domain", "pair", "frequency",
            "obs_start", "obs_end", "last_updated", "freshness"]
    records = [dict(zip(cols, r)) for r in rows]
    for rec in records:
        for k in ("obs_start", "obs_end", "last_updated"):
            if rec[k] is not None:
                rec[k] = str(rec[k])
    return ApiResponse(data=records, as_of=datetime.utcnow())
