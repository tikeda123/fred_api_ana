"""
OHLC データ API
- CSV OHLC ロード状況確認
- 日足 / イントラデイ OHLC 取得
"""

from datetime import date, datetime
from typing import Optional

from fastapi import APIRouter, Query

from app.core.logging import get_logger
from app.models.api_models import ApiResponse

logger = get_logger(__name__)
router = APIRouter(prefix="/ohlc", tags=["ohlc"])


@router.get("/daily/{pair}")
async def get_daily_ohlc(
    pair: str,
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    limit: int = Query(500, le=5000),
) -> ApiResponse:
    """日足 OHLC を取得"""
    from app.services.csv_loader_service import get_daily_ohlc as _get

    start = date.fromisoformat(start_date) if start_date else None
    end = date.fromisoformat(end_date) if end_date else None

    df = _get(pair, start, end)
    records = df.to_dicts() if not df.is_empty() else []
    if limit and len(records) > limit:
        records = records[-limit:]

    return ApiResponse(
        data={"pair": pair, "timeframe": "1440", "count": len(records), "records": records},
        as_of=datetime.utcnow(),
    )


@router.get("/intraday/{pair}/{timeframe}")
async def get_intraday_ohlc(
    pair: str,
    timeframe: str,
    start_dt: Optional[str] = Query(None, description="YYYY-MM-DD HH:MM"),
    end_dt: Optional[str] = Query(None, description="YYYY-MM-DD HH:MM"),
    limit: int = Query(1000, le=10000),
) -> ApiResponse:
    """イントラデイ OHLC を取得"""
    from app.services.csv_loader_service import get_intraday_ohlc as _get

    start = datetime.fromisoformat(start_dt) if start_dt else None
    end = datetime.fromisoformat(end_dt) if end_dt else None

    df = _get(pair, timeframe, start, end)
    records = df.to_dicts() if not df.is_empty() else []
    if limit and len(records) > limit:
        records = records[-limit:]

    return ApiResponse(
        data={"pair": pair, "timeframe": timeframe, "count": len(records), "records": records},
        as_of=datetime.utcnow(),
    )


@router.get("/summary")
async def ohlc_summary() -> ApiResponse:
    """ロード済み OHLC データのサマリー"""
    from app.storage.duckdb import get_connection
    conn = get_connection()
    rows = conn.execute("""
        SELECT pair, timeframe,
               COUNT(*) AS row_count,
               MIN(datetime_utc) AS min_dt,
               MAX(datetime_utc) AS max_dt
        FROM fact_ohlc_intraday
        GROUP BY pair, timeframe
        ORDER BY pair, timeframe
    """).fetchall()

    summary = [
        {
            "pair": r[0], "timeframe": r[1],
            "row_count": r[2],
            "min_datetime": str(r[3]),
            "max_datetime": str(r[4]),
        }
        for r in rows
    ]
    return ApiResponse(
        data=summary,
        as_of=datetime.utcnow(),
    )
