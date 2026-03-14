"""
Market Bars API
- GET  /market/bars           : 正規化バー取得
- POST /market/bars/rebuild-daily : 日次集約再構築
- GET  /quality/market-report : 品質レポート
"""

from datetime import date, datetime
from typing import Optional

from fastapi import APIRouter, Query

from app.models.api_models import ApiResponse, DailyRebuildRequest
from app.services import market_bar_service

router = APIRouter(tags=["market-bars"])


@router.get("/market/bars")
async def get_market_bars(
    instrument_id: str = Query(...),
    timeframe: str = Query("240"),
    start: Optional[datetime] = Query(None),
    end: Optional[datetime] = Query(None),
    limit: int = Query(10000, le=50000),
) -> ApiResponse:
    """正規化済みバーを返す"""
    bars = market_bar_service.get_normalized_bars(
        instrument_id=instrument_id,
        timeframe=timeframe,
        start=start,
        end=end,
        limit=limit,
    )
    return ApiResponse(data=bars, as_of=datetime.utcnow())


@router.post("/market/bars/rebuild-daily")
async def rebuild_daily(req: DailyRebuildRequest) -> ApiResponse:
    """H4 正規化バーから日次集約を再構築する"""
    n = market_bar_service.rebuild_daily(
        instrument_id=req.instrument_id,
        start=req.start,
        end=req.end,
    )
    return ApiResponse(
        data={"instrument_id": req.instrument_id, "days_rebuilt": n},
        as_of=datetime.utcnow(),
    )


@router.get("/quality/market-report")
async def get_market_quality_report(
    instrument_id: str = Query(...),
    start: Optional[date] = Query(None),
    end: Optional[date] = Query(None),
) -> ApiResponse:
    """gap / 欠損 / 重複 / 異常 range のレポートを返す"""
    report = market_bar_service.quality_report(
        instrument_id=instrument_id,
        start=start,
        end=end,
    )
    return ApiResponse(data=report, as_of=datetime.utcnow())
