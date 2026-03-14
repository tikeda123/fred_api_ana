"""
Events API
- POST /events/analyze-window : イベント周辺リターン分析
"""

from datetime import datetime

from fastapi import APIRouter

from app.models.api_models import ApiResponse, EventAnalyzeRequest
from app.services import event_study_service

router = APIRouter(tags=["events"])


@router.post("/events/analyze-window")
async def analyze_event_window(req: EventAnalyzeRequest) -> ApiResponse:
    result = event_study_service.analyze_event_window(
        pair=req.pair,
        event_dates=req.event_dates,
        window=req.window,
        start=req.start,
        end=req.end,
    )
    return ApiResponse(data=result, as_of=datetime.utcnow())
