"""
Filters API
- POST /filters/evaluate-custom : カスタム閾値フィルタ評価
- POST /filters/simulate-pnl   : PnL シミュレーション
"""

from datetime import datetime

from fastapi import APIRouter

from app.models.api_models import ApiResponse, FilterEvaluateRequest, FilterSimulatePnlRequest
from app.services import filter_lab_service

router = APIRouter(tags=["filters"])


@router.post("/filters/evaluate-custom")
async def evaluate_custom_filter(req: FilterEvaluateRequest) -> ApiResponse:
    result = filter_lab_service.evaluate_custom_filter(
        pair=req.pair,
        start=req.start,
        end=req.end,
        dd_threshold=req.thresholds.drawdown_pct,
        rv_percentile=req.thresholds.rv_percentile,
        vix_percentile=req.thresholds.vix_percentile,
        usd_z_threshold=req.thresholds.usd_z,
        min_conditions=req.thresholds.min_conditions,
    )
    return ApiResponse(data=result, as_of=datetime.utcnow())


@router.post("/filters/simulate-pnl")
async def simulate_pnl(req: FilterSimulatePnlRequest) -> ApiResponse:
    result = filter_lab_service.simulate_pnl(
        pair=req.pair,
        start=req.start,
        end=req.end,
        dd_threshold=req.thresholds.drawdown_pct,
        rv_percentile=req.thresholds.rv_percentile,
        vix_percentile=req.thresholds.vix_percentile,
        usd_z_threshold=req.thresholds.usd_z,
        min_conditions=req.thresholds.min_conditions,
    )
    return ApiResponse(data=result, as_of=datetime.utcnow())
