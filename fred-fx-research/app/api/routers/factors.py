from datetime import date, datetime
from typing import Optional
from fastapi import APIRouter, Query
from pydantic import BaseModel
from app.models.api_models import ApiResponse
from app.services import factor_service, panel_service

router = APIRouter(prefix="/factors")


class FactorsComputeRequest(BaseModel):
    pair: str
    date_start: date
    date_end: date
    features: Optional[list[str]] = None
    save: bool = True


@router.post("/compute", response_model=ApiResponse)
async def compute_factors(req: FactorsComputeRequest) -> ApiResponse:
    # panel を取得
    result = panel_service.get_panel(
        pair=req.pair,
        date_start=req.date_start,
        date_end=req.date_end,
        features=req.features,
    )
    panel = result["panel"]

    # マクロ因子計算
    factors = factor_service.compute_factors(panel, req.pair)

    # OHLC 因子を計算して統合
    ohlc_factors = factor_service.compute_ohlc_factors(
        req.pair, req.date_start, req.date_end
    )
    if not ohlc_factors.is_empty() and not factors.is_empty():
        factors = factors.join(ohlc_factors, on="obs_date", how="left")
    elif ohlc_factors.is_empty():
        pass  # マクロ因子のみ
    else:
        factors = ohlc_factors  # OHLC のみ（通常は発生しない）

    saved = 0
    if req.save:
        saved = factor_service.save_factors(factors, req.pair)

    factor_cols = [c for c in factors.columns if c != "obs_date"]
    return ApiResponse(
        data={
            "pair": req.pair,
            "factors": factors.to_dicts(),
            "factor_names": factor_cols,
            "row_count": len(factors),
            "saved": saved,
        },
        as_of=datetime.utcnow(),
    )


@router.get("/load", response_model=ApiResponse)
async def load_factors(
    pair: str,
    date_start: Optional[date] = None,
    date_end: Optional[date] = None,
) -> ApiResponse:
    df = factor_service.load_factors(pair, date_start, date_end)
    return ApiResponse(
        data={
            "pair": pair,
            "factors": df.to_dicts() if not df.is_empty() else [],
            "row_count": len(df),
        },
        as_of=datetime.utcnow(),
    )


@router.get("/ohlc-derived", response_model=ApiResponse)
async def get_ohlc_derived(
    pair: str = Query(...),
    start: Optional[date] = Query(None),
    end: Optional[date] = Query(None),
) -> ApiResponse:
    df = factor_service.compute_ohlc_factors(pair, start, end)
    records = df.to_dicts() if not df.is_empty() else []
    return ApiResponse(
        data={
            "pair": pair,
            "factors": records,
            "factor_names": ["parkinson_vol_20d", "daily_range_pct", "daily_range_ma20", "overnight_gap"],
            "row_count": len(records),
        },
        as_of=datetime.utcnow(),
    )
