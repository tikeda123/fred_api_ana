from datetime import date, datetime
from typing import Optional
from fastapi import APIRouter
from pydantic import BaseModel
from app.models.api_models import ApiResponse
from app.services import factor_service, panel_service, regime_service

router = APIRouter(prefix="/regimes")


class RegimeTagRequest(BaseModel):
    pair: str
    date_start: date
    date_end: date
    features: Optional[list[str]] = None
    save: bool = True
    include_stats: bool = True


@router.post("/tag", response_model=ApiResponse)
async def tag_regimes(req: RegimeTagRequest) -> ApiResponse:
    # panel 取得
    result = panel_service.get_panel(
        pair=req.pair,
        date_start=req.date_start,
        date_end=req.date_end,
        features=req.features,
    )
    panel = result["panel"]

    # 因子計算
    factors = factor_service.compute_factors(panel, req.pair)

    # regime タグ付け
    regimes = regime_service.tag_regimes(panel, factors, req.pair)

    saved = 0
    if req.save:
        saved = regime_service.save_regimes(regimes, req.pair)

    stats = {}
    if req.include_stats:
        stats = regime_service.compute_regime_stats(panel, regimes, req.pair)

    return ApiResponse(
        data={
            "pair": req.pair,
            "regime_rows": regimes.to_dicts(),
            "row_count": len(regimes),
            "saved": saved,
            "regime_stats": stats,
        },
        as_of=datetime.utcnow(),
    )
