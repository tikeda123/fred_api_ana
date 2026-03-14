from datetime import date, datetime
from typing import Optional
from fastapi import APIRouter
from pydantic import BaseModel
from app.models.api_models import ApiResponse
from app.services import panel_service, normalize_service

router = APIRouter(prefix="/panel")


class PanelBuildRequest(BaseModel):
    pair: str
    date_start: date
    date_end: date
    frequency: str = "D"
    vintage_mode: bool = False
    features: Optional[list[str]] = None
    auto_normalize: bool = True  # 未正規化なら自動で normalize する


class PanelBuildResponse(BaseModel):
    pair: str
    row_count: int
    available_features: list[str]
    missing_rates: dict[str, float]
    date_start: date
    date_end: date
    frequency: str
    vintage_mode: bool
    records: list[dict]


@router.post("/build", response_model=ApiResponse)
async def build_panel(req: PanelBuildRequest) -> ApiResponse:
    warnings: list[str] = []

    result = panel_service.get_panel(
        pair=req.pair,
        date_start=req.date_start,
        date_end=req.date_end,
        frequency=req.frequency,
        features=req.features,
        vintage_mode=req.vintage_mode,
    )

    # 欠測率が高いものを warning に追加
    for feat, rate in result["missing_rates"].items():
        if rate > 0.5:
            warnings.append(f"{feat}: missing rate {rate:.0%}")

    resp = PanelBuildResponse(
        pair=result["pair"],
        row_count=result["row_count"],
        available_features=result["available_features"],
        missing_rates=result["missing_rates"],
        date_start=result["date_start"],
        date_end=result["date_end"],
        frequency=result["frequency"],
        vintage_mode=result["vintage_mode"],
        records=panel_service.panel_to_records(result["panel"]),
    )
    return ApiResponse(data=resp, warnings=warnings, as_of=datetime.utcnow())


@router.get("/pairs", response_model=ApiResponse)
async def list_pairs() -> ApiResponse:
    return ApiResponse(
        data=panel_service.get_supported_pairs(),
        as_of=datetime.utcnow(),
    )
