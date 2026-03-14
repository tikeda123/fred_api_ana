from datetime import date, datetime
from typing import Optional
from fastapi import APIRouter, HTTPException
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel
from app.models.api_models import ApiResponse
from app.services import memo_service

router = APIRouter(prefix="/memo")


class MemoGenerateRequest(BaseModel):
    pair: str
    date_start: date
    date_end: date
    research_question: str
    hypothesis: str
    observations: str
    uncertainty: str = ""
    next_experiment: str = ""
    risk_controls: str = ""
    selected_factors: list[str] = []
    vintage_mode: bool = False
    regime_summary: Optional[dict] = None
    missing_rates: Optional[dict[str, float]] = None


@router.post("/generate", response_model=ApiResponse)
async def generate_memo(req: MemoGenerateRequest) -> ApiResponse:
    memo = memo_service.generate_memo(
        pair=req.pair,
        date_start=req.date_start,
        date_end=req.date_end,
        research_question=req.research_question,
        hypothesis=req.hypothesis,
        observations=req.observations,
        uncertainty=req.uncertainty,
        next_experiment=req.next_experiment,
        risk_controls=req.risk_controls,
        selected_factors=req.selected_factors,
        vintage_mode=req.vintage_mode,
        regime_summary=req.regime_summary,
        missing_rates=req.missing_rates,
    )
    return ApiResponse(data=memo, as_of=datetime.utcnow())


@router.get("/list", response_model=ApiResponse)
async def list_memos() -> ApiResponse:
    return ApiResponse(data=memo_service.list_memos(), as_of=datetime.utcnow())


@router.get("/{memo_id}/markdown", response_class=PlainTextResponse)
async def get_memo_markdown(memo_id: str) -> str:
    memo = memo_service.get_memo(memo_id)
    if not memo:
        raise HTTPException(status_code=404, detail="Memo not found")
    return memo_service.memo_to_markdown(memo)


@router.get("/{memo_id}", response_model=ApiResponse)
async def get_memo(memo_id: str) -> ApiResponse:
    memo = memo_service.get_memo(memo_id)
    if not memo:
        raise HTTPException(status_code=404, detail="Memo not found")
    return ApiResponse(data=memo, as_of=datetime.utcnow())
