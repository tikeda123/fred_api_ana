from datetime import datetime
from fastapi import APIRouter, Query, HTTPException
from app.models.api_models import ApiResponse, SeriesSearchResult
from app.services.fred_client import FredClient
from app.services.registry_service import upsert_from_fred_response
from app.storage.repositories import registry_repo

router = APIRouter(prefix="/series")


@router.get("/search", response_model=ApiResponse)
async def search_series(
    q: str = Query(..., description="検索キーワード"),
    tag_names: str | None = Query(None),
    exclude_tag_names: str | None = Query(None),
    limit: int = Query(50, ge=1, le=200),
    refresh_from_fred: bool = Query(False),
) -> ApiResponse:
    # ローカル registry を先に検索
    if not refresh_from_fred:
        local = registry_repo.search(q, limit)
        if local:
            return ApiResponse(
                data=[_meta_to_result(m) for m in local],
                as_of=datetime.utcnow(),
            )

    # FRED から取得して registry に登録
    async with FredClient() as client:
        resp = await client.search_series(
            search_text=q,
            tag_names=tag_names,
            exclude_tag_names=exclude_tag_names,
            limit=limit,
        )
    seriess = resp.get("seriess", [])
    metas = upsert_from_fred_response(seriess)
    return ApiResponse(
        data=[_meta_to_result(m) for m in metas],
        as_of=datetime.utcnow(),
    )


@router.get("/{series_id}", response_model=ApiResponse)
async def get_series_metadata(series_id: str) -> ApiResponse:
    meta = registry_repo.get(series_id)
    if meta:
        return ApiResponse(data=_meta_to_result(meta), as_of=datetime.utcnow())

    # FRED から取得
    async with FredClient() as client:
        resp = await client.get_series_metadata(series_id)
    seriess = resp.get("seriess", [])
    if not seriess:
        raise HTTPException(status_code=404, detail=f"Series not found: {series_id}")
    metas = upsert_from_fred_response(seriess)
    return ApiResponse(data=_meta_to_result(metas[0]), as_of=datetime.utcnow())


def _meta_to_result(m) -> SeriesSearchResult:
    return SeriesSearchResult(
        series_id=m.series_id,
        title=m.title,
        frequency=m.frequency_native,
        units=m.units_native,
        observation_start=m.observation_start,
        observation_end=m.observation_end,
        last_updated=m.last_updated,
        domain=m.domain,
        freshness_status=m.freshness_status,
    )
