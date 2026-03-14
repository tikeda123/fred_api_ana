from datetime import datetime
from fastapi import APIRouter
from app.models.api_models import ApiResponse, ObservationsFetchRequest
from app.services import ingest_service

router = APIRouter(prefix="/observations")


@router.post("/fetch", response_model=ApiResponse)
async def fetch_observations(req: ObservationsFetchRequest) -> ApiResponse:
    result = await ingest_service.fetch_and_store(
        series_id=req.series_id,
        observation_start=req.observation_start,
        observation_end=req.observation_end,
        units=req.units,
        frequency=req.frequency,
        aggregation_method=req.aggregation_method,
        realtime_start=req.realtime_start,
        realtime_end=req.realtime_end,
        vintage_dates=req.vintage_dates,
        store_raw=req.store_raw,
        normalize=req.normalize,
    )
    return ApiResponse(data=result, as_of=datetime.utcnow())
