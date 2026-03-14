from fastapi import APIRouter
from app.models.api_models import HealthResponse, ServiceStatus
from app.storage.duckdb import check_health as duckdb_health
from pathlib import Path
from app.core.config import settings

router = APIRouter()


@router.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    db_status = duckdb_health()
    parquet_status = "ok" if Path(settings.raw_data_root).exists() else "degraded"
    return HealthResponse(
        status="ok" if db_status == "ok" else "degraded",
        services=ServiceStatus(
            duckdb=db_status,
            parquet=parquet_status,
            fred="ok",
        ),
    )
