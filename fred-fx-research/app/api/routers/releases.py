from datetime import date, datetime
from typing import Optional
from fastapi import APIRouter
from pydantic import BaseModel
from app.models.api_models import ApiResponse
from app.services.fred_client import FredClient
from app.storage.duckdb import get_connection
from app.core.logging import get_logger

router = APIRouter(prefix="/releases")
logger = get_logger(__name__)


class ReleasesFetchRequest(BaseModel):
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    include_no_data: bool = False


@router.post("/fetch", response_model=ApiResponse)
async def fetch_releases(req: ReleasesFetchRequest) -> ApiResponse:
    async with FredClient() as client:
        resp = await client.get_release_dates(
            start_date=req.start_date,
            end_date=req.end_date,
            include_release_dates_with_no_data=req.include_no_data,
        )

    release_dates = resp.get("release_dates", [])
    now = datetime.utcnow()
    conn = get_connection()

    inserted = 0
    for rd in release_dates:
        try:
            conn.execute(
                """
                INSERT OR REPLACE INTO fact_release_calendar
                (release_id, release_name, release_date, press_release, link, retrieved_at_utc)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    rd.get("release_id"),
                    rd.get("release_name", ""),
                    rd.get("date"),
                    rd.get("press_release"),
                    rd.get("link"),
                    now,
                ),
            )
            inserted += 1
        except Exception:
            pass

    logger.info("releases_fetched", count=inserted)
    return ApiResponse(
        data={"inserted": inserted, "sample": release_dates[:5]},
        as_of=now,
    )


@router.get("/list", response_model=ApiResponse)
async def list_releases(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    limit: int = 50,
) -> ApiResponse:
    conn = get_connection()
    cond = []
    params: list = []
    if start_date:
        cond.append("release_date >= ?")
        params.append(start_date)
    if end_date:
        cond.append("release_date <= ?")
        params.append(end_date)
    where = f"WHERE {' AND '.join(cond)}" if cond else ""
    rows = conn.execute(
        f"SELECT release_id, release_name, release_date FROM fact_release_calendar {where} ORDER BY release_date LIMIT ?",
        params + [limit],
    ).fetchall()
    return ApiResponse(
        data=[{"release_id": r[0], "release_name": r[1], "release_date": str(r[2])} for r in rows],
        as_of=datetime.utcnow(),
    )
