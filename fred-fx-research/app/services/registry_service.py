"""
Series registry 管理
- FRED メタデータを dim_series_registry に upsert
- domain / quote_direction / freshness_status を付与
"""

from datetime import date, datetime
from typing import Optional

from app.core.logging import get_logger
from app.models.domain_models import FX_SERIES_MAP, SeriesMetadata
from app.storage.repositories import registry_repo

logger = get_logger(__name__)


def upsert_from_fred_response(fred_seriess: list[dict]) -> list[SeriesMetadata]:
    """FRED series/search または series の response から registry に登録"""
    results = []
    for s in fred_seriess:
        meta = _parse_metadata(s)
        registry_repo.upsert(meta)
        results.append(meta)
    return results


def _parse_metadata(s: dict) -> SeriesMetadata:
    series_id = s.get("id", "")
    fx_info = FX_SERIES_MAP.get(series_id, {})

    obs_start = _parse_date(s.get("observation_start"))
    obs_end = _parse_date(s.get("observation_end"))
    last_updated = _parse_datetime(s.get("last_updated"))

    return SeriesMetadata(
        series_id=series_id,
        title=s.get("title", ""),
        source_name=s.get("source", None),
        release_id=s.get("release_id", None),
        release_name=s.get("release", None),
        units_native=s.get("units", ""),
        frequency_native=s.get("frequency_short", s.get("frequency", "")),
        seasonal_adjustment=s.get("seasonal_adjustment_short", ""),
        observation_start=obs_start,
        observation_end=obs_end,
        last_updated=last_updated,
        notes=s.get("notes", None),
        domain=fx_info.get("domain"),
        base_ccy=fx_info.get("base_ccy"),
        quote_ccy=fx_info.get("quote_ccy"),
        pair=fx_info.get("pair"),
        quote_direction=fx_info.get("quote_direction"),
        freshness_status="unknown",
    )


def _parse_date(s: Optional[str]) -> Optional[date]:
    if not s:
        return None
    try:
        return date.fromisoformat(s)
    except ValueError:
        return None


def _parse_datetime(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        # FRED returns "2026-03-14 12:00:00-05"
        return datetime.fromisoformat(s.replace(" ", "T"))
    except ValueError:
        return None
