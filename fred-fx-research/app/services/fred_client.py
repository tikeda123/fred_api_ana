"""
FRED v1 API クライアント
- httpx async
- 2 req/sec レート制限
- 429/5xx 指数バックオフ
- API key はログにマスク
"""

import asyncio
import time
from datetime import date
from typing import Any, Optional

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from app.core.config import settings
from app.core.exceptions import FredApiError, FredRateLimitError, FredSeriesNotFoundError
from app.core.logging import get_logger

logger = get_logger(__name__)

_RETRY_STATUS = {429, 500, 502, 503, 504}


class FredClient:
    """FRED v1 REST API クライアント"""

    def __init__(self) -> None:
        self._api_key = settings.fred_api_key
        self._base_url = settings.fred_base_url
        self._min_interval = 1.0 / settings.fred_max_rps
        self._last_request_time: float = 0.0
        self._lock = asyncio.Lock()
        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self) -> "FredClient":
        self._client = httpx.AsyncClient(
            timeout=30.0,
            headers={"User-Agent": "fred-fx-research/0.1"},
        )
        return self

    async def __aexit__(self, *_: Any) -> None:
        if self._client:
            await self._client.aclose()

    def _mask_key(self, params: dict) -> dict:
        """ログ用に api_key をマスク"""
        return {k: ("***" if k == "api_key" else v) for k, v in params.items()}

    async def _throttle(self) -> None:
        """2 req/sec を超えないよう待機"""
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_request_time
            if elapsed < self._min_interval:
                await asyncio.sleep(self._min_interval - elapsed)
            self._last_request_time = time.monotonic()

    async def _get(self, path: str, params: dict) -> dict:
        params = {"api_key": self._api_key, "file_type": "json", **params}
        url = f"{self._base_url}/{path}"

        await self._throttle()

        logger.debug("fred_request", path=path, params=self._mask_key(params))

        @retry(
            retry=retry_if_exception_type((FredRateLimitError, FredApiError)),
            stop=stop_after_attempt(settings.fred_retry_max_attempts),
            wait=wait_exponential(
                min=settings.fred_retry_wait_min,
                max=settings.fred_retry_wait_max,
            ),
            reraise=True,
        )
        async def _do_request() -> dict:
            assert self._client is not None
            resp = await self._client.get(url, params=params)
            if resp.status_code == 404:
                raise FredSeriesNotFoundError("Series not found", status_code=404)
            if resp.status_code == 429:
                raise FredRateLimitError("Rate limited", status_code=429)
            if resp.status_code in _RETRY_STATUS:
                raise FredApiError(
                    f"FRED error {resp.status_code}", status_code=resp.status_code
                )
            resp.raise_for_status()
            return resp.json()

        return await _do_request()

    async def search_series(
        self,
        search_text: str,
        tag_names: Optional[str] = None,
        exclude_tag_names: Optional[str] = None,
        order_by: str = "search_rank",
        limit: int = 50,
    ) -> dict:
        params: dict = {"search_text": search_text, "order_by": order_by, "limit": limit}
        if tag_names:
            params["tag_names"] = tag_names
        if exclude_tag_names:
            params["exclude_tag_names"] = exclude_tag_names
        return await self._get("series/search", params)

    async def get_series_metadata(self, series_id: str) -> dict:
        return await self._get("series", {"series_id": series_id})

    async def get_observations(
        self,
        series_id: str,
        observation_start: Optional[date] = None,
        observation_end: Optional[date] = None,
        units: str = "lin",
        frequency: Optional[str] = None,
        aggregation_method: str = "eop",
        realtime_start: Optional[date] = None,
        realtime_end: Optional[date] = None,
        vintage_dates: Optional[list[date]] = None,
        output_type: int = 1,
    ) -> dict:
        params: dict = {
            "series_id": series_id,
            "units": units,
            "aggregation_method": aggregation_method,
            "output_type": output_type,
        }
        if observation_start:
            params["observation_start"] = observation_start.isoformat()
        if observation_end:
            params["observation_end"] = observation_end.isoformat()
        if frequency:
            params["frequency"] = frequency
        if realtime_start:
            params["realtime_start"] = realtime_start.isoformat()
        if realtime_end:
            params["realtime_end"] = realtime_end.isoformat()
        if vintage_dates:
            params["vintage_dates"] = ",".join(d.isoformat() for d in vintage_dates)
        return await self._get("series/observations", params)

    async def get_vintagedates(
        self,
        series_id: str,
        realtime_start: Optional[date] = None,
        realtime_end: Optional[date] = None,
    ) -> dict:
        params: dict = {"series_id": series_id}
        if realtime_start:
            params["realtime_start"] = realtime_start.isoformat()
        if realtime_end:
            params["realtime_end"] = realtime_end.isoformat()
        return await self._get("series/vintagedates", params)

    async def get_release_dates(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        include_release_dates_with_no_data: bool = False,
    ) -> dict:
        params: dict = {
            "include_release_dates_with_no_data": str(include_release_dates_with_no_data).lower()
        }
        if start_date:
            params["realtime_start"] = start_date.isoformat()
        if end_date:
            params["realtime_end"] = end_date.isoformat()
        return await self._get("releases/dates", params)
