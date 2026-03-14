"""
Unit tests: API response schema の検証
- ApiResponse envelope
- ObservationsFetchResponse
- HealthResponse
"""

import pytest
from datetime import date, datetime
from pydantic import ValidationError

from app.models.api_models import (
    ApiResponse,
    ObservationsFetchResponse,
    ObservationRecord,
    HealthResponse,
    ServiceStatus,
    SeriesSearchResult,
)


class TestApiResponseEnvelope:

    def test_default_status_ok(self):
        r = ApiResponse()
        assert r.status == "ok"
        assert r.warnings == []
        assert r.errors == []
        assert r.timezone == "UTC"

    def test_with_data(self):
        r = ApiResponse(data={"key": "value"})
        assert r.data == {"key": "value"}

    def test_with_warnings(self):
        r = ApiResponse(warnings=["series stale"])
        assert len(r.warnings) == 1

    def test_as_of_is_datetime(self):
        r = ApiResponse()
        assert isinstance(r.as_of, datetime)


class TestObservationsFetchResponse:

    def test_valid_response(self):
        r = ObservationsFetchResponse(
            series_id="DEXJPUS",
            count=3,
            observations=[
                ObservationRecord(
                    date=date(2026, 1, 2),
                    value_raw="156.72",
                    value_num=156.72,
                    realtime_start=date(2026, 3, 14),
                    realtime_end=date(2026, 3, 14),
                )
            ],
            retrieved_at_utc=datetime.utcnow(),
            batch_id="abc-123",
        )
        assert r.series_id == "DEXJPUS"
        assert r.count == 3

    def test_null_value_num_allowed(self):
        """value_num は None (欠測) を許容する"""
        obs = ObservationRecord(
            date=date(2026, 1, 8),
            value_raw=".",
            value_num=None,
            realtime_start=date(2026, 3, 14),
            realtime_end=date(2026, 3, 14),
        )
        assert obs.value_num is None


class TestHealthResponse:

    def test_all_ok(self):
        h = HealthResponse(
            status="ok",
            services=ServiceStatus(duckdb="ok", parquet="ok", fred="ok")
        )
        assert h.status == "ok"

    def test_degraded(self):
        h = HealthResponse(
            status="degraded",
            services=ServiceStatus(duckdb="ok", parquet="ok", fred="degraded")
        )
        assert h.services.fred == "degraded"


class TestSeriesSearchResult:

    def test_minimal(self):
        s = SeriesSearchResult(
            series_id="DEXJPUS",
            title="Japanese Yen to U.S. Dollar",
            frequency="D",
            units="JPY",
            observation_start=None,
            observation_end=None,
            last_updated=None,
            domain="fx_spot",
            freshness_status="ok",
        )
        assert s.series_id == "DEXJPUS"
