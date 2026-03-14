"""
Integration tests: ingest → normalize → panel build
FRED は respx でモック。実際のネットワーク通信なし。
"""

import pytest
import respx
import httpx
import json
from datetime import date, datetime

from app.services.ingest_service import fetch_and_store
from app.services.normalize_service import normalize_series
from app.storage.duckdb import get_connection


FRED_BASE = "https://api.stlouisfed.org/fred"


@pytest.fixture
def mock_fred(fred_series_response, fred_observations_response):
    """FRED API エンドポイントをモック"""
    with respx.mock(base_url=FRED_BASE, assert_all_called=False) as mock:
        mock.get("/series").mock(return_value=httpx.Response(
            200, json=fred_series_response
        ))
        mock.get("/series/observations").mock(return_value=httpx.Response(
            200, json=fred_observations_response
        ))
        yield mock


class TestIngestPipeline:

    @pytest.mark.asyncio
    async def test_fetch_stores_raw_observations(self, mock_fred):
        """FRED から取得した observations が DuckDB に保存される"""
        result = await fetch_and_store("DEXJPUS", store_raw=True)
        assert result.series_id == "DEXJPUS"
        assert result.count == 6

        conn = get_connection()
        rows = conn.execute(
            "SELECT COUNT(*) FROM fact_series_observations_raw WHERE series_id='DEXJPUS'"
        ).fetchone()
        assert rows[0] == 6

    @pytest.mark.asyncio
    async def test_fetch_stores_registry_metadata(self, mock_fred):
        """series metadata が dim_series_registry に登録される"""
        await fetch_and_store("DEXJPUS")
        conn = get_connection()
        row = conn.execute(
            "SELECT title, domain, pair FROM dim_series_registry WHERE series_id='DEXJPUS'"
        ).fetchone()
        assert row is not None
        assert "Yen" in row[0]
        assert row[1] == "fx_spot"
        assert row[2] == "USDJPY"

    @pytest.mark.asyncio
    async def test_missing_dot_stored_as_null(self, mock_fred):
        """value='.' が fact_series_observations_raw では raw 保存、value_num=NULL"""
        await fetch_and_store("DEXJPUS")
        conn = get_connection()
        row = conn.execute(
            "SELECT value_raw, value_num FROM fact_series_observations_raw "
            "WHERE series_id='DEXJPUS' AND value_raw='.'"
        ).fetchone()
        assert row is not None
        assert row[0] == "."
        assert row[1] is None

    @pytest.mark.asyncio
    async def test_ingest_then_normalize(self, mock_fred):
        """ingest → normalize で fact_market_series_normalized に保存される"""
        await fetch_and_store("DEXJPUS")
        count = normalize_series("DEXJPUS")
        assert count == 6

        conn = get_connection()
        rows = conn.execute(
            "SELECT obs_date, pair, value FROM fact_market_series_normalized "
            "WHERE series_id='DEXJPUS' ORDER BY obs_date"
        ).fetchall()
        assert len(rows) == 6
        # 最初の有効値
        assert rows[0][1] == "USDJPY"
        assert abs(rows[0][2] - 156.72) < 0.001
        # "." → NULL
        null_rows = [r for r in rows if r[2] is None]
        assert len(null_rows) == 1

    @pytest.mark.asyncio
    async def test_audit_log_recorded(self, mock_fred):
        """ingestion 監査ログが audit_ingestion_runs に記録される"""
        await fetch_and_store("DEXJPUS")
        conn = get_connection()
        row = conn.execute(
            "SELECT status, record_count FROM audit_ingestion_runs WHERE endpoint='series/observations'"
        ).fetchone()
        assert row is not None
        assert row[0] == "ok"
        assert row[1] == 6

    @pytest.mark.asyncio
    async def test_idempotent_ingest(self, mock_fred):
        """同じ series を 2 回 ingest しても重複しない (INSERT OR REPLACE)"""
        await fetch_and_store("DEXJPUS")
        await fetch_and_store("DEXJPUS")
        conn = get_connection()
        row = conn.execute(
            "SELECT COUNT(*) FROM fact_series_observations_raw WHERE series_id='DEXJPUS'"
        ).fetchone()
        assert row[0] == 6  # 重複しない
