"""
E2E tests: FastAPI エンドポイント
- TestClient を使いサーバー起動不要
- FRED 通信は respx でモック
"""

import pytest
import respx
import httpx
from fastapi.testclient import TestClient
from datetime import date

from app.api.main import app


@pytest.fixture
def client():
    with TestClient(app) as c:
        yield c


@pytest.fixture
def mock_fred_full(fred_series_response, fred_observations_response):
    with respx.mock(base_url="https://api.stlouisfed.org/fred", assert_all_called=False) as mock:
        mock.get("/series").mock(return_value=httpx.Response(200, json=fred_series_response))
        mock.get("/series/observations").mock(return_value=httpx.Response(200, json=fred_observations_response))
        mock.get("/series/search").mock(return_value=httpx.Response(200, json={"seriess": fred_series_response["seriess"]}))
        yield mock


class TestHealthEndpoint:

    def test_health_ok(self, client):
        r = client.get("/health")
        assert r.status_code == 200
        data = r.json()
        assert data["status"] in ("ok", "degraded")
        assert "duckdb" in data["services"]


class TestObservationsEndpoint:

    def test_fetch_observations(self, client, mock_fred_full):
        r = client.post("/api/v1/observations/fetch", json={
            "series_id": "DEXJPUS",
            "observation_start": "2026-01-01",
            "observation_end": "2026-03-14",
            "store_raw": True,
        })
        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "ok"
        assert body["data"]["series_id"] == "DEXJPUS"
        assert body["data"]["count"] == 6

    def test_response_has_envelope(self, client, mock_fred_full):
        r = client.post("/api/v1/observations/fetch", json={
            "series_id": "DEXJPUS",
            "store_raw": True,
        })
        body = r.json()
        assert "status" in body
        assert "as_of" in body
        assert "warnings" in body
        assert "errors" in body


class TestSeriesEndpoint:

    def test_search_from_fred(self, client, mock_fred_full):
        r = client.get("/api/v1/series/search?q=Japanese+Yen&refresh_from_fred=true")
        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "ok"
        assert len(body["data"]) >= 1
        assert body["data"][0]["series_id"] == "DEXJPUS"

    def test_get_series_metadata(self, client, mock_fred_full):
        r = client.get("/api/v1/series/DEXJPUS")
        assert r.status_code == 200
        body = r.json()
        assert body["data"]["series_id"] == "DEXJPUS"
        assert body["data"]["domain"] == "fx_spot"


class TestPanelEndpoint:

    def test_panel_build_empty(self, client):
        """データなしでも 200 を返す (空パネル)"""
        r = client.post("/api/v1/panel/build", json={
            "pair": "USDJPY",
            "date_start": "2026-01-01",
            "date_end": "2026-03-14",
            "features": ["spot"],
        })
        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "ok"
        assert "row_count" in body["data"]

    def test_panel_build_with_data(self, client, mock_fred_full):
        """ingest 後にパネルを構築できる"""
        # 先に ingest
        client.post("/api/v1/observations/fetch", json={
            "series_id": "DEXJPUS", "store_raw": True, "normalize": False
        })
        from app.services.normalize_service import normalize_series
        normalize_series("DEXJPUS")

        r = client.post("/api/v1/panel/build", json={
            "pair": "USDJPY",
            "date_start": "2026-01-01",
            "date_end": "2026-01-31",
            "features": ["spot"],
        })
        assert r.status_code == 200
        data = r.json()["data"]
        assert data["row_count"] > 0


class TestMemoEndpoint:

    def test_generate_memo(self, client):
        r = client.post("/api/v1/memo/generate", json={
            "pair": "USDJPY",
            "date_start": "2023-01-01",
            "date_end": "2026-03-14",
            "research_question": "金利差は有効か？",
            "hypothesis": "3M 金利差で説明できる。",
            "observations": "carry_positive が継続。",
            "selected_factors": ["spot", "us_3m", "jp_3m"],
        })
        assert r.status_code == 200
        body = r.json()
        memo = body["data"]
        assert "memo_id" in memo
        assert memo["pair"] == "USDJPY"
        assert "as_of" in memo
        assert "sources" in memo
        assert "missing_evidence" in memo
        assert memo["vintage_mode"] is False

    def test_list_memos(self, client):
        # 1件生成してから一覧取得
        client.post("/api/v1/memo/generate", json={
            "pair": "EURUSD",
            "date_start": "2023-01-01",
            "date_end": "2026-03-14",
            "research_question": "ECB 利上げの影響",
            "hypothesis": "ECB との金利差が縮小するとユーロ安。",
            "observations": "2024 年に収束傾向。",
        })
        r = client.get("/api/v1/memo/list")
        assert r.status_code == 200
        memos = r.json()["data"]
        assert len(memos) >= 1
