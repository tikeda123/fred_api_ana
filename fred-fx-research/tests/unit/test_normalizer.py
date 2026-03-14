"""
Unit tests: NormalizeService
- quote 方向の正規化
- "." (欠測) → None 変換
- raw は上書きされないこと
"""

import pytest
from datetime import date, datetime

from app.models.domain_models import RawObservation, FX_SERIES_MAP
from app.storage.repositories.observation_repo import bulk_insert
from app.services.normalize_service import normalize_series


def _make_raw_obs(series_id: str, date_str: str, value: str) -> RawObservation:
    return RawObservation(
        series_id=series_id,
        date=date.fromisoformat(date_str),
        value_raw=value,
        value_num=float(value) if value != "." else None,
        realtime_start=date(2026, 3, 14),
        realtime_end=date(2026, 3, 14),
        retrieved_at_utc=datetime.utcnow(),
        file_batch_id="test-batch",
    )


class TestFxQuoteNormalizer:
    """FX series の quote 方向が canonical に正規化されること"""

    @pytest.mark.parametrize("series_id,expected_pair", [
        ("DEXJPUS", "USDJPY"),
        ("DEXUSEU", "EURUSD"),
        ("DEXUSAL", "AUDUSD"),
    ])
    def test_fx_series_map(self, series_id, expected_pair):
        info = FX_SERIES_MAP[series_id]
        assert info["pair"] == expected_pair
        assert info["domain"] == "fx_spot"
        assert info["quote_direction"] == "quote_per_base"

    def test_usdjpy_normalized_correctly(self):
        """DEXJPUS が USDJPY として正規化される"""
        obs = [_make_raw_obs("DEXJPUS", "2026-01-02", "156.72")]
        bulk_insert(obs)

        count = normalize_series("DEXJPUS")
        assert count == 1

        from app.storage.duckdb import get_connection
        conn = get_connection()
        row = conn.execute(
            "SELECT pair, value FROM fact_market_series_normalized WHERE series_id='DEXJPUS' AND value IS NOT NULL"
        ).fetchone()
        assert row is not None
        assert row[0] == "USDJPY"
        assert abs(row[1] - 156.72) < 0.001

    def test_missing_value_becomes_null(self):
        """value_raw='.' が value_num=None に変換される"""
        obs = [
            _make_raw_obs("DEXJPUS", "2026-01-02", "156.72"),
            _make_raw_obs("DEXJPUS", "2026-01-08", "."),
        ]
        bulk_insert(obs)
        normalize_series("DEXJPUS")

        from app.storage.duckdb import get_connection
        conn = get_connection()
        rows = conn.execute(
            "SELECT obs_date, value FROM fact_market_series_normalized WHERE series_id='DEXJPUS' ORDER BY obs_date"
        ).fetchall()
        assert len(rows) == 2
        assert rows[0][1] == pytest.approx(156.72)
        assert rows[1][1] is None   # "." → NULL

    def test_raw_not_overwritten(self):
        """正規化後も raw テーブルの value_raw は変わらない"""
        obs = [_make_raw_obs("DEXJPUS", "2026-01-02", "156.72")]
        bulk_insert(obs)
        normalize_series("DEXJPUS")

        from app.storage.duckdb import get_connection
        conn = get_connection()
        row = conn.execute(
            "SELECT value_raw FROM fact_series_observations_raw WHERE series_id='DEXJPUS' AND value_raw != '.'"
        ).fetchone()
        assert row[0] == "156.72"

    def test_eurusd_domain(self):
        info = FX_SERIES_MAP["DEXUSEU"]
        assert info["base_ccy"] == "EUR"
        assert info["quote_ccy"] == "USD"

    def test_audusd_domain(self):
        info = FX_SERIES_MAP["DEXUSAL"]
        assert info["base_ccy"] == "AUD"
        assert info["quote_ccy"] == "USD"
