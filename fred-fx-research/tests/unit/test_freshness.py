"""
Unit tests: FreshnessService
- ok / warning / reject 判定ルール
"""

import pytest
from datetime import date, timedelta

from app.models.domain_models import SeriesMetadata
from app.services.freshness_service import audit, _normalize_freq


def _make_meta(
    series_id="DEXJPUS",
    observation_end=None,
    frequency_native="D",
    notes="",
    domain="fx_spot",
) -> SeriesMetadata:
    return SeriesMetadata(
        series_id=series_id,
        title="Test Series",
        frequency_native=frequency_native,
        units_native="USD",
        seasonal_adjustment="NSA",
        observation_start=date(2020, 1, 1),
        observation_end=observation_end or date.today(),
        last_updated=None,
        notes=notes,
        domain=domain,
    )


class TestFreshnessRules:

    def test_recent_daily_is_ok(self):
        """日次で最終観測が昨日 → ok"""
        meta = _make_meta(observation_end=date.today() - timedelta(days=1))
        result = audit(meta)
        assert result["status"] == "ok"

    def test_stale_daily_warning(self):
        """日次で最終観測が 15 日前 → warning"""
        meta = _make_meta(observation_end=date.today() - timedelta(days=15))
        result = audit(meta)
        assert result["status"] == "warning"
        assert any("stale" in r for r in result["reasons"])

    def test_monthly_recent_ok(self):
        """月次で最終観測が 60 日前 → ok (閾値 120 日)"""
        meta = _make_meta(
            observation_end=date.today() - timedelta(days=60),
            frequency_native="Monthly",
        )
        result = audit(meta)
        assert result["status"] == "ok"

    def test_monthly_stale_warning(self):
        """月次で最終観測が 150 日前 → warning"""
        meta = _make_meta(
            observation_end=date.today() - timedelta(days=150),
            frequency_native="Monthly",
        )
        result = audit(meta)
        assert result["status"] == "warning"

    def test_discontinued_is_reject(self):
        """notes に 'discontinued' → reject"""
        meta = _make_meta(notes="This series has been discontinued as of 2019.")
        result = audit(meta)
        assert result["status"] == "reject"
        assert any("discontinued" in r for r in result["reasons"])

    def test_superseded_is_reject(self):
        """notes に 'superseded' → reject"""
        meta = _make_meta(notes="Series superseded by newer release.")
        result = audit(meta)
        assert result["status"] == "reject"

    def test_quarterly_as_primary_daily_warning(self):
        """四半期 series を日次モデルに使うと warning"""
        meta = _make_meta(
            observation_end=date.today() - timedelta(days=5),
            frequency_native="Quarterly",
            domain="macro",
        )
        result = audit(meta)
        assert result["status"] == "warning"
        assert any("low frequency" in r for r in result["reasons"])

    def test_no_observation_end_ok(self):
        """observation_end が None でもエラーにならない"""
        meta = _make_meta(observation_end=None)
        meta.observation_end = None
        result = audit(meta)
        assert "status" in result

    def test_multiple_warnings(self):
        """stale かつ 四半期 → warning, reasons が複数"""
        meta = _make_meta(
            observation_end=date.today() - timedelta(days=300),
            frequency_native="Quarterly",
            domain="macro",
        )
        result = audit(meta)
        assert result["status"] == "warning"
        assert len(result["reasons"]) >= 2


class TestFreqNormalize:

    @pytest.mark.parametrize("raw,expected", [
        ("Daily", "D"),
        ("D", "D"),
        ("Weekly", "W"),
        ("Monthly", "M"),
        ("M", "M"),
        ("Quarterly", "Q"),
        ("Annual", "A"),
        ("", "M"),
        (None, "M"),
    ])
    def test_normalize_freq(self, raw, expected):
        assert _normalize_freq(raw) == expected
