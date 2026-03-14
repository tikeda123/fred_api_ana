"""
Unit tests: FactorService
- 計算式の正確性
- 欠損値の扱い
- pair 別 factor 定義
- OHLC ファクター計算
"""

import math
from unittest.mock import patch

import pytest
import polars as pl
from datetime import date


class TestFactorFormulas:

    def _make_panel(self) -> pl.DataFrame:
        """テスト用 panel DataFrame (10行)"""
        return pl.DataFrame({
            "obs_date": [date(2026, 1, i + 1) for i in range(10)],
            "spot":      [150.0, 151.0, 152.0, 151.5, 153.0, 152.0, 154.0, 153.5, 155.0, 156.0],
            "us_3m":     [5.0] * 10,
            "jp_3m":     [0.1] * 10,
            "us_10y":    [4.5] * 10,
            "jp_10y":    [1.5] * 10,
            "vix":       [15.0, 16.0, 14.0, 13.0, 18.0, 20.0, 19.0, 17.0, 16.0, 15.5],
            "usd_broad": [100.0, 100.5, 101.0, 100.8, 101.5, 102.0, 101.8, 102.5, 103.0, 103.2],
        })

    def test_rate_spread_usdjpy(self):
        """us_jp_3m_spread = us_3m - jp_3m"""
        from app.services.factor_service import compute_factors
        panel = self._make_panel()
        factors = compute_factors(panel, "USDJPY")
        assert "us_jp_3m_spread" in factors.columns
        spreads = factors["us_jp_3m_spread"].drop_nulls().to_list()
        assert all(abs(s - (5.0 - 0.1)) < 0.001 for s in spreads)

    def test_yield_spread_usdjpy(self):
        """us_jp_10y_spread = us_10y - jp_10y"""
        from app.services.factor_service import compute_factors
        panel = self._make_panel()
        factors = compute_factors(panel, "USDJPY")
        assert "us_jp_10y_spread" in factors.columns
        spreads = factors["us_jp_10y_spread"].drop_nulls().to_list()
        assert all(abs(s - (4.5 - 1.5)) < 0.001 for s in spreads)

    def test_us_curve_slope(self):
        """us_curve = us_10y - us_3m = 4.5 - 5.0 = -0.5"""
        from app.services.factor_service import compute_factors
        panel = self._make_panel()
        factors = compute_factors(panel, "USDJPY")
        assert "us_curve" in factors.columns
        curves = factors["us_curve"].drop_nulls().to_list()
        assert all(abs(c - (-0.5)) < 0.001 for c in curves)

    def test_spot_return_1d(self):
        """spot_return_1d = pct_change(1)"""
        from app.services.factor_service import compute_factors
        panel = self._make_panel()
        factors = compute_factors(panel, "USDJPY")
        assert "spot_return_1d" in factors.columns
        # 2行目: (151-150)/150 ≈ 0.00667
        val = factors["spot_return_1d"][1]
        assert abs(val - (151.0 - 150.0) / 150.0) < 1e-6

    def test_realized_vol_not_nan_after_window(self):
        """realized_vol_20d は window 分の後から non-null になる"""
        from app.services.factor_service import compute_factors
        # 25行分のデータ
        n = 25
        panel = pl.DataFrame({
            "obs_date": [date(2026, 1, 1)] * n,
            "spot":     [150.0 + i * 0.1 for i in range(n)],
            "vix":      [15.0] * n,
            "usd_broad":[100.0] * n,
        })
        factors = compute_factors(panel, "USDJPY")
        assert "realized_vol_20d" in factors.columns
        non_null = factors["realized_vol_20d"].drop_nulls()
        assert len(non_null) > 0

    def test_vix_change_1d(self):
        """vix_change_1d = vix.diff(1)"""
        from app.services.factor_service import compute_factors
        panel = self._make_panel()
        factors = compute_factors(panel, "USDJPY")
        assert "vix_change_1d" in factors.columns
        # 2行目: 16.0 - 15.0 = 1.0
        val = factors["vix_change_1d"][1]
        assert abs(val - 1.0) < 1e-6

    def test_eurusd_factors(self):
        """EURUSD 用の因子が計算される"""
        from app.services.factor_service import compute_factors, FACTOR_SPEC
        panel = pl.DataFrame({
            "obs_date": [date(2026, 1, i + 1) for i in range(10)],
            "spot":     [1.05] * 10,
            "us_3m":    [5.0] * 10,
            "ez_3m":    [3.0] * 10,
            "us_10y":   [4.5] * 10,
            "ez_10y":   [2.5] * 10,
            "vix":      [15.0] * 10,
            "usd_broad":[100.0] * 10,
        })
        factors = compute_factors(panel, "EURUSD")
        assert "us_ez_3m_spread" in factors.columns
        assert "us_ez_10y_spread" in factors.columns
        val = factors["us_ez_3m_spread"][0]
        assert abs(val - 2.0) < 0.001

    def test_missing_factor_col_skipped(self):
        """panel に必要な列がない場合はその因子をスキップする"""
        from app.services.factor_service import compute_factors
        # vix, usd_broad なし
        panel = pl.DataFrame({
            "obs_date": [date(2026, 1, i + 1) for i in range(5)],
            "spot":     [150.0 + i for i in range(5)],
            "us_3m":    [5.0] * 5,
            "jp_3m":    [0.1] * 5,
        })
        factors = compute_factors(panel, "USDJPY")
        # エラーにならない
        assert "us_jp_3m_spread" in factors.columns
        assert "vix_change_1d" not in factors.columns


class TestOhlcFactorFormulas:
    """OHLC ベースのファクター計算テスト"""

    def _make_ohlc(self, n: int = 25) -> pl.DataFrame:
        """テスト用 OHLC DataFrame"""
        return pl.DataFrame({
            "obs_date": [date(2026, 1, i + 1) for i in range(n)],
            "open":  [150.0 + i * 0.5 for i in range(n)],
            "high":  [151.0 + i * 0.5 for i in range(n)],
            "low":   [149.0 + i * 0.5 for i in range(n)],
            "close": [150.5 + i * 0.5 for i in range(n)],
            "volume": [10000] * n,
        })

    @patch("app.services.csv_loader_service.get_daily_ohlc")
    def test_parkinson_vol(self, mock_ohlc):
        """Parkinson vol: 既知の H/L 値で手計算と比較"""
        from app.services.factor_service import compute_ohlc_factors

        # 全行で H=151, L=149 の固定値 OHLC (25行)
        n = 25
        ohlc = pl.DataFrame({
            "obs_date": [date(2026, 1, i + 1) for i in range(n)],
            "open":  [150.0] * n,
            "high":  [151.0] * n,
            "low":   [149.0] * n,
            "close": [150.5] * n,
            "volume": [10000] * n,
        })
        mock_ohlc.return_value = ohlc

        result = compute_ohlc_factors("USDJPY")
        assert "parkinson_vol_20d" in result.columns

        # 手計算: ln(151/149)^2 ≈ 0.0001779, / (4*ln2) ≈ 0.00006416, sqrt ≈ 0.008010
        hl_log_sq = math.log(151.0 / 149.0) ** 2
        expected = math.sqrt(hl_log_sq / (4 * math.log(2)))

        # 20行目以降は non-null
        vals = result["parkinson_vol_20d"].drop_nulls().to_list()
        assert len(vals) > 0
        assert abs(vals[0] - expected) < 1e-6

    @patch("app.services.csv_loader_service.get_daily_ohlc")
    def test_daily_range_pct(self, mock_ohlc):
        """daily_range_pct = (high - low) / open * 100"""
        from app.services.factor_service import compute_ohlc_factors

        ohlc = self._make_ohlc(5)
        mock_ohlc.return_value = ohlc

        result = compute_ohlc_factors("USDJPY")
        assert "daily_range_pct" in result.columns

        # 1行目: (151 - 149) / 150 * 100 = 1.3333...
        val = result["daily_range_pct"][0]
        expected = (151.0 - 149.0) / 150.0 * 100
        assert abs(val - expected) < 1e-4

    @patch("app.services.csv_loader_service.get_daily_ohlc")
    def test_overnight_gap(self, mock_ohlc):
        """overnight_gap: 初日は null、2行目以降は open/prev_close - 1"""
        from app.services.factor_service import compute_ohlc_factors

        ohlc = self._make_ohlc(5)
        mock_ohlc.return_value = ohlc

        result = compute_ohlc_factors("USDJPY")
        assert "overnight_gap" in result.columns

        # 初日は null
        assert result["overnight_gap"][0] is None

        # 2行目: open[1] / close[0] - 1 = 150.5 / 150.5 - 1 = 0
        val = result["overnight_gap"][1]
        expected = 150.5 / 150.5 - 1
        assert abs(val - expected) < 1e-6

    @patch("app.services.csv_loader_service.get_daily_ohlc")
    def test_empty_ohlc_graceful(self, mock_ohlc):
        """OHLC データが空の場合は空 DataFrame を返す"""
        from app.services.factor_service import compute_ohlc_factors

        mock_ohlc.return_value = pl.DataFrame()
        result = compute_ohlc_factors("USDJPY")
        assert result.is_empty()
