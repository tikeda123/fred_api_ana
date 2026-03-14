"""
Unit tests: CrossAssetFeatureService
- ret_1d, mom_5d, rv_20d, drawdown_20d 計算の正確性
- corr_20d, beta_60d, lagcorr 計算
- risk_filter_flag ルール
- 空データの graceful フォールバック
"""

import math
from datetime import date, timedelta
from unittest.mock import patch

import polars as pl
import pytest

from app.services.cross_asset_feature_service import (
    _compute_instrument_features,
    _compute_pair_features,
    _apply_risk_filter,
    INSTRUMENT_FEATURES,
    PAIR_FEATURES,
)


# ── テスト用データ生成 ─────────────────────────────────────────────

def _make_daily_df(n: int = 30, start_close: float = 100.0, step: float = 1.0) -> pl.DataFrame:
    """単調増加する close を持つ n 日分の daily DataFrame"""
    closes = [start_close + i * step for i in range(n)]
    dates = [date(2024, 1, 1) + timedelta(days=i) for i in range(n)]
    return pl.DataFrame({
        "obs_date": dates,
        "open":     closes,
        "high":     [c + 1.0 for c in closes],
        "low":      [c - 1.0 for c in closes],
        "close":    closes,
        "volume":   [1000.0] * n,
        "gap_from_prev_close_pct": [0.0] + [step / c for c in closes[:-1]],
    })


def _make_pair_df(n: int = 65) -> pl.DataFrame:
    """USATECH ret と FX ret を持つ結合済み DataFrame"""
    import random
    random.seed(42)
    dates = [date(2024, 1, 1) + timedelta(days=i) for i in range(n)]
    usatech_ret = [random.gauss(0.0005, 0.01) for _ in range(n)]
    fx_ret      = [0.3 * ur + random.gauss(0, 0.005) for ur in usatech_ret]
    usatech_dd  = [-0.02] * n
    usatech_rv  = [0.10] * n
    vix         = [20.0] * n
    usd_broad   = [100.0] * n
    return pl.DataFrame({
        "obs_date":     dates,
        "usatech_ret":  usatech_ret,
        "fx_ret":       fx_ret,
        "vix":          vix,
        "usd_broad":    usd_broad,
        "_usatech_dd20": usatech_dd,
        "_usatech_rv20": usatech_rv,
    })


# ── instrument-level 特徴量 ────────────────────────────────────────

class TestInstrumentFeatures:

    def test_ret_1d_first_null(self):
        df = _make_daily_df(10)
        result = _compute_instrument_features(df, "usatechidxusd_h4")
        assert result["ret_1d"][0] is None

    def test_ret_1d_value(self):
        df = _make_daily_df(10, start_close=100.0, step=1.0)
        result = _compute_instrument_features(df, "usatechidxusd_h4")
        # row 1: (101 - 100) / 100 = 0.01
        assert result["ret_1d"][1] == pytest.approx(0.01, rel=1e-6)

    def test_mom_5d(self):
        df = _make_daily_df(10, start_close=100.0, step=1.0)
        result = _compute_instrument_features(df, "usatechidxusd_h4")
        # row 5: close=105, close_shift5=100 → 5/100 = 0.05
        assert result["mom_5d"][5] == pytest.approx(0.05, rel=1e-6)

    def test_mom_5d_first_5_null(self):
        df = _make_daily_df(10)
        result = _compute_instrument_features(df, "usatechidxusd_h4")
        assert all(result["mom_5d"][i] is None for i in range(5))

    def test_rv_20d_annualized(self):
        """rv_20d は ret の rolling std × sqrt(252)"""
        df = _make_daily_df(25, start_close=100.0, step=0.0)
        # 全 close 同値 → ret=0 → std=0 → rv=0
        result = _compute_instrument_features(df, "usatechidxusd_h4")
        # 20 日分揃った先は rv_20d = 0
        assert result["rv_20d"][24] == pytest.approx(0.0, abs=1e-10)

    def test_drawdown_20d_flat(self):
        """単調増加なら drawdown_20d = 0"""
        df = _make_daily_df(25)
        result = _compute_instrument_features(df, "usatechidxusd_h4")
        # 単調増加なら rolling_max = close なので drawdown = 0
        assert result["drawdown_20d"][24] == pytest.approx(0.0, abs=1e-10)

    def test_drawdown_20d_negative_after_drop(self):
        """価格が下落したらドローダウンが負"""
        closes = [100.0] * 15 + [90.0] * 5 + [85.0] * 5
        dates = [date(2024, 1, 1) + timedelta(days=i) for i in range(25)]
        df = pl.DataFrame({
            "obs_date": dates,
            "open": closes, "high": closes, "low": closes, "close": closes,
            "volume": [1000.0] * 25,
            "gap_from_prev_close_pct": [0.0] * 25,
        })
        result = _compute_instrument_features(df, "usatechidxusd_h4")
        # 末尾: close=85, rolling_max(20)=100 → -0.15
        assert result["drawdown_20d"][24] == pytest.approx(-0.15, rel=1e-6)

    def test_range_pct_1d(self):
        df = _make_daily_df(5)
        result = _compute_instrument_features(df, "usatechidxusd_h4")
        # row 0: (high - low) / close = 2.0 / 100.0
        assert result["range_pct_1d"][0] == pytest.approx(2.0 / 100.0, rel=1e-6)

    def test_all_instrument_feature_columns_present(self):
        df = _make_daily_df(25)
        result = _compute_instrument_features(df, "usatechidxusd_h4")
        for feat in INSTRUMENT_FEATURES:
            assert feat["name"] in result.columns, f"Missing: {feat['name']}"


# ── pair-level 特徴量 ──────────────────────────────────────────────

class TestPairFeatures:

    def test_corr_columns_present(self):
        df = _make_pair_df(65)
        result = _compute_pair_features(df, "usatechidxusd_h4")
        assert "corr_usatech_pair_20d" in result.columns
        assert "beta_usatech_pair_60d" in result.columns
        assert "lagcorr_usatech_pair_l1_20d" in result.columns
        assert "lagcorr_usatech_pair_l2_20d" in result.columns
        assert "risk_filter_flag" in result.columns

    def test_corr_range(self):
        """相関係数は -1 ≤ corr ≤ 1"""
        df = _make_pair_df(65)
        result = _compute_pair_features(df, "usatechidxusd_h4")
        vals = result["corr_usatech_pair_20d"].drop_nulls()
        assert (vals >= -1.0).all() and (vals <= 1.0).all()

    def test_positive_correlation_with_correlated_data(self):
        """fx_ret = 0.3 * usatech_ret + noise なので相関は正"""
        df = _make_pair_df(65)
        result = _compute_pair_features(df, "usatechidxusd_h4")
        # 十分なデータ後の相関は正
        last_corr = result["corr_usatech_pair_20d"].drop_nulls()[-1]
        assert last_corr > 0


# ── risk_filter_flag ──────────────────────────────────────────────

class TestRiskFilterFlag:

    def _make_filter_df(
        self,
        dd_last: float,
        rv_last: float,
        vix_last: float,
        usd_last: float,
        n: int = 50,
    ) -> pl.DataFrame:
        """
        先頭 n-1 行は正常値（各指標が clearly 低い）で、
        最終行だけ指定値にした DataFrame を生成する。
        これにより p80 は先頭行の分布から決まり、最終行の値が
        p80 超かどうかを明確にテストできる。
        """
        dates = [date(2024, 1, 1) + timedelta(days=i) for i in range(n)]
        # 先頭 n-1 行: 正常 (dd=-0.01, rv=0.05, vix=15, usd=100)
        dd   = [-0.01] * (n - 1) + [dd_last]
        rv   = [0.05]  * (n - 1) + [rv_last]
        vix  = [15.0]  * (n - 1) + [vix_last]
        usd  = [100.0] * (n - 1) + [usd_last]
        return pl.DataFrame({
            "obs_date":       dates,
            "usatech_ret":    [0.0] * n,
            "fx_ret":         [0.0] * n,
            "vix":            vix,
            "usd_broad":      usd,
            "_usatech_dd20":  dd,
            "_usatech_rv20":  rv,
        })

    def test_no_conditions_ok(self):
        """全条件が正常値 → 最終行は ok"""
        # 最終行も正常値のまま
        df = self._make_filter_df(dd_last=-0.01, rv_last=0.05, vix_last=15.0, usd_last=100.0)
        result = _apply_risk_filter(df)
        # 全行 ok (全値正常 → どの条件も非成立)
        assert (result["risk_filter_flag"] == "ok").all()

    def test_two_conditions_avoid(self):
        """dd + rv の 2 条件 → avoid"""
        # 最終行だけ dd=-0.10(<=−0.08: hit) & rv を全行中の最大に
        df = self._make_filter_df(dd_last=-0.10, rv_last=0.99, vix_last=15.0, usd_last=100.0)
        result = _apply_risk_filter(df)
        # 最終行: dd hit + rv hit (0.99 > p80 of [0.05]*49 + [0.99])
        assert result["risk_filter_flag"][-1] == "avoid"

    def test_one_condition_ok(self):
        """dd のみ条件 → ok (1 条件のみ)"""
        # rv / vix / usd は正常値のまま → p80 も正常値 → これらは非成立
        df = self._make_filter_df(dd_last=-0.10, rv_last=0.05, vix_last=15.0, usd_last=100.0)
        result = _apply_risk_filter(df)
        # dd hit のみ → 1 条件 → ok
        assert result["risk_filter_flag"][-1] == "ok"
