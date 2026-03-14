"""
Unit tests: ValidationService
- 正常データ, high<low, null ts, 重複 ts, 逆順 ts, 異常 gap, 負 volume
"""

from datetime import datetime

import polars as pl
import pytest

from app.services.validation_service import validate_bars


def _make_df(**overrides) -> pl.DataFrame:
    """正常な 5 行の DataFrame を返す。overrides で列を上書き可能。"""
    base = {
        "ts_utc": [
            datetime(2024, 1, i + 1, 0, 0) for i in range(5)
        ],
        "open":   [100.0, 101.0, 102.0, 101.5, 103.0],
        "high":   [101.0, 102.0, 103.0, 102.5, 104.0],
        "low":    [ 99.0, 100.0, 101.0, 100.5, 102.0],
        "close":  [100.5, 101.5, 102.5, 102.0, 103.5],
        "volume": [1000.0, 1100.0, 900.0, 1200.0, 1050.0],
    }
    base.update(overrides)
    return pl.DataFrame(base)


class TestValidateBars:

    def test_valid_data(self):
        df = _make_df()
        result = validate_bars(df)
        assert result.is_valid is True
        assert result.total_rows == 5
        assert result.rejected_rows == 0
        assert result.valid_rows == 5
        assert result.flags == []

    def test_high_lt_low(self):
        df = _make_df(
            high=[101.0, 99.0, 103.0, 102.5, 104.0],  # row 1: high(99) < low(100)
            low=[99.0, 100.0, 101.0, 100.5, 102.0],
        )
        result = validate_bars(df)
        assert result.is_valid is False
        assert result.rejected_rows >= 1
        reasons = [f.reason for f in result.flags]
        assert "high_lt_low" in reasons

    def test_zero_price(self):
        df = _make_df(close=[100.5, 0.0, 102.5, 102.0, 103.5])
        result = validate_bars(df)
        assert result.is_valid is False
        reasons = [f.reason for f in result.flags]
        assert "zero_price" in reasons

    def test_null_timestamp(self):
        df = pl.DataFrame({
            "ts_utc": [datetime(2024, 1, 1), None, datetime(2024, 1, 3), datetime(2024, 1, 4), datetime(2024, 1, 5)],
            "open":   [100.0] * 5,
            "high":   [101.0] * 5,
            "low":    [99.0] * 5,
            "close":  [100.5] * 5,
            "volume": [1000.0] * 5,
        })
        result = validate_bars(df)
        assert result.is_valid is False
        reasons = [f.reason for f in result.flags]
        assert "ts_null" in reasons

    def test_duplicate_timestamp(self):
        ts = datetime(2024, 1, 1)
        df = _make_df(ts_utc=[ts, ts, datetime(2024, 1, 3), datetime(2024, 1, 4), datetime(2024, 1, 5)])
        result = validate_bars(df)
        assert result.is_valid is False
        reasons = [f.reason for f in result.flags]
        assert "duplicate_ts" in reasons

    def test_reverse_order(self):
        df = _make_df(ts_utc=[
            datetime(2024, 1, 5),
            datetime(2024, 1, 4),
            datetime(2024, 1, 3),
            datetime(2024, 1, 2),
            datetime(2024, 1, 1),
        ])
        result = validate_bars(df)
        reasons = [f.reason for f in result.flags]
        assert "reverse_order" in reasons

    def test_abnormal_gap_is_warning_only(self):
        """異常 gap は bad_rows に追加されないため rejected_rows は増えない"""
        df = _make_df(close=[100.0, 100.0, 100.0, 100.0, 200.0])  # 最終バーが +100%
        result = validate_bars(df)
        reasons = [f.reason for f in result.flags]
        assert "abnormal_gap" in reasons
        # gap は警告のみなので is_valid=True のまま（他エラーなし）
        assert result.is_valid is True

    def test_negative_volume(self):
        df = _make_df(volume=[1000.0, -500.0, 900.0, 1200.0, 1050.0])
        result = validate_bars(df)
        assert result.is_valid is False
        reasons = [f.reason for f in result.flags]
        assert "negative_volume" in reasons

    def test_summary_keys(self):
        df = _make_df()
        result = validate_bars(df)
        summary = result.summary()
        assert "is_valid" in summary
        assert "total_rows" in summary
        assert "valid_rows" in summary
        assert "rejected_rows" in summary
        assert "flag_count" in summary
