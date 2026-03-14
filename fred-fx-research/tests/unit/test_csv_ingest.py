"""
Unit tests: CsvIngestService
- CSV パース, ハッシュ計算, 正規化列, graceful 処理
"""

import hashlib
import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from app.services.csv_ingest_service import (
    _parse_csv,
    _build_norm_df,
    compute_file_hash,
)


# ── テスト用 CSV 作成ヘルパー ──────────────────────────────────────

def _write_csv(rows: list[str], delimiter: str = "\t") -> Path:
    """temp ファイルに CSV を書き込んで Path を返す"""
    content = "\n".join(rows)
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
    tmp.write(content)
    tmp.close()
    return Path(tmp.name)


SAMPLE_ROWS = [
    "2024-01-01 00:00\t100.0\t101.0\t99.0\t100.5\t1000",
    "2024-01-01 04:00\t100.5\t102.0\t100.0\t101.5\t1100",
    "2024-01-01 08:00\t101.5\t103.0\t101.0\t102.5\t900",
    "2024-01-02 00:00\t102.5\t104.0\t102.0\t103.5\t1200",
    "2024-01-02 04:00\t103.5\t105.0\t103.0\t104.5\t1050",
]


class TestParseCsv:

    def test_parse_basic(self):
        path = _write_csv(SAMPLE_ROWS)
        df = _parse_csv(path, delimiter="\t", has_header=False, ts_format="%Y-%m-%d %H:%M")
        assert len(df) == 5
        assert "ts_utc" in df.columns
        assert "open" in df.columns
        assert "close" in df.columns

    def test_parse_timestamp_type(self):
        path = _write_csv(SAMPLE_ROWS)
        df = _parse_csv(path, delimiter="\t", has_header=False, ts_format="%Y-%m-%d %H:%M")
        assert df["ts_utc"].dtype == pl.Datetime
        assert df["ts_utc"][0] == datetime(2024, 1, 1, 0, 0)

    def test_parse_price_values(self):
        path = _write_csv(SAMPLE_ROWS)
        df = _parse_csv(path, delimiter="\t", has_header=False, ts_format="%Y-%m-%d %H:%M")
        assert df["open"][0] == pytest.approx(100.0)
        assert df["close"][0] == pytest.approx(100.5)
        assert df["volume"][0] == pytest.approx(1000.0)

    def test_parse_empty_file(self):
        path = _write_csv([])
        df = _parse_csv(path, delimiter="\t", has_header=False, ts_format="%Y-%m-%d %H:%M")
        assert len(df) == 0


class TestComputeFileHash:

    def test_hash_consistency(self):
        path = _write_csv(SAMPLE_ROWS)
        h1 = compute_file_hash(path)
        h2 = compute_file_hash(path)
        assert h1 == h2
        assert len(h1) == 64  # SHA-256 hex

    def test_hash_differs_for_different_content(self):
        path1 = _write_csv(SAMPLE_ROWS)
        path2 = _write_csv(SAMPLE_ROWS[:-1])  # 1 行少ない
        assert compute_file_hash(path1) != compute_file_hash(path2)


class TestBuildNormDf:

    def _make_df(self) -> pl.DataFrame:
        path = _write_csv(SAMPLE_ROWS)
        return _parse_csv(path, delimiter="\t", has_header=False, ts_format="%Y-%m-%d %H:%M")

    def test_normalization_columns_exist(self):
        df = self._make_df()
        norm = _build_norm_df(df, "usatechidxusd_h4", "240", "upl_test_001")
        expected = [
            "instrument_id", "timeframe", "ts_utc", "trade_date_utc",
            "bar_year", "bar_month",
            "open", "high", "low", "close", "volume",
            "simple_ret_1bar", "log_ret_1bar", "hl_range_pct", "oc_body_pct",
            "gap_from_prev_close_pct", "h4_slot_utc", "session_bucket",
            "is_weekend_gap", "quality_status", "source_upload_id", "created_at",
        ]
        for col in expected:
            assert col in norm.columns, f"Missing column: {col}"

    def test_instrument_id_filled(self):
        df = self._make_df()
        norm = _build_norm_df(df, "usatechidxusd_h4", "240", "upl_test_001")
        assert norm["instrument_id"].unique().to_list() == ["usatechidxusd_h4"]

    def test_session_bucket_asia(self):
        """00:00 UTC は asia バケット"""
        df = self._make_df()
        norm = _build_norm_df(df, "usatechidxusd_h4", "240", "upl_test_001")
        first = norm.filter(pl.col("h4_slot_utc") == "00:00")["session_bucket"].to_list()
        assert all(b == "asia" for b in first)

    def test_simple_ret_1bar_first_null(self):
        """先頭行の ret は null"""
        df = self._make_df()
        norm = _build_norm_df(df, "usatechidxusd_h4", "240", "upl_test_001")
        assert norm["simple_ret_1bar"][0] is None

    def test_simple_ret_1bar_calculation(self):
        """2行目: close[1]/close[0] - 1 = 101.5/100.5 - 1"""
        df = self._make_df()
        norm = _build_norm_df(df, "usatechidxusd_h4", "240", "upl_test_001")
        expected = 101.5 / 100.5 - 1
        assert norm["simple_ret_1bar"][1] == pytest.approx(expected, rel=1e-6)

    def test_hl_range_pct(self):
        """row 0: (101 - 99) / 100.5"""
        df = self._make_df()
        norm = _build_norm_df(df, "usatechidxusd_h4", "240", "upl_test_001")
        expected = (101.0 - 99.0) / 100.5
        assert norm["hl_range_pct"][0] == pytest.approx(expected, rel=1e-6)

    def test_trade_date_utc(self):
        """ts_utc の日付部分が trade_date_utc に入る"""
        from datetime import date
        df = self._make_df()
        norm = _build_norm_df(df, "usatechidxusd_h4", "240", "upl_test_001")
        assert norm["trade_date_utc"][0] == date(2024, 1, 1)

    def test_is_weekend_gap_false_for_4h(self):
        """4h 連続バーは weekend gap ではない"""
        df = self._make_df()
        norm = _build_norm_df(df, "usatechidxusd_h4", "240", "upl_test_001")
        # row 1 (4h 後): is_weekend_gap = False
        assert norm["is_weekend_gap"][1] is False
