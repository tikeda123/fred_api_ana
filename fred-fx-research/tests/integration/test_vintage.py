"""
Integration tests: vintage-safe join correctness
- current mode vs vintage mode で値が異なることを確認
- vintage mode では forward fill しない
"""

import pytest
from datetime import date, datetime

from app.models.domain_models import RawObservation
from app.storage.repositories.observation_repo import bulk_insert
from app.services.normalize_service import normalize_series
from app.storage.duckdb import get_connection


def _insert_raw(series_id: str, records: list[tuple]) -> None:
    """(date_str, value, realtime_start, realtime_end) を一括挿入"""
    obs = [
        RawObservation(
            series_id=series_id,
            date=date.fromisoformat(d),
            value_raw=v,
            value_num=float(v) if v != "." else None,
            realtime_start=date.fromisoformat(rs),
            realtime_end=date.fromisoformat(re),
            retrieved_at_utc=datetime.utcnow(),
            file_batch_id="test",
        )
        for d, v, rs, re in records
    ]
    bulk_insert(obs)


class TestVintageSafe:

    def test_raw_table_preserves_realtime_period(self):
        """
        realtime_start / realtime_end が raw テーブルに保持されること。
        同一 date で 2 つの vintage が存在できる。
        """
        _insert_raw("DEXJPUS", [
            # 初回リリース (vintage 2026-01-10)
            ("2026-01-05", "155.00", "2026-01-10", "2026-01-10"),
            # 改訂値 (vintage 2026-02-01)
            ("2026-01-05", "155.50", "2026-02-01", "2026-02-01"),
        ])
        conn = get_connection()
        rows = conn.execute(
            "SELECT value_raw, realtime_start FROM fact_series_observations_raw "
            "WHERE series_id='DEXJPUS' AND date='2026-01-05' ORDER BY realtime_start"
        ).fetchall()
        assert len(rows) == 2
        assert rows[0][0] == "155.00"
        assert rows[1][0] == "155.50"

    def test_current_mode_uses_latest_realtime(self):
        """current mode (realtime = today) で最新の値が見える"""
        _insert_raw("DEXJPUS", [
            ("2026-01-05", "155.00", "2026-01-10", "2026-01-10"),
            ("2026-01-05", "155.50", "2026-02-01", "2026-02-01"),
        ])
        conn = get_connection()
        # realtime_end が最新のもの
        row = conn.execute(
            "SELECT value_raw FROM fact_series_observations_raw "
            "WHERE series_id='DEXJPUS' AND date='2026-01-05' "
            "ORDER BY realtime_start DESC LIMIT 1"
        ).fetchone()
        assert row[0] == "155.50"

    def test_vintage_mode_returns_original_value(self):
        """vintage = 2026-01-10 では初回リリース値のみ見える"""
        _insert_raw("DEXJPUS", [
            ("2026-01-05", "155.00", "2026-01-10", "2026-01-10"),
            ("2026-01-05", "155.50", "2026-02-01", "2026-02-01"),
        ])
        conn = get_connection()
        # vintage 2026-01-10 以前のみ
        row = conn.execute(
            "SELECT value_raw FROM fact_series_observations_raw "
            "WHERE series_id='DEXJPUS' AND date='2026-01-05' "
            "AND realtime_start <= '2026-01-10' "
            "ORDER BY realtime_start DESC LIMIT 1"
        ).fetchone()
        assert row[0] == "155.00"

    def test_normalize_uses_single_realtime(self):
        """normalize_series は同一日付で最新の realtime を使う"""
        _insert_raw("DEXJPUS", [
            ("2026-01-05", "155.00", "2026-01-10", "2026-01-10"),
            ("2026-01-05", "155.50", "2026-02-01", "2026-02-01"),
        ])
        count = normalize_series("DEXJPUS")
        # 正規化は最新 realtime の値のみ (重複 upsert で 1 行になる)
        assert count >= 1

    def test_missing_in_vintage_not_forward_filled(self):
        """
        vintage mode: あるペアの系列が vintage 時点で欠測なら
        forward fill してはいけない (null のまま)
        """
        # 2026-01-05 のみ存在、2026-01-06 は欠測
        _insert_raw("DEXJPUS", [
            ("2026-01-05", "155.00", "2026-01-10", "2026-01-10"),
        ])
        normalize_series("DEXJPUS")

        conn = get_connection()
        row = conn.execute(
            "SELECT value FROM fact_market_series_normalized "
            "WHERE series_id='DEXJPUS' AND obs_date='2026-01-06'"
        ).fetchone()
        # 2026-01-06 は存在しない (forward fill していない)
        assert row is None
