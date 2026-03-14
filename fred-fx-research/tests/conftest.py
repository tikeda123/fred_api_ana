"""
共通フィクスチャ
- インメモリ DuckDB (テスト用)
- golden dataset (DEXJPUS サンプル)
- FRED mock レスポンス
"""

import os
import pytest
import duckdb
from datetime import date, datetime
from pathlib import Path
import tempfile

# テスト中は一時 DuckDB を使う
@pytest.fixture(autouse=True)
def patch_duckdb(monkeypatch, tmp_path):
    """テストごとにクリーンな DuckDB を使う"""
    import app.storage.duckdb as db_mod
    import app.core.config as cfg_mod
    import app.services.normalize_service as norm_mod
    import app.storage.parquet_io as parquet_mod

    db_path = str(tmp_path / "test.duckdb")
    raw_root = str(tmp_path / "raw")
    norm_root = str(tmp_path / "normalized")
    der_root  = str(tmp_path / "derived")

    # settings オブジェクトを直接パッチ
    monkeypatch.setattr(cfg_mod.settings, "duckdb_path", db_path)
    monkeypatch.setattr(cfg_mod.settings, "raw_data_root", raw_root)
    monkeypatch.setattr(cfg_mod.settings, "normalized_data_root", norm_root)
    monkeypatch.setattr(cfg_mod.settings, "derived_data_root", der_root)
    monkeypatch.setattr(cfg_mod.settings, "fred_api_key", "test_key_dummy")

    # 各モジュールが参照している settings も同期
    monkeypatch.setattr(db_mod.settings,     "duckdb_path", db_path)
    monkeypatch.setattr(parquet_mod.settings, "raw_data_root", raw_root)
    monkeypatch.setattr(parquet_mod.settings, "normalized_data_root", norm_root)
    monkeypatch.setattr(parquet_mod.settings, "derived_data_root", der_root)

    # 接続キャッシュをリセット
    db_mod._conn = None
    yield
    if db_mod._conn:
        db_mod._conn.close()
        db_mod._conn = None


@pytest.fixture
def sample_observations():
    """DEXJPUS の固定サンプルデータ (golden dataset)"""
    return [
        {"date": "2026-01-02", "value": "156.72", "realtime_start": "2026-03-14", "realtime_end": "2026-03-14"},
        {"date": "2026-01-05", "value": "156.32", "realtime_start": "2026-03-14", "realtime_end": "2026-03-14"},
        {"date": "2026-01-06", "value": "156.70", "realtime_start": "2026-03-14", "realtime_end": "2026-03-14"},
        {"date": "2026-01-07", "value": "156.73", "realtime_start": "2026-03-14", "realtime_end": "2026-03-14"},
        {"date": "2026-01-08", "value": ".",       "realtime_start": "2026-03-14", "realtime_end": "2026-03-14"},
        {"date": "2026-01-09", "value": "157.80", "realtime_start": "2026-03-14", "realtime_end": "2026-03-14"},
    ]


@pytest.fixture
def fred_series_response():
    """FRED series metadata のモックレスポンス"""
    return {
        "seriess": [{
            "id": "DEXJPUS",
            "title": "Japanese Yen to U.S. Dollar Spot Exchange Rate",
            "frequency_short": "D",
            "units": "Japanese Yen to One U.S. Dollar",
            "seasonal_adjustment_short": "NSA",
            "observation_start": "1971-01-04",
            "observation_end": "2026-03-14",
            "last_updated": "2026-03-14T16:16:06-05:00",
            "notes": "",
        }]
    }


@pytest.fixture
def fred_observations_response(sample_observations):
    """FRED series/observations のモックレスポンス"""
    return {
        "realtime_start": "2026-03-14",
        "realtime_end": "2026-03-14",
        "observation_start": "2026-01-01",
        "observation_end": "2026-03-14",
        "observations": sample_observations,
    }
