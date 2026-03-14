"""
DuckDB 接続管理
- シングルトン接続
- スキーマ自動初期化
"""

import os
from pathlib import Path

import duckdb

from app.core.config import settings
from app.core.exceptions import StorageError
from app.core.logging import get_logger

logger = get_logger(__name__)

_conn: duckdb.DuckDBPyConnection | None = None


def get_connection() -> duckdb.DuckDBPyConnection:
    """DuckDB 接続を返す（初回はスキーマを初期化する）"""
    global _conn
    if _conn is not None:
        return _conn

    db_path = settings.duckdb_path
    os.makedirs(Path(db_path).parent, exist_ok=True)

    try:
        _conn = duckdb.connect(db_path)
        _init_schema(_conn)
        logger.info("duckdb_connected", path=db_path)
        return _conn
    except Exception as e:
        raise StorageError(f"DuckDB 接続失敗: {e}") from e


def _init_schema(conn: duckdb.DuckDBPyConnection) -> None:
    schema_path = Path(__file__).parent.parent.parent / "sql" / "schema.sql"
    if not schema_path.exists():
        raise StorageError(f"schema.sql が見つかりません: {schema_path}")
    sql = schema_path.read_text(encoding="utf-8")
    conn.execute(sql)
    _migrate_schema(conn)

    # クロスアセット用スキーマ（md/ops/fact/mart）
    cross_asset_path = Path(__file__).parent.parent.parent / "sql" / "schema_cross_asset.sql"
    if cross_asset_path.exists():
        sql_ca = cross_asset_path.read_text(encoding="utf-8")
        conn.execute(sql_ca)

    logger.info("duckdb_schema_initialized")


def _migrate_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """既存 DB に対して後付け列を追加するマイグレーション"""
    migrations = [
        # fact_series_observations_raw に source_last_updated 追加
        (
            "fact_series_observations_raw",
            "source_last_updated",
            "ALTER TABLE fact_series_observations_raw ADD COLUMN source_last_updated TIMESTAMP",
        ),
        # fact_market_series_normalized に is_supplemental 追加 (CSV 補完フラグ)
        (
            "fact_market_series_normalized",
            "is_supplemental",
            "ALTER TABLE fact_market_series_normalized ADD COLUMN is_supplemental BOOLEAN DEFAULT FALSE",
        ),
    ]
    for table, column, ddl in migrations:
        try:
            existing = conn.execute(
                f"SELECT column_name FROM information_schema.columns "
                f"WHERE table_name = '{table}' AND column_name = '{column}'"
            ).fetchone()
            if existing is None:
                conn.execute(ddl)
                logger.info("schema_migrated", table=table, column=column)
        except Exception as e:
            logger.warning("schema_migration_skipped", table=table, column=column, reason=str(e))


def close_connection() -> None:
    global _conn
    if _conn is not None:
        _conn.close()
        _conn = None
        logger.info("duckdb_closed")


def check_health() -> str:
    """接続確認。ok / degraded を返す"""
    try:
        conn = get_connection()
        conn.execute("SELECT 1").fetchone()
        return "ok"
    except Exception:
        return "degraded"
