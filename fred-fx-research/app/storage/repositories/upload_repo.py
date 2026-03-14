"""
Upload Audit リポジトリ
- ops.fact_upload_audit の CRUD
"""

import json
from datetime import datetime
from typing import Optional

from app.core.logging import get_logger
from app.storage.duckdb import get_connection

logger = get_logger(__name__)


def create_audit(
    upload_id: str,
    instrument_id: str,
    source_system: str,
    file_name: str,
    file_hash: str,
    file_size: Optional[int],
    parser_options: Optional[dict] = None,
) -> None:
    """ops.fact_upload_audit に accepted レコードを作成する"""
    conn = get_connection()
    now = datetime.utcnow()
    conn.execute(
        """
        INSERT INTO ops.fact_upload_audit (
            upload_id, instrument_id, source_system,
            source_file_name, source_file_sha256, source_file_size_bytes,
            parser_name, parser_options_json,
            status, started_at, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'accepted', ?, ?)
        """,
        [
            upload_id, instrument_id, source_system,
            file_name, file_hash, file_size,
            "csv_tsv_parser",
            json.dumps(parser_options or {}),
            now, now,
        ],
    )
    logger.info("upload_audit_created", upload_id=upload_id, instrument_id=instrument_id)


def update_status(
    upload_id: str,
    status: str,
    row_count_detected: Optional[int] = None,
    row_count_loaded: Optional[int] = None,
    row_count_rejected: Optional[int] = None,
    error_message: Optional[str] = None,
    finished_at: Optional[datetime] = None,
) -> None:
    """upload の status と集計値を更新する"""
    conn = get_connection()
    conn.execute(
        """
        UPDATE ops.fact_upload_audit SET
            status = ?,
            row_count_detected = COALESCE(?, row_count_detected),
            row_count_loaded   = COALESCE(?, row_count_loaded),
            row_count_rejected = COALESCE(?, row_count_rejected),
            error_message = COALESCE(?, error_message),
            finished_at   = COALESCE(?, finished_at)
        WHERE upload_id = ?
        """,
        [
            status,
            row_count_detected, row_count_loaded, row_count_rejected,
            error_message, finished_at,
            upload_id,
        ],
    )
    logger.info("upload_status_updated", upload_id=upload_id, status=status)


def get_audit(upload_id: str) -> Optional[dict]:
    """upload_id で audit レコードを取得する"""
    conn = get_connection()
    row = conn.execute(
        """
        SELECT upload_id, instrument_id, source_system,
               source_file_name, source_file_sha256, source_file_size_bytes,
               parser_name, parser_options_json,
               row_count_detected, row_count_loaded, row_count_rejected,
               started_at, finished_at, status, error_message, created_at
        FROM ops.fact_upload_audit
        WHERE upload_id = ?
        """,
        [upload_id],
    ).fetchone()
    if row is None:
        return None
    cols = [
        "upload_id", "instrument_id", "source_system",
        "source_file_name", "source_file_sha256", "source_file_size_bytes",
        "parser_name", "parser_options_json",
        "row_count_detected", "row_count_loaded", "row_count_rejected",
        "started_at", "finished_at", "status", "error_message", "created_at",
    ]
    rec = dict(zip(cols, row))
    for k in ("started_at", "finished_at", "created_at"):
        if rec[k] is not None:
            rec[k] = str(rec[k])
    return rec


def list_recent(limit: int = 20) -> list[dict]:
    """最近の upload audit を新しい順に返す"""
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT upload_id, instrument_id, source_file_name,
               row_count_detected, row_count_loaded, row_count_rejected,
               started_at, finished_at, status, error_message
        FROM ops.fact_upload_audit
        ORDER BY created_at DESC
        LIMIT ?
        """,
        [limit],
    ).fetchall()
    cols = [
        "upload_id", "instrument_id", "source_file_name",
        "row_count_detected", "row_count_loaded", "row_count_rejected",
        "started_at", "finished_at", "status", "error_message",
    ]
    result = []
    for row in rows:
        rec = dict(zip(cols, row))
        for k in ("started_at", "finished_at"):
            if rec[k] is not None:
                rec[k] = str(rec[k])
        result.append(rec)
    return result
