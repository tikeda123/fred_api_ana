"""
Market Uploads API
- POST /market/uploads  : CSV アップロード＆取り込み
- GET  /market/uploads/{upload_id} : 状態確認
- GET  /market/uploads  : 最近の一覧
"""

import uuid
from datetime import datetime
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, BackgroundTasks, File, Form, HTTPException, UploadFile
from pydantic import BaseModel

from app.core.config import settings
from app.core.logging import get_logger
from app.models.api_models import ApiResponse, MarketUploadAccepted, UploadStatusResponse
from app.services import csv_ingest_service
from app.storage.repositories import upload_repo

logger = get_logger(__name__)
router = APIRouter(prefix="/market", tags=["market-uploads"])

# アップロードファイルの保存先
UPLOAD_DIR = Path(settings.raw_data_root) / "uploads"


def _instrument_id(canonical_symbol: str, timeframe: str) -> str:
    """instrument_id を生成する: 'usatechidxusd_h4' 形式"""
    sym = canonical_symbol.lower()
    tf_map = {"240": "h4", "1440": "d1", "60": "h1", "15": "m15", "5": "m5", "1": "m1"}
    suffix = tf_map.get(timeframe, f"tf{timeframe}")
    return f"{sym}_{suffix}"


def _upsert_instrument(
    instrument_id: str,
    vendor_symbol: str,
    canonical_symbol: str,
    instrument_name: str | None,
    asset_class: str,
    base_ccy: str,
    quote_ccy: str,
    timeframe: str,
    delimiter: str,
    has_header: bool,
    ts_format: str,
) -> None:
    """md.dim_market_instrument を upsert する"""
    from app.storage.duckdb import get_connection
    conn = get_connection()
    now = datetime.utcnow()
    conn.execute(
        """
        INSERT INTO md.dim_market_instrument (
            instrument_id, source_system, source_vendor,
            vendor_symbol, canonical_symbol, instrument_name,
            asset_class, base_ccy, quote_ccy,
            timeframe_native, timezone_native,
            file_format, delimiter, has_header, ts_format,
            is_active, created_at, updated_at
        ) VALUES (?, 'local_csv', 'manual_upload', ?, ?, ?, ?, ?, ?, ?, 'UTC',
                  'csv_tsv', ?, ?, ?, TRUE, ?, ?)
        ON CONFLICT (instrument_id) DO UPDATE SET
            updated_at = excluded.updated_at,
            is_active  = TRUE
        """,
        [
            instrument_id, vendor_symbol, canonical_symbol,
            instrument_name or f"{canonical_symbol} ({timeframe})",
            asset_class, base_ccy, quote_ccy, timeframe,
            delimiter, has_header, ts_format,
            now, now,
        ],
    )


@router.post("/uploads", status_code=202)
async def upload_market_csv(
    background_tasks: BackgroundTasks,
    file: Annotated[UploadFile, File()],
    vendor_symbol: Annotated[str, Form()],
    canonical_symbol: Annotated[str, Form()],
    instrument_name: Annotated[str | None, Form()] = None,
    asset_class: Annotated[str, Form()] = "equity_index",
    base_ccy: Annotated[str, Form()] = "USATECHIDX",
    quote_ccy: Annotated[str, Form()] = "USD",
    timeframe: Annotated[str, Form()] = "240",
    timezone_native: Annotated[str, Form()] = "UTC",
    delimiter: Annotated[str, Form()] = "\t",
    has_header: Annotated[bool, Form()] = False,
    ts_format: Annotated[str, Form()] = "%Y-%m-%d %H:%M",
) -> ApiResponse:
    """CSV/TSV をアップロードしてバックグラウンドで取り込む"""
    # ── ファイル保存 ───────────────────────────────────────────────
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    upload_id = f"upl_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    safe_name = Path(file.filename or "upload.csv").name
    dest_path = UPLOAD_DIR / f"{upload_id}_{safe_name}"

    contents = await file.read()
    dest_path.write_bytes(contents)

    file_size = len(contents)
    file_hash = csv_ingest_service.compute_file_hash(dest_path)
    instrument_id = _instrument_id(canonical_symbol, timeframe)

    # ── instrument 登録 ────────────────────────────────────────────
    _upsert_instrument(
        instrument_id, vendor_symbol, canonical_symbol, instrument_name,
        asset_class, base_ccy, quote_ccy, timeframe, delimiter, has_header, ts_format,
    )

    # ── audit 作成 ────────────────────────────────────────────────
    parser_options = {
        "delimiter": delimiter,
        "has_header": has_header,
        "ts_format": ts_format,
        "timeframe": timeframe,
    }
    upload_repo.create_audit(
        upload_id=upload_id,
        instrument_id=instrument_id,
        source_system="api_upload",
        file_name=safe_name,
        file_hash=file_hash,
        file_size=file_size,
        parser_options=parser_options,
    )

    # ── バックグラウンド取り込み ──────────────────────────────────
    background_tasks.add_task(
        csv_ingest_service.ingest,
        file_path=dest_path,
        instrument_id=instrument_id,
        upload_id=upload_id,
        timeframe=timeframe,
        delimiter=delimiter,
        has_header=has_header,
        ts_format=ts_format,
    )

    logger.info("upload_accepted", upload_id=upload_id, file=safe_name, size=file_size)

    return ApiResponse(
        data=MarketUploadAccepted(
            upload_id=upload_id,
            instrument_id=instrument_id,
            status="accepted",
            next=f"/api/v1/market/uploads/{upload_id}",
        ).model_dump(),
        as_of=datetime.utcnow(),
    )


class IngestFromPathRequest(BaseModel):
    """AI エージェント向け: ローカルファイルパス指定で CSV を取り込む"""
    file_path: str
    vendor_symbol: str = "USATECHIDXUSD"
    canonical_symbol: str = "USATECHIDXUSD"
    instrument_name: str | None = None
    asset_class: str = "equity_index"
    base_ccy: str = "USATECHIDX"
    quote_ccy: str = "USD"
    timeframe: str = "240"
    delimiter: str = "\t"
    has_header: bool = False
    ts_format: str = "%Y-%m-%d %H:%M"


@router.post("/uploads/from-path", status_code=202)
async def upload_from_path(
    req: IngestFromPathRequest,
    background_tasks: BackgroundTasks,
) -> ApiResponse:
    """
    ローカルファイルパスを指定して CSV を取り込む（AI エージェント向け）。
    multipart 不要で JSON リクエストのみで呼び出し可能。
    """
    src = Path(req.file_path)
    if not src.exists():
        raise HTTPException(status_code=400, detail=f"ファイルが見つかりません: {req.file_path}")

    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    upload_id = f"upl_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    safe_name = src.name

    # ファイルをアップロードディレクトリにコピー
    import shutil
    dest_path = UPLOAD_DIR / f"{upload_id}_{safe_name}"
    shutil.copy2(src, dest_path)

    file_size = dest_path.stat().st_size
    file_hash = csv_ingest_service.compute_file_hash(dest_path)
    instrument_id = _instrument_id(req.canonical_symbol, req.timeframe)

    _upsert_instrument(
        instrument_id, req.vendor_symbol, req.canonical_symbol, req.instrument_name,
        req.asset_class, req.base_ccy, req.quote_ccy, req.timeframe,
        req.delimiter, req.has_header, req.ts_format,
    )

    parser_options = {
        "delimiter": req.delimiter,
        "has_header": req.has_header,
        "ts_format": req.ts_format,
        "timeframe": req.timeframe,
    }
    upload_repo.create_audit(
        upload_id=upload_id,
        instrument_id=instrument_id,
        source_system="api_path",
        file_name=safe_name,
        file_hash=file_hash,
        file_size=file_size,
        parser_options=parser_options,
    )

    background_tasks.add_task(
        csv_ingest_service.ingest,
        file_path=dest_path,
        instrument_id=instrument_id,
        upload_id=upload_id,
        timeframe=req.timeframe,
        delimiter=req.delimiter,
        has_header=req.has_header,
        ts_format=req.ts_format,
    )

    logger.info("upload_from_path_accepted", upload_id=upload_id, path=req.file_path)

    return ApiResponse(
        data=MarketUploadAccepted(
            upload_id=upload_id,
            instrument_id=instrument_id,
            status="accepted",
            next=f"/api/v1/market/uploads/{upload_id}",
        ).model_dump(),
        as_of=datetime.utcnow(),
    )


@router.get("/uploads/{upload_id}")
async def get_upload_status(upload_id: str) -> ApiResponse:
    """upload の処理状態を返す"""
    rec = upload_repo.get_audit(upload_id)
    if rec is None:
        raise HTTPException(status_code=404, detail=f"upload_id not found: {upload_id}")

    return ApiResponse(
        data=UploadStatusResponse(
            upload_id=rec["upload_id"],
            instrument_id=rec["instrument_id"],
            status=rec["status"],
            row_count_detected=rec.get("row_count_detected"),
            row_count_loaded=rec.get("row_count_loaded"),
            row_count_rejected=rec.get("row_count_rejected"),
            started_at=rec.get("started_at"),
            finished_at=rec.get("finished_at"),
            error_message=rec.get("error_message"),
        ).model_dump(),
        as_of=datetime.utcnow(),
    )


@router.get("/uploads")
async def list_uploads(limit: int = 20) -> ApiResponse:
    """最近のアップロード一覧を返す"""
    records = upload_repo.list_recent(limit=min(limit, 100))
    return ApiResponse(data=records, as_of=datetime.utcnow())
