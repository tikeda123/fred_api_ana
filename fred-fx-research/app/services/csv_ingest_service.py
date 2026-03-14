"""
CSV Ingest Service
- USATECH CSV の取り込みパイプライン
- raw → norm の 2 層に保存し、upload audit を更新する
"""

import hashlib
import json
import math
from datetime import datetime
from pathlib import Path
from typing import Optional

import polars as pl

from app.core.exceptions import StorageError
from app.core.logging import get_logger
from app.services.validation_service import validate_bars
from app.storage.repositories import upload_repo, market_bar_repo

logger = get_logger(__name__)


def ingest(
    file_path: Path,
    instrument_id: str,
    upload_id: str,
    timeframe: str = "240",
    delimiter: str = "\t",
    has_header: bool = False,
    ts_format: str = "%Y-%m-%d %H:%M",
) -> dict:
    """
    CSV ファイルを raw + norm 層に取り込む。

    Returns:
        {"row_count_detected": int, "row_count_loaded": int, "row_count_rejected": int}
    """
    now = datetime.utcnow()

    # 取り込み開始を audit に反映
    upload_repo.update_status(upload_id, status="processing", finished_at=None)

    try:
        # 1. CSV パース
        df = _parse_csv(file_path, delimiter=delimiter, has_header=has_header, ts_format=ts_format)
        row_count_detected = len(df)
        logger.info("csv_parsed", upload_id=upload_id, rows=row_count_detected)

        # 2. バリデーション
        result = validate_bars(df)

        # 3. raw 挿入（全行を append。rejected 行にはフラグを付与）
        raw_df = _build_raw_df(df, upload_id, instrument_id, timeframe, file_path.name, result)
        market_bar_repo.insert_raw(raw_df)

        # 4. valid 行のみ norm に upsert
        valid_df = df.filter(pl.col("ts_utc").is_not_null())
        norm_df = _build_norm_df(valid_df, instrument_id, timeframe, upload_id)
        min_ts = norm_df["ts_utc"].min()
        max_ts = norm_df["ts_utc"].max()
        market_bar_repo.upsert_norm(instrument_id, timeframe, min_ts, max_ts, norm_df)

        row_count_loaded = len(norm_df)
        row_count_rejected = result.rejected_rows

        # 5. audit 完了
        upload_repo.update_status(
            upload_id,
            status="loaded",
            row_count_detected=row_count_detected,
            row_count_loaded=row_count_loaded,
            row_count_rejected=row_count_rejected,
            finished_at=datetime.utcnow(),
        )
        logger.info(
            "ingest_complete",
            upload_id=upload_id,
            detected=row_count_detected,
            loaded=row_count_loaded,
            rejected=row_count_rejected,
        )
        return {
            "row_count_detected": row_count_detected,
            "row_count_loaded": row_count_loaded,
            "row_count_rejected": row_count_rejected,
        }

    except Exception as e:
        upload_repo.update_status(
            upload_id,
            status="failed",
            error_message=str(e),
            finished_at=datetime.utcnow(),
        )
        logger.error("ingest_failed", upload_id=upload_id, error=str(e))
        raise StorageError(f"CSV 取り込み失敗: {e}") from e


def compute_file_hash(path: Path) -> str:
    """SHA-256 ハッシュを返す"""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


# ── 内部ヘルパー ─────────────────────────────────────────────────


def _parse_csv(
    path: Path,
    delimiter: str,
    has_header: bool,
    ts_format: str,
) -> pl.DataFrame:
    """タブ区切り・ヘッダーなし CSV をパースして DataFrame を返す"""
    try:
        df = pl.read_csv(
            path,
            separator=delimiter,
            has_header=has_header,
            new_columns=["datetime_str", "open", "high", "low", "close", "volume"],
            schema_overrides={
                "datetime_str": pl.Utf8,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Float64,
            },
            ignore_errors=True,
        )
    except Exception:
        # 空ファイル等
        return pl.DataFrame({"ts_utc": [], "open": [], "high": [], "low": [], "close": [], "volume": []}).cast(
            {"open": pl.Float64, "high": pl.Float64, "low": pl.Float64, "close": pl.Float64, "volume": pl.Float64}
        )

    df = df.with_columns(
        pl.col("datetime_str")
        .str.strptime(pl.Datetime, ts_format, strict=False)
        .alias("ts_utc")
    )
    return df.drop("datetime_str")


def _build_raw_df(
    df: pl.DataFrame,
    upload_id: str,
    instrument_id: str,
    timeframe: str,
    file_name: str,
    validation_result,
) -> pl.DataFrame:
    """raw テーブル用 DataFrame を構築する"""
    bad_rows = {f.row_number for f in validation_result.flags}

    now = datetime.utcnow()
    records = []
    for i, row in enumerate(df.iter_rows(named=True)):
        # source_line_hash: 全列を | 結合した md5
        raw_line = "|".join(str(v) if v is not None else "" for v in row.values())
        line_hash = hashlib.md5(raw_line.encode()).hexdigest()

        flags = [f.reason for f in validation_result.flags if f.row_number == i]
        is_bad = i in bad_rows

        records.append({
            "upload_id": upload_id,
            "instrument_id": instrument_id,
            "timeframe": timeframe,
            "source_file_name": file_name,
            "source_row_number": i,
            "ts_utc": row.get("ts_utc"),
            "open": row.get("open"),
            "high": row.get("high"),
            "low": row.get("low"),
            "close": row.get("close"),
            "volume": row.get("volume"),
            "source_line_hash": line_hash,
            "ingest_status": "rejected" if is_bad else "loaded",
            "quality_flags": json.dumps(flags) if flags else "",
            "ingested_at": now,
        })

    return pl.DataFrame(records, schema_overrides={"quality_flags": pl.Utf8})


def _build_norm_df(
    df: pl.DataFrame,
    instrument_id: str,
    timeframe: str,
    upload_id: str,
) -> pl.DataFrame:
    """正規化列を計算して norm テーブル用 DataFrame を返す"""
    df = df.sort("ts_utc")

    prev_close = df["close"].shift(1)
    prev_ts = df["ts_utc"].shift(1)

    # gap 時間（秒）
    ts_diff_seconds = (
        df["ts_utc"].cast(pl.Int64) - prev_ts.cast(pl.Int64)
    ) / 1_000_000  # microseconds → seconds

    now = datetime.utcnow()

    df = df.with_columns([
        pl.lit(instrument_id).alias("instrument_id"),
        pl.lit(timeframe).alias("timeframe"),
        pl.col("ts_utc").cast(pl.Date).alias("trade_date_utc"),
        pl.col("ts_utc").dt.year().alias("bar_year"),
        pl.col("ts_utc").dt.month().alias("bar_month"),
        (pl.col("close") / prev_close - 1).alias("simple_ret_1bar"),
        (pl.col("close") / prev_close).log(math.e).alias("log_ret_1bar"),
        ((pl.col("high") - pl.col("low")) / pl.col("close")).alias("hl_range_pct"),
        ((pl.col("close") - pl.col("open")).abs() / pl.col("open")).alias("oc_body_pct"),
        ((pl.col("open") - prev_close) / prev_close).alias("gap_from_prev_close_pct"),
        pl.col("ts_utc").dt.strftime("%H:%M").alias("h4_slot_utc"),
        pl.lit("ok").alias("quality_status"),
        pl.lit(upload_id).alias("source_upload_id"),
        pl.lit(now).alias("created_at"),
    ])

    # セッションバケット
    hour = pl.col("ts_utc").dt.hour()
    df = df.with_columns(
        pl.when(hour.is_in([0, 4, 8])).then(pl.lit("asia"))
        .when(hour == 12).then(pl.lit("europe_open"))
        .when(hour == 16).then(pl.lit("us_open"))
        .when(hour == 20).then(pl.lit("us_late"))
        .otherwise(pl.lit("other"))
        .alias("session_bucket")
    )

    # 週末ギャップ: 前バーとの時間差が 8h 超
    df = df.with_columns(
        (pl.Series("_ts_diff", ts_diff_seconds.to_list()) > 8 * 3600)
        .alias("is_weekend_gap")
    )

    return df.select([
        "instrument_id", "timeframe", "ts_utc", "trade_date_utc",
        "bar_year", "bar_month",
        "open", "high", "low", "close", "volume",
        "simple_ret_1bar", "log_ret_1bar", "hl_range_pct", "oc_body_pct",
        "gap_from_prev_close_pct", "h4_slot_utc", "session_bucket",
        "is_weekend_gap", "quality_status", "source_upload_id", "created_at",
    ])
